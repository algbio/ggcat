use crate::compressed_read::{CompressedRead, CompressedReadIndipendent};
use crate::hash::HashableSequence;
use crate::sequences_reader::FastaSequence;
use crate::types::BucketIndexType;
use crate::utils::{cast_static, cast_static_mut, Utils};
use crate::varint::{decode_varint, encode_varint};
use crate::DEFAULT_BUFFER_SIZE;
use byteorder::{BigEndian, ByteOrder, LittleEndian, ReadBytesExt, WriteBytesExt};
use desse::{Desse, DesseSized};
use flate2::write::GzEncoder;
use flate2::Compression;
use lz4::{BlockMode, BlockSize, ContentChecksum};
use os_pipe::{PipeReader, PipeWriter};
use parallel_processor::multi_thread_buckets::{BucketType, MultiThreadBuckets};
use replace_with::replace_with_or_abort;
use serde::{Deserialize, Serialize};
use std::cell::{Cell, UnsafeCell};
use std::cmp::{max, min};
use std::fmt::Debug;
use std::fs::{File, OpenOptions};
use std::hash::Hasher;
use std::io::{stdin, stdout, BufRead, BufReader, BufWriter, Error, Read, Seek, SeekFrom, Write};
use std::marker::PhantomData;
use std::ops::DerefMut;
use std::path::{Path, PathBuf};
use std::process::{ChildStdin, Command, Stdio};
use std::slice::from_raw_parts;

pub trait SequenceExtraData: Sized + Send + Debug {
    fn decode(reader: impl Read) -> Option<Self>;
    fn encode(&self, writer: impl Write);
}

impl SequenceExtraData for () {
    #[inline(always)]
    fn decode(reader: impl Read) -> Option<Self> {
        Some(())
    }

    #[inline(always)]
    fn encode(&self, writer: impl Write) {}
}

const INTERMEDIATE_READS_MAGIC: [u8; 16] = *b"BILOKI_INTEREADS";

#[derive(Debug, Desse, DesseSized, Default)]
struct IntermediateReadsHeader {
    magic: [u8; 16],
    index_offset: u64,
}

#[derive(Serialize, Deserialize)]
struct IntermediateReadsIndex {
    index: Vec<u64>,
}

pub struct IntermediateReadsWriter<T> {
    writer: lz4::Encoder<BufWriter<File>>,
    chunk_size: u64,
    index: IntermediateReadsIndex,
    path: PathBuf,
    _phantom: PhantomData<T>,
}

struct SequentialReader {
    reader: lz4::Decoder<BufReader<File>>,
    index: IntermediateReadsIndex,
    index_position: u64,
}

impl Read for SequentialReader {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        loop {
            match self.reader.read(buf) {
                Ok(read) => {
                    if read != 0 {
                        return Ok(read);
                    }
                }
                Err(err) => return Err(err),
            }
            self.index_position += 1;
            if self.index.index.len() as u64 >= self.index_position {
                return Err(std::io::Error::from(std::io::ErrorKind::UnexpectedEof));
            }

            replace_with_or_abort(&mut self.reader, |reader| {
                let mut file = reader.finish().0;
                file.seek(SeekFrom::Start(
                    self.index.index[self.index_position as usize],
                ))
                .unwrap();
                lz4::Decoder::new(file).unwrap()
            });
        }
    }
}

pub struct IntermediateReadsReader<T: SequenceExtraData> {
    remove_file: bool,
    reader: SequentialReader,
    file_path: PathBuf,
    _phantom: PhantomData<T>,
}

pub struct IntermediateSequencesStorage<'a, T: SequenceExtraData> {
    buckets: &'a MultiThreadBuckets<IntermediateReadsWriter<T>>,
    buffers: Vec<Vec<u8>>,
}
impl<'a, T: SequenceExtraData> IntermediateSequencesStorage<'a, T> {
    const ALLOWED_LEN: usize = 65536;

    pub fn new(
        buckets_count: usize,
        buckets: &'a MultiThreadBuckets<IntermediateReadsWriter<T>>,
    ) -> Self {
        let mut buffers = Vec::with_capacity(buckets_count);
        for i in 0..buckets_count {
            buffers.push(Vec::with_capacity(parallel_processor::Utils::multiply_by(
                Self::ALLOWED_LEN,
                1.05,
            )));
        }

        Self { buckets, buffers }
    }

    fn flush_buffers(&mut self, bucket: BucketIndexType) {
        if self.buffers[bucket as usize].len() == 0 {
            return;
        }

        self.buckets
            .add_data(bucket, &self.buffers[bucket as usize]);
        self.buffers[bucket as usize].clear();
    }

    pub fn add_read(&mut self, el: T, seq: &[u8], bucket: BucketIndexType) {
        if self.buffers[bucket as usize].len() > 0
            && self.buffers[bucket as usize].len() + seq.len() > Self::ALLOWED_LEN
        {
            self.flush_buffers(bucket);
        }

        // println!(
        //     "Saving sequence {} to bucket {} with flags {:?}",
        //     std::str::from_utf8(seq).unwrap(),
        //     bucket,
        //     el
        // );

        el.encode(&mut self.buffers[bucket as usize]);
        CompressedRead::from_plain_write_directly_to_buffer(
            seq,
            &mut self.buffers[bucket as usize],
        );
    }

    pub fn finalize(self) {}
}

impl<'a, T: SequenceExtraData> Drop for IntermediateSequencesStorage<'a, T> {
    fn drop(&mut self) {
        for bucket in 0..self.buffers.len() {
            if self.buffers[bucket].len() > 0 {
                self.flush_buffers(bucket as BucketIndexType);
            }
        }
    }
}

impl<T: SequenceExtraData> IntermediateReadsWriter<T> {
    fn create_new_block(&mut self) {
        replace_with_or_abort(&mut self.writer, |writer| {
            let (mut file_buf, res) = writer.finish();
            res.unwrap();

            let checkpoint_pos = file_buf.stream_position().unwrap();
            self.index.index.push(checkpoint_pos);

            lz4::EncoderBuilder::new().level(0).build(file_buf).unwrap()
        });
    }
}

const MAXIMUM_CHUNK_SIZE: u64 = 1024 * 1024 * 32;

impl<T: SequenceExtraData> BucketType for IntermediateReadsWriter<T> {
    type InitType = Path;
    const SUPPORTS_LOCK_FREE: bool = false;

    fn new(init_data: &Path, index: usize) -> Self {
        let path = init_data.parent().unwrap().join(format!(
            "{}.{}.tmp",
            init_data.file_name().unwrap().to_str().unwrap(),
            index
        ));

        let mut file = File::create(&path).unwrap();

        // Write empty header
        file.write_all(&IntermediateReadsHeader::default().serialize()[..]);

        let first_block_pos = file.stream_position().unwrap();
        let mut compress_stream = lz4::EncoderBuilder::new()
            .level(0)
            .checksum(ContentChecksum::NoChecksum)
            .block_mode(BlockMode::Independent)
            .block_size(BlockSize::Default)
            .build(BufWriter::with_capacity(1024 * 512, file))
            .unwrap();

        IntermediateReadsWriter {
            writer: compress_stream,
            chunk_size: 0,
            index: IntermediateReadsIndex {
                index: vec![first_block_pos],
            },
            path,
            _phantom: PhantomData,
        }
    }

    fn write_bytes(&mut self, bytes: &[u8]) {
        self.writer.write_all(bytes).unwrap();
        self.chunk_size += bytes.len() as u64;
        if self.chunk_size > MAXIMUM_CHUNK_SIZE {
            self.create_new_block();
        }
    }

    fn get_path(&self) -> PathBuf {
        self.path.clone()
    }

    fn finalize(mut self) {
        self.writer.flush();
        let (mut file, res) = self.writer.finish();
        res.unwrap();

        file.flush();
        let index_position = file.stream_position().unwrap();
        bincode::serialize_into(&mut file, &self.index).unwrap();

        file.seek(SeekFrom::Start(0));
        file.write_all(
            &IntermediateReadsHeader {
                magic: INTERMEDIATE_READS_MAGIC,
                index_offset: index_position,
            }
            .serialize()[..],
        )
        .unwrap();

        file.flush().unwrap();
    }
}

pub struct VecReader<'a, R: Read> {
    vec: Vec<u8>,
    fill: usize,
    pos: usize,
    reader: &'a mut R,
    stream_ended: bool,
}

impl<'a, R: Read> VecReader<'a, R> {
    pub fn new(capacity: usize, reader: &'a mut R) -> VecReader<'a, R> {
        let mut vec = vec![];
        vec.resize(capacity, 0);
        VecReader {
            vec,
            fill: 0,
            pos: 0,
            reader,
            stream_ended: false,
        }
    }

    fn update_buffer(&mut self) {
        self.fill = match self.reader.read(&mut self.vec[..]) {
            Ok(fill) => fill,
            Err(_) => 0,
        };
        self.stream_ended = self.fill == 0;
        self.pos = 0;
    }

    pub fn read_byte(&mut self) -> u8 {
        if self.fill == self.pos {
            self.update_buffer();

            if self.fill == self.pos {
                return 0;
            }
        }
        let value = unsafe { *self.vec.get_unchecked(self.pos) };

        self.pos += 1;
        return value;
    }

    pub fn read_bytes(&mut self, slice: &mut [u8]) -> usize {
        let mut offset = 0;

        while offset < slice.len() {
            if self.fill == self.pos {
                self.update_buffer();

                if self.fill == self.pos {
                    return offset;
                }
            }

            let amount = min(slice.len() - offset, self.fill - self.pos);

            unsafe {
                std::ptr::copy(
                    self.vec.as_ptr().add(self.pos),
                    slice.as_mut_ptr().add(offset),
                    amount,
                );
            }

            self.pos += amount;
            offset += amount;
        }
        offset
    }
}

impl<'a, R: Read> Read for VecReader<'a, R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        Ok(self.read_bytes(buf))
    }
}
impl<T: SequenceExtraData> IntermediateReadsReader<T> {
    pub fn new(name: impl AsRef<Path>, remove_file: bool) -> Self {
        let mut file = File::open(&name)
            .unwrap_or_else(|_| panic!("Cannot open file {}", name.as_ref().display()));

        let mut header_buffer = [0; IntermediateReadsHeader::SIZE];
        file.read_exact(&mut header_buffer);

        let header: IntermediateReadsHeader =
            IntermediateReadsHeader::deserialize_from(&header_buffer);
        assert_eq!(header.magic, INTERMEDIATE_READS_MAGIC);

        file.seek(SeekFrom::Start(header.index_offset)).unwrap();
        let index: IntermediateReadsIndex = bincode::deserialize_from(&mut file).unwrap();

        file.seek(SeekFrom::Start(index.index[0])).unwrap();

        Self {
            reader: SequentialReader {
                reader: lz4::Decoder::new(BufReader::with_capacity(1024 * 1024 * 4, file)).unwrap(),
                index,
                index_position: 0,
            },
            remove_file,
            file_path: name.as_ref().to_path_buf(),
            _phantom: PhantomData,
        }
    }

    pub fn for_each(mut self, mut lambda: impl FnMut(T, CompressedRead)) {
        let mut vec_reader = VecReader::new(1024 * 1024, &mut self.reader);

        // const LETTERS: [u8; 4] = [b'A', b'C', b'T', b'G'];
        let mut read = vec![];

        while let Some(el) = T::decode(&mut vec_reader) {
            let size = match decode_varint(|| Some(vec_reader.read_byte())) {
                None => break,
                Some(x) => x,
            } as usize;

            if size == 0 && vec_reader.stream_ended {
                break;
            }
            let bytes = ((size + 3) / 4);
            read.resize(max(read.len(), bytes), 0);

            vec_reader.read_bytes(&mut read[..bytes]);

            lambda(
                el,
                CompressedRead::new_from_compressed(&read[..bytes], size),
            );

            // read.resize(max(read.len(), size + 32), 0);
            //
            // let bytes = ((size + 15) / 16) * 4;
            // let mut pos = 0;
            //
            // //             let mut remlen = len;
            // //
            // //             while remlen > 0 {
            // //                 let mut value = self.reader.read_u32::<LittleEndian>().unwrap();
            // //                 let enclen = min(16, remlen);
            // // `                // for b in (0..enclen).rev() {
            // //                 //     data[b] = LETTERS[(value & 0x3) as usize];
            // //                 //     value >>= 2;
            // //                 // }
            // //                 // read.extend_from_slice(&data[0..enclen]);
            // //                 remlen -= enclen;
            // //             }
            //
            // for _ in 0..bytes {
            //     let byte = vec_reader.read_byte();
            //     read[pos + 0] = LETTERS[((byte >> 0) & 0x3) as usize];
            //     read[pos + 1] = LETTERS[((byte >> 2) & 0x3) as usize];
            //     read[pos + 2] = LETTERS[((byte >> 4) & 0x3) as usize];
            //     read[pos + 3] = LETTERS[((byte >> 6) & 0x3) as usize];
            //     pos += 4;
            // }
            //
            // lambda(&read[0..size]);
        }

        drop(vec_reader);
        if self.remove_file {
            std::fs::remove_file(self.file_path);
        }

        //
        //
        //         let mut count = 0;
        //         let mut tot_len = 0;
        //         while let Some(len) = decode_varint(&mut self.reader) {
        //             let len = len as usize;
        //             read.clear();
        //             let size = (len + 15) / 16;
        //             let mut data: [u8; 16] = [0; 16];
        //
        //             count += 1;
        //             tot_len += len;
        //             if count % 100000 == 0 || size > 1000 {
        //                 eprintln!("Count {} size {} len {}", count, len, tot_len);
        //             }
        //

        //             // lambda(read.as_slice())
        //         }
    }
}
