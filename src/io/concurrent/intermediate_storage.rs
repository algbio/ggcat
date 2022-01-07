use crate::config::BucketIndexType;
use crate::hashes::HashableSequence;
use crate::io::sequences_reader::FastaSequence;
use crate::io::varint::{decode_varint, encode_varint};
use crate::io::{DataReader, DataWriter};
use crate::utils::compressed_read::{CompressedRead, CompressedReadIndipendent};
use crate::utils::{cast_static, cast_static_mut, Utils};
use crate::DEFAULT_BUFFER_SIZE;
use byteorder::{BigEndian, ByteOrder, LittleEndian, ReadBytesExt, WriteBytesExt};
use desse::{Desse, DesseSized};
use filebuffer::FileBuffer;
use flate2::write::GzEncoder;
use flate2::Compression;
use lz4::{BlockMode, BlockSize, ContentChecksum};
use parallel_processor::memory_fs::file::reader::FileReader;
use parallel_processor::memory_fs::file::writer::FileWriter;
use parallel_processor::memory_fs::MemoryFs;
use parallel_processor::multi_thread_buckets::{BucketType, MultiThreadBuckets};
use replace_with::replace_with_or_abort;
use serde::{Deserialize, Serialize};
use std::cell::{Cell, UnsafeCell};
use std::cmp::{max, min};
use std::fmt::Debug;
use std::fs::{File, OpenOptions};
use std::hash::Hasher;
use std::io::{
    stdin, stdout, BufRead, BufReader, BufWriter, Cursor, Error, Read, Seek, SeekFrom, Write,
};
use std::marker::PhantomData;
use std::ops::DerefMut;
use std::path::{Path, PathBuf};
use std::process::{ChildStdin, Command, Stdio};
use std::slice::from_raw_parts;
use std::sync::atomic::{AtomicU64, Ordering};

struct PointerDecoder {
    ptr: *const u8,
}

impl Read for PointerDecoder {
    #[inline(always)]
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        unsafe {
            std::ptr::copy_nonoverlapping(self.ptr, buf.as_mut_ptr(), buf.len());
            self.ptr = self.ptr.add(buf.len());
        }
        Ok(buf.len())
    }

    #[inline(always)]
    fn read_exact(&mut self, buf: &mut [u8]) -> std::io::Result<()> {
        unsafe {
            std::ptr::copy_nonoverlapping(self.ptr, buf.as_mut_ptr(), buf.len());
            self.ptr = self.ptr.add(buf.len());
        }
        Ok(())
    }
}

pub trait SequenceExtraData: Sized + Send + Debug {
    fn decode_from_slice(slice: &[u8]) -> Option<Self> {
        let cursor = Cursor::new(slice);
        Self::decode(cursor)
    }

    unsafe fn decode_from_pointer(ptr: *const u8) -> Option<Self> {
        let stream = PointerDecoder { ptr };
        Self::decode(stream)
    }

    fn decode(reader: impl Read) -> Option<Self>;
    fn encode(&self, writer: impl Write);

    fn max_size(&self) -> usize;
}

impl SequenceExtraData for () {
    #[inline(always)]
    fn decode(reader: impl Read) -> Option<Self> {
        Some(())
    }

    #[inline(always)]
    fn encode(&self, writer: impl Write) {}

    #[inline(always)]
    fn max_size(&self) -> usize {
        0
    }
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

pub type IntermediateReadsWriterFile<T> = IntermediateReadsWriter<T, BufWriter<File>>;

pub type IntermediateReadsWriterMemFs<T> = IntermediateReadsWriter<T, FileWriter>;

pub struct IntermediateReadsWriter<T, W: DataWriter> {
    writer: lz4::Encoder<W>,
    chunk_size: u64,
    index: IntermediateReadsIndex,
    path: PathBuf,
    _phantom: PhantomData<T>,
}

struct SequentialReader<R: DataReader> {
    reader: lz4::Decoder<R>,
    index: IntermediateReadsIndex,
    index_position: u64,
}

impl<R: DataReader> Read for SequentialReader<R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        loop {
            match self.reader.read(buf) {
                Ok(read) => {
                    if read != 0 {
                        return Ok(read);
                    }
                }
                Err(err) => {
                    return Err(err);
                }
            }
            self.index_position += 1;

            if self.index_position >= self.index.index.len() as u64 {
                return Err(std::io::Error::from(std::io::ErrorKind::UnexpectedEof));
            }

            replace_with_or_abort(&mut self.reader, |reader| {
                let mut file = reader.finish().0;
                assert_eq!(
                    file.stream_position().unwrap(),
                    self.index.index[self.index_position as usize]
                );
                file.seek(SeekFrom::Start(
                    self.index.index[self.index_position as usize],
                ))
                .unwrap();
                lz4::Decoder::new(file).unwrap()
            });
        }
    }
}

pub struct IntermediateReadsReader<T: SequenceExtraData, R: DataReader> {
    remove_file: bool,
    sequential_reader: SequentialReader<R>,
    parallel_reader: FileReader,
    parallel_index: AtomicU64,
    file_path: PathBuf,
    _phantom: PhantomData<T>,
}

unsafe impl<T: SequenceExtraData, R: DataReader> Sync for IntermediateReadsReader<T, R> {}

pub struct IntermediateSequencesStorage<'a, T: SequenceExtraData, W: DataWriter> {
    buckets: &'a MultiThreadBuckets<IntermediateReadsWriter<T, W>>,
    buffers: Vec<Vec<u8>>,
}
impl<'a, T: SequenceExtraData, W: DataWriter> IntermediateSequencesStorage<'a, T, W> {
    const ALLOWED_LEN: usize = 65536;

    pub fn new(
        buckets_count: usize,
        buckets: &'a MultiThreadBuckets<IntermediateReadsWriter<T, W>>,
    ) -> Self {
        let mut buffers = Vec::with_capacity(buckets_count);
        for _ in 0..buckets_count {
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

impl<'a, T: SequenceExtraData, W: DataWriter> Drop for IntermediateSequencesStorage<'a, T, W> {
    fn drop(&mut self) {
        for bucket in 0..self.buffers.len() {
            if self.buffers[bucket].len() > 0 {
                self.flush_buffers(bucket as BucketIndexType);
            }
        }
    }
}

impl<T: SequenceExtraData, W: DataWriter> IntermediateReadsWriter<T, W> {
    fn create_new_block(&mut self) {
        replace_with_or_abort(&mut self.writer, |writer| {
            let (mut file_buf, res) = writer.finish();
            res.unwrap();

            let checkpoint_pos = file_buf.stream_position().unwrap();
            self.index.index.push(checkpoint_pos);

            lz4::EncoderBuilder::new().level(0).build(file_buf).unwrap()
        });
        self.chunk_size = 0;
    }
}

const MAXIMUM_CHUNK_SIZE: u64 = 1024 * 1024 * 16;

impl<T: SequenceExtraData, W: DataWriter> BucketType for IntermediateReadsWriter<T, W> {
    type InitType = Path;
    const SUPPORTS_LOCK_FREE: bool = false;

    fn new(init_data: &Path, index: usize) -> Self {
        let path = init_data.parent().unwrap().join(format!(
            "{}.{}.tmp",
            init_data.file_name().unwrap().to_str().unwrap(),
            index
        ));

        let mut file = W::create_default(&path);

        // Write empty header
        file.write_all(&IntermediateReadsHeader::default().serialize()[..]);

        let first_block_pos = file.stream_position().unwrap();
        let mut compress_stream = lz4::EncoderBuilder::new()
            .level(0)
            .checksum(ContentChecksum::NoChecksum)
            .block_mode(BlockMode::Independent)
            .block_size(BlockSize::Default)
            .build(file)
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

    fn write_data(&mut self, bytes: &[u8]) {
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

        file.overwrite_at_start(
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

    fn read_exact(&mut self, buf: &mut [u8]) -> std::io::Result<()> {
        Ok(self.read_bytes(buf)).map(|_| ())
    }
}
impl<T: SequenceExtraData, R: DataReader> IntermediateReadsReader<T, R> {
    pub fn new(name: impl AsRef<Path>, remove_file: bool) -> Self {
        let mut file = R::open_file(&name);

        let mut header_buffer = [0; IntermediateReadsHeader::SIZE];
        file.read_exact(&mut header_buffer);

        let header: IntermediateReadsHeader =
            IntermediateReadsHeader::deserialize_from(&header_buffer);
        assert_eq!(header.magic, INTERMEDIATE_READS_MAGIC);

        file.seek(SeekFrom::Start(header.index_offset)).unwrap();
        let index: IntermediateReadsIndex = bincode::deserialize_from(&mut file).unwrap();

        file.seek(SeekFrom::Start(index.index[0])).unwrap();

        Self {
            sequential_reader: SequentialReader {
                reader: lz4::Decoder::new(file).unwrap(),
                index,
                index_position: 0,
            },
            parallel_reader: FileReader::open(&name).unwrap(),
            parallel_index: AtomicU64::new(0),
            remove_file,
            file_path: name.as_ref().to_path_buf(),
            _phantom: PhantomData,
        }
    }

    fn read_single_stream(mut stream: impl Read, mut lambda: impl FnMut(T, CompressedRead)) {
        let mut read = vec![];

        while let Some(el) = T::decode(&mut stream) {
            let size = match decode_varint(|| Some(stream.read_u8().unwrap())) {
                None => break,
                Some(x) => x,
            } as usize;

            if size == 0 {
                break;
            }
            let bytes = ((size + 3) / 4);
            read.resize(max(read.len(), bytes), 0);

            stream.read_exact(&mut read[..bytes]);

            lambda(
                el,
                CompressedRead::new_from_compressed(&read[..bytes], size),
            );
        }
    }

    pub fn for_each(mut self, mut lambda: impl FnMut(T, CompressedRead)) {
        let mut vec_reader = VecReader::new(DEFAULT_BUFFER_SIZE, &mut self.sequential_reader);

        Self::read_single_stream(vec_reader, lambda);
    }

    pub fn is_finished(&self) -> bool {
        self.parallel_index.load(Ordering::Relaxed) as usize
            >= self.sequential_reader.index.index.len()
    }

    pub fn read_parallel(&self, mut lambda: impl FnMut(T, CompressedRead)) -> bool {
        let index = self.parallel_index.fetch_add(1, Ordering::Relaxed) as usize;

        if index >= self.sequential_reader.index.index.len() {
            return false;
        }

        let addr_start = self.sequential_reader.index.index[index] as usize;

        let mut reader = self.parallel_reader.clone();
        reader.seek(SeekFrom::Start(addr_start as u64));

        let mut compressed_stream = lz4::Decoder::new(reader).unwrap();
        let mut vec_reader = VecReader::new(DEFAULT_BUFFER_SIZE, &mut compressed_stream);

        Self::read_single_stream(vec_reader, lambda);

        return true;
    }
}

impl<T: SequenceExtraData, R: DataReader> Drop for IntermediateReadsReader<T, R> {
    fn drop(&mut self) {
        if self.remove_file {
            MemoryFs::remove_file(&self.file_path, self.remove_file);
        }
    }
}
