use crate::config::{BucketIndexType, DEFAULT_OUTPUT_BUFFER_SIZE};
use crate::io::varint::decode_varint_flags;
use crate::utils::compressed_read::CompressedRead;
use crate::utils::get_memory_mode;
use byteorder::ReadBytesExt;
use desse::{Desse, DesseSized};
use lz4::{BlockMode, BlockSize, ContentChecksum};
use parallel_processor::memory_fs::file::reader::FileReader;
use parallel_processor::memory_fs::file::writer::FileWriter;
use parallel_processor::memory_fs::MemoryFs;
use parallel_processor::multi_thread_buckets::{BucketType, MultiThreadBuckets};
use replace_with::replace_with_or_abort;
use serde::{Deserialize, Serialize};
use std::cmp::{max, min};
use std::fmt::Debug;
use std::io::{Cursor, Read, Seek, SeekFrom, Write};
use std::marker::PhantomData;
use std::path::{Path, PathBuf};
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
        let mut cursor = Cursor::new(slice);
        Self::decode(&mut cursor)
    }

    unsafe fn decode_from_pointer(ptr: *const u8) -> Option<Self> {
        let mut stream = PointerDecoder { ptr };
        Self::decode(&mut stream)
    }

    fn decode<'a>(reader: &'a mut impl Read) -> Option<Self>;
    fn encode<'a>(&self, writer: &'a mut impl Write);

    fn max_size(&self) -> usize;
}

impl SequenceExtraData for () {
    #[inline(always)]
    fn decode<'a>(_reader: &'a mut impl Read) -> Option<Self> {
        Some(())
    }

    #[inline(always)]
    fn encode<'a>(&self, _writer: &'a mut impl Write) {}

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

pub struct IntermediateReadsWriter<T> {
    writer: lz4::Encoder<FileWriter>,
    chunk_size: u64,
    index: IntermediateReadsIndex,
    path: PathBuf,
    _phantom: PhantomData<T>,
}

struct SequentialReader {
    reader: lz4::Decoder<FileReader>,
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

pub struct IntermediateReadsReader<T: SequenceExtraData> {
    remove_file: bool,
    sequential_reader: SequentialReader,
    parallel_reader: FileReader,
    parallel_index: AtomicU64,
    file_path: PathBuf,
    _phantom: PhantomData<T>,
}

unsafe impl<T: SequenceExtraData> Sync for IntermediateReadsReader<T> {}

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

    #[allow(non_camel_case_types)]
    pub fn add_read<FLAGS_COUNT: typenum::Unsigned>(
        &mut self,
        el: T,
        seq: &[u8],
        bucket: BucketIndexType,
        flags: u8,
    ) {
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
        CompressedRead::from_plain_write_directly_to_buffer_with_flags::<FLAGS_COUNT>(
            seq,
            &mut self.buffers[bucket as usize],
            flags,
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
            let (file_buf, res) = writer.finish();
            res.unwrap();

            let checkpoint_pos = file_buf.len();
            self.index.index.push(checkpoint_pos as u64);

            lz4::EncoderBuilder::new().level(0).build(file_buf).unwrap()
        });
        self.chunk_size = 0;
    }
}

const MAXIMUM_CHUNK_SIZE: u64 = 1024 * 1024 * 16;

impl<T: SequenceExtraData> BucketType for IntermediateReadsWriter<T> {
    type InitType = (usize, PathBuf);
    type DataType = u8;
    const SUPPORTS_LOCK_FREE: bool = false;

    fn new((swap_priority, base_path): &(usize, PathBuf), index: usize) -> Self {
        let path = base_path.parent().unwrap().join(format!(
            "{}.{}.tmp",
            base_path.file_name().unwrap().to_str().unwrap(),
            index
        ));

        let mut file = FileWriter::create(&path, get_memory_mode(*swap_priority));

        // Write empty header
        file.write_all(&IntermediateReadsHeader::default().serialize()[..])
            .unwrap();

        let first_block_pos = file.len() as u64;
        let compress_stream = lz4::EncoderBuilder::new()
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
        self.writer.flush().unwrap();
        let (mut file, res) = self.writer.finish();
        res.unwrap();

        file.flush().unwrap();
        let index_position = file.len() as u64;
        bincode::serialize_into(&mut file, &self.index).unwrap();

        file.write_at_start(
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

    #[inline]
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
impl<T: SequenceExtraData> IntermediateReadsReader<T> {
    pub fn new(name: impl AsRef<Path>, remove_file: bool) -> Self {
        let mut file = FileReader::open(&name)
            .unwrap_or_else(|| panic!("Cannot open file {}", name.as_ref().display()));

        let mut header_buffer = [0; IntermediateReadsHeader::SIZE];
        file.read_exact(&mut header_buffer)
            .unwrap_or_else(|_| panic!("File {} is corrupted", name.as_ref().display()));

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

    #[allow(non_camel_case_types)]
    fn read_single_stream<
        S: Read,
        F: FnMut(u8, T, CompressedRead),
        FLAGS_COUNT: typenum::Unsigned,
    >(
        mut stream: S,
        mut lambda: F,
    ) {
        let mut read = vec![];

        while let Some(el) = T::decode(&mut stream) {
            let (size, flags) =
                match decode_varint_flags::<_, FLAGS_COUNT>(|| Some(stream.read_u8().unwrap())) {
                    None => break,
                    Some(x) => (x.0 as usize, x.1),
                };

            if size == 0 {
                break;
            }
            let bytes = (size + 3) / 4;
            read.resize(max(read.len(), bytes), 0);

            stream.read_exact(&mut read[..bytes]).unwrap();

            lambda(
                flags,
                el,
                CompressedRead::new_from_compressed(&read[..bytes], size),
            );
        }
    }

    #[allow(non_camel_case_types)]
    pub fn for_each<F: FnMut(u8, T, CompressedRead), FLAGS_COUNT: typenum::Unsigned>(
        mut self,
        lambda: F,
    ) {
        let vec_reader = VecReader::new(DEFAULT_OUTPUT_BUFFER_SIZE, &mut self.sequential_reader);

        Self::read_single_stream::<_, _, FLAGS_COUNT>(vec_reader, lambda);
    }

    pub fn is_finished(&self) -> bool {
        self.parallel_index.load(Ordering::Relaxed) as usize
            >= self.sequential_reader.index.index.len()
    }

    #[allow(non_camel_case_types)]
    pub fn read_parallel<F: FnMut(u8, T, CompressedRead), FLAGS_COUNT: typenum::Unsigned>(
        &self,
        lambda: F,
    ) -> bool {
        let index = self.parallel_index.fetch_add(1, Ordering::Relaxed) as usize;

        if index >= self.sequential_reader.index.index.len() {
            return false;
        }

        let addr_start = self.sequential_reader.index.index[index] as usize;

        let mut reader = self.parallel_reader.clone();
        reader.seek(SeekFrom::Start(addr_start as u64)).unwrap();

        let mut compressed_stream = lz4::Decoder::new(reader).unwrap();
        let vec_reader = VecReader::new(DEFAULT_OUTPUT_BUFFER_SIZE, &mut compressed_stream);

        Self::read_single_stream::<_, _, FLAGS_COUNT>(vec_reader, lambda);

        return true;
    }
}

impl<T: SequenceExtraData> Drop for IntermediateReadsReader<T> {
    fn drop(&mut self) {
        MemoryFs::remove_file(&self.file_path, self.remove_file).unwrap();
    }
}
