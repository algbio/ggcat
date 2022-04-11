use crate::buckets::bucket_writer::BucketItem;
use crate::buckets::readers::BucketReader;
use crate::buckets::writers::{BucketCheckpoints, BucketHeader};
use crate::memory_fs::file::reader::FileReader;
use crate::memory_fs::{MemoryFs, RemoveFileMode};
use desse::Desse;
use desse::DesseSized;
use replace_with::replace_with_or_abort;
use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;

pub trait ChunkDecoder {
    const MAGIC_HEADER: &'static [u8; 16];
    type ReaderType: Read;
    fn decode_stream(reader: FileReader, size: u64) -> Self::ReaderType;
    fn dispose_stream(stream: Self::ReaderType) -> FileReader;
}

pub struct GenericChunkedBinaryReader<D: ChunkDecoder> {
    remove_file: RemoveFileMode,
    sequential_reader: SequentialReader<D>,
    parallel_reader: FileReader,
    parallel_index: AtomicU64,
    file_path: PathBuf,
}

unsafe impl<D: ChunkDecoder> Sync for GenericChunkedBinaryReader<D> {}

struct SequentialReader<D: ChunkDecoder> {
    reader: D::ReaderType,
    index: BucketCheckpoints,
    last_byte_position: u64,
    index_position: u64,
}

impl<D: ChunkDecoder> SequentialReader<D> {
    fn get_chunk_size(
        checkpoints: &BucketCheckpoints,
        last_byte_position: u64,
        index: usize,
    ) -> u64 {
        if checkpoints.index.len() > (index + 1) as usize {
            checkpoints.index[(index + 1) as usize] - checkpoints.index[index as usize]
        } else {
            last_byte_position - checkpoints.index[index as usize]
        }
    }
}

impl<D: ChunkDecoder> Read for SequentialReader<D> {
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
                return Ok(0);
            }

            replace_with_or_abort(&mut self.reader, |reader| {
                let mut file = D::dispose_stream(reader);
                assert_eq!(
                    file.stream_position().unwrap(),
                    self.index.index[self.index_position as usize]
                );
                file.seek(SeekFrom::Start(
                    self.index.index[self.index_position as usize],
                ))
                .unwrap();
                let size = SequentialReader::<D>::get_chunk_size(
                    &self.index,
                    self.last_byte_position,
                    self.index_position as usize,
                );
                D::decode_stream(file, size)
            });
        }
    }
}

impl<D: ChunkDecoder> GenericChunkedBinaryReader<D> {
    pub fn new(
        name: impl AsRef<Path>,
        remove_file: RemoveFileMode,
        prefetch_amount: Option<usize>,
    ) -> Self {
        let mut file = FileReader::open(&name, prefetch_amount)
            .unwrap_or_else(|| panic!("Cannot open file {}", name.as_ref().display()));

        let mut header_buffer = [0; BucketHeader::SIZE];
        file.read_exact(&mut header_buffer)
            .unwrap_or_else(|_| panic!("File {} is corrupted", name.as_ref().display()));

        let header: BucketHeader = BucketHeader::deserialize_from(&header_buffer);
        assert_eq!(&header.magic, D::MAGIC_HEADER);

        file.seek(SeekFrom::Start(header.index_offset)).unwrap();
        let index: BucketCheckpoints = bincode::deserialize_from(&mut file).unwrap();

        // println!(
        //     "Index: {} for {}",
        //     index.index.len(),
        //     name.as_ref().display()
        // );

        file.seek(SeekFrom::Start(index.index[0])).unwrap();

        let size = SequentialReader::<D>::get_chunk_size(&index, header.index_offset, 0);

        Self {
            sequential_reader: SequentialReader {
                reader: D::decode_stream(file, size),
                index,
                last_byte_position: header.index_offset,
                index_position: 0,
            },
            parallel_reader: FileReader::open(&name, prefetch_amount).unwrap(),
            parallel_index: AtomicU64::new(0),
            remove_file,
            file_path: name.as_ref().to_path_buf(),
        }
    }

    pub fn get_length(&self) -> usize {
        self.parallel_reader.total_file_size()
    }

    pub fn is_finished(&self) -> bool {
        self.parallel_index.load(Ordering::Relaxed) as usize
            >= self.sequential_reader.index.index.len()
    }

    pub fn get_single_stream<'a>(&'a mut self) -> impl Read + 'a {
        &mut self.sequential_reader
    }

    pub fn get_read_parallel_stream(&self) -> Option<impl Read> {
        let index = self.parallel_index.fetch_add(1, Ordering::Relaxed) as usize;

        if index >= self.sequential_reader.index.index.len() {
            return None;
        }

        let addr_start = self.sequential_reader.index.index[index] as usize;

        let mut reader = self.parallel_reader.clone();
        reader.seek(SeekFrom::Start(addr_start as u64)).unwrap();

        let size = SequentialReader::<D>::get_chunk_size(
            &self.sequential_reader.index,
            self.sequential_reader.last_byte_position,
            index,
        );

        Some(D::decode_stream(reader, size))
    }

    pub fn decode_bucket_items_parallel<E: BucketItem, F: for<'a> FnMut(E::ReadType<'a>)>(
        &self,
        mut buffer: E::ReadBuffer,
        mut func: F,
    ) -> bool {
        let mut stream = match self.get_read_parallel_stream() {
            None => return false,
            Some(stream) => stream,
        };

        while let Some(el) = E::read_from(&mut stream, &mut buffer) {
            func(el);
        }
        return true;
    }
}

impl<D: ChunkDecoder> BucketReader for GenericChunkedBinaryReader<D> {
    fn decode_all_bucket_items<E: BucketItem, F: for<'a> FnMut(E::ReadType<'a>)>(
        mut self,
        mut buffer: E::ReadBuffer,
        mut func: F,
    ) {
        let mut stream = self.get_single_stream();

        while let Some(el) = E::read_from(&mut stream, &mut buffer) {
            func(el);
        }
    }
}

impl<D: ChunkDecoder> Drop for GenericChunkedBinaryReader<D> {
    fn drop(&mut self) {
        MemoryFs::remove_file(&self.file_path, self.remove_file).unwrap();
    }
}
