use crate::config::DEFAULT_OUTPUT_BUFFER_SIZE;
use crate::io::concurrent::temp_reads::extra_data::SequenceExtraData;
use crate::io::concurrent::temp_reads::{
    IntermediateReadsHeader, IntermediateReadsIndex, INTERMEDIATE_READS_MAGIC,
};
use crate::io::varint::decode_varint_flags;
use crate::utils::vec_reader::VecReader;
use crate::CompressedRead;
use byteorder::ReadBytesExt;
use desse::Desse;
use desse::DesseSized;
use parallel_processor::memory_fs::file::reader::FileReader;
use parallel_processor::memory_fs::{MemoryFs, RemoveFileMode};
use replace_with::replace_with_or_abort;
use std::cmp::max;
use std::io::{Read, Seek, SeekFrom};
use std::marker::PhantomData;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

pub struct IntermediateReadsReader<T: SequenceExtraData> {
    remove_file: RemoveFileMode,
    sequential_reader: SequentialReader,
    parallel_reader: FileReader,
    parallel_index: AtomicU64,
    file_path: PathBuf,
    _phantom: PhantomData<T>,
}

unsafe impl<T: SequenceExtraData> Sync for IntermediateReadsReader<T> {}

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

impl<T: SequenceExtraData> IntermediateReadsReader<T> {
    pub fn new(name: impl AsRef<Path>, remove_file: RemoveFileMode) -> Self {
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
