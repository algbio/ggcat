use crate::DEFAULT_BUFFER_SIZE;
use parallel_processor::memory_fs::file::internal::MemoryFileMode;
use parallel_processor::memory_fs::file::reader::FileReader;
use parallel_processor::memory_fs::file::writer::FileWriter;
use std::fs::File;
use std::io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::Path;

pub mod chunks_writer;
pub mod concurrent;
pub mod lines_reader;
pub mod reads_reader;
pub mod reads_writer;
pub mod sequences_reader;
pub mod structs;
pub mod varint;

pub trait DataWriter: Write + Send + Sync + 'static {
    fn create_default(path: impl AsRef<Path>) -> Self;
    fn write_all_at(&mut self, position: u64, data: &[u8]) -> Result<(), ()>;
    fn stream_position(&mut self) -> std::io::Result<u64>;
}
pub trait DataReader: Read + Seek + Send + Sync + 'static {
    fn open_file(path: impl AsRef<Path>) -> Self;
}

pub type FileOnlyDataReader = BufReader<File>;
pub type FileOnlyDataWriter = BufWriter<File>;

pub type MemoryFsDataReader = FileReader;
pub type MemoryFsDataWriter = FileWriter;

impl DataReader for FileOnlyDataReader {
    fn open_file(path: impl AsRef<Path>) -> Self {
        BufReader::with_capacity(
            DEFAULT_BUFFER_SIZE,
            File::open(&path)
                .unwrap_or_else(|_| panic!("Cannot open file {}", path.as_ref().display())),
        )
    }
}
impl DataReader for MemoryFsDataReader {
    fn open_file(path: impl AsRef<Path>) -> Self {
        todo!()
    }
}

impl DataWriter for FileOnlyDataWriter {
    fn create_default(path: impl AsRef<Path>) -> Self {
        BufWriter::with_capacity(
            DEFAULT_BUFFER_SIZE,
            File::create(&path)
                .expect(&format!("Failed to open file: {}", path.as_ref().display())),
        )
    }

    fn write_all_at(&mut self, position: u64, data: &[u8]) -> Result<(), ()> {
        let orig_position = <Self as Seek>::stream_position(self).map_err(|_| ())?;
        self.seek(SeekFrom::Start(position)).map_err(|_| ())?;
        self.write_all(&data).map_err(|_| ())?;

        self.seek(SeekFrom::Start(orig_position)).map_err(|_| ())?;
        Ok(())
    }

    fn stream_position(&mut self) -> std::io::Result<u64> {
        <Self as Seek>::stream_position(self)
    }
}

impl DataWriter for MemoryFsDataWriter {
    fn create_default(path: impl AsRef<Path>) -> Self {
        FileWriter::create(path, MemoryFileMode::PreferMemory)
    }

    fn write_all_at(&mut self, position: u64, data: &[u8]) -> Result<(), ()> {
        todo!()
    }

    fn stream_position(&mut self) -> std::io::Result<u64> {
        todo!()
    }
}
