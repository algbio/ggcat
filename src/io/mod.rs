use crate::config::{SwapPriority, DEFAULT_OUTPUT_BUFFER_SIZE};
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
    /// Overwrites bytes at the start of the file, the data field should not be longer than 128 bytes
    fn overwrite_at_start(&mut self, data: &[u8]) -> Result<(), ()>;
    fn stream_position(&mut self) -> std::io::Result<u64>;
}
pub trait DataReader: Read + Seek + Send + Sync + 'static {
    fn open_file(path: impl AsRef<Path>) -> Self;
}

pub type MemoryFsDataReader = FileReader;
pub type MemoryFsDataWriter = FileWriter;

impl DataReader for MemoryFsDataReader {
    fn open_file(path: impl AsRef<Path>) -> Self {
        FileReader::open(&path)
            .unwrap_or_else(|| panic!("Cannot open file {}", path.as_ref().display()))
    }
}

impl DataWriter for MemoryFsDataWriter {
    fn create_default(path: impl AsRef<Path>) -> Self {
        FileWriter::create(
            path,
            MemoryFileMode::PreferMemory {
                swap_priority: SwapPriority::Default,
            },
        )
    }

    fn overwrite_at_start(&mut self, data: &[u8]) -> Result<(), ()> {
        self.write_at_start(data)
    }

    fn stream_position(&mut self) -> std::io::Result<u64> {
        Ok(self.len() as u64)
    }
}
