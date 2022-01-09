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
