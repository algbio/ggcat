use crate::buckets::bucket_type::BucketType;
use crate::memory_fs::file::internal::MemoryFileMode;
use crate::memory_fs::file::writer::FileWriter;
use rand::{thread_rng, RngCore};
use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Write};
use std::path::PathBuf;

pub enum StorageMode {
    AppendOrCreate,
    Plain { buffer_size: usize },
    PlainUnbuffered,
    MemoryFile { mode: MemoryFileMode },
    LZ4Compression { level: u8 },
    GZIPCompression { level: u8 },
}

pub struct BinaryWriter {
    writer: Box<dyn Write>,
    path: PathBuf,
}
unsafe impl Send for BinaryWriter {}

impl BinaryWriter {
    #[inline]
    pub fn get_writer(&mut self) -> &mut dyn Write {
        self.writer.as_mut()
    }
}

impl BucketType for BinaryWriter {
    type InitType = (PathBuf, StorageMode);
    type DataType = u8;
    const SUPPORTS_LOCK_FREE: bool = false;

    fn new((name, mode): &(PathBuf, StorageMode), index: usize) -> Self {
        let path = name.parent().unwrap().join(format!(
            "{}.{}",
            name.file_name().unwrap().to_str().unwrap(),
            index
        ));

        let mut randomval = thread_rng();
        let fraction = (randomval.next_u64() as f64 / (u64::MAX as f64)) * 0.40 - 0.20;

        let mode = match *mode {
            StorageMode::AppendOrCreate => StorageMode::AppendOrCreate,
            StorageMode::Plain { buffer_size } => StorageMode::Plain {
                buffer_size: ((buffer_size as f64 * (1.0 + fraction)) as usize),
            },
            StorageMode::PlainUnbuffered => StorageMode::PlainUnbuffered,
            StorageMode::LZ4Compression { level } => StorageMode::LZ4Compression { level },
            StorageMode::GZIPCompression { level } => StorageMode::GZIPCompression { level },
            StorageMode::MemoryFile { mode } => StorageMode::MemoryFile { mode },
        };

        let writer: Box<dyn Write> = match mode {
            StorageMode::AppendOrCreate => Box::new(BufWriter::with_capacity(
                1024 * 256,
                OpenOptions::new()
                    .append(true)
                    .create(true)
                    .open(&path)
                    .unwrap(),
            )),
            StorageMode::Plain { buffer_size } => Box::new(BufWriter::with_capacity(
                buffer_size,
                File::create(&path).unwrap(),
            )),
            StorageMode::PlainUnbuffered => Box::new(File::create(&path).unwrap()),
            StorageMode::LZ4Compression { .. } => Box::new(BufWriter::with_capacity(
                1024 * 256,
                File::create(&path).unwrap(),
            )),
            StorageMode::GZIPCompression { .. } => Box::new(BufWriter::with_capacity(
                1024 * 256,
                File::create(&path).unwrap(),
            )),
            StorageMode::MemoryFile { mode } => Box::new(FileWriter::create(&path, mode)),
        };

        Self { writer, path }
    }

    fn write_batch_data(&mut self, bytes: &[u8]) {
        update_stat!("UNKNOWN_BYTES_WRITTEN", bytes.len() as f64, StatMode::Sum);
        self.writer.write_all(bytes).unwrap();
    }

    fn write_batch_data_lock_free(&self, _bytes: &[u8]) {
        todo!()
    }

    fn get_path(&self) -> PathBuf {
        self.path.clone()
    }

    fn finalize(mut self) {
        self.writer.flush().unwrap();
    }
}
