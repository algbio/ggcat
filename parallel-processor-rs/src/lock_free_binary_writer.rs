use crate::memory_fs::{MemoryFile, MemoryMode};
use crate::multi_thread_buckets::BucketType;
use crate::stats_logger::{StatMode, StatRaiiCounter, DEFAULT_STATS_LOGGER};
use crate::Utils;

use rand::{thread_rng, RngCore};

use std::io::Write;
use std::path::PathBuf;
use std::sync::Arc;

pub struct LockFreeBinaryWriter {
    writer: Arc<MemoryFile>,
}
unsafe impl Send for LockFreeBinaryWriter {}

impl BucketType for LockFreeBinaryWriter {
    type InitType = (PathBuf, MemoryMode);
    const SUPPORTS_LOCK_FREE: bool = true;

    fn new((name, mut mode): &(PathBuf, MemoryMode), index: usize) -> Self {
        let path = name.parent().unwrap().join(format!(
            "{}.{}",
            name.file_name().unwrap().to_str().unwrap(),
            index
        ));

        if let MemoryMode::ChunksFileBuffer { buffer_size } = mode {
            let mut randomval = thread_rng();
            let fraction = (randomval.next_u64() as f64 / (u64::MAX as f64)) * 0.20 - 0.10;

            mode = MemoryMode::ChunksFileBuffer {
                buffer_size: Utils::multiply_by(buffer_size, 1.0 + fraction),
            }
        }

        Self {
            writer: MemoryFile::create(path, mode),
        }
    }

    fn write_bytes(&mut self, bytes: &[u8]) {
        self.writer.write_all(bytes);
    }

    fn write_bytes_lock_free(&self, bytes: &[u8]) {
        let stat_raii = StatRaiiCounter::create("THREADS_BUSY_WRITING");
        self.writer.write_all(bytes);
        drop(stat_raii);
    }

    fn get_path(&self) -> PathBuf {
        self.writer.get_path().into()
    }

    fn finalize(self) {
        self.writer.flush();
    }
}
