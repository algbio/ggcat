use crate::multi_thread_buckets::BucketType;
use crate::stats_logger::StatRaiiCounter;

use crate::memory_fs::file::internal::MemoryFileMode;
use crate::memory_fs::file::writer::FileWriter;
use std::io::Write;
use std::path::PathBuf;

pub struct LockFreeBinaryWriter {
    writer: FileWriter,
}
unsafe impl Send for LockFreeBinaryWriter {}

impl BucketType for LockFreeBinaryWriter {
    type InitType = (PathBuf, MemoryFileMode);
    type DataType = u8;
    const SUPPORTS_LOCK_FREE: bool = true;

    fn new((name, mode): &(PathBuf, MemoryFileMode), index: usize) -> Self {
        let path = name.parent().unwrap().join(format!(
            "{}.{}",
            name.file_name().unwrap().to_str().unwrap(),
            index
        ));

        // TODO: Maybe randomize again
        // if let MemoryMode::ChunksFileBuffer = mode {
        //     // let mut randomval = thread_rng();
        //     // let fraction = (randomval.next_u64() as f64 / (u64::MAX as f64)) * 0.20 - 0.10;
        //
        //     // mode = MemoryMode::ChunksFileBuffer {
        //     // }
        // }

        Self {
            writer: FileWriter::create(path, *mode),
        }
    }

    fn write_data(&mut self, bytes: &[u8]) {
        self.writer.write_all(bytes).unwrap();
    }

    fn write_data_lock_free(&self, bytes: &[u8]) {
        let stat_raii = StatRaiiCounter::create("THREADS_BUSY_WRITING");
        self.writer.write_all_parallel(bytes, 1);
        drop(stat_raii);
    }

    fn get_path(&self) -> PathBuf {
        self.writer.get_path()
    }

    fn finalize(self) {
        self.writer.flush_async();
    }
}
