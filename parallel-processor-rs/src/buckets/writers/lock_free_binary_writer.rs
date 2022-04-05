use crate::buckets::writers::{finalize_bucket_file, initialize_bucket_file, THREADS_BUSY_WRITING};
use crate::buckets::LockFreeBucket;
use crate::memory_data_size::MemoryDataSize;
use crate::memory_fs::file::internal::MemoryFileMode;
use crate::memory_fs::file::writer::FileWriter;
use crate::utils::memory_size_to_log2;
use counter_stats::counter::AtomicCounterGuardSum;
use parking_lot::Mutex;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

pub const LOCK_FREE_BUCKET_MAGIC: &[u8; 16] = b"PLAIN_INTR_BKT_M";

pub struct LockFreeCheckpointSize(u8);
impl LockFreeCheckpointSize {
    pub const fn new_from_size(size: MemoryDataSize) -> Self {
        Self(memory_size_to_log2(size))
    }
    pub const fn new_from_log2(val: u8) -> Self {
        Self(val)
    }
}

pub struct LockFreeBinaryWriter {
    writer: FileWriter,
    checkpoint_max_size_log2: u8,
    checkpoints: Mutex<Vec<u64>>,
    file_size: AtomicU64,
}
unsafe impl Send for LockFreeBinaryWriter {}

impl LockFreeBinaryWriter {
    pub const CHECKPOINT_SIZE_UNLIMITED: LockFreeCheckpointSize =
        LockFreeCheckpointSize::new_from_log2(62);
}

impl LockFreeBucket for LockFreeBinaryWriter {
    type InitData = (MemoryFileMode, LockFreeCheckpointSize);

    fn new(
        path_prefix: &Path,
        (file_mode, checkpoint_max_size): &(MemoryFileMode, LockFreeCheckpointSize),
        index: usize,
    ) -> Self {
        let path = path_prefix.parent().unwrap().join(format!(
            "{}.{}",
            path_prefix.file_name().unwrap().to_str().unwrap(),
            index
        ));

        let mut writer = FileWriter::create(path, *file_mode);

        let first_checkpoint = initialize_bucket_file(&mut writer);

        Self {
            writer,
            checkpoint_max_size_log2: checkpoint_max_size.0,
            checkpoints: Mutex::new(vec![first_checkpoint]),
            file_size: AtomicU64::new(0),
        }
    }

    fn write_data(&self, bytes: &[u8]) {
        let stat_raii = AtomicCounterGuardSum::new(&THREADS_BUSY_WRITING, 1);

        // let _lock = self.checkpoints.lock();
        let position = self.writer.write_all_parallel(bytes, 1);

        let old_size = self
            .file_size
            .fetch_add(bytes.len() as u64, Ordering::Relaxed);
        if old_size >> self.checkpoint_max_size_log2
            != (old_size + bytes.len() as u64) >> self.checkpoint_max_size_log2
        {
            self.checkpoints.lock().push(position);
        }

        drop(stat_raii);
    }

    fn get_path(&self) -> PathBuf {
        self.writer.get_path()
    }
    fn finalize(self) {
        finalize_bucket_file(self.writer, LOCK_FREE_BUCKET_MAGIC, {
            let mut checkpoints = self.checkpoints.into_inner();
            checkpoints.sort();
            checkpoints.dedup();
            checkpoints
        });
    }
}
