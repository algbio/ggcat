use crate::buckets::writers::{finalize_bucket_file, initialize_bucket_file, THREADS_BUSY_WRITING};
use crate::buckets::LockFreeBucket;
use crate::memory_fs::file::internal::MemoryFileMode;
use crate::memory_fs::file::writer::FileWriter;
use counter_stats::counter::AtomicCounterGuardSum;
use parking_lot::Mutex;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

pub const LOCK_FREE_BUCKET_MAGIC: &[u8; 16] = b"PLAIN_INTR_BKT_M";

pub struct LockFreeBinaryWriter {
    writer: FileWriter,
    checkpoint_max_size: u64,
    checkpoints: Mutex<Vec<u64>>,
    last_checkpoint: AtomicU64,
}
unsafe impl Send for LockFreeBinaryWriter {}

impl LockFreeBinaryWriter {
    pub const CHECKPOINT_SIZE_UNLIMITED: u64 = u64::MAX / 2;
}

impl LockFreeBucket for LockFreeBinaryWriter {
    type InitData = (MemoryFileMode, u64);

    fn new(
        path_prefix: &Path,
        (file_mode, checkpoint_max_size): &(MemoryFileMode, u64),
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
            checkpoint_max_size: *checkpoint_max_size,
            checkpoints: Mutex::new(vec![first_checkpoint]),
            last_checkpoint: AtomicU64::new(first_checkpoint),
        }
    }

    fn write_data(&self, bytes: &[u8]) {
        let stat_raii = AtomicCounterGuardSum::new(&THREADS_BUSY_WRITING, 1);

        let position = self.writer.write_all_parallel(bytes, 1);

        self.last_checkpoint.fetch_update(
            Ordering::Relaxed,
            Ordering::Relaxed,
            |last_checkpoint| {
                if last_checkpoint + self.checkpoint_max_size < position + (bytes.len() as u64) {
                    self.checkpoints.lock().push(position);
                    Some(position)
                } else {
                    None
                }
            },
        );

        drop(stat_raii);
    }

    fn get_path(&self) -> PathBuf {
        self.writer.get_path()
    }
    fn finalize(self) {
        finalize_bucket_file(
            self.writer,
            LOCK_FREE_BUCKET_MAGIC,
            self.checkpoints.into_inner(),
        );
    }
}
