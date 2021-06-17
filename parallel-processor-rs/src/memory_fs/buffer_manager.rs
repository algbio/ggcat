use crate::memory_data_size::{MemoryDataSize, MemoryDataSizeExt};
use crate::memory_fs::allocator::ALLOCATED_CHUNK_USABLE_SIZE;
use crate::memory_fs::flushable_buffer::{FlushMode, FlushableBuffer};
use crate::stats_logger::StatMode;
use measurements::Measurement;
use parking_lot::Mutex;
use std::fs::File;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

pub struct BufferManager {
    allocated_buffers: AtomicUsize,
}

impl BufferManager {
    const fn new() -> BufferManager {
        BufferManager {
            allocated_buffers: AtomicUsize::new(0),
        }
    }

    pub fn update_buffer_stats(&self) {
        let buffers_count = self.allocated_buffers.load(Ordering::Relaxed);

        update_stat!(
            "BUFFERS_TOTAL_BYTES",
            (buffers_count * ALLOCATED_CHUNK_USABLE_SIZE) as f64,
            StatMode::Replace
        );
        update_stat!(
            "BUFFERS_TOTAL_COUNT",
            buffers_count as f64,
            StatMode::Replace
        );
    }

    pub(crate) fn notify_drop(&self) {
        self.allocated_buffers.fetch_sub(1, Ordering::Relaxed);
    }

    pub(crate) fn notify_alloc(&self) {
        self.allocated_buffers.fetch_add(1, Ordering::Relaxed);
    }

    #[track_caller]
    pub fn request_buffer(
        &self,
        file: Arc<(PathBuf, Mutex<File>)>,
        mode: FlushMode,
    ) -> FlushableBuffer {
        FlushableBuffer::new_alloc(file, mode)
    }
}

pub static BUFFER_MANAGER: BufferManager = BufferManager::new();
