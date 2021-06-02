use crate::memory_fs::flushable_buffer::{FlushMode, FlushableBuffer};
use parking_lot::Mutex;
use std::fs::File;
use std::sync::Arc;

pub struct BufferManager {}

impl BufferManager {
    const fn new() -> BufferManager {
        BufferManager {}
    }

    pub(crate) fn notify_drop(&self, _value: usize) {}
    pub(crate) fn notify_alloc(&self, _value: usize) {}

    pub fn request_buffer(
        &self,
        file: Arc<Mutex<File>>,
        size: usize,
        mode: FlushMode,
    ) -> Arc<FlushableBuffer> {
        FlushableBuffer::new_alloc(file, size, mode)
    }
}

pub static BUFFER_MANAGER: BufferManager = BufferManager::new();
