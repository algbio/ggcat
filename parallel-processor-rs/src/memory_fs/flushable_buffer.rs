use std::fs::File;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Weak};

use parking_lot::{Mutex, RwLock};

use crate::memory_fs::buffer_manager::BUFFER_MANAGER;
use crate::memory_fs::concurrent_memory_chunk::ConcurrentMemoryChunk;
use parking_lot::lock_api::RawRwLock;

pub enum FlushMode {
    Append,
    WriteAt(usize),
}

pub struct FlushableBuffer {
    pub(crate) buffer: ConcurrentMemoryChunk,
    pub(crate) flush_pending_lock: RwLock<()>,
    pub(crate) underlying_file: Arc<Mutex<File>>,
    pub(crate) flush_mode: FlushMode,
    pub(crate) request_id: AtomicU32,
    _private: (),
}

impl Drop for FlushableBuffer {
    fn drop(&mut self) {
        BUFFER_MANAGER.notify_drop(self.buffer.capacity())
    }
}

impl FlushableBuffer {
    pub fn new_alloc(
        file: Arc<Mutex<File>>,
        size: usize,
        flush_mode: FlushMode,
    ) -> Arc<FlushableBuffer> {
        let ret = FlushableBuffer {
            buffer: ConcurrentMemoryChunk::new(size),
            flush_pending_lock: RwLock::new(()),
            underlying_file: file,
            flush_mode,
            request_id: AtomicU32::new(0),
            _private: (),
        };

        BUFFER_MANAGER.notify_alloc(ret.buffer.capacity());
        Arc::new(ret)
    }

    pub fn from_existing_memory_ready_to_flush(
        file: Arc<Mutex<File>>,
        buffer: Vec<u8>,
        flush_mode: FlushMode,
    ) -> Arc<FlushableBuffer> {
        BUFFER_MANAGER.notify_alloc(buffer.capacity());

        Arc::new(FlushableBuffer {
            buffer: ConcurrentMemoryChunk::from_memory(buffer),
            flush_pending_lock: {
                let lock = RwLock::new(());
                unsafe { lock.raw().lock_exclusive() };
                lock
            },
            underlying_file: file,
            flush_mode,
            request_id: AtomicU32::new(0),
            _private: (),
        })
    }

    pub fn wait_for_flush(&self) {
        self.flush_pending_lock.read();
    }

    pub fn get_flush_handle(self: &Arc<FlushableBuffer>) -> FlushingBufferHandle {
        FlushingBufferHandle {
            request_id: self.request_id.load(Ordering::Relaxed),
            underlying: Arc::downgrade(self),
        }
    }
}

pub struct FlushingBufferHandle {
    request_id: u32,
    underlying: Weak<FlushableBuffer>,
}

impl FlushingBufferHandle {
    pub fn wait_flush(self) {
        if let Some(buffer) = self.underlying.upgrade() {
            if self.request_id == buffer.request_id.load(Ordering::Relaxed) {
                // Wait for the buffer to be flushed
                buffer.flush_pending_lock.read();
            }
        }
    }
}
