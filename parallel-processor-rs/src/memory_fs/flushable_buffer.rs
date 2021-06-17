use std::fs::File;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Weak};

use parking_lot::{Mutex, RwLock};

use crate::memory_data_size::*;
use crate::memory_fs::allocator::{AllocatedChunk, CHUNKS_ALLOCATOR};
use crate::memory_fs::buffer_manager::BUFFER_MANAGER;
use crate::memory_fs::FILES_FLUSH_HASH_MAP;
use parking_lot::lock_api::RawRwLock;
use std::panic::Location;
use std::path::PathBuf;

pub enum FlushMode {
    Append,
    WriteAt(usize),
}

pub struct FlushableBuffer {
    buffer: AllocatedChunk,
    pub(crate) underlying_file: Arc<(PathBuf, Mutex<File>)>,
    pub(crate) flush_mode: FlushMode,
    _private: (),
}

impl Drop for FlushableBuffer {
    fn drop(&mut self) {
        if Arc::strong_count(&self.underlying_file) == 1 {
            unsafe {
                FILES_FLUSH_HASH_MAP
                    .as_mut()
                    .unwrap()
                    .lock()
                    .entry(self.underlying_file.0.clone())
                    .or_insert(Vec::new())
                    .push(self.underlying_file.clone())
            }
        }

        BUFFER_MANAGER.notify_drop()
    }
}

impl FlushableBuffer {
    #[track_caller]
    pub fn new_alloc(file: Arc<(PathBuf, Mutex<File>)>, flush_mode: FlushMode) -> FlushableBuffer {
        let ret = FlushableBuffer {
            buffer: CHUNKS_ALLOCATOR.request_chunk(chunk_usage!(FileBuffer {
                path: Location::caller().to_string() //String::from("Flushable buffer")
            })),
            underlying_file: file,
            flush_mode,
            _private: (),
        };

        BUFFER_MANAGER.notify_alloc();
        ret
    }

    #[inline(always)]
    pub fn write_bytes_noextend(&self, data: &[u8]) -> bool {
        self.buffer.write_bytes_noextend(data)
    }

    #[inline(always)]
    pub fn has_space_for(&self, data_len: usize) -> bool {
        self.buffer.has_space_for(data_len)
    }

    pub fn from_existing_memory_ready_to_flush(
        file: Arc<(PathBuf, Mutex<File>)>,
        buffer: AllocatedChunk,
        flush_mode: FlushMode,
    ) -> FlushableBuffer {
        BUFFER_MANAGER.notify_alloc();

        FlushableBuffer {
            buffer,
            underlying_file: file,
            flush_mode,
            _private: (),
        }
    }

    #[inline(always)]
    pub fn get_buffer(&self) -> &[u8] {
        self.buffer.get()
    }

    #[inline(always)]
    pub fn clear_buffer(&self) {
        self.buffer.clear();
    }
}
