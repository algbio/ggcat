use std::fs::File;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Weak};

use parking_lot::{Mutex, RawRwLock, RwLock};

use crate::memory_data_size::*;
use crate::memory_fs::allocator::{AllocatedChunk, CHUNKS_ALLOCATOR};
use crate::memory_fs::file::internal::{FileChunk, MemoryFileInternal};
use crate::memory_fs::FILES_FLUSH_HASH_MAP;
use parking_lot::lock_api::ArcRwLockWriteGuard;
use std::panic::Location;
use std::path::PathBuf;

pub struct FlushableItem {
    pub underlying_file: Arc<(PathBuf, Mutex<File>)>,
    pub mode: FileFlushMode,
}

pub enum FileFlushMode {
    Append {
        chunk: ArcRwLockWriteGuard<RawRwLock, FileChunk>,
    },
    WriteAt {
        buffer: AllocatedChunk,
        offset: u64,
    },
}

// impl Drop for FlushableItem {
//     fn drop(&mut self) {
//         if Arc::strong_count(&self.underlying_file) == 1 {
//             unsafe {
//                 FILES_FLUSH_HASH_MAP
//                     .as_mut()
//                     .unwrap()
//                     .lock()
//                     .entry(self.underlying_file.0.clone())
//                     .or_insert(Vec::new())
//                     .push(self.underlying_file.clone())
//             }
//         }
//
//         BUFFER_MANAGER.notify_drop()
//     }
// }
