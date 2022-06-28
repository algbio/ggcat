use std::fs::File;

use crate::memory_fs::allocator::AllocatedChunk;
use crate::memory_fs::file::internal::FileChunk;
use parking_lot::lock_api::ArcRwLockWriteGuard;
use parking_lot::{Mutex, RawRwLock};
use std::path::PathBuf;
use std::sync::Arc;

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
