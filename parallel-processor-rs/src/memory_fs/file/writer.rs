use crate::memory_fs::allocator::{AllocatedChunk, CHUNKS_ALLOCATOR};
use crate::memory_fs::file::internal::{MemoryFileInternal, MemoryFileMode, OpenMode};
use parking_lot::{Mutex, RwLock};
use std::cmp::min;
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::ops::DerefMut;
use std::path::{Path, PathBuf};
use std::sync::Arc;

pub struct FileWriter {
    path: PathBuf,
    current_buffer: RwLock<AllocatedChunk>,
    file: Arc<MemoryFileInternal>,
}

impl FileWriter {
    pub fn create(path: impl AsRef<Path>, mode: MemoryFileMode) -> Self {
        Self {
            path: PathBuf::from(path.as_ref()),
            current_buffer: RwLock::new(
                CHUNKS_ALLOCATOR.request_chunk(chunk_usage!(TemporarySpace)),
            ),
            file: {
                let file = MemoryFileInternal::create_new(path, mode);
                file.open(OpenMode::Write).unwrap();
                file
            },
        }
    }

    pub fn write_all_parallel(&self, buf: &[u8], el_size: usize) {
        let buffer = self.current_buffer.read();
        if buffer.write_bytes_noextend(buf) {
            return;
        } else {
            drop(buffer);
            let mut buffer = self.current_buffer.write();

            let mut temp_vec = Vec::new();

            replace_with::replace_with_or_abort(buffer.deref_mut(), |buffer| {
                let new_buffer = self
                    .file
                    .reserve_space(buffer, &mut temp_vec, buf.len(), el_size);
                new_buffer
            });

            let mut offset = 0;
            for (_lock, part) in temp_vec.drain(..) {
                part.copy_from_slice(&buf[offset..(offset + part.len())]);
                offset += part.len();
            }

            if self.file.is_on_disk() {
                self.file.flush_chunks(usize::MAX);
            }
        }
    }

    pub fn get_path(&self) -> PathBuf {
        self.path.clone()
    }

    pub fn flush_async(&self) {}
}

impl Write for FileWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.write_all_parallel(buf, 1);
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl Drop for FileWriter {
    fn drop(&mut self) {
        let mut current_buffer = self.current_buffer.write();
        if current_buffer.len() > 0 {
            self.file.add_chunk(std::mem::replace(
                current_buffer.deref_mut(),
                AllocatedChunk::INVALID,
            ));
            if self.file.is_on_disk() {
                self.file.flush_chunks(usize::MAX);
            }
        }
        self.file.close();
    }
}
