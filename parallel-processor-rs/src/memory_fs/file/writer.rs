use crate::memory_fs::allocator::{AllocatedChunk, CHUNKS_ALLOCATOR};
use crate::memory_fs::file::internal::{
    FileChunk, MemoryFileInternal, MemoryFileMode, OpenMode, UnderlyingFile,
};
use parking_lot::RwLock;
use std::io::{Seek, SeekFrom, Write};
use std::ops::{Deref, DerefMut};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

pub struct FileWriter {
    path: PathBuf,
    current_buffer: RwLock<AllocatedChunk>,
    file_length: AtomicU64,
    file: Arc<MemoryFileInternal>,
}

impl FileWriter {
    /// Creates a new file with the specified mode
    pub fn create(path: impl AsRef<Path>, mode: MemoryFileMode) -> Self {
        Self {
            path: PathBuf::from(path.as_ref()),
            current_buffer: RwLock::new(
                CHUNKS_ALLOCATOR.request_chunk(chunk_usage!(TemporarySpace)),
            ),
            file_length: AtomicU64::new(0),
            file: {
                let file = MemoryFileInternal::create_new(path, mode);
                file.open(OpenMode::Write).unwrap();
                file
            },
        }
    }

    /// Returns the total length of the file (slow method)
    pub fn len(&self) -> usize {
        self.file.len() + self.current_buffer.read().len()
    }

    /// Overwrites bytes at the start of the file, the data field should not be longer than 128 bytes
    pub fn write_at_start(&mut self, data: &[u8]) -> Result<(), ()> {
        if data.len() > 128 {
            return Err(());
        }

        unsafe {
            if self.file.get_chunks_count() > 0 {
                match self.file.get_chunk(0).read().deref() {
                    FileChunk::OnDisk { .. } => {
                        if let UnderlyingFile::WriteMode { file, .. } =
                            self.file.get_underlying_file().deref()
                        {
                            let mut file_lock = file.1.lock();
                            let position = file_lock.stream_position().unwrap();
                            file_lock.seek(SeekFrom::Start(0)).unwrap();
                            file_lock.write_all(data).unwrap();
                            file_lock.seek(SeekFrom::Start(position)).unwrap();
                            Ok(())
                        } else {
                            Err(())
                        }
                    }
                    FileChunk::OnMemory { chunk } => {
                        std::ptr::copy_nonoverlapping(
                            data.as_ptr(),
                            chunk.get_mut_ptr(),
                            data.len(),
                        );
                        Ok(())
                    }
                }
            } else {
                std::ptr::copy_nonoverlapping(
                    data.as_ptr(),
                    self.current_buffer.read().get_mut_ptr(),
                    data.len(),
                );
                Ok(())
            }
        }
    }

    /// Appends atomically all the buffer to the file, returning the start position of the buffer in the file
    pub fn write_all_parallel(&self, buf: &[u8], el_size: usize) -> u64 {
        let buffer = self.current_buffer.read();
        if let Some(chunk_position) = buffer.write_bytes_noextend(buf) {
            self.file_length.load(Ordering::Relaxed) + chunk_position
        } else {
            drop(buffer);
            let mut buffer = self.current_buffer.write();

            let mut temp_vec = Vec::new();

            let position = self
                .file_length
                .fetch_add(buffer.len() as u64, Ordering::SeqCst)
                + (buffer.len() as u64);

            replace_with::replace_with_or_abort(buffer.deref_mut(), |buffer| {
                let new_buffer = self
                    .file
                    .reserve_space(buffer, &mut temp_vec, buf.len(), el_size);
                new_buffer
            });

            // Add the completely filled chunks to the file, removing the last size as it is already used
            self.file_length
                .fetch_add((buf.len() - buffer.len()) as u64, Ordering::SeqCst);

            drop(buffer);

            let mut offset = 0;
            for (_lock, part) in temp_vec.drain(..) {
                part.copy_from_slice(&buf[offset..(offset + part.len())]);
                offset += part.len();
            }

            if self.file.is_on_disk() {
                self.file.flush_chunks(usize::MAX);
            }
            position
        }
    }

    pub fn get_path(&self) -> PathBuf {
        self.path.clone()
    }

    pub fn flush_async(&self) {}
}

impl Write for FileWriter {
    #[inline(always)]
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.write_all_parallel(buf, 1);
        Ok(buf.len())
    }

    #[inline(always)]
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
