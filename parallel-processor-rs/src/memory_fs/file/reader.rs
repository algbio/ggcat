use crate::memory_fs::file::internal::{FileChunk, MemoryFileInternal, OpenMode};
use parking_lot::lock_api::ArcRwLockReadGuard;
use parking_lot::RawRwLock;
use std::cmp::min;
use std::io;
use std::io::{ErrorKind, Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::sync::Arc;

pub struct FileReader {
    path: PathBuf,
    file: Arc<MemoryFileInternal>,
    current_chunk_ref: Option<ArcRwLockReadGuard<RawRwLock, FileChunk>>,
    current_chunk_index: usize,
    chunks_count: usize,
    current_ptr: *const u8,
    current_len: usize,
}

unsafe impl Sync for FileReader {}
unsafe impl Send for FileReader {}

impl FileReader {
    fn set_chunk_info(&mut self, index: usize) {
        let chunk = self.file.get_chunk(index);
        let chunk_guard = chunk.read_arc();

        let underlying_file = self.file.get_underlying_file();

        self.current_ptr = chunk_guard.get_ptr(&underlying_file);
        self.current_len = chunk_guard.get_length();
        self.current_chunk_ref = Some(chunk_guard);
    }

    pub fn open(path: impl AsRef<Path>) -> Option<Self> {
        let file = MemoryFileInternal::retrieve_reference(&path)?;

        file.open(OpenMode::Read).unwrap();

        let chunks_count = file.get_chunks_count();

        let mut reader = Self {
            path: path.as_ref().into(),
            file,
            current_chunk_ref: None,
            current_chunk_index: 0,
            chunks_count,
            current_ptr: std::ptr::null(),
            current_len: 0,
        };

        if reader.chunks_count > 0 {
            reader.set_chunk_info(0);
        }

        Some(reader)
    }

    // pub fn get_typed_chunks_mut<T>(&mut self) -> Option<impl Iterator<Item = &mut [T]>> {
    //     todo!();
    //     Some((0..1).into_iter().map(|_| &mut [0, 1][..]))
    // }
}

impl Read for FileReader {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let mut bytes_written = 0;

        while bytes_written != buf.len() {
            if self.current_len == 0 {
                self.current_chunk_index += 1;
                if self.current_chunk_index >= self.chunks_count {
                    // End of file
                    return Ok(bytes_written);
                }
                self.set_chunk_info(self.current_chunk_index);
            }

            let copyable_bytes = min(buf.len() - bytes_written, self.current_len);

            unsafe {
                std::ptr::copy_nonoverlapping(
                    self.current_ptr,
                    buf.as_mut_ptr().add(bytes_written),
                    copyable_bytes,
                );
                self.current_ptr = self.current_ptr.add(copyable_bytes);
                self.current_len -= copyable_bytes;
                bytes_written += copyable_bytes;
            }
        }

        Ok(bytes_written)
    }

    #[inline(always)]
    fn read_exact(&mut self, buf: &mut [u8]) -> std::io::Result<()> {
        match self.read(buf) {
            Ok(count) => {
                if count == buf.len() {
                    Ok(())
                } else {
                    Err(io::Error::new(
                        io::ErrorKind::Other,
                        "Unexpected error while reading",
                    ))
                }
            }
            Err(err) => Err(err),
        }
    }
}

impl Seek for FileReader {
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        match pos {
            SeekFrom::Start(mut offset) => {
                let mut chunk_idx = 0;

                while chunk_idx < self.chunks_count {
                    let len = self.file.get_chunk(chunk_idx).read().get_length();
                    if offset < (len as u64) {
                        break;
                    }
                    chunk_idx += 1;
                    offset -= len as u64;
                }

                if chunk_idx == self.chunks_count {
                    return Err(std::io::Error::new(
                        ErrorKind::UnexpectedEof,
                        "Unexpected eof",
                    ));
                }

                self.current_chunk_index = chunk_idx;
                self.set_chunk_info(chunk_idx);
                unsafe {
                    self.current_ptr = self.current_ptr.add(offset as usize);
                    self.current_len -= offset as usize;
                }

                return Ok(offset);
            }
            _ => {
                unimplemented!()
            }
        }
    }
}
