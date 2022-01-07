use crate::memory_fs::allocator::{AllocatedChunk, CHUNKS_ALLOCATOR};
use crate::memory_fs::file::flush::GlobalFlush;
use crate::memory_fs::flushable_buffer::{FileFlushMode, FlushableItem};
use crate::stats_logger::StatRaiiCounter;
use crossbeam::queue::SegQueue;
use dashmap::DashMap;
use filebuffer::FileBuffer;
use lazy_static::lazy_static;
use parking_lot::lock_api::{ArcRwLockReadGuard, ArcRwLockWriteGuard};
use parking_lot::{Mutex, RawRwLock, RwLock, RwLockReadGuard, RwLockWriteGuard};
use std::cmp::min;
use std::fs::{remove_file, File, OpenOptions};
use std::hint::unreachable_unchecked;
use std::io::Write;
use std::mem::size_of;
use std::ops::{Deref, DerefMut};
use std::path::{Path, PathBuf};
use std::slice::from_raw_parts_mut;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Weak};

lazy_static! {
    static ref MEMORY_MAPPED_FILES: DashMap<PathBuf, Arc<MemoryFileInternal>> = DashMap::new();
}

pub static SWAPPABLE_FILES: SegQueue<Weak<MemoryFileInternal>> = SegQueue::new();

#[derive(Copy, Clone, Eq, PartialEq)]
pub enum MemoryFileMode {
    AlwaysMemory,
    PreferMemory,
    DiskOnly,
}

#[derive(Copy, Clone, Eq, PartialEq)]
pub enum OpenMode {
    None,
    Read,
    Write,
}

struct RwLockIterator<'a, A, B, I: Iterator<Item = B>> {
    lock: RwLockReadGuard<'a, A>,
    iterator: I,
}

impl<'a, A, B, I: Iterator<Item = B>> Iterator for RwLockIterator<'a, A, B, I> {
    type Item = B;

    fn next(&mut self) -> Option<Self::Item> {
        self.iterator.next()
    }
}

pub struct MemoryFileChunksIterator<'a, I: Iterator<Item = &'a mut [u8]>> {
    iter: I,
}

impl<'a, I: Iterator<Item = &'a mut [u8]>> Iterator for MemoryFileChunksIterator<'a, I> {
    type Item = &'a mut [u8];

    #[inline(always)]
    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next()
    }
}

pub enum FileChunk {
    OnDisk { offset: u64, len: usize },
    OnMemory { chunk: AllocatedChunk },
}

impl FileChunk {
    pub fn get_length(&self) -> usize {
        match self {
            FileChunk::OnDisk { len, .. } => *len,
            FileChunk::OnMemory { chunk } => chunk.len(),
        }
    }

    pub fn get_ptr(&self, file: &UnderlyingFile) -> *const u8 {
        unsafe {
            match self {
                FileChunk::OnDisk { offset, .. } => {
                    if let UnderlyingFile::ReadMode(file) = file {
                        file.as_ref().unwrap().as_ptr().add(*offset as usize)
                    } else {
                        panic!("Error, wrong underlying file!");
                    }
                }
                FileChunk::OnMemory { chunk } => chunk.get_mut_ptr() as *const u8,
            }
        }
    }
}

pub enum UnderlyingFile {
    NotOpened,
    MemoryOnly,
    MemoryPreferred,
    WriteMode {
        file: Arc<(PathBuf, Mutex<File>)>,
        chunk_position: usize,
    },
    ReadMode(Option<FileBuffer>),
}

pub struct MemoryFileInternal {
    /// Path associated with the current file
    path: PathBuf,
    /// Disk read/write structure
    file: RwLock<UnderlyingFile>,
    /// Memory mode
    memory_mode: RwLock<MemoryFileMode>,
    /// Read or write mode
    open_mode: Mutex<(OpenMode, usize)>,
    /// Actual memory mapping
    memory: RwLock<Vec<Arc<RwLock<FileChunk>>>>,
}

impl MemoryFileInternal {
    pub fn create_new(path: impl AsRef<Path>, mode: MemoryFileMode) -> Arc<Self> {
        // Remove the file if it existed from a previous run
        remove_file(&path);

        let new_file = Arc::new(Self {
            path: path.as_ref().into(),
            file: RwLock::new(UnderlyingFile::NotOpened),
            memory_mode: RwLock::new(mode),
            open_mode: Mutex::new((OpenMode::None, 0)),
            memory: RwLock::new(Vec::new()),
        });

        MEMORY_MAPPED_FILES.insert(path.as_ref().into(), new_file.clone());

        if mode == MemoryFileMode::PreferMemory {
            SWAPPABLE_FILES.push(Arc::downgrade(&new_file))
        }

        new_file
    }

    pub fn create_from_fs(path: impl AsRef<Path>) -> Option<Arc<Self>> {
        if !path.as_ref().exists() || !path.as_ref().is_file() {
            return None;
        }
        let len = path.as_ref().metadata().ok()?.len() as usize;

        let new_file = Arc::new(Self {
            path: path.as_ref().into(),
            file: RwLock::new(UnderlyingFile::NotOpened),
            memory_mode: RwLock::new(MemoryFileMode::DiskOnly),
            open_mode: Mutex::new((OpenMode::None, 0)),
            memory: RwLock::new(vec![Arc::new(RwLock::new(FileChunk::OnDisk {
                offset: 0,
                len,
            }))]),
        });

        MEMORY_MAPPED_FILES.insert(path.as_ref().into(), new_file.clone());

        Some(new_file)
    }

    pub fn is_on_disk(&self) -> bool {
        *self.memory_mode.read() == MemoryFileMode::DiskOnly
    }

    pub fn is_memory_preferred(&self) -> bool {
        *self.memory_mode.read() == MemoryFileMode::PreferMemory
    }

    pub fn get_chunk(&self, index: usize) -> Arc<RwLock<FileChunk>> {
        self.memory.read()[index].clone()
    }

    pub fn get_chunks_count(&self) -> usize {
        self.memory.read().len()
    }

    pub fn retrieve_reference(path: impl AsRef<Path>) -> Option<Arc<Self>> {
        MEMORY_MAPPED_FILES.get(path.as_ref()).map(|f| f.clone())
    }

    pub fn active_files_count() -> usize {
        MEMORY_MAPPED_FILES.len()
    }

    pub fn delete(path: impl AsRef<Path>, remove_fs: bool) -> bool {
        if let Some(file) = MEMORY_MAPPED_FILES.remove(path.as_ref()) {
            if remove_fs {
                if let MemoryFileMode::DiskOnly = *file.1.memory_mode.read() {
                    remove_file(path);
                }
            }
            true
        } else {
            false
        }
    }

    fn create_writing_underlying_file(&self) -> UnderlyingFile {
        UnderlyingFile::WriteMode {
            file: Arc::new((
                self.path.clone(),
                Mutex::new(
                    OpenOptions::new()
                        .create(true)
                        .write(true)
                        .append(false)
                        // .custom_flags(O_DIRECT)
                        .open(&self.path)
                        .unwrap(),
                ),
            )),
            chunk_position: 0,
        }
    }

    pub fn open(&self, mode: OpenMode) -> Result<(), String> {
        if let Some(mut lock) = self.open_mode.try_lock() {
            if lock.0 == mode {
                lock.1 += 1;
                return Ok(());
            }

            if lock.0 != OpenMode::None {
                return Err(format!("File {} is already opened!", self.path.display()));
            }

            *self.file.write() = match mode {
                OpenMode::None => UnderlyingFile::NotOpened,
                OpenMode::Read => {
                    lock.1 = 1;
                    if *self.memory_mode.read() != MemoryFileMode::DiskOnly {
                        UnderlyingFile::MemoryOnly
                    } else {
                        // Ensure that all chunks are not pending
                        for chunk in self.memory.read().iter() {
                            drop(chunk.read());
                        }

                        UnderlyingFile::ReadMode(FileBuffer::open(&self.path).ok())
                    }
                }
                OpenMode::Write => {
                    lock.1 = 1;
                    match *self.memory_mode.read() {
                        MemoryFileMode::AlwaysMemory => UnderlyingFile::MemoryOnly,
                        MemoryFileMode::PreferMemory => UnderlyingFile::MemoryPreferred,
                        MemoryFileMode::DiskOnly => self.create_writing_underlying_file(),
                    }
                }
            };

            lock.0 = mode;
            drop(lock);
            Ok(())
        } else {
            Err(format!(
                "File {}: cannot lock the open mode!",
                self.path.display()
            ))
        }
    }

    pub fn close(&self) {
        if let Some(mut lock) = self.open_mode.try_lock() {
            lock.1 -= 1;

            if lock.1 == 0 {
                lock.0 = OpenMode::None;
                match self.file.write().deref_mut() {
                    UnderlyingFile::WriteMode { file, .. } => {
                        file.1.lock().flush();
                    }
                    _ => {}
                }
            }
        } else {
            panic!("File {}: cannot lock the open mode!", self.path.display());
        }
    }

    pub fn reserve_space(
        &self,
        last_chunk: AllocatedChunk,
        out_chunks: &mut Vec<(Option<ArcRwLockReadGuard<RawRwLock, FileChunk>>, &mut [u8])>,
        mut size: usize,
        el_size: usize,
    ) -> AllocatedChunk {
        let mut chunk = last_chunk;

        loop {
            let rem_bytes = chunk.remaining_bytes();
            let rem_elements = rem_bytes / el_size;
            let el_bytes = min(size, rem_elements * el_size);

            assert!(chunk.max_len() >= el_size);

            let space = if el_bytes > 0 {
                Some(unsafe { chunk.prealloc_bytes_single_thread(el_bytes) })
            } else {
                None
            };

            size -= el_bytes;

            if size > 0 {
                let mut mem_lock = self.memory.write();
                mem_lock.push(Arc::new(RwLock::new(FileChunk::OnMemory { chunk })));
                if let Some(space) = space {
                    let chunk_lock = mem_lock.last().unwrap().clone();
                    let chunk_guard = chunk_lock.read_arc();
                    out_chunks.push((Some(chunk_guard), space));
                }
                drop(mem_lock);
                chunk = CHUNKS_ALLOCATOR.request_chunk(chunk_usage!(FileBuffer {
                    path: std::panic::Location::caller().to_string()
                }));
            } else {
                if let Some(space) = space {
                    out_chunks.push((None, space));
                }
                return chunk;
            }
        }
    }

    pub fn get_underlying_file(&self) -> RwLockReadGuard<UnderlyingFile> {
        self.file.read()
    }

    pub fn add_chunk(&self, chunk: AllocatedChunk) {
        let mut memory = self.memory.write();
        memory.push(Arc::new(RwLock::new(FileChunk::OnMemory { chunk })))
    }

    pub fn flush_chunks(&self, limit: usize) -> usize {
        if let Some(mut file) = self.file.try_write() {
            match &*file {
                UnderlyingFile::NotOpened | UnderlyingFile::MemoryPreferred => {
                    *file = self.create_writing_underlying_file();
                }
                _ => {}
            }

            if let UnderlyingFile::WriteMode {
                file,
                chunk_position,
            } = file.deref_mut()
            {
                if let memory = self.memory.read() {
                    let mut flushed_count = 0;
                    while flushed_count < limit {
                        if *chunk_position >= memory.len() {
                            return flushed_count;
                        }

                        if let Some(flushable_chunk) = memory[*chunk_position].try_write_arc() {
                            GlobalFlush::add_item_to_flush_queue(FlushableItem {
                                underlying_file: file.clone(),
                                mode: FileFlushMode::Append {
                                    chunk: flushable_chunk,
                                },
                            });
                            *chunk_position += 1;
                            flushed_count += 1;
                        } else {
                            return flushed_count;
                        }
                    }
                    return flushed_count;
                }
            }
        }
        return 0;
    }

    pub fn flush_pending_chunks_count(&self) -> usize {
        match self.file.read().deref() {
            UnderlyingFile::NotOpened
            | UnderlyingFile::MemoryOnly
            | UnderlyingFile::ReadMode(_) => 0,
            UnderlyingFile::WriteMode { chunk_position, .. } => {
                self.get_chunks_count() - *chunk_position
            }
            UnderlyingFile::MemoryPreferred => self.get_chunks_count(),
        }
    }

    #[inline(always)]
    pub fn has_flush_pending_chunks(&self) -> bool {
        self.flush_pending_chunks_count() > 0
    }

    pub fn change_to_disk_only(&self) {
        if self.is_memory_preferred() {
            *self.memory_mode.write() = MemoryFileMode::DiskOnly;
        }
    }

    #[inline(always)]
    pub fn has_only_one_chunk(&self) -> bool {
        self.memory.read().len() == 1
    }

    #[inline(always)]
    pub fn get_path(&self) -> &Path {
        self.path.as_ref()
    }

    #[inline(always)]
    pub fn len(&self) -> usize {
        self.memory
            .read()
            .iter()
            .map(|x| match x.read().deref() {
                FileChunk::OnDisk { len, .. } => *len,
                FileChunk::OnMemory { chunk } => chunk.len(),
            })
            .sum::<usize>()
    }
}

unsafe impl Sync for MemoryFileInternal {}
unsafe impl Send for MemoryFileInternal {}
