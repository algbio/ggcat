use std::cell::UnsafeCell;
use std::cmp::max;
use std::collections::HashMap;
use std::fs::{read_link, File, OpenOptions};
use std::io::{Seek, SeekFrom, Write};

use std::ops::{Deref, DerefMut};
use std::path::{Path, PathBuf};

use std::sync::Arc;
use std::thread::JoinHandle;

use crossbeam::channel::*;

use flushable_buffer::{FlushMode, FlushableBuffer};
use parking_lot::lock_api::{RawRwLock, RwLockReadGuard};
use parking_lot::{Mutex, MutexGuard, RwLock, RwLockWriteGuard};

use crate::memory_data_size::*;
use crate::memory_fs::allocator::{AllocatedChunk, CHUNKS_ALLOCATOR};
use crate::memory_fs::buffer_manager::BUFFER_MANAGER;
use crate::stats_logger::StatMode;
use crate::Utils;
use std::mem::{replace, size_of};
// use std::os::unix::fs::OpenOptionsExt;
// use std::os::unix::io::AsRawFd;
use std::panic::Location;
use std::slice::from_raw_parts_mut;
use std::time::Duration;

pub const O_DIRECT: i32 = 0x4000;

#[macro_use]
pub mod allocator;
pub mod buffer_manager;
pub mod flushable_buffer;

const FLUSH_BUFFERS_COUNT: usize = 2;

#[derive(Copy, Clone)]
pub enum MemoryMode {
    Chunks,
    ChunksFileBuffer,
}

enum InternalMemoryMode {
    Chunks {
        memory: RwLock<Vec<AllocatedChunk>>,
    },
    ChunksFileBuffer {
        current_buffer: RwLock<FlushableBuffer>,
        file: Arc<(PathBuf, Mutex<File>)>,
    },
}

static mut MEMORY_MAPPED_FILES: Option<Mutex<HashMap<PathBuf, Arc<MemoryFile>>>> = None;

static mut FILES_FLUSH_HASH_MAP: Option<Mutex<HashMap<PathBuf, Vec<Arc<(PathBuf, Mutex<File>)>>>>> =
    None;

static TAKE_FROM_QUEUE_MUTEX: Mutex<()> = Mutex::new(());
static mut GLOBAL_FLUSH_QUEUE: Option<Sender<FlushableBuffer>> = None;
static WRITING_CHECK: RwLock<()> = RwLock::new(());

const JOIN_HANDLE_OPT: Option<JoinHandle<()>> = None;
static mut FLUSH_THREADS: [Option<JoinHandle<()>>; 16] = [JOIN_HANDLE_OPT; 16];

pub struct MemoryFile {
    path: PathBuf,
    data: UnsafeCell<InternalMemoryMode>,
}

pub struct CMCIterator<'a, I: Iterator<Item = &'a mut [u8]>> {
    iter: I,
}

impl<'a, I: Iterator<Item = &'a mut [u8]>> Iterator for CMCIterator<'a, I> {
    type Item = &'a mut [u8];

    #[inline(always)]
    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next()
    }
}

struct RwLockIterator<'a, A, B, R: parking_lot::lock_api::RawRwLock, I: Iterator<Item = B>> {
    lock: RwLockReadGuard<'a, R, A>,
    iterator: I,
}
impl<'a, A, B, R: parking_lot::lock_api::RawRwLock, I: Iterator<Item = B>> Iterator
    for RwLockIterator<'a, A, B, R, I>
{
    type Item = B;

    fn next(&mut self) -> Option<Self::Item> {
        self.iterator.next()
    }
}

unsafe impl Sync for MemoryFile {}
unsafe impl Send for MemoryFile {}

pub struct MemoryFileWriter {
    file: Arc<MemoryFile>,
}

impl Write for MemoryFileWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.file.write_all(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.file.flush_async();
        Ok(())
    }
}

impl MemoryFile {
    fn flush_buffer_and_replace(
        &self,
        buffer: &mut FlushableBuffer,
        file: Arc<(PathBuf, Mutex<File>)>,
        mode: FlushMode,
    ) {
        unsafe {
            GLOBAL_FLUSH_QUEUE
                .as_mut()
                .unwrap()
                .send(std::ptr::read(buffer as *const FlushableBuffer))
                .unwrap();
            std::ptr::write(
                buffer as *mut FlushableBuffer,
                BUFFER_MANAGER.request_buffer(file, mode),
            );
        }
    }

    pub fn init(flush_queue_size: usize, threads_count: usize) {
        unsafe {
            MEMORY_MAPPED_FILES = Some(Mutex::new(HashMap::with_capacity(8192)));
            FILES_FLUSH_HASH_MAP = Some(Mutex::new(HashMap::with_capacity(8192)));

            let (flush_channel_sender, flush_channel_receiver) = bounded(flush_queue_size);

            GLOBAL_FLUSH_QUEUE = Some(flush_channel_sender);

            for i in 0..max(1, threads_count) {
                let flush_channel_receiver = flush_channel_receiver.clone();
                FLUSH_THREADS[i] = Some(
                    std::thread::Builder::new()
                        .name(String::from("flushing-thread"))
                        .spawn(move || {
                            let mut queue_take_lock = TAKE_FROM_QUEUE_MUTEX.lock();
                            while let Ok(file_to_flush) = flush_channel_receiver.recv() {
                                // Here it's held an exclusive lock to ensure that sequential writes to a file are not processed concurrently
                                let mut file_lock = file_to_flush.underlying_file.1.lock();
                                let writing_check = WRITING_CHECK.read();
                                drop(queue_take_lock);

                                update_stat!(
                                    "GLOBAL_WRITING_QUEUE_SIZE",
                                    flush_channel_receiver.len() as f64,
                                    StatMode::Replace
                                );

                                update_stat!("TOTAL_DISK_FLUSHES", 1.0, StatMode::Sum);

                                match file_to_flush.flush_mode {
                                    FlushMode::Append => {
                                        file_lock.write_all(file_to_flush.get_buffer());
                                    }
                                    FlushMode::WriteAt(offset) => {
                                        file_lock.seek(SeekFrom::Start(offset as u64));
                                        file_lock.write_all(file_to_flush.get_buffer());
                                    }
                                }

                                update_stat!(
                                    "DISK_BYTES_WRITTEN",
                                    file_to_flush.get_buffer().len() as f64,
                                    StatMode::Sum
                                );
                                file_to_flush.clear_buffer();

                                // Try lock the queue again
                                drop(file_lock);
                                drop(file_to_flush);
                                drop(writing_check);
                                queue_take_lock = TAKE_FROM_QUEUE_MUTEX.lock();
                            }
                        })
                        .unwrap(),
                );
            }
        }
    }

    pub fn ensure_flushed(file: impl AsRef<Path>) {
        unsafe {
            FILES_FLUSH_HASH_MAP
                .as_mut()
                .unwrap()
                .lock()
                .remove(&file.as_ref().to_path_buf());
        }
    }

    pub fn schedule_disk_write(
        file: Arc<(PathBuf, Mutex<File>)>,
        buffer: AllocatedChunk,
        flush_mode: FlushMode,
    ) {
        let flushable_buffer =
            FlushableBuffer::from_existing_memory_ready_to_flush(file, buffer, flush_mode);
        unsafe {
            GLOBAL_FLUSH_QUEUE.as_mut().unwrap().send(flushable_buffer);
        }
    }

    pub fn flush_to_disk() {
        while !unsafe { GLOBAL_FLUSH_QUEUE.as_mut().unwrap().is_empty() } {
            std::thread::sleep(Duration::from_millis(50));
        }
        // Ensure that no writers are still writing!
        WRITING_CHECK.write();
    }

    pub fn terminate() {
        unsafe {
            Self::flush_to_disk();
            GLOBAL_FLUSH_QUEUE.take();
            for thread in FLUSH_THREADS.iter_mut() {
                if let Some(thread) = thread.take() {
                    thread.join().unwrap();
                }
            }
        };
    }

    pub fn has_only_one_chunk(&self) -> bool {
        match unsafe { &mut *(self.data.get()) } {
            InternalMemoryMode::Chunks { memory, .. } => memory.read().len() == 1,
            InternalMemoryMode::ChunksFileBuffer { .. } => false,
        }
    }

    pub fn len(&self) -> usize {
        match unsafe { &mut *(self.data.get()) } {
            InternalMemoryMode::Chunks { memory, .. } => {
                memory.read().iter().map(|x| x.len()).sum::<usize>()
            }
            InternalMemoryMode::ChunksFileBuffer { .. } => panic!("Unsupported!"),
        }
    }

    #[inline(always)]
    pub fn get_chunks(&self) -> impl Iterator<Item = &[u8]> {
        unsafe { self.get_chunks_mut().map(|c| &*c) }
    }

    pub unsafe fn get_typed_chunks_mut<T: 'static>(&self) -> impl Iterator<Item = &mut [T]> {
        self.get_chunks_mut().map(|c| {
            let elcnt = c.len() / size_of::<T>();
            assert_eq!(c.len() % size_of::<T>(), 0);
            from_raw_parts_mut(c.as_mut_ptr() as *mut T, elcnt)
        })
    }

    pub unsafe fn get_chunks_mut(&self) -> impl Iterator<Item = &mut [u8]> {
        CMCIterator {
            iter: match unsafe { &mut *(self.data.get()) } {
                InternalMemoryMode::Chunks { memory } => {
                    let lock = memory.read();

                    // Safe
                    let data: &Vec<AllocatedChunk> = std::mem::transmute(lock.deref());

                    let iterator = RwLockIterator {
                        lock,
                        iterator: data.iter(),
                    };

                    iterator.map(|c| unsafe { c.get_mut_slice() })
                }
                InternalMemoryMode::ChunksFileBuffer { .. } => {
                    panic!("Read not supported for file buffer!");
                }
            },
        }
    }

    fn get_map() -> MutexGuard<'static, HashMap<PathBuf, Arc<MemoryFile>>> {
        unsafe { MEMORY_MAPPED_FILES.as_mut().unwrap().lock() }
    }

    fn internal_mode_from_underlying_file(file: Arc<(PathBuf, Mutex<File>)>) -> InternalMemoryMode {
        InternalMemoryMode::ChunksFileBuffer {
            current_buffer: RwLock::new(
                BUFFER_MANAGER.request_buffer(file.clone(), FlushMode::Append),
            ),
            file,
        }
    }

    pub fn new(path: impl AsRef<Path>, mode: MemoryMode) -> Self {
        Self {
            path: PathBuf::from(path.as_ref()),
            data: UnsafeCell::new(match mode {
                MemoryMode::Chunks => InternalMemoryMode::Chunks {
                    memory: RwLock::new(vec![]),
                },
                MemoryMode::ChunksFileBuffer => {
                    let underlying_file = Arc::new((
                        path.as_ref().into(),
                        Mutex::new(
                            OpenOptions::new()
                                .create(true)
                                .write(true)
                                .truncate(true)
                                // .custom_flags(O_DIRECT)
                                .open(path)
                                .unwrap(),
                        ),
                    ));
                    Self::internal_mode_from_underlying_file(underlying_file)
                }
            }),
        }
    }

    pub fn get_path(&self) -> &Path {
        self.path.as_ref()
    }

    pub fn create_writer(path: impl AsRef<Path>, mode: MemoryMode) -> MemoryFileWriter {
        let mem_file = Self::create(path, mode);
        MemoryFileWriter { file: mem_file }
    }

    pub fn create_from_joined_files(files: Vec<Arc<MemoryFile>>) -> Arc<MemoryFile> {
        let new_file = MemoryFile::new("", MemoryMode::Chunks);
        for file in files.into_iter() {
            // Guaranteed
            if let InternalMemoryMode::Chunks { memory } = unsafe { &mut *(file.data.get()) } {
                memory.write();
            }
        }

        Arc::new(new_file)
    }

    pub fn create_from_owned_memory(
        path: impl AsRef<Path>,
        owned_memory: Vec<AllocatedChunk>,
    ) -> Arc<MemoryFile> {
        let mem_file = Arc::new(MemoryFile::new(path.as_ref(), MemoryMode::Chunks));

        // Guaranteed
        if let InternalMemoryMode::Chunks { memory } = unsafe { &mut *(mem_file.data.get()) } {
            let mut data = memory.write();
            data.clear();
            data.reserve(owned_memory.len());
            for el in owned_memory {
                data.push(el);
            }
        }

        let mut map = Self::get_map();
        map.insert(path.as_ref().to_path_buf(), mem_file.clone());

        mem_file
    }

    pub fn create_buffer_from_existing(file: Arc<(PathBuf, Mutex<File>)>) -> Arc<MemoryFile> {
        Arc::new(MemoryFile {
            path: PathBuf::from(""),
            data: UnsafeCell::new(Self::internal_mode_from_underlying_file(file)),
        })
    }

    pub fn create(path: impl AsRef<Path>, mode: MemoryMode) -> Arc<MemoryFile> {
        let mut map = Self::get_map();
        let mem_file = Arc::new(MemoryFile::new(path.as_ref(), mode));
        match mode {
            MemoryMode::Chunks => {
                map.insert(path.as_ref().to_path_buf(), mem_file.clone());
            }
            MemoryMode::ChunksFileBuffer { .. } => {}
        }
        mem_file
    }

    pub fn open(path: impl AsRef<Path>) -> Option<Arc<MemoryFile>> {
        let map = Self::get_map();
        map.get(&PathBuf::from(path.as_ref())).map(|x| x.clone())
    }

    pub fn get_and_remove(path: impl AsRef<Path>) -> Option<Arc<MemoryFile>> {
        let mut map = Self::get_map();
        map.remove(&PathBuf::from(path.as_ref()))
    }

    pub fn flush_async(&self) {
        match unsafe { &mut *(self.data.get()) } {
            InternalMemoryMode::ChunksFileBuffer {
                current_buffer,
                file,
                ..
            } => {
                let mut current_buffer = current_buffer.write();
                if current_buffer.get_buffer().len() > 0 {
                    self.flush_buffer_and_replace(
                        &mut current_buffer,
                        file.clone(),
                        FlushMode::Append,
                    );
                }
            }
            _ => {}
        }
    }

    pub fn extend_with_full_allocated_chunk(&self, chunk: AllocatedChunk) {
        match unsafe { &mut *(self.data.get()) } {
            InternalMemoryMode::Chunks { memory } => {
                let mut write_memory_guard = memory.write();
                write_memory_guard.push(chunk);
            }
            InternalMemoryMode::ChunksFileBuffer { .. } => {
                self.write_all(chunk.get());
            }
        }
    }

    pub fn write_all(&self, buf: &[u8]) {
        match unsafe { &mut *(self.data.get()) } {
            InternalMemoryMode::Chunks { memory } => {
                let mut memory_guard = memory.read();
                loop {
                    if let Some(chunk) = memory_guard.last() {
                        if chunk.write_bytes_noextend(buf) {
                            return;
                        }
                    }
                    // Extension needed
                    drop(memory_guard);
                    let mut write_memory_guard = memory.write();

                    if let Some(last_chunk) = write_memory_guard.last() {
                        if last_chunk.has_space_for(buf.len()) {
                            memory_guard = RwLockWriteGuard::downgrade(write_memory_guard);
                            continue;
                        }
                    }

                    write_memory_guard.push(CHUNKS_ALLOCATOR.request_chunk(chunk_usage!(
                        InMemoryFile {
                            path: String::from(self.path.to_str().unwrap())
                        }
                    )));
                    memory_guard = RwLockWriteGuard::downgrade(write_memory_guard);
                }
            }
            InternalMemoryMode::ChunksFileBuffer {
                current_buffer,
                file,
            } => {
                let mut current_buffer_read_lock = current_buffer.read();

                loop {
                    if current_buffer_read_lock.write_bytes_noextend(buf) {
                        return;
                    }
                    drop(current_buffer_read_lock);
                    let mut current_buffer_write_lock = current_buffer.write();

                    if current_buffer_write_lock.has_space_for(buf.len()) {
                        drop(current_buffer_write_lock);
                        current_buffer_read_lock = current_buffer.read();
                        continue;
                    }

                    unsafe {
                        self.flush_buffer_and_replace(
                            current_buffer_write_lock.deref_mut(),
                            file.clone(),
                            FlushMode::Append,
                        )
                    }
                    current_buffer_read_lock =
                        RwLockWriteGuard::downgrade(current_buffer_write_lock);
                }
            }
        }
    }
}

impl Drop for MemoryFile {
    fn drop(&mut self) {
        self.flush_async();
    }
}
