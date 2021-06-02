use std::cell::UnsafeCell;
use std::cmp::max;
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{Seek, SeekFrom, Write};

use std::ops::Deref;
use std::path::{Path, PathBuf};

use std::sync::Arc;
use std::thread::JoinHandle;

use crossbeam::channel::*;

use flushable_buffer::{FlushMode, FlushableBuffer};
use parking_lot::lock_api::{RawRwLock, RwLockReadGuard};
use parking_lot::{Mutex, MutexGuard, RwLock, RwLockWriteGuard};

use crate::memory_fs::buffer_manager::BUFFER_MANAGER;
use crate::memory_fs::concurrent_memory_chunk::ConcurrentMemoryChunk;
use crate::stats_logger::{StatMode, DEFAULT_STATS_LOGGER};
use crate::Utils;
use std::time::Duration;

pub mod buffer_manager;
pub mod concurrent_memory_chunk;
pub mod flushable_buffer;

#[derive(Copy, Clone)]
pub enum MemoryMode {
    Continuous { hint_size: usize },
    Chunks { chunk_size: usize },
    ChunksFileBuffer { buffer_size: usize },
}

enum InternalMemoryMode {
    Continuous {
        global_vector: ConcurrentMemoryChunk,
    },
    Chunks {
        chunk_size: usize,
        memory: RwLock<Vec<ConcurrentMemoryChunk>>,
    },
    ChunksFileBuffer {
        buffer_index: usize,
        buffer_vectors: RwLock<heapless::Vec<Arc<FlushableBuffer>, 8>>,
        file: Arc<Mutex<File>>,
        buffer_size: usize,
    },
}

static mut MEMORY_MAPPED_FILES: Option<Mutex<HashMap<PathBuf, Arc<MemoryFile>>>> = None;

static TAKE_FROM_QUEUE_MUTEX: Mutex<()> = Mutex::new(());
static mut GLOBAL_FLUSH_QUEUE: Option<Sender<Arc<FlushableBuffer>>> = None;

const JOIN_HANDLE_OPT: Option<JoinHandle<()>> = None;
static mut FLUSH_THREADS: [Option<JoinHandle<()>>; 16] = [JOIN_HANDLE_OPT; 16];

pub struct MemoryFile {
    path: PathBuf,
    data: UnsafeCell<InternalMemoryMode>,
}

pub struct CMCIterator<'a, A: Iterator<Item = &'a mut [u8]>, B: Iterator<Item = &'a mut [u8]>> {
    iter: CMCIteratorsEnum<'a, A, B>,
}

enum CMCIteratorsEnum<'a, A: Iterator<Item = &'a mut [u8]>, B: Iterator<Item = &'a mut [u8]>> {
    Continuous(A),
    Chunks(B),
}

impl<'a, A: Iterator<Item = &'a mut [u8]>, B: Iterator<Item = &'a mut [u8]>> Iterator
    for CMCIterator<'a, A, B>
{
    type Item = &'a mut [u8];

    #[inline(always)]
    fn next(&mut self) -> Option<Self::Item> {
        match &mut self.iter {
            CMCIteratorsEnum::Continuous(iter) => iter.next(),
            CMCIteratorsEnum::Chunks(iter) => iter.next(),
        }
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
        self.file.flush();
        Ok(())
    }
}

impl MemoryFile {
    pub fn init(flush_queue_size: usize, threads_count: usize) {
        unsafe {
            MEMORY_MAPPED_FILES = Some(Mutex::new(HashMap::with_capacity(512)));

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
                                let mut file_lock = file_to_flush.underlying_file.lock();
                                drop(queue_take_lock);

                                update_stat!(
                                    "GLOBAL_WRITING_QUEUE_SIZE",
                                    flush_channel_receiver.len() as f64,
                                    StatMode::Replace
                                );

                                update_stat!("TOTAL_DISK_FLUSHES", 1.0, StatMode::Sum);

                                match file_to_flush.flush_mode {
                                    FlushMode::Append => {
                                        file_lock.write_all(file_to_flush.buffer.get());
                                    }
                                    FlushMode::WriteAt(offset) => {
                                        file_lock.seek(SeekFrom::Start(offset as u64));
                                        file_lock.write_all(file_to_flush.buffer.get());
                                    }
                                }

                                update_stat!(
                                    "DISK_BYTES_WRITTEN",
                                    file_to_flush.buffer.get().len() as f64,
                                    StatMode::Sum
                                );

                                assert!(file_to_flush.flush_pending_lock.raw().is_locked());
                                file_to_flush.buffer.clear();
                                file_to_flush.flush_pending_lock.raw().unlock_exclusive();

                                // Try lock the queue again
                                drop(file_lock);
                                queue_take_lock = TAKE_FROM_QUEUE_MUTEX.lock();
                            }
                        })
                        .unwrap(),
                );
            }
        }
    }

    pub fn schedule_disk_write(
        file: Arc<Mutex<File>>,
        buffer: Vec<u8>,
        flush_mode: FlushMode,
    ) -> Arc<FlushableBuffer> {
        let flushable_buffer =
            FlushableBuffer::from_existing_memory_ready_to_flush(file, buffer, flush_mode);
        unsafe {
            GLOBAL_FLUSH_QUEUE
                .as_mut()
                .unwrap()
                .send(flushable_buffer.clone());
        }
        flushable_buffer
    }

    pub fn flush_to_disk() {
        while !unsafe { GLOBAL_FLUSH_QUEUE.as_mut().unwrap().is_empty() } {
            std::thread::sleep(Duration::from_millis(50));
        }
    }

    pub fn terminate() {
        unsafe {
            Self::flush_to_disk();
            GLOBAL_FLUSH_QUEUE.take();
        };
    }

    pub fn has_only_one_chunk(&self) -> bool {
        match unsafe { &mut *(self.data.get()) } {
            InternalMemoryMode::Continuous { .. } => true,
            InternalMemoryMode::Chunks { memory, .. } => memory.read().len() == 1,
            InternalMemoryMode::ChunksFileBuffer { .. } => false,
        }
    }

    pub fn len(&self) -> usize {
        match unsafe { &mut *(self.data.get()) } {
            InternalMemoryMode::Continuous { global_vector } => global_vector.len(),
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

    pub unsafe fn get_chunks_mut(&self) -> impl Iterator<Item = &mut [u8]> {
        CMCIterator {
            iter: match unsafe { &mut *(self.data.get()) } {
                InternalMemoryMode::Continuous { global_vector } => CMCIteratorsEnum::Continuous(
                    Some(global_vector).into_iter().map(|x| x.get_mut()),
                ),
                InternalMemoryMode::Chunks {
                    chunk_size: _,
                    memory,
                } => {
                    let lock = memory.read();

                    // Safe
                    let data: &Vec<ConcurrentMemoryChunk> = std::mem::transmute(lock.deref());

                    let iterator = RwLockIterator {
                        lock,
                        iterator: data.iter(),
                    };

                    CMCIteratorsEnum::Chunks(iterator.map(|c| c.get_mut()))
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

    fn internal_mode_from_underlying_file(
        file: Arc<Mutex<File>>,
        buffer_size: usize,
    ) -> InternalMemoryMode {
        InternalMemoryMode::ChunksFileBuffer {
            buffer_index: 0,
            buffer_vectors: {
                let mut vec = heapless::Vec::new();
                vec.push(BUFFER_MANAGER.request_buffer(
                    file.clone(),
                    buffer_size,
                    FlushMode::Append,
                ));
                RwLock::new(vec)
            },
            file,
            buffer_size,
        }
    }

    fn new(path: impl AsRef<Path>, mode: MemoryMode) -> Self {
        Self {
            path: PathBuf::from(path.as_ref()),
            data: UnsafeCell::new(match mode {
                MemoryMode::Continuous { hint_size } => InternalMemoryMode::Continuous {
                    global_vector: ConcurrentMemoryChunk::new(hint_size),
                },
                MemoryMode::Chunks { chunk_size } => InternalMemoryMode::Chunks {
                    chunk_size,
                    memory: RwLock::new(vec![]),
                },
                MemoryMode::ChunksFileBuffer { buffer_size } => {
                    let underlying_file = Arc::new(Mutex::new(
                        OpenOptions::new()
                            .create(true)
                            .write(true)
                            .truncate(true)
                            .open(path)
                            .unwrap(),
                    ));
                    Self::internal_mode_from_underlying_file(underlying_file, buffer_size)
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

    pub fn create_from_owned_memory(
        path: impl AsRef<Path>,
        owned_memory: Vec<Vec<u8>>,
    ) -> Arc<MemoryFile> {
        let mem_file = Arc::new(MemoryFile::new(
            path.as_ref(),
            MemoryMode::Chunks {
                chunk_size: 1024 * 1024 * 24,
            },
        ));

        // Guaranteed
        if let InternalMemoryMode::Chunks {
            chunk_size: _chunk_size,
            memory,
        } = unsafe { &mut *(mem_file.data.get()) }
        {
            let mut data = memory.write();
            data.clear();
            data.reserve(owned_memory.len());
            for el in owned_memory {
                data.push(ConcurrentMemoryChunk::from_memory(el));
            }
        }

        let mut map = Self::get_map();
        map.insert(path.as_ref().to_path_buf(), mem_file.clone());

        mem_file
    }

    pub fn create_buffer_from_existing(
        file: Arc<Mutex<File>>,
        buffer_size: usize,
    ) -> Arc<MemoryFile> {
        Arc::new(MemoryFile {
            path: PathBuf::from(""),
            data: UnsafeCell::new(Self::internal_mode_from_underlying_file(file, buffer_size)),
        })
    }

    pub fn create(path: impl AsRef<Path>, mode: MemoryMode) -> Arc<MemoryFile> {
        let mut map = Self::get_map();
        let mem_file = Arc::new(MemoryFile::new(path.as_ref(), mode));
        match mode {
            MemoryMode::Continuous { .. } | MemoryMode::Chunks { .. } => {
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

    pub fn flush(&self) {
        match unsafe { &mut *(self.data.get()) } {
            InternalMemoryMode::ChunksFileBuffer {
                buffer_index,
                buffer_vectors,
                ..
            } => {
                let buffers = buffer_vectors.write();
                for index in 0..buffers.len() {
                    let buffer = &buffers[(*buffer_index + index) % buffers.len()];
                    if buffer.buffer.len() > 0 {
                        unsafe {
                            if buffer.flush_pending_lock.raw().try_lock_exclusive() {
                                GLOBAL_FLUSH_QUEUE
                                    .as_mut()
                                    .unwrap()
                                    .send(buffer.clone())
                                    .unwrap();
                            }
                        }
                    }
                }
                for buffer in buffers.iter() {
                    // Wait for the buffers to be flushed
                    buffer.flush_pending_lock.read();
                }
            }
            _ => {}
        }
    }

    pub fn write_all(&self, buf: &[u8]) {
        match unsafe { &mut *(self.data.get()) } {
            InternalMemoryMode::Continuous { global_vector } => {
                if global_vector.write_bytes_extend(buf) {
                    update_stat!("RESIZED_CONTIGUOUS_FILES", 1.0, StatMode::Sum);
                    println!(
                        "Resized \"{}\" to {}",
                        self.path.display(),
                        Utils::convert_human(global_vector.len() as f64)
                    );
                }
            }
            InternalMemoryMode::Chunks { chunk_size, memory } => {
                let mut memory_guard = memory.read();
                loop {
                    if let Some(chunk) = memory_guard.last() {
                        if let Ok(_) = chunk.write_bytes_noextend(buf) {
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

                    write_memory_guard.push(ConcurrentMemoryChunk::new(*chunk_size));
                    memory_guard = RwLockWriteGuard::downgrade(write_memory_guard);
                }
            }
            InternalMemoryMode::ChunksFileBuffer {
                buffer_index,
                buffer_vectors,
                file,
                buffer_size,
            } => {
                let mut current_buffer_read_lock = buffer_vectors.read();

                loop {
                    if unsafe {
                        !current_buffer_read_lock[*buffer_index]
                            .flush_pending_lock
                            .raw()
                            .try_lock_shared()
                    } {
                        // Try to add another buffer
                        // FIXME: Put constant size
                        if current_buffer_read_lock.len() < 8 {
                            drop(current_buffer_read_lock);
                            let mut current_buffer_write_lock = buffer_vectors.write();

                            current_buffer_write_lock.push(BUFFER_MANAGER.request_buffer(
                                file.clone(),
                                *buffer_size,
                                FlushMode::Append,
                            ));
                            *buffer_index = current_buffer_write_lock.len() - 1;

                            current_buffer_read_lock =
                                RwLockWriteGuard::downgrade(current_buffer_write_lock);
                        }

                        unsafe {
                            current_buffer_read_lock[*buffer_index]
                                .flush_pending_lock
                                .raw()
                                .lock_shared()
                        }
                    };

                    if let Ok(_) = current_buffer_read_lock[*buffer_index]
                        .buffer
                        .write_bytes_noextend(buf)
                    {
                        unsafe {
                            current_buffer_read_lock[*buffer_index]
                                .flush_pending_lock
                                .raw()
                                .unlock_shared()
                        }
                        return;
                    }

                    unsafe {
                        current_buffer_read_lock[*buffer_index]
                            .flush_pending_lock
                            .raw()
                            .unlock_shared()
                    }

                    drop(current_buffer_read_lock);
                    let current_buffer_write_lock = buffer_vectors.write();

                    if current_buffer_write_lock[*buffer_index]
                        .buffer
                        .has_space_for(buf.len())
                    {
                        drop(current_buffer_write_lock);
                        current_buffer_read_lock = buffer_vectors.read();
                        continue;
                    }

                    unsafe {
                        if !current_buffer_write_lock[*buffer_index]
                            .flush_pending_lock
                            .raw()
                            .try_lock_exclusive()
                        {
                            current_buffer_read_lock = buffer_vectors.read();
                            continue;
                        }
                        GLOBAL_FLUSH_QUEUE
                            .as_mut()
                            .unwrap()
                            .send(current_buffer_write_lock[*buffer_index].clone());
                        // Change the buffer
                        *buffer_index = (*buffer_index + 1) % current_buffer_write_lock.len();
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
        self.flush();
    }
}
