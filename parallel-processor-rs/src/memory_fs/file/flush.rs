use crate::memory_fs::allocator::AllocatedChunk;
use crate::memory_fs::file::internal::FileChunk;
use crate::memory_fs::flushable_buffer::{FileFlushMode, FlushableItem};
use crate::stats_logger::{StatMode, StatRaiiCounter};
use crossbeam::channel::*;
use parking_lot::{Mutex, RwLock};
use std::cmp::max;
use std::fs::File;
use std::io::{Seek, SeekFrom, Write};
use std::ops::{Deref, DerefMut};
use std::path::PathBuf;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::Duration;

static mut GLOBAL_FLUSH_QUEUE: Option<Sender<FlushableItem>> = None;
static FLUSH_THREADS: Mutex<Vec<JoinHandle<()>>> = Mutex::new(vec![]);

static TAKE_FROM_QUEUE_MUTEX: Mutex<()> = Mutex::new(());
static WRITING_CHECK: RwLock<()> = RwLock::new(());

pub struct GlobalFlush;

impl GlobalFlush {
    pub fn global_queue_occupation() -> (usize, usize) {
        unsafe {
            (
                GLOBAL_FLUSH_QUEUE.as_ref().unwrap().len(),
                GLOBAL_FLUSH_QUEUE
                    .as_ref()
                    .unwrap()
                    .capacity()
                    .unwrap_or(usize::MAX),
            )
        }
    }

    pub fn is_queue_empty() -> bool {
        unsafe { GLOBAL_FLUSH_QUEUE.as_ref().unwrap().len() == 0 }
    }

    pub fn add_item_to_flush_queue(item: FlushableItem) {
        unsafe { GLOBAL_FLUSH_QUEUE.as_mut().unwrap().send(item).unwrap() }
    }

    fn flush_thread(flush_channel_receiver: Receiver<FlushableItem>) {
        let mut queue_take_lock = TAKE_FROM_QUEUE_MUTEX.lock();

        while let Ok(file_to_flush) = flush_channel_receiver.recv() {
            // Here it's held an exclusive lock to ensure that sequential writes to a file are not processed concurrently
            let mut file_lock = file_to_flush.underlying_file.1.lock();
            let _writing_check = WRITING_CHECK.read();
            drop(queue_take_lock);

            match file_to_flush.mode {
                FileFlushMode::Append {
                    chunk: mut file_chunk,
                } => {
                    if let FileChunk::OnMemory { chunk } = file_chunk.deref_mut() {
                        let _stat = StatRaiiCounter::create("FILE_DISK_WRITING_APPEND");
                        let offset = file_lock.stream_position().unwrap();

                        file_lock.write_all(chunk.get()).unwrap();
                        let len = chunk.len();
                        *file_chunk = FileChunk::OnDisk { offset, len };
                    }
                }
                FileFlushMode::WriteAt { buffer, offset } => {
                    let _writing_check = WRITING_CHECK.read();
                    update_stat!(
                        "GLOBAL_WRITING_QUEUE_SIZE",
                        flush_channel_receiver.len() as f64,
                        StatMode::Replace
                    );

                    update_stat!("TOTAL_DISK_FLUSHES", 1.0, StatMode::Sum);
                    let _stat = StatRaiiCounter::create("FILE_DISK_WRITEAT");
                    file_lock.seek(SeekFrom::Start(offset));
                    file_lock.write_all(buffer.get()).unwrap();
                }
            }

            drop(_writing_check);
            drop(file_lock);
            // Try lock the queue again
            queue_take_lock = TAKE_FROM_QUEUE_MUTEX.lock();
        }
    }

    pub fn is_initialized() -> bool {
        return unsafe { GLOBAL_FLUSH_QUEUE.is_some() };
    }

    pub fn init(flush_queue_size: usize, threads_count: usize) {
        let (flush_channel_sender, flush_channel_receiver) = bounded(flush_queue_size);

        unsafe {
            GLOBAL_FLUSH_QUEUE = Some(flush_channel_sender);
        }

        for _ in 0..max(1, threads_count) {
            let flush_channel_receiver = flush_channel_receiver.clone();
            FLUSH_THREADS.lock().push(
                std::thread::Builder::new()
                    .name(String::from("flushing-thread"))
                    .spawn(move || Self::flush_thread(flush_channel_receiver))
                    .unwrap(),
            );
        }
    }

    pub fn schedule_disk_write(
        file: Arc<(PathBuf, Mutex<File>)>,
        buffer: AllocatedChunk,
        offset: u64,
    ) {
        Self::add_item_to_flush_queue(FlushableItem {
            underlying_file: file,
            mode: FileFlushMode::WriteAt { buffer, offset },
        })
    }

    pub fn flush_to_disk() {
        while !unsafe { GLOBAL_FLUSH_QUEUE.as_mut().unwrap().is_empty() } {
            std::thread::sleep(Duration::from_millis(50));
        }
        // Ensure that no writers are still writing!
        drop(WRITING_CHECK.write());
    }

    pub fn terminate() {
        Self::flush_to_disk();
        unsafe {
            GLOBAL_FLUSH_QUEUE.take();
        }
        let mut threads = FLUSH_THREADS.lock();
        for thread in threads.drain(..) {
            thread.join().unwrap();
        }
    }
}
