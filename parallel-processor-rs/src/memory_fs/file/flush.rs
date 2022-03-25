use crate::memory_fs::allocator::AllocatedChunk;
use crate::memory_fs::file::internal::FileChunk;
use crate::memory_fs::flushable_buffer::{FileFlushMode, FlushableItem};
use counter_stats::counter::{AtomicCounter, AtomicCounterGuardSum, MaxMode, SumMode};
use crossbeam::channel::*;
use parking_lot::lock_api::{RawMutex, RawRwLock};
use parking_lot::{Mutex, RwLock};
use std::cmp::max;
use std::fs::File;
use std::io::{Seek, SeekFrom, Write};
use std::ops::DerefMut;
use std::path::PathBuf;
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::Duration;

static mut GLOBAL_FLUSH_QUEUE: Option<Sender<FlushableItem>> = None;
static FLUSH_THREADS: Mutex<Vec<JoinHandle<()>>> =
    Mutex::const_new(parking_lot::RawMutex::INIT, vec![]);

static TAKE_FROM_QUEUE_MUTEX: Mutex<()> = Mutex::const_new(parking_lot::RawMutex::INIT, ());
static WRITING_CHECK: RwLock<()> = RwLock::const_new(parking_lot::RawRwLock::INIT, ());

static COUNTER_WRITING_APPEND: AtomicCounter<SumMode> =
    declare_counter_i64!("threads_file_append_count", SumMode, false);

static COUNTER_WRITE_AT: AtomicCounter<SumMode> =
    declare_counter_i64!("threads_write_at_count", SumMode, false);

static COUNTER_DISK_FLUSHES: AtomicCounter<SumMode> =
    declare_counter_i64!("disk_flushes", SumMode, false);

static COUNTER_BYTES_WRITTEN: AtomicCounter<SumMode> =
    declare_counter_i64!("bytes_written_count", SumMode, false);

static GLOBAL_QUEUE_MAX_SIZE_NOW: AtomicCounter<MaxMode> =
    declare_counter_i64!("global_queue_max_size_now", MaxMode, true);

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
                        let _stat = AtomicCounterGuardSum::new(&COUNTER_WRITING_APPEND, 1);
                        GLOBAL_QUEUE_MAX_SIZE_NOW.max(flush_channel_receiver.len() as i64);
                        COUNTER_DISK_FLUSHES.inc();

                        let offset = file_lock.stream_position().unwrap();

                        file_lock.write_all(chunk.get()).unwrap();
                        let len = chunk.len();
                        *file_chunk = FileChunk::OnDisk { offset, len };
                        COUNTER_BYTES_WRITTEN.inc_by(len as i64);
                    }
                }
                FileFlushMode::WriteAt { buffer, offset } => {
                    let _writing_check = WRITING_CHECK.read();
                    GLOBAL_QUEUE_MAX_SIZE_NOW.max(flush_channel_receiver.len() as i64);

                    COUNTER_DISK_FLUSHES.inc();
                    let _stat = AtomicCounterGuardSum::new(&COUNTER_WRITE_AT, 1);
                    file_lock.seek(SeekFrom::Start(offset)).unwrap();
                    file_lock.write_all(buffer.get()).unwrap();
                    COUNTER_BYTES_WRITTEN.inc_by(buffer.get().len() as i64);
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
