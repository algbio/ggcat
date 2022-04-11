use crate::buckets::bucket_writer::BucketItem;
use crate::buckets::readers::compressed_binary_reader::CompressedBinaryReader;
use crate::buckets::readers::lock_free_binary_reader::LockFreeBinaryReader;
use crate::buckets::readers::BucketReader;
use crate::memory_fs::RemoveFileMode;
use crossbeam::channel::*;
use parking_lot::{Condvar, Mutex};
use std::cmp::min;
use std::io::Read;
use std::path::PathBuf;
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::Duration;

enum OpenedFile {
    None,
    Plain(LockFreeBinaryReader),
    Compressed(CompressedBinaryReader),
}

pub struct AsyncReaderThread {
    buffers: (Sender<Vec<u8>>, Receiver<Vec<u8>>),
    buffers_pool: (Sender<Vec<u8>>, Receiver<Vec<u8>>),
    opened_file: Mutex<OpenedFile>,
    file_wait_condvar: Condvar,
    thread: Mutex<Option<JoinHandle<()>>>,
}

impl AsyncReaderThread {
    pub fn new(buffers_size: usize, buffers_count: usize) -> Arc<Self> {
        let buffers_pool = bounded(buffers_count);

        for _ in 0..buffers_count {
            buffers_pool
                .0
                .send(Vec::with_capacity(buffers_size))
                .unwrap();
        }

        Arc::new(Self {
            buffers: bounded(buffers_count),
            buffers_pool,
            opened_file: Mutex::new(OpenedFile::None),
            file_wait_condvar: Condvar::new(),
            thread: Mutex::new(None),
        })
    }

    fn read_thread(self: Arc<Self>) {
        while Arc::strong_count(&self) > 1 {
            let mut file = self.opened_file.lock();
            let mut buffer = self.buffers_pool.1.recv().unwrap();
            unsafe {
                buffer.set_len(buffer.capacity());
            }

            let bytes_read = (match &mut *file {
                OpenedFile::None => {
                    self.file_wait_condvar
                        .wait_for(&mut file, Duration::from_secs(5));
                    let _ = self.buffers_pool.0.send(buffer);
                    continue;
                }
                OpenedFile::Plain(file) => file.get_single_stream().read(buffer.as_mut_slice()),
                OpenedFile::Compressed(file) => {
                    file.get_single_stream().read(buffer.as_mut_slice())
                }
            })
            .unwrap();

            unsafe {
                buffer.set_len(bytes_read);
            }

            // File completely read
            if bytes_read == 0 {
                *file = OpenedFile::None;
            }

            let _ = self.buffers.0.send(buffer);
        }
    }

    fn read_bucket(
        self: Arc<Self>,
        path: PathBuf,
        compressed: bool,
        remove_file: RemoveFileMode,
        prefetch: Option<usize>,
    ) -> AsyncStreamThreadReader {
        let mut opened_file = self.opened_file.lock();
        match &*opened_file {
            OpenedFile::None => {}
            _ => panic!("File already opened!"),
        }

        *opened_file = if compressed {
            OpenedFile::Compressed(CompressedBinaryReader::new(path, remove_file, prefetch))
        } else {
            OpenedFile::Plain(LockFreeBinaryReader::new(path, remove_file, prefetch))
        };
        self.file_wait_condvar.notify_all();
        drop(opened_file);

        let stream_recv = self.buffers.1.clone();
        let owner = self.clone();

        let mut thread = self.thread.lock();
        let mt_self = self.clone();
        if thread.is_none() {
            *thread = Some(std::thread::spawn(move || {
                mt_self.read_thread();
            }));
        }
        drop(thread);

        let current = stream_recv.recv().unwrap();

        AsyncStreamThreadReader {
            receiver: stream_recv,
            owner,
            current,
            current_pos: 0,
        }
    }
}

pub struct AsyncStreamThreadReader {
    receiver: Receiver<Vec<u8>>,
    owner: Arc<AsyncReaderThread>,
    current: Vec<u8>,
    current_pos: usize,
}

impl Read for AsyncStreamThreadReader {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let mut bytes_read = 0;
        loop {
            if self.current_pos == self.current.len() {
                if self.current.len() == 0 {
                    return Ok(bytes_read);
                }
                let next = self.receiver.recv().unwrap();
                let _ = self
                    .owner
                    .buffers_pool
                    .0
                    .send(std::mem::replace(&mut self.current, next));
                self.current_pos = 0;
                continue;
            }

            let avail = self.current.len() - self.current_pos;
            let to_read = min(buf.len() - bytes_read, avail);
            buf[bytes_read..(bytes_read + to_read)]
                .copy_from_slice(&mut self.current[self.current_pos..(self.current_pos + to_read)]);
            bytes_read += to_read;
            self.current_pos += to_read;

            if bytes_read == buf.len() {
                return Ok(bytes_read);
            }
        }
    }
}

impl Drop for AsyncStreamThreadReader {
    fn drop(&mut self) {
        let _ = self
            .owner
            .buffers_pool
            .0
            .send(std::mem::take(&mut self.current));
    }
}

pub struct AsyncBinaryReader {
    path: PathBuf,
    compressed: bool,
    read_thread: Arc<AsyncReaderThread>,
    remove_file: RemoveFileMode,
    prefetch: Option<usize>,
}

// static ASYNC_READERS: ThreadLocalVariable<AsyncDecoderInternal> = ThreadLocalVariable::

impl AsyncBinaryReader {
    pub fn new(
        path: &PathBuf,
        read_thread: Arc<AsyncReaderThread>,
        compressed: bool,
        remove_file: RemoveFileMode,
        prefetch: Option<usize>,
    ) -> Self {
        Self {
            path: path.clone(),
            compressed,
            read_thread,
            remove_file,
            prefetch,
        }
    }
}

impl BucketReader for AsyncBinaryReader {
    fn decode_all_bucket_items<E: BucketItem, F: for<'a> FnMut(E::ReadType<'a>)>(
        self,
        mut buffer: E::ReadBuffer,
        mut func: F,
    ) {
        let mut stream = self.read_thread.read_bucket(
            self.path,
            self.compressed,
            self.remove_file,
            self.prefetch,
        );
        while let Some(el) = E::read_from(&mut stream, &mut buffer) {
            func(el);
        }
    }
}
