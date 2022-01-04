use crate::utils::flexible_pool::{FlexiblePool, PoolableObject};
use parallel_processor::mem_tracker::tracked_box::TrackedBox;
use std::io::{Error, ErrorKind, Write};
use std::mem::MaybeUninit;
use std::ops::DerefMut;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

#[derive(Clone)]
pub struct ChunkedVectorPool<T: Copy> {
    pool: FlexiblePool<TrackedBox<[MaybeUninit<T>]>>,
}

// static ALLOCATED_COUNT: AtomicU64 = AtomicU64::new(0);
// static ALLOCATED_SIZE: AtomicU64 = AtomicU64::new(0);

impl<T> PoolableObject for TrackedBox<[MaybeUninit<T>]> {
    type AllocData = usize;

    #[inline(always)]
    fn allocate(suggested_length: usize) -> Self {
        // ALLOCATED_COUNT.fetch_add(1, Ordering::Relaxed);
        // ALLOCATED_SIZE.fetch_add(suggested_length as u64, Ordering::Relaxed);
        // println!(
        //     "Count: {} size: {}",
        //     ALLOCATED_COUNT.load(Ordering::Relaxed),
        //     ALLOCATED_SIZE.load(Ordering::Relaxed)
        // );
        unsafe { TrackedBox::new_uninit_slice(suggested_length) }
    }

    #[inline(always)]
    fn reinitialize(&mut self) {}
}

impl<T: Copy> ChunkedVectorPool<T> {
    pub fn new(suggested_length: usize) -> Self {
        Self {
            pool: FlexiblePool::new(suggested_length),
        }
    }
}

pub struct ChunkedVector<T: Copy> {
    pool: ChunkedVectorPool<T>,
    pub chunks: Vec<TrackedBox<[MaybeUninit<T>]>>,
    current_data: *mut T,
    pub current_size_left: usize,
}

unsafe impl<T: Copy> Sync for ChunkedVector<T> {}
unsafe impl<T: Copy> Send for ChunkedVector<T> {}

impl<T: Copy> ChunkedVector<T> {
    pub fn new(pool: ChunkedVectorPool<T>) -> Self {
        let mut first_chunk = pool.pool.take_object_owned();

        let data_ptr = first_chunk.as_mut_ptr();
        let length = first_chunk.len();

        ChunkedVector {
            pool,
            chunks: vec![first_chunk],
            current_data: data_ptr as *mut T,
            current_size_left: length,
        }
    }

    pub fn clear(&mut self) {
        self.chunks
            .drain(1..)
            .for_each(|chunk| self.pool.pool.release_object(chunk));
        self.current_data = self.chunks[0].as_mut_ptr() as *mut T;
        self.current_size_left = self.chunks[0].len();
    }

    #[inline(always)]
    pub fn ensure_reserve(&mut self, size: usize) -> *const T {
        if self.current_size_left < size {
            let mut new_chunk = self.pool.pool.take_object_owned();

            if new_chunk.len() < size {
                replace_with::replace_with_or_abort(&mut new_chunk, |_b| unsafe {
                    TrackedBox::new_uninit_slice(size)
                })
            }

            self.current_data = new_chunk.as_mut_ptr() as *mut T;
            self.current_size_left = new_chunk.len();
            self.chunks.push(new_chunk);
        }
        self.current_data
    }

    #[inline(always)]
    pub fn push_contiguous(&mut self, data: &T) {
        assert!(self.current_size_left > 0);
        unsafe {
            std::ptr::copy_nonoverlapping(data as *const T, self.current_data, 1);
            self.current_data = self.current_data.add(1);
            #[cfg(feature = "mem-analysis")]
            self.chunks
                .last_mut()
                .unwrap()
                .notify_maximum_usage(self.current_data as *const _);
            self.current_size_left -= 1;
        }
    }

    #[inline(always)]
    #[cfg_attr(feature = "mem-analysis", track_caller)]
    pub fn push_contiguous_slice(&mut self, data: &[T]) {
        assert!(data.len() <= self.current_size_left);
        unsafe {
            std::ptr::copy_nonoverlapping(data.as_ptr(), self.current_data, data.len());
            self.current_data = self.current_data.add(data.len());
            self.current_size_left -= data.len();

            #[cfg(feature = "mem-analysis")]
            self.chunks
                .last_mut()
                .unwrap()
                .notify_maximum_usage(self.current_data as *const _);

            if self.current_data as usize
                > (self
                    .chunks
                    .last()
                    .unwrap()
                    .as_ptr()
                    .add(self.chunks.last().unwrap().len()) as usize)
            {
                std::io::stdout().lock().flush();
                panic!(
                    "{} > {} => {} > {} / {}",
                    self.current_data as usize,
                    (self
                        .chunks
                        .last()
                        .unwrap()
                        .as_ptr()
                        .add(self.chunks.last().unwrap().len()) as usize),
                    data.len(),
                    self.current_size_left,
                    std::process::id()
                )
            }
        }
    }
}

impl<T: Copy> Drop for ChunkedVector<T> {
    fn drop(&mut self) {
        self.chunks
            .drain(..)
            .for_each(|chunk| self.pool.pool.release_object(chunk));
    }
}

impl Write for ChunkedVector<u8> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.push_contiguous_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }

    fn write_all(&mut self, mut buf: &[u8]) -> std::io::Result<()> {
        self.push_contiguous_slice(buf);
        Ok(())
    }
}
