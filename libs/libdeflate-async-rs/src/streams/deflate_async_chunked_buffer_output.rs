use crate::utils::copy_rolling;
use crate::OutStreamResult;
use counter_stats::counter::{AtomicCounter, SumMode};
use crc32fast::Hasher;
use nightly_quirks::utils::NightlyUtils;
use std::cmp::min;
use std::future::Future;
use std::mem::size_of;
use std::slice::from_raw_parts_mut;

const MAX_LOOK_BACK: usize = 32768;

pub trait OutputFnHelper<'a, C> {
    type R: Future<Output = Result<(), ()>> + 'a;
    fn call(&mut self, context: &'a mut C, data: &'a [u8]) -> Self::R;
}

impl<'a, C, R, F> OutputFnHelper<'a, C> for F
where
    C: 'a,
    R: Future<Output = Result<(), ()>> + 'a,
    F: FnMut(&'a mut C, &'a [u8]) -> R,
{
    type R = R;
    fn call(&mut self, context: &'a mut C, data: &'a [u8]) -> R {
        (self)(context, data)
    }
}

pub struct DeflateAsyncChunkedBufferOutput {
    buffer: Box<[u8]>,
    lookback_pos: usize,
    position: usize,
    crc32: Hasher,
    written: usize,
}

static COUNTER_THREADS_BUSY_READING: AtomicCounter<SumMode> =
    declare_counter_i64!("libdeflate_reading_threads", SumMode, false);

static COUNTER_THREADS_PROCESSING_READS: AtomicCounter<SumMode> =
    declare_counter_i64!("libdeflate_processing_threads", SumMode, false);

impl DeflateAsyncChunkedBufferOutput {
    pub fn new(buf_size: usize) -> Self {
        COUNTER_THREADS_BUSY_READING.inc();
        Self {
            buffer: unsafe { NightlyUtils::box_new_uninit_slice_assume_init(buf_size) },
            lookback_pos: 0,
            position: 0,
            crc32: Hasher::new(),
            written: 0,
        }
    }

    #[inline(always)]
    async fn flush_buffer(&mut self, ensure_size: usize) -> bool {
        self.crc32
            .update(&self.buffer[self.lookback_pos..self.position]);
        COUNTER_THREADS_BUSY_READING.sub(1);
        COUNTER_THREADS_PROCESSING_READS.inc();
        if self
            .func
            .call(
                &mut self.context,
                &self.buffer[self.lookback_pos..self.position],
            )
            .await
            .is_err()
        {
            COUNTER_THREADS_BUSY_READING.inc();
            COUNTER_THREADS_PROCESSING_READS.sub(1);
            return false;
        }
        COUNTER_THREADS_BUSY_READING.inc();
        COUNTER_THREADS_PROCESSING_READS.sub(1);
        self.written += self.position - self.lookback_pos;

        let keep_buf_len = min(self.position, MAX_LOOK_BACK);
        unsafe {
            std::ptr::copy(
                self.buffer.as_ptr().add(self.position - keep_buf_len),
                self.buffer.as_mut_ptr(),
                keep_buf_len,
            );
        }
        self.lookback_pos = keep_buf_len;
        self.position = keep_buf_len;

        self.buffer.len() - self.position > ensure_size
    }

    #[inline(always)]
    pub async fn copy_forward(&mut self, prev_offset: usize, length: usize) -> bool {
        if self.buffer.len() - self.position <= length {
            if !self.flush_buffer(length).await {
                return false;
            }
        }

        if prev_offset > self.position {
            return false;
        }

        unsafe {
            let dest = self.buffer.as_mut_ptr().add(self.position);
            copy_rolling(
                dest,
                dest.add(length),
                prev_offset,
                self.get_available_buffer().len() >= (length + 3 * size_of::<usize>()),
            );
        }
        self.position += length;

        true
    }

    #[inline(always)]
    pub fn has_buffer_size(&mut self, needed: usize) -> bool {
        self.buffer.len() - self.position >= needed
    }

    #[inline(always)]
    pub async fn assert_buffer_size_await(&mut self, needed: usize) -> bool {
        self.flush_buffer(needed).await
    }

    #[inline(always)]
    pub fn write(&mut self, data: &[u8]) -> bool {
        if self.buffer.len() - self.position <= data.len() {
            return false;
        }
        self.buffer[self.position..self.position + data.len()].copy_from_slice(data);
        self.position += data.len();
        true
    }

    #[inline(always)]
    pub fn get_available_buffer(&mut self) -> &mut [u8] {
        unsafe {
            from_raw_parts_mut(
                self.buffer.as_mut_ptr().add(self.position),
                self.buffer.len() - self.position,
            )
        }
    }

    #[inline(always)]
    pub unsafe fn advance_available_buffer_position(&mut self, offset: usize) {
        self.position += offset;
    }

    #[inline(always)]
    pub async fn final_flush(&mut self) -> Result<OutStreamResult, ()> {
        self.flush_buffer(0).await;
        self.position = 0;
        self.lookback_pos = 0;

        let result = OutStreamResult {
            written: self.written,
            crc32: self.crc32.clone().finalize(),
        };

        self.crc32 = Hasher::new();
        self.written = 0;
        Ok(result)
    }
}

impl Drop for DeflateAsyncChunkedBufferOutput {
    fn drop(&mut self) {
        COUNTER_THREADS_BUSY_READING.sub(1);
    }
}
