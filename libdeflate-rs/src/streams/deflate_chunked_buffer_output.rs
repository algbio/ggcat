use crate::utils::copy_rolling;
use crate::{DeflateOutput, OutStreamResult};
use counter_stats::counter::{AtomicCounter, SumMode};
use crc32fast::Hasher;
use nightly_quirks::utils::NightlyUtils;
use std::cmp::min;
use std::mem::size_of;
use std::slice::from_raw_parts_mut;

pub struct DeflateChunkedBufferOutput<'a> {
    buffer: Box<[u8]>,
    lookback_pos: usize,
    position: usize,
    crc32: Hasher,
    written: usize,
    func: Box<dyn FnMut(&[u8]) -> Result<(), ()> + 'a>,
}

static COUNTER_THREADS_BUSY_READING: AtomicCounter<SumMode> =
    declare_counter_i64!("libdeflate_reading_threads", SumMode, false);

impl<'a> DeflateChunkedBufferOutput<'a> {
    pub fn new<F: FnMut(&[u8]) -> Result<(), ()> + 'a>(write_func: F, buf_size: usize) -> Self {
        COUNTER_THREADS_BUSY_READING.inc();
        Self {
            buffer: unsafe { NightlyUtils::box_new_uninit_slice_assume_init(buf_size) },
            lookback_pos: 0,
            position: 0,
            crc32: Hasher::new(),
            written: 0,
            func: Box::new(write_func),
        }
    }

    fn flush_buffer(&mut self, ensure_size: usize) -> bool {
        self.crc32
            .update(&self.buffer[self.lookback_pos..self.position]);
        COUNTER_THREADS_BUSY_READING.sub(1);
        if (self.func)(&self.buffer[self.lookback_pos..self.position]).is_err() {
            COUNTER_THREADS_BUSY_READING.inc();
            return false;
        }
        COUNTER_THREADS_BUSY_READING.inc();
        self.written += self.position - self.lookback_pos;

        let keep_buf_len = min(self.position, Self::MAX_LOOK_BACK);
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
}

impl<'a> DeflateOutput for DeflateChunkedBufferOutput<'a> {
    #[inline(always)]
    fn copy_forward(&mut self, prev_offset: usize, length: usize) -> bool {
        if self.buffer.len() - self.position <= length {
            if !self.flush_buffer(length) {
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
    fn write(&mut self, data: &[u8]) -> bool {
        if self.buffer.len() - self.position <= data.len() {
            if !self.flush_buffer(data.len()) {
                return false;
            }
        }
        self.buffer[self.position..self.position + data.len()].copy_from_slice(data);
        self.position += data.len();
        true
    }

    #[inline(always)]
    fn get_available_buffer(&mut self) -> &mut [u8] {
        unsafe {
            from_raw_parts_mut(
                self.buffer.as_mut_ptr().add(self.position),
                self.buffer.len() - self.position,
            )
        }
    }

    #[inline(always)]
    unsafe fn advance_available_buffer_position(&mut self, offset: usize) {
        self.position += offset;
        if self.buffer.len() == self.position {
            self.flush_buffer(1);
        }
    }

    #[inline(always)]
    fn final_flush(&mut self) -> Result<OutStreamResult, ()> {
        self.flush_buffer(0);
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

impl Drop for DeflateChunkedBufferOutput<'_> {
    fn drop(&mut self) {
        COUNTER_THREADS_BUSY_READING.sub(1);
    }
}
