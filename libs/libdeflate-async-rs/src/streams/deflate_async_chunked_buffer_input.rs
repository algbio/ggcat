use crate::streams::deflate_async_chunked_buffer_output::OutputFnHelper;
use crate::DeflateAsyncChunkedBufferOutput;
use nightly_quirks::utils::NightlyUtils;
use std::cmp::min;
use std::io::Read;
use std::mem::size_of;
use std::pin::Pin;
use tokio::io::AsyncReadExt;

const MAX_LOOK_BACK: usize = size_of::<usize>();

pub struct DeflateAsyncChunkedBufferInput {
    buffer: Box<[u8]>,
    position: usize,
    last_position: usize,
}

impl DeflateAsyncChunkedBufferInput {
    pub fn new(buf_size: usize) -> Self {
        Self {
            buffer: unsafe { NightlyUtils::box_new_uninit_slice_assume_init(buf_size) },
            position: 0,
            last_position: 0,
        }
    }

    pub fn refill_buffer(&mut self, mut stream: impl Read, min_amount: usize) -> bool {
        let keep_buf_len = min(self.position, MAX_LOOK_BACK);

        let move_offset = self.position - keep_buf_len;
        let move_amount = self.last_position - move_offset;

        unsafe {
            std::ptr::copy(
                self.buffer.as_ptr().add(move_offset),
                self.buffer.as_mut_ptr(),
                move_amount,
            );
        }
        self.position -= move_offset;
        self.last_position -= move_offset;

        while (self.last_position - self.position) < min_amount {
            let count = stream
                .read(&mut self.buffer[self.last_position..])
                .unwrap_or(0);

            if count == 0 {
                break;
            }

            self.last_position += count;
        }

        (self.last_position - self.position) >= min_amount
    }

    #[inline(always)]
    pub(crate) unsafe fn get_le_word_no_advance(&mut self) -> usize {
        usize::from_le_bytes(
            *(self.buffer.as_ptr().add(self.position) as *const [u8; std::mem::size_of::<usize>()]),
        )
    }

    #[inline(always)]
    pub(crate) fn move_stream_pos(&mut self, amount: isize) -> bool {
        if amount > 0 {
            self.position += amount as usize
        } else {
            self.position -= (-amount) as usize
        }
        self.position <= self.last_position
    }

    #[inline(always)]
    pub fn read(&mut self, out_data: &mut [u8]) -> usize {
        let avail_bytes = min(out_data.len(), self.last_position - self.position);
        unsafe {
            self.read_unchecked(&mut out_data[0..avail_bytes]);
        }
        avail_bytes
    }

    #[inline(always)]
    pub fn ensure_length(&mut self, len: usize) -> bool {
        if self.position + len > self.last_position {
            return false;
        }
        true
    }

    #[inline(always)]
    pub unsafe fn read_unchecked(&mut self, out_data: &mut [u8]) {
        std::ptr::copy_nonoverlapping(
            self.buffer.as_ptr().add(self.position),
            out_data.as_mut_ptr(),
            out_data.len(),
        );
        self.position += out_data.len();
    }

    #[inline(always)]
    pub fn read_exact_into(
        &mut self,
        out_stream: &mut DeflateAsyncChunkedBufferOutput,
        length: usize,
    ) -> bool {
        let buffer = out_stream.get_available_buffer();
        if buffer.len() < length || self.read(&mut buffer[0..length]) != length {
            return false;
        }
        unsafe {
            out_stream.advance_available_buffer_position(length);
        }
        true
    }

    #[inline(always)]
    pub fn read_byte(&mut self) -> u8 {
        let mut byte = [0];
        self.read(&mut byte);
        byte[0]
    }

    #[inline(always)]
    pub async fn read_le_u16(&mut self) -> u16 {
        let mut bytes = [0, 0];
        self.read(&mut bytes);
        u16::from_le_bytes(bytes)
    }

    #[inline(always)]
    pub async fn read_le_u32(&mut self) -> u32 {
        let mut bytes = [0, 0, 0, 0];
        self.read(&mut bytes);
        u32::from_le_bytes(bytes)
    }
}
