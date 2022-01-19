use crate::{DeflateInput, DeflateOutput};
use nightly_quirks::utils::NightlyUtils;
use std::cmp::min;

pub struct DeflateChunkedBufferInput<'a> {
    buffer: Box<[u8]>,
    position: usize,
    last_position: usize,
    func: Box<dyn FnMut(&mut [u8]) -> usize + 'a>,
}

impl<'a> DeflateChunkedBufferInput<'a> {
    pub fn new<F: FnMut(&mut [u8]) -> usize + 'a>(read_func: F, buf_size: usize) -> Self {
        Self {
            buffer: unsafe { NightlyUtils::box_new_uninit_slice_assume_init(buf_size) },
            position: 0,
            last_position: 0,
            func: Box::new(read_func),
        }
    }

    fn refill_buffer(&mut self, min_amount: usize) -> bool {
        let keep_buf_len = min(self.position, Self::MAX_LOOK_BACK);

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

        let count = (self.func)(&mut self.buffer[self.last_position..]);

        self.last_position += count;

        (self.last_position - self.position) >= min_amount
    }
}

impl<'a> DeflateInput for DeflateChunkedBufferInput<'a> {
    #[inline(always)]
    unsafe fn get_le_word_no_advance(&mut self) -> usize {
        usize::from_le_bytes(
            *(self.buffer.as_ptr().add(self.position) as *const [u8; std::mem::size_of::<usize>()]),
        )
    }

    #[inline(always)]
    fn move_stream_pos(&mut self, amount: isize) -> bool {
        if amount > 0 {
            if self.position + amount as usize > self.last_position {
                if !self.refill_buffer(amount as usize) {
                    return false;
                }
            }
            self.position += amount as usize
        } else {
            self.position -= (-amount) as usize
        }
        self.position <= self.last_position
    }

    #[inline(always)]
    fn read(&mut self, out_data: &mut [u8]) -> usize {
        if self.last_position - self.position < out_data.len() {
            self.refill_buffer(out_data.len());
        }

        let avail_bytes = min(out_data.len(), self.last_position - self.position);
        unsafe {
            self.read_unchecked(&mut out_data[0..avail_bytes]);
        }
        avail_bytes
    }

    #[inline(always)]
    fn ensure_length(&mut self, len: usize) -> bool {
        if self.position + len > self.last_position {
            if !self.refill_buffer(len) {
                return false;
            }
        }
        true
    }

    #[inline(always)]
    unsafe fn read_unchecked(&mut self, out_data: &mut [u8]) {
        std::ptr::copy_nonoverlapping(
            self.buffer.as_ptr().add(self.position),
            out_data.as_mut_ptr(),
            out_data.len(),
        );
        self.position += out_data.len();
    }

    #[inline(always)]
    fn read_exact_into<O: DeflateOutput>(&mut self, out_stream: &mut O, mut length: usize) -> bool {
        while length > 0 {
            let buffer = out_stream.get_available_buffer();
            let copyable = min(buffer.len(), length);
            if self.read(&mut buffer[0..copyable]) != copyable {
                return false;
            }
            unsafe {
                out_stream.advance_available_buffer_position(copyable);
            }
            length -= copyable;
        }
        true
    }
}
