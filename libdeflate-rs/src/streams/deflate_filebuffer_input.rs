use crate::{DeflateInput, DeflateOutput};
use filebuffer::FileBuffer;
use std::cmp::min;
use std::path::Path;

pub struct DeflateFileBufferInput {
    file: FileBuffer,
    position: usize,
}

impl DeflateFileBufferInput {
    pub fn new(path: impl AsRef<Path>) -> Self {
        Self {
            file: FileBuffer::open(path).unwrap(),
            position: 0,
        }
    }
}

impl DeflateInput for DeflateFileBufferInput {
    #[inline(always)]
    unsafe fn get_le_word_no_advance(&mut self) -> usize {
        usize::from_le_bytes(
            *(self.file.as_ptr().add(self.position) as *const [u8; std::mem::size_of::<usize>()]),
        )
    }

    #[inline(always)]
    fn move_stream_pos(&mut self, amount: isize) -> bool {
        if amount > 0 {
            self.position += amount as usize
        } else {
            self.position -= (-amount) as usize
        }
        self.position < self.file.len()
    }

    #[inline(always)]
    fn read(&mut self, out_data: &mut [u8]) -> usize {
        let avail_bytes = min(out_data.len(), self.file.len() - self.position);
        unsafe {
            self.read_unchecked(&mut out_data[0..avail_bytes]);
        }
        avail_bytes
    }

    #[inline(always)]
    fn ensure_length(&mut self, len: usize) -> bool {
        self.position + len <= self.file.len()
    }

    #[inline(always)]
    unsafe fn read_unchecked(&mut self, out_data: &mut [u8]) {
        std::ptr::copy_nonoverlapping(
            self.file.as_ptr().add(self.position),
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
