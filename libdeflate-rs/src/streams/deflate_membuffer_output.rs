use std::fs::File;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::slice::from_raw_parts_mut;
use crate::decompress_deflate::OutStreamResult;
use crate::DeflateOutput;
use crate::utils::copy_rolling;

pub struct DeflateMemBufferOutput {
    buffer: Vec<u8>,
    path: PathBuf
}

impl DeflateMemBufferOutput {
    pub fn new(path: impl AsRef<Path>) -> Self {
        Self {
            buffer: vec![0; 100000],//Vec::new(),
            path: path.as_ref().to_path_buf()
        }
    }
}

impl DeflateOutput for DeflateMemBufferOutput {
    #[inline(always)]
    fn copy_forward(&mut self, prev_offset: usize, length: usize) -> bool {
        if prev_offset > self.buffer.len() {
            return false;
        }
        self.buffer.reserve(length);

        unsafe {
            let dest = self.buffer.as_mut_ptr().add(self.buffer.len());
            copy_rolling(dest, dest.add(length), prev_offset, self.get_available_buffer().len() > 3);
            self.buffer.set_len(self.buffer.len() + length);
        }
        true
    }

    #[inline(always)]
    fn write(&mut self, data: &[u8]) -> bool {
        self.buffer.extend_from_slice(data);
        true
    }

    #[inline(always)]
    fn get_available_buffer(&mut self) -> &mut [u8] {
        self.buffer.reserve(1);
        let capacity = self.buffer.capacity();
        let len = self.buffer.len();
        unsafe {
            from_raw_parts_mut(self.buffer.as_mut_ptr().add(len), capacity - len)
        }
    }

    #[inline(always)]
    unsafe fn advance_available_buffer_position(&mut self, offset: usize) {
        self.buffer.set_len(self.buffer.len() + offset);
    }

    #[inline(always)]
    fn final_flush(&mut self) -> Result<OutStreamResult, ()> {
        println!("Successfully read {} bytes!", self.buffer.len());
        // File::create(&self.path).unwrap().write_all(&self.buffer).unwrap();
        Ok(OutStreamResult {
            written: 0,
            crc32: 0
        })
    }
}