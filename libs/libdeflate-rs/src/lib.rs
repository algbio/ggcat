#![deny(warnings)]
pub mod decompress_deflate;
pub mod decompress_gzip;
mod decompress_utils;
mod deflate_constants;
mod gzip_constants;
pub mod streams;
mod utils;

#[macro_use]
extern crate static_assertions;

#[macro_use]
extern crate counter_stats;

use crate::decompress_deflate::{
    LenType, OutStreamResult, _DecStruct, LITLEN_ENOUGH, OFFSET_ENOUGH,
};
use crate::decompress_gzip::libdeflate_gzip_decompress;
use crate::deflate_constants::{DEFLATE_MAX_NUM_SYMS, DEFLATE_NUM_PRECODE_SYMS};
use crate::streams::deflate_chunked_buffer_input::DeflateChunkedBufferInput;
use crate::streams::deflate_chunked_buffer_output::DeflateChunkedBufferOutput;
use std::fs::File;
use std::io::Read;
use std::mem::{size_of, MaybeUninit};
use std::path::Path;

/*
 * The main DEFLATE decompressor structure.  Since this implementation only
 * supports full buffer decompression, this structure does not store the entire
 * decompression state, but rather only some arrays that are too large to
 * comfortably allocate on the stack.
 */
pub struct LibdeflateDecompressor {
    pub(crate) precode_lens: [LenType; DEFLATE_NUM_PRECODE_SYMS],
    pub(crate) l: _DecStruct,
    pub(crate) litlen_decode_table: [u32; LITLEN_ENOUGH],

    pub(crate) offset_decode_table: [u32; OFFSET_ENOUGH],

    /* used only during build_decode_table() */
    pub(crate) sorted_syms: [u16; DEFLATE_MAX_NUM_SYMS],
    pub(crate) static_codes_loaded: bool,
}

/*
 * Result of a call to libdeflate_deflate_decompress(),
 * libdeflate_zlib_decompress(), or libdeflate_gzip_decompress().
 */
#[derive(Debug)]
pub enum LibdeflateError {
    /* Decompressed failed because the compressed data was invalid, corrupt,
     * or otherwise unsupported.  */
    BadData = 1,

    /* A NULL 'actual_out_nbytes_ret' was provided, but the data would have
     * decompressed to fewer than 'out_nbytes_avail' bytes.  */
    ShortOutput = 2,

    /* The data would have decompressed to more than 'out_nbytes_avail'
     * bytes.  */
    InsufficientSpace = 3,
}

pub trait DeflateInput {
    const MAX_LOOK_BACK: usize = size_of::<usize>();

    unsafe fn get_le_word_no_advance(&mut self) -> usize;
    fn move_stream_pos(&mut self, amount: isize) -> bool;
    fn read(&mut self, out_data: &mut [u8]) -> usize;
    fn ensure_length(&mut self, len: usize) -> bool;
    unsafe fn read_unchecked(&mut self, out_data: &mut [u8]);
    fn read_exact_into<O: DeflateOutput>(&mut self, out_stream: &mut O, length: usize) -> bool;

    #[inline(always)]
    fn read_byte(&mut self) -> u8 {
        let mut byte = [0];
        self.read(&mut byte);
        byte[0]
    }

    #[inline(always)]
    fn read_le_u16(&mut self) -> u16 {
        let mut bytes = [0, 0];
        self.read(&mut bytes);
        u16::from_le_bytes(bytes)
    }

    #[inline(always)]
    fn read_le_u32(&mut self) -> u32 {
        let mut bytes = [0, 0, 0, 0];
        self.read(&mut bytes);
        u32::from_le_bytes(bytes)
    }
}

pub trait DeflateOutput {
    const MAX_LOOK_BACK: usize = 32768;

    fn copy_forward(&mut self, prev_offset: usize, length: usize) -> bool;
    fn write(&mut self, data: &[u8]) -> bool;
    fn get_available_buffer(&mut self) -> &mut [u8];
    unsafe fn advance_available_buffer_position(&mut self, offset: usize);
    fn final_flush(&mut self) -> Result<OutStreamResult, ()>;
}

pub fn libdeflate_alloc_decompressor() -> LibdeflateDecompressor {
    /*
     * Note that only certain parts of the decompressor actually must be
     * initialized here:
     *
     * - 'static_codes_loaded' must be initialized to false.
     *
     * - The first half of the main portion of each decode table must be
     *   initialized to any value, to avoid reading from uninitialized
     *   memory during table expansion in build_decode_table().  (Although,
     *   this is really just to avoid warnings with dynamic tools like
     *   valgrind, since build_decode_table() is guaranteed to initialize
     *   all entries eventually anyway.)
     *
     * But for simplicity, we currently just zero the whole decompressor.
     */
    unsafe { MaybeUninit::<LibdeflateDecompressor>::zeroed().assume_init() }
}

pub fn decompress_file_buffered(
    file: impl AsRef<Path>,
    func: impl FnMut(&[u8]) -> Result<(), ()>,
    buf_size: usize,
) -> Result<(), LibdeflateError> {
    let mut read_file = File::open(file).unwrap();

    let mut input_stream =
        DeflateChunkedBufferInput::new(|buf| read_file.read(buf).unwrap_or(0), buf_size);

    let mut output_stream = DeflateChunkedBufferOutput::new(func, buf_size);

    let mut decompressor = libdeflate_alloc_decompressor();

    while input_stream.ensure_length(1) {
        libdeflate_gzip_decompress(&mut decompressor, &mut input_stream, &mut output_stream)?
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::decompress_file_buffered;
    use rayon::prelude::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use std::time::Instant;

    #[test]
    fn decompression_speed() {
        let context = Arc::new(AtomicUsize::new(0));

        const PATH: &str = "/home/andrea/genome-assembly/data/salmonella-strains/strains-test";

        let paths = std::fs::read_dir(PATH).unwrap();
        let mut paths_vec = Vec::new();

        for path in paths {
            paths_vec.push(path.unwrap().path());
        }

        paths_vec.sort();
        paths_vec.truncate(10000);
        let start = Instant::now();

        paths_vec.into_par_iter().for_each(|file| {
            let context = context.clone();

            match decompress_file_buffered(
                &file,
                |data| {
                    let mut rem = 0;
                    for d in data {
                        rem += *d as usize;
                    }
                    context.fetch_add(rem, Ordering::Relaxed);
                    Ok(())
                },
                1024 * 512,
            ) {
                Ok(_) => {}
                Err(_error) => {
                    println!("Error: {}", file.display());
                }
            }
        });

        println!("Bench duration: {:.2}", start.elapsed().as_secs_f32());
    }
}
