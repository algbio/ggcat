#![deny(unused_must_use)]
// #![deny(warnings)]
// #![deny(warnings)]
#![feature(trait_alias)]
#![feature(type_alias_impl_trait)]

pub mod decompress_deflate;
pub mod decompress_gzip;
mod decompress_utils;
mod deflate_constants;
mod gzip_constants;
pub mod streams;
mod utils;

use std::future::Future;
use std::io::Read;
use tokio::io::AsyncReadExt;

#[macro_use]
extern crate static_assertions;

#[macro_use]
extern crate counter_stats;

use crate::decompress_deflate::{
    LenType, OutStreamResult, _DecStruct, LITLEN_ENOUGH, OFFSET_ENOUGH,
};
use crate::decompress_gzip::libdeflate_gzip_decompress;
use crate::deflate_constants::{DEFLATE_MAX_NUM_SYMS, DEFLATE_NUM_PRECODE_SYMS};
use crate::streams::deflate_async_chunked_buffer_input::DeflateAsyncChunkedBufferInput;
use crate::streams::deflate_async_chunked_buffer_output::{
    DeflateAsyncChunkedBufferOutput, OutputFnHelper,
};
use std::mem::MaybeUninit;

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

pub trait AsyncReadBuffer<'a> {
    type RetType: Future<Output = Result<usize, ()>> + 'a;

    fn read_from(&'a mut self, data: &'a mut [u8]) -> Self::RetType;
}

impl<'a> AsyncReadBuffer<'a> for tokio::fs::File {
    type RetType = impl Future<Output = Result<usize, ()>> + 'a;

    fn read_from(&'a mut self, data: &'a mut [u8]) -> Self::RetType {
        async { self.read(data).await.map_err(|_| ()) }
    }
}

pub async fn decompress_file_buffered<
    R: for<'r> crate::AsyncReadBuffer<'r>,
    C,
    F: for<'r> OutputFnHelper<'r, C>,
>(
    stream: R,
    func: F,
    context: C,
    buf_size: usize,
) -> Result<(), LibdeflateError> {
    let mut input_stream = DeflateAsyncChunkedBufferInput::new(buf_size);
    let mut output_stream = DeflateAsyncChunkedBufferOutput::new(buf_size);

    let mut decompressor = libdeflate_alloc_decompressor();

    while input_stream.ensure_length_await(1).await {
        libdeflate_gzip_decompress(&mut decompressor, &mut input_stream, &mut output_stream)
            .await?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::{decompress_file_buffered, AsyncReadBuffer, LibdeflateError};
    use std::future::Future;
    use std::io::Read;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use std::time::Instant;
    use tokio_uring::fs::File;

    struct UringReader {
        file: File,
        position: u64,
    }

    unsafe impl Sync for UringReader {}
    unsafe impl Send for UringReader {}

    struct IoBuffer {
        data: *mut u8,
        len: usize,
        bytes_init: usize,
    }

    unsafe impl tokio_uring::buf::IoBuf for IoBuffer {
        fn stable_ptr(&self) -> *const u8 {
            self.data
        }

        fn bytes_init(&self) -> usize {
            self.bytes_init
        }

        fn bytes_total(&self) -> usize {
            self.len
        }
    }

    unsafe impl tokio_uring::buf::IoBufMut for IoBuffer {
        fn stable_mut_ptr(&mut self) -> *mut u8 {
            self.data
        }

        unsafe fn set_init(&mut self, pos: usize) {
            self.bytes_init = pos;
        }
    }

    impl<'a> AsyncReadBuffer<'a> for std::fs::File {
        type RetType = impl Future<Output = Result<usize, ()>> + 'a;

        fn read_from(&'a mut self, data: &'a mut [u8]) -> Self::RetType {
            async { self.read(data).map_err(|_| ()) }
        }
    }

    impl<'a> AsyncReadBuffer<'a> for UringReader {
        type RetType = impl Future<Output = Result<usize, ()>> + 'a;

        fn read_from(&'a mut self, data: &'a mut [u8]) -> Self::RetType {
            async {
                let awaited = self.file.read_at(
                    IoBuffer {
                        data: data.as_mut_ptr(),
                        len: data.len(),
                        bytes_init: 0,
                    },
                    self.position,
                );

                let count = awaited.await.0.map_err(|_| ())?;
                self.position += count as u64;
                Ok(count)
            }
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn decompression_speed() {
        let context = Arc::new(AtomicUsize::new(0));

        async fn dummy_calc(val: &mut Arc<AtomicUsize>, data: &[u8]) -> Result<(), ()> {
            let mut rem = 0;
            for d in data {
                rem += *d as usize;
            }
            val.fetch_add(rem, Ordering::Relaxed);
            Ok(())
        }

        const PATH: &str = "/home/andrea/genome-assembly/data/salmonella-strains/strains-test";

        let paths = std::fs::read_dir(PATH).unwrap();
        let mut paths_vec = Vec::new();

        for path in paths {
            paths_vec.push(path.unwrap().path());
        }

        paths_vec.sort();
        paths_vec.truncate(10000);

        let semaphore = Arc::new(tokio::sync::Semaphore::new(32));
        let start = Instant::now();

        tokio_scoped::scope(|s| {
            for file in paths_vec {
                let context = context.clone();
                let semaphore = semaphore.clone();

                s.spawn(async move {
                    let permit = semaphore.acquire().await;
                    match decompress_file_buffered(
                        std::fs::File::open(&file).unwrap(),
                        dummy_calc,
                        context,
                        1024 * 512,
                    )
                    .await
                    {
                        Ok(_) => {}
                        Err(error) => {
                            println!("Error: {}", file.display());
                        }
                    }
                });
            }
        });
        println!("Bench duration: {:.2}", start.elapsed().as_secs_f32());
    }
}
