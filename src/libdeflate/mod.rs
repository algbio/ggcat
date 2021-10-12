mod raw;

use self::raw::*;
use std::ffi::{c_void, CString};
use std::fs::File;
use std::io::ErrorKind;
use std::os::raw::c_int;
use std::path::Path;
use std::ptr::null_mut;
use std::slice::from_raw_parts;

pub fn decompress_file(
    file: impl AsRef<Path>,
    mut callback: impl FnMut(&[u8]),
    buf_size: usize,
) -> Result<(), ErrorKind> {
    unsafe {
        let decompressor = unsafe { libdeflate_alloc_decompressor() };

        let mut buffer = Vec::with_capacity(buf_size);
        buffer.set_len(buf_size);

        let mut file_map = filebuffer::FileBuffer::open(&file).unwrap();

        libc::madvise(
            file_map.as_ptr() as *mut c_void,
            file_map.len(),
            libc::MADV_SEQUENTIAL,
        );

        struct FuncWrapper<'a> {
            func: &'a mut dyn FnMut(&[u8]),
        }

        let mut wrapper = FuncWrapper {
            func: &mut callback,
        };

        unsafe extern "C" fn flush_function(
            data: *mut c_void,
            buffer: *mut c_void,
            len: u64,
        ) -> c_int {
            let wrapper = &mut *(data as *mut FuncWrapper);
            (wrapper.func)(from_raw_parts(buffer as *const u8, len as usize));
            return 0;
        }

        let mut input_bytes = 0;
        let mut output_bytes = 0;

        // let mut total_reads = 0;

        let mut in_ptr = file_map.as_ptr();
        let mut rem_len = file_map.len();
        loop {
            input_bytes = 0;
            output_bytes = 0;

            let result = libdeflate_gzip_decompress_ex(
                decompressor,
                in_ptr as *const c_void,
                rem_len as u64,
                buffer.as_mut_ptr() as *mut c_void,
                buffer.len() as u64,
                &mut input_bytes,
                &mut output_bytes,
                Some(flush_function),
                &mut wrapper as *mut FuncWrapper as *mut c_void,
            );
            rem_len -= input_bytes as usize;
            in_ptr = in_ptr.add(input_bytes as usize);
            // total_reads += output_bytes;
            if result != 0 {
                return Err(ErrorKind::InvalidData);
            }
            if rem_len == 0 {
                break;
            }
        }

        // println!(
        //     "File: {} Size: {} IO[{}/{}] Reads: {}",
        //     file.as_ref().display(),
        //     rem_len,
        //     input_bytes,
        //     output_bytes,
        //     total_reads
        // );
        libdeflate_free_decompressor(decompressor);

        drop(file_map);

        return Ok(());
    }
}
