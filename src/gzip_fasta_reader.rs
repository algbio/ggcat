use bio::io::fastq;
use std::fs::File;

use crate::libdeflate::{
    libdeflate_alloc_decompressor, libdeflate_deflate_compress, libdeflate_deflate_decompress_ex,
    libdeflate_free_decompressor, libdeflate_gzip_decompress, libdeflate_gzip_decompress_ex,
};
use bio::io::fastq::Record;
use nix::sys::signal::{self, Signal};
use nix::unistd::Pid;
use rand::Rng;
use serde::Serialize;
use std::ffi::{c_void, CString};
use std::hint::unreachable_unchecked;
use std::io::Read;
use std::num::Wrapping;
use std::os::raw::c_int;
use std::process::Command;
use std::process::Stdio;
use std::ptr::null_mut;
use std::slice::from_raw_parts;

enum ParsingState {
    Ident,
    Sequence,
    Quality,
}

#[derive(Copy, Clone)]
pub struct FastaSequence<'a> {
    pub ident: &'a [u8],
    pub seq: &'a [u8],
    pub qual: &'a [u8],
}

pub struct GzipFastaReader;
impl GzipFastaReader {
    pub fn process_file<F: FnMut(&[u8])>(source: String, mut func: F) {
        let mut process = Command::new("./libdeflate/gzip")
            .args(&["-cd", source.as_str()])
            .stdout(Stdio::piped())
            .spawn()
            .unwrap();
        let pid = process.id() as i32;
        let decompress = process.stdout.unwrap();
        let reader = fastq::Reader::new(decompress);
        for record in reader.records() {
            func(record.unwrap().seq());
        }
    }
    pub fn process_file_extended<F: FnMut(FastaSequence)>(source: String, mut func: F) {
        let mut decompress_cmp = None;
        let mut decompress_ucmp = None;

        let mut decompress: &mut dyn Read;

        if source.ends_with(".gz") {
            let decompressor = unsafe { libdeflate_alloc_decompressor() };

            let mut buffer = Vec::with_capacity(1024 * 1024);
            unsafe {
                buffer.set_len(1024 * 1024);

                let path = CString::new(source.as_str()).unwrap();
                let fd = libc::open(path.as_ptr(), libc::O_RDONLY);

                // FIXME: Better error handling!
                assert!(fd >= 0);

                let len = libc::lseek(fd, 0, libc::SEEK_END);

                assert!(len >= 0);

                let mmapped = libc::mmap64(
                    null_mut(),
                    len as usize,
                    libc::PROT_READ,
                    libc::MAP_SHARED,
                    fd,
                    0,
                );
                libc::madvise(mmapped, len as usize, libc::MADV_SEQUENTIAL);

                assert_ne!(mmapped, null_mut());

                let mut input_bytes = 0;
                let mut output_bytes = 0;

                let mut total_reads = 0;

                let mut in_ptr = mmapped as *const u8;
                let mut rem_len = len;
                loop {
                    input_bytes = 0;
                    output_bytes = 0;

                    extern "C" fn flush_function(
                        data: *mut c_void,
                        buffer: *mut c_void,
                        len: u64,
                    ) -> c_int {
                        return 0;
                    }

                    let result = libdeflate_gzip_decompress_ex(
                        decompressor,
                        in_ptr as *const c_void,
                        rem_len as u64,
                        buffer.as_mut_ptr() as *mut c_void,
                        buffer.len() as u64,
                        &mut input_bytes,
                        &mut output_bytes,
                        Some(flush_function),
                        null_mut(),
                    );
                    rem_len -= input_bytes as i64;
                    in_ptr = in_ptr.add(input_bytes as usize);
                    total_reads += output_bytes;
                    if result != 0 {
                        println!("Result failed {}!", result);
                        break;
                    }
                    if rem_len <= 0 {
                        break;
                    }
                }

                println!(
                    "File: {} Size: {} IO[{}/{}] Reads: {}",
                    &source, rem_len, input_bytes, output_bytes, total_reads
                );
                libdeflate_free_decompressor(decompressor);

                libc::munmap(mmapped, len as usize);
                libc::close(fd);

                return;
            }

            let mut process = Command::new("./libdeflate/gzip")
                .args(&["-cd", source.as_str()])
                .stdout(Stdio::piped())
                .spawn()
                .unwrap();
            let pid = process.id() as i32;
            decompress_cmp = Some(process.stdout.unwrap());
            decompress = decompress_cmp.as_mut().unwrap();
        } else {
            decompress_ucmp = Some(File::open(source).unwrap());
            decompress = decompress_ucmp.as_mut().unwrap();
        }

        let mut buffer = [0; 32768];
        let mut ident: Vec<u8> = Vec::new();
        let mut seq: Vec<u8> = Vec::new();
        let mut qual: Vec<u8> = Vec::new();

        let mut state = ParsingState::Ident;

        let mut read: isize = 0;

        while let Ok(count) = decompress.read(&mut buffer) {
            continue;
            let count: isize = count as isize;
            if count == 0 {
                break;
            }

            while read < count {
                match state {
                    ParsingState::Ident => {
                        while read < count && buffer[read as usize] != b'\n' {
                            ident.push(buffer[read as usize]);
                            read += 1;
                            continue;
                        }
                        if read < count {
                            read += 1;
                            state = ParsingState::Sequence
                        }
                    }
                    ParsingState::Sequence => {
                        while read < count && buffer[read as usize] != b'\n' {
                            seq.push(buffer[read as usize]);
                            read += 1;
                            continue;
                        }
                        if read < count {
                            read += 3; // Skip newline, + and another newline
                            state = ParsingState::Quality
                        }
                    }
                    ParsingState::Quality => {
                        while read < count && buffer[read as usize] != b'\n' {
                            qual.push(buffer[read as usize]);
                            read += 1;
                            continue;
                        }
                        if read < count {
                            // Process sequence
                            func(FastaSequence {
                                ident: ident.as_slice(),
                                seq: seq.as_slice(),
                                qual: qual.as_slice(),
                            });
                            //                            println!("{}", String::from_utf8_lossy(&seq.as_slice()));
                            //                            println!("{}", String::from_utf8_lossy(&qual.as_slice()));
                            ident.clear();
                            seq.clear();
                            qual.clear();
                            read += 1;
                            state = ParsingState::Ident
                        }
                    }
                }
            }
            read -= count;
        }

        //        let reader = fastq::Reader::new(decompress);
        //        for record in reader.records() {
        //            func(&record.unwrap());
        //        }
    }
}
