use bio::io::fastq;
use std::fs::File;

use bio::io::fastq::Record;
use nix::sys::signal::{self, Signal};
use nix::unistd::Pid;
use rand::Rng;
use serde::Serialize;
use std::hint::unreachable_unchecked;
use std::io::Read;
use std::num::Wrapping;
use std::process::Command;
use std::process::Stdio;

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
