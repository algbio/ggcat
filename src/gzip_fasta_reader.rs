
use std::fs::File;
use bio::io::fastq;

use serde::Serialize;
use std::process::{Stdio};
use std::process::Command;
use rand::Rng;
use std::num::Wrapping;
use nix::unistd::Pid;
use nix::sys::signal::{self, Signal};
use std::hint::unreachable_unchecked;

pub struct GzipFastaReader;
impl GzipFastaReader {
    pub fn process_file<F: FnMut(&[u8])>(source: String, mut func: F) {
        let mut process = Command::new("gzip").args(&["-cd", source.as_str()]).stdout(Stdio::piped()).spawn().unwrap();
        let pid = process.id() as i32;
        let decompress = process.stdout.unwrap();
        let reader = fastq::Reader::new(decompress);
        for record in reader.records() {
            func(record.unwrap().seq());
        }
    }
}