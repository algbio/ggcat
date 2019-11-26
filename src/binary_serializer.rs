
use std::fs::File;
use bio::io::fastq;


use crate::single_dna_read_half::SingleDnaReadHalf;

use serde::Serialize;
use std::process::{Stdio};
use std::process::Command;
use rand::Rng;
use std::num::Wrapping;
use nix::unistd::Pid;
use nix::sys::signal::{self, Signal};
use std::hint::unreachable_unchecked;

const BUFFER_SIZE: usize = 1024 * 1024 * 32;

#[inline(always)]
fn xorshift32(x: &mut u32)
{
/* Algorithm "xor" from p. 4 of Marsaglia, "Xorshift RNGs" */
    *x ^= *x << 13;
    *x ^= *x >> 17;
    *x ^= *x << 5;
}


pub struct BinarySerializer;
impl BinarySerializer {

    pub fn process_file<F: FnMut(&[u8])>(source: String, mut func: F) {

        // /home/andrea/Desktop/pigz-2.4/unpigz
        let mut process = Command::new("gzip").args(&["-cd", source.as_str()]).stdout(Stdio::piped()).spawn().unwrap();
        let pid = process.id() as i32;
        let decompress = process.stdout.unwrap();
//File::open(source).unwrap();//
        let reader = fastq::Reader::new(decompress);
        let _records = 0;
        let bases = [b'A', b'C', b'G', b'T', b'N'];
        let mut x = Wrapping(2847386477usize);
        let mut recordo: Option<Vec<u8>> = None;
        for record in reader.records() {
//            signal::kill(Pid::from_raw(pid), Signal::SIGTERM);
//            if let None = recordo.as_ref() {
                recordo = Some(Vec::from(record.as_ref().unwrap().seq()));
//            }
//            let seq2 = record.seq();
//            let mut seq1 = seq2.to_vec();//&seq;
//            loop {
//                for (idx, y) in seq1.iter_mut().enumerate() {
//                    *y = bases[(x.0 % 5) as usize]; //rand::thread_rng().gen_range::<usize, _, _>(0, 5)
//                    x = (x * Wrapping(6364136223846793005)) + Wrapping(1442695040888963407);
//                    xorshift32(&mut x);
//                    x ^= x << 13;
//                    x ^= x >> 17;
//                    x ^= x << 5;
//                }
//                println!("{}", std::str::from_utf8(&seq).unwrap());
//                for x in 0..10 {

//                    func(seq1.as_slice());
                    func(recordo.as_ref().unwrap_or_else(|| unsafe { unreachable_unchecked() }).as_slice());
//                }
//            }
        }
    }

    pub fn serialize_file(source: String, dest: String) {
        let mut serialized = flate2::write::GzEncoder::new(
            File::create(dest).unwrap(),flate2::Compression::best());

        Self::process_file(source, |record| {
            SingleDnaReadHalf::from_ascii(record).serialize(&mut serialized);
        });
    }

    pub fn count_entries(source: String) -> u64 {
        let mut records = 0;
        Self::process_file(source, |_record| {
            records += 1;
        });
        records
    }
}