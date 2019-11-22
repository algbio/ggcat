
use std::fs::File;
use bio::io::fastq;


use crate::single_dna_read_half::SingleDnaReadHalf;

use serde::Serialize;
use std::process::{Stdio};
use std::process::Command;
use rand::Rng;
use std::num::Wrapping;

const BUFFER_SIZE: usize = 1024 * 1024 * 32;

pub struct BinarySerializer;
impl BinarySerializer {

    pub fn process_file<F: FnMut(&[u8])>(source: String, mut func: F) {

        // /home/andrea/Desktop/pigz-2.4/unpigz
        let process = Command::new("gzip").args(&["-cd", source.as_str()]).stdout(Stdio::piped()).spawn().unwrap();
        let decompress = process.stdout.unwrap();

        let reader = fastq::Reader::new(decompress);
        let _records = 0;
        let bases = [b'A', b'C', b'G', b'T', b'N'];
        let mut x = Wrapping(2847386477usize);
        for record in reader.records() {
//            let mut seq = [0; 100];
//            loop {
                let record = record.as_ref().unwrap();
//                for el in seq.iter_mut() {
//                    *el = bases[x.0 % 5]; //rand::thread_rng().gen_range::<usize, _, _>(0, 5)
//                    x = (x * Wrapping(6364136223846793005)) + Wrapping(1442695040888963407);
//                }
//                println!("{}", std::str::from_utf8(&seq).unwrap());
                func(record.seq());
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