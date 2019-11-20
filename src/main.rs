use bio::io::fastq;
use flate2::read::{GzDecoder, GzEncoder};
use std::fs::File;
use std::io::{Write, copy, BufReader};
use std::env::args;
use stopwatch::Stopwatch;
use std::path::Path;
use flate2::{GzBuilder, Compression};
use crate::binary_serializer::BinarySerializer;

#[macro_use]
extern crate lazy_static;

mod binary_serializer;
mod single_dna_read;
mod utils;

fn main() {

    let mut stopwatch = Stopwatch::new();
    stopwatch.start();

    let mut records = 0u64;
    let mut gb = 0u64;

    let mut freqs:[u64; 4] = [0; 4];

    for file in args().skip(1) {
        println!("Reading {}", file);
        records += BinarySerializer::serialize_file(file.clone(), format!("{}.bin", file));
//        println!("Compressed to {}", file);
        gb += File::open(file.as_str()).unwrap().metadata().unwrap().len();
    }



    stopwatch.stop();
    println!("Elapsed {} seconds, read {} records and {} gb! => freqs {:?}", stopwatch.elapsed().as_secs_f64(), records, (gb as f64) / 1024.0 / 1024.0 / 1024.0, freqs);
}
