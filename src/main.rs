use bio::io::fastq;
use flate2::read::GzDecoder;
use std::fs::File;
use std::io::{Write, copy, BufReader};
use std::env::args;
use stopwatch::Stopwatch;

const BUFFER_SIZE: usize = 1024 * 1024 * 128;

fn main() {

    let mut stopwatch = Stopwatch::new();
    stopwatch.start();

    let mut records = 0u64;
    let mut gb = 0u64;

    for file in args().skip(1) {
        let compressed = BufReader::with_capacity(BUFFER_SIZE, File::open(file.as_str()).unwrap());
        let mut decompress = GzDecoder::new(compressed);
        println!("Reading {}", file);
//    copy(&mut decompress, &mut File::create("D3_S1_L002_R1_014.fastq").unwrap());
//    let flat = File::open("D3_S1_L002_R1_014.fastq").unwrap();

        let reader = fastq::Reader::new(decompress);
        for record in reader.records() {
//        println!("Hello, world! {:?}",
            record.unwrap().to_string();//);
            records += 1;
        }
        gb += File::open(file.as_str()).unwrap().metadata().unwrap().len();
    }

    stopwatch.stop();
    println!("Elapsed {} seconds, read {} records and {} gb !", stopwatch.elapsed().as_secs_f64(), records, (gb as f64) / 1024.0 / 1024.0 / 1024.0);
}
