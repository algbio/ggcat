use std::io::{BufReader, BufWriter, Write, Error, IoSlice, Read};
use std::fs::File;
use bio::io::fastq;
use crate::utils::Utils;
use crate::single_dna_read::SingleDnaRead;
use serde::export::fmt::Arguments;
use serde::Serialize;
use std::process::{exit, Stdio};
use std::process::Command;

const BUFFER_SIZE: usize = 1024 * 1024 * 32;

pub struct BinarySerializer;
impl BinarySerializer {

    pub fn serialize_file(source: String, dest: String) -> u64 {

//        let mut compressed = File::open(source.as_str()).unwrap();
//        let mut decompress = Decoder::new(&vec[..]).unwrap();
        let process = Command::new("gzip").args(&["-cd", source.as_str()]).stdout(Stdio::piped()).spawn().unwrap();
        let mut decompress = process.stdout.unwrap();

//File::open("D1_S1_L001_R1_001.fastq").unwrap();//
        let mut serialized = flate2::read::GzEncoder::new(
            File::create(dest).unwrap(),flate2::Compression::best());


//    copy(&mut decompress, &mut File::create("D3_S1_L002_R1_014.fastq").unwrap());
//    let flat = File::open("D3_S1_L002_R1_014.fastq").unwrap();

        let reader = fastq::Reader::new(decompress);
        let mut records = 0;
        for record in reader.records() {
//        println!("Hello, world! {:?}",
            let record = record.unwrap();
            records += 1;
            SingleDnaRead::from_ascii(record.seq()).serialize(&mut serialized);
        }
        records
    }


}