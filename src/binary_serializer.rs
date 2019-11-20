use std::io::{BufReader, BufWriter, Write, Error, IoSlice};
use flate2::read::GzDecoder;
use flate2::write::GzEncoder;
use std::fs::File;
use flate2::Compression;
use bio::io::fastq;
use crate::utils::Utils;
use crate::single_dna_read::SingleDnaRead;
use serde::export::fmt::Arguments;
use serde::Serialize;

const BUFFER_SIZE: usize = 1024 * 1024 * 128;

pub struct BinarySerializer;
impl BinarySerializer {

    pub fn serialize_file(source: String, dest: String) -> u64 {
        let compressed = BufReader::with_capacity(BUFFER_SIZE, File::open(source.as_str()).unwrap());
        let mut decompress = GzDecoder::new(compressed);

        let mut serialized = BufWriter::with_capacity(BUFFER_SIZE, GzEncoder::new(File::create(dest).unwrap(), Compression::best()));


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