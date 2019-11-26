use std::fs::File;
use std::io::{Write, BufReader, BufRead, BufWriter};
use byteorder::WriteBytesExt;

pub struct ReadsFreezer {
    encoder: Option<BufWriter<File>>,
    decoder: Option<BufReader<File>>,
}

impl ReadsFreezer {
    pub fn create(name: String) -> ReadsFreezer {
        let file = BufWriter::with_capacity(1024*1024*16, File::create(name).unwrap());
//        let encoder = lz4::EncoderBuilder::new().level(1).build(file).unwrap();
        ReadsFreezer {
            encoder: Some(file),
            decoder: None
        }
    }

    pub fn open(name: String) -> ReadsFreezer {
        let file = File::open(name).unwrap();
        let decoder = BufReader::with_capacity(1024*1024*16, file);//lz4::Decoder::new(file).unwrap();
        ReadsFreezer {
            encoder: None,
            decoder: Some(decoder)
        }
    }

    pub fn add_read(&mut self, read: &[u8]) {
        self.encoder.as_mut().unwrap().write_all(read);
        self.encoder.as_mut().unwrap().write_u8(b'\n');
    }

    pub fn for_each<F: FnMut(&[u8])>(&mut self, mut func: F) {
        let reader = BufReader::new(self.decoder.take().unwrap());
        for line in reader.lines() {
            func(line.unwrap().as_bytes());
        }
    }
}