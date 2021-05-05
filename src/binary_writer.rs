use crate::multi_thread_buckets::BucketType;
use byteorder::WriteBytesExt;
use std::fs::File;
use std::io::{BufWriter, Write};

pub enum StorageMode {
    Plain,
    LZ4Compression { level: u8 },
    GZIPCompression { level: u8 },
}

pub struct BinaryWriter {
    writer: Box<dyn Write>,
}

impl BinaryWriter {
    #[inline]
    pub fn get_writer(&mut self) -> &mut dyn Write {
        self.writer.as_mut()
    }
}

impl BucketType for BinaryWriter {
    type InitType = (String, StorageMode);

    fn new((name, mode): &(String, StorageMode), index: usize) -> Self {
        let name = format!("{}.{}", name, index);

        let writer = match mode {
            StorageMode::Plain => Box::new(BufWriter::with_capacity(
                1024 * 256,
                File::create(name).unwrap(),
            )),
            StorageMode::LZ4Compression { .. } => Box::new(BufWriter::with_capacity(
                1024 * 256,
                File::create(name).unwrap(),
            )),
            StorageMode::GZIPCompression { .. } => Box::new(BufWriter::with_capacity(
                1024 * 256,
                File::create(name).unwrap(),
            )),
        };

        Self { writer }
    }

    fn finalize(self) {}
}
