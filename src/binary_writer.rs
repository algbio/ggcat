use crate::multi_thread_buckets::BucketType;
use byteorder::WriteBytesExt;
use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Write};
use std::path::PathBuf;

pub enum StorageMode {
    AppendOrCreate,
    Plain,
    LZ4Compression { level: u8 },
    GZIPCompression { level: u8 },
}

pub struct BinaryWriter {
    writer: Box<dyn Write>,
    path: PathBuf,
}

impl BinaryWriter {
    #[inline]
    pub fn get_writer(&mut self) -> &mut dyn Write {
        self.writer.as_mut()
    }
}

impl BucketType for BinaryWriter {
    type InitType = (PathBuf, StorageMode);

    fn new((name, mode): &(PathBuf, StorageMode), index: usize) -> Self {
        let path = name.parent().unwrap().join(format!(
            "{}.{}",
            name.file_name().unwrap().to_str().unwrap(),
            index
        ));

        let writer = match mode {
            StorageMode::AppendOrCreate => Box::new(BufWriter::with_capacity(
                1024 * 256,
                OpenOptions::new()
                    .append(true)
                    .create(true)
                    .open(&path)
                    .unwrap(),
            )),
            StorageMode::Plain => Box::new(BufWriter::with_capacity(
                1024 * 256,
                File::create(&path).unwrap(),
            )),
            StorageMode::LZ4Compression { .. } => Box::new(BufWriter::with_capacity(
                1024 * 256,
                File::create(&path).unwrap(),
            )),
            StorageMode::GZIPCompression { .. } => Box::new(BufWriter::with_capacity(
                1024 * 256,
                File::create(&path).unwrap(),
            )),
        };

        Self { writer, path }
    }

    fn get_path(&self) -> PathBuf {
        self.path.clone()
    }

    fn finalize(self) {}
}
