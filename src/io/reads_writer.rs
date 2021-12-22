use crate::io::sequences_reader::FastaSequence;
use byteorder::WriteBytesExt;
use flate2::write::GzEncoder;
use flate2::Compression;
use lz4::{BlockMode, BlockSize, ContentChecksum};
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};

enum WriterChannels {
    None,
    File(BufWriter<File>),
    CompressedFileGzip(BufWriter<GzEncoder<BufWriter<File>>>),
    CompressedFileLZ4(BufWriter<lz4::Encoder<BufWriter<File>>>),
}

impl WriterChannels {
    fn get_writer(&mut self) -> &mut dyn Write {
        match self {
            WriterChannels::File(x) => x,
            WriterChannels::CompressedFileGzip(x) => x,
            WriterChannels::CompressedFileLZ4(x) => x,
            WriterChannels::None => unreachable!(),
        }
    }
}

pub struct ReadsWriter {
    writer: WriterChannels,
    path: PathBuf,
    reads_count: usize,
}

impl ReadsWriter {
    pub fn new_compressed_gzip(path: impl AsRef<Path>, level: u32) -> ReadsWriter {
        let mut compress_stream = GzEncoder::new(
            BufWriter::with_capacity(1024 * 1024 * 16, File::create(&path).unwrap()),
            Compression::new(level),
        );

        ReadsWriter {
            writer: WriterChannels::CompressedFileGzip(BufWriter::with_capacity(
                1024 * 1024,
                compress_stream,
            )),
            path: path.as_ref().to_path_buf(),
            reads_count: 0,
        }
    }

    pub fn new_compressed_lz4(path: impl AsRef<Path>, level: u32) -> ReadsWriter {
        let mut compress_stream = lz4::EncoderBuilder::new()
            .level(level)
            .checksum(ContentChecksum::NoChecksum)
            .block_mode(BlockMode::Linked)
            .block_size(BlockSize::Max1MB)
            .build(BufWriter::with_capacity(
                1024 * 1024 * 8,
                File::create(&path).unwrap(),
            ))
            .unwrap();

        ReadsWriter {
            writer: WriterChannels::CompressedFileLZ4(BufWriter::with_capacity(
                1024 * 1024 * 8,
                compress_stream,
            )),
            path: path.as_ref().to_path_buf(),
            reads_count: 0,
        }
    }

    pub fn new_plain(path: impl AsRef<Path>) -> ReadsWriter {
        ReadsWriter {
            writer: WriterChannels::File(BufWriter::with_capacity(
                1024 * 128,
                File::create(&path).unwrap(),
            )),
            path: path.as_ref().to_path_buf(),
            reads_count: 0,
        }
    }

    pub fn get_reads_count(&mut self) -> usize {
        self.reads_count
    }

    pub fn add_read(&mut self, read: FastaSequence) {
        let writer = self.writer.get_writer();
        writer.write_all(read.ident);
        writer.write_all(b"\n");
        writer.write_all(read.seq).unwrap();
        if let Some(qual) = read.qual {
            writer.write_all(b"\n+\n").unwrap();
            writer.write_all(qual).unwrap();
        }
        writer.write_u8(b'\n').unwrap();

        self.reads_count += 1;
    }

    pub fn get_path(&self) -> PathBuf {
        self.path.clone()
    }

    pub fn finalize(self) {}
}

impl Drop for ReadsWriter {
    fn drop(&mut self) {
        let writer = std::mem::replace(&mut self.writer, WriterChannels::None);
        match writer {
            WriterChannels::File(mut writer) => {}
            WriterChannels::CompressedFileGzip(mut writer) => {
                writer.flush();
                writer
                    .into_inner()
                    .unwrap_or_else(|_| panic!("Cannot unwrap!"))
                    .finish()
                    .unwrap_or_else(|_| panic!("Cannot unwrap!"))
                    .flush()
                    .unwrap();
            }
            WriterChannels::CompressedFileLZ4(mut writer) => {
                writer.flush();
                writer
                    .into_inner()
                    .unwrap_or_else(|_| panic!("Cannot unwrap!"))
                    .finish()
                    .0
                    .flush()
                    .unwrap();
            }
            WriterChannels::None => unreachable!(),
        }
    }
}
