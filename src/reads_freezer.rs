use crate::sequences_reader::FastaSequence;
use crate::utils::{cast_static, cast_static_mut, Utils};
use crate::vec_slice::VecSlice;
use byteorder::WriteBytesExt;
use flate2::write::GzEncoder;
use flate2::Compression;
use lz4::{BlockMode, BlockSize, ContentChecksum};
use os_pipe::{PipeReader, PipeWriter};
use parking_lot::Mutex;
use std::cell::{Cell, UnsafeCell};
use std::fs::{File, OpenOptions};
use std::hash::Hasher;
use std::io::{BufRead, BufReader, BufWriter, Read, Write};
use std::ops::DerefMut;
use std::path::{Path, PathBuf};
use std::process::{ChildStdin, Command, Stdio};
use std::thread;

pub struct ReadsFreezer {
    reader: UnsafeCell<Box<dyn Read>>,
}

unsafe impl Send for ReadsFreezer {}
unsafe impl Sync for ReadsFreezer {}

trait UnsafeCellGetMutable {
    type Output;
    fn uget(&self) -> &mut Self::Output;
}

impl<T> UnsafeCellGetMutable for UnsafeCell<T> {
    type Output = T;

    fn uget(&self) -> &mut Self::Output {
        unsafe { &mut *self.get() }
    }
}

struct RefThreadWrapper<'a, T: ?Sized>(&'a mut T);
unsafe impl<'a, T: ?Sized> Sync for RefThreadWrapper<'a, T> {}
unsafe impl<'a, T: ?Sized> Send for RefThreadWrapper<'a, T> {}

pub enum WriterChannels {
    None,
    Pipe(PipeWriter),
    File(BufWriter<File>),
    CompressedFile(BufWriter<GzEncoder<BufWriter<File>>>),
    CompressedFileLZ4(BufWriter<lz4::Encoder<BufWriter<File>>>),
}

impl WriterChannels {
    fn get_writer(&mut self) -> &mut dyn Write {
        match self {
            WriterChannels::Pipe(x) => x,
            WriterChannels::File(x) => x,
            WriterChannels::CompressedFile(x) => x,
            WriterChannels::CompressedFileLZ4(x) => x,
            WriterChannels::None => unreachable!(),
        }
    }
}

pub struct FastaWriterConcurrentBuffer<'a> {
    target: &'a Mutex<ReadsWriter>,
    sequences: Vec<(VecSlice<u8>, VecSlice<u8>, Option<VecSlice<u8>>)>,
    ident_buf: Vec<u8>,
    seq_buf: Vec<u8>,
    qual_buf: Vec<u8>,
}

impl<'a> FastaWriterConcurrentBuffer<'a> {
    pub fn new(target: &'a Mutex<ReadsWriter>, max_size: usize) -> Self {
        Self {
            target,
            sequences: Vec::with_capacity(max_size / 128),
            ident_buf: Vec::with_capacity(max_size),
            seq_buf: Vec::with_capacity(max_size),
            qual_buf: Vec::new(),
        }
    }

    fn flush(&mut self) -> usize {
        let mut buffer = self.target.lock();

        let first_read_index = buffer.get_reads_count();

        for (ident, seq, qual) in self.sequences.iter() {
            buffer.add_read(FastaSequence {
                ident: ident.get_slice(&self.ident_buf),
                seq: seq.get_slice(&self.seq_buf),
                qual: qual.as_ref().map(|qual| qual.get_slice(&self.qual_buf)),
            })
        }
        drop(buffer);
        self.sequences.clear();
        self.ident_buf.clear();
        self.seq_buf.clear();
        self.qual_buf.clear();

        first_read_index
    }

    #[inline(always)]
    fn will_overflow(vec: &Vec<u8>, len: usize) -> bool {
        vec.len() > 0 && (vec.len() + len > vec.capacity())
    }

    pub fn add_read(&mut self, read: FastaSequence) -> Option<usize> {
        let mut result = None;

        if Self::will_overflow(&self.ident_buf, read.ident.len())
            || Self::will_overflow(&self.seq_buf, read.seq.len())
            || match read.qual {
                None => false,
                Some(qual) => Self::will_overflow(&self.qual_buf, qual.len()),
            }
        {
            result = Some(self.flush());
        }
        let qual = read
            .qual
            .map(|qual| VecSlice::new_extend(&mut self.qual_buf, qual));

        self.sequences.push((
            VecSlice::new_extend(&mut self.ident_buf, read.ident),
            VecSlice::new_extend(&mut self.seq_buf, read.seq),
            qual,
        ));

        result
    }

    pub fn finalize(mut self) -> usize {
        self.flush()
    }
}

impl Drop for FastaWriterConcurrentBuffer<'_> {
    fn drop(&mut self) {
        self.flush();
    }
}

pub struct ReadsWriter {
    writer: WriterChannels,
    path: PathBuf,
    reads_count: usize,
}

impl ReadsWriter {
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
    pub fn pipe_freezer(&mut self, mut freezer: ReadsFreezer) {
        let writer = self.writer.get_writer();
        std::io::copy(&mut freezer.reader.uget(), writer).unwrap();
    }

    pub fn get_path(&self) -> PathBuf {
        self.path.clone()
    }

    pub fn finalize(self) {}
}

impl ReadsFreezer {
    pub fn from_generator<F: 'static + FnOnce(&mut ReadsWriter) + Send>(func: F) -> ReadsFreezer {
        let (reader, writer) = os_pipe::pipe().unwrap();

        Utils::thread_safespawn(move || {
            func(&mut ReadsWriter {
                writer: WriterChannels::Pipe(writer),
                path: PathBuf::new(),
                reads_count: 0,
            });
        });

        ReadsFreezer {
            reader: UnsafeCell::new(Box::new(reader)),
        }
    }

    pub fn new_splitted() -> (ReadsFreezer, ReadsWriter) {
        let (reader, writer) = os_pipe::pipe().unwrap();

        (
            ReadsFreezer {
                reader: UnsafeCell::new(Box::new(reader)),
            },
            ReadsWriter {
                writer: WriterChannels::Pipe(writer),
                path: PathBuf::new(),
                reads_count: 0,
            },
        )
    }

    pub fn optfile_splitted_compressed(path: impl AsRef<Path>) -> ReadsWriter {
        let mut compress_stream = GzEncoder::new(
            BufWriter::with_capacity(1024 * 1024 * 16, File::create(&path).unwrap()),
            Compression::new(2),
        );

        ReadsWriter {
            writer: WriterChannels::CompressedFile(BufWriter::with_capacity(
                1024 * 1024,
                compress_stream,
            )),
            path: path.as_ref().to_path_buf(),
            reads_count: 0,
        }
    }

    pub fn optfile_splitted_compressed_lz4(path: impl AsRef<Path>) -> ReadsWriter {
        let mut compress_stream = lz4::EncoderBuilder::new()
            .level(2)
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

    pub fn optifile_splitted(path: impl AsRef<Path>) -> ReadsWriter {
        ReadsWriter {
            writer: WriterChannels::File(BufWriter::with_capacity(
                1024 * 128,
                File::create(&path).unwrap(),
            )),
            path: path.as_ref().to_path_buf(),
            reads_count: 0,
        }
    }

    pub fn from_file(name: String) -> ReadsFreezer {
        let is_compressed = name.ends_with(".lz4");
        let file = File::open(name).unwrap();

        let reader: Box<dyn Read> = if is_compressed {
            let decoder = lz4::Decoder::new(file).unwrap();
            Box::new(decoder)
        } else {
            Box::new(file)
        };
        ReadsFreezer {
            reader: UnsafeCell::new(reader),
        }
    }

    pub fn freeze(&self, name: String, compress: bool) {
        let file = File::create(if compress {
            name + ".freeze.fa.lz4"
        } else {
            name + ".freeze.fa"
        })
        .unwrap();

        let mut uncompressed = None;
        let mut compressed = None;

        let mut value: &mut dyn Read = self.reader.uget().as_mut();

        let reader_ref = RefThreadWrapper(cast_static_mut(value));

        Utils::thread_safespawn(move || {
            let freezer: &mut dyn Write = if compress {
                compressed = Some(lz4::EncoderBuilder::new().build(file).unwrap());
                compressed.as_mut().unwrap()
            } else {
                uncompressed = Some(file);
                uncompressed.as_mut().unwrap()
            };

            std::io::copy(reader_ref.0, freezer).unwrap();
            if let Some(mut stream) = compressed {
                let (file, result) = stream.finish();
            }
        });
    }

    pub fn for_each<F: FnMut(&[u8])>(&self, mut func: F) {
        let mut reader_cell = self.reader.uget();
        let reader = BufReader::new(reader_cell);
        for line in reader.lines() {
            func(line.unwrap().as_bytes());
        }
    }
}

impl Drop for ReadsWriter {
    fn drop(&mut self) {
        let writer = std::mem::replace(&mut self.writer, WriterChannels::None);
        match writer {
            WriterChannels::Pipe(mut writer) => {}
            WriterChannels::File(mut writer) => {}
            WriterChannels::CompressedFile(mut writer) => {}
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
