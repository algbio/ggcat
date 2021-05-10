use crate::sequences_reader::FastaSequence;
use crate::utils::{cast_static, cast_static_mut, Utils};
use byteorder::WriteBytesExt;
use flate2::write::GzEncoder;
use flate2::Compression;
use lz4::{BlockMode, BlockSize, ContentChecksum};
use os_pipe::{PipeReader, PipeWriter};
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
        }
    }
}

pub struct ReadsWriter {
    writer: WriterChannels,
    path: PathBuf,
}

impl ReadsWriter {
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
    }
    pub fn pipe_freezer(&mut self, mut freezer: ReadsFreezer) {
        let writer = self.writer.get_writer();
        std::io::copy(&mut freezer.reader.uget(), writer).unwrap();
    }

    pub fn get_path(&self) -> PathBuf {
        self.path.clone()
    }

    pub fn finalize(mut self) {
        match self.writer {
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
        }
    }
}

impl ReadsFreezer {
    pub fn from_generator<F: 'static + FnOnce(&mut ReadsWriter) + Send>(func: F) -> ReadsFreezer {
        let (reader, writer) = os_pipe::pipe().unwrap();

        Utils::thread_safespawn(move || {
            func(&mut ReadsWriter {
                writer: WriterChannels::Pipe(writer),
                path: PathBuf::new(),
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
            },
        )
    }

    pub fn optfile_splitted_compressed(name: String) -> ReadsWriter {
        let path = PathBuf::from(name + ".freeze.fa.gz");

        let mut compress_stream = GzEncoder::new(
            BufWriter::with_capacity(1024 * 1024 * 16, File::create(&path).unwrap()),
            Compression::new(2),
        );

        ReadsWriter {
            writer: WriterChannels::CompressedFile(BufWriter::with_capacity(
                1024 * 1024,
                compress_stream,
            )),
            path,
        }
    }

    pub fn optfile_splitted_compressed_lz4(name: impl AsRef<Path>) -> ReadsWriter {
        let path = PathBuf::from(name.as_ref().parent().unwrap()).join(
            name.as_ref()
                .file_name()
                .unwrap()
                .to_str()
                .unwrap()
                .to_string()
                + ".fa.lz4",
        );

        //        let mut process = Command::new("./libdeflate/gzip").args(&["-c2"])
        //            .stdin(Stdio::piped())
        //            .stdout(Stdio::from(File::create(file).unwrap())).spawn().unwrap();
        //
        //        let compress_stream = process.stdin.unwrap();
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
            path,
        }
    }

    pub fn optifile_splitted(name: String) -> ReadsWriter {
        let path = PathBuf::from(name + ".freeze.fa");
        ReadsWriter {
            writer: WriterChannels::File(BufWriter::with_capacity(
                1024 * 128,
                File::create(&path).unwrap(),
            )),
            path,
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
