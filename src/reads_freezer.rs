use std::fs::{File, OpenOptions};
use std::io::{Write, BufReader, BufRead, BufWriter, Read};
use byteorder::WriteBytesExt;
use std::path::Path;
use os_pipe::{PipeReader, PipeWriter};
use std::hash::Hasher;
use std::cell::{Cell, UnsafeCell};
use std::thread;
use crate::utils::{cast_static, cast_static_mut, Utils};
use std::ops::DerefMut;


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
        unsafe {
            &mut *self.get()
        }
    }
}

struct RefThreadWrapper<'a, T: ?Sized>(&'a mut T);
unsafe impl<'a, T: ?Sized> Sync for RefThreadWrapper<'a, T> {}
unsafe impl<'a, T: ?Sized> Send for RefThreadWrapper<'a, T> {}

pub enum WriterChannels {
    Pipe(PipeWriter),
    File(BufWriter<File>)
}

pub struct ReadsWriter {
    writer: WriterChannels
}
impl ReadsWriter {
    pub fn add_read(&mut self, read: &[u8]) {
        match self.writer {
            WriterChannels::Pipe(ref mut writer) => {
                writer.write_all(read).unwrap();
                writer.write_u8(b'\n').unwrap();
            },
            WriterChannels::File(ref mut writer) => {
                writer.write_all(read).unwrap();
                writer.write_u8(b'\n').unwrap();
            },
        }
    }
    pub fn pipe_freezer(&mut self, mut freezer: ReadsFreezer) {
        match self.writer {
            WriterChannels::Pipe(ref mut writer) => {
                std::io::copy(&mut freezer.reader.uget(), writer).unwrap();
            },
            WriterChannels::File(ref mut writer) => {
                std::io::copy(&mut freezer.reader.uget(), writer).unwrap();
            }
        }
    }
}

impl ReadsFreezer {
    pub fn from_generator<F: 'static + FnOnce(&mut ReadsWriter) + Send>(func: F) -> ReadsFreezer {
        let (reader, writer) = os_pipe::pipe().unwrap();

        Utils::thread_safespawn(move || {
           func(&mut ReadsWriter { writer: WriterChannels::Pipe(writer) } );
        });

        ReadsFreezer {
            reader: UnsafeCell::new(Box::new(reader))
        }
    }

    pub fn new_splitted() -> (ReadsFreezer, ReadsWriter) {
        let (reader, writer) = os_pipe::pipe().unwrap();

        (ReadsFreezer {
            reader: UnsafeCell::new(Box::new(reader))
        }, ReadsWriter { writer: WriterChannels::Pipe(writer) })
    }

    pub fn optifile_splitted(name: String) -> ReadsWriter {
        let file = name + ".freeze";
        ReadsWriter {
            writer: WriterChannels::File(BufWriter::with_capacity(1024 * 1024 * 4, File::create(file).unwrap()))
        }
    }

    pub fn from_file(name: String) -> ReadsFreezer {

        let is_compressed = name.ends_with(".lz4");
        let file = File::open(name).unwrap();

        let reader: Box<dyn Read> =
            if is_compressed {
                let decoder = lz4::Decoder::new(file).unwrap();
                Box::new(decoder)
            }
            else {
                Box::new(file)
            };
        ReadsFreezer {
            reader: UnsafeCell::new(reader),
        }
    }

    pub fn freeze(&self, name: String, compress: bool) {
        let file = File::create(
            if compress {
                name + ".freeze.lz4"
            } else {
                name + ".freeze"
            }).unwrap();

        let mut uncompressed = None;
        let mut compressed = None;

        let mut value: &mut dyn Read = self.reader.uget().as_mut();

        let reader_ref = RefThreadWrapper(cast_static_mut(value));

        Utils::thread_safespawn(move || {
            let freezer: &mut dyn Write =
                if compress {
                    compressed = Some(lz4::EncoderBuilder::new().build(file).unwrap());
                    compressed.as_mut().unwrap()
                }
                else {
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