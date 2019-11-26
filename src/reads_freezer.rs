use std::fs::{File, OpenOptions};
use std::io::{Write, BufReader, BufRead, BufWriter, Read};
use byteorder::WriteBytesExt;
use std::path::Path;
use os_pipe::{PipeReader, PipeWriter};
use std::hash::Hasher;
use std::cell::{Cell, UnsafeCell};
use std::thread;


pub struct ReadsFreezer {
    reader: UnsafeCell<Option<Box<dyn Read>>>,
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

pub struct ReadsWriter {
    writer: PipeWriter
}
impl ReadsWriter {
    pub fn add_read(&mut self, read: &[u8]) {
        self.writer.write_all(read).unwrap();
        self.writer.write_u8(b'\n').unwrap();
    }
    pub fn pipe_freezer(&mut self, freezer: ReadsFreezer) {
        std::io::copy(freezer.reader.uget().as_mut().unwrap(), &mut self.writer).unwrap();
    }
}


impl ReadsFreezer {
    pub fn from_generator<F: 'static + FnOnce(&mut ReadsWriter) + Send>(func: F) -> ReadsFreezer {
        let (reader, writer) = os_pipe::pipe().unwrap();

        let reader_box = Box::new(reader);

        thread::spawn(move || {
           func(&mut ReadsWriter { writer } );
        });

        ReadsFreezer {
            reader: UnsafeCell::new(Some(reader_box))
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
            reader: UnsafeCell::new(Some(reader)),
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
        let freezer: &mut dyn Write =
            if compress {
                compressed = Some(lz4::EncoderBuilder::new().build(file).unwrap());
                compressed.as_mut().unwrap()
            }
            else {
                uncompressed = Some(file);
                uncompressed.as_mut().unwrap()
            };


        std::io::copy(self.reader.uget().as_mut().unwrap(), freezer).unwrap();
        if let Some(mut stream) = compressed {
            let (file, result) = stream.finish();
        }
    }

    pub fn for_each<F: FnMut(&[u8])>(&self, mut func: F) {
        let mut reader_cell = self.reader.uget().as_mut().take().unwrap();
        let reader = BufReader::new(reader_cell);
        for line in reader.lines() {
            func(line.unwrap().as_bytes());
        }
    }
}