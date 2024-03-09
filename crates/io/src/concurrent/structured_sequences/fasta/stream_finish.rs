use flate2::write::GzEncoder;
use std::{
    fmt::Debug,
    fs::File,
    io::{BufWriter, Write},
};

pub(crate) trait FastaFileFinish: Write + Debug {
    fn finalize(self);
}
impl<W: FastaFileFinish> FastaFileFinish for BufWriter<W> {
    fn finalize(self) {
        self.into_inner().unwrap().finalize();
    }
}
impl FastaFileFinish for File {
    fn finalize(mut self) {
        self.flush().unwrap();
    }
}
impl<W: FastaFileFinish> FastaFileFinish for lz4::Encoder<W> {
    fn finalize(self) {
        let (w, err) = self.finish();
        err.unwrap();
        w.finalize();
    }
}
impl<W: FastaFileFinish> FastaFileFinish for GzEncoder<W> {
    fn finalize(self) {
        let w = self.finish().unwrap();
        w.finalize();
    }
}

pub(crate) struct FastaWriterWrapper<W: FastaFileFinish> {
    writer: Option<W>,
}

impl<W: FastaFileFinish> FastaWriterWrapper<W> {
    pub fn new(writer: W) -> Self {
        Self {
            writer: Some(writer),
        }
    }
}

impl<W: FastaFileFinish> Write for FastaWriterWrapper<W> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        unsafe { self.writer.as_mut().unwrap_unchecked() }.write(buf)
    }

    fn write_all(&mut self, buf: &[u8]) -> std::io::Result<()> {
        unsafe { self.writer.as_mut().unwrap_unchecked() }.write_all(buf)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        unsafe { self.writer.as_mut().unwrap_unchecked() }.flush()
    }
}

impl<W: FastaFileFinish> Drop for FastaWriterWrapper<W> {
    fn drop(&mut self) {
        self.writer.take().unwrap().finalize();
    }
}
