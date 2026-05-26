use std::{
    fs::{File, OpenOptions},
    io,
    path::Path,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

pub struct ConcurrentFileWriter {
    file: Arc<File>,
    next_offset: AtomicU64,
}

impl ConcurrentFileWriter {
    pub fn create(path: impl AsRef<Path>) -> io::Result<Self> {
        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .read(true)
            .truncate(true)
            .open(path)?;

        Ok(Self {
            file: Arc::new(file),
            next_offset: AtomicU64::new(0),
        })
    }

    pub fn append_existing(path: impl AsRef<Path>) -> io::Result<Self> {
        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .read(true)
            .open(path)?;

        let len = file.metadata()?.len();

        Ok(Self {
            file: Arc::new(file),
            next_offset: AtomicU64::new(len),
        })
    }

    /// Atomically reserves space, writes `buf`, and returns the starting offset.
    pub fn write(&self, buf: &[u8]) -> io::Result<u64> {
        let len = u64::try_from(buf.len())
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "buffer too large"))?;

        let offset = self.next_offset.fetch_add(len, Ordering::Relaxed);

        write_all_at(&self.file, buf, offset)?;

        Ok(offset)
    }

    /// Resizes `buf` to `len` and reads exactly `len` bytes from `offset`.
    ///
    /// Existing contents are overwritten from the start of the vector.
    pub fn read_all_at(&self, buf: &mut Vec<u8>, offset: u64, len: usize) -> io::Result<()> {
        buf.clear();
        buf.reserve(len);
        unsafe {
            buf.set_len(len);
        }

        let mut slice = buf.as_mut_slice();
        let mut offset = offset;

        while !slice.is_empty() {
            let read = read_at(&self.file, slice, offset)?;

            if read == 0 {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "failed to fill whole buffer",
                ));
            }

            let (_, rest) = slice.split_at_mut(read);
            slice = rest;
            offset += read as u64;
        }

        Ok(())
    }

    pub fn file(&self) -> &File {
        &self.file
    }
}

#[cfg(unix)]
fn write_all_at(file: &File, mut buf: &[u8], mut offset: u64) -> io::Result<()> {
    use std::os::unix::fs::FileExt;

    while !buf.is_empty() {
        let written = file.write_at(buf, offset)?;

        if written == 0 {
            return Err(io::Error::new(
                io::ErrorKind::WriteZero,
                "failed to write whole buffer",
            ));
        }

        buf = &buf[written..];
        offset += written as u64;
    }

    Ok(())
}

#[cfg(windows)]
fn write_all_at(file: &File, mut buf: &[u8], mut offset: u64) -> io::Result<()> {
    use std::os::windows::fs::FileExt;

    while !buf.is_empty() {
        let written = file.seek_write(buf, offset)?;

        if written == 0 {
            return Err(io::Error::new(
                io::ErrorKind::WriteZero,
                "failed to write whole buffer",
            ));
        }

        buf = &buf[written..];
        offset += written as u64;
    }

    Ok(())
}

#[cfg(unix)]
fn read_at(file: &File, buf: &mut [u8], offset: u64) -> io::Result<usize> {
    use std::os::unix::fs::FileExt;
    file.read_at(buf, offset)
}

#[cfg(windows)]
fn read_at(file: &File, buf: &mut [u8], offset: u64) -> io::Result<usize> {
    use std::os::windows::fs::FileExt;
    file.seek_read(buf, offset)
}
