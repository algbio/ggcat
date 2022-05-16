use std::cmp::min;
use std::io::Read;

pub struct VecReader<R: Read> {
    vec: Vec<u8>,
    fill: usize,
    pos: usize,
    reader: R,
    stream_ended: bool,
}

impl<R: Read> VecReader<R> {
    pub fn new(capacity: usize, reader: R) -> VecReader<R> {
        let mut vec = Vec::with_capacity(capacity);
        unsafe {
            vec.set_len(capacity);
        }
        VecReader {
            vec,
            fill: 0,
            pos: 0,
            reader,
            stream_ended: false,
        }
    }

    fn update_buffer(&mut self) {
        self.fill = match self.reader.read(&mut self.vec[..]) {
            Ok(fill) => fill,
            Err(_) => 0,
        };
        self.stream_ended = self.fill == 0;
        self.pos = 0;
    }

    #[inline]
    pub fn read_bytes(&mut self, slice: &mut [u8]) -> usize {
        let mut offset = 0;

        while offset < slice.len() {
            if self.fill == self.pos {
                self.update_buffer();

                if self.fill == self.pos {
                    return offset;
                }
            }

            let amount = min(slice.len() - offset, self.fill - self.pos);

            unsafe {
                std::ptr::copy(
                    self.vec.as_ptr().add(self.pos),
                    slice.as_mut_ptr().add(offset),
                    amount,
                );
            }

            self.pos += amount;
            offset += amount;
        }
        offset
    }

    pub fn into_inner(self) -> R {
        self.reader
    }
}

impl<R: Read> Read for VecReader<R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        Ok(self.read_bytes(buf))
    }

    fn read_exact(&mut self, buf: &mut [u8]) -> std::io::Result<()> {
        Ok(self.read_bytes(buf)).map(|_| ())
    }
}
