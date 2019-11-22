
use crate::utils::Utils;
use std::ptr::{null_mut};
use std::alloc::Layout;
use std::slice::{from_raw_parts, from_raw_parts_mut};
use std::io::{Write, Read};
use byteorder::{WriteBytesExt, ByteOrder, LittleEndian, ReadBytesExt};

const BUFFER_LEN: usize = 8000000000;

pub struct SingleDnaReadHalf<'a> {
    data: &'a [u8],
    size: u32
}

static mut BUFFER: *mut u8 = null_mut();
static mut COUNT: usize = 0;

impl<'a> SingleDnaReadHalf<'a> {

    fn init_buffer() {
        unsafe {
            if BUFFER == null_mut() {
                BUFFER = std::alloc::alloc(Layout::from_size_align(BUFFER_LEN, 32).unwrap());
            }
            COUNT = 0;
        }
    }

    pub fn from_ascii(data: &[u8]) -> SingleDnaReadHalf {

        Self::init_buffer();

        let buffer = unsafe { from_raw_parts_mut(BUFFER, BUFFER_LEN) };

        for byte_letters in data.chunks_exact(2) {
            unsafe {
                buffer[COUNT] = (Utils::pos_from_letter(byte_letters[0]) << 4) |
                    Utils::pos_from_letter(byte_letters[1]);
                COUNT += 1;
            }
        }

        let reminder = data.chunks_exact(2).remainder();

        if reminder.len() > 0 {
            unsafe {
                let mut value = 0;
                for x in reminder {
                    value = (value << 4) | *x;
                }
                buffer[COUNT] = value;
                COUNT += 1;
            }
        }

        let result = SingleDnaReadHalf {
            data: unsafe { from_raw_parts(BUFFER, COUNT) },
            size: data.len() as u32
        };
        result
    }

    pub fn serialize<W: Write>(&self, stream: &mut W) {
        stream.write_u32::<LittleEndian>(self.size as u32);
        stream.write_all(self.data);
    }

    pub fn from_stream<R: Read>(stream: &mut R) -> Result<SingleDnaReadHalf<'static>, std::io::Error> {
        let mut result = SingleDnaReadHalf {
            data: &[],
            size: stream.read_u32::<LittleEndian>()?
        };

        Self::init_buffer();

        let buffer = unsafe { from_raw_parts_mut(BUFFER, BUFFER_LEN) };

        let bytes_count = ((result.size + 3) / 4) as usize;

        unsafe {
            stream.read_exact(&mut buffer[0..bytes_count])?;
            result.data = &mut buffer[0..bytes_count];
        }
        Ok(result)
    }

//    pub fn iter() -> SingleDnaIterator {
//
//    }
}

//struct SingleDnaIterator<'a>(&'a SingleDnaReadHalf<'a>, usize);
//
//impl<'a> Iterator for SingleDnaIterator<'a> {
//    type Item = u8;
//
//    fn next(&mut self) -> Option<Self::Item> {
//        if self.0.size >= (self.1 as u32) {
//            None
//        }
//        else {
//            let bucket = self.1 / 4;
//            let shift = (self.1 % 4) * 2;
//            self.1 += 1;
//            Some((self.0.data[bucket] >> (shift as u8)) & 0b11)
//        }
//    }
//}