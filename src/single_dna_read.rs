use serde::{Serialize, Deserialize, Serializer};
use crate::utils::Utils;
use std::ptr::{null_mut, null};
use std::alloc::Layout;
use std::slice::{from_raw_parts, from_raw_parts_mut};
use std::io::Write;
use byteorder::{WriteBytesExt, ByteOrder, BigEndian, LittleEndian};

const BUFFER_LEN: usize = 8000000000;

pub struct SingleDnaRead<'a> {
    data: &'a [u8],
    size: u32
}

static mut BUFFER: *mut u8 = null_mut();
static mut COUNT: usize = 0;

impl<'a> SingleDnaRead<'a> {
    pub fn from_ascii(data: &[u8]) -> SingleDnaRead {

        unsafe {
            if BUFFER == null_mut() {
                BUFFER = std::alloc::alloc(Layout::from_size_align(BUFFER_LEN, 32).unwrap());
            }
            COUNT = 0;
        }

        let buffer = unsafe { from_raw_parts_mut(BUFFER, BUFFER_LEN) };

        for byte_letters in data.chunks_exact(4) {
            unsafe {
                buffer[COUNT] = (Utils::pos_from_letter(byte_letters[0]) << 6) |
                    (Utils::pos_from_letter(byte_letters[1]) << 4) |
                    (Utils::pos_from_letter(byte_letters[2]) << 2) |
                    Utils::pos_from_letter(byte_letters[3]);
                COUNT += 1;
            }
        }

        let reminder = data.chunks_exact(4).remainder();

        if reminder.len() > 0 {
            unsafe {
                let mut value = 0;
                for x in reminder {
                    value = (value << 2) | *x;
                }
                buffer[COUNT] = value;
                COUNT += 1;
            }
        }

        let mut result = SingleDnaRead {
            data: unsafe { from_raw_parts(BUFFER, COUNT) },
            size: data.len() as u32
        };
        result
    }

    pub fn serialize<W: Write>(&self, stream: &mut W) {
        stream.write_u32::<LittleEndian>(self.size as u32);
        stream.write_all(self.data);
    }
}