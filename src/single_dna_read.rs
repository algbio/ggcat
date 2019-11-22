
use crate::utils::Utils;



use std::io::{Write, Read};
use byteorder::{WriteBytesExt, ByteOrder, BigEndian, LittleEndian, ReadBytesExt};

const BUFFER_LEN: usize = 8000000000;

pub struct SingleDnaRead {
    data: Vec<u8>,
}

impl SingleDnaRead {

    pub fn from_ascii(data: &[u8]) -> SingleDnaRead {
        let mut result = SingleDnaRead {
            data: Vec::from(data)
        };
        for base in result.data.iter_mut() {
            *base = Utils::pos_from_letter(*base);
        }
        result
    }

    pub fn serialize<W: Write>(&self, stream: &mut W) {
//        stream.write_u32::<LittleEndian>(self.data.len() as u32);
        stream.write_all(self.data.as_slice());
    }

    pub fn from_stream<R: Read>(stream: &mut R) -> SingleDnaRead {
        let size = stream.read_u32::<LittleEndian>().unwrap();
        let mut result = SingleDnaRead {
            data: Vec::with_capacity(size as usize)
        };
        result.data.resize(size as usize, 0);
        stream.read_exact(result.data.as_mut_slice());
        result
    }

//    pub fn iter() -> SingleDnaIterator {
//
//    }
}

//struct SingleDnaIterator(&'a SingleDnaRead, usize);
//
//impl Iterator for SingleDnaIterator {
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