use crate::intermediate_storage::{decode_varint, encode_varint, VecReader};
use byteorder::{ByteOrder, LittleEndian, ReadBytesExt, WriteBytesExt};
use std::io::{Read, Write};

#[derive(Copy, Clone, Eq, PartialEq)]
pub enum Direction {
    Forward,
    Backward,
}

#[derive(Copy, Clone)]
pub struct HashEntry {
    pub hash: u64,
    pub bucket: u32,
    pub entry: u64,
    pub direction: Direction,
}

impl HashEntry {
    // pub fn new()

    pub fn serialize_to_file<'a>(&self, mut writer: impl Write + 'a) {
        writer.write_u64::<LittleEndian>(self.hash);
        encode_varint(&mut writer, self.bucket as u64);
        encode_varint(&mut writer, self.entry);
        writer.write_u8(self.direction as u8);
    }

    pub fn deserialize_from_file(mut reader: impl Read) -> HashEntry {
        let mut buffer = [0; 8];

        reader.read(&mut buffer[..]);
        let hash = LittleEndian::read_u64(&buffer[..]);

        let bucket = decode_varint(&mut reader).unwrap() as u32;
        let entry = decode_varint(&mut reader).unwrap();
        let direction = reader.read_u8().unwrap();

        HashEntry {
            hash,
            bucket,
            entry,
            direction: match direction {
                0 => Direction::Forward,
                _ => Direction::Backward,
            },
        }
    }
}

#[derive(Copy, Clone)]
pub struct UnitigLink {
    pub bucket1: u32,
    pub entry1: u64,
    pub bucket2: u32,
    pub entry2: u64,
}

impl UnitigLink {
    // pub fn new()

    pub fn serialize_to_file<'a>(&self, mut writer: impl Write + 'a) {
        encode_varint(&mut writer, self.bucket1 as u64);
        encode_varint(&mut writer, self.entry1);
        encode_varint(&mut writer, self.bucket2 as u64);
        encode_varint(&mut writer, self.entry2);

        // writer.write_fmt(format_args!(
        //     "{} {} > {} {}\n",
        //     self.bucket1, self.entry1, self.bucket2, self.entry2
        // ));
    }

    pub fn deserialize_from_file(mut reader: impl Read) -> UnitigLink {
        let bucket1 = decode_varint(&mut reader).unwrap() as u32;
        let entry1 = decode_varint(&mut reader).unwrap();
        let bucket2 = decode_varint(&mut reader).unwrap() as u32;
        let entry2 = decode_varint(&mut reader).unwrap();

        UnitigLink {
            bucket1,
            entry1,
            bucket2,
            entry2,
        }
    }
}
