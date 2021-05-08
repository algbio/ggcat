use std::io::{Read, Write};
use std::marker::PhantomData;

use byteorder::{ByteOrder, LittleEndian, ReadBytesExt, WriteBytesExt};
use serde::{Deserialize, Serialize, Serializer};

use crate::binary_writer::BinaryWriter;
use crate::intermediate_storage::VecReader;
use crate::multi_thread_buckets::BucketWriter;
use crate::varint::{decode_varint, encode_varint};
use crate::vec_slice::VecSlice;
use serde::ser::SerializeTuple;

#[derive(Copy, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub enum Direction {
    Forward,
    Backward,
}

#[derive(Copy, Clone)]
pub struct UnitigIndex {
    index: usize,
}

impl UnitigIndex {
    const INDEX_MASK: usize = !((1 << 48) - 1);

    #[inline]
    pub fn new(bucket: usize, index: usize) -> Self {
        Self {
            index: (bucket << 48) | (index & Self::INDEX_MASK),
        }
    }
    #[inline]
    pub fn bucket(&self) -> usize {
        self.index >> 48
    }
    #[inline]
    pub fn index(&self) -> usize {
        self.index & Self::INDEX_MASK
    }
}

#[derive(Clone)]
pub struct UnitigLink {
    pub entry: u64,
    pub direction: Direction,
    pub entries: VecSlice<UnitigIndex>,
}

struct UnitigLinkSerializer {
    link: UnitigLink,
    indexes: Vec<UnitigIndex>,
}

#[derive(Copy, Clone)]
pub struct UnitigPointer {
    pub entry: u64,
    pub link_index: u64,
}

impl UnitigLink {
    pub fn read_from(mut reader: impl Read, out_vec: &mut Vec<UnitigIndex>) -> Option<Self> {
        let entry = decode_varint(|| reader.read_u8().ok())?;
        let direction = match reader.read_u8().ok()? {
            0 => Direction::Forward,
            1 => Direction::Backward,
            _ => return None
        };

        let len = decode_varint(|| reader.read_u8().ok())? as usize;

        let start = out_vec.len();
        for i in 0..len {
            let bucket = decode_varint(|| reader.read_u8().ok())?;
            let index = decode_varint(|| reader.read_u8().ok())?;
            out_vec.push(UnitigIndex::new(bucket as usize, index as usize))
        }

        Some(Self {
            entry,
            direction,
            entries: VecSlice::new(start, len)
        })
    }
}

impl BucketWriter for UnitigLink {
    type BucketType = BinaryWriter;
    type ExtraData = Vec<UnitigIndex>;

    #[inline(always)]
    fn write_to(&self, bucket: &mut Self::BucketType, extra_data: &Self::ExtraData) {
        let writer = bucket.get_writer();
        encode_varint(|b| writer.write(b), self.entry);
        writer.write(&[self.direction as u8]);

        let entries = self.entries.get_slice(extra_data);

        encode_varint(|b| writer.write(b), entries.len() as u64);

        for entry in entries {
            encode_varint(|b| writer.write(b), entry.bucket() as u64);
            encode_varint(|b| writer.write(b), entry.index() as u64);
        }
    }
}
