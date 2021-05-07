use crate::binary_writer::BinaryWriter;
use crate::intermediate_storage::VecReader;
use crate::multi_thread_buckets::BucketWriter;
use crate::varint::{decode_varint, encode_varint};
use byteorder::{ByteOrder, LittleEndian, ReadBytesExt, WriteBytesExt};
use serde::{Deserialize, Serialize, Serializer};
use std::io::{Read, Write};
use std::marker::PhantomData;

#[derive(Copy, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub enum Direction {
    Forward,
    Backward,
}

#[derive(Copy, Clone, Serialize, Deserialize)]
pub struct HashEntry {
    pub hash: u64,
    pub bucket: u32,
    pub entry: u64,
    pub direction: Direction,
}

impl BucketWriter for HashEntry {
    type BucketType = BinaryWriter;

    #[inline(always)]
    fn write_to(&self, bucket: &mut Self::BucketType) {
        bincode::serialize_into(bucket.get_writer(), self);
    }
}

pub const TRASH_SIZE: usize = 7;

#[derive(Copy, Clone)]
pub struct VecSlice<T> {
    pos: usize,
    len: usize,
    _phantom: PhantomData<T>,
}

impl<T> VecSlice<T> {
    pub fn new(pos: usize, len: usize) -> Self {
        Self {
            pos,
            len,
            _phantom: Default::default(),
        }
    }
    fn get_slice<'a>(&self, vec: &'a Vec<T>) -> &'a [T] {
        &vec[self.pos..self.pos + self.len]
    }
    fn get_slice_mut<'a>(&self, vec: &'a mut Vec<T>) -> &'a mut [T] {
        &mut vec[self.pos..self.pos + self.len]
    }
}

// impl<T: Serialize> Serialize for VecSlice<T> {
//     fn serialize<S>(&self, serializer: S) -> Result<<S as Serializer>::Ok, <S as Serializer>::Error>
//     where
//         S: Serializer,
//     {
//         self.get_slice().serialize(serializer)
//     }
// }

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
    pub is_forward: bool,
    pub entries: VecSlice<UnitigIndex>,
}

#[derive(Copy, Clone, Serialize, Deserialize)]
pub struct UnitigPointer {
    pub entry: u64,
    pub link_index: u64,
}

impl BucketWriter for UnitigLink {
    type BucketType = BinaryWriter;

    #[inline(always)]
    fn write_to(&self, bucket: &mut Self::BucketType) {}
}
