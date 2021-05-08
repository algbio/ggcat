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

#[derive(Copy, Clone, Serialize, Deserialize)]
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

impl Serialize for UnitigLinkSerializer {
    fn serialize<S>(&self, serializer: S) -> Result<<S as Serializer>::Ok, <S as Serializer>::Error>
    where
        S: Serializer,
    {
        let mut serializer = serializer.serialize_tuple(3)?;
        serializer.serialize_element(&self.link.entry);
        serializer.serialize_element(&self.link.direction);
        serializer.serialize_element(&self.link.entries.get_slice(&self.indexes));
        serializer.end()
    }
}

#[derive(Copy, Clone, Serialize, Deserialize)]
pub struct UnitigPointer {
    pub entry: u64,
    pub link_index: u64,
}

impl BucketWriter for UnitigLink {
    type BucketType = BinaryWriter;
    type ExtraData = Vec<UnitigIndex>;

    #[inline(always)]
    fn write_to(&self, bucket: &mut Self::BucketType, extra_data: &Self::ExtraData) {}
}
