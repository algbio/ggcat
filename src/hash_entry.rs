use std::io::{Read, Write};
use std::marker::PhantomData;

use byteorder::{ByteOrder, LittleEndian, ReadBytesExt, WriteBytesExt};
use serde::{Deserialize, Serialize, Serializer};

use crate::intermediate_storage::VecReader;
use crate::types::BucketIndexType;
use crate::varint::{decode_varint, encode_varint};
use crate::vec_slice::VecSlice;
use bincode::serialize_into;
use parallel_processor::binary_writer::BinaryWriter;
use parallel_processor::multi_thread_buckets::BucketWriter;
use serde::de::DeserializeOwned;
use std::mem::size_of;

#[derive(Copy, Clone, Eq, PartialEq, Serialize, Deserialize)]
#[repr(u8)]
pub enum Direction {
    Forward,
    Backward,
}

#[derive(Copy, Clone, Serialize, Deserialize)]
pub struct HashEntry<H: Copy> {
    pub hash: H,
    pub bucket: BucketIndexType,
    pub entry: u64,
    pub direction: Direction,
}

impl<H: Serialize + DeserializeOwned + Copy> BucketWriter for HashEntry<H> {
    type ExtraData = ();

    #[inline(always)]
    fn write_to(&self, mut bucket: impl Write, _extra_data: &Self::ExtraData) {
        serialize_into(bucket, self);
    }

    #[inline(always)]
    fn get_size(&self) -> usize {
        size_of::<H>() + size_of::<BucketIndexType>() + 8 + 1
    }
}
