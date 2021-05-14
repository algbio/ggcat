use std::io::{Read, Write};
use std::marker::PhantomData;

use byteorder::{ByteOrder, LittleEndian, ReadBytesExt, WriteBytesExt};
use serde::{Deserialize, Serialize, Serializer};

use crate::binary_writer::BinaryWriter;
use crate::intermediate_storage::VecReader;
use crate::multi_thread_buckets::BucketWriter;
use crate::types::BucketIndexType;
use crate::varint::{decode_varint, encode_varint};
use crate::vec_slice::VecSlice;
use serde::de::DeserializeOwned;

#[derive(Copy, Clone, Eq, PartialEq, Serialize, Deserialize)]
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
    type BucketType = BinaryWriter;
    type ExtraData = ();

    #[inline(always)]
    fn write_to(&self, bucket: &mut Self::BucketType, _extra_data: &Self::ExtraData) {
        bincode::serialize_into(bucket.get_writer(), self);
    }
}
