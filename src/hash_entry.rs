use std::io::{Read, Write};
use std::marker::PhantomData;

use byteorder::{ByteOrder, LittleEndian, ReadBytesExt, WriteBytesExt};
use serde::{Deserialize, Serialize, Serializer};

use crate::binary_writer::BinaryWriter;
use crate::intermediate_storage::VecReader;
use crate::multi_thread_buckets::BucketWriter;
use crate::unitig_link::Direction;
use crate::varint::{decode_varint, encode_varint};
use crate::vec_slice::VecSlice;

#[derive(Copy, Clone, Serialize, Deserialize)]
pub struct HashEntry {
    pub hash: u64,
    pub bucket: u32,
    pub entry: u64,
    pub direction: Direction,
}

impl BucketWriter for HashEntry {
    type BucketType = BinaryWriter;
    type ExtraData = ();

    #[inline(always)]
    fn write_to(&self, bucket: &mut Self::BucketType, _extra_data: &Self::ExtraData) {
        bincode::serialize_into(bucket.get_writer(), self);
    }
}
