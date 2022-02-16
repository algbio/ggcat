use crate::config::BucketIndexType;
use bincode::serialize_into;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::mem::size_of;
use parallel_processor::buckets::bucket_writer::BucketWriter;

#[derive(Copy, Clone, Eq, PartialEq, Serialize, Deserialize, Debug)]
#[repr(u8)]
pub enum Direction {
    Forward,
    Backward,
}

#[derive(Copy, Clone, Serialize, Deserialize, Debug)]
pub struct HashEntry<H: Copy> {
    pub hash: H,
    pub bucket: BucketIndexType,
    pub entry: u64,
    pub direction: Direction,
}

impl<H: Serialize + DeserializeOwned + Copy> BucketWriter for HashEntry<H> {
    type ExtraData = ();

    #[inline(always)]
    fn write_to(&self, bucket: &mut Vec<u8>, _extra_data: &Self::ExtraData) {
        serialize_into(bucket, self).unwrap();
    }

    #[inline(always)]
    fn get_size(&self) -> usize {
        size_of::<H>() + size_of::<BucketIndexType>() + 8 + 1
    }
}
