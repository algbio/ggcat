use crate::hashes::HashFunctionFactory;
use crate::io::structs::hash_entry::{Direction, HashEntry};
use bincode::serialize_into;
use parallel_processor::buckets::bucket_writer::BucketWriter;
use parallel_processor::fast_smart_bucket_sort::SortKey;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::marker::PhantomData;
use std::mem::size_of;

#[derive(Copy, Clone, Serialize, Deserialize, Debug)]
pub struct EdgeHashEntry<H: Copy> {
    pub hentry: HashEntry<H>,
    pub orig_dir: Direction,
}

impl<H: Serialize + DeserializeOwned + Copy> BucketWriter for EdgeHashEntry<H> {
    type ExtraData = ();

    #[inline(always)]
    fn write_to(&self, bucket: &mut Vec<u8>, _extra_data: &Self::ExtraData) {
        serialize_into(bucket, self).unwrap();
    }

    #[inline(always)]
    fn get_size(&self) -> usize {
        <HashEntry<H> as BucketWriter<u8>>::get_size(&self.hentry) + 1
    }
}

pub struct EdgeHashCompare<H: HashFunctionFactory> {
    _phantom: PhantomData<H>,
}

impl<H: HashFunctionFactory> SortKey<EdgeHashEntry<H::HashTypeUnextendable>>
    for EdgeHashCompare<H>
{
    type KeyType = H::HashTypeUnextendable;
    const KEY_BITS: usize = size_of::<H::HashTypeUnextendable>() * 8;

    #[inline(always)]
    fn compare(
        left: &EdgeHashEntry<<H as HashFunctionFactory>::HashTypeUnextendable>,
        right: &EdgeHashEntry<<H as HashFunctionFactory>::HashTypeUnextendable>,
    ) -> std::cmp::Ordering {
        left.hentry.hash.cmp(&right.hentry.hash)
    }

    #[inline(always)]
    fn get_shifted(value: &EdgeHashEntry<H::HashTypeUnextendable>, rhs: u8) -> u8 {
        H::get_shifted(value.hentry.hash, rhs) as u8
    }
}
