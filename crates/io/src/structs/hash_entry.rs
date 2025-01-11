use bincode::{deserialize_from, serialize_into};
use config::BucketIndexType;
use hashes::HashFunctionFactory;
use parallel_processor::buckets::bucket_writer::BucketItemSerializer;
use parallel_processor::fast_smart_bucket_sort::SortKey;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::io::Read;
use std::marker::PhantomData;
use std::mem::size_of;

#[derive(Copy, Clone, Eq, PartialEq, Serialize, Deserialize, Debug)]
#[repr(u8)]
pub enum Direction {
    Backward,
    Forward,
}

#[derive(Copy, Clone, Serialize, Deserialize, Debug)]
pub struct HashEntry<H: Copy> {
    pub hash: H,
    encoded: u64,
    // pub bucket: BucketIndexType,
    // pub entry: u64,
    // pub direction: Direction,
}

impl<H: Copy> HashEntry<H> {
    const ENTRY_OFFSET: usize = (size_of::<BucketIndexType>() * 8) + 1;
    const BUCKET_OFFSET: usize = 1;
    const DIRECTION_OFFSET: usize = 0;

    pub fn new(hash: H, bucket: BucketIndexType, entry: u64, direction: Direction) -> Self {
        Self {
            hash,
            encoded: (entry << Self::ENTRY_OFFSET)
                | ((bucket as u64) << Self::BUCKET_OFFSET)
                | ((match direction {
                    Direction::Forward => 1,
                    Direction::Backward => 0,
                }) << Self::DIRECTION_OFFSET),
        }
    }

    pub fn entry(&self) -> u64 {
        self.encoded >> Self::ENTRY_OFFSET
    }

    pub fn bucket(&self) -> BucketIndexType {
        (self.encoded >> Self::BUCKET_OFFSET) as BucketIndexType
    }

    pub fn direction(&self) -> Direction {
        if (self.encoded >> Self::DIRECTION_OFFSET) & 0x1 == 0 {
            Direction::Backward
        } else {
            Direction::Forward
        }
    }
}

pub struct HashEntrySerializer<H: Copy>(PhantomData<H>);

impl<H: Serialize + DeserializeOwned + Copy> BucketItemSerializer for HashEntrySerializer<H> {
    type InputElementType<'a> = HashEntry<H>;
    type ExtraData = ();
    type ReadBuffer = ();
    type ExtraDataBuffer = ();
    type ReadType<'a> = HashEntry<H>;

    type CheckpointData = ();

    #[inline(always)]
    fn new() -> Self {
        Self(PhantomData)
    }

    #[inline(always)]
    fn reset(&mut self) {}

    #[inline(always)]
    fn write_to(
        &mut self,
        element: &Self::InputElementType<'_>,
        bucket: &mut Vec<u8>,
        _extra_data: &Self::ExtraData,
        _: &(),
    ) {
        serialize_into(bucket, element).unwrap();
    }

    fn read_from<'a, S: Read>(
        &mut self,
        stream: S,
        _read_buffer: &'a mut Self::ReadBuffer,
        _: &mut (),
    ) -> Option<Self::ReadType<'a>> {
        deserialize_from(stream).ok()
    }

    #[inline(always)]
    fn get_size(&self, _: &Self::InputElementType<'_>, _: &()) -> usize {
        size_of::<H>() + size_of::<BucketIndexType>() + 8 + 1
    }
}

pub struct HashCompare<H: HashFunctionFactory> {
    _phantom: PhantomData<H>,
}

impl<H: HashFunctionFactory> SortKey<HashEntry<H::HashTypeUnextendable>> for HashCompare<H> {
    type KeyType = H::HashTypeUnextendable;
    const KEY_BITS: usize = size_of::<H::HashTypeUnextendable>() * 8;

    #[inline(always)]
    fn compare(
        left: &HashEntry<<H as HashFunctionFactory>::HashTypeUnextendable>,
        right: &HashEntry<<H as HashFunctionFactory>::HashTypeUnextendable>,
    ) -> std::cmp::Ordering {
        left.hash.cmp(&right.hash)
    }

    #[inline(always)]
    fn get_shifted(value: &HashEntry<H::HashTypeUnextendable>, rhs: u8) -> u8 {
        H::get_shifted(value.hash, rhs) as u8
    }
}
