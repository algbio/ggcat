use bincode::{deserialize_from, serialize_into};
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
pub enum MaximalUnitigPosition {
    Beginning,
    Ending,
}

#[derive(Copy, Clone, Serialize, Deserialize, Debug)]
pub struct MaximalHashEntry<H: Copy> {
    pub hash: H,
    encoded: u64,
    overlap_start: u64,
}

impl<H: Copy> MaximalHashEntry<H> {
    const ENTRY_OFFSET: usize = 2;
    const DIRECTION_OFFSET: usize = 1;
    const POSITION_OFFSET: usize = 0;

    pub fn new(
        hash: H,
        entry: u64,
        position: MaximalUnitigPosition,
        direction_forward: bool,
        overlap_start: u64,
    ) -> Self {
        Self {
            hash,
            encoded: (entry << Self::ENTRY_OFFSET)
                | ((match position {
                    MaximalUnitigPosition::Ending => 1,
                    MaximalUnitigPosition::Beginning => 0,
                }) << Self::POSITION_OFFSET)
                | ((if direction_forward { 1 } else { 0 }) << Self::DIRECTION_OFFSET),
            overlap_start,
        }
    }

    pub fn entry(&self) -> u64 {
        self.encoded >> Self::ENTRY_OFFSET
    }

    pub fn overlap_start(&self) -> u64 {
        self.overlap_start
    }

    pub fn position(&self) -> MaximalUnitigPosition {
        if (self.encoded >> Self::POSITION_OFFSET) & 0x1 == 0 {
            MaximalUnitigPosition::Beginning
        } else {
            MaximalUnitigPosition::Ending
        }
    }

    pub fn direction(&self) -> bool {
        ((self.encoded >> Self::DIRECTION_OFFSET) & 0x1) == 1
    }
}

pub struct MaximalHashEntrySerializer<H: Serialize + DeserializeOwned + Copy>(PhantomData<H>);
impl<H: Serialize + DeserializeOwned + Copy> BucketItemSerializer
    for MaximalHashEntrySerializer<H>
{
    type InputElementType<'a> = MaximalHashEntry<H>;
    type ExtraData = ();
    type ReadBuffer = ();
    type ExtraDataBuffer = ();
    type ReadType<'a> = MaximalHashEntry<H>;
    type InitData = ();

    type CheckpointData = ();

    #[inline(always)]
    fn new(_: ()) -> Self {
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
    fn get_size(&self, _element: &Self::InputElementType<'_>, _: &()) -> usize {
        size_of::<H>() + 8 + 1
    }
}

pub struct MaximalHashCompare<H: HashFunctionFactory> {
    _phantom: PhantomData<H>,
}

impl<H: HashFunctionFactory> SortKey<MaximalHashEntry<H::HashTypeUnextendable>>
    for MaximalHashCompare<H>
{
    type KeyType = H::HashTypeUnextendable;
    const KEY_BITS: usize = size_of::<H::HashTypeUnextendable>() * 8;

    #[inline(always)]
    fn compare(
        left: &MaximalHashEntry<<H as HashFunctionFactory>::HashTypeUnextendable>,
        right: &MaximalHashEntry<<H as HashFunctionFactory>::HashTypeUnextendable>,
    ) -> std::cmp::Ordering {
        left.hash.cmp(&right.hash)
    }

    #[inline(always)]
    fn get_shifted(value: &MaximalHashEntry<H::HashTypeUnextendable>, rhs: u8) -> u8 {
        H::get_shifted(value.hash, rhs) as u8
    }
}
