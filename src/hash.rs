use std::fmt::{Debug, Display};
use std::hash::Hash;
use std::ops::{Shl, Shr};

use crate::hashes::fw_nthash::ForwardNtHashIterator;
use serde::de::DeserializeOwned;
use serde::Serialize;

use crate::types::{BucketIndexType, MinimizerType};

pub trait UnextendableHashTraitType =
    Copy + Clone + Debug + Display + Eq + Ord + Hash + Serialize + DeserializeOwned + From<u64>;

pub trait ExtendableHashTraitType: Copy + Clone + Debug + Eq + Ord {
    type HashTypeUnextendable: UnextendableHashTraitType;
    fn to_unextendable(self) -> Self::HashTypeUnextendable;
}

pub trait HashFunctionFactory: Ord + Sized + Clone {
    type HashTypeUnextendable: UnextendableHashTraitType;
    type HashTypeExtendable: ExtendableHashTraitType<
        HashTypeUnextendable = Self::HashTypeUnextendable,
    >;
    type HashIterator<N: HashableSequence>: HashFunction<Self>;

    const NULL_BASE: u8;

    fn new<N: HashableSequence>(seq: N, k: usize) -> Self::HashIterator<N>;

    fn get_bucket(hash: Self::HashTypeUnextendable) -> BucketIndexType;
    fn get_second_bucket(hash: Self::HashTypeUnextendable) -> BucketIndexType;
    fn get_minimizer(hash: Self::HashTypeUnextendable) -> MinimizerType;
    fn get_shifted(hash: Self::HashTypeUnextendable, shift: u8) -> u8;

    fn manual_roll_forward(
        hash: Self::HashTypeExtendable,
        k: usize,
        out_base: u8,
        in_base: u8,
    ) -> Self::HashTypeExtendable;

    fn manual_roll_reverse(
        hash: Self::HashTypeExtendable,
        k: usize,
        out_base: u8,
        in_base: u8,
    ) -> Self::HashTypeExtendable;

    fn manual_remove_only_forward(
        hash: Self::HashTypeExtendable,
        k: usize,
        out_base: u8,
    ) -> Self::HashTypeExtendable;

    fn manual_remove_only_reverse(
        hash: Self::HashTypeExtendable,
        k: usize,
        out_base: u8,
    ) -> Self::HashTypeExtendable;
}

pub trait HashFunction<HF: HashFunctionFactory> {
    type IteratorType: Iterator<Item = HF::HashTypeExtendable>;
    type EnumerableIteratorType: Iterator<Item = (usize, HF::HashTypeExtendable)>;

    fn iter(self) -> Self::IteratorType;
    fn iter_enumerate(self) -> Self::EnumerableIteratorType;
}

pub trait HashableSequence: Clone {
    unsafe fn get_unchecked_cbase(&self, index: usize) -> u8;
    fn bases_count(&self) -> usize;
}

impl HashableSequence for &[u8] {
    #[inline(always)]
    unsafe fn get_unchecked_cbase(&self, index: usize) -> u8 {
        *self.get_unchecked(index)
    }

    #[inline(always)]
    fn bases_count(&self) -> usize {
        self.len()
    }
}
