use std::fmt::{Debug, Display};
use std::hash::Hash;
use std::ops::{Shl, Shr};

use crate::hashes::nthash::NtHashIterator;
use serde::de::DeserializeOwned;
use serde::Serialize;

use crate::types::{BucketIndexType, MinimizerType};

pub trait HashTraitType =
    Copy + Clone + Debug + Display + Eq + Ord + Hash + Serialize + DeserializeOwned + From<u64>;

pub trait HashFunctionFactory: Sized + Clone {
    type HashType: HashTraitType;
    type HashIterator<N: HashableSequence>: HashFunction<Self>;

    const NULL_BASE: u8;

    fn new<N: HashableSequence>(seq: N, k: usize) -> Self::HashIterator<N>;

    fn get_bucket(hash: Self::HashType) -> BucketIndexType;
    fn get_second_bucket(hash: Self::HashType) -> BucketIndexType;
    fn get_minimizer(hash: Self::HashType) -> MinimizerType;
    fn get_shifted(hash: Self::HashType, shift: u8) -> u8;

    fn manual_roll_forward(
        hash: Self::HashType,
        k: usize,
        out_base: u8,
        in_base: u8,
    ) -> Self::HashType;

    fn manual_roll_reverse(
        hash: Self::HashType,
        k: usize,
        out_base: u8,
        in_base: u8,
    ) -> Self::HashType;

    fn manual_remove_only_forward(hash: Self::HashType, k: usize, out_base: u8) -> Self::HashType;

    fn manual_remove_only_reverse(hash: Self::HashType, k: usize, out_base: u8) -> Self::HashType;
}

pub trait HashFunction<HF: HashFunctionFactory> {
    type IteratorType: Iterator<Item = HF::HashType>;
    type EnumerableIteratorType: Iterator<Item = (usize, HF::HashType)>;

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
