//! Scalar twin of the vectorized canonical ntHash.
//!
//! The vectorized minimizer hash works on 32-bit lanes, so its rotations wrap
//! at 32 bits and it cannot be obtained by truncating the 64-bit hash. This
//! module reproduces it one sequence at a time, which is what the bucketing
//! phase uses and what lets the vectorized path be compared against the
//! ordinary one.

use crate::dummy_hasher::DummyHasherBuilder;
use crate::nthash_base::MULTIPLIER;
use crate::{ExtendableHashTraitType, HashFunction, HashFunctionFactory, HashableSequence};
use config::BucketIndexType;
use dynamic_dispatch::dynamic_dispatch;
use std::cmp::min;

pub const MULTIPLIER32: u32 = MULTIPLIER as u32;

#[inline(always)]
fn h32(c: u8, compressed: bool) -> u32 {
    let mult = if compressed { c << 1 } else { c & 0x6 } + 1;
    (mult as u32).wrapping_mul(MULTIPLIER32)
}

#[inline(always)]
fn rc32(c: u8, compressed: bool) -> u32 {
    let mult = (if compressed { c << 1 } else { c & 0x6 } ^ 4) + 1;
    (mult as u32).wrapping_mul(MULTIPLIER32)
}

#[derive(Debug, Clone)]
pub struct CanonicalNtHash32Iterator<N: HashableSequence> {
    seq: N,
    k_minus1: usize,
    fh: u32,
    rc: u32,
}

impl<N: HashableSequence> CanonicalNtHash32Iterator<N> {
    pub fn new(seq: N, k: usize) -> Result<Self, &'static str> {
        if k == 0 || k > seq.bases_count() {
            return Err("K out of range!");
        }
        let mut fh = 0u32;
        let mut bw = 0u32;
        for i in 0..(k - 1) {
            fh ^= unsafe { h32(seq.get_unchecked_cbase(i), N::IS_COMPRESSED) }
                .rotate_left((k - i - 2) as u32);
            bw ^=
                unsafe { rc32(seq.get_unchecked_cbase(i), N::IS_COMPRESSED) }.rotate_left(i as u32);
        }
        Ok(Self {
            seq,
            k_minus1: k - 1,
            fh,
            rc: bw,
        })
    }

    #[inline(always)]
    fn roll_hash(&mut self, i: usize) -> ExtCanonicalNtHash32 {
        let base_i = unsafe { self.seq.get_unchecked_cbase(i) };
        let base_k = unsafe { self.seq.get_unchecked_cbase(i + self.k_minus1) };

        let res = self.fh.rotate_left(1) ^ h32(base_k, N::IS_COMPRESSED);
        self.fh = res ^ h32(base_i, N::IS_COMPRESSED).rotate_left(self.k_minus1 as u32);

        let res_rc = self.rc ^ rc32(base_k, N::IS_COMPRESSED).rotate_left(self.k_minus1 as u32);
        self.rc = (res_rc ^ rc32(base_i, N::IS_COMPRESSED)).rotate_right(1);
        ExtCanonicalNtHash32(res, res_rc)
    }
}

impl<N: HashableSequence> HashFunction<CanonicalNtHash32IteratorFactory>
    for CanonicalNtHash32Iterator<N>
{
    #[inline(always)]
    fn iter(mut self) -> impl ExactSizeIterator + Iterator<Item = ExtCanonicalNtHash32> {
        (0..self.seq.bases_count() - self.k_minus1).map(move |idx| self.roll_hash(idx))
    }

    #[inline(always)]
    fn iter_enumerate(
        mut self,
    ) -> impl ExactSizeIterator + Iterator<Item = (usize, ExtCanonicalNtHash32)> {
        (0..self.seq.bases_count() - self.k_minus1).map(move |idx| (idx, self.roll_hash(idx)))
    }
}

#[derive(Copy, Clone, Ord, PartialOrd, Eq, PartialEq, Debug)]
pub struct CanonicalNtHash32IteratorFactory;

#[derive(Copy, Clone, Debug, Ord, PartialOrd, Eq, PartialEq)]
pub struct ExtCanonicalNtHash32(pub u32, pub u32);

impl ExtendableHashTraitType for ExtCanonicalNtHash32 {
    type HashTypeUnextendable = u64;

    #[inline(always)]
    fn to_unextendable(self) -> u64 {
        // The shift happens in 32 bits, dropping the top bit, exactly as the
        // vectorized version does; the freed low bit carries the unique flag.
        ((min(self.0, self.1) << 1) as u64) & u32::MAX as u64
    }

    #[inline(always)]
    fn is_forward(&self) -> bool {
        self.0 < self.1
    }

    #[inline(always)]
    fn is_rc_symmetric(&self) -> bool {
        self.0 == self.1
    }
}

#[dynamic_dispatch]
impl HashFunctionFactory for CanonicalNtHash32IteratorFactory {
    type HashTypeUnextendable = u64;
    type HashTypeExtendable = ExtCanonicalNtHash32;
    type HashIterator<N: HashableSequence> = CanonicalNtHash32Iterator<N>;
    type PreferredRandomState = DummyHasherBuilder;

    #[inline(always)]
    fn get_random_state() -> Self::PreferredRandomState {
        DummyHasherBuilder
    }

    const NULL_BASE: u8 = 4;
    const USABLE_HASH_BITS: usize = 32;

    fn initialize(_k: usize) {}

    #[inline(always)]
    fn new<N: HashableSequence>(seq: N, k: usize) -> Self::HashIterator<N> {
        CanonicalNtHash32Iterator::new(seq, k).unwrap()
    }

    #[inline(always)]
    fn get_bucket(used_bits: usize, requested_bits: usize, hash: u64) -> BucketIndexType {
        // Discard the unique flag
        ((hash >> (used_bits + 1)) % (1 << requested_bits)) as BucketIndexType
    }

    fn get_shifted(hash: u64, shift: u8) -> u8 {
        (hash >> shift) as u8
    }

    #[inline(always)]
    fn get_u64(hash: u64) -> u64 {
        hash
    }

    fn debug_eq_to_u128(hash: u64, value: u128) -> bool {
        hash as u128 == value
    }

    fn manual_roll_forward(
        _hash: Self::HashTypeExtendable,
        _k: usize,
        _out_base: u8,
        _in_base: u8,
    ) -> Self::HashTypeExtendable {
        unimplemented!()
    }

    fn manual_roll_reverse(
        _hash: Self::HashTypeExtendable,
        _k: usize,
        _out_base: u8,
        _in_base: u8,
    ) -> Self::HashTypeExtendable {
        unimplemented!()
    }

    fn manual_remove_only_forward(
        _hash: Self::HashTypeExtendable,
        _k: usize,
        _out_base: u8,
    ) -> Self::HashTypeExtendable {
        unimplemented!()
    }

    fn manual_remove_only_reverse(
        _hash: Self::HashTypeExtendable,
        _k: usize,
        _out_base: u8,
    ) -> Self::HashTypeExtendable {
        unimplemented!()
    }

    const INVERTIBLE: bool = false;
    const CANONICAL: bool = false;
    type SeqType = [u8; 0];
    fn invert(_hash: u64) -> Self::SeqType {
        unimplemented!()
    }
}
