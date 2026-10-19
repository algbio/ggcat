//! NtHash impl adapted from https://github.com/luizirber/nthash.git

use crate::dummy_hasher::DummyHasherBuilder;
use crate::nthash_base::{h, rc};
use crate::{ExtendableHashTraitType, HashFunction, HashFunctionFactory, HashableSequence};
use config::BucketIndexType;
use dynamic_dispatch::dynamic_dispatch;
use std::cmp::min;
use std::mem::size_of;

#[derive(Debug, Clone)]
pub struct CanonicalNtHashIterator<N: HashableSequence> {
    seq: N,
    k_minus1: usize,
    fh: u64,
    rc: u64,
}

impl<N: HashableSequence> CanonicalNtHashIterator<N> {
    /// Creates a new NtHashIterator with internal state properly initialized.
    pub fn new(seq: N, k: usize) -> Result<CanonicalNtHashIterator<N>, &'static str> {
        if k > seq.bases_count() {
            return Err("K out of range!");
        }

        let mut fh = 0;
        let mut bw = 0;
        for i in 0..(k - 1) {
            fh ^= unsafe { h(seq.get_unchecked_cbase(i), N::IS_COMPRESSED) }
                .rotate_left((k - i - 2) as u32);
            bw ^= unsafe { rc(seq.get_unchecked_cbase(i), N::IS_COMPRESSED) }.rotate_left(i as u32);
        }

        Ok(CanonicalNtHashIterator {
            seq,
            k_minus1: k - 1,
            fh,
            rc: bw,
        })
    }

    #[inline(always)]
    fn roll_hash(&mut self, i: usize) -> ExtCanonicalNtHash {
        let base_i = unsafe { self.seq.get_unchecked_cbase(i) };
        let base_k = unsafe { self.seq.get_unchecked_cbase(i + self.k_minus1) };

        let seqi_h = h(base_i, N::IS_COMPRESSED);
        let seqk_h = h(base_k, N::IS_COMPRESSED);
        let seqi_rc = rc(base_i, N::IS_COMPRESSED);
        let seqk_rc = rc(base_k, N::IS_COMPRESSED);

        let res = self.fh.rotate_left(1) ^ seqk_h;
        self.fh = res ^ seqi_h.rotate_left((self.k_minus1) as u32);

        let res_rc = self.rc ^ seqk_rc.rotate_left(self.k_minus1 as u32);
        self.rc = (res_rc ^ seqi_rc).rotate_right(1);
        ExtCanonicalNtHash(res, res_rc)
    }
}

impl<N: HashableSequence> HashFunction<CanonicalNtHashIteratorFactory>
    for CanonicalNtHashIterator<N>
{
    #[inline(always)]
    fn iter(
        mut self,
    ) -> impl ExactSizeIterator
    + Iterator<
        Item = <CanonicalNtHashIteratorFactory as HashFunctionFactory>::HashTypeExtendable,
    > {
        (0..self.seq.bases_count() - self.k_minus1).map(move |idx| self.roll_hash(idx))
    }

    #[inline(always)]
    fn iter_enumerate(
        mut self,
    ) -> impl ExactSizeIterator
    + Iterator<
        Item = (
            usize,
            <CanonicalNtHashIteratorFactory as HashFunctionFactory>::HashTypeExtendable,
        ),
    > {
        (0..self.seq.bases_count() - self.k_minus1).map(move |idx| (idx, self.roll_hash(idx)))
    }
}

#[derive(Copy, Clone, Ord, PartialOrd, Eq, PartialEq, Debug)]
pub struct CanonicalNtHashIteratorFactory;

#[derive(Copy, Clone, Debug, Ord, PartialOrd, Eq, PartialEq)]
pub struct ExtCanonicalNtHash(u64, u64);
impl ExtendableHashTraitType for ExtCanonicalNtHash {
    type HashTypeUnextendable = u64;
    #[inline(always)]
    fn to_unextendable(self) -> Self::HashTypeUnextendable {
        // Make sure that the first bit is zero to allow for unique flag in minimizer queue
        min(self.0, self.1) << 1
    }

    #[inline(always)]
    fn is_forward(&self) -> bool {
        self.0 < self.1
    }

    fn is_rc_symmetric(&self) -> bool {
        self.0 == self.1
    }
}

#[dynamic_dispatch]
impl HashFunctionFactory for CanonicalNtHashIteratorFactory {
    type HashTypeUnextendable = u64;
    type HashTypeExtendable = ExtCanonicalNtHash;
    type HashIterator<N: HashableSequence> = CanonicalNtHashIterator<N>;
    type PreferredRandomState = DummyHasherBuilder;

    #[inline(always)]
    fn get_random_state() -> Self::PreferredRandomState {
        DummyHasherBuilder
    }

    // Corresponds to 'N' hash (zero)
    const NULL_BASE: u8 = 4;
    const USABLE_HASH_BITS: usize = size_of::<Self::HashTypeUnextendable>() * 8;

    fn initialize(_k: usize) {}

    #[inline(always)]
    fn new<N: HashableSequence>(seq: N, k: usize) -> Self::HashIterator<N> {
        CanonicalNtHashIterator::new(seq, k).unwrap()
    }

    #[inline(always)]
    fn get_bucket(
        used_bits: usize,
        requested_bits: usize,
        hash: Self::HashTypeUnextendable,
    ) -> BucketIndexType {
        // Discard one bit
        ((hash >> (used_bits + 1)) % (1 << requested_bits)) as BucketIndexType
    }

    fn get_shifted(hash: Self::HashTypeUnextendable, shift: u8) -> u8 {
        (hash >> shift) as u8
    }

    #[inline(always)]
    fn get_u64(hash: Self::HashTypeUnextendable) -> u64 {
        hash as u64
    }

    fn debug_eq_to_u128(hash: Self::HashTypeUnextendable, value: u128) -> bool {
        hash as u128 == value
    }

    // A k-mer's forward hash is the xor of h(base j) rotated left by k - 1 - j, its reverse one
    // that of rc(base j) rotated left by j: as the iterator rolls them. Bases are their codes.

    /// The hash of the k-mer one base on: `out_base` its first, `in_base` the next after its last.
    #[inline(always)]
    fn manual_roll_forward(
        hash: Self::HashTypeExtendable,
        k: usize,
        out_base: u8,
        in_base: u8,
    ) -> Self::HashTypeExtendable {
        let ExtCanonicalNtHash(forward, reverse) = hash;
        let k = k as u32;
        ExtCanonicalNtHash(
            (forward ^ h(out_base, true).rotate_left(k - 1)).rotate_left(1) ^ h(in_base, true),
            (reverse ^ rc(out_base, true)).rotate_right(1) ^ rc(in_base, true).rotate_left(k - 1),
        )
    }

    /// The hash of the k-mer one base back: `out_base` its last, `in_base` the one before its
    /// first.
    #[inline(always)]
    fn manual_roll_reverse(
        hash: Self::HashTypeExtendable,
        k: usize,
        out_base: u8,
        in_base: u8,
    ) -> Self::HashTypeExtendable {
        let ExtCanonicalNtHash(forward, reverse) = hash;
        let k = k as u32;
        ExtCanonicalNtHash(
            (forward ^ h(out_base, true)).rotate_right(1) ^ h(in_base, true).rotate_left(k - 1),
            (reverse ^ rc(out_base, true).rotate_left(k - 1)).rotate_left(1) ^ rc(in_base, true),
        )
    }

    /// The hash of the (k - 1)-mer left without the first base, `out_base`.
    #[inline(always)]
    fn manual_remove_only_forward(
        hash: Self::HashTypeExtendable,
        k: usize,
        out_base: u8,
    ) -> Self::HashTypeExtendable {
        let ExtCanonicalNtHash(forward, reverse) = hash;
        ExtCanonicalNtHash(
            forward ^ h(out_base, true).rotate_left(k as u32 - 1),
            (reverse ^ rc(out_base, true)).rotate_right(1),
        )
    }

    /// The hash of the (k - 1)-mer left without the last base, `out_base`.
    #[inline(always)]
    fn manual_remove_only_reverse(
        hash: Self::HashTypeExtendable,
        k: usize,
        out_base: u8,
    ) -> Self::HashTypeExtendable {
        let ExtCanonicalNtHash(forward, reverse) = hash;
        ExtCanonicalNtHash(
            (forward ^ h(out_base, true)).rotate_right(1),
            reverse ^ rc(out_base, true).rotate_left(k as u32 - 1),
        )
    }

    const INVERTIBLE: bool = false;
    const CANONICAL: bool = false;
    type SeqType = [u8; 0];
    fn invert(_hash: Self::HashTypeUnextendable) -> Self::SeqType {
        unimplemented!()
    }
}

// #[inline(always)]
// fn cnc_nt_manual_roll<const IS_COMPRESSED: bool>(
//     hash: ExtCanonicalNtHash,
//     k: usize,
//     out_b: u8,
//     in_b: u8,
// ) -> ExtCanonicalNtHash {
//     let res = hash.0.rotate_left(1) ^ h(in_b, IS_COMPRESSED);
//     let res_rc = hash.1 ^ rc(in_b, IS_COMPRESSED).rotate_left(k as u32);

//     ExtCanonicalNtHash(
//         res ^ h(out_b, IS_COMPRESSED).rotate_left(k as u32),
//         (res_rc ^ rc(out_b, IS_COMPRESSED)).rotate_right(1),
//     )
// }

// #[inline(always)]
// fn cnc_nt_manual_roll_rev<const IS_COMPRESSED: bool>(
//     hash: ExtCanonicalNtHash,
//     k: usize,
//     out_b: u8,
//     in_b: u8,
// ) -> ExtCanonicalNtHash {
//     let res = hash.0 ^ h(in_b, IS_COMPRESSED).rotate_left(k as u32);
//     let res_rc = hash.1.rotate_left(1) ^ rc(in_b, IS_COMPRESSED);
//     ExtCanonicalNtHash(
//         (res ^ h(out_b, IS_COMPRESSED)).rotate_right(1),
//         res_rc ^ rc(out_b, IS_COMPRESSED).rotate_left(k as u32),
//     )
// }

#[cfg(test)]
mod tests {
    use crate::cn_nthash::CanonicalNtHashIteratorFactory;
    use crate::tests::test_hash_function;

    #[test]
    fn cn_nthash_test() {
        test_hash_function::<CanonicalNtHashIteratorFactory>(&(32..512).collect::<Vec<_>>(), true);
    }
}
