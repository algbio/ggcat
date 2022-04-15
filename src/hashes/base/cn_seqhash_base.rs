use crate::config::{BucketIndexType, MinimizerType};
use crate::hashes::{ExtendableHashTraitType, HashFunction, HashFunctionFactory, HashableSequence};
use std::cmp::min;
use std::mem::size_of;

pub struct CanonicalSeqHashIterator<N: HashableSequence> {
    seq: N,
    mask: HashIntegerType,
    fh: HashIntegerType,
    rc: HashIntegerType,
    k_minus1: usize,
}

#[inline(always)]
fn get_mask(k: usize) -> HashIntegerType {
    HashIntegerType::MAX >> ((((size_of::<HashIntegerType>() * 4) - k) * 2) as HashIntegerType)
}

impl<N: HashableSequence> CanonicalSeqHashIterator<N> {
    pub fn new(seq: N, k: usize) -> Result<CanonicalSeqHashIterator<N>, &'static str> {
        if k > seq.bases_count() || k > (size_of::<HashIntegerType>() * 4) {
            return Err("K out of range!");
        }

        let mut fh = 0;
        let mut bw = 0;
        for i in 0..(k - 1) {
            fh = (fh << 2) | unsafe { seq.get_unchecked_cbase(i) as HashIntegerType };
            bw |= unsafe { xrc(seq.get_unchecked_cbase(i) as HashIntegerType) } << (i * 2);
        }

        let mask = get_mask(k);

        Ok(CanonicalSeqHashIterator {
            seq,
            mask,
            fh: fh & mask,
            rc: bw << 2,
            k_minus1: k - 1,
        })
    }

    #[inline(always)]
    fn roll_hash(&mut self, index: usize) -> ExtCanonicalSeqHash {
        assert!(unsafe { self.seq.get_unchecked_cbase(index) } < 4);

        self.fh = ((self.fh << 2)
            | unsafe { self.seq.get_unchecked_cbase(index) as HashIntegerType })
            & self.mask;

        self.rc = (self.rc >> 2)
            | ((unsafe { xrc(self.seq.get_unchecked_cbase(index) as HashIntegerType) })
                << (self.k_minus1 * 2));
        ExtCanonicalSeqHash(self.fh, self.rc)
    }
}

impl<N: HashableSequence> HashFunction<CanonicalSeqHashFactory> for CanonicalSeqHashIterator<N> {
    type IteratorType =
        impl Iterator<Item = <CanonicalSeqHashFactory as HashFunctionFactory>::HashTypeExtendable>;
    type EnumerableIteratorType = impl Iterator<
        Item = (
            usize,
            <CanonicalSeqHashFactory as HashFunctionFactory>::HashTypeExtendable,
        ),
    >;

    fn iter(mut self) -> Self::IteratorType {
        (self.k_minus1..self.seq.bases_count()).map(move |idx| self.roll_hash(idx))
    }

    fn iter_enumerate(mut self) -> Self::EnumerableIteratorType {
        (self.k_minus1..self.seq.bases_count())
            .map(move |idx| (idx - self.k_minus1, self.roll_hash(idx)))
    }
}

#[derive(Copy, Clone, Ord, PartialOrd, Eq, PartialEq, Debug)]
pub struct CanonicalSeqHashFactory;

#[derive(Copy, Clone, Debug, Ord, PartialOrd, Eq, PartialEq)]
pub struct ExtCanonicalSeqHash(HashIntegerType, HashIntegerType);

impl ExtendableHashTraitType for ExtCanonicalSeqHash {
    type HashTypeUnextendable = HashIntegerType;

    #[inline(always)]
    fn to_unextendable(self) -> Self::HashTypeUnextendable {
        min(self.0, self.1)
    }

    #[inline(always)]
    fn is_forward(&self) -> bool {
        self.0 < self.1
    }
}

impl HashFunctionFactory for CanonicalSeqHashFactory {
    type HashTypeUnextendable = HashIntegerType;
    type HashTypeExtendable = ExtCanonicalSeqHash;
    type HashIterator<N: HashableSequence> = CanonicalSeqHashIterator<N>;
    type PreferredRandomState = ahash::RandomState;

    #[inline(always)]
    fn get_random_state() -> Self::PreferredRandomState {
        ahash::RandomState::new()
    }

    const NULL_BASE: u8 = 0;

    fn new<N: HashableSequence>(seq: N, k: usize) -> Self::HashIterator<N> {
        CanonicalSeqHashIterator::new(seq, k).unwrap()
    }

    fn get_first_bucket(hash: Self::HashTypeUnextendable) -> BucketIndexType {
        hash as BucketIndexType
    }

    fn get_full_minimizer(_hash: Self::HashTypeUnextendable) -> MinimizerType {
        panic!("Not supported!")
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

    fn manual_roll_forward(
        hash: Self::HashTypeExtendable,
        k: usize,
        _out_base: u8,
        in_base: u8,
    ) -> Self::HashTypeExtendable {
        assert!(in_base < 4);
        // K = 2
        // 00AABB => roll CC
        // 00BBCC

        let mask = get_mask(k);
        ExtCanonicalSeqHash(
            ((hash.0 << 2) | (in_base as HashIntegerType)) & mask,
            (hash.1 >> 2) | (xrc(in_base as HashIntegerType) << ((k - 1) * 2)),
        )
    }

    fn manual_roll_reverse(
        hash: Self::HashTypeExtendable,
        k: usize,
        _out_base: u8,
        in_base: u8,
    ) -> Self::HashTypeExtendable {
        assert!(in_base < 4);
        // K = 2
        // 00AABB => roll rev CC
        // 00CCAA

        let mask = get_mask(k);
        ExtCanonicalSeqHash(
            (hash.0 >> 2) | ((in_base as HashIntegerType) << ((k - 1) * 2)),
            ((hash.1 << 2) | (xrc(in_base as HashIntegerType))) & mask,
        )
    }

    fn manual_remove_only_forward(
        hash: Self::HashTypeExtendable,
        k: usize,
        _out_base: u8,
    ) -> Self::HashTypeExtendable {
        // K = 2
        // 00AABB => roll
        // 0000BB
        let mask = get_mask(k - 1);
        ExtCanonicalSeqHash(hash.0 & mask, hash.1 >> 2)
    }

    fn manual_remove_only_reverse(
        hash: Self::HashTypeExtendable,
        k: usize,
        _out_base: u8,
    ) -> Self::HashTypeExtendable {
        // K = 2
        // 00AABB => roll rev
        // 0000AA
        let mask = get_mask(k - 1);
        ExtCanonicalSeqHash(hash.0 >> 2, hash.1 & mask)
    }
}

// Returns the complement of a compressed format base
#[inline(always)]
fn xrc(base: HashIntegerType) -> HashIntegerType {
    base ^ 2
}

#[cfg(test)]
mod tests {
    use super::CanonicalSeqHashFactory;
    use super::HashIntegerType;
    use crate::hashes::tests::test_hash_function;
    use crate::hashes::{HashFunction, HashFunctionFactory};
    use std::mem::size_of;

    #[test]
    fn cn_seqhash_test() {
        test_hash_function::<CanonicalSeqHashFactory>(
            &(2..(size_of::<HashIntegerType>() * 4)).collect::<Vec<_>>(),
            true,
        );
    }
}
