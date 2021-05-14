use crate::hash::{HashFunction, HashFunctionFactory, HashableSequence};
use crate::types::MinimizerType;

pub struct SeqHashIterator<N: HashableSequence> {
    seq: N,
    mask: u64,
    fh: u64,
    k_minus1: usize,
}

#[inline(always)]
fn get_mask(k: usize) -> u64 {
    u64::MAX >> (((32 - k) * 2) as u64)
}

impl<N: HashableSequence> SeqHashIterator<N> {
    pub fn new(seq: N, k: usize) -> Result<SeqHashIterator<N>, &'static str> {
        if k > seq.bases_count() || k > 32 {
            return Err("K out of range!");
        }

        let mut fh = 0;
        for i in 0..(k - 1) {
            fh = (fh << 2) ^ unsafe { seq.get_unchecked_cbase(i) as u64 };
        }

        let mask = get_mask(k);

        Ok(SeqHashIterator {
            seq,
            mask,
            fh: fh & mask,
            k_minus1: k - 1,
        })
    }

    #[inline(always)]
    fn roll_hash(&mut self, index: usize) -> u64 {
        assert!(unsafe { self.seq.get_unchecked_cbase(index) } < 4);

        self.fh =
            ((self.fh << 2) ^ unsafe { self.seq.get_unchecked_cbase(index) as u64 }) & self.mask;
        self.fh
    }
}

impl<N: HashableSequence> HashFunction<SeqHashFactory> for SeqHashIterator<N> {
    type IteratorType = impl Iterator<Item = <SeqHashFactory as HashFunctionFactory>::HashType>;
    type EnumerableIteratorType =
        impl Iterator<Item = (usize, <SeqHashFactory as HashFunctionFactory>::HashType)>;

    fn iter(mut self) -> Self::IteratorType {
        (self.k_minus1..self.seq.bases_count()).map(move |idx| self.roll_hash(idx))
    }

    fn iter_enumerate(mut self) -> Self::EnumerableIteratorType {
        (self.k_minus1..self.seq.bases_count()).map(move |idx| (idx, self.roll_hash(idx)))
    }
}

#[derive(Copy, Clone)]
pub struct SeqHashFactory;

impl HashFunctionFactory for SeqHashFactory {
    type HashType = u64;
    type HashIterator<N: HashableSequence> = SeqHashIterator<N>;
    const NULL_BASE: u8 = 0;

    fn new<N: HashableSequence>(seq: N, k: usize) -> Self::HashIterator<N> {
        SeqHashIterator::new(seq, k).unwrap()
    }

    fn get_bucket(hash: Self::HashType) -> u32 {
        let mut x = hash;
        // x ^= x >> 12; // a
        // x ^= x << 25; // b
        // x ^= x >> 27; // c
        x as u32
    }

    fn get_second_bucket(hash: Self::HashType) -> u32 {
        panic!("Not supported!")
    }

    fn get_minimizer(hash: Self::HashType) -> MinimizerType {
        panic!("Not supported!")
    }

    fn get_shifted(hash: Self::HashType, shift: u8) -> u8 {
        (hash >> shift) as u8
    }

    fn manual_roll_forward(
        hash: Self::HashType,
        k: usize,
        _out_base: u8,
        in_base: u8,
    ) -> Self::HashType {
        assert!(in_base < 4);
        // K = 2
        // 00AABB => roll CC
        // 00BBCC

        let mask = get_mask(k);
        ((hash << 2) | (in_base as u64)) & mask
    }

    fn manual_roll_reverse(
        hash: Self::HashType,
        k: usize,
        _out_base: u8,
        in_base: u8,
    ) -> Self::HashType {
        assert!(in_base < 4);
        // K = 2
        // 00AABB => roll rev CC
        // 00CCAA

        ((hash >> 2) | ((in_base as u64) << ((k - 1) * 2)))
    }

    fn manual_remove_only_forward(hash: Self::HashType, k: usize, _out_base: u8) -> Self::HashType {
        // K = 2
        // 00AABB => roll
        // 0000BB
        let mask = get_mask(k - 1);
        hash & mask
    }

    fn manual_remove_only_reverse(hash: Self::HashType, k: usize, _out_base: u8) -> Self::HashType {
        // K = 2
        // 00AABB => roll rev
        // 0000AA
        hash >> 2
    }
}
