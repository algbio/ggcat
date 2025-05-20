use crate::{
    init_rmmult, ExtendableHashTraitType, HashFunction, HashFunctionFactory, HashableSequence,
    RMMULT_CACHE_SIZE,
};
use config::BucketIndexType;
use dynamic_dispatch::dynamic_dispatch;
use std::cmp::min;
use std::mem::size_of;

const FWD_LOOKUP: [HashIntegerType; 256] = {
    let mut lookup = [1; 256];

    // Support compressed reads transparently
    lookup[0 /*b'A'*/] = MULT_A;
    lookup[1 /*b'C'*/] = MULT_C;
    lookup[2 /*b'T'*/] = MULT_T;
    lookup[3 /*b'G'*/] = MULT_G;
    lookup[4 /*b'N'*/] = 0;

    lookup[b'A' as usize] = MULT_A;
    lookup[b'C' as usize] = MULT_C;
    lookup[b'G' as usize] = MULT_G;
    lookup[b'T' as usize] = MULT_T;
    lookup[b'N' as usize] = 0;
    lookup
};

const BKW_LOOKUP: [HashIntegerType; 256] = {
    let mut lookup = [1; 256];

    // Support compressed reads transparently
    lookup[0 /*b'A'*/] = MULT_T;
    lookup[1 /*b'C'*/] = MULT_G;
    lookup[2 /*b'T'*/] = MULT_A;
    lookup[3 /*b'G'*/] = MULT_C;
    lookup[4 /*b'N'*/] = 0;

    lookup[b'A' as usize] = MULT_T;
    lookup[b'C' as usize] = MULT_G;
    lookup[b'G' as usize] = MULT_C;
    lookup[b'T' as usize] = MULT_A;
    lookup[b'N' as usize] = 0;
    lookup
};

#[inline(always)]
pub fn fwd_l(c: u8) -> HashIntegerType {
    unsafe { *FWD_LOOKUP.get_unchecked(c as usize) }
}

#[inline(always)]
pub fn bkw_l(c: u8) -> HashIntegerType {
    unsafe { *BKW_LOOKUP.get_unchecked(c as usize) }
}

pub struct CanonicalRabinKarpHashIterator<N: HashableSequence> {
    seq: N,
    rmmult: HashIntegerType,
    fh: HashIntegerType,
    rc: HashIntegerType,
    k_minus1: usize,
}

impl<N: HashableSequence> CanonicalRabinKarpHashIterator<N> {
    pub fn new(seq: N, k: usize) -> Result<CanonicalRabinKarpHashIterator<N>, &'static str> {
        let mut fh: HashIntegerType = 0;
        let mut bw: HashIntegerType = 0;
        for i in 0..(k - 1) {
            fh = fh
                .wrapping_mul(MULTIPLIER)
                .wrapping_add(fwd_l(unsafe { seq.get_unchecked_cbase(i) }));
        }

        for i in (0..(k - 1)).rev() {
            bw = bw
                .wrapping_mul(MULTIPLIER)
                .wrapping_add(bkw_l(unsafe { seq.get_unchecked_cbase(i) }));
        }
        bw = bw.wrapping_mul(MULTIPLIER);

        let rmmult = get_rmmult(k) as HashIntegerType;

        Ok(CanonicalRabinKarpHashIterator {
            seq,
            rmmult,
            fh,
            rc: bw,
            k_minus1: k - 1,
        })
    }

    #[inline(always)]
    fn roll_hash(&mut self, index: usize) -> ExtCanonicalRabinKarpHash {
        let in_base = unsafe { self.seq.get_unchecked_cbase(index) };
        let out_base = unsafe { self.seq.get_unchecked_cbase(index - self.k_minus1) };

        let current_fh = self
            .fh
            .wrapping_mul(MULTIPLIER)
            .wrapping_add(fwd_l(in_base));
        self.fh = current_fh.wrapping_sub(fwd_l(out_base).wrapping_mul(self.rmmult));

        let current_bk = self
            .rc
            .wrapping_mul(MULT_INV)
            .wrapping_add(bkw_l(in_base).wrapping_mul(self.rmmult));
        self.rc = current_bk.wrapping_sub(bkw_l(out_base));
        ExtCanonicalRabinKarpHash(current_fh, current_bk)
    }
}

impl<N: HashableSequence> HashFunction<CanonicalRabinKarpHashFactory>
    for CanonicalRabinKarpHashIterator<N>
{
    fn iter(
        mut self,
    ) -> impl ExactSizeIterator
           + Iterator<
        Item = <CanonicalRabinKarpHashFactory as HashFunctionFactory>::HashTypeExtendable,
    > {
        (self.k_minus1..self.seq.bases_count()).map(move |idx| self.roll_hash(idx))
    }

    fn iter_enumerate(
        mut self,
    ) -> impl ExactSizeIterator
           + Iterator<
        Item = (
            usize,
            <CanonicalRabinKarpHashFactory as HashFunctionFactory>::HashTypeExtendable,
        ),
    > {
        (self.k_minus1..self.seq.bases_count())
            .map(move |idx| (idx - self.k_minus1, self.roll_hash(idx)))
    }
}

#[derive(Copy, Clone, Ord, PartialOrd, Eq, PartialEq, Debug)]
pub struct CanonicalRabinKarpHashFactory;

#[derive(Copy, Clone, Debug, Ord, PartialOrd, Eq, PartialEq)]
pub struct ExtCanonicalRabinKarpHash(HashIntegerType, HashIntegerType);

impl ExtendableHashTraitType for ExtCanonicalRabinKarpHash {
    type HashTypeUnextendable = HashIntegerType;

    #[inline(always)]
    fn to_unextendable(self) -> Self::HashTypeUnextendable {
        min(self.0, self.1)
    }

    #[inline(always)]
    fn is_forward(&self) -> bool {
        self.0 < self.1
    }

    fn is_rc_symmetric(&self) -> bool {
        self.0 == self.1
    }
}

static mut RMMULT_CACHE: [u128; RMMULT_CACHE_SIZE] = [0; RMMULT_CACHE_SIZE];
#[inline(always)]
fn get_rmmult(k: usize) -> u128 {
    unsafe { RMMULT_CACHE[k % RMMULT_CACHE_SIZE] }
}

#[dynamic_dispatch]
impl HashFunctionFactory for CanonicalRabinKarpHashFactory {
    type HashTypeUnextendable = HashIntegerType;
    type HashTypeExtendable = ExtCanonicalRabinKarpHash;
    type HashIterator<N: HashableSequence> = CanonicalRabinKarpHashIterator<N>;
    type PreferredRandomState = DummyHasherBuilder;

    #[inline(always)]
    fn get_random_state() -> Self::PreferredRandomState {
        DummyHasherBuilder
    }

    const NULL_BASE: u8 = 0;
    const USABLE_HASH_BITS: usize = size_of::<Self::HashTypeUnextendable>() * 8 - 1; // -1 because the hash is always odd

    fn initialize(k: usize) {
        unsafe {
            RMMULT_CACHE = init_rmmult(k, MULTIPLIER as u128);
        }
    }

    fn new<N: HashableSequence>(seq: N, k: usize) -> Self::HashIterator<N> {
        CanonicalRabinKarpHashIterator::new(seq, k).unwrap()
    }

    #[inline(always)]
    fn get_bucket(
        used_bits: usize,
        requested_bits: usize,
        hash: Self::HashTypeUnextendable,
    ) -> BucketIndexType {
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

    fn manual_roll_forward(
        hash: Self::HashTypeExtendable,
        k: usize,
        out_base: u8,
        in_base: u8,
    ) -> Self::HashTypeExtendable {
        // K = 2
        // 00AABB => roll CC
        // 00BBCC
        let rmmult = get_rmmult(k) as HashIntegerType;
        ExtCanonicalRabinKarpHash(
            hash.0
                .wrapping_sub(fwd_l(out_base).wrapping_mul(rmmult))
                .wrapping_mul(MULTIPLIER)
                .wrapping_add(fwd_l(in_base)),
            (hash.1.wrapping_sub(bkw_l(out_base)))
                .wrapping_mul(MULT_INV)
                .wrapping_add(bkw_l(in_base).wrapping_mul(rmmult)),
        )
    }

    fn manual_roll_reverse(
        hash: Self::HashTypeExtendable,
        k: usize,
        out_base: u8,
        in_base: u8,
    ) -> Self::HashTypeExtendable {
        // K = 2
        // 00AABB => roll rev CC
        // 00CCAA
        let rmmult = get_rmmult(k) as HashIntegerType;

        ExtCanonicalRabinKarpHash(
            (hash.0.wrapping_sub(fwd_l(out_base)))
                .wrapping_mul(MULT_INV)
                .wrapping_add(fwd_l(in_base).wrapping_mul(rmmult)),
            hash.1
                .wrapping_sub(bkw_l(out_base).wrapping_mul(rmmult))
                .wrapping_mul(MULTIPLIER)
                .wrapping_add(bkw_l(in_base)),
        )
    }

    fn manual_remove_only_forward(
        hash: Self::HashTypeExtendable,
        k: usize,
        out_base: u8,
    ) -> Self::HashTypeExtendable {
        // K = 2
        // 00AABB => roll
        // 0000BB
        let rmmult = get_rmmult(k) as HashIntegerType;

        ExtCanonicalRabinKarpHash(
            hash.0.wrapping_sub(rmmult.wrapping_mul(fwd_l(out_base))),
            hash.1.wrapping_sub(bkw_l(out_base)).wrapping_mul(MULT_INV),
        )
    }

    fn manual_remove_only_reverse(
        hash: Self::HashTypeExtendable,
        k: usize,
        out_base: u8,
    ) -> Self::HashTypeExtendable {
        // K = 2
        // 00AABB => roll rev
        // 0000AA
        let rmmult = get_rmmult(k) as HashIntegerType;

        ExtCanonicalRabinKarpHash(
            hash.0.wrapping_sub(fwd_l(out_base)).wrapping_mul(MULT_INV),
            hash.1.wrapping_sub(rmmult.wrapping_mul(bkw_l(out_base))),
        )
    }

    const INVERTIBLE: bool = false;
    type SeqType = [u8; 0];
    fn invert(_hash: Self::HashTypeUnextendable) -> Self::SeqType {
        unimplemented!()
    }
}

#[cfg(test)]
mod tests {
    use super::CanonicalRabinKarpHashFactory;
    use crate::tests::test_hash_function;

    #[test]
    fn cn_rkhash_test() {
        test_hash_function::<CanonicalRabinKarpHashFactory>(&(2..4096).collect::<Vec<_>>(), true);
    }
}
