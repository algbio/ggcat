use crate::{
    init_rmmult, ExtendableHashTraitType, HashFunction, HashFunctionFactory, HashableSequence,
    RMMULT_CACHE_SIZE,
};
use config::BucketIndexType;
use static_dispatch::static_dispatch;
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

#[inline(always)]
pub fn fwd_l(c: u8) -> HashIntegerType {
    unsafe { *FWD_LOOKUP.get_unchecked(c as usize) }
}

pub struct ForwardRabinKarpHashIterator<N: HashableSequence> {
    seq: N,
    rmmult: HashIntegerType,
    fh: HashIntegerType,
    k_minus1: usize,
}

impl<N: HashableSequence> ForwardRabinKarpHashIterator<N> {
    pub fn new(seq: N, k: usize) -> Result<ForwardRabinKarpHashIterator<N>, &'static str> {
        let mut fh = 0 as HashIntegerType;
        for i in 0..(k - 1) {
            fh = fh
                .wrapping_mul(MULTIPLIER)
                .wrapping_add(fwd_l(unsafe { seq.get_unchecked_cbase(i) }));
        }

        let rmmult = get_rmmult(k) as HashIntegerType;

        Ok(ForwardRabinKarpHashIterator {
            seq,
            rmmult,
            fh,
            k_minus1: k - 1,
        })
    }

    #[inline(always)]
    fn roll_hash(&mut self, index: usize) -> ExtForwardRabinKarpHash {
        let current = self
            .fh
            .wrapping_mul(MULTIPLIER)
            .wrapping_add(fwd_l(unsafe { self.seq.get_unchecked_cbase(index) }));
        self.fh = current.wrapping_sub(
            fwd_l(unsafe { self.seq.get_unchecked_cbase(index - self.k_minus1) })
                .wrapping_mul(self.rmmult),
        );
        ExtForwardRabinKarpHash(current)
    }
}

impl<N: HashableSequence> HashFunction<ForwardRabinKarpHashFactory>
    for ForwardRabinKarpHashIterator<N>
{
    type IteratorType = impl Iterator<
        Item = <ForwardRabinKarpHashFactory as HashFunctionFactory>::HashTypeExtendable,
    >;
    type EnumerableIteratorType = impl Iterator<
        Item = (
            usize,
            <ForwardRabinKarpHashFactory as HashFunctionFactory>::HashTypeExtendable,
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
pub struct ForwardRabinKarpHashFactory;

#[repr(transparent)]
#[derive(Copy, Clone, Debug, Ord, PartialOrd, Eq, PartialEq)]
pub struct ExtForwardRabinKarpHash(HashIntegerType);

impl ExtendableHashTraitType for ExtForwardRabinKarpHash {
    type HashTypeUnextendable = HashIntegerType;

    #[inline(always)]
    fn to_unextendable(self) -> Self::HashTypeUnextendable {
        self.0
    }

    #[inline(always)]
    fn is_forward(&self) -> bool {
        true
    }
}

static mut RMMULT_CACHE: [u128; RMMULT_CACHE_SIZE] = [0; RMMULT_CACHE_SIZE];
#[inline(always)]
fn get_rmmult(k: usize) -> u128 {
    unsafe { RMMULT_CACHE[k % RMMULT_CACHE_SIZE] }
}

#[static_dispatch]
impl HashFunctionFactory for ForwardRabinKarpHashFactory {
    type HashTypeUnextendable = HashIntegerType;
    type HashTypeExtendable = ExtForwardRabinKarpHash;
    type HashIterator<N: HashableSequence> = ForwardRabinKarpHashIterator<N>;
    type PreferredRandomState = DummyHasherBuilder;

    #[inline(always)]
    fn get_random_state() -> Self::PreferredRandomState {
        DummyHasherBuilder {}
    }

    const NULL_BASE: u8 = 0;
    const USABLE_HASH_BITS: usize = size_of::<Self::HashTypeUnextendable>() * 8 - 1; // -1 because the hash is always odd

    fn initialize(k: usize) {
        unsafe {
            RMMULT_CACHE = init_rmmult(k, MULTIPLIER as u128);
        }
    }

    fn new<N: HashableSequence>(seq: N, k: usize) -> Self::HashIterator<N> {
        ForwardRabinKarpHashIterator::new(seq, k).unwrap()
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
        assert!(in_base < 4);
        // K = 2
        // 00AABB => roll CC
        // 00BBCC
        let rmmult = get_rmmult(k) as HashIntegerType;
        ExtForwardRabinKarpHash(
            (hash.0.wrapping_sub(fwd_l(out_base).wrapping_mul(rmmult)))
                .wrapping_mul(MULTIPLIER)
                .wrapping_add(fwd_l(in_base)),
        )
    }

    fn manual_roll_reverse(
        hash: Self::HashTypeExtendable,
        k: usize,
        out_base: u8,
        in_base: u8,
    ) -> Self::HashTypeExtendable {
        assert!(in_base < 4);
        // K = 2
        // 00AABB => roll rev CC
        // 00CCAA
        let rmmult = get_rmmult(k) as HashIntegerType;

        ExtForwardRabinKarpHash(
            (hash.0.wrapping_sub(fwd_l(out_base)))
                .wrapping_mul(MULT_INV)
                .wrapping_add(fwd_l(in_base).wrapping_mul(rmmult)),
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

        ExtForwardRabinKarpHash(hash.0.wrapping_sub(rmmult.wrapping_mul(fwd_l(out_base))))
    }

    fn manual_remove_only_reverse(
        hash: Self::HashTypeExtendable,
        _k: usize,
        out_base: u8,
    ) -> Self::HashTypeExtendable {
        // K = 2
        // 00AABB => roll rev
        // 0000AA
        ExtForwardRabinKarpHash(hash.0.wrapping_sub(fwd_l(out_base)).wrapping_mul(MULT_INV))
    }

    const INVERTIBLE: bool = false;
    fn invert(_hash: Self::HashTypeUnextendable, _k: usize, _out_buf: &mut [u8]) {
        unimplemented!()
    }
}

#[cfg(test)]
mod tests {
    use super::ForwardRabinKarpHashFactory;
    use crate::tests::test_hash_function;

    #[test]
    fn fw_rkhash_test() {
        test_hash_function::<ForwardRabinKarpHashFactory>(&(2..4096).collect::<Vec<_>>(), false);
    }
}
