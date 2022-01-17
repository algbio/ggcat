use crate::config::{BucketIndexType, MinimizerType, SortingHashType};
use crate::hashes::{
    ExtendableHashTraitType, HashFunction, HashFunctionFactory, HashableSequence,
    UnextendableHashTraitType,
};
use parking_lot::Mutex;
use std::intrinsics::unlikely;
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

fn fastexp(base: HashIntegerType, mut exp: usize) -> HashIntegerType {
    let mut result: HashIntegerType = 1;
    let mut sqv = base;

    while exp > 0 {
        if exp & 0x1 == 1 {
            result = result.wrapping_mul(sqv);
        }
        exp /= 2;
        sqv = sqv.wrapping_mul(sqv);
    }

    result
}

fn get_rmmult(k: usize) -> HashIntegerType {
    unsafe {
        static mut LAST_K: usize = 0;
        static mut LAST_RMMULT: HashIntegerType = 0;
        static CHANGE_LOCK: Mutex<()> = Mutex::new(());

        if unsafe { unlikely(LAST_K != k) } {
            let rmmult = fastexp(MULTIPLIER, k - 1);
            let _lock = CHANGE_LOCK.lock();
            LAST_RMMULT = rmmult;
            LAST_K = k;
            rmmult
        } else {
            LAST_RMMULT
        }
    }
}

impl<N: HashableSequence> ForwardRabinKarpHashIterator<N> {
    pub fn new(seq: N, k: usize) -> Result<ForwardRabinKarpHashIterator<N>, &'static str> {
        let mut fh = 0 as HashIntegerType;
        for i in 0..(k - 1) {
            fh = fh
                .wrapping_mul(MULTIPLIER)
                .wrapping_add(fwd_l(unsafe { seq.get_unchecked_cbase(i) }));
        }

        let rmmult = get_rmmult(k);

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
}

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

    fn new<N: HashableSequence>(seq: N, k: usize) -> Self::HashIterator<N> {
        ForwardRabinKarpHashIterator::new(seq, k).unwrap()
    }

    fn get_first_bucket(hash: Self::HashTypeUnextendable) -> BucketIndexType {
        hash as BucketIndexType
    }

    fn get_second_bucket(_hash: Self::HashTypeUnextendable) -> BucketIndexType {
        panic!("Not supported!")
    }

    fn get_sorting_hash(_hash: Self::HashTypeUnextendable) -> SortingHashType {
        panic!("Not supported!")
    }

    fn get_full_minimizer<const MASK: MinimizerType>(
        _hash: Self::HashTypeUnextendable,
    ) -> MinimizerType {
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
        out_base: u8,
        in_base: u8,
    ) -> Self::HashTypeExtendable {
        assert!(in_base < 4);
        // K = 2
        // 00AABB => roll CC
        // 00BBCC
        let rmmult = get_rmmult(k);
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
        let rmmult = get_rmmult(k);

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
        let rmmult = get_rmmult(k);

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
}

#[cfg(test)]
mod tests {
    use super::ForwardRabinKarpHashFactory;
    use crate::hashes::tests::test_hash_function;
    use crate::hashes::{HashFunction, HashFunctionFactory};

    #[test]
    fn fw_rkhash_test() {
        test_hash_function::<ForwardRabinKarpHashFactory>(&(2..4096).collect::<Vec<_>>(), false);
    }
}
