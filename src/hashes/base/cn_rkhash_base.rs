use crate::config::{BucketIndexType, MinimizerType, SortingHashType};
use crate::hashes::{ExtendableHashTraitType, HashFunction, HashFunctionFactory, HashableSequence};
use nightly_quirks::branch_pred::unlikely;
use nightly_quirks::utils::NightlyUtils;
use parking_lot::Mutex;
use std::cmp::min;

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
        static CHANGE_LOCK: Mutex<()> = NightlyUtils::new_mutex(());

        if unlikely(LAST_K != k) {
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

impl<N: HashableSequence> CanonicalRabinKarpHashIterator<N> {
    pub fn new(seq: N, k: usize) -> Result<CanonicalRabinKarpHashIterator<N>, &'static str> {
        let mut fh = 0;
        let mut bw = 0;
        for i in 0..(k - 1) {
            fh = fh * MULTIPLIER + fwd_l(unsafe { seq.get_unchecked_cbase(i) });
        }

        for i in (0..(k - 1)).rev() {
            bw = bw * MULTIPLIER + bkw_l(unsafe { seq.get_unchecked_cbase(i) });
        }
        bw *= MULTIPLIER;

        let rmmult = get_rmmult(k);

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
    type IteratorType = impl Iterator<
        Item = <CanonicalRabinKarpHashFactory as HashFunctionFactory>::HashTypeExtendable,
    >;
    type EnumerableIteratorType = impl Iterator<
        Item = (
            usize,
            <CanonicalRabinKarpHashFactory as HashFunctionFactory>::HashTypeExtendable,
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
}

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

    fn new<N: HashableSequence>(seq: N, k: usize) -> Self::HashIterator<N> {
        CanonicalRabinKarpHashIterator::new(seq, k).unwrap()
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
        // K = 2
        // 00AABB => roll CC
        // 00BBCC
        let rmmult = get_rmmult(k);
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
        let rmmult = get_rmmult(k);

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
        let rmmult = get_rmmult(k);

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
        let rmmult = get_rmmult(k);

        ExtCanonicalRabinKarpHash(
            hash.0.wrapping_sub(fwd_l(out_base)).wrapping_mul(MULT_INV),
            hash.1.wrapping_sub(rmmult.wrapping_mul(bkw_l(out_base))),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::CanonicalRabinKarpHashFactory;
    use crate::hashes::tests::test_hash_function;
    use crate::hashes::{HashFunction, HashFunctionFactory};

    #[test]
    fn cn_rkhash_test() {
        test_hash_function::<CanonicalRabinKarpHashFactory>(&(2..4096).collect::<Vec<_>>(), true);
    }
}
