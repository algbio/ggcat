use crate::hash::{HashFunction, HashFunctionFactory, HashableSequence};
use crate::types::{BucketIndexType, MinimizerType};

pub const HASH_A: u64 = 0x3c8b_fbb3_95c6_0474;
pub const HASH_C: u64 = 0x3193_c185_62a0_2b4c;
pub const HASH_G: u64 = 0x2032_3ed0_8257_2324;
pub const HASH_T: u64 = 0x2955_49f5_4be2_4456;

#[inline(always)]
fn h(c: u8) -> u64 {
    unsafe { *H_LOOKUP.get_unchecked(c as usize) }
}

#[inline(always)]
fn rc(c: u8) -> u64 {
    unsafe { *RC_LOOKUP.get_unchecked(c as usize) }
}

const H_LOOKUP: [u64; 256] = {
    let mut lookup = [1; 256];

    // Support compressed reads transparently
    lookup[0 /*b'A'*/] = HASH_A;
    lookup[1 /*b'C'*/] = HASH_C;
    lookup[2 /*b'T'*/] = HASH_T;
    lookup[3 /*b'G'*/] = HASH_G;
    lookup[4 /*b'N'*/] = 0;

    lookup[b'A' as usize] = HASH_A;
    lookup[b'C' as usize] = HASH_C;
    lookup[b'G' as usize] = HASH_G;
    lookup[b'T' as usize] = HASH_T;
    lookup[b'N' as usize] = 0;
    lookup
};

const RC_LOOKUP: [u64; 256] = {
    let mut lookup = [1; 256];

    // Support compressed reads transparently
    lookup[0 /*b'A'*/] = HASH_T;
    lookup[1 /*b'C'*/] = HASH_G;
    lookup[2 /*b'T'*/] = HASH_A;
    lookup[3 /*b'G'*/] = HASH_C;
    lookup[4 /*b'N'*/] = 0;

    lookup[b'A' as usize] = HASH_T;
    lookup[b'C' as usize] = HASH_G;
    lookup[b'G' as usize] = HASH_C;
    lookup[b'T' as usize] = HASH_A;
    lookup[b'N' as usize] = 0;
    lookup
};

#[derive(Debug, Clone)]
pub struct NtHashIterator<N: HashableSequence> {
    seq: N,
    k_minus1: usize,
    fh: u64,
}

impl<N: HashableSequence> NtHashIterator<N> {
    /// Creates a new NtHashIterator1 with internal state properly initialized.
    pub fn new(seq: N, k: usize) -> Result<NtHashIterator<N>, &'static str> {
        if k > seq.bases_count() {
            return Err("K out of range!");
        }

        let mut fh = 0;
        for i in 0..(k - 1) {
            fh ^= unsafe { h(seq.get_unchecked_cbase(i)) }.rotate_left((k - i - 2) as u32);
        }

        Ok(NtHashIterator {
            seq,
            k_minus1: k - 1,
            fh,
        })
    }

    #[inline(always)]
    fn roll_hash(&mut self, i: usize) -> u64 {
        let seqi_h = unsafe { h(self.seq.get_unchecked_cbase(i)) };
        let seqk_h = unsafe { h(self.seq.get_unchecked_cbase(i + self.k_minus1)) };

        let res = self.fh.rotate_left(1) ^ seqk_h;
        self.fh = res ^ seqi_h.rotate_left((self.k_minus1) as u32);
        res
    }
}

impl<N: HashableSequence> HashFunction<NtHashIteratorFactory> for NtHashIterator<N> {
    type IteratorType =
        impl Iterator<Item = <NtHashIteratorFactory as HashFunctionFactory>::HashType>;
    type EnumerableIteratorType = impl Iterator<
        Item = (
            usize,
            <NtHashIteratorFactory as HashFunctionFactory>::HashType,
        ),
    >;

    #[inline(always)]
    fn iter(mut self) -> Self::IteratorType {
        (0..self.seq.bases_count() - self.k_minus1).map(move |idx| self.roll_hash(idx))
    }

    #[inline(always)]
    fn iter_enumerate(mut self) -> Self::EnumerableIteratorType {
        (0..self.seq.bases_count() - self.k_minus1).map(move |idx| (idx, self.roll_hash(idx)))
    }
}

#[derive(Copy, Clone)]
pub struct NtHashIteratorFactory;

impl HashFunctionFactory for NtHashIteratorFactory {
    type HashType = u64;
    type HashIterator<N: HashableSequence> = NtHashIterator<N>;

    // Corresponds to 'N' hash (zero)
    const NULL_BASE: u8 = 4;

    #[inline(always)]
    fn new<N: HashableSequence>(seq: N, k: usize) -> Self::HashIterator<N> {
        NtHashIterator::new(seq, k).unwrap()
    }

    #[inline(always)]
    fn get_bucket(hash: Self::HashType) -> u32 {
        hash as u32
    }

    #[inline(always)]
    fn get_second_bucket(hash: Self::HashType) -> BucketIndexType {
        (hash >> 12) as u32
    }

    #[inline(always)]
    fn get_minimizer(hash: Self::HashType) -> MinimizerType {
        hash
    }

    fn get_shifted(hash: Self::HashType, shift: u8) -> u8 {
        (hash >> shift) as u8
    }

    #[inline(always)]
    fn manual_roll_forward(
        hash: Self::HashType,
        k: usize,
        out_base: u8,
        in_base: u8,
    ) -> Self::HashType {
        nt_manual_roll(hash, k, out_base, in_base)
    }

    #[inline(always)]
    fn manual_roll_reverse(
        hash: Self::HashType,
        k: usize,
        out_base: u8,
        in_base: u8,
    ) -> Self::HashType {
        nt_manual_roll_rev(hash, k, out_base, in_base)
    }

    #[inline(always)]
    fn manual_remove_only_forward(hash: Self::HashType, k: usize, out_base: u8) -> Self::HashType {
        nt_manual_roll(hash, k, out_base, Self::NULL_BASE).rotate_right(1)
    }

    #[inline(always)]
    fn manual_remove_only_reverse(hash: Self::HashType, k: usize, out_base: u8) -> Self::HashType {
        nt_manual_roll_rev(hash, k, out_base, Self::NULL_BASE)
    }
}

#[inline(always)]
pub fn nt_manual_roll(hash: u64, klen: usize, out_b: u8, in_b: u8) -> u64 {
    let res = hash.rotate_left(1) ^ h(in_b);
    return res ^ h(out_b).rotate_left(klen as u32);
}

#[inline(always)]
pub fn nt_manual_roll_rev(hash: u64, klen: usize, out_b: u8, in_b: u8) -> u64 {
    let res = hash ^ h(in_b).rotate_left(klen as u32);
    (res ^ h(out_b)).rotate_right(1)
}
