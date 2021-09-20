use crate::hash::{
    ExtendableHashTraitType, HashFunction, HashFunctionFactory, HashableSequence,
    UnextendableHashTraitType,
};
use crate::hashes::nthash_base::h;
use crate::types::{BucketIndexType, MinimizerType};

#[derive(Debug, Clone)]
pub struct ForwardNtHashIterator<N: HashableSequence> {
    seq: N,
    k_minus1: usize,
    fh: u64,
}

impl<N: HashableSequence> ForwardNtHashIterator<N> {
    /// Creates a new NtHashIterator1 with internal state properly initialized.
    pub fn new(seq: N, k: usize) -> Result<ForwardNtHashIterator<N>, &'static str> {
        if k > seq.bases_count() {
            return Err("K out of range!");
        }

        let mut fh = 0;
        for i in 0..(k - 1) {
            fh ^= unsafe { h(seq.get_unchecked_cbase(i)) }.rotate_left((k - i - 2) as u32);
        }

        Ok(ForwardNtHashIterator {
            seq,
            k_minus1: k - 1,
            fh,
        })
    }

    #[inline(always)]
    fn roll_hash(&mut self, i: usize) -> ExtForwardNtHash {
        let seqi_h = unsafe { h(self.seq.get_unchecked_cbase(i)) };
        let seqk_h = unsafe { h(self.seq.get_unchecked_cbase(i + self.k_minus1)) };

        let res = self.fh.rotate_left(1) ^ seqk_h;
        self.fh = res ^ seqi_h.rotate_left((self.k_minus1) as u32);
        ExtForwardNtHash(res)
    }
}

impl<N: HashableSequence> HashFunction<ForwardNtHashIteratorFactory> for ForwardNtHashIterator<N> {
    type IteratorType = impl Iterator<
        Item = <ForwardNtHashIteratorFactory as HashFunctionFactory>::HashTypeExtendable,
    >;
    type EnumerableIteratorType = impl Iterator<
        Item = (
            usize,
            <ForwardNtHashIteratorFactory as HashFunctionFactory>::HashTypeExtendable,
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

#[derive(Copy, Clone, Ord, PartialOrd, Eq, PartialEq, Debug)]
pub struct ForwardNtHashIteratorFactory;

#[repr(transparent)]
#[derive(Copy, Clone, Debug, Ord, PartialOrd, Eq, PartialEq)]
pub struct ExtForwardNtHash(u64);
impl ExtendableHashTraitType for ExtForwardNtHash {
    type HashTypeUnextendable = u64;
    #[inline(always)]
    fn to_unextendable(self) -> Self::HashTypeUnextendable {
        self.0
    }
}

impl HashFunctionFactory for ForwardNtHashIteratorFactory {
    type HashTypeUnextendable = u64;
    type HashTypeExtendable = ExtForwardNtHash;
    type HashIterator<N: HashableSequence> = ForwardNtHashIterator<N>;

    // Corresponds to 'N' hash (zero)
    const NULL_BASE: u8 = 4;

    #[inline(always)]
    fn new<N: HashableSequence>(seq: N, k: usize) -> Self::HashIterator<N> {
        ForwardNtHashIterator::new(seq, k).unwrap()
    }

    #[inline(always)]
    fn get_bucket(hash: Self::HashTypeUnextendable) -> u32 {
        hash as u32
    }

    #[inline(always)]
    fn get_second_bucket(hash: Self::HashTypeUnextendable) -> BucketIndexType {
        (hash >> 12) as u32
    }

    #[inline(always)]
    fn get_minimizer(hash: Self::HashTypeUnextendable) -> MinimizerType {
        hash
    }

    fn get_shifted(hash: Self::HashTypeUnextendable, shift: u8) -> u8 {
        (hash >> shift) as u8
    }

    #[inline(always)]
    fn manual_roll_forward(
        hash: Self::HashTypeExtendable,
        k: usize,
        out_base: u8,
        in_base: u8,
    ) -> Self::HashTypeExtendable {
        fwd_nt_manual_roll(hash, k, out_base, in_base)
    }

    #[inline(always)]
    fn manual_roll_reverse(
        hash: Self::HashTypeExtendable,
        k: usize,
        out_base: u8,
        in_base: u8,
    ) -> Self::HashTypeExtendable {
        fwd_nt_manual_roll_rev(hash, k, out_base, in_base)
    }

    #[inline(always)]
    fn manual_remove_only_forward(
        hash: Self::HashTypeExtendable,
        k: usize,
        out_base: u8,
    ) -> Self::HashTypeExtendable {
        ExtForwardNtHash(
            fwd_nt_manual_roll(hash, k, out_base, Self::NULL_BASE)
                .0
                .rotate_right(1),
        )
    }

    #[inline(always)]
    fn manual_remove_only_reverse(
        hash: Self::HashTypeExtendable,
        k: usize,
        out_base: u8,
    ) -> Self::HashTypeExtendable {
        fwd_nt_manual_roll_rev(hash, k, out_base, Self::NULL_BASE)
    }
}

#[inline(always)]
fn fwd_nt_manual_roll(hash: ExtForwardNtHash, k: usize, out_b: u8, in_b: u8) -> ExtForwardNtHash {
    let res = hash.0.rotate_left(1) ^ h(in_b);
    ExtForwardNtHash(res ^ h(out_b).rotate_left(k as u32))
}

#[inline(always)]
fn fwd_nt_manual_roll_rev(
    hash: ExtForwardNtHash,
    k: usize,
    out_b: u8,
    in_b: u8,
) -> ExtForwardNtHash {
    let res = hash.0 ^ h(in_b).rotate_left(k as u32);
    ExtForwardNtHash((res ^ h(out_b)).rotate_right(1))
}

#[cfg(test)]
mod tests {
    use crate::hash::tests::test_hash_function;
    use crate::hashes::fw_nthash::ForwardNtHashIteratorFactory;
    use std::mem::size_of;

    #[test]
    fn fw_nthash_test() {
        test_hash_function::<ForwardNtHashIteratorFactory>(&(2..4096).collect::<Vec<_>>(), false);
    }
}
