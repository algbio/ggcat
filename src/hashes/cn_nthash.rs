use crate::hash::{
    ExtendableHashTraitType, HashFunction, HashFunctionFactory, HashableSequence,
    UnextendableHashTraitType,
};
use crate::hashes::nthash_base::{h, rc};
use crate::types::{BucketIndexType, MinimizerType};
use std::cmp::min;

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
            fh ^= unsafe { h(seq.get_unchecked_cbase(i)) }.rotate_left((k - i - 2) as u32);
            bw ^= unsafe { rc(seq.get_unchecked_cbase(i)) }.rotate_left(i as u32);
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

        let seqi_h = h(base_i);
        let seqk_h = h(base_k);
        let seqi_rc = rc(base_i);
        let seqk_rc = rc(base_k);

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
    type IteratorType = impl Iterator<
        Item = <CanonicalNtHashIteratorFactory as HashFunctionFactory>::HashTypeExtendable,
    >;
    type EnumerableIteratorType = impl Iterator<
        Item = (
            usize,
            <CanonicalNtHashIteratorFactory as HashFunctionFactory>::HashTypeExtendable,
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
pub struct CanonicalNtHashIteratorFactory;

#[derive(Copy, Clone, Debug, Ord, PartialOrd, Eq, PartialEq)]
pub struct ExtCanonicalNtHash(u64, u64);
impl ExtendableHashTraitType for ExtCanonicalNtHash {
    type HashTypeUnextendable = u64;
    #[inline(always)]
    fn to_unextendable(self) -> Self::HashTypeUnextendable {
        min(self.0, self.1)
    }
}

impl HashFunctionFactory for CanonicalNtHashIteratorFactory {
    type HashTypeUnextendable = u64;
    type HashTypeExtendable = ExtCanonicalNtHash;
    type HashIterator<N: HashableSequence> = CanonicalNtHashIterator<N>;

    // Corresponds to 'N' hash (zero)
    const NULL_BASE: u8 = 4;

    #[inline(always)]
    fn new<N: HashableSequence>(seq: N, k: usize) -> Self::HashIterator<N> {
        CanonicalNtHashIterator::new(seq, k).unwrap()
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
        cnc_nt_manual_roll(hash, k - 1, out_base, in_base)
    }

    #[inline(always)]
    fn manual_roll_reverse(
        hash: Self::HashTypeExtendable,
        k: usize,
        out_base: u8,
        in_base: u8,
    ) -> Self::HashTypeExtendable {
        cnc_nt_manual_roll_rev(hash, k - 1, out_base, in_base)
    }

    #[inline(always)]
    fn manual_remove_only_forward(
        hash: Self::HashTypeExtendable,
        k: usize,
        out_base: u8,
    ) -> Self::HashTypeExtendable {
        let ExtCanonicalNtHash(fw, rc) = cnc_nt_manual_roll(hash, k - 1, out_base, Self::NULL_BASE);
        ExtCanonicalNtHash(fw.rotate_right(1), rc)
    }

    #[inline(always)]
    fn manual_remove_only_reverse(
        hash: Self::HashTypeExtendable,
        k: usize,
        out_base: u8,
    ) -> Self::HashTypeExtendable {
        let ExtCanonicalNtHash(fw, rc) =
            cnc_nt_manual_roll_rev(hash, k - 1, out_base, Self::NULL_BASE);
        ExtCanonicalNtHash(fw, rc.rotate_right(1))
    }
}

#[inline(always)]
fn cnc_nt_manual_roll(
    hash: ExtCanonicalNtHash,
    kminus_1: usize,
    out_b: u8,
    in_b: u8,
) -> ExtCanonicalNtHash {
    let res = hash.0.rotate_left(1) ^ h(in_b);
    let res_rc = hash.0 ^ rc(in_b).rotate_left(kminus_1 as u32);

    ExtCanonicalNtHash(
        res ^ h(out_b).rotate_left(kminus_1 as u32),
        (res_rc ^ rc(out_b)).rotate_right(1),
    )
}

#[inline(always)]
fn cnc_nt_manual_roll_rev(
    hash: ExtCanonicalNtHash,
    kminus_1: usize,
    out_b: u8,
    in_b: u8,
) -> ExtCanonicalNtHash {
    let res = hash.0 ^ h(in_b).rotate_left(kminus_1 as u32);
    let res_rc = hash.0.rotate_left(1) ^ rc(in_b);
    ExtCanonicalNtHash(
        (res ^ h(out_b)).rotate_right(1),
        res_rc ^ rc(out_b).rotate_left(kminus_1 as u32),
    )
}

#[cfg(test)]
mod tests {
    use crate::hash::{ExtendableHashTraitType, HashFunction, HashFunctionFactory};
    use crate::hashes::cn_nthash::CanonicalNtHashIteratorFactory;
    use crate::rolling_minqueue::RollingMinQueue;

    #[test]
    fn cn_nthash_test() {
        let first = CanonicalNtHashIteratorFactory::new(
            "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAG".as_bytes(),
            15,
        ); //"ACGTACGTTTCTACCA".as_bytes(), 16);
        println!("AAAA");
        let second = CanonicalNtHashIteratorFactory::new(
            "CTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTT".as_bytes(),
            15,
        ); //"TGGTAGAAACGTACGT".as_bytes(), 16);

        let mut minimizer_queue = RollingMinQueue::<CanonicalNtHashIteratorFactory>::new(63 - 15);
        let mut rolling_iter =
            minimizer_queue.make_iter(first.clone().iter().map(|x| x.to_unextendable()));

        println!("A raw: {:x?}", first.clone().iter().collect::<Vec<_>>());

        println!(
            "A unext: {:x?}",
            first
                .clone()
                .iter()
                .map(|x| x.to_unextendable())
                .collect::<Vec<_>>()
        );
        println!("A {:x?}", rolling_iter.collect::<Vec<_>>());

        println!("B raw: {:x?}", second.clone().iter().collect::<Vec<_>>());

        println!(
            "B unext: {:x?}",
            second
                .clone()
                .iter()
                .map(|x| x.to_unextendable())
                .collect::<Vec<_>>()
        );

        let mut rolling_iter_second =
            minimizer_queue.make_iter(second.iter().map(|x| x.to_unextendable()));
        let mut second = rolling_iter_second.collect::<Vec<_>>();
        second.reverse();

        println!("B {:x?}", second);
    }
}
