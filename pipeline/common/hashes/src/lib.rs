#![feature(type_alias_impl_trait)]
#![feature(trait_alias)]
#![feature(const_type_id)]
#![feature(int_roundings)]

use static_dispatch::static_dispatch;

pub mod cn_nthash;
pub mod cn_seqhash;
pub mod fw_nthash;
pub mod fw_seqhash;
mod nthash_base;

pub mod cn_rkhash;
pub mod dummy_hasher;
pub mod fw_rkhash;
pub mod rolling;

use std::fmt::{Debug, Display};
use std::hash::{BuildHasher, Hash};

use serde::de::DeserializeOwned;
use serde::Serialize;

use config::{BucketIndexType, MinimizerType};

pub trait UnextendableHashTraitType = Copy
    + Clone
    + Debug
    + Default
    + Display
    + Eq
    + Ord
    + Hash
    + Send
    + Sync
    + Serialize
    + DeserializeOwned
    + 'static;

pub trait ExtendableHashTraitType: Copy + Clone + Debug + Eq + Ord + Send + Sync {
    type HashTypeUnextendable: UnextendableHashTraitType;
    fn to_unextendable(self) -> Self::HashTypeUnextendable;
    fn is_forward(&self) -> bool;
    fn is_rc_symmetric(&self) -> bool;
}

#[static_dispatch]
pub trait HashFunctionFactory: Sized + Clone + Debug + Send + Sync + 'static {
    type HashTypeUnextendable: UnextendableHashTraitType;
    type HashTypeExtendable: ExtendableHashTraitType<
        HashTypeUnextendable = Self::HashTypeUnextendable,
    >;
    type HashIterator<N: HashableSequence>: HashFunction<Self>;

    type PreferredRandomState: BuildHasher;
    fn get_random_state() -> Self::PreferredRandomState;

    const NULL_BASE: u8;
    const USABLE_HASH_BITS: usize;

    fn initialize(k: usize);
    fn new<N: HashableSequence>(seq: N, k: usize) -> Self::HashIterator<N>;

    fn can_get_bucket(used_bits: usize, requested_bits: usize) -> bool {
        Self::USABLE_HASH_BITS >= (used_bits + requested_bits)
    }

    /// Gets the first buckets count, used in MinimizerBucketing phase and to sort hashes
    fn get_bucket(
        used_bits: usize,
        requested_bits: usize,
        hash: <Self as HashFunctionFactory>::HashTypeUnextendable,
    ) -> BucketIndexType;

    fn get_shifted(hash: Self::HashTypeUnextendable, shift: u8) -> u8;
    fn get_u64(hash: Self::HashTypeUnextendable) -> u64;

    fn debug_eq_to_u128(hash: Self::HashTypeUnextendable, value: u128) -> bool;

    fn manual_roll_forward(
        hash: Self::HashTypeExtendable,
        k: usize,
        out_base: u8,
        in_base: u8,
    ) -> Self::HashTypeExtendable;

    fn manual_roll_reverse(
        hash: Self::HashTypeExtendable,
        k: usize,
        out_base: u8,
        in_base: u8,
    ) -> Self::HashTypeExtendable;

    fn manual_remove_only_forward(
        hash: Self::HashTypeExtendable,
        k: usize,
        out_base: u8,
    ) -> Self::HashTypeExtendable;

    fn manual_remove_only_reverse(
        hash: Self::HashTypeExtendable,
        k: usize,
        out_base: u8,
    ) -> Self::HashTypeExtendable;

    const INVERTIBLE: bool;
    type SeqType: AsRef<[u8]>;
    fn invert(hash: Self::HashTypeUnextendable) -> Self::SeqType;
}

#[static_dispatch]
pub trait MinimizerHashFunctionFactory: HashFunctionFactory {
    /// Gets the full minimizer
    fn get_full_minimizer(
        hash: <Self as HashFunctionFactory>::HashTypeUnextendable,
    ) -> MinimizerType;
}

pub trait HashFunction<HF: HashFunctionFactory> {
    type IteratorType: Iterator<Item = HF::HashTypeExtendable>;
    type EnumerableIteratorType: Iterator<Item = (usize, HF::HashTypeExtendable)>;

    fn iter(self) -> Self::IteratorType;
    fn iter_enumerate(self) -> Self::EnumerableIteratorType;
}

pub trait HashableSequence: Clone {
    unsafe fn get_unchecked_cbase(&self, index: usize) -> u8;
    fn bases_count(&self) -> usize;
}

impl HashableSequence for &[u8] {
    #[inline(always)]
    unsafe fn get_unchecked_cbase(&self, index: usize) -> u8 {
        *self.get_unchecked(index)
    }

    #[inline(always)]
    fn bases_count(&self) -> usize {
        self.len()
    }
}

const RMMULT_CACHE_SIZE: usize = 8;

fn init_rmmult(k: usize, multiplier: u128) -> [u128; RMMULT_CACHE_SIZE] {
    fn fastexp(base: u128, mut exp: usize) -> u128 {
        let mut result: u128 = 1;
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

    let mut cache = [0; RMMULT_CACHE_SIZE];
    for k in k.saturating_sub(RMMULT_CACHE_SIZE / 2)..(k + RMMULT_CACHE_SIZE / 2) {
        cache[k % RMMULT_CACHE_SIZE] = fastexp(multiplier, k.saturating_sub(1));
    }

    cache
}

#[cfg(test)]
pub mod tests {
    use super::ExtendableHashTraitType;
    use super::HashFunction;
    use super::HashFunctionFactory;
    use crate::HashableSequence;
    use io::compressed_read::CompressedRead;
    use rand::{RngCore, SeedableRng};
    use std::mem::size_of;
    use utils::Utils;

    // From rand test library
    /// Construct a deterministic RNG with the given seed
    pub fn rng(seed: u64) -> impl RngCore {
        // For tests, we want a statistically good, fast, reproducible RNG.
        // PCG32 will do fine, and will be easy to embed if we ever need to.
        pcg_rand::Pcg32::seed_from_u64(seed)
    }

    fn to_compressed(bases: &[u8]) -> Vec<u8> {
        let mut res = Vec::new();
        for base in bases {
            res.push(Utils::compress_base(*base))
        }
        res
    }

    fn compute_hashes<FACTORY: HashFunctionFactory>(
        bases: &[u8],
        k: usize,
        compress: bool,
    ) -> Vec<FACTORY::HashTypeExtendable> {
        let bases_vec;

        let bases = if compress {
            bases_vec = to_compressed(bases);
            bases_vec.as_slice()
        } else {
            bases
        };

        FACTORY::new(bases, k).iter().collect()
    }

    fn generate_bases(len: usize, seed: u64) -> Vec<u8> {
        let mut rng = rng(seed);

        let result = (0..len)
            .map(|_| Utils::decompress_base((rng.next_u32() % 4) as u8))
            .collect::<Vec<_>>();

        result
    }

    impl<'a> HashableSequence for CompressedRead<'a> {
        #[inline(always)]
        unsafe fn get_unchecked_cbase(&self, index: usize) -> u8 {
            self.get_base_unchecked(index)
        }

        #[inline(always)]
        fn bases_count(&self) -> usize {
            self.get_length()
        }
    }

    pub fn test_hash_function<FACTORY: HashFunctionFactory>(kvalues: &[usize], canonical: bool) {
        for kval in kvalues {
            FACTORY::initialize(*kval);

            let test_bases = generate_bases(*kval * 10, 773 + *kval as u64);

            let hashes = compute_hashes::<FACTORY>(test_bases.as_slice(), *kval, true);

            // Distribution test
            {
                let mut collisions_count = 0;
                let mut tmp = hashes
                    .iter()
                    .enumerate()
                    .map(|(i, x)| (*x, i))
                    .collect::<Vec<_>>();
                tmp.sort();
                for i in 1..tmp.len() {
                    if tmp[i - 1].0 == tmp[i].0 {
                        let f = tmp[i - 1].1;
                        let s = tmp[i].1;

                        let fx = &test_bases[f..f + *kval];
                        let sx = &test_bases[s..s + *kval];

                        if fx != sx {
                            println!(
                                "Found collision {:?} {} != {}!",
                                tmp[i - 1].0,
                                std::str::from_utf8(fx).unwrap(),
                                std::str::from_utf8(sx).unwrap()
                            );
                            collisions_count += 1;
                        }
                    }
                }

                if size_of::<FACTORY::HashTypeUnextendable>() >= 8 {
                    assert_eq!(collisions_count, 0);
                }
            }

            // Double hash test
            {
                let mut dtest_bases = test_bases.clone();
                dtest_bases.extend_from_slice(test_bases.as_slice());

                let dhashes = compute_hashes::<FACTORY>(dtest_bases.as_slice(), *kval, true);

                let kmers_count = test_bases.len() - *kval + 1;

                let first_range = ..kmers_count;
                let second_range = (kmers_count + *kval - 1)..;

                assert_eq!(dhashes[first_range], dhashes[second_range]);
            }

            // Canonical test
            if canonical {
                let rc_bases = test_bases
                    .iter()
                    .map(|x| match *x {
                        b'A' => b'T',
                        b'C' => b'G',
                        b'G' => b'C',
                        b'T' => b'A',
                        _ => unreachable!(),
                    })
                    .rev()
                    .collect::<Vec<_>>();

                let rc_hashes = compute_hashes::<FACTORY>(rc_bases.as_slice(), *kval, true);

                assert_eq!(
                    hashes
                        .iter()
                        .map(|x| x.to_unextendable())
                        .collect::<Vec<_>>(),
                    rc_hashes
                        .iter()
                        .map(|x| x.to_unextendable())
                        .rev()
                        .collect::<Vec<_>>(),
                );
            }

            // Manual forward+reverse test
            {
                for i in 0..hashes.len() - 1 {
                    let manual_roll = FACTORY::manual_roll_forward(
                        hashes[i],
                        *kval,
                        Utils::compress_base(test_bases[i]),
                        Utils::compress_base(test_bases[i + *kval]),
                    );
                    assert_eq!(hashes[i + 1], manual_roll);
                }

                for i in (1..hashes.len()).rev() {
                    let manual_roll = FACTORY::manual_roll_reverse(
                        hashes[i],
                        *kval,
                        Utils::compress_base(test_bases[i + *kval - 1]),
                        Utils::compress_base(test_bases[i - 1]),
                    );
                    assert_eq!(hashes[i - 1], manual_roll);
                }
            }

            // Manual remove forward test
            {
                let lhashes = compute_hashes::<FACTORY>(test_bases.as_slice(), *kval - 1, true);

                for i in 0..hashes.len() {
                    let manual_roll = FACTORY::manual_remove_only_forward(
                        hashes[i],
                        *kval,
                        Utils::compress_base(test_bases[i]),
                    );
                    assert_eq!(lhashes[i + 1], manual_roll);
                }
            }

            // Manual remove backward test
            {
                let lhashes = compute_hashes::<FACTORY>(test_bases.as_slice(), *kval - 1, true);

                for i in (0..hashes.len()).rev() {
                    let manual_roll = FACTORY::manual_remove_only_reverse(
                        hashes[i],
                        *kval,
                        Utils::compress_base(test_bases[i + *kval - 1]),
                    );
                    assert_eq!(lhashes[i], manual_roll);
                }
            }

            // Invertibility test
            if FACTORY::INVERTIBLE {
                let compressed_slice_vec = to_compressed(test_bases.as_slice());
                let compressed_read = CompressedRead::new_from_compressed(
                    compressed_slice_vec.as_slice(),
                    test_bases.len(),
                );

                for i in 0..(test_bases.len() - *kval) {
                    let kmer = compressed_read.sub_slice(i..(i + *kval));
                    let hash = FACTORY::new(kmer, *kval)
                        .iter()
                        .next()
                        .unwrap()
                        .to_unextendable();

                    let mut buffer = vec![];
                    kmer.copy_to_buffer(&mut buffer);

                    let buffer1 = FACTORY::invert(hash);
                    println!("Original slice: {:?}", buffer);

                    // assert_eq!(buffer, buffer1, "Hash: {}", hash);

                    // buffer1 = buffer.clone();

                    println!(
                        "Old decoded: {}",
                        CompressedRead::new_from_compressed(buffer.as_slice(), *kval).to_string()
                    );
                    println!(
                        "New decoded: {}",
                        CompressedRead::new_from_compressed(buffer1.as_ref(), *kval).to_string()
                    );

                    let first = kmer.to_string();
                    let second_read = CompressedRead::new_from_compressed(buffer1.as_ref(), *kval);

                    let second_fwd = second_read.to_string();
                    let second_rcomp = second_read
                        .as_reverse_complement_bases_iter()
                        .map(|x| x as char)
                        .collect::<String>();

                    assert!(
                        (first == second_fwd) || (first == second_rcomp),
                        "Hash: {:?}",
                        hash
                    );
                }
            }
        }
    }
}
