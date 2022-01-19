#![allow(dead_code)]

use crate::config::{BucketIndexType, DEFAULT_MINIMIZER_MASK};
use crate::hashes::ExtendableHashTraitType;
use crate::hashes::HashFunction;
use crate::hashes::HashFunctionFactory;
use crate::pipeline_common::minimizer_bucketing::MinimizerInputSequence;
use crate::rolling::minqueue::RollingMinQueue;
use std::sync::atomic::{AtomicU64, Ordering};

pub static KCOUNTER: AtomicU64 = AtomicU64::new(0);

pub fn debug_increase() {
    KCOUNTER.fetch_add(1, Ordering::Relaxed);
}

pub fn debug_print() {
    println!("COUNTER: {:?}", KCOUNTER.load(Ordering::Relaxed));
}

pub fn debug_minimizers<H: HashFunctionFactory, R: MinimizerInputSequence, const MASK: u32>(
    read: R,
    m: usize,
    k: usize,
) {
    println!("Debugging sequence: {}", read.debug_to_string());

    let mut queue = RollingMinQueue::<H>::new(k - m);

    let hashes = H::new(read, m);

    let rolling_iter = queue.make_iter::<_, MASK>(hashes.iter().map(|x| x.to_unextendable()));

    for (idx, hash) in rolling_iter.enumerate() {
        println!(
            "Minimizer info for kmer: {}\nHASH: {} UNMASKED_HASH: {} FB: {} SB: {} SH: {}",
            read.get_subslice(idx..(idx + k - 1)).debug_to_string(),
            H::get_full_minimizer::<MASK>(hash),
            H::get_full_minimizer::<DEFAULT_MINIMIZER_MASK>(hash),
            H::get_first_bucket(hash),
            H::get_second_bucket(hash),
            H::get_sorting_hash(hash),
        );
    }
}

fn assert_reads<H: HashFunctionFactory>(read: &[u8], bucket: BucketIndexType) {
    // Test ***************************
    const K: usize = 32;

    if read.len() == 33 {
        let hashes = H::new(&read[0..K], M);
        let minimizer = hashes
            .iter()
            .min_by_key(|read| {
                H::get_full_minimizer::<{ DEFAULT_MINIMIZER_MASK }>(read.to_unextendable())
            })
            .unwrap();

        let hashes1 = H::new(&read[1..K + 1], M);
        let minimizer1 = hashes1
            .iter()
            .min_by_key(|read| {
                H::get_full_minimizer::<{ DEFAULT_MINIMIZER_MASK }>(read.to_unextendable())
            })
            .unwrap();

        assert!(
            H::get_first_bucket(minimizer.to_unextendable()) % 512 == bucket
                || H::get_first_bucket(minimizer1.to_unextendable()) % 512 == bucket
        );
        println!(
            "{} / {}",
            minimizer.to_unextendable(),
            minimizer1.to_unextendable()
        );
    }

    if read.len() < 34 {
        return;
    }

    let x = &read[1..read.len() - 1];

    const M: usize = 12;

    let hashes = H::new(&x[0..K], M);
    let minimizer = hashes
        .iter()
        .min_by_key(|x| H::get_full_minimizer::<{ DEFAULT_MINIMIZER_MASK }>(x.to_unextendable()))
        .unwrap();

    assert_eq!(
        H::get_first_bucket(minimizer.to_unextendable()) % 512,
        bucket
    );

    if x.len() > K {
        let hashes2 = H::new(&x[..], M);
        let minimizer2 = hashes2
            .iter()
            .min_by_key(|x| {
                H::get_full_minimizer::<{ DEFAULT_MINIMIZER_MASK }>(x.to_unextendable())
            })
            .unwrap();

        if minimizer.to_unextendable() != minimizer2.to_unextendable() {
            let vec: Vec<_> = H::new(&x[..], M)
                .iter()
                .map(|x| H::get_full_minimizer::<{ DEFAULT_MINIMIZER_MASK }>(x.to_unextendable()))
                .collect();

            println!("Kmers {}", std::str::from_utf8(x).unwrap());
            println!("Hashes {:?}", vec);
            panic!(
                "AA {} {}",
                minimizer.to_unextendable(),
                minimizer2.to_unextendable()
            );
        }
    }
    // Test ***************************
}
