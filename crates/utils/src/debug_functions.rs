#![allow(dead_code)]

use std::sync::atomic::{AtomicU64, Ordering};

pub static KCOUNTER: AtomicU64 = AtomicU64::new(0);

pub fn debug_increase() {
    KCOUNTER.fetch_add(1, Ordering::Relaxed);
}

pub fn debug_print() {
    println!("COUNTER: {:?}", KCOUNTER.load(Ordering::Relaxed));
}

#[macro_export]
macro_rules! track {
    ($code:expr, $tracker:ident) => {{
        use parallel_processor::mt_debug_counters::counter::AtomicCounterGuardSum;
        let guard = AtomicCounterGuardSum::new(&$tracker, 1);
        $code
    }};
}

// pub fn debug_minimizers<H: HashFunctionFactory, R: MinimizerInputSequence>(
//     read: R,
//     m: usize,
//     k: usize,
// ) {
//     println!("Debugging sequence: {}", read.debug_to_string());
//
//     let mut queue = RollingMinQueue::<H>::new(k - m);
//
//     let hashes = H::new(read, m);
//
//     let rolling_iter = queue.make_iter(hashes.iter().map(|x| x.to_unextendable()));
//
//     for (idx, hash) in rolling_iter.enumerate() {
//         println!(
//             "Minimizer info for kmer: {}\nHASH: {} UNMASKED_HASH: {} FB: {} SB: {} SH: {}",
//             read.get_subslice(idx..(idx + k - 1)).debug_to_string(),
//             H::get_full_minimizer(hash),
//             H::get_full_minimizer(hash),
//             H::get_first_bucket(hash),
//             H::get_second_bucket(hash),
//             H::get_sorting_hash(hash),
//         );
//     }
// }
