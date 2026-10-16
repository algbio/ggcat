//! Hashing and minimizers together: the canonical ntHash of every m-mer of eight lanes of packed
//! bases, through a minimizer queue, as the bucketing would split them into super-k-mers. The
//! SIMD queue of [`ggcat_hashes::simd::minqueue`] on each kernel this CPU has, against the
//! pipeline's 64-bit scalar hash through the scalar queue, a lane at a time. On one thread, and
//! on 16 at once, each thread splitting the whole input `REPETITIONS` times.
//!
//! The defaults are those of the queue benches of the `simd-accel` crate: m = 31, windows of 31
//! m-mers, eight lanes of 16 Ki random bases each, apart.

use criterion::*;
use ggcat_hashes::rolling::batch_minqueue::BatchMinQueue;
use ggcat_hashes::simd::minqueue::BatchMinQueueLanes;
use ggcat_hashes::simd::nthash::CanonicalNtHashLanes;
use ggcat_hashes::simd::{HashKernel, PackedLanes, SIMD_LANES};
use ggcat_hashes::{
    ExtendableHashTraitType, HashFunction, HashFunctionFactory, default::MNHFactory,
};
use io::compressed_read::CompressedRead;
use rayon::prelude::*;
use std::time::Duration;

/// The bases of a lane, unless `MINQUEUE_BENCH_LANE_BASES` sets another number, a multiple of
/// four; `MINQUEUE_BENCH_M` and `MINQUEUE_BENCH_WINDOW` set m and the window likewise.
const LANE_BASES: usize = 16 * 1024;
const M: usize = 31;
const WINDOW: usize = 31;
const THREADS: usize = 16;
/// The splits of the whole input each thread makes per iteration: enough for the pool's
/// dispatch to be small against them.
const REPETITIONS: usize = 32;

fn setting(name: &str, default: usize) -> usize {
    std::env::var(name).map_or(default, |value| value.parse().expect(name))
}

/// `count` random bases, packed.
fn packed_bases(count: usize) -> Vec<u8> {
    let mut state = 0x9e37_79b9_7f4a_7c15u64;
    (0..count / 4)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state as u8
        })
        .collect()
}

/// The fold of a split, as the `simd-accel` benches fold theirs.
#[inline(always)]
fn fold_split(lane: usize, index: usize, (value, extra): (u64, u32), last: bool) -> u64 {
    value ^ extra as u64 ^ index as u64 ^ lane as u64 ^ ((last as u64) << 63)
}

/// The pipeline today: each lane's m-mers hashed by the 64-bit scalar hash, with the unique flag
/// and the extra the assembler gives them, and split by the scalar queue.
fn splits_64_scalar(
    bases: &[u8],
    lane_bases: usize,
    m: usize,
    queue: &mut BatchMinQueue<u32>,
) -> u64 {
    let mut checksum = 0;
    for lane in 0..SIMD_LANES {
        let bytes = &bases[lane * lane_bases / 4..(lane + 1) * lane_bases / 4];
        let read = CompressedRead::new_from_compressed(bytes, lane_bases);
        let hashes = MNHFactory::new(read, m);
        queue.get_minimizer_splits::<_, true>(
            hashes.iter_enumerate().map(|(index, hash)| {
                (
                    hash.to_unextendable() | !hash.is_rc_symmetric() as u64,
                    (index as u32) << 1 | hash.is_forward() as u32,
                )
            }),
            0,
            0,
            |index, minimizer, last| checksum ^= fold_split(lane, index, minimizer, last),
        );
    }
    checksum
}

fn splits_simd(
    bases: &[u8],
    lane_bases: usize,
    m: usize,
    kernel: HashKernel,
    queue: &mut BatchMinQueueLanes,
) -> u64 {
    let lanes = PackedLanes::<SIMD_LANES>::new(bases, 0, lane_bases * SIMD_LANES, 0);
    let mut checksum = 0;
    queue.get_minimizer_splits::<_, true>(
        CanonicalNtHashLanes::new(lanes, m).unwrap(),
        kernel,
        0,
        0,
        |lane, index, (value, extra), last| {
            checksum ^= fold_split(lane, index, (value as u64, extra), last)
        },
    );
    checksum
}

/// Every window's minimum of every lane folded in, a lane at a time.
fn minimizers_simd(
    bases: &[u8],
    lane_bases: usize,
    m: usize,
    kernel: HashKernel,
    queue: &mut BatchMinQueueLanes,
) -> u64 {
    let lanes = PackedLanes::<SIMD_LANES>::new(bases, 0, lane_bases * SIMD_LANES, 0);
    let mut checksum = [0u32; SIMD_LANES];
    queue.get_minimizers::<_, true>(
        CanonicalNtHashLanes::new(lanes, m).unwrap(),
        kernel,
        0,
        |_, minimum| {
            for lane in 0..SIMD_LANES {
                checksum[lane] ^= minimum.value[lane] ^ minimum.extra[lane];
            }
        },
        |_| {},
    );
    checksum.iter().fold(0, |all, &lane| all ^ lane as u64)
}

/// A version to time: makes its own queue, so that each thread has one.
type Version<'a> = Box<dyn Fn() -> Box<dyn FnMut() -> u64 + Send + 'a> + Sync + 'a>;

fn bench(c: &mut Criterion) {
    let lane_bases = setting("MINQUEUE_BENCH_LANE_BASES", LANE_BASES);
    let m = setting("MINQUEUE_BENCH_M", M);
    let window = setting("MINQUEUE_BENCH_WINDOW", WINDOW);
    let data = packed_bases(lane_bases * SIMD_LANES);
    let bases: &[u8] = &data;
    let mut versions: Vec<(String, Version)> = vec![(
        "64-bit-scalar-splits".into(),
        Box::new(move || {
            let mut queue = BatchMinQueue::new(window);
            Box::new(move || splits_64_scalar(bases, lane_bases, m, &mut queue))
        }),
    )];
    for kernel in HashKernel::supported() {
        versions.push((
            format!("32-bit-{kernel:?}-splits"),
            Box::new(move || {
                let mut queue = BatchMinQueueLanes::new(window);
                Box::new(move || splits_simd(bases, lane_bases, m, kernel, &mut queue))
            }),
        ));
        versions.push((
            format!("32-bit-{kernel:?}-minimizers-every-lane"),
            Box::new(move || {
                let mut queue = BatchMinQueueLanes::new(window);
                Box::new(move || minimizers_simd(bases, lane_bases, m, kernel, &mut queue))
            }),
        ));
    }
    println!(
        "m = {m}, window {window}, {SIMD_LANES} lanes of {lane_bases} bases, kernels {:?}",
        HashKernel::supported()
    );

    let mut group = c.benchmark_group("minqueue-1-thread");
    group.throughput(Throughput::Elements((lane_bases * SIMD_LANES) as u64));
    for (name, version) in &versions {
        let mut run = version();
        group.bench_function(name.as_str(), |b| b.iter(|| black_box(run())));
    }
    group.finish();

    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(THREADS)
        .build()
        .unwrap();
    let mut group = c.benchmark_group(format!("minqueue-{THREADS}-threads"));
    group.measurement_time(Duration::from_secs(8));
    group.throughput(Throughput::Elements(
        (lane_bases * SIMD_LANES * THREADS * REPETITIONS) as u64,
    ));
    for (name, version) in &versions {
        let mut runs: Vec<_> = (0..THREADS).map(|_| version()).collect();
        group.bench_function(name.as_str(), |b| {
            b.iter(|| {
                pool.install(|| {
                    runs.par_iter_mut()
                        .map(|run| (0..REPETITIONS).fold(0, |all, _| all ^ run()))
                        .reduce(|| 0, |a, b| a ^ b)
                })
            })
        });
    }
    group.finish();
}

criterion_group! {
    name = benches;
    config = Criterion::default()
        .warm_up_time(Duration::from_millis(500))
        .measurement_time(Duration::from_millis(1_500));
    targets = bench
}
criterion_main!(benches);
