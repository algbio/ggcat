//! Hashing alone: the canonical ntHash of every m-mer of one bucketing packet's bases, 512 Ki
//! bases packed four to a byte, with m as the assembler picks it for k = 31. The 64-bit scalar
//! hash the pipeline runs, against the 32-bit one of [`ggcat_hashes::simd::nthash`] on one lane
//! and on eight, each kernel this CPU has; on one thread and on 16 at once, each thread hashing
//! the whole input. The hashes are folded into a checksum: all of them, or for the eight lanes
//! also only two, to time the hashing without a consumer taking every lane.

use criterion::*;
use ggcat_hashes::simd::nthash::{CanonicalNtHashLanes, NtHashLanes};
use ggcat_hashes::simd::{HashKernel, PackedLanes, SIMD_LANES};
use ggcat_hashes::{
    ExtendableHashTraitType, HashFunction, HashFunctionFactory, default::MNHFactory,
};
use io::compressed_read::CompressedRead;
use rayon::prelude::*;

/// One bucketing packet's bases, unless `NTHASH_BENCH_BASES` sets another number, a multiple of
/// four; `NTHASH_BENCH_M` sets m likewise.
const BASES: usize = 512 * 1024;
const THREADS: usize = 16;

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

fn hash_64_scalar(bases: &[u8], m: usize) -> u64 {
    let read = CompressedRead::new_from_compressed(bases, 4 * bases.len());
    MNHFactory::new(read, m)
        .iter()
        .fold(0, |checksum, hash| checksum ^ hash.to_unextendable())
}

fn hash_32_one_lane(bases: &[u8], m: usize, kernel: HashKernel) -> u32 {
    let lane = PackedLanes::<1>::new(bases, 0, 4 * bases.len(), 0);
    let mut checksum = 0;
    CanonicalNtHashLanes::new(lane, m)
        .unwrap()
        .for_each(kernel, |_, hash: &NtHashLanes<1>| {
            checksum ^= hash.canonical[0]
        });
    checksum
}

/// With `EVERY_LANE`, every lane's hash folded in, a lane at a time, as a scalar consumer would
/// take them; else only lanes 0 and 4, so that the hashing alone is timed. One lane of each half:
/// the kernels with the eight lanes in two registers of four, SSSE3 and NEON, still compute both,
/// where with one lane read the compiler drops the other register.
fn hash_32_simd_lanes<const EVERY_LANE: bool>(bases: &[u8], m: usize, kernel: HashKernel) -> u32 {
    let lanes = PackedLanes::<SIMD_LANES>::new(bases, 0, 4 * bases.len(), m - 1);
    let mut checksum = [0u32; SIMD_LANES];
    CanonicalNtHashLanes::new(lanes, m).unwrap().for_each(
        kernel,
        |_, hash: &NtHashLanes<SIMD_LANES>| {
            if EVERY_LANE {
                for lane in 0..SIMD_LANES {
                    checksum[lane] ^= hash.canonical[lane];
                }
            } else {
                checksum[0] ^= hash.canonical[0];
                checksum[SIMD_LANES / 2] ^= hash.canonical[SIMD_LANES / 2];
            }
        },
    );
    checksum.iter().fold(0, |all, lane| all ^ lane)
}

fn bench(c: &mut Criterion) {
    let count = setting("NTHASH_BENCH_BASES", BASES);
    let data = packed_bases(count);
    let bases: &[u8] = &data;
    let m = setting("NTHASH_BENCH_M", utils::compute_best_m(31));
    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(THREADS)
        .build()
        .unwrap();
    let mut versions: Vec<(String, Box<dyn Fn() -> u64 + Sync + '_>)> = vec![(
        "64-bit-scalar".into(),
        Box::new(move || hash_64_scalar(bases, m)),
    )];
    let kernel = HashKernel::detect();
    versions.push((
        format!("32-bit-1-lane-{kernel:?}"),
        Box::new(move || hash_32_one_lane(bases, m, kernel) as u64),
    ));
    for kernel in HashKernel::supported() {
        versions.push((
            format!("32-bit-{SIMD_LANES}-lanes-{kernel:?}-every-lane"),
            Box::new(move || hash_32_simd_lanes::<true>(bases, m, kernel) as u64),
        ));
        versions.push((
            format!("32-bit-{SIMD_LANES}-lanes-{kernel:?}-two-lanes-folded"),
            Box::new(move || hash_32_simd_lanes::<false>(bases, m, kernel) as u64),
        ));
    }
    println!(
        "m = {m}, {count} bases, kernels {:?}",
        HashKernel::supported()
    );

    let mut group = c.benchmark_group("nthash-1-thread");
    group.throughput(Throughput::Elements(count as u64));
    for (name, version) in &versions {
        group.bench_function(name.as_str(), |b| b.iter(|| black_box(version())));
    }
    group.finish();

    let mut group = c.benchmark_group(format!("nthash-{THREADS}-threads"));
    group.throughput(Throughput::Elements((count * THREADS) as u64));
    for (name, version) in &versions {
        group.bench_function(name.as_str(), |b| {
            b.iter(|| {
                pool.install(|| {
                    (0..THREADS)
                        .into_par_iter()
                        .map(|_| version())
                        .reduce(|| 0, |a, b| a ^ b)
                })
            })
        });
    }
    group.finish();
}

criterion_group!(benches, bench);
criterion_main!(benches);
