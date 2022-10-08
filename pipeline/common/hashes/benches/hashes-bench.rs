use criterion::*;
use hashes::*;
use io::compressed_read::CompressedRead;
use rand::{RngCore, SeedableRng};
use utils::Utils;

// From rand test library
/// Construct a deterministic RNG with the given seed
pub fn rng(seed: u64) -> impl RngCore {
    // For tests, we want a statistically good, fast, reproducible RNG.
    // PCG32 will do fine, and will be easy to embed if we ever need to.
    pcg_rand::Pcg32::seed_from_u64(seed)
}

fn generate_bases(len: usize, seed: u64) -> Vec<u8> {
    let mut rng = rng(seed);

    let result = (0..len)
        .map(|_| Utils::decompress_base((rng.next_u32() % 4) as u8))
        .collect::<Vec<_>>();

    result
}

pub fn criterion_benchmark(c: &mut Criterion) {
    let bases = generate_bases(63, 0);

    let read = CompressedRead::new_from_compressed(&bases, 63);

    for k in [15, 35, 47, 63] {
        c.bench_function(&format!("single-canonical-u128-k{}", k), |b| {
            b.iter(|| {
                let hashes = cn_seqhash::u128::CanonicalSeqHashFactory::new(read, k);
                let hash = hashes.iter().next().unwrap();
                black_box(hash);
            })
        });
    }
}

criterion_group!(benches, criterion_benchmark);

criterion_main!(benches);
