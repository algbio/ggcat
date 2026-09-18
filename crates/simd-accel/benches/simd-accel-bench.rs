use criterion::*;
use ggcat_simd_accel::{
    batch::{LaneBatch, VecBatchSink},
    fasta_lexer::FastaSimdLexer,
    hashing as cn_nthash_simd,
    minimizer::SimdBatchMinQueue,
    packer::LanePacker,
};
use hashes::*;
use io::compressed_read::CompressedRead;
use io::sequences_reader::SequencesReader;
use io::sequences_splitter::split_and_compress_sequences;
use rand::{RngCore, SeedableRng};
use rayon::prelude::*;
use std::path::{Path, PathBuf};
use std::time::Duration;
use utils::Utils;
use wide::u32x8;

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

fn generate_fasta(sequence_bases: usize, seed: u64) -> Vec<u8> {
    const LINE_BASES: usize = 80;

    let mut rng = rng(seed);
    let mut fasta = Vec::with_capacity(sequence_bases + sequence_bases / LINE_BASES + 4096);
    let mut remaining = sequence_bases;
    let mut record = 0usize;
    while remaining != 0 {
        let record_len = remaining.min(11_000 + record.wrapping_mul(7_919) % 17_000);
        fasta.extend_from_slice(format!(">synthetic-{record}\n").as_bytes());
        for position in 0..record_len {
            let base = if position % 4_093 == record % 4_093 {
                b'N'
            } else {
                [b'A', b'C', b'T', b'G'][(rng.next_u32() & 3) as usize]
            };
            fasta.push(base);
            if (position + 1) % LINE_BASES == 0 {
                fasta.push(b'\n');
            }
        }
        if record_len % LINE_BASES != 0 {
            fasta.push(b'\n');
        }
        remaining -= record_len;
        record += 1;
    }
    fasta
}

const FASTA_HASH_K: usize = 31;
const FASTA_WINDOW: usize = 31;
const FASTA_PARSER_K: usize = FASTA_HASH_K + FASTA_WINDOW - 1;
const FASTA_BASES_PER_LANE: usize = 64 * 1024;
const FASTA_INPUT_CHUNK_BYTES: usize = 64 * 1024 + 13;
// Leave enough room for lane overlaps so the fixture does not create a nearly
// empty third batch solely because its logical size equals two batch capacities.
const FASTA_SEQUENCE_BASES: usize = 1024 * 1024 - 8 * 1024;
const FASTA_THREADS: usize = 16;

fn parse_fasta_simd(input: &[u8]) -> Vec<LaneBatch<()>> {
    let mut packer = LanePacker::new(
        FASTA_PARSER_K,
        FASTA_BASES_PER_LANE,
        FASTA_PARSER_K,
        false,
        1 << 20,
    )
    .unwrap();
    let mut sink = VecBatchSink::new(FASTA_BASES_PER_LANE);
    let mut lexer = FastaSimdLexer::new();
    lexer.begin_stream(());
    for chunk in input.chunks(FASTA_INPUT_CHUNK_BYTES) {
        lexer.push(&mut packer, &mut sink, chunk);
    }
    lexer.end_stream(&mut packer, &mut sink);
    packer.finish(&mut sink);
    sink.batches
}

/// Marks the minimizer windows that fit inside a single lane fragment.
fn valid_windows(batch: &LaneBatch<()>, span: usize) -> Vec<u8> {
    let windows = batch.bases_per_lane + 1 - span;
    let mut masks = vec![0u8; windows];
    for lane in 0..cn_nthash_simd::SIMD_LANES {
        let bit = 1u8 << lane;
        for pair in batch.lanes[lane].windows(2) {
            let (start, end) = (pair[0].lane_start as usize, pair[1].lane_start as usize);
            if end - start < span {
                continue;
            }
            for mask in &mut masks[start..=end - span] {
                *mask |= bit;
            }
        }
    }
    masks
}

fn run_fasta_parse(input: &[u8]) -> u64 {
    let batches = parse_fasta_simd(input);
    let mut checksum = 0u64;
    for batch in &batches {
        checksum = checksum
            .wrapping_add(batch.records.len() as u64)
            .wrapping_add(batch.lane_fill.iter().map(|fill| *fill as u64).sum::<u64>());
    }
    black_box(batches);
    checksum
}

fn run_fasta_prepare_minqueue(input: &[u8]) -> u64 {
    let batches = parse_fasta_simd(input);
    let mut checksum = 0u64;
    for batch in &batches {
        let masks = valid_windows(batch, FASTA_PARSER_K);
        let hashes: Vec<_> = cn_nthash_simd::canonical_minimizer_items::<true>(
            batch.packed_sequence(),
            FASTA_HASH_K,
        )
        .unwrap()
        .collect();
        checksum = checksum
            .wrapping_add(hashes.len() as u64)
            .wrapping_add(masks.len() as u64);
        black_box((hashes, masks));
    }
    checksum
}

fn run_fasta_minimizer_splits(input: &[u8]) -> u64 {
    let batches = parse_fasta_simd(input);
    let mut checksum = 0u64;
    let mut queue = SimdBatchMinQueue::<u32>::new(FASTA_WINDOW);
    for batch in &batches {
        let masks = valid_windows(batch, FASTA_PARSER_K);
        let hashes = cn_nthash_simd::canonical_minimizer_items::<true>(
            batch.packed_sequence(),
            FASTA_HASH_K,
        )
        .unwrap();
        queue.get_valid_minimizer_splits::<_, true>(hashes, &masks, |run| {
            checksum ^= run.hash as u64
                ^ run.start as u64
                ^ run.lane as u64
                ^ ((run.finished as u64) << 63);
        });
    }
    checksum
}

fn fasta_simd_hash_elements(input: &[u8]) -> u64 {
    parse_fasta_simd(input)
        .iter()
        .map(|batch| {
            ((batch.bases_per_lane - FASTA_HASH_K + 1) * cn_nthash_simd::SIMD_LANES) as u64
        })
        .sum()
}

struct BenchmarkFastaFile {
    path: PathBuf,
}

impl BenchmarkFastaFile {
    fn new(contents: &[u8]) -> Self {
        let path = std::env::temp_dir().join(format!(
            "ggcat-scalar-fasta-benchmark-{}-{}.fa",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::write(&path, contents).unwrap();
        Self { path }
    }
}

impl Drop for BenchmarkFastaFile {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}

fn run_scalar_fasta_minimizer_splits(path: &Path) -> u64 {
    let mut checksum = 0u64;
    let mut reader = SequencesReader::new();
    let mut compressed = Vec::new();
    let mut queue = rolling::batch_minqueue::BatchMinQueue::<()>::new(FASTA_WINDOW);
    reader.process_file_extended(
        path,
        |record| {
            split_and_compress_sequences(
                &mut compressed,
                FASTA_PARSER_K,
                &record,
                |sequence, range| {
                    checksum ^= range.start as u64 ^ range.end as u64;
                    let hashes =
                        cn_nthash::CanonicalNtHashIterator::new(sequence, FASTA_HASH_K).unwrap();
                    queue.get_minimizer_splits::<_, true>(
                        hashes.iter().map(|hash| {
                            (
                                hash.to_unextendable() | u64::from(!hash.is_rc_symmetric()),
                                (),
                            )
                        }),
                        0,
                        0,
                        |index, item, last| {
                            checksum ^= item.0 ^ index as u64 ^ ((last as u64) << 63);
                        },
                    );
                },
            );
        },
        None,
        false,
        false,
    );
    checksum
}

fn scalar_fasta_hash_elements(path: &Path) -> u64 {
    let mut elements = 0u64;
    let mut reader = SequencesReader::new();
    let mut compressed = Vec::new();
    reader.process_file_extended(
        path,
        |record| {
            split_and_compress_sequences(
                &mut compressed,
                FASTA_PARSER_K,
                &record,
                |sequence, _| {
                    elements += (sequence.bases_count() - FASTA_HASH_K + 1) as u64;
                },
            );
        },
        None,
        false,
        false,
    );
    elements
}

fn bench_scalar_fasta_pipeline(c: &mut Criterion) {
    let fasta = generate_fasta(FASTA_SEQUENCE_BASES, 0xFA57_A51D);
    let fasta_file = BenchmarkFastaFile::new(&fasta);
    let hash_elements = scalar_fasta_hash_elements(&fasta_file.path);

    let mut group = c.benchmark_group("fasta-scalar-minimizer-splits");
    group.throughput(Throughput::Elements(hash_elements));
    group.bench_function("parse-hash-and-valid-split", |b| {
        b.iter(|| {
            black_box(run_scalar_fasta_minimizer_splits(black_box(
                fasta_file.path.as_path(),
            )))
        })
    });
    group.finish();

    let thread_pool = rayon::ThreadPoolBuilder::new()
        .num_threads(FASTA_THREADS)
        .build()
        .unwrap();
    let mut threaded = c.benchmark_group("fasta-scalar-minimizer-splits-16-threads");
    threaded.measurement_time(Duration::from_secs(5));
    threaded.throughput(Throughput::Elements(hash_elements * FASTA_THREADS as u64));
    threaded.bench_function("parse-hash-and-valid-split", |b| {
        b.iter(|| {
            let path = black_box(fasta_file.path.as_path());
            let checksum = thread_pool.install(|| {
                (0..FASTA_THREADS)
                    .into_par_iter()
                    .map(|worker| run_scalar_fasta_minimizer_splits(path) ^ worker as u64)
                    .reduce(|| 0, |left, right| left ^ right)
            });
            black_box(checksum)
        })
    });
    threaded.finish();
}

fn bench_fasta_simd_pipeline(c: &mut Criterion) {
    let fasta = generate_fasta(FASTA_SEQUENCE_BASES, 0xFA57_A51D);
    let hash_elements = fasta_simd_hash_elements(&fasta);
    let mut group = c.benchmark_group("fasta-simd-minimizer-splits");
    group.throughput(Throughput::Bytes(fasta.len() as u64));

    group.bench_function("parse-only", |b| {
        b.iter(|| black_box(run_fasta_parse(black_box(fasta.as_slice()))))
    });

    group.throughput(Throughput::Elements(hash_elements));
    group.bench_function("parse-and-prepare-minqueue", |b| {
        b.iter(|| black_box(run_fasta_prepare_minqueue(black_box(fasta.as_slice()))))
    });

    group.bench_function("parse-prepare-and-split", |b| {
        b.iter(|| black_box(run_fasta_minimizer_splits(black_box(fasta.as_slice()))))
    });

    group.finish();

    let thread_pool = rayon::ThreadPoolBuilder::new()
        .num_threads(FASTA_THREADS)
        .build()
        .unwrap();
    let mut threaded = c.benchmark_group("fasta-simd-minimizer-splits-16-threads");
    threaded.measurement_time(Duration::from_secs(5));
    threaded.throughput(Throughput::Bytes((fasta.len() * FASTA_THREADS) as u64));

    threaded.bench_function("parse-only", |b| {
        b.iter(|| {
            let input = black_box(fasta.as_slice());
            let checksum = thread_pool.install(|| {
                (0..FASTA_THREADS)
                    .into_par_iter()
                    .map(|worker| run_fasta_parse(input) ^ worker as u64)
                    .reduce(|| 0, |left, right| left ^ right)
            });
            black_box(checksum)
        })
    });

    threaded.throughput(Throughput::Elements(hash_elements * FASTA_THREADS as u64));
    threaded.bench_function("parse-and-prepare-minqueue", |b| {
        b.iter(|| {
            let input = black_box(fasta.as_slice());
            let checksum = thread_pool.install(|| {
                (0..FASTA_THREADS)
                    .into_par_iter()
                    .map(|worker| run_fasta_prepare_minqueue(input) ^ worker as u64)
                    .reduce(|| 0, |left, right| left ^ right)
            });
            black_box(checksum)
        })
    });

    threaded.bench_function("parse-prepare-and-split", |b| {
        b.iter(|| {
            let input = black_box(fasta.as_slice());
            let checksum = thread_pool.install(|| {
                (0..FASTA_THREADS)
                    .into_par_iter()
                    .map(|worker| run_fasta_minimizer_splits(input) ^ worker as u64)
                    .reduce(|| 0, |left, right| left ^ right)
            });
            black_box(checksum)
        })
    });

    threaded.finish();
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

    let len: usize = 16 * 1024;
    let packed_lanes: [Vec<u8>; cn_nthash_simd::SIMD_LANES] = std::array::from_fn(|lane| {
        let mut r = rng(100 + lane as u64);
        (0..len).map(|_| (r.next_u32() & 3) as u8).collect()
    });
    let plain_lanes: [Vec<u8>; cn_nthash_simd::SIMD_LANES] = std::array::from_fn(|lane| {
        packed_lanes[lane]
            .iter()
            .map(|b| [b'A', b'C', b'T', b'G'][*b as usize])
            .collect()
    });
    let mut packed = vec![0u32; len.div_ceil(16) * cn_nthash_simd::SIMD_LANES];
    for lane in 0..cn_nthash_simd::SIMD_LANES {
        for (i, &base) in packed_lanes[lane].iter().enumerate() {
            packed[(i / 16) * cn_nthash_simd::SIMD_LANES + lane] |= (base as u32) << ((i % 16) * 2);
        }
    }

    let k = 31;
    let mut nthash = c.benchmark_group("canonical-nthash-8-sequences");
    nthash.throughput(Throughput::Elements(
        (len * cn_nthash_simd::SIMD_LANES) as u64,
    ));
    nthash.bench_function("scalar", |b| {
        b.iter(|| {
            let mut sum = 0u64;
            for lane in &plain_lanes {
                for hash in cn_nthash::CanonicalNtHashIterator::new(black_box(lane.as_slice()), k)
                    .unwrap()
                    .iter()
                {
                    sum ^= hash.to_unextendable();
                }
            }
            black_box(sum)
        })
    });
    nthash.bench_function("simd", |b| {
        b.iter(|| {
            let seq = cn_nthash_simd::PackedSimdSequence::<{ cn_nthash_simd::SIMD_LANES }>::new(
                black_box(&packed),
                len,
            )
            .unwrap();
            let sum = cn_nthash_simd::CanonicalNtHashSimdIterator::new(seq, k)
                .unwrap()
                .fold(u32x8::splat(0), |sum, hash| sum ^ hash.to_unextendable());
            black_box(sum)
        })
    });
    nthash.finish();

    let hashes: Vec<_> = cn_nthash_simd::CanonicalNtHashSimdIterator::new(
        cn_nthash_simd::PackedSimdSequence::<{ cn_nthash_simd::SIMD_LANES }>::new(&packed, len)
            .unwrap(),
        k,
    )
    .unwrap()
    .map(|h| {
        (
            h.to_unextendable() | u32x8::splat(1),
            [(); cn_nthash_simd::SIMD_LANES],
        )
    })
    .collect();
    let hashes_with_u32: Vec<_> = hashes
        .iter()
        .enumerate()
        .map(|(index, (hash, _))| {
            (
                *hash,
                std::array::from_fn(|lane| (index * cn_nthash_simd::SIMD_LANES + lane) as u32),
            )
        })
        .collect();
    let window = 31;
    let hash_vectors: Vec<_> = hashes.iter().map(|item| item.0).collect();
    let valid_split_lanes: Vec<u8> = (0..hash_vectors.len().saturating_sub(window - 1))
        .map(|index| {
            (0..cn_nthash_simd::SIMD_LANES).fold(0u8, |mask, lane| {
                // Model sparse Ns whose complete minimizer windows invalidate
                // consecutive starts at lane-specific positions.
                if (index + lane * 503) % 4096 >= k + window - 1 {
                    mask | (1 << lane)
                } else {
                    mask
                }
            })
        })
        .collect();
    let mut queue = c.benchmark_group("batch-minqueue-8-sequences");
    queue.throughput(Throughput::Elements(
        (hashes.len() * cn_nthash_simd::SIMD_LANES) as u64,
    ));
    queue.bench_function("scalar", |b| {
        b.iter(|| {
            let mut sum = 0u64;
            for lane in 0..cn_nthash_simd::SIMD_LANES {
                rolling::batch_minqueue::BatchMinQueue::new(window).get_minimizers::<_, true>(
                    black_box(hashes.as_slice())
                        .iter()
                        .map(|(h, _)| (h.to_array()[lane] as u64, ())),
                    0,
                    |item, _| sum ^= item.0,
                    |_| {},
                );
            }
            black_box(sum)
        })
    });
    queue.bench_function("simd", |b| {
        b.iter(|| {
            let mut sum = u32x8::splat(0);
            SimdBatchMinQueue::new(window).get_minimizer_hashes_simd::<_, true>(
                black_box(hashes.as_slice()).iter().map(|item| item.0),
                0,
                |hash, _| sum ^= hash,
                |_| {},
            );
            black_box(sum)
        })
    });
    queue.bench_function("scalar-x-u32", |b| {
        b.iter(|| {
            let mut hash_sum = 0u64;
            let mut metadata_sum = 0u32;
            for lane in 0..cn_nthash_simd::SIMD_LANES {
                rolling::batch_minqueue::BatchMinQueue::new(window).get_minimizers::<_, true>(
                    black_box(hashes_with_u32.as_slice())
                        .iter()
                        .map(|(hash, metadata)| (hash.to_array()[lane] as u64, metadata[lane])),
                    0,
                    |item, _| {
                        hash_sum ^= item.0;
                        metadata_sum = metadata_sum.wrapping_add(item.1);
                    },
                    |_| {},
                );
            }
            black_box((hash_sum, metadata_sum))
        })
    });
    queue.bench_function("simd-x-u32", |b| {
        b.iter(|| {
            let mut hash_sum = u32x8::splat(0);
            let mut metadata_sum = [0u32; cn_nthash_simd::SIMD_LANES];
            SimdBatchMinQueue::new(window).get_minimizers_simd::<_, true>(
                black_box(hashes_with_u32.as_slice()).iter().copied(),
                0,
                |item, _| {
                    hash_sum ^= item.0;
                    for lane in 0..cn_nthash_simd::SIMD_LANES {
                        metadata_sum[lane] = metadata_sum[lane].wrapping_add(item.1[lane]);
                    }
                },
                |_| {},
            );
            black_box((hash_sum, metadata_sum))
        })
    });
    queue.finish();

    let mut splits = c.benchmark_group("minimizer-splits-8-sequences");
    splits.throughput(Throughput::Elements(
        (hashes_with_u32.len() * cn_nthash_simd::SIMD_LANES) as u64,
    ));
    let mut scalar_split_queues: [_; cn_nthash_simd::SIMD_LANES] =
        std::array::from_fn(|_| rolling::batch_minqueue::BatchMinQueue::<u32>::new(window));
    splits.bench_function("scalar-x-u32", |b| {
        b.iter(|| {
            let mut checksum = 0u64;
            for lane in 0..cn_nthash_simd::SIMD_LANES {
                scalar_split_queues[lane].get_minimizer_splits::<_, true>(
                    black_box(hashes_with_u32.as_slice())
                        .iter()
                        .map(|(hash, metadata)| (hash.to_array()[lane] as u64, metadata[lane])),
                    0,
                    0,
                    |index, item, last| {
                        checksum ^= item.0
                            ^ item.1 as u64
                            ^ index as u64
                            ^ lane as u64
                            ^ ((last as u64) << 63);
                    },
                );
            }
            black_box(checksum)
        })
    });
    let mut simd_split_queue = SimdBatchMinQueue::<u32>::new(window);
    splits.bench_function("simd-x-u32", |b| {
        b.iter(|| {
            let mut checksum = 0u64;
            simd_split_queue.get_minimizer_splits_slice::<true, { cn_nthash_simd::SIMD_LANES }>(
                black_box(hashes_with_u32.as_slice()),
                |lane, index, item, last| {
                    checksum ^= item.0 as u64
                        ^ item.1 as u64
                        ^ index as u64
                        ^ lane as u64
                        ^ ((last as u64) << 63);
                },
            );
            black_box(checksum)
        })
    });
    let mut simd_hash_split_queue = SimdBatchMinQueue::<()>::new(window);
    splits.bench_function("simd-hash-only", |b| {
        b.iter(|| {
            let mut checksum = 0u64;
            simd_hash_split_queue
                .get_minimizer_hash_splits_slice::<true, { cn_nthash_simd::SIMD_LANES }>(
                    black_box(hash_vectors.as_slice()),
                    |lane, index, hash, last| {
                        checksum ^=
                            hash as u64 ^ index as u64 ^ lane as u64 ^ ((last as u64) << 63);
                    },
                );
            black_box(checksum)
        })
    });
    let mut simd_valid_split_queue = SimdBatchMinQueue::<()>::new(window);
    splits.bench_function("simd-valid-dna", |b| {
        b.iter(|| {
            let mut checksum = 0u64;
            simd_valid_split_queue.get_valid_minimizer_splits::<_, true>(
                black_box(hash_vectors.as_slice()).iter().map(|&h| (h, ())),
                black_box(valid_split_lanes.as_slice()),
                |run| {
                    checksum ^= run.hash as u64
                        ^ run.start as u64
                        ^ run.lane as u64
                        ^ ((run.finished as u64) << 63);
                },
            );
            black_box(checksum)
        })
    });
    splits.finish();

    let thread_pool = rayon::ThreadPoolBuilder::new()
        .num_threads(16)
        .build()
        .unwrap();
    // Keep every Rayon dispatch alive long enough that Criterion measures the
    // split computation rather than worker wake-up/parking and work stealing.
    const THREADED_REPETITIONS: usize = 32;
    let mut threaded_splits = c.benchmark_group("minimizer-splits-16-threads");
    threaded_splits.measurement_time(Duration::from_secs(8));
    threaded_splits.throughput(Throughput::Elements(
        (hashes_with_u32.len() * cn_nthash_simd::SIMD_LANES * 16 * THREADED_REPETITIONS) as u64,
    ));
    let mut threaded_scalar_queues: Vec<[_; cn_nthash_simd::SIMD_LANES]> = (0..16)
        .map(|_| {
            std::array::from_fn(|_| rolling::batch_minqueue::BatchMinQueue::<u32>::new(window))
        })
        .collect();
    threaded_splits.bench_function("scalar-x-u32", |b| {
        b.iter(|| {
            let checksum = thread_pool.install(|| {
                threaded_scalar_queues
                    .par_iter_mut()
                    .enumerate()
                    .map(|(worker, queues)| {
                        let mut checksum = worker as u64;
                        for repetition in 0..THREADED_REPETITIONS {
                            checksum = checksum.rotate_left(1) ^ repetition as u64;
                            for lane in 0..cn_nthash_simd::SIMD_LANES {
                                queues[lane].get_minimizer_splits::<_, true>(
                                    black_box(hashes_with_u32.as_slice()).iter().map(
                                        |(hash, metadata)| {
                                            (hash.to_array()[lane] as u64, metadata[lane])
                                        },
                                    ),
                                    0,
                                    0,
                                    |index, item, last| {
                                        checksum ^= item.0
                                            ^ item.1 as u64
                                            ^ index as u64
                                            ^ lane as u64
                                            ^ ((last as u64) << 63);
                                    },
                                );
                            }
                        }
                        checksum
                    })
                    .reduce(|| 0, |left, right| left ^ right)
            });
            black_box(checksum)
        })
    });
    let mut threaded_simd_queues: Vec<_> = (0..16)
        .map(|_| SimdBatchMinQueue::<u32>::new(window))
        .collect();
    threaded_splits.bench_function("simd-x-u32", |b| {
        b.iter(|| {
            let checksum = thread_pool.install(|| {
                threaded_simd_queues
                    .par_iter_mut()
                    .enumerate()
                    .map(|(worker, queue)| {
                        let mut checksum = worker as u64;
                        for repetition in 0..THREADED_REPETITIONS {
                            checksum = checksum.rotate_left(1) ^ repetition as u64;
                            queue
                                .get_minimizer_splits_slice::<true, { cn_nthash_simd::SIMD_LANES }>(
                                    black_box(hashes_with_u32.as_slice()),
                                    |lane, index, item, last| {
                                        checksum ^= item.0 as u64
                                            ^ item.1 as u64
                                            ^ index as u64
                                            ^ lane as u64
                                            ^ ((last as u64) << 63);
                                    },
                                );
                        }
                        checksum
                    })
                    .reduce(|| 0, |left, right| left ^ right)
            });
            black_box(checksum)
        })
    });
    let mut threaded_hash_split_queues: Vec<_> = (0..16)
        .map(|_| SimdBatchMinQueue::<()>::new(window))
        .collect();
    threaded_splits.bench_function("simd-hash-only", |b| {
        b.iter(|| {
            let checksum = thread_pool.install(|| {
                threaded_hash_split_queues
                    .par_iter_mut()
                    .enumerate()
                    .map(|(worker, queue)| {
                        let mut checksum = worker as u64;
                        for repetition in 0..THREADED_REPETITIONS {
                            checksum = checksum.rotate_left(1) ^ repetition as u64;
                            queue.get_minimizer_hash_splits_slice::<
                                true,
                                { cn_nthash_simd::SIMD_LANES },
                            >(
                                black_box(hash_vectors.as_slice()),
                                |lane, index, hash, last| {
                                    checksum ^= hash as u64
                                        ^ index as u64
                                        ^ lane as u64
                                        ^ ((last as u64) << 63);
                                },
                            );
                        }
                        checksum
                    })
                    .reduce(|| 0, |left, right| left ^ right)
            });
            black_box(checksum)
        })
    });
    let mut threaded_valid_split_queues: Vec<_> = (0..16)
        .map(|_| SimdBatchMinQueue::<()>::new(window))
        .collect();
    threaded_splits.bench_function("simd-valid-dna", |b| {
        b.iter(|| {
            let checksum = thread_pool.install(|| {
                threaded_valid_split_queues
                    .par_iter_mut()
                    .enumerate()
                    .map(|(worker, queue)| {
                        let mut checksum = worker as u64;
                        for repetition in 0..THREADED_REPETITIONS {
                            checksum = checksum.rotate_left(1) ^ repetition as u64;
                            queue.get_valid_minimizer_splits::<_, true>(
                                black_box(hash_vectors.as_slice()).iter().map(|&h| (h, ())),
                                black_box(valid_split_lanes.as_slice()),
                                |run| {
                                    checksum ^= run.hash as u64
                                        ^ run.start as u64
                                        ^ run.lane as u64
                                        ^ ((run.finished as u64) << 63);
                                },
                            );
                        }
                        checksum
                    })
                    .reduce(|| 0, |left, right| left ^ right)
            });
            black_box(checksum)
        })
    });
    threaded_splits.finish();

    let mut pipeline = c.benchmark_group("nthash-minqueue-pipeline-8-sequences");
    pipeline.throughput(Throughput::Elements(
        (len * cn_nthash_simd::SIMD_LANES) as u64,
    ));
    pipeline.bench_function("scalar", |b| {
        b.iter(|| {
            let mut sum = 0u64;
            for lane in &plain_lanes {
                let hashes: Vec<_> =
                    cn_nthash::CanonicalNtHashIterator::new(black_box(lane.as_slice()), k)
                        .unwrap()
                        .iter()
                        .map(|h| (h.to_unextendable() | 1, ()))
                        .collect();
                rolling::batch_minqueue::BatchMinQueue::new(window).get_minimizers::<_, true>(
                    hashes.into_iter(),
                    0,
                    |item, _| sum ^= item.0,
                    |_| {},
                );
            }
            black_box(sum)
        })
    });
    pipeline.bench_function("simd", |b| {
        b.iter(|| {
            let seq = cn_nthash_simd::PackedSimdSequence::<{ cn_nthash_simd::SIMD_LANES }>::new(
                black_box(&packed),
                len,
            )
            .unwrap();
            let hashes: Vec<_> = cn_nthash_simd::CanonicalNtHashSimdIterator::new(seq, k)
                .unwrap()
                .map(|h| {
                    (
                        h.to_unextendable() | u32x8::splat(1),
                        [(); cn_nthash_simd::SIMD_LANES],
                    )
                })
                .collect();
            let mut sum = u32x8::splat(0);
            SimdBatchMinQueue::new(window).get_minimizer_hashes_simd::<_, true>(
                hashes.into_iter().map(|item| item.0),
                0,
                |hash, _| sum ^= hash,
                |_| {},
            );
            black_box(sum)
        })
    });
    pipeline.finish();

    bench_fasta_simd_pipeline(c);
    bench_scalar_fasta_pipeline(c);
}

criterion_group! {
    name = benches;
    config = Criterion::default()
        .warm_up_time(Duration::from_millis(500))
        .measurement_time(Duration::from_millis(1_500));
    targets = criterion_benchmark
}

criterion_main!(benches);
