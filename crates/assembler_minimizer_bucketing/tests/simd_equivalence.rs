//! The vectorized super-k-mer splitting must agree with the ordinary one.
//!
//! Both sides are driven with the same minimizer hash, so every super-k-mer,
//! its bucket, its flags and its minimizer position have to match exactly.

use colors::non_colored::NonColoredManager;
use ggcat_assembler_minibuck::{
    AssemblerMinimizerBucketingExecutorFactory, AssemblerPreprocessInfo,
};
use hashes::HashableSequence;
use hashes::cn_nthash32::CanonicalNtHash32IteratorFactory;
use io::DUPLICATES_BUCKET_EXTRA;
use io::sequences_reader::DnaSequencesFileType;
use io::sequences_stream::SequenceInfo;
use minimizer_bucketing::lane_runs::SimdScratch;
use minimizer_bucketing::simd_batch::{RecordExtra, RecordInfo, SequencesLaneBatch};
use minimizer_bucketing::{
    MinimizerBucketingCommonData, MinimizerBucketingExecutor, MinimizerBucketingExecutorFactory,
    PushSequenceInfo,
};
use parallel_processor::buckets::{BucketsCount, ExtraBuckets};
use simd_accel::batch::VecBatchSink;
use simd_accel::hashing::SIMD_LANES;
use simd_accel::packer::LanePacker;
use std::sync::Arc;

type Factory = AssemblerMinimizerBucketingExecutorFactory<NonColoredManager>;

/// One emitted super-k-mer, in a form both paths can be compared on.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct SuperKmer {
    bucket: u16,
    second_bucket: u16,
    bases: String,
    flags: u8,
    minimizer_pos: u16,
    reverse_complement: bool,
}

fn collect<S: minimizer_bucketing::MinimizerInputSequence>(
    info: PushSequenceInfo<'_, S, Factory>,
    output: &mut Vec<SuperKmer>,
) {
    output.push(SuperKmer {
        bucket: info.bucket,
        second_bucket: info.second_bucket,
        bases: info.sequence.debug_to_string(),
        flags: info.flags,
        minimizer_pos: info.minimizer_pos,
        reverse_complement: info.rc,
    });
}

fn pseudo_random_sequence(length: usize, seed: u32, ambiguity_every: usize) -> Vec<u8> {
    let mut state = seed | 1;
    (0..length)
        .map(|index| {
            state = state.wrapping_mul(1_664_525).wrapping_add(1_013_904_223);
            if ambiguity_every != 0 && index % ambiguity_every == ambiguity_every - 1 {
                b'N'
            } else {
                b"ACGT"[(state >> 29) as usize & 3]
            }
        })
        .collect()
}

fn common_data(k: usize, m: usize, canonical: bool) -> Arc<MinimizerBucketingCommonData<()>> {
    Arc::new(MinimizerBucketingCommonData::new(
        k,
        m,
        BucketsCount::new(
            4,
            ExtraBuckets::Extra {
                count: 1,
                data: DUPLICATES_BUCKET_EXTRA,
            },
        ),
        k,
        BucketsCount::new(3, ExtraBuckets::None),
        (),
        canonical,
    ))
}

fn pack(records: &[Vec<u8>], k: usize, bases_per_lane: usize) -> Vec<SequencesLaneBatch> {
    let mut packer = LanePacker::<RecordExtra>::new(k, bases_per_lane, k, false, 1 << 20).unwrap();
    let mut sink = VecBatchSink::new(bases_per_lane);
    for (index, record) in records.iter().enumerate() {
        packer.push_record(
            &mut sink,
            (
                SequenceInfo {
                    color: Some(index as u32),
                },
                DnaSequencesFileType::FASTA,
            ),
            b"",
            record,
            false,
        );
    }
    packer.finish(&mut sink);
    for batch in &sink.batches {
        batch.debug_check(k);
    }
    sink.batches
}

fn run_case(k: usize, m: usize, canonical: bool, bases_per_lane: usize, records: &[Vec<u8>]) {
    let common = common_data(k, m, canonical);
    let first_bits = common.buckets_count.normal_buckets_count_log;
    let second_bits = common.second_buckets_count.normal_buckets_count_log;
    let mut executor = Factory::new(&common);
    let mut scratch = SimdScratch::default();

    for batch in pack(records, k, bases_per_lane) {
        let mut preprocess: Vec<AssemblerPreprocessInfo<NonColoredManager>> =
            (0..batch.records.len())
                .map(|_| Default::default())
                .collect();
        for (index, record) in batch.records.iter().enumerate() {
            executor.preprocess_dna_sequence(
                &Default::default(),
                record.extra.0,
                record.read_index,
                &RecordInfo {
                    ident_data: b"",
                    format: record.extra.1,
                    bases_count: record.bases_total as usize,
                },
                &mut preprocess[index],
            );
        }

        let mut vectorized = Vec::new();
        executor.process_simd_batch::<_, true>(
            &batch,
            &preprocess,
            &mut scratch,
            0,
            first_bits,
            second_bits,
            |info| collect(info, &mut vectorized),
        );

        // Every lane fragment is an independent sequence, so the ordinary
        // splitting is run on each of them separately.
        let mut ordinary = Vec::new();
        scratch.destride(&batch);
        for lane in 0..SIMD_LANES {
            let read = scratch.lane_read(lane);
            for pair in batch.lanes[lane].windows(2) {
                let (start, end) = (pair[0].lane_start as usize, pair[1].lane_start as usize);
                let fragment = read.sub_slice((start..end).into());
                executor.process_sequence_with::<CanonicalNtHash32IteratorFactory, _, _, true>(
                    &preprocess[pair[0].record_idx as usize],
                    fragment,
                    0..fragment.bases_count(),
                    0,
                    first_bits,
                    second_bits,
                    |info| collect(info, &mut ordinary),
                );
            }
        }

        vectorized.sort();
        ordinary.sort();
        assert_eq!(
            vectorized, ordinary,
            "k {k}, m {m}, canonical {canonical}, lane {bases_per_lane}"
        );
        assert!(!vectorized.is_empty());
        for super_kmer in &vectorized {
            assert!(
                (super_kmer.minimizer_pos as usize) < k,
                "the minimizer must lie inside the first k-mer"
            );
            assert!(super_kmer.bases.len() >= k);
        }
    }
}

#[test]
fn vectorized_splitting_matches_the_ordinary_one() {
    for (k, m) in [(21usize, 10usize), (31, 12), (63, 14)] {
        for canonical in [true, false] {
            run_case(
                k,
                m,
                canonical,
                1024,
                &[
                    pseudo_random_sequence(900, 7, 0),
                    pseudo_random_sequence(500, 11, 0),
                    pseudo_random_sequence(k, 13, 0),
                    pseudo_random_sequence(k - 1, 17, 0),
                ],
            );
        }
    }
}

#[test]
fn ambiguity_characters_do_not_change_the_result() {
    for (k, m) in [(21usize, 10usize), (31, 12)] {
        run_case(
            k,
            m,
            true,
            512,
            &[
                pseudo_random_sequence(800, 3, 97),
                pseudo_random_sequence(300, 5, 40),
                pseudo_random_sequence(200, 23, k),
            ],
        );
    }
}

#[test]
fn records_split_over_several_lanes_match() {
    // Long records cross lanes and batches, which repeats k-1 bases at every
    // seam; each piece must still be split exactly like a standalone sequence.
    for (k, m) in [(31usize, 12usize), (21, 10)] {
        run_case(
            k,
            m,
            true,
            256,
            &[
                pseudo_random_sequence(5000, 29, 0),
                pseudo_random_sequence(3000, 31, 700),
            ],
        );
    }
}

#[test]
fn repeated_sequences_take_the_duplicates_bucket() {
    let (k, m) = (31usize, 12usize);
    let common = common_data(k, m, true);
    let duplicates_bucket = common.buckets_count.normal_buckets_count as u16;
    let mut executor = Factory::new(&common);
    let mut scratch = SimdScratch::default();
    // A single repeated base makes every minimizer of the window identical, so
    // the super-k-mers can only be routed to the duplicates bucket.
    let records = vec![vec![b'A'; 400]];
    let batches = pack(&records, k, 1024);
    let mut found = 0;
    for batch in batches {
        let mut preprocess: Vec<AssemblerPreprocessInfo<NonColoredManager>> =
            (0..batch.records.len())
                .map(|_| Default::default())
                .collect();
        for (index, record) in batch.records.iter().enumerate() {
            executor.preprocess_dna_sequence(
                &Default::default(),
                record.extra.0,
                record.read_index,
                &RecordInfo {
                    ident_data: b"",
                    format: record.extra.1,
                    bases_count: record.bases_total as usize,
                },
                &mut preprocess[index],
            );
        }
        executor.process_simd_batch::<_, true>(
            &batch,
            &preprocess,
            &mut scratch,
            0,
            common.buckets_count.normal_buckets_count_log,
            common.second_buckets_count.normal_buckets_count_log,
            |info| {
                assert_eq!(info.bucket, duplicates_bucket);
                assert_eq!(info.minimizer_pos, 0);
                assert!(!info.rc);
                found += 1;
            },
        );
    }
    assert!(found > 0);
}
