use super::formats::InputFormat;
use super::{BoundedDeduplicator, BypassPolicy};
use crate::MinimizerBucketMode;
use colors::bucket_colors::expand_runs;
use colors::colors_manager::{
    MinimizerBucketingSeqColorData, MinimizerBucketingSeqColorDataIterable,
};
use colors::non_colored::NonColoredManager;
use colors::parsers::separate::{MinBkMultipleColors, MinBkSingleColor};
use colors::parsers::{SequenceIdent, SingleSequenceInfo};
use config::ColorIndexType;
use config::MultiplicityCounterType;
use io::concurrent::temp_reads::creads_utils::helpers::helper_read_bucket;
use io::concurrent::temp_reads::creads_utils::{
    AssemblerMinimizerPosition, CompressedReadsBucketData, NoAlignment, NoMultiplicity,
    WithMultiplicity, WithSecondBucket,
};
use io::concurrent::temp_reads::extra_data::SequenceExtraDataCombiner;
use parallel_processor::buckets::LockFreeBucket;
use parallel_processor::buckets::bucket_writer::BucketItemSerializer;
use parallel_processor::buckets::readers::binary_reader::ChunkedBinaryReaderIndex;
use parallel_processor::buckets::writers::lock_free_binary_writer::LockFreeBinaryWriter;
use parallel_processor::memory_fs::file::internal::MemoryFileMode;
use parallel_processor::memory_fs::{MemoryFs, RemoveFileMode};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

const K: usize = 31;
const M: usize = 64 * 1024;
type Flags = typenum::U2;

/// `MemoryFs` is process global and `flush_to_disk` flushes every file any
/// test created, so two tests that both write buckets would read each
/// other's half-flushed state. Every test here holds this for its body.
fn memory_fs() -> parking_lot::MutexGuard<'static, ()> {
    static LOCK: parking_lot::Mutex<()> = parking_lot::Mutex::new(());
    static INIT: std::sync::Once = std::sync::Once::new();
    let guard = LOCK.lock();
    INIT.call_once(|| {
        MemoryFs::init(
            parallel_processor::memory_data_size::MemoryDataSize::from_bytes(64 * 1024 * 1024),
            4,
            1,
            16,
            None,
        );
    });
    guard
}

fn root(name: &str) -> PathBuf {
    let path = std::env::temp_dir().join(format!("ggcat-dedup-{}-{name}", std::process::id()));
    std::fs::create_dir_all(&path).unwrap();
    path
}

fn writer(path: &Path) -> Arc<LockFreeBinaryWriter> {
    Arc::new(LockFreeBinaryWriter::new(
        path,
        &(
            MemoryFileMode::DiskOnly,
            LockFreeBinaryWriter::CHECKPOINT_SIZE_UNLIMITED,
        ),
        0,
        &MinimizerBucketMode::Single,
    ))
}

/// One input record in the format `add` expects.
#[derive(Clone)]
struct Record {
    sequence: Vec<u8>,
    flags: u8,
    second_bucket: u8,
    minimizer_pos: u16,
    multiplicity: MultiplicityCounterType,
    colors: Vec<ColorIndexType>,
}

impl Record {
    fn new(sequence: &[u8], color: ColorIndexType) -> Self {
        Self {
            sequence: sequence.to_vec(),
            flags: 1,
            second_bucket: 3,
            minimizer_pos: 0,
            multiplicity: 1,
            colors: vec![color],
        }
    }
    fn key(&self) -> (Vec<u8>, u8, u8) {
        (self.sequence.clone(), self.flags, self.second_bucket)
    }
}

/// Encodes records the way a producer does: one narrow record per
/// occurrence, each on its own, so where a slice ends cannot matter.
///
/// A fixture record standing for a multiplicity of `m` over `c` colours is
/// written as `m` occurrences taking those colours in turn, which is what a
/// producer would have emitted for it.
fn encode(records: &[Record]) -> Vec<u8> {
    let mut format = InputFormat::<MinBkMultipleColors, Flags>::new(K);
    let mut out = Vec::new();
    for record in records {
        for occurrence in 0..record.multiplicity as usize {
            format.reset();
            format.write_to(
                &CompressedReadsBucketData::new(
                    record.sequence.as_slice(),
                    record.flags,
                    record.second_bucket,
                    record.minimizer_pos,
                ),
                &mut out,
                &MinBkSingleColor::create(
                    SingleSequenceInfo {
                        static_color: record.colors[occurrence % record.colors.len()],
                        sequence_ident: SequenceIdent::FASTA(b"fixture"),
                    },
                    &mut (),
                ),
                &(),
            );
        }
    }
    out
}

/// Reads the output bucket back and groups it the way the input was built.
fn decode(path: &Path) -> BTreeMap<(Vec<u8>, u8, u8), (u64, Vec<ColorIndexType>)> {
    MemoryFs::flush_to_disk(true);
    MemoryFs::ensure_flushed(path);
    let index = ChunkedBinaryReaderIndex::from_file(path, RemoveFileMode::Keep);
    let mut out: BTreeMap<_, (u64, Vec<ColorIndexType>)> = BTreeMap::new();
    helper_read_bucket::<
        MinBkMultipleColors,
        WithSecondBucket,
        WithMultiplicity,
        AssemblerMinimizerPosition,
        Flags,
        NoAlignment,
    >(
        index.into_chunks(),
        None,
        |read, buffer| {
            let key = (
                read.read.to_string().into_bytes(),
                read.flags,
                read.second_bucket,
            );
            let entry = out.entry(key).or_default();
            entry.0 += read.multiplicity as u64;
            entry
                .1
                .extend(expand_runs(read.extra.get_unique_color(buffer)));
        },
        K,
    );
    for value in out.values_mut() {
        value.1.sort_unstable();
        value.1.dedup();
    }
    out
}

/// What the deduplicator should produce, computed independently.
fn expected(records: &[Record]) -> BTreeMap<(Vec<u8>, u8, u8), (u64, Vec<ColorIndexType>)> {
    let mut out: BTreeMap<_, (u64, Vec<ColorIndexType>)> = BTreeMap::new();
    for record in records {
        let entry = out.entry(record.key()).or_default();
        entry.0 += record.multiplicity as u64;
        entry.1.extend(record.colors.iter().copied());
    }
    for value in out.values_mut() {
        value.1.sort_unstable();
        value.1.dedup();
    }
    out
}

fn sequence(seed: usize, len: usize) -> Vec<u8> {
    const BASES: &[u8] = b"ACGT";
    let mut state = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
    (0..len)
        .map(|_| {
            state = state
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            BASES[(state >> 33) as usize % 4]
        })
        .collect()
}

/// How many records the wide output holds as written. [`decode`] sums
/// repeats by key, so this is the only place a fold shows.
fn count_records(path: &Path) -> usize {
    MemoryFs::flush_to_disk(true);
    MemoryFs::ensure_flushed(path);
    let index = ChunkedBinaryReaderIndex::from_file(path, RemoveFileMode::Keep);
    let mut count = 0;
    helper_read_bucket::<
        MinBkMultipleColors,
        WithSecondBucket,
        WithMultiplicity,
        AssemblerMinimizerPosition,
        Flags,
        NoAlignment,
    >(index.into_chunks(), None, |_, _| count += 1, K);
    count
}

fn run(name: &str, memory: usize, slices: &[Vec<u8>]) -> PathBuf {
    let path = root(name).join("out");
    let bucket = writer(&path);
    let dedup = BoundedDeduplicator::<MinBkMultipleColors, Flags, _>::new(memory, K, bucket);
    for slice in slices {
        dedup.add(slice);
    }
    let bucket = dedup.finalize();
    let file = bucket.get_path();
    Arc::try_unwrap(bucket).ok().unwrap().finalize();
    file
}

/// The shipped policy, but with a window floor that fits the 64 KiB test
/// budget: a real bucket drains every ~6000 records, a test one every ~2000.
fn test_policy() -> BypassPolicy {
    BypassPolicy {
        min_records: 64,
        ..BypassPolicy::from_config()
    }
}

/// A writer tagged for the narrow format the bypass forwards.
fn narrow_writer(path: &Path) -> Arc<LockFreeBinaryWriter> {
    Arc::new(LockFreeBinaryWriter::new(
        path,
        &(
            MemoryFileMode::DiskOnly,
            LockFreeBinaryWriter::CHECKPOINT_SIZE_UNLIMITED,
        ),
        0,
        &MinimizerBucketMode::UncompactedNarrow,
    ))
}

/// Reads a bypass bucket, which holds the producer's own narrow records.
fn decode_narrow(path: &Path) -> BTreeMap<(Vec<u8>, u8, u8), (u64, Vec<ColorIndexType>)> {
    MemoryFs::flush_to_disk(true);
    MemoryFs::ensure_flushed(path);
    let index = ChunkedBinaryReaderIndex::from_file(path, RemoveFileMode::Keep);
    let mut out: BTreeMap<_, (u64, Vec<ColorIndexType>)> = BTreeMap::new();
    helper_read_bucket::<
        MinBkSingleColor,
        WithSecondBucket,
        NoMultiplicity,
        AssemblerMinimizerPosition,
        Flags,
        NoAlignment,
    >(
        index.into_chunks(),
        None,
        |read, _buffer| {
            let key = (
                read.read.to_string().into_bytes(),
                read.flags,
                read.second_bucket,
            );
            let entry = out.entry(key).or_default();
            // The narrow format carries no multiplicity, so each record
            // stands for exactly one occurrence.
            assert_eq!(read.multiplicity, 1);
            entry.0 += 1;
            entry.1.push(read.extra.get_unique_color(&()));
        },
        K,
    );
    for value in out.values_mut() {
        value.1.sort_unstable();
        value.1.dedup();
    }
    out
}

/// As [`run`], but with somewhere to bypass to. Returns both sides.
fn run_with_bypass(
    name: &str,
    memory: usize,
    policy: BypassPolicy,
    slices: &[Vec<u8>],
) -> (PathBuf, PathBuf) {
    let root = root(name);
    let wide_path = root.join("wide");
    let narrow_path = root.join("narrow");
    let wide = writer(&wide_path);
    let narrow = narrow_writer(&narrow_path);
    let dedup = BoundedDeduplicator::<MinBkMultipleColors, Flags, _>::new_with_bypass(
        memory,
        K,
        wide,
        Some(narrow),
        policy,
    );
    for slice in slices {
        dedup.add(slice);
    }
    let (wide, narrow) = dedup.finalize_with_bypass();
    let (wide_file, narrow_file) = (wide.get_path(), narrow.as_ref().unwrap().get_path());
    Arc::try_unwrap(wide).ok().unwrap().finalize();
    Arc::try_unwrap(narrow.unwrap()).ok().unwrap().finalize();
    (wide_file, narrow_file)
}

/// Merges what the two sides hold, the way the compactor does.
fn union(
    wide: BTreeMap<(Vec<u8>, u8, u8), (u64, Vec<ColorIndexType>)>,
    narrow: BTreeMap<(Vec<u8>, u8, u8), (u64, Vec<ColorIndexType>)>,
) -> BTreeMap<(Vec<u8>, u8, u8), (u64, Vec<ColorIndexType>)> {
    let mut out = wide;
    for (key, (multiplicity, colors)) in narrow {
        let entry = out.entry(key).or_default();
        entry.0 += multiplicity;
        entry.1.extend(colors);
    }
    for value in out.values_mut() {
        value.1.sort_unstable();
        value.1.dedup();
    }
    out
}

#[test]
fn bypass_forwards_records_untouched() {
    let _guard = memory_fs();
    let records: Vec<_> = (0..200)
        .map(|i| Record::new(&sequence(i, 40 + i % 25), (i % 17) as ColorIndexType))
        .collect();

    // One slice per record, then the whole lot in one slice: where a slice
    // ends must not change what comes out, which is what lets the bypass
    // forward the producer's bytes without looking at them.
    for (name, slices) in [
        (
            "bypass-per-record",
            records
                .iter()
                .map(|r| encode(std::slice::from_ref(r)))
                .collect::<Vec<_>>(),
        ),
        ("bypass-one-slice", vec![encode(&records)]),
        (
            "bypass-uneven",
            records.chunks(7).map(encode).collect::<Vec<_>>(),
        ),
    ] {
        let (wide, narrow) = run_with_bypass(name, M, BypassPolicy::always(), &slices);
        assert!(
            decode(&wide).is_empty(),
            "{name}: wide side should be empty"
        );
        assert_eq!(decode_narrow(&narrow), expected(&records), "{name}");
    }
}

#[test]
fn bypass_triggers_on_unique_records() {
    let _guard = memory_fs();
    // All distinct, so nothing ever collapses.
    let records: Vec<_> = (0..6000)
        .map(|i| Record::new(&sequence(i, 40 + i % 25), (i % 97) as ColorIndexType))
        .collect();
    let slices: Vec<_> = records.chunks(64).map(encode).collect();

    let (wide, narrow) = run_with_bypass("bypass-unique", M, test_policy(), &slices);

    assert!(
        !decode_narrow(&narrow).is_empty(),
        "a bucket that collapses nothing should stand down"
    );
    // Whatever the split, together they are the input.
    assert_eq!(
        union(decode(&wide), decode_narrow(&narrow)),
        expected(&records)
    );
}

#[test]
fn bypass_does_not_trigger_on_redundant_records() {
    let _guard = memory_fs();
    // Sixty distinct super-kmers, each repeated a hundred times.
    let records: Vec<_> = (0..6000)
        .map(|i| Record::new(&sequence(i % 60, 45), (i % 13) as ColorIndexType))
        .collect();
    let slices: Vec<_> = records.chunks(64).map(encode).collect();

    let (wide, narrow) = run_with_bypass("bypass-redundant", M, test_policy(), &slices);

    assert!(
        decode_narrow(&narrow).is_empty(),
        "a bucket collapsing 99% must keep folding"
    );
    assert_eq!(decode(&wide), expected(&records));
}

#[test]
fn records_survive_a_mid_stream_flip() {
    let _guard = memory_fs();
    // Redundant first, then unique: the bucket should keep folding, then
    // stand down, and nothing may be lost or counted twice at the seam.
    let redundant: Vec<_> = (0..3000)
        .map(|i| Record::new(&sequence(i % 40, 45), (i % 11) as ColorIndexType))
        .collect();
    let unique: Vec<_> = (0..6000)
        .map(|i| {
            Record::new(
                &sequence(100_000 + i, 40 + i % 25),
                (i % 23) as ColorIndexType,
            )
        })
        .collect();
    let all: Vec<_> = redundant.iter().chain(unique.iter()).cloned().collect();
    let slices: Vec<_> = all.chunks(64).map(encode).collect();

    let (wide, narrow) = run_with_bypass("bypass-flip", M, test_policy(), &slices);

    assert!(
        !decode_narrow(&narrow).is_empty(),
        "the unique suffix should stand down"
    );
    assert!(
        !decode(&wide).is_empty(),
        "the redundant prefix should have been folded"
    );
    assert_eq!(
        union(decode(&wide), decode_narrow(&narrow)),
        expected(&all),
        "the two sides together must be exactly the input"
    );
}

/// An extra data type that cannot be combined at all must never reach the
/// folding path, because its `combine_entries` is `unimplemented!()`. That
/// is the querier and dumper configuration, where a repeated super-kmer
/// used to panic.
#[test]
fn a_non_combinable_type_always_bypasses() {
    use colors::parsers::graph::MinBkMultipleColors as NonCombinable;
    let _guard = memory_fs();
    assert!(
        !<NonCombinable as SequenceExtraDataCombiner>::ALLOW_COMBINE,
        "fixture must be a type that cannot be combined"
    );

    let root = root("bypass-no-combine");
    let dedup = BoundedDeduplicator::<NonCombinable, Flags, _>::new_with_bypass(
        M,
        K,
        writer(&root.join("wide")),
        Some(narrow_writer(&root.join("narrow"))),
        // Even a policy that would never choose to stand down must not
        // override this: the decision is not the heuristic's to make.
        BypassPolicy::never(),
    );
    assert!(
        dedup.is_bypassing(),
        "a non-combinable type has to bypass from the first record"
    );
    let (wide, narrow) = dedup.finalize_with_bypass();
    Arc::try_unwrap(wide).ok().unwrap().finalize();
    Arc::try_unwrap(narrow.unwrap()).ok().unwrap().finalize();
}

#[test]
fn deduplicates_and_unions_colors() {
    let _guard = memory_fs();
    let a = b"ACGTACGTACGTACGTACGTACGTACGTACGTA".to_vec();
    let b = b"TTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTT".to_vec();
    let records: Vec<_> = [(&a, 1), (&b, 7), (&a, 2), (&a, 1), (&b, 8), (&a, 3)]
        .into_iter()
        .map(|(sequence, color)| Record::new(sequence, color))
        .collect();

    let path = run("union", M, &[encode(&records)]);
    assert_eq!(
        count_records(&path),
        2,
        "six records of two super-kmers fold into two"
    );
    assert_eq!(decode(&path), expected(&records));
}

// Records that differ only in a field of the key stay separate. second_bucket
// matters most: memstorage does not serialize it, so it is carried
// separately and could silently collapse or be zeroed on output.
#[test]
fn key_fields_separate_records() {
    let _guard = memory_fs();
    let sequence = b"ACGTACGTACGTACGTACGTACGTACGTACGTA";
    let mut records = Vec::new();
    for (flags, second_bucket, minimizer_pos) in [(1u8, 3u8, 0u16), (2, 3, 0), (1, 4, 0), (1, 3, 0)]
    {
        let mut record = Record::new(sequence, 5);
        record.flags = flags;
        record.second_bucket = second_bucket;
        record.minimizer_pos = minimizer_pos;
        records.push(record);
    }
    let path = run("keys", M, &[encode(&records)]);
    assert_eq!(count_records(&path), 3);
    let decoded = decode(&path);
    assert_eq!(decoded, expected(&records));
    // The sub-bucket byte really is reproduced, not zeroed.
    assert!(
        decoded
            .keys()
            .any(|(_, _, second_bucket)| *second_bucket == 4)
    );
    assert!(
        decoded
            .keys()
            .all(|(_, _, second_bucket)| *second_bucket != 0)
    );
}

#[test]
fn multiplicity_is_summed() {
    let _guard = memory_fs();
    let sequence = b"ACGTACGTACGTACGTACGTACGTACGTACGTA";
    let records: Vec<_> = (0..4)
        .map(|i| {
            let mut record = Record::new(sequence, 5);
            record.multiplicity = 10 + i;
            record
        })
        .collect();
    let path = run("mult", M, &[encode(&records)]);
    assert_eq!(count_records(&path), 1);
    assert_eq!(decode(&path).values().next().unwrap().0, 10 + 11 + 12 + 13);
}

// Enough distinct super-kmers to force repeated drains. A super-kmer that
// spans two drains is emitted twice with partial multiplicity, which is the
// defining semantic of a bounded deduplicator -- the union still has to
// reconstruct exactly.
#[test]
fn drains_repeatedly_and_reconstructs_the_input() {
    let _guard = memory_fs();
    let records: Vec<_> = (0..4000)
        .map(|i| Record::new(&sequence(i, 40 + i % 25), (i % 97) as ColorIndexType))
        .collect();
    let slices: Vec<_> = records.chunks(64).map(encode).collect();
    let path = run("drain", M, &slices);
    assert_eq!(decode(&path), expected(&records));
}

// Long super-kmers make the bases storage, rather than the table, the first
// bound to be reached. With short records the table always binds first, so
// without this the storage branch is never taken.
#[test]
fn long_records_drain_on_the_storage_bound() {
    let _guard = memory_fs();
    let records: Vec<_> = (0..1200)
        .map(|i| Record::new(&sequence(i, 250), (i % 7) as ColorIndexType))
        .collect();
    let slices: Vec<_> = records.chunks(32).map(encode).collect();
    let path = run("storage", M, &slices);
    assert_eq!(decode(&path), expected(&records));
}

// A record too large to ever be stored bypasses the table and the storage
// and is written straight out, instead of looping forever on a drain that
// cannot make room.
#[test]
fn a_record_larger_than_the_storage_is_written_straight_out() {
    let _guard = memory_fs();
    let huge = Record::new(&sequence(1, 4 * M), 5);
    let small = Record::new(&sequence(2, 40), 6);
    let records = vec![small.clone(), huge.clone(), small];
    let path = run("huge", M, &[encode(&records)]);
    assert_eq!(decode(&path), expected(&records));
}

// A slice larger than one buffer bypasses them entirely.
#[test]
fn oversized_slice_is_processed_inline() {
    let _guard = memory_fs();
    let records: Vec<_> = (0..3000)
        .map(|i| Record::new(&sequence(i, 60), (i % 11) as ColorIndexType))
        .collect();
    let slice = encode(&records);
    assert!(slice.len() > M / 2, "fixture is not oversized");
    let path = run("inline", M, &[slice]);
    assert_eq!(decode(&path), expected(&records));
}

// Many slices that each fit, so the buffers fill and swap.
#[test]
fn buffers_swap_without_losing_records() {
    let _guard = memory_fs();
    let records: Vec<_> = (0..6000)
        .map(|i| Record::new(&sequence(i % 500, 45), (i % 31) as ColorIndexType))
        .collect();
    let slices: Vec<_> = records.chunks(16).map(encode).collect();
    let path = run("swap", M, &slices);
    assert_eq!(decode(&path), expected(&records));
}

// The lock-free append path, hammered from several threads. Total
// multiplicity and the colour union must come out exact.
#[test]
fn concurrent_appends_lose_nothing() {
    let _guard = memory_fs();
    let records: Vec<_> = (0..8000)
        .map(|i| Record::new(&sequence(i % 700, 40 + i % 30), (i % 53) as ColorIndexType))
        .collect();
    let slices: Vec<Vec<u8>> = records.chunks(8).map(encode).collect();

    let path = root("threads").join("out");
    let bucket = writer(&path);
    let dedup = Arc::new(BoundedDeduplicator::<MinBkMultipleColors, Flags, _>::new(
        M, K, bucket,
    ));
    std::thread::scope(|scope| {
        for shard in 0..8 {
            let dedup = &dedup;
            let slices = &slices;
            scope.spawn(move || {
                for slice in slices.iter().skip(shard).step_by(8) {
                    dedup.add(slice);
                }
            });
        }
    });
    let dedup = Arc::try_unwrap(dedup).ok().unwrap();
    let bucket = dedup.finalize();
    let file = bucket.get_path();
    Arc::try_unwrap(bucket).ok().unwrap().finalize();

    assert_eq!(decode(&file), expected(&records));
}

// The drain buffer belongs to the thread, not the deduplicator, so a record
// written by one thread must never be left sitting in that thread's buffer
// for another thread's drain to miss. Oversized records take the one path
// that writes outside a drain, so they are what exercises it.
#[test]
fn thread_local_staging_loses_nothing_across_threads() {
    let _guard = memory_fs();
    // Enough distinct super-kmers to force several drains while the
    // oversized records are going out.
    let mut records: Vec<Record> = (0..6000)
        .map(|i| Record::new(&sequence(i % 3000, 45), (i % 23) as ColorIndexType))
        .collect();
    // One record per thread that no drain could make room for.
    for shard in 0..4 {
        records.insert(
            shard * 1500,
            Record::new(
                &sequence(9000 + shard, 4 * M),
                (shard + 90) as ColorIndexType,
            ),
        );
    }
    let slices: Vec<Vec<u8>> = records.chunks(4).map(|c| encode(c)).collect();

    let path = root("tls").join("out");
    let bucket = writer(&path);
    let dedup = Arc::new(BoundedDeduplicator::<MinBkMultipleColors, Flags, _>::new(
        M, K, bucket,
    ));
    std::thread::scope(|scope| {
        for shard in 0..4 {
            let dedup = &dedup;
            let slices = &slices;
            scope.spawn(move || {
                for slice in slices.iter().skip(shard).step_by(4) {
                    dedup.add(slice);
                }
            });
        }
    });
    let bucket = Arc::try_unwrap(dedup).ok().unwrap().finalize();
    let file = bucket.get_path();
    Arc::try_unwrap(bucket).ok().unwrap().finalize();

    assert_eq!(decode(&file), expected(&records));
}

// The deduplicator's own output is valid input, and feeding it back changes
// nothing.
#[test]
fn slicing_does_not_change_the_result() {
    let _guard = memory_fs();
    let records: Vec<_> = (0..1500)
        .map(|i| Record::new(&sequence(i % 200, 50), (i % 17) as ColorIndexType))
        .collect();
    let whole = run("slice-whole", M, &[encode(&records)]);

    // One slice per record is the worst case for a format that carried
    // state from one record to the next.
    let per_record: Vec<_> = records
        .iter()
        .map(|record| encode(std::slice::from_ref(record)))
        .collect();
    let split = run("slice-each", M, &per_record);

    // And a few uneven groupings, which is what threads actually produce.
    let grouped: Vec<_> = records.chunks(7).map(encode).collect();
    let chunked = run("slice-groups", M, &grouped);

    let reference = decode(&whole);
    assert_eq!(decode(&split), reference);
    assert_eq!(decode(&chunked), reference);
}

// The non-coloured instantiation has to compile and behave, with every arena
// operation monomorphized away.
#[test]
fn non_colored_deduplicates() {
    let _guard = memory_fs();
    let sequence = b"ACGTACGTACGTACGTACGTACGTACGTACGTA";
    let mut format = InputFormat::<NonColoredManager, Flags>::new(K);
    let mut bytes = Vec::new();
    for _ in 0..10 {
        format.reset();
        format.write_to(
            &CompressedReadsBucketData::new(sequence.as_slice(), 1, 3, 0),
            &mut bytes,
            &NonColoredManager,
            &(),
        );
    }
    let path = root("plain").join("out");
    let bucket = writer(&path);
    let dedup = BoundedDeduplicator::<NonColoredManager, Flags, _>::new(M, K, bucket);
    dedup.add(&bytes);
    let bucket = dedup.finalize();
    let file = bucket.get_path();
    Arc::try_unwrap(bucket).ok().unwrap().finalize();

    MemoryFs::flush_to_disk(true);
    MemoryFs::ensure_flushed(&file);
    let index = ChunkedBinaryReaderIndex::from_file(&file, RemoveFileMode::Keep);
    let mut seen = 0;
    helper_read_bucket::<
        NonColoredManager,
        WithSecondBucket,
        WithMultiplicity,
        AssemblerMinimizerPosition,
        Flags,
        NoAlignment,
    >(
        index.into_chunks(),
        None,
        |read, _| {
            assert_eq!(read.read.to_string().as_bytes(), sequence);
            assert_eq!(read.multiplicity, 10);
            assert_eq!(read.second_bucket, 3);
            seen += 1;
        },
        K,
    );
    assert_eq!(seen, 1);
}

#[test]
fn empty_input_writes_nothing() {
    let _guard = memory_fs();
    let path = run("empty", M, &[Vec::new()]);
    assert_eq!(count_records(&path), 0);
}

#[test]
#[should_panic(expected = "power of two")]
fn rejects_a_non_power_of_two_budget() {
    let _guard = memory_fs();
    let bucket = writer(&root("bad").join("out"));
    let _ = BoundedDeduplicator::<MinBkMultipleColors, Flags, _>::new(100_000, K, bucket);
}

// Silence the unused warnings for helpers only some instantiations use.
#[allow(dead_code)]
fn _assert_no_multiplicity_alias(_: NoMultiplicity) {}
