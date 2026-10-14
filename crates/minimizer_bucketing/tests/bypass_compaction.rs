//! Compaction of the two uncompacted flavours together.
//!
//! A bucket whose deduplicator bypasses writes the bucketing threads' own
//! narrow records, while one that does not writes the wide drained form. Both
//! reach the compactor for the same bucket, and it has to treat them as a
//! single uncompacted blob: same super-kmer folded together, multiplicities
//! summed, colors united.

use colors::{
    bucket_colors::expand_runs,
    colors_manager::{MinimizerBucketingSeqColorData, MinimizerBucketingSeqColorDataIterable},
    parsers::{
        SequenceIdent, SingleSequenceInfo,
        separate::{MinBkMultipleColors, MinBkSingleColor},
    },
};
use ggcat_minimizer_bucketing::{
    MinimizerBucketMode, compactor::BucketsCompactor, decode_helper::decode_sequences,
    split_buckets::SplittedBucket,
};
use io::concurrent::temp_reads::{
    creads_utils::{
        AssemblerMinimizerPosition, CompressedReadsBucketData, CompressedReadsBucketDataSerializer,
        NoAlignment, NoMultiplicity, WithMultiplicity, WithSecondBucket,
    },
    extra_data::{SequenceExtraDataCombiner, SequenceExtraDataTempBufferManagement},
};
use parallel_processor::{
    buckets::{
        BucketsCount, LockFreeBucket, MultiChunkBucket, bucket_writer::BucketItemSerializer,
        writers::lock_free_binary_writer::LockFreeBinaryWriter,
    },
    memory_data_size::MemoryDataSize,
    memory_fs::{MemoryFs, RemoveFileMode, file::internal::MemoryFileMode},
};
use parking_lot::Mutex;

const K: usize = 31;
const SEQUENCE: &[u8] = b"ACGTACGTACGTACGTACGTACGTACGTACGTA";

fn bucket() -> Mutex<MultiChunkBucket> {
    Mutex::new(MultiChunkBucket {
        index: 0,
        chunks: vec![],
        extra_bucket_data: None,
    })
}

fn single(color: u32) -> MinBkSingleColor {
    MinBkSingleColor::create(
        SingleSequenceInfo {
            static_color: color,
            sequence_ident: SequenceIdent::FASTA(b"fixture"),
        },
        &mut (),
    )
}

/// One chunk of wide records, the shape a deduplicator drains.
fn write_wide(
    root: &std::path::Path,
    name: &str,
    colors: &[u32],
    repeats: usize,
) -> std::path::PathBuf {
    let writer = LockFreeBinaryWriter::new(
        &root.join(name),
        &(
            MemoryFileMode::DiskOnly,
            LockFreeBinaryWriter::CHECKPOINT_SIZE_UNLIMITED,
        ),
        0,
        &MinimizerBucketMode::Single,
    );
    let mut serializer = CompressedReadsBucketDataSerializer::<
        MinBkMultipleColors,
        WithSecondBucket,
        WithMultiplicity,
        AssemblerMinimizerPosition,
        typenum::U2,
    >::new(K);
    let mut bytes = Vec::new();
    let mut arena = MinBkMultipleColors::new_temp_buffer();
    for color in colors.iter().copied().cycle().take(colors.len() * repeats) {
        let (extra, arena) = MinBkMultipleColors::from_single_entry(&mut arena, single(color), &());
        serializer.write_to(
            &CompressedReadsBucketData::new_plain_opt_rc(SEQUENCE, 3, 0, false, 0),
            &mut bytes,
            &extra,
            arena,
        );
    }
    writer.write_data(&bytes);
    let path = writer.get_path();
    writer.finalize();
    path
}

/// One chunk of narrow records, byte for byte what the bucketing threads hand
/// to the deduplicator and what a bypassing one forwards untouched. The codec
/// is reset per record, exactly as `DeduplicatingDispatcher` does.
fn write_narrow(
    root: &std::path::Path,
    name: &str,
    colors: &[u32],
    repeats: usize,
) -> std::path::PathBuf {
    let writer = LockFreeBinaryWriter::new(
        &root.join(name),
        &(
            MemoryFileMode::DiskOnly,
            LockFreeBinaryWriter::CHECKPOINT_SIZE_UNLIMITED,
        ),
        0,
        &MinimizerBucketMode::UncompactedNarrow,
    );
    let mut serializer = CompressedReadsBucketDataSerializer::<
        MinBkSingleColor,
        WithSecondBucket,
        NoMultiplicity,
        AssemblerMinimizerPosition,
        typenum::U2,
    >::new(K);
    let mut bytes = Vec::new();
    for color in colors.iter().copied().cycle().take(colors.len() * repeats) {
        serializer.reset();
        serializer.write_to(
            &CompressedReadsBucketData::new_plain_opt_rc(SEQUENCE, 3, 0, false, 0),
            &mut bytes,
            &single(color),
            &(),
        );
    }
    writer.write_data(&bytes);
    let path = writer.get_path();
    writer.finalize();
    path
}

/// Reads back everything the compaction produced.
fn collect(compacted: &Mutex<MultiChunkBucket>) -> (u32, std::collections::BTreeSet<u32>) {
    let paths = compacted.lock().chunks.clone();
    let buckets = SplittedBucket::generate(paths.iter(), RemoveFileMode::Keep, 1);
    let mut multiplicity = 0;
    let mut colors = std::collections::BTreeSet::new();
    for mut bucket in buckets.into_iter().flatten() {
        decode_sequences::<MinBkSingleColor, MinBkMultipleColors, typenum::U2, NoAlignment>(
            None,
            &mut MinBkMultipleColors::new_temp_buffer(),
            &mut bucket,
            K,
            |read, buffer| {
                assert_eq!(read.read.to_string().as_bytes(), SEQUENCE);
                multiplicity += read.multiplicity;
                colors.extend(expand_runs(read.extra.get_unique_color(buffer)));
            },
        );
    }
    (multiplicity, colors)
}

fn with_root(name: &str, body: impl FnOnce(&std::path::Path)) {
    let root = std::env::temp_dir().join(format!("{name}-{}", std::process::id()));
    std::fs::create_dir_all(&root).unwrap();
    MemoryFs::init(MemoryDataSize::from_bytes(32 * 1024 * 1024), 4, 1, 16, None);
    body(&root);
    MemoryFs::flush_to_disk(true);
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn wide_and_narrow_chunks_compact_as_one_blob() {
    with_root("bypass-compaction-both", |root| {
        let (wide, narrow, compacted) = (bucket(), bucket(), bucket());
        let mut compactor =
            BucketsCompactor::<MinBkSingleColor, MinBkMultipleColors, typenum::U2>::new(
                K,
                &BucketsCount::ONE,
                1,
            );

        // Disjoint color sets, so a lost source would show up as a missing
        // color rather than only as a smaller count.
        wide.lock()
            .chunks
            .push(write_wide(root, "wide-0", &[1, 2, 3], 40));
        narrow
            .lock()
            .chunks
            .push(write_narrow(root, "narrow-0", &[7, 8], 30));

        compactor.compact_buckets(&wide, Some(&narrow), &compacted, 0, root);

        let (multiplicity, colors) = collect(&compacted);
        assert_eq!(multiplicity, 3 * 40 + 2 * 30);
        assert_eq!(
            colors,
            [1, 2, 3, 7, 8]
                .into_iter()
                .collect::<std::collections::BTreeSet<_>>()
        );
        assert!(
            wide.lock().chunks.is_empty(),
            "wide chunks were not consumed"
        );
        assert!(
            narrow.lock().chunks.is_empty(),
            "narrow chunks were not consumed"
        );
    });
}

#[test]
fn narrow_chunks_alone_still_compact() {
    with_root("bypass-compaction-narrow", |root| {
        let (wide, narrow, compacted) = (bucket(), bucket(), bucket());
        let mut compactor =
            BucketsCompactor::<MinBkSingleColor, MinBkMultipleColors, typenum::U2>::new(
                K,
                &BucketsCount::ONE,
                1,
            );

        // The wide list is empty, which used to be the early-return condition.
        narrow
            .lock()
            .chunks
            .push(write_narrow(root, "narrow-only", &[4, 5, 6], 20));

        compactor.compact_buckets(&wide, Some(&narrow), &compacted, 0, root);

        let (multiplicity, colors) = collect(&compacted);
        assert_eq!(multiplicity, 3 * 20);
        assert_eq!(
            colors,
            [4, 5, 6]
                .into_iter()
                .collect::<std::collections::BTreeSet<_>>()
        );
    });
}
