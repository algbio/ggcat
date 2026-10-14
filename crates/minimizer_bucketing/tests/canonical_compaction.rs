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
        NoAlignment, NoMultiplicity, WithSecondBucket,
    },
    extra_data::SequenceExtraDataTempBufferManagement,
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
use std::path::PathBuf;

#[test]
fn repeated_compaction_keeps_canonical_union_and_multiplicity() {
    let root = std::env::temp_dir().join(format!("canonical-compaction-{}", std::process::id()));
    std::fs::create_dir_all(&root).unwrap();
    MemoryFs::init(MemoryDataSize::from_bytes(32 * 1024 * 1024), 4, 1, 16, None);
    let raw = Mutex::new(MultiChunkBucket {
        index: 0,
        chunks: vec![],
        extra_bucket_data: None,
    });
    let compacted = Mutex::new(MultiChunkBucket {
        index: 0,
        chunks: vec![],
        extra_bucket_data: None,
    });
    let mut compactor = BucketsCompactor::<MinBkSingleColor, MinBkMultipleColors, typenum::U2>::new(
        31,
        &BucketsCount::ONE,
        1,
    );
    let sequence = b"ACGTACGTACGTACGTACGTACGTACGTACGTA";
    let mut expected = std::collections::BTreeSet::new();
    let mut total = 0;
    for (round, colors) in [vec![1, 2, 3], vec![4, 5, 6], vec![2, 3, 4, 5]]
        .into_iter()
        .enumerate()
    {
        let writer = LockFreeBinaryWriter::new(
            &root.join(format!("raw-{round}")),
            &(
                MemoryFileMode::DiskOnly,
                LockFreeBinaryWriter::CHECKPOINT_SIZE_UNLIMITED,
            ),
            0,
            &MinimizerBucketMode::Single,
        );
        let mut serializer = CompressedReadsBucketDataSerializer::<
            MinBkSingleColor,
            WithSecondBucket,
            NoMultiplicity,
            AssemblerMinimizerPosition,
            typenum::U2,
        >::new(31);
        let mut bytes = Vec::new();
        // Enough raw bytes to select the earlier compacted chunk for merging.
        for color in colors.iter().copied().cycle().take(colors.len() * 100) {
            let extra = MinBkSingleColor::create(
                SingleSequenceInfo {
                    static_color: color,
                    sequence_ident: SequenceIdent::FASTA(b"fixture"),
                },
                &mut (),
            );
            serializer.write_to(
                &CompressedReadsBucketData::new_plain_opt_rc(sequence, 3, 0, false, 0),
                &mut bytes,
                &extra,
                &(),
            );
            total += 1;
        }
        expected.extend(colors);
        writer.write_data(&bytes);
        let path = writer.get_path();
        writer.finalize();
        raw.lock().chunks.push(path);
        compactor.compact_buckets(&raw, &compacted, 0, &root);
        let paths = compacted.lock().chunks.clone();
        // Which kind ran is readable from the names, and the rule makes the
        // three rounds go light, extended, light: the first has nothing
        // compacted to weigh against, the second finds a light chunk and no
        // extended one so it promotes it, and the third is back to nothing
        // light because the second consumed it.
        let light = |p: &PathBuf| p.file_name().unwrap().to_str().unwrap().contains("light-");
        match round {
            0 => assert!(paths.iter().all(light), "{paths:?}"),
            1 => assert!(!paths.iter().any(light), "{paths:?}"),
            _ => {
                assert!(paths.iter().any(light), "{paths:?}");
                assert!(!paths.iter().all(light), "{paths:?}");
            }
        }
        let buckets = SplittedBucket::generate(paths.iter(), RemoveFileMode::Keep, 1);
        // A light compaction leaves the earlier chunks alone, so the
        // occurrences of one super-kmer may be spread over several records
        // until an extended compaction folds them back together. What has to
        // hold at every round is the union: every record is the same
        // super-kmer, the multiplicities sum to everything written so far, and
        // the colors together are exactly the colors fed in.
        let mut records = 0;
        let mut seen_multiplicity = 0;
        let mut seen_colors = std::collections::BTreeSet::new();
        for mut bucket in buckets.into_iter().flatten() {
            decode_sequences::<MinBkSingleColor, MinBkMultipleColors, typenum::U2, NoAlignment>(
                None,
                &mut MinBkMultipleColors::new_temp_buffer(),
                &mut bucket,
                31,
                |read, buffer| {
                    assert_eq!(read.read.to_string().as_bytes(), sequence);
                    seen_multiplicity += read.multiplicity;
                    // The set is carried as runs now, so it is expanded here
                    // rather than in the pipeline.
                    seen_colors.extend(expand_runs(read.extra.get_unique_color(buffer)));
                    records += 1;
                },
            );
        }
        assert_eq!(seen_multiplicity, total);
        assert_eq!(seen_colors, expected);
        assert!(records >= 1);
    }
    MemoryFs::flush_to_disk(true);
    std::fs::remove_dir_all(root).unwrap();
}
