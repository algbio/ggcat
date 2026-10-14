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
        let buckets = SplittedBucket::generate(paths.iter(), RemoveFileMode::Keep, 1);
        let mut records = 0;
        for mut bucket in buckets.into_iter().flatten() {
            decode_sequences::<MinBkSingleColor, MinBkMultipleColors, typenum::U2, NoAlignment>(
                None,
                &mut MinBkMultipleColors::new_temp_buffer(),
                &mut bucket,
                31,
                |read, buffer| {
                    assert_eq!(read.read.to_string().as_bytes(), sequence);
                    assert_eq!(read.multiplicity, total);
                    let expected: Vec<_> = expected.iter().copied().collect();
                    // The set is carried as runs now, so it is expanded here
                    // rather than in the pipeline.
                    let decoded: Vec<_> =
                        expand_runs(read.extra.get_unique_color(buffer)).collect();
                    assert_eq!(decoded, expected);
                    records += 1;
                },
            );
        }
        assert_eq!(records, 1);
    }
    MemoryFs::flush_to_disk(true);
    std::fs::remove_dir_all(root).unwrap();
}
