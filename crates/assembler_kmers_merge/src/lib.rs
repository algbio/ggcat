use crate::final_executor::ParallelKmersMergeFinalExecutor;
use crate::map_processor::{KMERGE_TEMP_DIR, ParallelKmersMergeMapProcessor};
use crate::structs::{ResultsBucket, RetType};
use assembler_minimizer_bucketing::AssemblerMinimizerBucketingExecutorFactory;
use assembler_minimizer_bucketing::rewrite_bucket::RewriteBucketComputeAssembler;
use colors::colors_manager::color_types::{
    GlobalColorsTableWriter, MinimizerBucketingMultipleSeqColorDataType,
    MinimizerBucketingSeqColorDataType,
};
use colors::colors_manager::{ColorsManager, color_types};
use config::{
    BucketIndexType, MINIMUM_SUBBUCKET_KMERS_COUNT, RESPLITTING_MAX_K_M_DIFFERENCE, SwapPriority,
    get_compression_level_info, get_memory_mode,
};
use crossbeam::queue::*;
use hashes::HashFunctionFactory;
use hashes::default::MNHFactory;
use io::structs::hash_entry::HashEntry;
use io::structs::hash_entry::{Direction, HashEntrySerializer};
use kmers_transform::{
    KmersTransform, KmersTransformExecutorFactory, KmersTransformGlobalExtraData,
};
use minimizer_bucketing::{MinimizerBucketingCommonData, MinimizerBucketingExecutorFactory};
use parallel_processor::buckets::bucket_writer::BucketItemSerializer;
use parallel_processor::buckets::concurrent::BucketsThreadDispatcher;
use parallel_processor::buckets::writers::compressed_binary_writer::CompressedBinaryWriter;
use parallel_processor::buckets::writers::lock_free_binary_writer::LockFreeBinaryWriter;
use parallel_processor::buckets::{
    BucketsCount, LockFreeBucket, MultiChunkBucket, MultiThreadBuckets, SingleBucket,
};
use parallel_processor::phase_times_monitor::PHASES_TIMES_MONITOR;
use std::cmp::min;
use std::marker::PhantomData;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use unitigs_extender::GlobalExtenderParams;
use utils::owned_drop::OwnedDrop;

mod final_executor;
mod map_processor;
pub mod structs;
pub mod unitigs_extender;

pub struct GlobalMergeData<CX: ColorsManager> {
    k: usize,
    m: usize,
    buckets_count: BucketsCount,
    min_multiplicity: usize,
    colors_global_table: Arc<GlobalColorsTableWriter<CX>>,
    output_results_buckets:
        ArrayQueue<ResultsBucket<color_types::PartialUnitigsColorStructure<CX>>>,
    hashes_buckets: Arc<MultiThreadBuckets<LockFreeBinaryWriter>>,
    global_resplit_data: Arc<MinimizerBucketingCommonData<()>>,
    sequences_size_total: AtomicU64,
    hasnmap_kmers_total: AtomicU64,
    kmer_batches_count: AtomicU64,
}

impl<CX: ColorsManager> KmersTransformGlobalExtraData for GlobalMergeData<CX> {
    #[inline(always)]
    fn get_k(&self) -> usize {
        self.k
    }
    #[inline(always)]
    fn get_m(&self) -> usize {
        self.m
    }

    fn get_m_resplit(&self) -> usize {
        self.global_resplit_data.m
    }
}

pub struct ParallelKmersMergeFactory<
    MH: HashFunctionFactory,
    CX: ColorsManager,
    const COMPUTE_SIMPLITIGS: bool,
>(PhantomData<(MH, CX)>);

impl<MH: HashFunctionFactory, CX: ColorsManager, const COMPUTE_SIMPLITIGS: bool>
    KmersTransformExecutorFactory for ParallelKmersMergeFactory<MH, CX, COMPUTE_SIMPLITIGS>
{
    type KmersTransformPacketInitData = GlobalExtenderParams;
    type SequencesResplitterFactory =
        AssemblerMinimizerBucketingExecutorFactory<Self::AssociatedExtraDataWithMultiplicity>;
    type GlobalExtraData = GlobalMergeData<CX>;
    type AssociatedExtraData = MinimizerBucketingSeqColorDataType<CX>;
    type AssociatedExtraDataWithMultiplicity = MinimizerBucketingMultipleSeqColorDataType<CX>;

    type PreprocessorType = RewriteBucketComputeAssembler;
    type MapProcessorType = ParallelKmersMergeMapProcessor<MH, CX, COMPUTE_SIMPLITIGS>;
    type FinalExecutorType = ParallelKmersMergeFinalExecutor<MH, CX, COMPUTE_SIMPLITIGS>;

    type FlagsCount = typenum::U2;
    const HAS_COLORS: bool = CX::COLORS_ENABLED;

    fn get_packets_init_data(
        global_data: &Arc<Self::GlobalExtraData>,
    ) -> Self::KmersTransformPacketInitData {
        GlobalExtenderParams {
            k: global_data.k,
            m: global_data.m,
            min_multiplicity: global_data.min_multiplicity,
        }
    }

    fn new_resplitter(
        global_data: &Arc<Self::GlobalExtraData>,
        duplicates_bucket: u16,
    ) -> <Self::SequencesResplitterFactory as MinimizerBucketingExecutorFactory>::ExecutorType {
        AssemblerMinimizerBucketingExecutorFactory::new_with_duplicates(
            &global_data.global_resplit_data,
            duplicates_bucket,
        )
    }

    fn new_map_processor(_global_data: &Arc<Self::GlobalExtraData>) -> Self::MapProcessorType {
        ParallelKmersMergeMapProcessor::new()
    }

    fn new_final_executor(global_data: &Arc<Self::GlobalExtraData>) -> Self::FinalExecutorType {
        Self::FinalExecutorType::new(global_data)
    }
}

impl<MH: HashFunctionFactory, CX: ColorsManager, const COMPUTE_SIMPLITIGS: bool>
    ParallelKmersMergeFinalExecutor<MH, CX, COMPUTE_SIMPLITIGS>
{
    #[inline(always)]
    fn write_hashes(
        hashes_tmp: &mut BucketsThreadDispatcher<
            LockFreeBinaryWriter,
            HashEntrySerializer<MH::HashTypeUnextendable>,
        >,
        hash: MH::HashTypeUnextendable,
        bucket: BucketIndexType,
        entry: u64,
        direction: Direction,
        buckets_count_bits: usize,
    ) {
        hashes_tmp.add_element(
            MH::get_bucket(0, buckets_count_bits, hash),
            &(),
            &HashEntry::new(hash, bucket, entry, direction),
        );
    }
}

pub fn kmers_merge<MH: HashFunctionFactory, CX: ColorsManager, P: AsRef<Path> + Sync>(
    file_inputs: Vec<MultiChunkBucket>,
    colors_global_table: Arc<GlobalColorsTableWriter<CX>>,
    buckets_count: BucketsCount,
    second_buckets_count: BucketsCount,
    min_multiplicity: usize,
    out_directory: P,
    k: usize,
    m: usize,
    compute_simplitigs: bool,
    threads_count: usize,
) -> RetType {
    PHASES_TIMES_MONITOR
        .write()
        .start_phase("phase: kmers merge".to_string());

    MNHFactory::initialize(k);
    MH::initialize(k);
    *KMERGE_TEMP_DIR.write() = Some(out_directory.as_ref().to_path_buf());

    let hashes_buckets = Arc::new(MultiThreadBuckets::<LockFreeBinaryWriter>::new(
        buckets_count,
        out_directory.as_ref().join("hashes"),
        None,
        &(
            get_memory_mode(SwapPriority::HashBuckets),
            LockFreeBinaryWriter::CHECKPOINT_SIZE_UNLIMITED,
        ),
        &(),
    ));

    let mut sequences = Vec::new();

    let reads_buckets = MultiThreadBuckets::<CompressedBinaryWriter>::new(
        buckets_count,
        out_directory.as_ref().join("result"),
        None,
        &(
            get_memory_mode(SwapPriority::ResultBuckets),
            CompressedBinaryWriter::CHECKPOINT_SIZE_UNLIMITED,
            get_compression_level_info(),
        ),
        &(),
    );

    let output_results_buckets =
        ArrayQueue::new(reads_buckets.get_buckets_count().total_buckets_count);
    for (index, bucket) in reads_buckets.into_buckets().enumerate() {
        let bucket_read = ResultsBucket::<color_types::PartialUnitigsColorStructure<CX>> {
            read_index: 0,
            reads_writer: OwnedDrop::new(bucket),
            temp_buffer: Vec::with_capacity(256),
            bucket_index: index as BucketIndexType,
            _phantom: PhantomData,
            serializer: BucketItemSerializer::new(k),
        };
        sequences.push(SingleBucket {
            index,
            path: bucket_read.reads_writer.get_path(),
            extra_bucket_data: None,
        });
        let res = output_results_buckets.push(bucket_read).is_ok();
        assert!(res);
    }

    let global_data = Arc::new(GlobalMergeData::<CX> {
        k,
        m,
        buckets_count,
        min_multiplicity,
        colors_global_table,
        output_results_buckets,
        hashes_buckets: hashes_buckets.clone(),
        global_resplit_data: Arc::new(MinimizerBucketingCommonData::new(
            k,
            if k > RESPLITTING_MAX_K_M_DIFFERENCE + 1 {
                k - RESPLITTING_MAX_K_M_DIFFERENCE
            } else {
                min(m, 2)
            }, // m
            buckets_count,
            k,
            BucketsCount::ONE,
            (),
        )),
        sequences_size_total: AtomicU64::new(0),
        hasnmap_kmers_total: AtomicU64::new(0),
        kmer_batches_count: AtomicU64::new(0),
    });

    if compute_simplitigs {
        KmersTransform::<ParallelKmersMergeFactory<MH, CX, true>>::new(
            file_inputs,
            out_directory.as_ref(),
            buckets_count,
            second_buckets_count,
            global_data,
            threads_count,
            k,
            MINIMUM_SUBBUCKET_KMERS_COUNT as u64,
        )
        .parallel_kmers_transform();
    } else {
        KmersTransform::<ParallelKmersMergeFactory<MH, CX, false>>::new(
            file_inputs,
            out_directory.as_ref(),
            buckets_count,
            second_buckets_count,
            global_data,
            threads_count,
            k,
            MINIMUM_SUBBUCKET_KMERS_COUNT as u64,
        )
        .parallel_kmers_transform();
    }

    RetType {
        sequences,
        hashes: hashes_buckets.finalize_single(),
    }
}

#[cfg(test)]
mod tests {
    use colors::colors_manager::{ColorsManager, ColorsMergeManager};
    use colors::non_colored::NonColoredManager;
    use config::{DEFAULT_PREFETCH_AMOUNT, FLUSH_QUEUE_FACTOR, KEEP_FILES, PREFER_MEMORY};
    use hashes::cn_seqhash::u128::CanonicalSeqHashFactory;
    use io::concurrent::temp_reads::creads_utils::{
        AssemblerMinimizerPosition, ReadsCheckpointData,
    };
    use io::generate_bucket_names;
    use kmers_transform::KmersTransformExecutorFactory;
    use minimizer_bucketing::{MinimizerBucketMode, helper_read_bucket_with_type};
    use parallel_processor::buckets::readers::binary_reader::{
        BinaryReaderChunk, ChunkedBinaryReaderIndex,
    };
    use parallel_processor::buckets::{BucketsCount, ExtraBuckets, SingleBucket};
    use parallel_processor::memory_data_size::MemoryDataSize;
    use parallel_processor::memory_fs::{MemoryFs, RemoveFileMode};
    use rayon::ThreadPoolBuilder;
    use std::cmp::max;
    use std::path::Path;
    use std::sync::Arc;
    use std::sync::atomic::Ordering;

    use crate::ParallelKmersMergeFactory;

    #[test]
    #[ignore]
    fn test_deserialization_mismatch() {
        let mut bucket_chunks = vec![];

        type F = ParallelKmersMergeFactory<CanonicalSeqHashFactory, NonColoredManager, false>;

        let paths = [
            "../../.temp_files/build_graph_8a973650-c549-4eb9-b9b1-ccac626dcfbe/comp-single-15123.dat.0",
            "../../.temp_files/build_graph_8a973650-c549-4eb9-b9b1-ccac626dcfbe/comp-single-16937.dat.0",
            "../../.temp_files/build_graph_8a973650-c549-4eb9-b9b1-ccac626dcfbe/comp-single-18709.dat.0",
            "../../.temp_files/build_graph_8a973650-c549-4eb9-b9b1-ccac626dcfbe/comp-single-6164.dat.0",
            "../../.temp_files/build_graph_8a973650-c549-4eb9-b9b1-ccac626dcfbe/comp-single-2799.dat.0",
            "../../.temp_files/build_graph_8a973650-c549-4eb9-b9b1-ccac626dcfbe/comp-mult-13259.dat.0",
            "../../.temp_files/build_graph_8a973650-c549-4eb9-b9b1-ccac626dcfbe/comp-single-20523.dat.0",
            "../../.temp_files/build_graph_8a973650-c549-4eb9-b9b1-ccac626dcfbe/comp-mult-21902.dat.0",
            "../../.temp_files/build_graph_8a973650-c549-4eb9-b9b1-ccac626dcfbe/comp-single-21902.dat.0",
        ];

        for file in &paths {
            let file_index = ChunkedBinaryReaderIndex::from_file(
                file,
                RemoveFileMode::Remove {
                    remove_fs: false, // !KEEP_FILES.load(Ordering::Relaxed),
                },
                DEFAULT_PREFETCH_AMOUNT,
            );

            let data_format = file_index.get_data_format_info::<MinimizerBucketMode>();

            bucket_chunks.extend(
                file_index
                    .into_chunks()
                    .into_iter()
                    // The first element is empty and has no extra data, skip it
                    .filter_map(|c| {
                        Some((
                            (
                                c.get_extra_data::<ReadsCheckpointData>()?,
                                data_format,
                                c.get_unique_file_id(),
                            ),
                            c,
                        ))
                    }),
            );
        }

        bucket_chunks.sort_by_key(|c| c.0.0.target_subbucket);

        let mut sub_buckets: Vec<(
            Vec<(ReadsCheckpointData, MinimizerBucketMode, usize)>,
            Vec<BinaryReaderChunk>,
        )> = vec![];
        for chunk in bucket_chunks {
            if let Some(last) = sub_buckets.last_mut() {
                if last.0[last.0.len() - 1].0.target_subbucket == chunk.0.0.target_subbucket {
                    last.0.push(chunk.0);
                    last.1.push(chunk.1);
                } else {
                    sub_buckets.push((vec![chunk.0], vec![chunk.1]));
                }
            } else {
                sub_buckets.push((vec![chunk.0], vec![chunk.1]));
            }
        }

        for (mut info, chunks) in sub_buckets {
            let mut single_chunks = vec![];
            let mut multi_chunks = vec![];

            // if info[0].0.target_subbucket != 54 {
            //     continue;
            // }

            // println!("Infos: {:?}", info[0].0.sequences_count);

            for (chunk, info) in chunks.into_iter().zip(info.iter()) {
                match info.1 {
                    MinimizerBucketMode::Single => unreachable!(),
                    MinimizerBucketMode::SingleGrouped => single_chunks.push(chunk),
                    MinimizerBucketMode::Compacted => multi_chunks.push(chunk),
                }
            }

            let mut tot_count = 0;

            helper_read_bucket_with_type::<
                <F as KmersTransformExecutorFactory>::AssociatedExtraData,
                <F as KmersTransformExecutorFactory>::AssociatedExtraDataWithMultiplicity,
                AssemblerMinimizerPosition,
                <F as KmersTransformExecutorFactory>::FlagsCount,
            >(
                single_chunks,
                None, // Some(reader_thread.clone()),
                MinimizerBucketMode::SingleGrouped,
                |_, _| {
                    tot_count += 1;
                },
                27,
            );

            helper_read_bucket_with_type::<
                <F as KmersTransformExecutorFactory>::AssociatedExtraData,
                <F as KmersTransformExecutorFactory>::AssociatedExtraDataWithMultiplicity,
                AssemblerMinimizerPosition,
                <F as KmersTransformExecutorFactory>::FlagsCount,
            >(
                multi_chunks,
                None, // Some(reader_thread.clone()),
                MinimizerBucketMode::Compacted,
                |_, _| {
                    tot_count += 1;
                },
                27,
            );

            info.dedup_by(|a, b| a == b);

            let sum_tot_count = info.iter().map(|i| i.0.sequences_count).sum::<usize>();

            println!(
                "Found {} buckets for sub-bucket {}/{} => total seq count: {} vs {} => correct: {}",
                info.len(),
                0,
                // index,
                info[0].0.target_subbucket,
                sum_tot_count,
                tot_count,
                sum_tot_count == tot_count
            );
        }
    }

    #[ignore]
    #[test]
    fn test_single_bucket_processing() {
        const TEMP_DIR: &str = "../../../../temp-gut-test-new";

        let buckets_count = BucketsCount::new(10, ExtraBuckets::None);

        let buckets =
            generate_bucket_names(Path::new(TEMP_DIR).join("bucket"), buckets_count, None)
                .into_iter()
                .map(SingleBucket::to_multi_chunk)
                .collect();

        // let mut buckets = vec![buckets[322]];

        // buckets.remove(1009);
        // buckets.remove(886);
        // buckets.remove(829);
        // buckets.remove(806);
        // buckets.remove(727);
        // buckets.remove(440);
        // buckets.remove(113);
        // buckets.remove(33);

        let counters = Path::new(TEMP_DIR).join("buckets-counters.dat");

        let global_colors_table = Arc::new(
            <<NonColoredManager as ColorsManager>::ColorsMergeManagerType as ColorsMergeManager>::create_colors_table("", &[]).unwrap(),
        );

        let k = 63;
        let m = 12;
        let threads_count = 16;
        let min_multiplicity = 1;

        // Increase the maximum allowed number of open files
        let _ = fdlimit::raise_fd_limit();

        KEEP_FILES.store(true, Ordering::Relaxed);

        PREFER_MEMORY.store(false, Ordering::Relaxed);

        ThreadPoolBuilder::new()
            .num_threads(threads_count)
            .thread_name(|i| format!("rayon-thread-{}", i))
            .build_global()
            .unwrap();

        // enable_counters_logging(
        //     out_file.with_extension("stats.log"),
        //     Duration::from_millis(1000),
        //     |val| {
        //         val["phase"] = PHASES_TIMES_MONITOR.read().get_phase_desc().into();
        //     },
        // );

        MemoryFs::init(
            MemoryDataSize::from_bytes(
                (8.0 * (MemoryDataSize::OCTET_GIBIOCTET_FACTOR as f64)) as usize,
            ),
            FLUSH_QUEUE_FACTOR * threads_count,
            max(1, threads_count / 4),
            32768,
        );

        ggcat_logging::info!("Using m: {} with k: {}", m, k);

        // #[cfg(feature = "mem-analysis")]
        // debug_print_allocations("/tmp/allocations", Duration::from_secs(5));
        crate::kmers_merge::<hashes::cn_seqhash::u128::CanonicalSeqHashFactory, NonColoredManager, _>(
            buckets,
            global_colors_table.clone(),
            buckets_count,
            min_multiplicity,
            Path::new(TEMP_DIR),
            k,
            m,
            false,
            threads_count,
        );
    }
}
