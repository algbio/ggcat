use crate::final_executor::ParallelKmersMergeFinalExecutor;
use crate::map_processor::{ParallelKmersMergeMapProcessor, KMERGE_TEMP_DIR};
use crate::preprocessor::ParallelKmersMergePreprocessor;
use crate::structs::{ResultsBucket, RetType};
use assembler_minimizer_bucketing::AssemblerMinimizerBucketingExecutorFactory;
use colors::colors_manager::color_types::{
    GlobalColorsTableWriter, MinimizerBucketingSeqColorDataType,
};
use colors::colors_manager::{color_types, ColorsManager};
use config::{
    get_compression_level_info, get_memory_mode, BucketIndexType, SwapPriority,
    MINIMUM_SUBBUCKET_KMERS_COUNT, RESPLITTING_MAX_K_M_DIFFERENCE,
};
use crossbeam::queue::*;
use hashes::default::MNHFactory;
use hashes::HashFunctionFactory;
use io::structs::hash_entry::HashEntry;
use io::structs::hash_entry::{Direction, HashEntrySerializer};
use kmers_transform::processor::KmersTransformProcessor;
use kmers_transform::{KmersTransform, KmersTransformExecutorFactory};
use minimizer_bucketing::{MinimizerBucketingCommonData, MinimizerBucketingExecutorFactory};
use parallel_processor::buckets::bucket_writer::BucketItemSerializer;
use parallel_processor::buckets::concurrent::BucketsThreadDispatcher;
use parallel_processor::buckets::writers::compressed_binary_writer::CompressedBinaryWriter;
use parallel_processor::buckets::writers::lock_free_binary_writer::LockFreeBinaryWriter;
use parallel_processor::buckets::{
    LockFreeBucket, MultiChunkBucket, MultiThreadBuckets, SingleBucket,
};
use parallel_processor::execution_manager::memory_tracker::MemoryTracker;
use parallel_processor::phase_times_monitor::PHASES_TIMES_MONITOR;
use std::cmp::min;
use std::marker::PhantomData;
use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicU64;
use std::sync::Arc;
use utils::owned_drop::OwnedDrop;

mod final_executor;
mod map_processor;
mod preprocessor;
pub mod structs;

pub struct GlobalMergeData<CX: ColorsManager> {
    k: usize,
    m: usize,
    buckets_count: usize,
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

pub struct ParallelKmersMergeFactory<
    MH: HashFunctionFactory,
    CX: ColorsManager,
    const COMPUTE_SIMPLITIGS: bool,
>(PhantomData<(MH, CX)>);

impl<MH: HashFunctionFactory, CX: ColorsManager, const COMPUTE_SIMPLITIGS: bool>
    KmersTransformExecutorFactory for ParallelKmersMergeFactory<MH, CX, COMPUTE_SIMPLITIGS>
{
    type SequencesResplitterFactory = AssemblerMinimizerBucketingExecutorFactory<CX>;
    type GlobalExtraData = GlobalMergeData<CX>;
    type AssociatedExtraData = MinimizerBucketingSeqColorDataType<CX>;

    type PreprocessorType = ParallelKmersMergePreprocessor<MH, CX, COMPUTE_SIMPLITIGS>;
    type MapProcessorType = ParallelKmersMergeMapProcessor<MH, CX, COMPUTE_SIMPLITIGS>;
    type FinalExecutorType = ParallelKmersMergeFinalExecutor<MH, CX, COMPUTE_SIMPLITIGS>;

    #[allow(non_camel_case_types)]
    type FLAGS_COUNT = typenum::U2;
    const HAS_COLORS: bool = CX::COLORS_ENABLED;

    fn new_resplitter(
        global_data: &Arc<Self::GlobalExtraData>,
    ) -> <Self::SequencesResplitterFactory as MinimizerBucketingExecutorFactory>::ExecutorType {
        AssemblerMinimizerBucketingExecutorFactory::new(&global_data.global_resplit_data)
    }

    fn new_preprocessor(_global_data: &Arc<Self::GlobalExtraData>) -> Self::PreprocessorType {
        ParallelKmersMergePreprocessor::new()
    }

    fn new_map_processor(
        _global_data: &Arc<Self::GlobalExtraData>,
        mem_tracker: MemoryTracker<KmersTransformProcessor<Self>>,
    ) -> Self::MapProcessorType {
        ParallelKmersMergeMapProcessor::new(mem_tracker)
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
        do_merge: bool,
        direction: Direction,
        buckets_count_bits: usize,
    ) {
        if do_merge {
            hashes_tmp.add_element(
                MH::get_bucket(0, buckets_count_bits, hash),
                &(),
                &HashEntry::new(hash, bucket, entry, direction),
            );
        }
    }
}

pub fn kmers_merge<MH: HashFunctionFactory, CX: ColorsManager, P: AsRef<Path> + Sync>(
    file_inputs: Vec<MultiChunkBucket>,
    buckets_counters_path: PathBuf,
    colors_global_table: Arc<GlobalColorsTableWriter<CX>>,
    buckets_count: usize,
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

    let output_results_buckets = ArrayQueue::new(reads_buckets.count());
    for (index, bucket) in reads_buckets.into_buckets().enumerate() {
        let bucket_read = ResultsBucket::<color_types::PartialUnitigsColorStructure<CX>> {
            read_index: 0,
            reads_writer: OwnedDrop::new(bucket),
            temp_buffer: Vec::with_capacity(256),
            bucket_index: index as BucketIndexType,
            _phantom: PhantomData,
            serializer: BucketItemSerializer::new(),
        };
        sequences.push(SingleBucket {
            index,
            path: bucket_read.reads_writer.get_path(),
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
            1,
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
            buckets_counters_path,
            buckets_count,
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
            buckets_counters_path,
            buckets_count,
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
    use config::{FLUSH_QUEUE_FACTOR, KEEP_FILES, PREFER_MEMORY};
    use io::generate_bucket_names;
    use parallel_processor::memory_data_size::MemoryDataSize;
    use parallel_processor::memory_fs::MemoryFs;
    use rayon::ThreadPoolBuilder;
    use std::cmp::max;
    use std::path::Path;
    use std::sync::atomic::Ordering;
    use std::sync::Arc;

    #[ignore]
    #[test]
    fn test_single_bucket_processing() {
        const TEMP_DIR: &str = "../../../../temp-gut-test-new";

        let buckets_count = 1024;

        let buckets =
            generate_bucket_names(Path::new(TEMP_DIR).join("bucket"), buckets_count, None);

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
            <<NonColoredManager as ColorsManager>::ColorsMergeManagerType as ColorsMergeManager>::create_colors_table("", &[]),
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
            counters,
            global_colors_table.clone(),
            buckets_count,
            min_multiplicity,
            Path::new(TEMP_DIR),
            k,
            m,
            threads_count,
        );
    }
}
