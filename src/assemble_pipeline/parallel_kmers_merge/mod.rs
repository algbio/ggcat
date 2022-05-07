use crate::assemble_pipeline::assembler_minimizer_bucketing::AssemblerMinimizerBucketingExecutorFactory;
use crate::assemble_pipeline::parallel_kmers_merge::final_executor::ParallelKmersMergeFinalExecutor;
use crate::assemble_pipeline::parallel_kmers_merge::map_processor::{
    ParallelKmersMergeMapPacket, ParallelKmersMergeMapProcessor,
};
use crate::assemble_pipeline::parallel_kmers_merge::preprocessor::ParallelKmersMergePreprocessor;
use crate::assemble_pipeline::AssemblePipeline;
use crate::colors::colors_manager::ColorsMergeManager;
use crate::colors::colors_manager::{color_types, ColorsManager};
use crate::config::{
    BucketIndexType, SwapPriority, DEFAULT_LZ4_COMPRESSION_LEVEL, DEFAULT_PER_CPU_BUFFER_SIZE,
    RESPLITTING_MAX_K_M_DIFFERENCE,
};
use crate::hashes::{ExtendableHashTraitType, HashFunction, MinimizerHashFunctionFactory};
use crate::hashes::{HashFunctionFactory, HashableSequence};
use crate::io::structs::hash_entry::Direction;
use crate::io::structs::hash_entry::HashEntry;
use crate::pipeline_common::kmers_transform::{
    KmersTransform, KmersTransformExecutorFactory, KmersTransformMapProcessor,
    KmersTransformPreprocessor,
};
use crate::pipeline_common::minimizer_bucketing::{
    MinimizerBucketingCommonData, MinimizerBucketingExecutorFactory,
};
use crate::utils::compressed_read::CompressedReadIndipendent;
use crate::utils::owned_drop::OwnedDrop;
use crate::utils::{get_memory_mode, Utils};
use crate::CompressedRead;
use core::slice::from_raw_parts;
use crossbeam::queue::*;
use hashbrown::HashMap;
use parallel_processor::buckets::concurrent::{BucketsThreadBuffer, BucketsThreadDispatcher};
use parallel_processor::buckets::writers::compressed_binary_writer::CompressedBinaryWriter;
use parallel_processor::buckets::writers::lock_free_binary_writer::LockFreeBinaryWriter;
use parallel_processor::buckets::{LockFreeBucket, MultiThreadBuckets};
use parallel_processor::counter_stats::counter::{AtomicCounter, AvgMode, MaxMode};
use parallel_processor::counter_stats::{declare_avg_counter_i64, declare_counter_i64};
use parallel_processor::execution_manager::packet::Packet;
use parallel_processor::mem_tracker::tracked_vec::TrackedVec;
#[cfg(feature = "mem-analysis")]
use parallel_processor::mem_tracker::MemoryInfo;
use parallel_processor::phase_times_monitor::PHASES_TIMES_MONITOR;
use std::cmp::min;
use std::marker::PhantomData;
use std::ops::DerefMut;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use structs::*;

pub const READ_FLAG_INCL_BEGIN: u8 = 1 << 0;
pub const READ_FLAG_INCL_END: u8 = 1 << 1;

mod final_executor;
mod map_processor;
mod preprocessor;
pub mod structs;

pub struct GlobalMergeData<MH: HashFunctionFactory, CX: ColorsManager> {
    k: usize,
    m: usize,
    buckets_count: usize,
    min_multiplicity: usize,
    colors_global_table: Arc<CX::GlobalColorsTable>,
    output_results_buckets:
        ArrayQueue<ResultsBucket<color_types::PartialUnitigsColorStructure<MH, CX>>>,
    hashes_buckets: Arc<MultiThreadBuckets<LockFreeBinaryWriter>>,
    global_resplit_data: Arc<MinimizerBucketingCommonData<()>>,
}

struct ParallelKmersMergeFactory<
    H: MinimizerHashFunctionFactory,
    MH: HashFunctionFactory,
    CX: ColorsManager,
>(PhantomData<(H, MH, CX)>);

impl<H: MinimizerHashFunctionFactory, MH: HashFunctionFactory, CX: ColorsManager>
    KmersTransformExecutorFactory for ParallelKmersMergeFactory<H, MH, CX>
{
    type SequencesResplitterFactory = AssemblerMinimizerBucketingExecutorFactory<H, CX>;
    type GlobalExtraData = GlobalMergeData<MH, CX>;
    type AssociatedExtraData = CX::MinimizerBucketingSeqColorDataType;

    type PreprocessorType = ParallelKmersMergePreprocessor<H, MH, CX>;
    type MapProcessorType = ParallelKmersMergeMapProcessor<H, MH, CX>;
    type FinalExecutorType = ParallelKmersMergeFinalExecutor<H, MH, CX>;

    #[allow(non_camel_case_types)]
    type FLAGS_COUNT = typenum::U2;

    fn new_resplitter(
        global_data: &Arc<Self::GlobalExtraData>,
    ) -> <Self::SequencesResplitterFactory as MinimizerBucketingExecutorFactory>::ExecutorType {
        AssemblerMinimizerBucketingExecutorFactory::new(&global_data.global_resplit_data)
    }

    fn new_preprocessor(_global_data: &Arc<Self::GlobalExtraData>) -> Self::PreprocessorType {
        ParallelKmersMergePreprocessor::new()
    }

    fn new_map_processor(_global_data: &Arc<Self::GlobalExtraData>) -> Self::MapProcessorType {
        ParallelKmersMergeMapProcessor::new()
    }

    fn new_final_executor(global_data: &Arc<Self::GlobalExtraData>) -> Self::FinalExecutorType {
        Self::FinalExecutorType::new(global_data)
    }
}

impl<H: MinimizerHashFunctionFactory, MH: HashFunctionFactory, CX: ColorsManager>
    ParallelKmersMergeFinalExecutor<H, MH, CX>
{
    #[inline(always)]
    fn write_hashes(
        hashes_tmp: &mut BucketsThreadDispatcher<LockFreeBinaryWriter>,
        hash: MH::HashTypeUnextendable,
        bucket: BucketIndexType,
        entry: u64,
        do_merge: bool,
        direction: Direction,
        buckets_count_mask: BucketIndexType,
    ) {
        if do_merge {
            hashes_tmp.add_element(
                MH::get_first_bucket(hash) & buckets_count_mask,
                &(),
                &HashEntry {
                    hash,
                    bucket,
                    entry,
                    direction,
                },
            );
        }
    }
}

impl AssemblePipeline {
    pub fn parallel_kmers_merge<
        H: MinimizerHashFunctionFactory,
        MH: HashFunctionFactory,
        CX: ColorsManager,
        P: AsRef<Path> + std::marker::Sync,
    >(
        file_inputs: Vec<PathBuf>,
        buckets_counters_path: PathBuf,
        colors_global_table: Arc<CX::GlobalColorsTable>,
        buckets_count: usize,
        min_multiplicity: usize,
        out_directory: P,
        k: usize,
        m: usize,
        threads_count: usize,
    ) -> RetType {
        PHASES_TIMES_MONITOR
            .write()
            .start_phase("phase: kmers merge".to_string());

        let mut hashes_buckets = Arc::new(MultiThreadBuckets::<LockFreeBinaryWriter>::new(
            buckets_count,
            out_directory.as_ref().join("hashes"),
            &(
                get_memory_mode(SwapPriority::HashBuckets),
                LockFreeBinaryWriter::CHECKPOINT_SIZE_UNLIMITED,
            ),
        ));

        let mut sequences = Vec::new();

        let reads_buckets = MultiThreadBuckets::<CompressedBinaryWriter>::new(
            buckets_count,
            out_directory.as_ref().join("result"),
            &(
                get_memory_mode(SwapPriority::ResultBuckets),
                CompressedBinaryWriter::CHECKPOINT_SIZE_UNLIMITED,
                DEFAULT_LZ4_COMPRESSION_LEVEL,
            ),
        );

        let output_results_buckets = ArrayQueue::new(reads_buckets.count());
        for (index, bucket) in reads_buckets.into_buckets().enumerate() {
            let bucket_read = ResultsBucket::<color_types::PartialUnitigsColorStructure<MH, CX>> {
                read_index: 0,
                reads_writer: OwnedDrop::new(bucket),
                temp_buffer: Vec::with_capacity(256),
                bucket_index: index as BucketIndexType,
                _phantom: PhantomData,
            };
            sequences.push(bucket_read.reads_writer.get_path());
            let res = output_results_buckets.push(bucket_read).is_ok();
            assert!(res);
        }

        let global_data = Arc::new(GlobalMergeData::<MH, CX> {
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
                1,
                (),
            )),
        });

        KmersTransform::<ParallelKmersMergeFactory<H, MH, CX>>::new(
            file_inputs,
            out_directory.as_ref(),
            buckets_counters_path,
            buckets_count,
            global_data,
            threads_count,
        )
        .parallel_kmers_transform();

        RetType {
            sequences,
            hashes: hashes_buckets.finalize(),
        }
    }
}
