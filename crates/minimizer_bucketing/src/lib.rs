pub mod compactor;
pub mod counters_analyzer;
mod queue_data;
mod reader;
pub mod resplit_bucket;
mod sequences_splitter;

use crate::compactor::MinimizerBucketingCompactor;
use crate::counters_analyzer::CountersAnalyzer;
use crate::queue_data::MinimizerBucketingQueueData;
use crate::reader::MinimizerBucketingFilesReader;
use crate::sequences_splitter::SequencesSplitter;
use bincode::{Decode, Encode};
use compactor::CompactorInitData;
use config::USE_SECOND_BUCKET;
use config::{
    BucketIndexType, DEFAULT_PER_CPU_BUFFER_SIZE, MAXIMUM_SECOND_BUCKETS_LOG,
    MINIMIZER_BUCKETS_CHECKPOINT_SIZE, READ_INTERMEDIATE_CHUNKS_SIZE, SwapPriority,
    get_compression_level_info, get_memory_mode,
};
use ggcat_logging::stats;
use hashes::HashableSequence;
use io::compressed_read::CompressedRead;
use io::concurrent::temp_reads::creads_utils::{
    AssemblerMinimizerPosition, BucketModeFromBoolean, CompressedReadsBucketData,
    CompressedReadsBucketDataSerializer, NoMultiplicity,
};
use io::concurrent::temp_reads::extra_data::{
    SequenceExtraDataCombiner, SequenceExtraDataConsecutiveCompression,
    SequenceExtraDataTempBufferManagement,
};
use io::sequences_reader::DnaSequence;
use io::sequences_stream::{GenericSequencesStream, SequenceInfo};
use parallel_processor::buckets::concurrent::{BucketsThreadBuffer, BucketsThreadDispatcher};
use parallel_processor::buckets::writers::compressed_binary_writer::CompressedBinaryWriter;
use parallel_processor::buckets::{
    BucketsCount, ChunkingStatus, ExtraBuckets, MultiChunkBucket, MultiThreadBuckets, SingleBucket,
};
use parallel_processor::execution_manager::executor::{
    AddressProducer, AsyncExecutor, ExecutorAddressOperations, ExecutorReceiver,
};
use parallel_processor::execution_manager::packet::PacketsPool;
use parallel_processor::execution_manager::scheduler::Scheduler;
use parallel_processor::execution_manager::thread_pool::{ExecThreadPool, ExecutorsHandle};
use parallel_processor::phase_times_monitor::PHASES_TIMES_MONITOR;
use parking_lot::{Mutex, RwLock};
use resplit_bucket::RewriteBucketCompute;
use std::cmp::max;
use std::marker::PhantomData;
use std::ops::Deref;
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, AtomicUsize, Ordering};

#[derive(Encode, Decode, Clone, Copy, Debug, PartialEq, Eq)]
pub enum MinimizerBucketMode {
    Single,
    Compacted,
}

pub struct MinimzerBucketingFilesReaderInputPacket<
    Factory: MinimizerBucketingExecutorFactory,
    SequencesStream: GenericSequencesStream,
> {
    pub sequences: SequencesStream::SequenceBlockData,
    pub stream_info: Factory::StreamInfo,
}

pub trait MinimizerInputSequence: HashableSequence + Copy {
    fn get_subslice(&self, range: Range<usize>) -> Self;
    fn seq_len(&self) -> usize;
    fn debug_to_string(&self) -> String;
}

impl<'a> MinimizerInputSequence for CompressedRead<'a> {
    fn get_subslice(&self, range: Range<usize>) -> Self {
        self.sub_slice(range)
    }

    fn seq_len(&self) -> usize {
        self.bases_count()
    }

    fn debug_to_string(&self) -> String {
        self.to_string()
    }
}

impl MinimizerInputSequence for &[u8] {
    #[inline(always)]
    fn get_subslice(&self, range: Range<usize>) -> Self {
        &self[range]
    }

    fn seq_len(&self) -> usize {
        self.len()
    }

    fn debug_to_string(&self) -> String {
        std::str::from_utf8(self).unwrap().to_string()
    }
}

pub struct PushSequenceInfo<'a, S, F: MinimizerBucketingExecutorFactory> {
    pub bucket: BucketIndexType,
    pub second_bucket: BucketIndexType,
    pub sequence: S,
    pub extra_data: F::ReadExtraData,
    pub temp_buffer: &'a <F::ReadExtraData as SequenceExtraDataTempBufferManagement>::TempBuffer,
    pub minimizer_pos: u16,
    pub flags: u8,
    pub rc: bool,
    pub is_window_duplicate: bool,
}

pub trait MinimizerBucketingExecutorFactory: Sync + Send + Sized + 'static {
    type GlobalData: Sync + Send + 'static;
    type ReadExtraData: SequenceExtraDataConsecutiveCompression;
    type PreprocessInfo: Default;
    type StreamInfo: Clone + Sync + Send + Default + 'static;

    type RewriteBucketCompute: RewriteBucketCompute;

    #[allow(non_camel_case_types)]
    type FLAGS_COUNT: typenum::uint::Unsigned;

    type ExecutorType: MinimizerBucketingExecutor<Self>;

    fn new(global_data: &Arc<MinimizerBucketingCommonData<Self::GlobalData>>)
    -> Self::ExecutorType;
}

pub trait MinimizerBucketingExecutor<Factory: MinimizerBucketingExecutorFactory>:
    'static + Sync + Send
{
    fn preprocess_dna_sequence(
        &mut self,
        stream_info: &Factory::StreamInfo,
        sequence_info: SequenceInfo,
        read_index: u64,
        sequence: &DnaSequence,
        preprocess_info: &mut Factory::PreprocessInfo,
    );

    fn reprocess_sequence(
        &mut self,
        flags: u8,
        intermediate_data: &Factory::ReadExtraData,
        intermediate_data_buffer: &<Factory::ReadExtraData as SequenceExtraDataTempBufferManagement>::TempBuffer,
        preprocess_info: &mut Factory::PreprocessInfo,
    );

    fn process_sequence<
        S: MinimizerInputSequence,
        F: FnMut(PushSequenceInfo<S, Factory>),
        const SEPARATE_DUPLICATES: bool,
    >(
        &mut self,
        preprocess_info: &Factory::PreprocessInfo,
        sequence: S,
        range: Range<usize>,
        used_bits: usize,
        first_bits: usize,
        second_bits: usize,
        push_sequence: F,
    );
}

pub struct MinimizerBucketingCommonData<GlobalData> {
    pub k: usize,
    pub m: usize,
    pub ignored_length: usize,
    pub buckets_count: BucketsCount,
    pub second_buckets_count: BucketsCount,
    pub global_counters: Vec<Vec<AtomicU64>>,
    pub compaction_offsets: Vec<AtomicI64>,
    pub global_data: GlobalData,
    pub is_active: AtomicBool,
}

impl<GlobalData> MinimizerBucketingCommonData<GlobalData> {
    pub fn new(
        k: usize,
        m: usize,
        buckets_count: BucketsCount,
        ignored_length: usize,
        second_buckets_count: BucketsCount,
        global_data: GlobalData,
    ) -> Self {
        Self {
            k,
            m,
            ignored_length,
            buckets_count,
            second_buckets_count,
            global_counters: (0..buckets_count.total_buckets_count)
                .into_iter()
                .map(|_| {
                    (0..second_buckets_count.total_buckets_count)
                        .into_iter()
                        .map(|_| AtomicU64::new(0))
                        .collect()
                })
                .collect(),
            compaction_offsets: (0..buckets_count.total_buckets_count)
                .map(|_| AtomicI64::new(0))
                .collect(),
            global_data,
            is_active: AtomicBool::new(true),
        }
    }
}

pub struct CompationRatioInfo {
    pub avg_ratio: f64,
    pub compactions: usize,
    pub skip_step: usize,
    pub skipped: usize,
}

pub struct MinimizerBucketingExecutionContext<
    E: MinimizerBucketingExecutorFactory + Sync + Send + 'static,
> {
    pub buckets: Arc<MultiThreadBuckets<CompressedBinaryWriter>>,
    pub common: Arc<MinimizerBucketingCommonData<E::GlobalData>>,
    pub current_file: AtomicUsize,
    pub executor_group_address:
        RwLock<Option<AddressProducer<MinimizerBucketingQueueData<E::StreamInfo>>>>,
    pub processed_files: AtomicUsize,
    pub total_files: usize,
    pub read_threads_count: usize,
    pub threads_count: usize,
    pub output_path: PathBuf,

    pub seq_count: AtomicU64,
    pub last_total_count: AtomicU64,
    pub tot_bases_count: AtomicU64,
    pub valid_bases_count: AtomicU64,

    pub compaction_ratios: Vec<Mutex<CompationRatioInfo>>,

    pub packets_pool: PacketsPool<MinimizerBucketingQueueData<E::StreamInfo>>,

    pub partial_read_copyback: Option<usize>,
    pub copy_ident: bool,
}

pub struct GenericMinimizerBucketing;

struct MinimizerBucketingExecWriter<
    SingleData: SequenceExtraDataConsecutiveCompression + Sync + Send + 'static,
    MultipleData: SequenceExtraDataCombiner<SingleDataType = SingleData> + Sync + Send + 'static,
    Executor: MinimizerBucketingExecutorFactory<ReadExtraData = SingleData> + Sync + Send + 'static,
> {
    _phantom: PhantomData<(SingleData, MultipleData, Executor)>, // mem_tracker: MemoryTracker<Self>,
}

struct WriterContext<
    SingleData: SequenceExtraDataConsecutiveCompression + Sync + Send + 'static,
    MultipleData: SequenceExtraDataCombiner<SingleDataType = SingleData> + Sync + Send + 'static,
    Executor: MinimizerBucketingExecutorFactory<ReadExtraData = SingleData> + Sync + Send + 'static,
> {
    compaction_handle:
        ExecutorsHandle<MinimizerBucketingCompactor<SingleData, MultipleData, Executor>>,
    global: Arc<MinimizerBucketingExecutionContext<Executor>>,
}

impl<
    SingleData: SequenceExtraDataConsecutiveCompression + Sync + Send + 'static,
    MultipleData: SequenceExtraDataCombiner<SingleDataType = SingleData> + Sync + Send + 'static,
    Executor: MinimizerBucketingExecutorFactory<ReadExtraData = SingleData> + Sync + Send + 'static,
> MinimizerBucketingExecWriter<SingleData, MultipleData, Executor>
{
    fn execute(
        &self,
        context: &WriterContext<SingleData, MultipleData, Executor>,
        ops: &ExecutorAddressOperations<Self>,
    ) {
        let compaction_handle = &context.compaction_handle;
        let context = context.global.deref();

        let mut counters: Vec<u16> = vec![
            0;
            context.common.second_buckets_count.total_buckets_count
                * context.common.buckets_count.total_buckets_count
        ];

        let mut tmp_reads_buffer = BucketsThreadDispatcher::<
            _,
            CompressedReadsBucketDataSerializer<
                Executor::ReadExtraData,
                Executor::FLAGS_COUNT,
                BucketModeFromBoolean<USE_SECOND_BUCKET>,
                NoMultiplicity,
                AssemblerMinimizerPosition,
            >,
        >::new(
            &context.buckets,
            BucketsThreadBuffer::new(
                DEFAULT_PER_CPU_BUFFER_SIZE,
                context.buckets.get_buckets_count(),
            ),
            context.common.k,
        );

        // self.mem_tracker.update_memory_usage(&[
        //     DEFAULT_PER_CPU_BUFFER_SIZE.octets as usize * context.buckets.count()
        // ]);
        let global_counters = &context.common.global_counters;

        stats!(
            let thread_id = ggcat_logging::generate_stat_id!();
        );

        while let Some(input_packet) = ops.receive_packet() {
            let mut total_bases = 0;
            let mut sequences_splitter = SequencesSplitter::new(context.common.k);
            let mut buckets_processor = Executor::new(&context.common);

            let mut sequences_count = 0;

            let mut preprocess_info = Default::default();
            let input_packet = input_packet.deref();

            stats!(
                let stat_start_time = ggcat_logging::get_stat_opt!(stats.start_time).elapsed();
            );

            for (index, (x, seq_info)) in input_packet.iter_sequences().enumerate() {
                total_bases += x.seq.len() as u64;
                buckets_processor.preprocess_dna_sequence(
                    &input_packet.stream_info,
                    seq_info,
                    input_packet.start_read_index + index as u64,
                    &x,
                    &mut preprocess_info,
                );

                sequences_splitter.process_sequences(&x, &mut |sequence: &[u8], range| {
                    buckets_processor.process_sequence::<_, _, true>(
                        &preprocess_info,
                        sequence,
                        range,
                        0,
                        context.common.buckets_count.normal_buckets_count_log,
                        context.common.second_buckets_count.normal_buckets_count_log,
                        |info| {

                            let PushSequenceInfo { bucket, second_bucket, sequence, minimizer_pos, flags, extra_data, temp_buffer, rc, is_window_duplicate } = info;

                            let counter = &mut counters
                                [((bucket as usize) * context.common.second_buckets_count.total_buckets_count) + (second_bucket as usize)];

                            *counter = counter.wrapping_add(1);
                            if *counter == 0 {
                                global_counters[bucket as usize][second_bucket as usize]
                                    .fetch_add(65536, Ordering::Relaxed);
                            }

                            let chunking_status = tmp_reads_buffer.add_element_extended(
                                bucket,
                                &extra_data,
                                temp_buffer,
                                &CompressedReadsBucketData::new_plain_opt_rc(sequence, flags, second_bucket as u8, rc, minimizer_pos, is_window_duplicate),
                            );

                            // New chunks were produced, spawn new compactors
                            if let ChunkingStatus::NewChunks { bucket_indexes } = chunking_status {
                                stats!(let trigger_time = ggcat_logging::get_stat_opt!(stats.start_time).elapsed(););
                                stats!(stats.assembler.compact_checkpoints.push(ggcat_logging::stats::CompactCheckpointStats {
                                    trigger_time: trigger_time.into(),
                                    buckets: bucket_indexes.clone(),
                                    trigger_input_chunk: input_packet.stats_block_id,
                                    thread_id,
                                }));

                                for bucket_index in bucket_indexes {
                                    compaction_handle.create_new_address_with_limit(
                                        Arc::new(CompactorInitData { bucket_index }),
                                        true,
                                        // Avoid having too many compactions waiting
                                        (context.threads_count * 2).max(context.common.buckets_count.total_buckets_count / 32)
                                    );
                                }
                            }
                        },
                    );
                });

                sequences_count += 1;
            }

            context
                .seq_count
                .fetch_add(sequences_count, Ordering::Relaxed);
            let total_bases_count = context
                .tot_bases_count
                .fetch_add(total_bases, Ordering::Relaxed)
                + total_bases;
            context
                .valid_bases_count
                .fetch_add(sequences_splitter.valid_bases, Ordering::Relaxed);

            stats!(
                let end_time = ggcat_logging::get_stat_opt!(stats.start_time).elapsed();
            );

            stats!(stats.assembler.input_process_stats.push(
                ggcat_logging::stats::InputChunkProcessStats {
                    id: input_packet.stats_block_id,
                    start_time: stat_start_time.into(),
                    end_time: end_time.into(),
                    thread_id,
                }
            ));

            const TOTAL_BASES_DIFF_LOG: u64 = 10000000000;

            let do_print_log = context
                .last_total_count
                .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |x| {
                    if total_bases_count > x + TOTAL_BASES_DIFF_LOG {
                        Some(total_bases_count)
                    } else {
                        None
                    }
                })
                .is_ok();

            if do_print_log {
                let current_file = context.current_file.load(Ordering::Relaxed);
                let processed_files = context.processed_files.load(Ordering::Relaxed);

                ggcat_logging::info!(
                    "Elaborated {} sequences! [{} | {:.2}% qb] ({}[{}]/{} => {:.2}%) {}",
                    context.seq_count.load(Ordering::Relaxed),
                    context.valid_bases_count.load(Ordering::Relaxed),
                    (context.valid_bases_count.load(Ordering::Relaxed) as f64)
                        / (max(1, context.tot_bases_count.load(Ordering::Relaxed)) as f64)
                        * 100.0,
                    processed_files,
                    current_file,
                    context.total_files,
                    processed_files as f64 / max(1, context.total_files) as f64 * 100.0,
                    PHASES_TIMES_MONITOR
                        .read()
                        .get_formatted_counter_without_memory()
                );
            }
        }

        for bucket in 0..global_counters.len() {
            for next_bucket in 0..global_counters[0].len() {
                let counter = counters[((bucket as usize)
                    * context.common.second_buckets_count.total_buckets_count)
                    + (next_bucket as usize)];
                global_counters[bucket as usize][next_bucket as usize]
                    .fetch_add(counter as u64, Ordering::Relaxed);
            }
        }

        tmp_reads_buffer.finalize();
    }
}

impl<
    SingleData: SequenceExtraDataConsecutiveCompression + Sync + Send + 'static,
    MultipleData: SequenceExtraDataCombiner<SingleDataType = SingleData> + Sync + Send + 'static,
    Executor: MinimizerBucketingExecutorFactory<ReadExtraData = SingleData> + Sync + Send + 'static,
> AsyncExecutor for MinimizerBucketingExecWriter<SingleData, MultipleData, Executor>
{
    type InputPacket = MinimizerBucketingQueueData<Executor::StreamInfo>;
    type OutputPacket = ();
    type GlobalParams = WriterContext<SingleData, MultipleData, Executor>;
    type InitData = ();
    const ALLOW_PARALLEL_ADDRESS_EXECUTION: bool = true;

    fn new() -> Self {
        Self {
            _phantom: PhantomData,
        }
    }

    fn executor_main<'a>(
        &'a mut self,
        params: &'a Self::GlobalParams,
        mut receiver: ExecutorReceiver<Self>,
    ) {
        while let Ok(address) = receiver.obtain_address() {
            self.execute(params, &address);
        }
    }
}

// const STRICT_POOL_ALLOC: bool = false;
//
// const MEMORY_FIELDS_COUNT: usize = 1;
// const MEMORY_FIELDS: &'static [&'static str] = &["TMP_READS_BUFFER"];
//     fn pre_execute<EX: ExecutorOperations<Self>>(
//         &mut self,
//         _reinit_params: Self::BuildParams,
//         _ops: EX,
//     ) {
//     }
//
//     fn execute<EX: ExecutorOperations<Self>>(
//         &mut self,
//         input_packet: Packet<Self::InputPacket>,
//         _ops: EX,
//     ) {
//     }
//
//     fn finalize<EX: ExecutorOperations<Self>>(&mut self, _ops: EX) {
//         self.tmp_reads_buffer.take().unwrap().finalize();
//     }
// }

impl GenericMinimizerBucketing {
    pub fn do_bucketing_no_max_usage<
        SingleData: SequenceExtraDataConsecutiveCompression + Sync + Send + 'static,
        MultipleData: SequenceExtraDataCombiner<SingleDataType = SingleData> + Sync + Send + 'static,
        Executor: MinimizerBucketingExecutorFactory<ReadExtraData = SingleData> + Sync + Send + 'static,
        SequenceType: GenericSequencesStream,
    >(
        input_blocks: impl ExactSizeIterator<
            Item = MinimzerBucketingFilesReaderInputPacket<Executor, SequenceType>,
        >,
        output_path: &Path,
        buckets_count: BucketsCount,
        threads_count: usize,
        k: usize,
        m: usize,
        global_data: Executor::GlobalData,
        partial_read_copyback: Option<usize>,
        copy_ident: bool,
        ignored_length: usize,
    ) -> (Vec<SingleBucket>, PathBuf) {
        let (buckets, counters) =
            Self::do_bucketing::<SingleData, MultipleData, Executor, SequenceType>(
                input_blocks,
                output_path,
                buckets_count,
                threads_count,
                k,
                m,
                global_data,
                partial_read_copyback,
                copy_ident,
                ignored_length,
                None,
            );

        (
            buckets
                .into_iter()
                .map(MultiChunkBucket::into_single)
                .collect(),
            counters,
        )
    }

    pub fn do_bucketing<
        SingleData: SequenceExtraDataConsecutiveCompression + Sync + Send + 'static,
        MultipleData: SequenceExtraDataCombiner<SingleDataType = SingleData> + Sync + Send + 'static,
        Executor: MinimizerBucketingExecutorFactory<ReadExtraData = SingleData> + Sync + Send + 'static,
        SequenceType: GenericSequencesStream,
    >(
        input_blocks: impl ExactSizeIterator<
            Item = MinimzerBucketingFilesReaderInputPacket<Executor, SequenceType>,
        >,
        output_path: &Path,
        buckets_count: BucketsCount,
        threads_count: usize,
        k: usize,
        m: usize,
        global_data: Executor::GlobalData,
        partial_read_copyback: Option<usize>,
        copy_ident: bool,
        ignored_length: usize,
        maximum_disk_usage: Option<u64>,
    ) -> (Vec<MultiChunkBucket>, PathBuf) {
        let read_threads_count = max(1, threads_count / 2);
        let compute_threads_count = max(1, threads_count.saturating_sub(read_threads_count / 4));

        let buckets = Arc::new(MultiThreadBuckets::<CompressedBinaryWriter>::new(
            buckets_count,
            output_path.join("bucket"),
            maximum_disk_usage,
            &(
                get_memory_mode(SwapPriority::MinimizerBuckets),
                MINIMIZER_BUCKETS_CHECKPOINT_SIZE,
                get_compression_level_info(),
            ),
            &MinimizerBucketMode::Single,
        ));

        let second_buckets_count = BucketsCount::new(
            MAXIMUM_SECOND_BUCKETS_LOG.max(threads_count.next_power_of_two().ilog2() as usize),
            ExtraBuckets::None,
        );

        let global_context = Arc::new(MinimizerBucketingExecutionContext::<Executor> {
            buckets,
            current_file: AtomicUsize::new(0),
            executor_group_address: RwLock::new(None),
            processed_files: AtomicUsize::new(0),
            total_files: input_blocks.len(),
            common: Arc::new(MinimizerBucketingCommonData::new(
                k,
                m,
                buckets_count,
                ignored_length,
                second_buckets_count,
                global_data,
            )),
            threads_count: compute_threads_count,
            output_path: output_path.to_path_buf(),

            seq_count: AtomicU64::new(0),
            last_total_count: AtomicU64::new(0),
            tot_bases_count: AtomicU64::new(0),
            valid_bases_count: AtomicU64::new(0),

            compaction_ratios: (0..buckets_count.total_buckets_count)
                .map(|_| {
                    Mutex::new(CompationRatioInfo {
                        avg_ratio: 0.0,
                        compactions: 0,
                        skip_step: 0,
                        skipped: 0,
                    })
                })
                .collect(),

            packets_pool: PacketsPool::new(
                compute_threads_count * 4,
                READ_INTERMEDIATE_CHUNKS_SIZE,
            ),
            partial_read_copyback,
            read_threads_count,
            copy_ident,
        });

        {
            let scheduler = Scheduler::new(threads_count);

            let mut disk_thread_pool = ExecThreadPool::<
                MinimizerBucketingFilesReader<Executor, SequenceType>,
            >::new(
                global_context.read_threads_count, "mm_disk", false
            );
            let mut compute_thread_pool = ExecThreadPool::<
                MinimizerBucketingExecWriter<SingleData, MultipleData, Executor>,
            >::new(
                compute_threads_count, "mm_compute", false
            );
            let mut compaction_thread_pool =
                ExecThreadPool::<
                    compactor::MinimizerBucketingCompactor<SingleData, MultipleData, Executor>,
                >::new(compute_threads_count, "mm_compact", false);

            let compaction_thread_pool_handle =
                compaction_thread_pool.start(scheduler.clone(), &global_context);

            let compute_thread_pool_handle = compute_thread_pool.start(
                scheduler.clone(),
                &Arc::new(WriterContext {
                    compaction_handle: compaction_thread_pool_handle.clone(),
                    global: global_context.clone(),
                }),
            );

            let compute_address =
                compute_thread_pool_handle.create_new_address(Arc::new(()), false);
            *global_context.executor_group_address.write() = Some(compute_address);

            let disk_thread_pool_handle =
                disk_thread_pool.start(scheduler.clone(), &global_context);
            disk_thread_pool_handle.add_input_data((), input_blocks.into_iter());

            drop(disk_thread_pool_handle);
            drop(compaction_thread_pool_handle);
            drop(compute_thread_pool_handle);

            disk_thread_pool.join();

            // Let compactors know that the phase is finishing,
            // so they can shortcut and avoid processing other buckets
            global_context
                .common
                .is_active
                .store(false, Ordering::Relaxed);

            Option::take(&mut global_context.executor_group_address.write());
            compute_thread_pool.join();

            compaction_thread_pool.join();
        }

        let global_context = Arc::try_unwrap(global_context)
            .unwrap_or_else(|_| panic!("Cannot get execution context!"));

        let common_context = Arc::try_unwrap(global_context.common)
            .unwrap_or_else(|_| panic!("Cannot get common execution context!"));

        let counters_analyzer = CountersAnalyzer::new(
            common_context.global_counters,
            common_context.compaction_offsets,
        );
        // counters_analyzer.print_debug();

        let counters_file = output_path.join("buckets-counters.dat");

        counters_analyzer.serialize_to_file(&counters_file);

        (global_context.buckets.finalize(), counters_file)
    }
}
