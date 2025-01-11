pub mod compactor;
pub mod counters_analyzer;
mod queue_data;
mod reader;
mod sequences_splitter;

use crate::counters_analyzer::CountersAnalyzer;
use crate::queue_data::MinimizerBucketingQueueData;
use crate::reader::MinimizerBucketingFilesReader;
use crate::sequences_splitter::SequencesSplitter;
use colors::colors_manager::ColorsManager;
use compactor::CompactorInitData;
use config::{
    get_compression_level_info, get_memory_mode, BucketIndexType, SwapPriority,
    DEFAULT_PER_CPU_BUFFER_SIZE, MINIMIZER_BUCKETS_CHECKPOINT_SIZE, PACKETS_PRIORITY_COMPACT,
    PACKETS_PRIORITY_DEFAULT, READ_INTERMEDIATE_CHUNKS_SIZE, READ_INTERMEDIATE_QUEUE_MULTIPLIER,
    WORKERS_PRIORITY_BASE,
};
use config::{MAXIMUM_SECOND_BUCKETS_COUNT, USE_SECOND_BUCKET};
use hashes::HashableSequence;
use io::compressed_read::CompressedRead;
use io::concurrent::temp_reads::creads_utils::{
    BucketModeFromBoolean, CompressedReadsBucketData, CompressedReadsBucketDataSerializer,
    NoMultiplicity,
};
use io::concurrent::temp_reads::extra_data::{
    SequenceExtraDataConsecutiveCompression, SequenceExtraDataTempBufferManagement,
};
use io::sequences_reader::DnaSequence;
use io::sequences_stream::{GenericSequencesStream, SequenceInfo};
use parallel_processor::buckets::concurrent::{BucketsThreadBuffer, BucketsThreadDispatcher};
use parallel_processor::buckets::writers::compressed_binary_writer::CompressedBinaryWriter;
use parallel_processor::buckets::{
    ChunkingStatus, MultiChunkBucket, MultiThreadBuckets, SingleBucket,
};
use parallel_processor::execution_manager::execution_context::{ExecutionContext, PoolAllocMode};
use parallel_processor::execution_manager::executor::{
    AsyncExecutor, ExecutorAddressOperations, ExecutorReceiver,
};
use parallel_processor::execution_manager::executor_address::ExecutorAddress;
use parallel_processor::execution_manager::memory_tracker::MemoryTracker;
use parallel_processor::execution_manager::thread_pool::ExecThreadPool;
use parallel_processor::execution_manager::units_io::{ExecutorInput, ExecutorInputAddressMode};
use parallel_processor::phase_times_monitor::PHASES_TIMES_MONITOR;
use parking_lot::{Mutex, RwLock};
use serde::{Deserialize, Serialize};
use std::cmp::max;
use std::future::Future;
use std::marker::PhantomData;
use std::ops::Deref;
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicI64, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;

#[derive(Serialize, Deserialize, Clone, Copy, Debug)]
pub enum MinimizerBucketMode {
    Single,
    Compacted,
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

pub trait MinimizerBucketingExecutorFactory: Sized {
    type GlobalData: Sync + Send + 'static;
    type ExtraData: SequenceExtraDataConsecutiveCompression;
    type PreprocessInfo: Default;
    type StreamInfo: Clone + Sync + Send + Default + 'static;

    type ColorsManager: ColorsManager;

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
        intermediate_data: &Factory::ExtraData,
        intermediate_data_buffer: &<Factory::ExtraData as SequenceExtraDataTempBufferManagement>::TempBuffer,
        preprocess_info: &mut Factory::PreprocessInfo,
    );

    fn process_sequence<
        S: MinimizerInputSequence,
        F: FnMut(
            BucketIndexType,
            BucketIndexType,
            S,
            u8,
            Factory::ExtraData,
            &<Factory::ExtraData as SequenceExtraDataTempBufferManagement>::TempBuffer,
        ),
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
    pub buckets_count: usize,
    pub buckets_count_bits: usize,
    pub max_second_buckets_count: usize,
    pub max_second_buckets_count_bits: usize,
    pub global_counters: Vec<Vec<AtomicU64>>,
    pub compaction_offsets: Vec<AtomicI64>,
    pub global_data: GlobalData,
}

impl<GlobalData> MinimizerBucketingCommonData<GlobalData> {
    pub fn new(
        k: usize,
        m: usize,
        buckets_count: usize,
        ignored_length: usize,
        max_second_buckets_count: usize,
        global_data: GlobalData,
    ) -> Self {
        Self {
            k,
            m,
            ignored_length,
            buckets_count,
            buckets_count_bits: buckets_count.ilog2() as usize,
            max_second_buckets_count,
            max_second_buckets_count_bits: max_second_buckets_count.ilog2() as usize,
            global_counters: (0..buckets_count)
                .into_iter()
                .map(|_| {
                    (0..max_second_buckets_count)
                        .into_iter()
                        .map(|_| AtomicU64::new(0))
                        .collect()
                })
                .collect(),
            compaction_offsets: (0..buckets_count).map(|_| AtomicI64::new(0)).collect(),
            global_data,
        }
    }
}

pub struct MinimizerBucketingExecutionContext<GlobalData> {
    pub buckets: Arc<MultiThreadBuckets<CompressedBinaryWriter>>,
    pub common: Arc<MinimizerBucketingCommonData<GlobalData>>,
    pub current_file: AtomicUsize,
    pub executor_group_address: RwLock<Option<ExecutorAddress>>,
    pub processed_files: AtomicUsize,
    pub total_files: usize,
    pub read_threads_count: usize,
    pub threads_count: usize,
    pub output_path: PathBuf,

    pub bucket_compactors: Vec<Mutex<Option<ExecutorAddress>>>,

    pub partial_read_copyback: Option<usize>,
    pub copy_ident: bool,
}

pub struct GenericMinimizerBucketing;

static SEQ_COUNT: AtomicU64 = AtomicU64::new(0);
static LAST_TOTAL_COUNT: AtomicU64 = AtomicU64::new(0);
static TOT_BASES_COUNT: AtomicU64 = AtomicU64::new(0);
static VALID_BASES_COUNT: AtomicU64 = AtomicU64::new(0);

struct MinimizerBucketingExecWriter<E: MinimizerBucketingExecutorFactory + Sync + Send + 'static> {
    _phantom: PhantomData<E>, // mem_tracker: MemoryTracker<Self>,
}

impl<E: MinimizerBucketingExecutorFactory + Sync + Send + 'static> MinimizerBucketingExecWriter<E> {
    async fn execute(
        &self,
        context: &MinimizerBucketingExecutionContext<E::GlobalData>,
        ops: &ExecutorAddressOperations<'_, Self>,
    ) {
        let counters_log = context.common.max_second_buckets_count.ilog2();
        let mut counters: Vec<u8> =
            vec![0; context.common.max_second_buckets_count * context.common.buckets_count];

        let mut tmp_reads_buffer = BucketsThreadDispatcher::<
            _,
            CompressedReadsBucketDataSerializer<
                E::ExtraData,
                E::FLAGS_COUNT,
                BucketModeFromBoolean<USE_SECOND_BUCKET>,
                NoMultiplicity,
            >,
        >::new(
            &context.buckets,
            BucketsThreadBuffer::new(DEFAULT_PER_CPU_BUFFER_SIZE, context.buckets.count()),
        );

        // self.mem_tracker.update_memory_usage(&[
        //     DEFAULT_PER_CPU_BUFFER_SIZE.octets as usize * context.buckets.count()
        // ]);
        let global_counters = &context.common.global_counters;

        while let Some(input_packet) = ops.receive_packet().await {
            let mut total_bases = 0;
            let mut sequences_splitter = SequencesSplitter::new(context.common.k);
            let mut buckets_processor = E::new(&context.common);

            let mut sequences_count = 0;

            let mut preprocess_info = Default::default();
            let input_packet = input_packet.deref();

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
                    buckets_processor.process_sequence(
                        &preprocess_info,
                        sequence,
                        range,
                        0,
                        context.common.buckets_count_bits,
                        context.common.max_second_buckets_count_bits,
                        |bucket, next_bucket, seq, flags, extra, extra_buffer| {
                            let counter = &mut counters
                                [((bucket as usize) << counters_log) + (next_bucket as usize)];

                            *counter = counter.wrapping_add(1);
                            if *counter == 0 {
                                global_counters[bucket as usize][next_bucket as usize]
                                    .fetch_add(256, Ordering::Relaxed);
                            }

                            let chunking_status = tmp_reads_buffer.add_element_extended(
                                bucket,
                                &extra,
                                extra_buffer,
                                &CompressedReadsBucketData::new(seq, flags, next_bucket as u8),
                            );

                            // New chunks were produced, spawn new compactors
                            if let ChunkingStatus::NewChunks { bucket_indexes } = chunking_status {
                                for bucket_index in bucket_indexes {
                                    let new_address = compactor::MinimizerBucketingCompactor::<E>::generate_new_address(
                                        CompactorInitData { bucket_index }
                                    );

                                    ops.declare_addresses(
                                        vec![new_address.clone()],
                                        PACKETS_PRIORITY_COMPACT,
                                    );

                                    *context.bucket_compactors[bucket_index as usize].lock() = Some(new_address);
                                }
                            }
                        },
                    );
                });

                sequences_count += 1;
            }

            SEQ_COUNT.fetch_add(sequences_count, Ordering::Relaxed);
            let total_bases_count =
                TOT_BASES_COUNT.fetch_add(total_bases, Ordering::Relaxed) + total_bases;
            VALID_BASES_COUNT.fetch_add(sequences_splitter.valid_bases, Ordering::Relaxed);

            const TOTAL_BASES_DIFF_LOG: u64 = 10000000000;

            let do_print_log = LAST_TOTAL_COUNT
                .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |x| {
                    if total_bases_count - x > TOTAL_BASES_DIFF_LOG {
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
                    SEQ_COUNT.load(Ordering::Relaxed),
                    VALID_BASES_COUNT.load(Ordering::Relaxed),
                    (VALID_BASES_COUNT.load(Ordering::Relaxed) as f64)
                        / (max(1, TOT_BASES_COUNT.load(Ordering::Relaxed)) as f64)
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
                let counter =
                    counters[((bucket as usize) << counters_log) + (next_bucket as usize)];
                global_counters[bucket as usize][next_bucket as usize]
                    .fetch_add(counter as u64, Ordering::Relaxed);
            }
        }

        tmp_reads_buffer.finalize();
    }
}

impl<E: MinimizerBucketingExecutorFactory + Sync + Send + 'static> AsyncExecutor
    for MinimizerBucketingExecWriter<E>
{
    type InputPacket = MinimizerBucketingQueueData<E::StreamInfo>;
    type OutputPacket = ();
    type GlobalParams = MinimizerBucketingExecutionContext<E::GlobalData>;
    type InitData = ();

    fn new() -> Self {
        Self {
            _phantom: PhantomData,
        }
    }

    fn async_executor_main<'a>(
        &'a mut self,
        global_params: &'a Self::GlobalParams,
        mut receiver: ExecutorReceiver<Self>,
        _memory_tracker: MemoryTracker<Self>,
    ) -> impl Future<Output = ()> + Send + 'a {
        async move {
            while let Ok((address, _)) = receiver
                .obtain_address_with_priority(WORKERS_PRIORITY_BASE)
                .await
            {
                let max_concurrency = global_params.threads_count;

                let mut spawner = address.make_spawner();
                for _ in 0..max_concurrency {
                    spawner.spawn_executor(async {
                        self.execute(global_params, &address).await;
                    });
                }
                spawner.executors_await().await;
            }
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
        E: MinimizerBucketingExecutorFactory + Sync + Send + 'static,
        S: GenericSequencesStream,
    >(
        input_blocks: impl ExactSizeIterator<Item = (S::SequenceBlockData, E::StreamInfo)>,
        output_path: &Path,
        buckets_count: usize,
        threads_count: usize,
        k: usize,
        m: usize,
        global_data: E::GlobalData,
        partial_read_copyback: Option<usize>,
        copy_ident: bool,
        ignored_length: usize,
    ) -> (Vec<SingleBucket>, PathBuf) {
        let (buckets, counters) = Self::do_bucketing::<E, S>(
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
        E: MinimizerBucketingExecutorFactory + Sync + Send + 'static,
        S: GenericSequencesStream,
    >(
        input_blocks: impl ExactSizeIterator<Item = (S::SequenceBlockData, E::StreamInfo)>,
        output_path: &Path,
        buckets_count: usize,
        threads_count: usize,
        k: usize,
        m: usize,
        global_data: E::GlobalData,
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

        let second_buckets_count = max(
            MAXIMUM_SECOND_BUCKETS_COUNT,
            threads_count.next_power_of_two(),
        );

        let global_context = Arc::new(MinimizerBucketingExecutionContext {
            buckets,
            current_file: AtomicUsize::new(0),
            executor_group_address: RwLock::new(Some(
                MinimizerBucketingExecWriter::<E>::generate_new_address(()),
            )),
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

            bucket_compactors: (0..buckets_count).map(|_| Mutex::new(None)).collect(),
            partial_read_copyback,
            read_threads_count,
            copy_ident,
        });

        {
            let max_read_buffers_count =
                compute_threads_count * READ_INTERMEDIATE_QUEUE_MULTIPLIER.load(Ordering::Relaxed);

            let execution_context = ExecutionContext::new();

            let disk_thread_pool = ExecThreadPool::new(
                &execution_context,
                global_context.read_threads_count,
                "mm_disk",
            );
            let compute_thread_pool =
                ExecThreadPool::new(&execution_context, compute_threads_count, "mm_compute");
            let compaction_thread_pool =
                ExecThreadPool::new(&execution_context, compute_threads_count, "mm_compact");

            let mut input_files = ExecutorInput::from_iter(
                input_blocks.into_iter(),
                ExecutorInputAddressMode::Single,
            );

            let reader_executors = disk_thread_pool
                .register_executors::<MinimizerBucketingFilesReader<E::GlobalData, E::StreamInfo, S>>(
                    global_context.read_threads_count,
                    PoolAllocMode::Shared {
                        capacity: max_read_buffers_count,
                    },
                    READ_INTERMEDIATE_CHUNKS_SIZE,
                    &global_context,
                );

            let writer_executors = compute_thread_pool
                .register_executors::<MinimizerBucketingExecWriter<E>>(
                    compute_threads_count,
                    PoolAllocMode::None,
                    (),
                    &global_context,
                );

            let compactor_executors = compaction_thread_pool
                .register_executors::<compactor::MinimizerBucketingCompactor<E>>(
                    compute_threads_count,
                    PoolAllocMode::None,
                    (),
                    &global_context,
                );

            input_files.set_output_executor::<MinimizerBucketingFilesReader<E::GlobalData, E::StreamInfo, S>>(
                &execution_context,
                (),
                PACKETS_PRIORITY_DEFAULT,
            );

            execution_context.register_executors_batch(
                vec![global_context
                    .executor_group_address
                    .read()
                    .as_ref()
                    .unwrap()
                    .clone()],
                PACKETS_PRIORITY_DEFAULT,
            );

            execution_context.start();
            execution_context.wait_for_completion(reader_executors);

            global_context.executor_group_address.write().take();

            execution_context.wait_for_completion(writer_executors);
            execution_context.wait_for_completion(compactor_executors);

            execution_context.join_all();
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
