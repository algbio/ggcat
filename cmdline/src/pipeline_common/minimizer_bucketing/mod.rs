pub mod counters_analyzer;
mod queue_data;
mod reader;
mod sequences_splitter;

use crate::config::{
    BucketIndexType, SwapPriority, DEFAULT_LZ4_COMPRESSION_LEVEL, DEFAULT_PER_CPU_BUFFER_SIZE,
    MINIMIZER_BUCKETS_CHECKPOINT_SIZE, READ_INTERMEDIATE_CHUNKS_SIZE,
    READ_INTERMEDIATE_QUEUE_MULTIPLIER,
};
use crate::config::{SECOND_BUCKETS_COUNT, USE_SECOND_BUCKET};
use crate::hashes::HashableSequence;
use crate::io::concurrent::temp_reads::creads_utils::CompressedReadsBucketHelper;
use crate::io::concurrent::temp_reads::extra_data::SequenceExtraData;
use crate::io::sequences_reader::FastaSequence;
use crate::pipeline_common::minimizer_bucketing::counters_analyzer::CountersAnalyzer;
use crate::pipeline_common::minimizer_bucketing::queue_data::MinimizerBucketingQueueData;
use crate::pipeline_common::minimizer_bucketing::reader::MinimizerBucketingFilesReader;
use crate::pipeline_common::minimizer_bucketing::sequences_splitter::SequencesSplitter;
use crate::utils::get_memory_mode;
use parallel_processor::buckets::concurrent::{BucketsThreadBuffer, BucketsThreadDispatcher};
use parallel_processor::buckets::writers::compressed_binary_writer::CompressedBinaryWriter;
use parallel_processor::buckets::MultiThreadBuckets;
use parallel_processor::execution_manager::executor::{Executor, ExecutorOperations, ExecutorType};
use parallel_processor::execution_manager::executor_address::ExecutorAddress;
use parallel_processor::execution_manager::executors_list::{
    ExecOutputMode, ExecutorAllocMode, ExecutorsList, PoolAllocMode,
};
use parallel_processor::execution_manager::memory_tracker::MemoryTracker;
use parallel_processor::execution_manager::objects_pool::PoolObjectTrait;
use parallel_processor::execution_manager::packet::Packet;
use parallel_processor::execution_manager::thread_pool::{
    ExecThreadPoolBuilder, ExecThreadPoolDataAddTrait,
};
use parallel_processor::execution_manager::units_io::{ExecutorInput, ExecutorInputAddressMode};
use parallel_processor::phase_times_monitor::PHASES_TIMES_MONITOR;
use parking_lot::RwLock;
use std::cmp::max;
use std::ops::Deref;
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;

pub trait MinimizerInputSequence: HashableSequence + Copy {
    fn get_subslice(&self, range: Range<usize>) -> Self;
    fn seq_len(&self) -> usize;
    fn debug_to_string(&self) -> String;
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
    type ExtraData: SequenceExtraData;
    type PreprocessInfo: Default;
    type FileInfo: Clone + Sync + Send + Default + 'static;

    #[allow(non_camel_case_types)]
    type FLAGS_COUNT: typenum::uint::Unsigned;

    type ExecutorType: MinimizerBucketingExecutor<Self>;

    fn new(global_data: &Arc<MinimizerBucketingCommonData<Self::GlobalData>>)
        -> Self::ExecutorType;
}

pub trait MinimizerBucketingExecutor<Factory: MinimizerBucketingExecutorFactory>:
    'static + Sync + Send
{
    fn preprocess_fasta(
        &mut self,
        file_info: &Factory::FileInfo,
        read_index: u64,
        sequence: &FastaSequence,
        preprocess_info: &mut Factory::PreprocessInfo,
    );

    fn reprocess_sequence(
        &mut self,
        flags: u8,
        intermediate_data: &Factory::ExtraData,
        intermediate_data_buffer: &<Factory::ExtraData as SequenceExtraData>::TempBuffer,
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
            &<Factory::ExtraData as SequenceExtraData>::TempBuffer,
        ),
    >(
        &mut self,
        preprocess_info: &Factory::PreprocessInfo,
        sequence: S,
        range: Range<usize>,
        push_sequence: F,
    );
}

pub struct MinimizerBucketingCommonData<GlobalData> {
    pub k: usize,
    pub m: usize,
    pub buckets_count: usize,
    pub buckets_count_mask: BucketIndexType,
    pub second_buckets_count: usize,
    pub second_buckets_count_mask: BucketIndexType,
    pub global_counters: Vec<Vec<AtomicU64>>,
    pub global_data: GlobalData,
}

impl<GlobalData> MinimizerBucketingCommonData<GlobalData> {
    pub fn new(
        k: usize,
        m: usize,
        buckets_count: usize,
        second_buckets_count: usize,
        global_data: GlobalData,
    ) -> Self {
        Self {
            k,
            m,
            buckets_count,
            buckets_count_mask: (buckets_count - 1) as BucketIndexType,
            second_buckets_count,
            second_buckets_count_mask: (second_buckets_count - 1) as BucketIndexType,
            global_counters: (0..buckets_count)
                .into_iter()
                .map(|_| {
                    (0..second_buckets_count)
                        .into_iter()
                        .map(|_| AtomicU64::new(0))
                        .collect()
                })
                .collect(),
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
}

pub struct GenericMinimizerBucketing;

static SEQ_COUNT: AtomicU64 = AtomicU64::new(0);
static LAST_TOTAL_COUNT: AtomicU64 = AtomicU64::new(0);
static TOT_BASES_COUNT: AtomicU64 = AtomicU64::new(0);
static VALID_BASES_COUNT: AtomicU64 = AtomicU64::new(0);

struct MinimizerBucketingExecWriter<E: MinimizerBucketingExecutorFactory + 'static> {
    context: Arc<MinimizerBucketingExecutionContext<E::GlobalData>>,
    mem_tracker: MemoryTracker<Self>,
    counters: Vec<u8>,
    counters_log: u32,
    tmp_reads_buffer: Option<BucketsThreadDispatcher<CompressedBinaryWriter>>,
}

impl<E: MinimizerBucketingExecutorFactory + 'static> PoolObjectTrait
    for MinimizerBucketingExecWriter<E>
{
    type InitData = (
        Arc<MinimizerBucketingExecutionContext<E::GlobalData>>,
        MemoryTracker<Self>,
    );

    fn allocate_new((context, mem_tracker): &Self::InitData) -> Self {
        Self {
            context: context.clone(),
            mem_tracker: mem_tracker.clone(),
            counters: vec![],
            counters_log: 0,
            tmp_reads_buffer: None,
        }
    }

    fn reset(&mut self) {}
}

impl<E: MinimizerBucketingExecutorFactory + 'static> Executor for MinimizerBucketingExecWriter<E> {
    const EXECUTOR_TYPE: ExecutorType = ExecutorType::SimplePacketsProcessing;

    const BASE_PRIORITY: u64 = 0;
    const PACKET_PRIORITY_MULTIPLIER: u64 = 1;
    const STRICT_POOL_ALLOC: bool = false;

    const MEMORY_FIELDS_COUNT: usize = 1;
    const MEMORY_FIELDS: &'static [&'static str] = &["TMP_READS_BUFFER"];

    type InputPacket = MinimizerBucketingQueueData<E::FileInfo>;
    type OutputPacket = ();
    type GlobalParams = MinimizerBucketingExecutionContext<E::GlobalData>;
    type MemoryParams = ();
    type BuildParams = ();

    fn allocate_new_group<EX: ExecutorOperations<Self>>(
        global_params: Arc<Self::GlobalParams>,
        _memory_params: Option<Self::MemoryParams>,
        _common_packet: Option<Packet<Self::InputPacket>>,
        _ops: EX,
    ) -> (Self::BuildParams, usize) {
        let max_concurrency = global_params.threads_count;
        ((), max_concurrency)
    }

    fn required_pool_items(&self) -> u64 {
        0
    }

    fn pre_execute<EX: ExecutorOperations<Self>>(
        &mut self,
        _reinit_params: Self::BuildParams,
        _ops: EX,
    ) {
        self.counters_log = self.context.common.second_buckets_count.log2();
        self.counters.resize(
            self.context.common.second_buckets_count * self.context.common.buckets_count,
            0,
        );

        self.tmp_reads_buffer = Some(BucketsThreadDispatcher::new(
            &self.context.buckets,
            BucketsThreadBuffer::new(DEFAULT_PER_CPU_BUFFER_SIZE, self.context.buckets.count()),
        ));

        self.mem_tracker.update_memory_usage(&[
            DEFAULT_PER_CPU_BUFFER_SIZE.octets as usize * self.context.buckets.count()
        ]);
    }

    fn execute<EX: ExecutorOperations<Self>>(
        &mut self,
        input_packet: Packet<Self::InputPacket>,
        _ops: EX,
    ) {
        let global_counters = &self.context.common.global_counters;
        let second_buckets_count_mask = self.context.common.second_buckets_count_mask;

        let mut total_bases = 0;
        let mut sequences_splitter = SequencesSplitter::new(self.context.common.k);
        let mut buckets_processor = E::new(&self.context.common);

        let mut sequences_count = 0;

        let mut preprocess_info = Default::default();
        let input_packet = input_packet.deref();
        let tmp_reads_buffer = self.tmp_reads_buffer.as_mut().unwrap();

        for (index, x) in input_packet.iter_sequences().enumerate() {
            total_bases += x.seq.len() as u64;
            buckets_processor.preprocess_fasta(
                &input_packet.file_info,
                input_packet.start_read_index + index as u64,
                &x,
                &mut preprocess_info,
            );

            sequences_splitter.process_sequences(&x, None, &mut |sequence: &[u8], range| {
                buckets_processor.process_sequence(
                    &preprocess_info,
                    sequence,
                    range,
                    |bucket, second_bucket, seq, flags, extra, extra_buffer| {
                        let counter = &mut self.counters[((bucket as usize) << self.counters_log)
                            + (second_bucket & second_buckets_count_mask) as usize];

                        *counter = counter.wrapping_add(1);
                        if *counter == 0 {
                            global_counters[bucket as usize]
                                [(second_bucket & second_buckets_count_mask) as usize]
                                .fetch_add(256, Ordering::Relaxed);
                        }

                        tmp_reads_buffer.add_element_extended(
                            bucket,
                            &extra,
                            extra_buffer,
                            &CompressedReadsBucketHelper::<
                                _,
                                E::FLAGS_COUNT,
                                { USE_SECOND_BUCKET },
                            >::new(seq, flags, second_bucket as u8),
                        );
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
            let current_file = self.context.current_file.load(Ordering::Relaxed);
            let processed_files = self.context.processed_files.load(Ordering::Relaxed);

            println!(
                "Elaborated {} sequences! [{} | {:.2}% qb] ({}[{}]/{} => {:.2}%) {}",
                SEQ_COUNT.load(Ordering::Relaxed),
                VALID_BASES_COUNT.load(Ordering::Relaxed),
                (VALID_BASES_COUNT.load(Ordering::Relaxed) as f64)
                    / (max(1, TOT_BASES_COUNT.load(Ordering::Relaxed)) as f64)
                    * 100.0,
                processed_files,
                current_file,
                self.context.total_files,
                processed_files as f64 / max(1, self.context.total_files) as f64 * 100.0,
                PHASES_TIMES_MONITOR
                    .read()
                    .get_formatted_counter_without_memory()
            );
        }
    }

    fn finalize<EX: ExecutorOperations<Self>>(&mut self, _ops: EX) {
        self.tmp_reads_buffer.take().unwrap().finalize();
    }

    fn is_finished(&self) -> bool {
        false
    }
    fn get_current_memory_params(&self) -> Self::MemoryParams {
        ()
    }
}

impl GenericMinimizerBucketing {
    pub fn do_bucketing<E: MinimizerBucketingExecutorFactory + 'static>(
        mut input_files: Vec<(PathBuf, E::FileInfo)>,
        output_path: &Path,
        buckets_count: usize,
        threads_count: usize,
        k: usize,
        m: usize,
        global_data: E::GlobalData,
    ) -> (Vec<PathBuf>, PathBuf) {
        let read_threads_count = max(1, threads_count / 2);
        let compute_threads_count = max(1, threads_count.saturating_sub(read_threads_count / 2));

        let buckets = Arc::new(MultiThreadBuckets::<CompressedBinaryWriter>::new(
            buckets_count,
            output_path.join("bucket"),
            &(
                get_memory_mode(SwapPriority::MinimizerBuckets),
                MINIMIZER_BUCKETS_CHECKPOINT_SIZE,
                DEFAULT_LZ4_COMPRESSION_LEVEL,
            ),
        ));

        input_files.sort_by_cached_key(|(file, _)| {
            std::fs::metadata(file)
                .expect(&format!("Error while opening file {}", file.display()))
                .len()
        });
        input_files.reverse();

        let second_buckets_count = max(SECOND_BUCKETS_COUNT, threads_count.next_power_of_two());

        let execution_context = Arc::new(MinimizerBucketingExecutionContext {
            buckets,
            current_file: AtomicUsize::new(0),
            executor_group_address: RwLock::new(Some(
                MinimizerBucketingExecWriter::<E>::generate_new_address(),
            )),
            processed_files: AtomicUsize::new(0),
            total_files: input_files.len(),
            common: Arc::new(MinimizerBucketingCommonData::new(
                k,
                m,
                buckets_count,
                second_buckets_count,
                global_data,
            )),
            threads_count: compute_threads_count,
            read_threads_count,
        });

        {
            let max_read_buffers_count =
                compute_threads_count * READ_INTERMEDIATE_QUEUE_MULTIPLIER.load(Ordering::Relaxed);

            let mut disk_thread_pool_builder =
                ExecThreadPoolBuilder::new(execution_context.read_threads_count);
            let mut compute_thread_pool_builcer = ExecThreadPoolBuilder::new(compute_threads_count);

            let mut input_files =
                ExecutorInput::from_iter(input_files.into_iter(), ExecutorInputAddressMode::Single);

            let mut file_readers =
                ExecutorsList::<MinimizerBucketingFilesReader<E::GlobalData, E::FileInfo>>::new(
                    ExecutorAllocMode::Fixed(execution_context.read_threads_count),
                    PoolAllocMode::Distinct {
                        capacity: max_read_buffers_count,
                    },
                    READ_INTERMEDIATE_CHUNKS_SIZE,
                    &execution_context,
                    &mut disk_thread_pool_builder,
                );

            let bucket_writers = ExecutorsList::<MinimizerBucketingExecWriter<E>>::new(
                ExecutorAllocMode::Fixed(compute_threads_count),
                PoolAllocMode::None,
                (),
                &execution_context,
                &mut compute_thread_pool_builcer,
            );

            let disk_thread_pool = disk_thread_pool_builder.build();
            let compute_thread_pool = compute_thread_pool_builcer.build();

            input_files
                .set_output_pool::<MinimizerBucketingFilesReader<E::GlobalData, E::FileInfo>>(
                    &disk_thread_pool,
                    ExecOutputMode::LowPriority,
                );
            file_readers.set_output_pool(&compute_thread_pool, ExecOutputMode::LowPriority);

            compute_thread_pool.add_executors_batch(vec![execution_context
                .executor_group_address
                .read()
                .as_ref()
                .unwrap()
                .clone()]);

            disk_thread_pool.start("mm_disk");
            compute_thread_pool.start("mm_comp");

            disk_thread_pool.wait_for_executors(&file_readers);
            disk_thread_pool.join();

            execution_context.executor_group_address.write().take();

            compute_thread_pool.wait_for_executors(&bucket_writers);
            compute_thread_pool.join();
        }

        let execution_context = Arc::try_unwrap(execution_context)
            .unwrap_or_else(|_| panic!("Cannot get execution context!"));

        let common_context = Arc::try_unwrap(execution_context.common)
            .unwrap_or_else(|_| panic!("Cannot get common execution context!"));

        let counters_analyzer = CountersAnalyzer::new(common_context.global_counters);
        // counters_analyzer.print_debug();

        let counters_file = output_path.join("buckets-counters.dat");

        counters_analyzer.serialize_to_file(&counters_file);

        (execution_context.buckets.finalize(), counters_file)
    }
}
