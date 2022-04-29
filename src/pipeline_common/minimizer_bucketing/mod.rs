pub mod counters_analyzer;
mod queue_data;
mod reader;
mod sequences_splitter;

use crate::config::USE_SECOND_BUCKET;
use crate::config::{
    BucketIndexType, SwapPriority, DEFAULT_LZ4_COMPRESSION_LEVEL, DEFAULT_PER_CPU_BUFFER_SIZE,
    MINIMIZER_BUCKETS_CHECKPOINT_SIZE, READ_INTERMEDIATE_CHUNKS_SIZE,
    READ_INTERMEDIATE_QUEUE_MULTIPLIER,
};
use crate::hashes::HashableSequence;
use crate::io::concurrent::temp_reads::creads_utils::CompressedReadsBucketHelper;
use crate::io::concurrent::temp_reads::extra_data::SequenceExtraData;
use crate::io::sequences_reader::FastaSequence;
use crate::pipeline_common::minimizer_bucketing::counters_analyzer::CountersAnalyzer;
use crate::pipeline_common::minimizer_bucketing::queue_data::MinimizerBucketingQueueData;
use crate::pipeline_common::minimizer_bucketing::reader::{
    minb_reader, MinimizerBucketingFilesReader,
};
use crate::pipeline_common::minimizer_bucketing::sequences_splitter::SequencesSplitter;
use crate::utils::get_memory_mode;
use parallel_processor::buckets::concurrent::{BucketsThreadBuffer, BucketsThreadDispatcher};
use parallel_processor::buckets::writers::compressed_binary_writer::CompressedBinaryWriter;
use parallel_processor::buckets::MultiThreadBuckets;
use parallel_processor::execution_manager::executor::{Executor, ExecutorType, Packet};
use parallel_processor::execution_manager::executor_address::ExecutorAddress;
use parallel_processor::execution_manager::executors_list::{
    ExecOutputMode, ExecutorAllocMode, ExecutorsList, PoolAllocMode,
};
use parallel_processor::execution_manager::objects_pool::PoolObjectTrait;
use parallel_processor::execution_manager::thread_pool::ExecThreadPool;
use parallel_processor::execution_manager::units_io::{
    ExecOutput, ExecutorInput, ExecutorInputAddressMode,
};
use parallel_processor::phase_times_monitor::PHASES_TIMES_MONITOR;
use parallel_processor::threadpools_chain::{
    ObjectsPoolManager, ThreadPoolDefinition, ThreadPoolsChain,
};
use std::cmp::max;
use std::marker::PhantomData;
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

    type ExecutorType<'a>: MinimizerBucketingExecutor<'a, Self>;

    fn new<'a>(
        global_data: &'a MinimizerBucketingCommonData<Self::GlobalData>,
    ) -> Self::ExecutorType<'a>;
}

pub trait MinimizerBucketingExecutor<'a, FACTORY: MinimizerBucketingExecutorFactory> {
    fn preprocess_fasta(
        &mut self,
        file_info: &FACTORY::FileInfo,
        read_index: u64,
        preprocess_info: &mut FACTORY::PreprocessInfo,
        sequence: &FastaSequence,
    );

    fn reprocess_sequence(
        &mut self,
        flags: u8,
        intermediate_data: &FACTORY::ExtraData,
        preprocess_info: &mut FACTORY::PreprocessInfo,
    );

    fn process_sequence<
        S: MinimizerInputSequence,
        F: FnMut(BucketIndexType, BucketIndexType, S, u8, FACTORY::ExtraData),
    >(
        &mut self,
        preprocess_info: &FACTORY::PreprocessInfo,
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
    pub buckets: MultiThreadBuckets<CompressedBinaryWriter>,
    pub common: MinimizerBucketingCommonData<GlobalData>,
    pub current_file: AtomicUsize,
    pub executor_group_address: ExecutorAddress,
    pub processed_files: AtomicUsize,
    pub total_files: usize,
    pub threads_count: usize,
}

pub struct GenericMinimizerBucketing;

static SEQ_COUNT: AtomicU64 = AtomicU64::new(0);
static LAST_TOTAL_COUNT: AtomicU64 = AtomicU64::new(0);
static TOT_BASES_COUNT: AtomicU64 = AtomicU64::new(0);
static VALID_BASES_COUNT: AtomicU64 = AtomicU64::new(0);

struct MinimizerBucketingExecWriter<E: MinimizerBucketingExecutorFactory + 'static> {
    context: Option<Arc<MinimizerBucketingExecutionContext<E::GlobalData>>>,
    tmp_reads_buffer: Option<BucketsThreadDispatcher<'static, CompressedBinaryWriter>>,
}

impl<E: MinimizerBucketingExecutorFactory + 'static> PoolObjectTrait
    for MinimizerBucketingExecWriter<E>
{
    type InitData = ();

    fn allocate_new(_: &Self::InitData) -> Self {
        Self {
            context: None,
            tmp_reads_buffer: None,
        }
    }

    fn reset(&mut self) {}
}

impl<E: MinimizerBucketingExecutorFactory + 'static> Executor for MinimizerBucketingExecWriter<E> {
    const EXECUTOR_TYPE: ExecutorType = ExecutorType::SingleUnit;
    const EXECUTOR_TYPE_INDEX: u64 = 0;
    type InputPacket = MinimizerBucketingQueueData<E::FileInfo>;
    type OutputPacket = ();
    type GlobalParams = MinimizerBucketingExecutionContext<E::GlobalData>;
    type MemoryParams = ();
    type BuildParams = Arc<MinimizerBucketingExecutionContext<E::GlobalData>>;

    fn allocate_new_group(
        global_params: Arc<Self::GlobalParams>,
        _memory_params: Option<Self::MemoryParams>,
    ) -> Self::BuildParams {
        global_params
    }

    fn get_maximum_concurrency(&self) -> usize {
        self.context.as_ref().unwrap().threads_count
    }

    fn reinitialize<P: FnMut() -> Packet<Self::OutputPacket>>(
        &mut self,
        context: &Self::BuildParams,
        packet_alloc: P,
    ) {
        self.context = Some(context.clone());
        self.tmp_reads_buffer = Some(BucketsThreadDispatcher::new_owned(
            unsafe { &*(&context.buckets as *const MultiThreadBuckets<CompressedBinaryWriter>) },
            BucketsThreadBuffer::new(DEFAULT_PER_CPU_BUFFER_SIZE, context.buckets.count()),
        ));
    }

    fn execute<
        P: FnMut() -> Packet<Self::OutputPacket>,
        S: FnMut(ExecutorAddress, Packet<Self::OutputPacket>),
    >(
        &mut self,
        input_packet: Packet<Self::InputPacket>,
        packet_alloc: P,
        packet_send: S,
    ) {
        println!(
            "Processing {} sequences!",
            input_packet.get_value().sequences.len()
        );
    }

    fn finalize<S: FnMut(ExecutorAddress, Packet<Self::OutputPacket>)>(&mut self, packet_send: S) {
        self.tmp_reads_buffer.take().unwrap().finalize();
        self.context.take();
    }

    fn get_total_memory(&self) -> u64 {
        0
    }

    fn get_current_memory_params(&self) -> Self::MemoryParams {
        ()
    }
}

fn worker<E: MinimizerBucketingExecutorFactory>(
    context: &MinimizerBucketingExecutionContext<E::GlobalData>,
    manager: ObjectsPoolManager<(), MinimizerBucketingQueueData<E::FileInfo>>,
) {
    let mut buffer = BucketsThreadBuffer::new(DEFAULT_PER_CPU_BUFFER_SIZE, context.buckets.count());

    let mut tmp_reads_buffer = BucketsThreadDispatcher::new(&context.buckets, &mut buffer);

    let mut buckets_processor = E::new(&context.common);

    let mut counters =
        vec![0u8; context.common.second_buckets_count * context.common.buckets_count];
    let counters_log = context.common.second_buckets_count.log2();

    while let Some(data) = manager.recv_obj() {
        let mut total_bases = 0;
        let mut sequences_splitter = SequencesSplitter::new(context.common.k);

        let mut preprocess_info = E::PreprocessInfo::default();

        let mut sequences_count = 0;

        for (index, x) in data.iter_sequences().enumerate() {
            total_bases += x.seq.len() as u64;
            buckets_processor.preprocess_fasta(
                &data.file_info,
                data.start_read_index + index as u64,
                &mut preprocess_info,
                &x,
            );

            sequences_splitter.process_sequences(&x, None, &mut |sequence: &[u8], range| {
                buckets_processor.process_sequence(
                    &preprocess_info,
                    sequence,
                    range,
                    |bucket, second_bucket, seq, flags, extra| {
                        let counter = &mut counters[((bucket as usize) << counters_log)
                            + (second_bucket & context.common.second_buckets_count_mask) as usize];

                        *counter = counter.wrapping_add(1);
                        if *counter == 0 {
                            context.common.global_counters[bucket as usize][(second_bucket
                                & context.common.second_buckets_count_mask)
                                as usize]
                                .fetch_add(256, Ordering::Relaxed);
                        }

                        tmp_reads_buffer.add_element(
                            bucket,
                            &extra,
                            &CompressedReadsBucketHelper::<
                                _,
                                E::FLAGS_COUNT,
                                { USE_SECOND_BUCKET },
                                true,
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
            let current_file = context.current_file.load(Ordering::Relaxed);
            let processed_files = context.processed_files.load(Ordering::Relaxed);

            println!(
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

        manager.return_obj(data);
    }

    tmp_reads_buffer.finalize();
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
        let buckets = MultiThreadBuckets::<CompressedBinaryWriter>::new(
            buckets_count,
            output_path.join("bucket"),
            &(
                get_memory_mode(SwapPriority::MinimizerBuckets),
                MINIMIZER_BUCKETS_CHECKPOINT_SIZE,
                DEFAULT_LZ4_COMPRESSION_LEVEL,
            ),
        );

        input_files.sort_by_cached_key(|(file, _)| {
            std::fs::metadata(file)
                .expect(&format!("Error while opening file {}", file.display()))
                .len()
        });
        input_files.reverse();

        let second_buckets_count = max(16, threads_count.next_power_of_two());

        const ATOMIC_COUNTER: AtomicUsize = AtomicUsize::new(0);

        let mut execution_context = Arc::new(MinimizerBucketingExecutionContext {
            buckets,
            current_file: AtomicUsize::new(0),
            executor_group_address: MinimizerBucketingExecWriter::<E>::generate_new_address(),
            processed_files: AtomicUsize::new(0),
            total_files: input_files.len(),
            common: MinimizerBucketingCommonData::new(
                k,
                m,
                buckets_count,
                second_buckets_count,
                global_data,
            ),
            threads_count,
        });

        {
            let input_files_count = input_files.len();

            let max_read_buffers_count =
                threads_count * READ_INTERMEDIATE_QUEUE_MULTIPLIER.load(Ordering::Relaxed);

            let disk_thread_pool =
                ExecThreadPool::new(max(1, threads_count / 2), input_files_count);
            let compute_thread_pool = ExecThreadPool::new(threads_count, max_read_buffers_count);

            let mut input_files =
                ExecutorInput::from_iter(input_files.into_iter(), ExecutorInputAddressMode::Single);

            let mut file_readers = ExecutorsList::<MinimizerBucketingFilesReader<_, _>>::new(
                ExecutorAllocMode::Fixed(4),
                PoolAllocMode::Instance {
                    capacity: max_read_buffers_count,
                },
                READ_INTERMEDIATE_CHUNKS_SIZE,
                &execution_context,
                &disk_thread_pool,
            );
            input_files.set_output_executors(&file_readers, ExecOutputMode::FIFO);

            let bucket_writers = ExecutorsList::<MinimizerBucketingExecWriter<E>>::new(
                ExecutorAllocMode::Fixed(threads_count),
                PoolAllocMode::None,
                (),
                &execution_context,
                &compute_thread_pool,
            );
            file_readers.set_output_executors(&bucket_writers, ExecOutputMode::FIFO);

            // disk_thread_pool.start();
            // compute_thread_pool.start();
            // ThreadPoolsChain::run_double(
            //     input_files,
            //     ThreadPoolDefinition::new(
            //         &execution_context,
            //         READ_INTERMEDIATE_CHUNKS_SIZE,
            //         String::from("r-assembler-minimizer"),
            //         max(1, threads_count / 2),
            //         &AtomicUsize::new(threads_count),
            //         threads_count * READ_INTERMEDIATE_QUEUE_MULTIPLIER.load(Ordering::Relaxed),
            //         minb_reader,
            //     ),
            //     ThreadPoolDefinition::new(
            //         &execution_context,
            //         (),
            //         String::from("w-assembler-minimizer"),
            //         threads_count,
            //         &AtomicUsize::new(threads_count),
            //         threads_count * READ_INTERMEDIATE_QUEUE_MULTIPLIER.load(Ordering::Relaxed),
            //         worker::<E>,
            //     ),
            // );

            disk_thread_pool.start();
            compute_thread_pool.start();

            disk_thread_pool.join();
            compute_thread_pool.join();
        }

        let mut execution_context = Arc::try_unwrap(execution_context)
            .unwrap_or_else(|_| panic!("Cannot get execution context!"));

        let counters_analyzer = CountersAnalyzer::new(execution_context.common.global_counters);
        // counters_analyzer.print_debug();

        let counters_file = output_path.join("buckets-counters.dat");

        counters_analyzer.serialize_to_file(&counters_file);

        (execution_context.buckets.finalize(), counters_file)
    }
}
