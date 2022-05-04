mod reader;

use crate::config::{
    BucketIndexType, DEFAULT_OUTPUT_BUFFER_SIZE, DEFAULT_PREFETCH_AMOUNT,
    KMERS_TRANSFORM_READS_CHUNKS_SIZE, MINIMUM_LOG_DELTA_TIME, READ_INTERMEDIATE_QUEUE_MULTIPLIER,
    USE_SECOND_BUCKET,
};
use crate::io::concurrent::temp_reads::creads_utils::CompressedReadsBucketHelper;
use crate::io::concurrent::temp_reads::extra_data::SequenceExtraData;
use crate::pipeline_common::kmers_transform::processor::KmersTransformProcessor;
use crate::pipeline_common::kmers_transform::reader::{InputBucketDesc, KmersTransformReader};
use crate::pipeline_common::kmers_transform::reads_buffer::ReadsBuffer;
use crate::pipeline_common::kmers_transform::resplitter::KmersTransformResplitter;
use crate::pipeline_common::kmers_transform::writer::KmersTransformWriter;
use crate::pipeline_common::minimizer_bucketing::counters_analyzer::CountersAnalyzer;
use crate::pipeline_common::minimizer_bucketing::MinimizerBucketingExecutorFactory;
use crate::utils::compressed_read::CompressedReadIndipendent;
use crate::utils::Utils;
use crate::{CompressedRead, KEEP_FILES};
use crossbeam::channel::{bounded, unbounded, Receiver};
use crossbeam::queue::ArrayQueue;
use parallel_processor::buckets::readers::async_binary_reader::{
    AsyncBinaryReader, AsyncReaderThread,
};
use parallel_processor::execution_manager::executor::Executor;
use parallel_processor::execution_manager::executor_address::ExecutorAddress;
use parallel_processor::execution_manager::executors_list::{
    ExecOutputMode, ExecutorAllocMode, ExecutorsList, PoolAllocMode,
};
use parallel_processor::execution_manager::objects_pool::PoolObjectTrait;
use parallel_processor::execution_manager::packet::{Packet, PacketTrait};
use parallel_processor::execution_manager::thread_pool::{
    ExecThreadPoolBuilder, ExecThreadPoolDataAddTrait,
};
use parallel_processor::execution_manager::units_io::{ExecutorInput, ExecutorInputAddressMode};
use parallel_processor::memory_data_size::MemoryDataSize;
use parallel_processor::memory_fs::{MemoryFs, RemoveFileMode};
use parallel_processor::phase_times_monitor::PHASES_TIMES_MONITOR;
use parallel_processor::utils::scoped_thread_local::ScopedThreadLocal;
use parking_lot::{Mutex, RwLock};
use std::cmp::max;
use std::marker::PhantomData;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

mod processor;
mod reads_buffer;
mod resplitter;
mod writer;

pub trait KmersTransformExecutorFactory: Sized + 'static + Sync + Send {
    type SequencesResplitterFactory: MinimizerBucketingExecutorFactory<
        ExtraData = Self::AssociatedExtraData,
    >;
    type GlobalExtraData: Sync + Send;
    type AssociatedExtraData: SequenceExtraData;
    type PreprocessorType: KmersTransformPreprocessor<Self>;
    type MapProcessorType: KmersTransformMapProcessor<
        Self,
        MapStruct = <Self::FinalExecutorType as KmersTransformFinalExecutor<Self>>::MapStruct,
    >;
    type FinalExecutorType: KmersTransformFinalExecutor<Self>;

    #[allow(non_camel_case_types)]
    type FLAGS_COUNT: typenum::uint::Unsigned;

    fn new_resplitter(
        global_data: &Arc<Self::GlobalExtraData>,
    ) -> <Self::SequencesResplitterFactory as MinimizerBucketingExecutorFactory>::ExecutorType;

    fn new_preprocessor(global_data: &Arc<Self::GlobalExtraData>) -> Self::PreprocessorType;
    fn new_map_processor(global_data: &Arc<Self::GlobalExtraData>) -> Self::MapProcessorType;
    fn new_final_executor(global_data: &Arc<Self::GlobalExtraData>) -> Self::FinalExecutorType;
}

pub trait KmersTransformPreprocessor<F: KmersTransformExecutorFactory>:
    Sized + 'static + Sync + Send
{
    fn get_sequence_bucket<C>(
        &self,
        global_data: &F::GlobalExtraData,
        seq_data: &(u8, u8, C, CompressedRead),
    ) -> BucketIndexType;
}

pub trait KmersTransformMapProcessor<F: KmersTransformExecutorFactory>:
    Sized + 'static + Sync + Send
{
    type MapStruct: PacketTrait + PoolObjectTrait<InitData = ()>;

    fn process_group_start(
        &mut self,
        map_struct: Packet<Self::MapStruct>,
        global_data: &F::GlobalExtraData,
    );
    fn process_group_batch_sequences(
        &mut self,
        global_data: &F::GlobalExtraData,
        batch: &Vec<(u8, F::AssociatedExtraData, CompressedReadIndipendent)>,
        ref_sequences: &Vec<u8>,
    );
    fn process_group_finalize(
        &mut self,
        global_data: &F::GlobalExtraData,
    ) -> Packet<Self::MapStruct>;
}

pub trait KmersTransformFinalExecutor<F: KmersTransformExecutorFactory>:
    Sized + 'static + Sync + Send
{
    type MapStruct: PacketTrait + PoolObjectTrait<InitData = ()>;

    fn process_map(
        &mut self,
        global_data: &F::GlobalExtraData,
        map_struct: Packet<Self::MapStruct>,
    );

    fn finalize(self, global_data: &F::GlobalExtraData);
}

pub struct KmersTransform<F: KmersTransformExecutorFactory> {
    execution_context: Arc<KmersTransformContext<F>>,
    buckets_list: Vec<InputBucketDesc>,

    last_info_log: Mutex<Instant>,
    _phantom: PhantomData<F>,
}

pub struct KmersTransformContext<F: KmersTransformExecutorFactory> {
    buckets_count: usize,
    processed_buckets: AtomicUsize,

    finalizer_address: Arc<RwLock<Option<ExecutorAddress>>>,

    global_extra_data: Arc<F::GlobalExtraData>,
    async_readers: ScopedThreadLocal<Arc<AsyncReaderThread>>,
    counters: CountersAnalyzer,
    compute_threads_count: usize,
    read_threads_count: usize,
}

impl<F: KmersTransformExecutorFactory> KmersTransform<F> {
    pub fn new(
        file_inputs: Vec<PathBuf>,
        buckets_counters_path: PathBuf,
        buckets_count: usize,
        global_extra_data: Arc<F::GlobalExtraData>,
        threads_count: usize,
    ) -> Self {
        let mut buckets_list = Vec::with_capacity(file_inputs.len());

        let mut files_with_sizes: Vec<_> = file_inputs
            .into_iter()
            .map(|f| {
                let file_size = MemoryFs::get_file_size(&f).unwrap_or(0);
                (f, file_size)
            })
            .collect();

        files_with_sizes.sort_by_key(|x| x.1);
        files_with_sizes.reverse();

        {
            let mut start_idx = 0;
            let mut end_idx = files_with_sizes.len();

            let mut matched_size = 0i64;

            while start_idx != end_idx {
                let file_entry = if matched_size <= 0 {
                    let target_file = &files_with_sizes[start_idx];
                    let entry = target_file.0.clone();
                    matched_size = target_file.1 as i64;
                    start_idx += 1;
                    entry
                } else {
                    let target_file = &files_with_sizes[end_idx - 1];
                    let entry = target_file.0.clone();
                    matched_size -= target_file.1 as i64;
                    end_idx -= 1;
                    entry
                };

                buckets_list.push(InputBucketDesc {
                    path: file_entry,
                    resplitted: false,
                })
            }
        }

        let read_threads_count = max(1, threads_count / 2);

        let execution_context = Arc::new(KmersTransformContext {
            buckets_count,
            processed_buckets: AtomicUsize::new(0),
            finalizer_address: Arc::new(RwLock::new(Some(
                KmersTransformWriter::<F>::generate_new_address(),
            ))),
            compute_threads_count: max(1, threads_count.saturating_sub(read_threads_count / 2)),
            read_threads_count,
            global_extra_data,
            async_readers: ScopedThreadLocal::new(|| {
                AsyncReaderThread::new(DEFAULT_OUTPUT_BUFFER_SIZE / 2, 4)
            }),
            counters: CountersAnalyzer::load_from_file(
                buckets_counters_path,
                !KEEP_FILES.load(Ordering::Relaxed),
            ),
        });

        Self {
            execution_context,
            buckets_list,
            last_info_log: Mutex::new(Instant::now()),
            _phantom: Default::default(),
        }
    }

    pub fn parallel_kmers_transform(&mut self) {
        let max_read_buffers_count = self.execution_context.compute_threads_count
            * READ_INTERMEDIATE_QUEUE_MULTIPLIER.load(Ordering::Relaxed);

        let compute_threads_count = self.execution_context.compute_threads_count;
        let read_threads_count = self.execution_context.read_threads_count;

        let mut disk_thread_pool_builder = ExecThreadPoolBuilder::new(read_threads_count);
        let mut compute_thread_pool_builder = ExecThreadPoolBuilder::new(compute_threads_count);

        let mut input_buckets = ExecutorInput::from_iter(
            std::mem::take(&mut self.buckets_list).into_iter(),
            ExecutorInputAddressMode::Multiple,
        );

        let mut bucket_readers = ExecutorsList::<KmersTransformReader<F>>::new(
            ExecutorAllocMode::Fixed(read_threads_count),
            PoolAllocMode::Distinct {
                capacity: max_read_buffers_count,
            },
            KMERS_TRANSFORM_READS_CHUNKS_SIZE,
            &self.execution_context,
            &mut disk_thread_pool_builder,
        );

        let mut bucket_sequences_processors = ExecutorsList::<KmersTransformProcessor<F>>::new(
            ExecutorAllocMode::MemoryLimited {
                min_count: compute_threads_count * 2,
                max_count: compute_threads_count * 2, //threads_count * 4, FIXME!
                max_memory: MemoryDataSize::from_gibioctets(4), // TODO: Make dynamic
            },
            PoolAllocMode::Shared {
                capacity: compute_threads_count * 4,
            },
            (),
            &self.execution_context,
            &mut compute_thread_pool_builder,
        );

        let mut bucket_resplitters = ExecutorsList::<KmersTransformResplitter<F>>::new(
            ExecutorAllocMode::Fixed(compute_threads_count),
            PoolAllocMode::None,
            (),
            &self.execution_context,
            &mut compute_thread_pool_builder,
        );

        let _bucket_writers = ExecutorsList::<KmersTransformWriter<F>>::new(
            ExecutorAllocMode::Fixed(compute_threads_count / 2),
            PoolAllocMode::None,
            (),
            &self.execution_context,
            &mut compute_thread_pool_builder,
        );

        let disk_thread_pool = disk_thread_pool_builder.build();
        let compute_thread_pool = compute_thread_pool_builder.build();

        input_buckets.set_output_pool::<KmersTransformReader<F>>(
            &disk_thread_pool,
            ExecOutputMode::LowPriority,
        );
        bucket_resplitters.set_output_pool(&disk_thread_pool, ExecOutputMode::HighPriority);
        bucket_readers.set_output_pool(&compute_thread_pool, ExecOutputMode::LowPriority);
        bucket_sequences_processors
            .set_output_pool(&compute_thread_pool, ExecOutputMode::LowPriority);

        compute_thread_pool.add_executors_batch(vec![self
            .execution_context
            .finalizer_address
            .read()
            .as_ref()
            .unwrap()
            .clone()]);

        disk_thread_pool.start("km_disk");
        compute_thread_pool.start("km_comp");

        println!("Disk joining!");
        disk_thread_pool.join();
        println!("Disk joined!");
        compute_thread_pool.join();
        println!("CTP joined!");

        // let mut execution_context = Arc::try_unwrap(execution_context)
        //     .unwrap_or_else(|_| panic!("Cannot get execution context!"));
        //
    }
}
