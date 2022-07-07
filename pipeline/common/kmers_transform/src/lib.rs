#![feature(int_log)]
#![feature(type_alias_impl_trait)]
#![feature(generic_associated_types)]

mod reader;

use crate::processor::KmersTransformProcessor;
use crate::reader::{InputBucketDesc, KmersTransformReader};
use crate::resplitter::KmersTransformResplitter;
use crate::writer::KmersTransformWriter;
use config::{
    BucketIndexType, DEFAULT_OUTPUT_BUFFER_SIZE, KEEP_FILES, KMERS_TRANSFORM_READS_CHUNKS_SIZE,
    MINIMUM_LOG_DELTA_TIME, SECOND_BUCKETS_COUNT,
};
use io::compressed_read::{CompressedRead, CompressedReadIndipendent};
use io::concurrent::temp_reads::extra_data::SequenceExtraData;
use io::get_bucket_index;
use minimizer_bucketing::counters_analyzer::CountersAnalyzer;
use minimizer_bucketing::MinimizerBucketingExecutorFactory;
use parallel_processor::buckets::readers::async_binary_reader::AsyncReaderThread;
use parallel_processor::execution_manager::execution_context::{ExecutionContext, PoolAllocMode};
use parallel_processor::execution_manager::executor::AsyncExecutor;
use parallel_processor::execution_manager::executor_address::ExecutorAddress;
use parallel_processor::execution_manager::memory_tracker::MemoryTracker;
use parallel_processor::execution_manager::objects_pool::PoolObjectTrait;
use parallel_processor::execution_manager::packet::{Packet, PacketTrait};
use parallel_processor::execution_manager::thread_pool::ExecThreadPool;
use parallel_processor::execution_manager::units_io::{ExecutorInput, ExecutorInputAddressMode};
use parallel_processor::memory_data_size::MemoryDataSize;
use parallel_processor::memory_fs::MemoryFs;
use parallel_processor::phase_times_monitor::PHASES_TIMES_MONITOR;
use parallel_processor::utils::scoped_thread_local::ScopedThreadLocal;
use parking_lot::{Mutex, RwLock};
use std::cmp::max;
use std::marker::PhantomData;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

pub mod processor;
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
    fn new_map_processor(
        global_data: &Arc<Self::GlobalExtraData>,
        mem_tracker: MemoryTracker<KmersTransformProcessor<Self>>,
    ) -> Self::MapProcessorType;
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
        extra_data_buffer: &<F::AssociatedExtraData as SequenceExtraData>::TempBuffer,
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
    global_context: Arc<KmersTransformContext<F>>,
    buckets_list: Vec<InputBucketDesc>,
    last_info_log: Mutex<Instant>,
    _phantom: PhantomData<F>,
}

pub struct KmersTransformContext<F: KmersTransformExecutorFactory> {
    k: usize,
    min_bucket_size: u64,
    buckets_count: usize,
    extra_buckets_count: AtomicUsize,

    finalizer_address: Arc<RwLock<Option<ExecutorAddress>>>,

    global_extra_data: Arc<F::GlobalExtraData>,
    // async_readers: ScopedThreadLocal<Arc<AsyncReaderThread>>,
    compute_threads_count: usize,
    read_threads_count: usize,
    max_second_buckets_count_log2: usize,
    temp_dir: PathBuf,
}

impl<F: KmersTransformExecutorFactory> KmersTransform<F> {
    pub fn new(
        file_inputs: Vec<PathBuf>,
        temp_dir: &Path,
        buckets_counters_path: PathBuf,
        buckets_count: usize,
        global_extra_data: Arc<F::GlobalExtraData>,
        threads_count: usize,
        k: usize,
        min_bucket_size: u64,
    ) -> Self {
        let mut buckets_list = Vec::with_capacity(file_inputs.len());

        let counters = CountersAnalyzer::load_from_file(
            buckets_counters_path,
            !KEEP_FILES.load(Ordering::Relaxed),
        );

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

                let bucket_index = get_bucket_index(&file_entry);

                buckets_list.push(InputBucketDesc {
                    path: file_entry,
                    sub_bucket_counters: counters.get_counters_for_bucket(bucket_index),
                    resplitted: false,
                })
            }
        }

        let read_threads_count = 16; //max(1, threads_count / 4 * 3);

        let execution_context = Arc::new(KmersTransformContext {
            k,
            min_bucket_size,
            buckets_count,
            extra_buckets_count: AtomicUsize::new(0),
            finalizer_address: Arc::new(RwLock::new(Some(
                KmersTransformWriter::<F>::generate_new_address(),
            ))),
            compute_threads_count: max(1, threads_count.saturating_sub(read_threads_count / 4)),
            read_threads_count,
            global_extra_data,
            max_second_buckets_count_log2: SECOND_BUCKETS_COUNT.log2() as usize,
            temp_dir: temp_dir.to_path_buf(),
        });

        Self {
            global_context: execution_context,
            buckets_list,
            last_info_log: Mutex::new(Instant::now()),
            _phantom: Default::default(),
        }
    }

    pub fn parallel_kmers_transform(mut self) {
        let max_second_buckets_count = 1 << self.global_context.max_second_buckets_count_log2;

        // FIXME:
        const MAX_CONCURRENCY_SINGLE_BUCKET: usize = 4;

        let max_extra_read_buffers_count = max(
            max_second_buckets_count,
            self.global_context.compute_threads_count,
        ) + self.global_context.read_threads_count;

        let compute_threads_count = self.global_context.compute_threads_count;
        let read_threads_count = self.global_context.read_threads_count;

        let execution_context = ExecutionContext::new();

        let mut disk_thread_pool =
            ExecThreadPool::new(&execution_context, read_threads_count, "km_disk");
        let mut compute_thread_pool =
            ExecThreadPool::new(&execution_context, compute_threads_count, "km_comp");

        let mut input_buckets = ExecutorInput::from_iter(
            std::mem::take(&mut self.buckets_list).into_iter(),
            ExecutorInputAddressMode::Multiple,
        );

        let mut bucket_readers = disk_thread_pool.register_executors::<KmersTransformReader<F>>(
            read_threads_count,
            PoolAllocMode::Distinct {
                capacity: self.global_context.buckets_count * MAX_CONCURRENCY_SINGLE_BUCKET
                    + max_extra_read_buffers_count,
            },
            KMERS_TRANSFORM_READS_CHUNKS_SIZE,
            &self.global_context,
        );

        let min_maps_count = max(max_second_buckets_count, compute_threads_count);

        let mut bucket_sequences_processors = compute_thread_pool
            .register_executors::<KmersTransformProcessor<F>>(
                min_maps_count * 2,
                PoolAllocMode::Shared {
                    capacity: min_maps_count * 2,
                },
                (),
                &self.global_context,
            );

        let mut bucket_resplitters = compute_thread_pool
            .register_executors::<KmersTransformResplitter<F>>(
                compute_threads_count,
                PoolAllocMode::None,
                (),
                &self.global_context,
            );

        let bucket_writers = compute_thread_pool.register_executors::<KmersTransformWriter<F>>(
            1, // Only one executor, that launches multiple sub-threads
            PoolAllocMode::None,
            (),
            &self.global_context,
        );

        input_buckets.set_output_executor::<KmersTransformReader<F>>(&execution_context);

        execution_context.register_executors_batch(vec![self
            .global_context
            .finalizer_address
            .read()
            .as_ref()
            .unwrap()
            .clone()]);

        execution_context.start();

        // Log progress info while waiting for completion
        let mut bucket_readers_count;
        while {
            bucket_readers_count = execution_context.get_pending_executors_count(bucket_readers);
            bucket_readers_count > 0
        } {
            self.maybe_log_completed_buckets(bucket_readers_count as usize, || {
                let debug_level = utils::DEBUG_LEVEL.load(Ordering::Relaxed);

                if debug_level >= 2 {
                    // compute_thread_pool.debug_print_queue();
                }

                if debug_level >= 1 {
                    // disk_thread_pool.debug_print_memory();
                    // compute_thread_pool.debug_print_memory();
                }
            });
            std::thread::sleep(Duration::from_millis(300));
        }

        // Wait for the main buckets to be processed
        execution_context.wait_for_completion(bucket_readers);

        // Wait for the resplitting to be complete
        execution_context.wait_for_completion(bucket_resplitters);
        // Wait for the new buckets reading
        execution_context.wait_for_completion(bucket_readers);

        // Wait for the maps to be complete
        execution_context.wait_for_completion(bucket_sequences_processors);

        // Remove the finalize address, as all maps have finished working
        self.global_context.finalizer_address.write().take();

        // Wait for the final writer to finish
        execution_context.wait_for_completion(bucket_writers);
    }

    fn maybe_log_completed_buckets(&self, remaining: usize, extra_debug: impl FnOnce()) -> bool {
        let mut last_info_log = match self.last_info_log.try_lock() {
            None => return false,
            Some(x) => x,
        };
        if last_info_log.elapsed() > MINIMUM_LOG_DELTA_TIME {
            *last_info_log = Instant::now();
            drop(last_info_log);

            extra_debug();

            let monitor = PHASES_TIMES_MONITOR.read();

            let buckets_count = self.global_context.buckets_count;
            let extra_buckets_count = self
                .global_context
                .extra_buckets_count
                .load(Ordering::Relaxed);

            let total_buckets_count = buckets_count + extra_buckets_count;

            let processed_count = max(1, total_buckets_count.checked_sub(remaining).unwrap_or(0));

            let eta = Duration::from_secs(
                (monitor.get_phase_timer().as_secs_f64() / (processed_count as f64)
                    * (remaining as f64)) as u64,
            );

            let est_tot = Duration::from_secs(
                (monitor.get_phase_timer().as_secs_f64() / (processed_count as f64)
                    * (total_buckets_count as f64)) as u64,
            );

            println!(
                "Processing bucket {} of [{}{}] {} phase eta: {:.0?} est.tot: {:.0?}",
                processed_count,
                buckets_count,
                if extra_buckets_count > 0 {
                    format!("+{}", extra_buckets_count)
                } else {
                    String::new()
                },
                monitor.get_formatted_counter_without_memory(),
                eta,
                est_tot
            );
            true
        } else {
            false
        }
    }
}
