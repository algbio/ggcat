#![feature(int_log)]
#![feature(type_alias_impl_trait)]
#![feature(generic_associated_types)]
#![feature(drain_filter)]

mod reader;

use crate::processor::KmersTransformProcessor;
use crate::reader::{InputBucketDesc, KmersTransformReader};
use crate::resplitter::KmersTransformResplitter;
use config::{
    BucketIndexType, KEEP_FILES, KMERS_TRANSFORM_READS_CHUNKS_SIZE, MAXIMUM_JIT_PROCESSED_BUCKETS,
    MAXIMUM_SECOND_BUCKETS_COUNT, MINIMUM_LOG_DELTA_TIME, PACKETS_PRIORITY_FILES,
};
use io::compressed_read::{CompressedRead, CompressedReadIndipendent};
use io::concurrent::temp_reads::extra_data::SequenceExtraData;
use io::get_bucket_index;
use minimizer_bucketing::counters_analyzer::CountersAnalyzer;
use minimizer_bucketing::MinimizerBucketingExecutorFactory;
use parallel_processor::execution_manager::execution_context::{ExecutionContext, PoolAllocMode};
use parallel_processor::execution_manager::memory_tracker::MemoryTracker;
use parallel_processor::execution_manager::objects_pool::PoolObjectTrait;
use parallel_processor::execution_manager::packet::{Packet, PacketTrait};
use parallel_processor::execution_manager::thread_pool::ExecThreadPool;
use parallel_processor::execution_manager::units_io::{ExecutorInput, ExecutorInputAddressMode};
use parallel_processor::memory_fs::MemoryFs;
use parallel_processor::phase_times_monitor::PHASES_TIMES_MONITOR;
use parking_lot::Mutex;
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
        used_hash_bits: usize,
        bucket_bits_count: usize,
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
    ) -> Packet<Self::MapStruct>;

    fn finalize(self, global_data: &F::GlobalExtraData);
}

pub struct KmersTransform<F: KmersTransformExecutorFactory> {
    global_context: Arc<KmersTransformContext<F>>,
    normal_buckets_list: Vec<InputBucketDesc>,
    // oversized_buckets_list: Vec<InputBucketDesc>,
    last_info_log: Mutex<Instant>,
    _phantom: PhantomData<F>,
}

pub struct KmersTransformContext<F: KmersTransformExecutorFactory> {
    k: usize,
    min_bucket_size: u64,
    buckets_count: usize,
    extra_buckets_count: AtomicUsize,
    rewritten_buckets_count: AtomicUsize,
    processed_buckets_count: AtomicUsize,
    processed_extra_buckets_count: AtomicUsize,

    total_buckets_size: usize,
    processed_buckets_size: AtomicUsize,
    processed_extra_buckets_size: AtomicUsize,

    // finalizer_address: Arc<RwLock<Option<ExecutorAddress>>>,
    global_extra_data: Arc<F::GlobalExtraData>,
    // async_readers: ScopedThreadLocal<Arc<AsyncReaderThread>>,
    compute_threads_count: usize,
    read_threads_count: usize,
    max_second_buckets_count_log2: usize,
    temp_dir: PathBuf,

    reader_init_lock: tokio::sync::Mutex<()>,
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
        let counters = CountersAnalyzer::load_from_file(
            buckets_counters_path,
            !KEEP_FILES.load(Ordering::Relaxed),
        );

        let mut total_buckets_size = 0;

        let mut files_with_sizes: Vec<_> = file_inputs
            .into_iter()
            .map(|f| {
                let file_size = MemoryFs::get_file_size(&f).unwrap_or(0);
                total_buckets_size += file_size;
                (f, file_size)
            })
            .collect();

        files_with_sizes.sort_by_key(|x| x.1);
        files_with_sizes.reverse();

        // let oversized_buckets_list = files_with_sizes
        //     .drain_filter(|b| {
        //         counters
        //             .get_counters_for_bucket(get_bucket_index(&b.0))
        //             .iter()
        //             .any(|sb| false) // sb.is_outlier)
        //     })
        //     .map(|(path, size)| {
        //         let bucket_index = get_bucket_index(&path);
        //         InputBucketDesc {
        //             path,
        //             sub_bucket_counters: counters.get_counters_for_bucket(bucket_index).clone(),
        //             resplitted: false,
        //             rewritten: false,
        //             used_hash_bits: 0,
        //         }
        //     })
        //     .collect();

        let normal_buckets_list = {
            let mut buckets_list = Vec::with_capacity(files_with_sizes.len());
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
                    sub_bucket_counters: counters.get_counters_for_bucket(bucket_index).clone(),
                    resplitted: false,
                    rewritten: false,
                    used_hash_bits: buckets_count.ilog2() as usize,
                })
            }
            buckets_list
        };

        let read_threads_count = max(1, threads_count / 4 * 3);

        let execution_context = Arc::new(KmersTransformContext {
            k,
            min_bucket_size,
            buckets_count,
            extra_buckets_count: AtomicUsize::new(0),
            rewritten_buckets_count: AtomicUsize::new(0),
            processed_buckets_count: AtomicUsize::new(0),
            processed_extra_buckets_count: AtomicUsize::new(0),
            total_buckets_size,
            processed_buckets_size: AtomicUsize::new(0),
            processed_extra_buckets_size: AtomicUsize::new(0),
            compute_threads_count: max(1, threads_count),
            read_threads_count,
            global_extra_data,
            max_second_buckets_count_log2: MAXIMUM_SECOND_BUCKETS_COUNT.ilog2() as usize,
            temp_dir: temp_dir.to_path_buf(),
            reader_init_lock: tokio::sync::Mutex::new(()),
        });

        Self {
            global_context: execution_context,
            normal_buckets_list,
            // oversized_buckets_list,
            last_info_log: Mutex::new(Instant::now()),
            _phantom: Default::default(),
        }
    }

    pub fn parallel_kmers_transform(mut self) {
        let max_extra_read_buffers_count = max(
            MAXIMUM_JIT_PROCESSED_BUCKETS,
            self.global_context.compute_threads_count,
        ) + 1;

        let compute_threads_count = self.global_context.compute_threads_count;
        let read_threads_count = self.global_context.read_threads_count;

        let execution_context = ExecutionContext::new();

        let disk_thread_pool =
            ExecThreadPool::new(&execution_context, read_threads_count, "km_disk");
        let compute_thread_pool =
            ExecThreadPool::new(&execution_context, compute_threads_count, "km_comp");

        let mut normal_input_buckets = ExecutorInput::from_iter(
            std::mem::take(&mut self.normal_buckets_list).into_iter(),
            ExecutorInputAddressMode::Multiple,
        );

        // let mut oversized_input_buckets = ExecutorInput::from_iter(
        //     std::mem::take(&mut self.oversized_buckets_list).into_iter(),
        //     ExecutorInputAddressMode::Multiple,
        // );

        let bucket_readers = disk_thread_pool.register_executors::<KmersTransformReader<F>>(
            read_threads_count,
            PoolAllocMode::Distinct {
                capacity: (1 << self.global_context.max_second_buckets_count_log2)
                    + max_extra_read_buffers_count,
            },
            KMERS_TRANSFORM_READS_CHUNKS_SIZE,
            &self.global_context,
        );

        let min_maps_count = max(MAXIMUM_JIT_PROCESSED_BUCKETS, compute_threads_count);

        let bucket_sequences_processors = compute_thread_pool
            .register_executors::<KmersTransformProcessor<F>>(
                min_maps_count + 2,
                PoolAllocMode::Shared {
                    capacity: min_maps_count + 2,
                },
                (),
                &self.global_context,
            );

        let bucket_resplitters = compute_thread_pool
            .register_executors::<KmersTransformResplitter<F>>(
                compute_threads_count,
                PoolAllocMode::None,
                (),
                &self.global_context,
            );

        normal_input_buckets.set_output_executor::<KmersTransformReader<F>>(
            &execution_context,
            (),
            PACKETS_PRIORITY_FILES,
        );
        // oversized_input_buckets.set_output_executor::<KmersTransformReader<F>>(
        //     &execution_context,
        //     (),
        //     PACKETS_PRIORITY_PENDING_RESPLIT,
        // );

        execution_context.start();

        // Log progress info while waiting for completion
        let mut bucket_readers_count;
        while {
            bucket_readers_count = execution_context.get_pending_executors_count(bucket_readers);
            bucket_readers_count > 0
        } {
            self.maybe_log_completed_buckets(|| {
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

        // // Remove the finalize address, as all maps have finished working
        // self.global_context.finalizer_address.write().take();

        // // Wait for the final writer to finish
        // execution_context.wait_for_completion(bucket_writers);
        execution_context.join_all();
    }

    fn maybe_log_completed_buckets(&self, extra_debug: impl FnOnce()) -> bool {
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

            let rewritten_buckets_count = self
                .global_context
                .rewritten_buckets_count
                .load(Ordering::Relaxed);

            let processed_count = max(
                1,
                self.global_context
                    .processed_buckets_count
                    .load(Ordering::Relaxed),
            );

            let extra_processed_buckets_count = self
                .global_context
                .processed_extra_buckets_count
                .load(Ordering::Relaxed);

            let eta_standard_processed_size = self
                .global_context
                .processed_buckets_size
                .load(Ordering::Relaxed);

            let eta_extra_processed_size = self
                .global_context
                .processed_extra_buckets_size
                .load(Ordering::Relaxed);

            let eta_processed_size = eta_standard_processed_size + eta_extra_processed_size;

            let eta_remaining_size =
                self.global_context.total_buckets_size - eta_standard_processed_size;
            let eta_total_buckets_size =
                self.global_context.total_buckets_size + eta_extra_processed_size;

            let eta = Duration::from_secs(
                (monitor.get_phase_timer().as_secs_f64() / (eta_processed_size as f64)
                    * (eta_remaining_size as f64)) as u64,
            );

            let est_tot = Duration::from_secs(
                (monitor.get_phase_timer().as_secs_f64() / (eta_processed_size as f64)
                    * (eta_total_buckets_size as f64)) as u64,
            );

            println!(
                "Processing bucket {}{} of [{}{}[R:{}]] {} phase eta: {:.0?} est. tot: {:.0?}",
                processed_count,
                if extra_processed_buckets_count > 0 {
                    format!("(+{})", extra_processed_buckets_count)
                } else {
                    String::new()
                },
                buckets_count,
                if extra_buckets_count > 0 {
                    format!("(+{})", extra_buckets_count)
                } else {
                    String::new()
                },
                rewritten_buckets_count,
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
