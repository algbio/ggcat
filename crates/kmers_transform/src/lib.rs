mod reader;

use crate::processor::KmersTransformProcessor;
use crate::reader::{InputBucketDesc, KmersTransformReader};
use crate::resplitter::KmersTransformResplitter;
use config::{
    BucketIndexType, KEEP_FILES, KMERS_TRANSFORM_READS_CHUNKS_SIZE,
    MAX_KMERS_TRANSFORM_READERS_PER_BUCKET, MAXIMUM_JIT_PROCESSED_BUCKETS,
    MAXIMUM_SECOND_BUCKETS_COUNT, MINIMUM_LOG_DELTA_TIME, PACKETS_PRIORITY_FILES,
};
use io::concurrent::temp_reads::extra_data::{
    SequenceExtraDataConsecutiveCompression, SequenceExtraDataTempBufferManagement,
};
use minimizer_bucketing::counters_analyzer::CountersAnalyzer;
use minimizer_bucketing::resplit_bucket::RewriteBucketCompute;
use minimizer_bucketing::{MinimizerBucketMode, MinimizerBucketingExecutorFactory};
use parallel_processor::buckets::MultiChunkBucket;
use parallel_processor::execution_manager::execution_context::{ExecutionContext, PoolAllocMode};
use parallel_processor::execution_manager::memory_tracker::MemoryTracker;
use parallel_processor::execution_manager::objects_pool::PoolObjectTrait;
use parallel_processor::execution_manager::packet::{Packet, PacketTrait};
use parallel_processor::execution_manager::thread_pool::ExecThreadPool;
use parallel_processor::execution_manager::units_io::{ExecutorInput, ExecutorInputAddressMode};
use parallel_processor::memory_fs::MemoryFs;
use parallel_processor::phase_times_monitor::PHASES_TIMES_MONITOR;
use parking_lot::Mutex;
use reads_buffer::ReadsVector;
use std::cmp::{max, min};
use std::marker::PhantomData;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

pub mod debug_bucket_stats;
pub mod processor;
pub mod reads_buffer;
mod resplitter;

pub trait KmersTransformGlobalExtraData: Sync + Send {
    fn get_k(&self) -> usize;
    fn get_m(&self) -> usize;
    fn get_m_resplit(&self) -> usize;
}

pub trait KmersTransformExecutorFactory: Sized + 'static + Sync + Send {
    type KmersTransformPacketInitData: Clone + Send + Sync;
    type SequencesResplitterFactory: MinimizerBucketingExecutorFactory<
        ExtraData = Self::AssociatedExtraData,
    >;
    type GlobalExtraData: KmersTransformGlobalExtraData;
    type AssociatedExtraData: SequenceExtraDataConsecutiveCompression + Copy;
    type PreprocessorType: RewriteBucketCompute;
    type MapProcessorType: KmersTransformMapProcessor<
            Self,
            MapStruct = <Self::FinalExecutorType as KmersTransformFinalExecutor<Self>>::MapStruct,
        >;
    type FinalExecutorType: KmersTransformFinalExecutor<Self>;

    #[allow(non_camel_case_types)]
    type FLAGS_COUNT: typenum::uint::Unsigned;

    const HAS_COLORS: bool;

    fn get_packets_init_data(
        global_data: &Arc<Self::GlobalExtraData>,
    ) -> Self::KmersTransformPacketInitData;

    fn new_resplitter(
        global_data: &Arc<Self::GlobalExtraData>,
    ) -> <Self::SequencesResplitterFactory as MinimizerBucketingExecutorFactory>::ExecutorType;

    fn new_map_processor(
        global_data: &Arc<Self::GlobalExtraData>,
        mem_tracker: MemoryTracker<KmersTransformProcessor<Self>>,
    ) -> Self::MapProcessorType;
    fn new_final_executor(global_data: &Arc<Self::GlobalExtraData>) -> Self::FinalExecutorType;
}

pub struct GroupProcessStats {
    pub total_kmers: u64,
    pub unique_kmers: u64,
    pub saved_read_bytes: u64,
}

pub trait KmersTransformMapProcessor<F: KmersTransformExecutorFactory>:
    Sized + 'static + Send
{
    type MapStruct: PacketTrait + PoolObjectTrait<InitData = F::KmersTransformPacketInitData>;
    const MAP_SIZE: usize;

    fn process_group_start(
        &mut self,
        map_struct: Packet<Self::MapStruct>,
        global_data: &F::GlobalExtraData,
    );
    fn process_group_batch_sequences(
        &mut self,
        global_data: &F::GlobalExtraData,
        batch: &ReadsVector<F::AssociatedExtraData>,
        extra_data_buffer: &<F::AssociatedExtraData as SequenceExtraDataTempBufferManagement>::TempBuffer,
        ref_sequences: &Vec<u8>,
    );
    fn get_stats(&self) -> GroupProcessStats;
    fn process_group_finalize(
        &mut self,
        global_data: &F::GlobalExtraData,
    ) -> Packet<Self::MapStruct>;
}

pub trait KmersTransformFinalExecutor<F: KmersTransformExecutorFactory>:
    Sized + 'static + Sync + Send
{
    type MapStruct: PacketTrait + PoolObjectTrait<InitData = F::KmersTransformPacketInitData>;

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
    max_buckets: usize,
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

    total_sequences: AtomicU64,
    total_kmers: AtomicU64,
    unique_kmers: AtomicU64,

    reader_init_lock: tokio::sync::Mutex<()>,
}

impl<F: KmersTransformExecutorFactory> KmersTransform<F> {
    pub fn new(
        file_inputs: Vec<MultiChunkBucket>,
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

        let mut file_batches_with_sizes: Vec<_> = file_inputs
            .into_iter()
            .map(|f| {
                let total_file_size: usize = f
                    .chunks
                    .iter()
                    .map(|f| MemoryFs::get_file_size(&f).unwrap_or(0))
                    .sum();
                total_buckets_size += total_file_size;
                (f, total_file_size)
            })
            .collect();

        file_batches_with_sizes.sort_by_key(|x| x.1);
        file_batches_with_sizes.reverse();

        let normal_buckets_list = {
            let mut buckets_list = Vec::with_capacity(file_batches_with_sizes.len());
            let mut start_idx = 0;
            let mut end_idx = file_batches_with_sizes.len();

            let mut matched_size = 0i64;

            let mut unique_estimator_buckets_count = min(buckets_count / 8, threads_count * 2);

            while start_idx != end_idx && unique_estimator_buckets_count > 0 {
                end_idx -= 1;
                unique_estimator_buckets_count -= 1;
                let bucket_entries = file_batches_with_sizes[end_idx].0.clone();

                buckets_list.push(InputBucketDesc {
                    paths: bucket_entries.chunks,
                    sub_bucket_counters: counters
                        .get_counters_for_bucket(bucket_entries.index as BucketIndexType)
                        .clone(),
                    compaction_delta: counters
                        .get_compaction_offset(bucket_entries.index as BucketIndexType),
                    resplitted: false,
                    rewritten: false,
                    used_hash_bits: buckets_count.ilog2() as usize,
                    out_data_format: if bucket_entries.was_compacted {
                        MinimizerBucketMode::Compacted
                    } else {
                        MinimizerBucketMode::Single
                    },
                });
            }

            while start_idx != end_idx {
                let bucket_entry = if matched_size <= 0 {
                    let target_file = &file_batches_with_sizes[start_idx];
                    let entry = target_file.0.clone();
                    matched_size = target_file.1 as i64;
                    start_idx += 1;
                    entry
                } else {
                    let target_file = &file_batches_with_sizes[end_idx - 1];
                    let entry = target_file.0.clone();
                    matched_size -= target_file.1 as i64;
                    end_idx -= 1;
                    entry
                };

                buckets_list.push(InputBucketDesc {
                    paths: bucket_entry.chunks,
                    sub_bucket_counters: counters
                        .get_counters_for_bucket(bucket_entry.index as BucketIndexType)
                        .clone(),
                    compaction_delta: counters
                        .get_compaction_offset(bucket_entry.index as BucketIndexType),
                    resplitted: false,
                    rewritten: false,
                    used_hash_bits: buckets_count.ilog2() as usize,
                    out_data_format: if bucket_entry.was_compacted {
                        MinimizerBucketMode::Compacted
                    } else {
                        MinimizerBucketMode::Single
                    },
                })
            }
            buckets_list
        };

        let compute_threads_count = max(1, threads_count);
        let read_threads_count = max(1, threads_count / 4 * 3);

        let max_buckets = max(MAXIMUM_SECOND_BUCKETS_COUNT, compute_threads_count);

        let execution_context = Arc::new(KmersTransformContext {
            k,
            min_bucket_size,
            buckets_count,
            max_buckets,
            extra_buckets_count: AtomicUsize::new(0),
            rewritten_buckets_count: AtomicUsize::new(0),
            processed_buckets_count: AtomicUsize::new(0),
            processed_extra_buckets_count: AtomicUsize::new(0),
            total_buckets_size,
            processed_buckets_size: AtomicUsize::new(0),
            processed_extra_buckets_size: AtomicUsize::new(0),
            global_extra_data,
            compute_threads_count,
            read_threads_count,
            max_second_buckets_count_log2: MAXIMUM_SECOND_BUCKETS_COUNT.ilog2() as usize,
            temp_dir: temp_dir.to_path_buf(),
            total_sequences: AtomicU64::new(0),
            total_kmers: AtomicU64::new(0),
            unique_kmers: AtomicU64::new(0),
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
                // The capacity of each packets pool
                capacity: MAX_KMERS_TRANSFORM_READERS_PER_BUCKET * 2,
            },
            max(
                16,
                KMERS_TRANSFORM_READS_CHUNKS_SIZE / self.global_context.k,
            ),
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
            self.maybe_log_completed_buckets(|| {});
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

            ggcat_logging::info!(
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
