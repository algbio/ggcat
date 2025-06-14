mod reader;

use crate::processor::KmersTransformProcessor;
use crate::reader::{InputBucketDesc, KmersTransformReader, TransformReaderContext};
use crate::resplitter::KmersTransformResplitter;
use config::{MAXIMUM_SECOND_BUCKETS_COUNT, MINIMUM_LOG_DELTA_TIME};
use io::concurrent::temp_reads::extra_data::{
    SequenceExtraDataCombiner, SequenceExtraDataConsecutiveCompression,
    SequenceExtraDataTempBufferManagement,
};
use minimizer_bucketing::resplit_bucket::RewriteBucketCompute;
use minimizer_bucketing::{MinimizerBucketMode, MinimizerBucketingExecutorFactory};
use parallel_processor::buckets::{BucketsCount, MultiChunkBucket};
use parallel_processor::execution_manager::objects_pool::PoolObjectTrait;
use parallel_processor::execution_manager::packet::{Packet, PacketTrait};
use parallel_processor::execution_manager::scheduler::Scheduler;
use parallel_processor::execution_manager::thread_pool::ExecThreadPool;
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
        ReadExtraData = Self::AssociatedExtraDataWithMultiplicity,
    >;
    type GlobalExtraData: KmersTransformGlobalExtraData;
    type AssociatedExtraData: SequenceExtraDataConsecutiveCompression + Copy;
    type AssociatedExtraDataWithMultiplicity: SequenceExtraDataConsecutiveCompression
        + SequenceExtraDataCombiner<SingleDataType = Self::AssociatedExtraData>
        + Copy;
    type PreprocessorType: RewriteBucketCompute;
    type MapProcessorType: KmersTransformMapProcessor<
            Self,
            MapStruct = <Self::FinalExecutorType as KmersTransformFinalExecutor<Self>>::MapStruct,
        >;
    type FinalExecutorType: KmersTransformFinalExecutor<Self>;

    type FlagsCount: typenum::uint::Unsigned;

    const HAS_COLORS: bool;

    fn get_packets_init_data(
        global_data: &Arc<Self::GlobalExtraData>,
    ) -> Self::KmersTransformPacketInitData;

    fn new_resplitter(
        global_data: &Arc<Self::GlobalExtraData>,
        duplicates_bucket: u16,
    ) -> <Self::SequencesResplitterFactory as MinimizerBucketingExecutorFactory>::ExecutorType;

    fn new_map_processor(global_data: &Arc<Self::GlobalExtraData>) -> Self::MapProcessorType;
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
        batch: &ReadsVector<F::AssociatedExtraDataWithMultiplicity>,
        extra_data_buffer: &<F::AssociatedExtraDataWithMultiplicity as SequenceExtraDataTempBufferManagement>::TempBuffer,
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
    buckets_count: BucketsCount,
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
    total_threads_count: usize,
    max_second_buckets_count_log2: usize,
    temp_dir: PathBuf,

    total_sequences: AtomicU64,
    total_kmers: AtomicU64,
    unique_kmers: AtomicU64,

    executors_register_lock: Mutex<()>,
}

impl<F: KmersTransformExecutorFactory> KmersTransform<F> {
    pub fn new(
        file_inputs: Vec<MultiChunkBucket>,
        temp_dir: &Path,
        buckets_count: BucketsCount,
        global_extra_data: Arc<F::GlobalExtraData>,
        threads_count: usize,
        k: usize,
        min_bucket_size: u64,
        compacted_buckets: bool,
    ) -> Self {
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

        // Reorder the buckets so that large buckets are alternated with each large bucket followed some small ones that match it in size
        // Don't do that with the first batch of processing (up to unique_estimator_buckets_count)

        let normal_buckets_list = {
            let mut buckets_list = Vec::with_capacity(file_batches_with_sizes.len());
            let mut start_idx = 0;
            let mut end_idx = file_batches_with_sizes.len();

            let mut matched_size = 0i64;

            let mut unique_estimator_buckets_count =
                min(buckets_count.normal_buckets_count / 8, threads_count * 2);

            while start_idx != end_idx && unique_estimator_buckets_count > 0 {
                end_idx -= 1;
                unique_estimator_buckets_count -= 1;
                let bucket_entries = file_batches_with_sizes[end_idx].0.clone();

                buckets_list.push(InputBucketDesc {
                    paths: bucket_entries.chunks,
                    resplitted: false,
                    rewritten: false,
                    used_hash_bits: buckets_count.normal_buckets_count_log,
                    out_data_format: if compacted_buckets {
                        MinimizerBucketMode::Compacted
                    } else {
                        MinimizerBucketMode::Single
                    },
                    extra_bucket_data: bucket_entries.extra_bucket_data,
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
                    resplitted: false,
                    rewritten: false,
                    used_hash_bits: buckets_count.normal_buckets_count_log as usize,
                    out_data_format: if compacted_buckets {
                        MinimizerBucketMode::Compacted
                    } else {
                        MinimizerBucketMode::Single
                    },
                    extra_bucket_data: bucket_entry.extra_bucket_data,
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
            total_threads_count: threads_count,
            max_second_buckets_count_log2: MAXIMUM_SECOND_BUCKETS_COUNT.ilog2() as usize,
            temp_dir: temp_dir.to_path_buf(),
            total_sequences: AtomicU64::new(0),
            total_kmers: AtomicU64::new(0),
            unique_kmers: AtomicU64::new(0),
            executors_register_lock: Mutex::new(()),
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

        let scheduler = Scheduler::new(self.global_context.total_threads_count);

        let mut disk_thread_pool =
            ExecThreadPool::<KmersTransformReader<F>>::new(read_threads_count, "km_disk", true);

        let mut compute_thread_pool = ExecThreadPool::<KmersTransformProcessor<F>>::new(
            compute_threads_count,
            "km_comp",
            false,
        );

        let mut resplit_thread_pool = ExecThreadPool::<KmersTransformResplitter<F>>::new(
            compute_threads_count,
            "km_respl",
            true,
        );

        let compute_thread_pool_handle =
            compute_thread_pool.start(scheduler.clone(), &self.global_context);

        let resplit_thread_pool_handle =
            resplit_thread_pool.start(scheduler.clone(), &self.global_context);

        let disk_thread_pool_handle = disk_thread_pool.start(
            scheduler.clone(),
            &Arc::new(TransformReaderContext {
                resplit_handle: resplit_thread_pool_handle.clone(),
                process_handle: compute_thread_pool_handle.clone(),
                global: self.global_context.clone(),
            }),
        );

        disk_thread_pool_handle.add_input_data(
            disk_thread_pool_handle.clone(),
            std::mem::take(&mut self.normal_buckets_list).into_iter(),
        );

        // Log progress info while waiting for completion
        let mut jobs_count;
        while {
            jobs_count = compute_thread_pool_handle.get_pending_executors_count()
                + disk_thread_pool_handle.get_pending_executors_count()
                + resplit_thread_pool_handle.get_pending_executors_count();
            jobs_count > 0
        } {
            self.maybe_log_completed_buckets(|| {}, &scheduler);
            std::thread::sleep(Duration::from_millis(300));
        }

        drop(disk_thread_pool_handle);
        drop(resplit_thread_pool_handle);
        drop(compute_thread_pool_handle);

        // Wait for the main buckets to be processed
        disk_thread_pool.join();

        // Wait for the resplitting to be complete
        resplit_thread_pool.join();

        // Wait for the maps to be complete
        compute_thread_pool.join();
    }

    fn maybe_log_completed_buckets(
        &self,
        extra_debug: impl FnOnce(),
        scheduler: &Scheduler,
    ) -> bool {
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
                "Processing bucket {}{} of [{}{}[R:{}]] {} phase eta: {:.0?} est. tot: {:.0?} running: {}",
                processed_count,
                if extra_processed_buckets_count > 0 {
                    format!("(+{})", extra_processed_buckets_count)
                } else {
                    String::new()
                },
                buckets_count.total_buckets_count,
                if extra_buckets_count > 0 {
                    format!("(+{})", extra_buckets_count)
                } else {
                    String::new()
                },
                rewritten_buckets_count,
                monitor.get_formatted_counter_without_memory(),
                eta,
                est_tot,
                scheduler.current_running()
            );
            true
        } else {
            false
        }
    }
}
