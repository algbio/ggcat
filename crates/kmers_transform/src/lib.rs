use crate::processor::{KmersProcessorInitData, KmersTransformProcessor, ResplitConfig};
use config::{
    DEFAULT_PREFETCH_AMOUNT, KEEP_FILES, MAX_RESPLIT_BUCKETS_COUNT,
    MAX_SUBBUCKET_AVERAGE_MULTIPLIER, MIN_AVERAGE_CAP, MIN_RESPLIT_BUCKETS_COUNT,
    MINIMUM_LOG_DELTA_TIME,
};
use ggcat_logging::generate_stat_id;
use io::DUPLICATES_BUCKET_EXTRA;
use io::concurrent::temp_reads::creads_utils::DeserializedRead;
use io::concurrent::temp_reads::extra_data::{
    SequenceExtraDataCombiner, SequenceExtraDataConsecutiveCompression,
    SequenceExtraDataTempBufferManagement,
};
use minimizer_bucketing::MinimizerBucketingExecutorFactory;
use minimizer_bucketing::resplit_bucket::RewriteBucketCompute;
use minimizer_bucketing::split_buckets::SplittedBucket;

use parallel_processor::buckets::{BucketsCount, ExtraBucketData, ExtraBuckets, MultiChunkBucket};
use parallel_processor::execution_manager::objects_pool::PoolObjectTrait;
use parallel_processor::execution_manager::packet::{Packet, PacketTrait};
use parallel_processor::execution_manager::scheduler::Scheduler;
use parallel_processor::execution_manager::thread_pool::ExecThreadPool;
use parallel_processor::memory_fs::{MemoryFs, RemoveFileMode};
use parallel_processor::phase_times_monitor::PHASES_TIMES_MONITOR;
use parking_lot::Mutex;
use std::cmp::{max, min};
use std::fmt::Debug;
use std::marker::PhantomData;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

pub mod debug_bucket_stats;
pub mod processor;
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
        buckets_count: &BucketsCount,
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
    fn process_group_add_sequence(
        &mut self,
        read: &DeserializedRead<'_, F::AssociatedExtraDataWithMultiplicity>,
        extra_data_buffer: &<F::AssociatedExtraDataWithMultiplicity as SequenceExtraDataTempBufferManagement>::TempBuffer,
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

pub struct InputBucketDesc {
    pub(crate) paths: Vec<PathBuf>,
    pub(crate) extra_bucket_data: Option<ExtraBucketData>,
}

pub struct KmersTransform<F: KmersTransformExecutorFactory> {
    global_context: Arc<KmersTransformContext<F>>,
    normal_buckets_list: Vec<InputBucketDesc>,
    last_info_log: Mutex<Instant>,
    _phantom: PhantomData<F>,
}

pub struct KmersTransformContext<F: KmersTransformExecutorFactory> {
    k: usize,
    buckets_count: BucketsCount,
    second_buckets_count: BucketsCount,
    extra_buckets_count: AtomicUsize,
    processed_subbuckets_count: AtomicUsize,
    processed_extra_buckets_count: AtomicUsize,

    total_buckets_size: usize,
    processed_buckets_size: AtomicUsize,
    processed_extra_buckets_size: AtomicUsize,

    global_extra_data: Arc<F::GlobalExtraData>,
    compute_threads_count: usize,
    total_threads_count: usize,
    temp_dir: PathBuf,

    total_sequences: AtomicU64,
    total_kmers: AtomicU64,
    unique_kmers: AtomicU64,
}

impl<F: KmersTransformExecutorFactory> KmersTransform<F> {
    pub fn new(
        file_inputs: Vec<MultiChunkBucket>,
        temp_dir: &Path,
        buckets_count: BucketsCount,
        second_buckets_count: BucketsCount,
        global_extra_data: Arc<F::GlobalExtraData>,
        threads_count: usize,
        k: usize,
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
                    extra_bucket_data: bucket_entry.extra_bucket_data,
                })
            }
            buckets_list
        };

        let compute_threads_count = max(1, threads_count);

        let execution_context = Arc::new(KmersTransformContext {
            k,
            buckets_count,
            second_buckets_count,
            extra_buckets_count: AtomicUsize::new(0),
            processed_subbuckets_count: AtomicUsize::new(0),
            processed_extra_buckets_count: AtomicUsize::new(0),
            total_buckets_size,
            processed_buckets_size: AtomicUsize::new(0),
            processed_extra_buckets_size: AtomicUsize::new(0),
            global_extra_data,
            compute_threads_count,
            total_threads_count: threads_count,
            temp_dir: temp_dir.to_path_buf(),
            total_sequences: AtomicU64::new(0),
            total_kmers: AtomicU64::new(0),
            unique_kmers: AtomicU64::new(0),
        });

        Self {
            global_context: execution_context,
            normal_buckets_list,
            last_info_log: Mutex::new(Instant::now()),
            _phantom: Default::default(),
        }
    }

    pub fn parallel_kmers_transform(self) {
        let compute_threads_count = self.global_context.compute_threads_count;

        let scheduler = Scheduler::new(self.global_context.total_threads_count);

        let mut compute_thread_pool = ExecThreadPool::<KmersTransformProcessor<F>>::new(
            compute_threads_count,
            "km_comp",
            false,
        );

        let compute_thread_pool_handle =
            compute_thread_pool.start(scheduler.clone(), &self.global_context);

        let mut total_subbuckets_sequences = 0;
        let mut total_subbuckets_count = 0;

        for bucket in self.normal_buckets_list.iter() {
            let splitted_buckets = SplittedBucket::generate(
                bucket.paths.iter(),
                RemoveFileMode::Remove {
                    remove_fs: !KEEP_FILES.load(Ordering::Relaxed),
                },
                DEFAULT_PREFETCH_AMOUNT,
                self.global_context.second_buckets_count.total_buckets_count,
            );

            for bucket in &splitted_buckets {
                if let Some(bucket) = bucket {
                    total_subbuckets_sequences += bucket.sequences_count;
                    total_subbuckets_count += 1;
                }
            }

            let bucket_sequences_average =
                (total_subbuckets_sequences / total_subbuckets_count.max(1)).max(MIN_AVERAGE_CAP);

            for splitted_bucket in splitted_buckets.into_iter() {
                let Some(splitted_bucket) = splitted_bucket else {
                    // Add the sub-bucket to the global counter
                    self.global_context
                        .processed_subbuckets_count
                        .fetch_add(1, Ordering::Relaxed);
                    continue;
                };

                let is_outlier = splitted_bucket.sequences_count
                    > bucket_sequences_average * MAX_SUBBUCKET_AVERAGE_MULTIPLIER;

                // Add the sub-bucket job
                compute_thread_pool_handle.create_new_address_with_limit(
                    Arc::new(KmersProcessorInitData {
                        process_stat_id: generate_stat_id!(),
                        is_resplitted: false,
                        resplit_config: if is_outlier {
                            let subbuckets_count = (splitted_bucket.sequences_count
                                / bucket_sequences_average)
                                .next_power_of_two()
                                .max(MIN_RESPLIT_BUCKETS_COUNT)
                                .min(MAX_RESPLIT_BUCKETS_COUNT);

                            println!(
                                "Resplitted with {} buckets average: {}!",
                                subbuckets_count, bucket_sequences_average
                            );
                            self.global_context
                                .extra_buckets_count
                                .fetch_add(subbuckets_count, Ordering::Relaxed);

                            Some(ResplitConfig {
                                subsplit_buckets_count: BucketsCount::new(
                                    subbuckets_count.ilog2() as usize,
                                    ExtraBuckets::Extra {
                                        count: 1,
                                        data: DUPLICATES_BUCKET_EXTRA,
                                    },
                                ),
                            })
                        } else {
                            None
                        },
                        splitted_bucket: Mutex::new(Some(splitted_bucket)),
                        debug_bucket_first_path: Some(bucket.paths[0].clone()),
                        extra_bucket_data: bucket.extra_bucket_data,
                        processor_handle: compute_thread_pool_handle.clone(),
                    }),
                    false,
                    self.global_context.total_threads_count * 8,
                );
            }

            self.maybe_log_completed_buckets(|| {}, &scheduler);
        }

        // Log progress info while waiting for completion
        while compute_thread_pool_handle.get_pending_executors_count() > 0 {
            self.maybe_log_completed_buckets(|| {}, &scheduler);
            std::thread::sleep(Duration::from_millis(300));
        }

        drop(compute_thread_pool_handle);

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

            let processed_count = max(
                1,
                self.global_context
                    .processed_subbuckets_count
                    .load(Ordering::Relaxed)
                    / self.global_context.second_buckets_count.total_buckets_count,
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
                "Processing bucket {}{} of [{}{}] {} phase eta: {:.0?} est. tot: {:.0?} running: {}",
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
                monitor.get_formatted_counter_without_memory(),
                if eta_processed_size > 0 {
                    &eta as &dyn Debug
                } else {
                    &"N/A" as &dyn Debug
                },
                est_tot,
                scheduler.current_running()
            );
            true
        } else {
            false
        }
    }
}
