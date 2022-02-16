mod process_subbucket;
pub mod structs;

use crate::config::{
    BucketIndexType, SortingHashType, DEFAULT_PER_CPU_BUFFER_SIZE, FIRST_BUCKETS_COUNT,
    SECOND_BUCKETS_COUNT,
};
use crate::io::concurrent::intermediate_storage::SequenceExtraData;
use crate::pipeline_common::minimizer_bucketing::MinimizerBucketingExecutorFactory;
use crate::utils::compressed_read::CompressedRead;
use crossbeam::queue::{ArrayQueue, SegQueue};
use parallel_processor::memory_fs::file::reader::FileReader;
use parallel_processor::memory_fs::MemoryFs;
use parallel_processor::phase_times_monitor::PHASES_TIMES_MONITOR;
use parking_lot::{Condvar, Mutex, RwLock};
use std::cmp::min;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use parallel_processor::buckets::concurrent::BucketsThreadDispatcher;
use structs::ReadRef;

pub struct ReadDispatchInfo<E: SequenceExtraData> {
    pub bucket: BucketIndexType,
    pub hash: SortingHashType,
    pub flags: u8,
    pub extra_data: E,
}

pub trait KmersTransformExecutorFactory: Sized + 'static {
    type SequencesResplitterFactory: MinimizerBucketingExecutorFactory<
        ExtraData = Self::AssociatedExtraData,
    >;
    type GlobalExtraData<'a>: Send + Sync + 'a;
    type AssociatedExtraData: SequenceExtraData;
    type ExecutorType<'a>: KmersTransformExecutor<'a, Self>;

    #[allow(non_camel_case_types)]
    type FLAGS_COUNT: typenum::uint::Unsigned;

    fn new_resplitter<'a, 'b: 'a>(
        global_data: &'a Self::GlobalExtraData<'b>,
    ) -> <Self::SequencesResplitterFactory as MinimizerBucketingExecutorFactory>::ExecutorType<'a>;

    fn new<'a>(global_data: &Self::GlobalExtraData<'a>) -> Self::ExecutorType<'a>;
}

pub trait KmersTransformExecutor<'x, F: KmersTransformExecutorFactory> {
    fn preprocess_bucket(
        &mut self,
        global_data: &F::GlobalExtraData<'x>,
        flags: u8,
        input_extra_data: F::AssociatedExtraData,
        read: CompressedRead,
    ) -> ReadDispatchInfo<F::AssociatedExtraData>;

    fn maybe_swap_bucket(&mut self, global_data: &F::GlobalExtraData<'x>);

    fn process_group(&mut self, global_data: &F::GlobalExtraData<'x>, stream: FileReader);

    fn finalize(self, global_data: &F::GlobalExtraData<'x>);
}

pub struct KmersTransform;

impl KmersTransform {
    pub fn parallel_kmers_transform<'a, F: KmersTransformExecutorFactory>(
        file_inputs: Vec<PathBuf>,
        buckets_count: usize,
        threads_count: usize,
        global_extra_data: F::GlobalExtraData<'a>,
        save_memory: bool,
    ) {
        let files_queue = ArrayQueue::new(file_inputs.len());

        let mut buckets_total_size = 0;

        file_inputs.into_iter().for_each(|f| {
            buckets_total_size += MemoryFs::get_file_size(&f).unwrap_or(0);
            files_queue.push(f).unwrap()
        });

        let typical_sub_bucket_size =
            buckets_total_size / (FIRST_BUCKETS_COUNT * SECOND_BUCKETS_COUNT);

        let vecs_process_queue = Arc::new(SegQueue::new());

        let last_info_log = Mutex::new(Instant::now());

        const MINIMUM_LOG_DELTA_TIME: Duration = Duration::from_secs(15);

        let data_wait_condvar = Arc::new(Condvar::new());
        let data_wait_mutex = Mutex::new(());

        let opened_buckets_count = Arc::new(AtomicUsize::new(0));

        let max_opened_buckets = if save_memory { 1 } else { threads_count / 2 };

        let open_bucket = || {
            while opened_buckets_count.load(Ordering::SeqCst) >= max_opened_buckets {
                data_wait_condvar.wait_for(&mut data_wait_mutex.lock(), Duration::from_millis(100));
            }

            let file = files_queue.pop()?;

            let mut last_info_log = last_info_log.lock();
            if last_info_log.elapsed() > MINIMUM_LOG_DELTA_TIME {
                let monitor = PHASES_TIMES_MONITOR.read();

                let processed_count = buckets_count - files_queue.len();

                let eta = Duration::from_secs(
                    (monitor.get_phase_timer().as_secs_f64() / (processed_count as f64)
                        * (files_queue.len() as f64)) as u64,
                );

                let est_tot = Duration::from_secs(
                    (monitor.get_phase_timer().as_secs_f64() / (processed_count as f64)
                        * (buckets_count as f64)) as u64,
                );

                println!(
                    "Processing bucket {} of {} {} phase eta: {:.0?} est.tot: {:.0?}",
                    processed_count,
                    buckets_count,
                    monitor.get_formatted_counter_without_memory(),
                    eta,
                    est_tot
                );
                *last_info_log = Instant::now();
            }

            Some(Arc::new(
                structs::BucketProcessData::<F::AssociatedExtraData>::new(
                    file,
                    vecs_process_queue.clone(),
                    opened_buckets_count.clone(),
                    data_wait_condvar.clone(),
                ),
            ))
        };

        let current_bucket = RwLock::new(open_bucket());
        let reading_finished = AtomicBool::new(false);

        crossbeam::thread::scope(|s| {
            for _ in 0..min(buckets_count, threads_count) {
                s.builder()
                    .name("kmers-transform".to_string())
                    .spawn(|_| {
                        let mut executor = F::new(&global_extra_data);

                        let process_pending_reads = |executor: &mut F::ExecutorType<'a>| {
                            while let Some((queue_path, can_resplit)) = vecs_process_queue.pop() {
                                let mut reader = FileReader::open(&queue_path).unwrap();

                                executor.maybe_swap_bucket(&global_extra_data);

                                let is_outlier =
                                    reader.total_file_size() > typical_sub_bucket_size * 512;

                                process_subbucket::process_subbucket::<F>(
                                    &global_extra_data,
                                    reader,
                                    executor,
                                    queue_path.as_ref(),
                                    &vecs_process_queue,
                                    can_resplit,
                                    is_outlier,
                                );
                            }
                        };

                        'outer_loop: loop {
                            process_pending_reads(&mut executor);

                            if reading_finished.load(Ordering::SeqCst) {
                                break 'outer_loop;
                            }
                            let bucket = {
                                let current_bucket_read_guard = current_bucket.read();

                                match current_bucket_read_guard.clone() {
                                    None => {
                                        drop(current_bucket_read_guard);
                                        data_wait_condvar.wait_for(
                                            &mut data_wait_mutex.lock(),
                                            Duration::from_millis(100),
                                        );
                                        continue;
                                    }
                                    Some(val) => val,
                                }
                            };

                            let mut cmp_reads = BucketsThreadDispatcher::new(
                                DEFAULT_PER_CPU_BUFFER_SIZE,
                                &bucket.buckets,
                            );

                            let mut continue_read = true;
                            let mut tmp_data = Vec::with_capacity(256);

                            while continue_read {
                                process_pending_reads(&mut executor);

                                continue_read = bucket.reader.read_parallel::<_, F::FLAGS_COUNT>(
                                    |flags, read_extra_data, read| {
                                        let preprocess_info = executor.preprocess_bucket(
                                            &global_extra_data,
                                            flags,
                                            read_extra_data,
                                            read,
                                        );

                                        let packed_slice = ReadRef::pack::<_, F::FLAGS_COUNT>(
                                            preprocess_info.flags,
                                            read,
                                            &preprocess_info.extra_data,
                                            &mut tmp_data,
                                        );

                                        cmp_reads.add_element(
                                            preprocess_info.bucket,
                                            &(),
                                            packed_slice,
                                        );
                                    },
                                );
                            }

                            cmp_reads.finalize();

                            drop(bucket);

                            let mut writable_bucket = current_bucket.write();
                            if writable_bucket
                                .as_ref()
                                .map(|x| {
                                    x.reader.is_finished()
                                        && (!save_memory || (Arc::strong_count(x) == 1))
                                })
                                .unwrap_or(false)
                            {
                                writable_bucket.take();
                            }

                            drop(writable_bucket);

                            loop {
                                process_pending_reads(&mut executor);

                                writable_bucket = current_bucket.write();
                                if vecs_process_queue.len() > 0 {
                                    drop(writable_bucket);
                                    continue;
                                }

                                if writable_bucket.is_none() {
                                    if let Some(bucket) = open_bucket() {
                                        *writable_bucket = Some(bucket);
                                    } else if opened_buckets_count.load(Ordering::SeqCst) == 0 {
                                        reading_finished.store(true, Ordering::SeqCst);
                                    }
                                    data_wait_condvar.notify_all();
                                }
                                break;
                            }
                        }
                        executor.finalize(&global_extra_data);
                    })
                    .unwrap();
            }
        })
        .unwrap();
    }
}
