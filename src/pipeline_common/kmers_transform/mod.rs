mod process_subbucket;
pub mod structs;

use crate::config::{
    BucketIndexType, SortingHashType, DEFAULT_PER_CPU_BUFFER_SIZE, FIRST_BUCKETS_COUNT,
    MINIMUM_LOG_DELTA_TIME, SECOND_BUCKETS_COUNT,
};
use crate::io::concurrent::temp_reads::creads_utils::CompressedReadsBucketHelper;
use crate::io::concurrent::temp_reads::extra_data::SequenceExtraData;
use crate::pipeline_common::kmers_transform::structs::{BucketProcessData, ProcessQueueItem};
use crate::pipeline_common::minimizer_bucketing::MinimizerBucketingExecutorFactory;
use crate::utils::compressed_read::CompressedRead;
use crate::utils::resource_counter::ResourceCounter;
use crossbeam::queue::{ArrayQueue, SegQueue};
use parallel_processor::buckets::concurrent::BucketsThreadDispatcher;
use parallel_processor::buckets::readers::lock_free_binary_reader::LockFreeBinaryReader;
use parallel_processor::memory_fs::file::reader::FileReader;
use parallel_processor::memory_fs::{MemoryFs, RemoveFileMode};
use parallel_processor::phase_times_monitor::PHASES_TIMES_MONITOR;
use parking_lot::{Condvar, Mutex, RwLock};
use std::cmp::{max, min};
use std::marker::PhantomData;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Weak};
use std::time::{Duration, Instant};
use structs::ReadRef;

pub struct ReadDispatchInfo<E: SequenceExtraData> {
    pub bucket: BucketIndexType,
    pub hash: SortingHashType,
    pub flags: u8,
    pub extra_data: E,
}

pub trait KmersTransformExecutorFactory: Sized + 'static + Sync + Send {
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
    fn preprocess_bucket<'y: 'x>(
        &mut self,
        global_data: &F::GlobalExtraData<'y>,
        flags: u8,
        input_extra_data: F::AssociatedExtraData,
        read: CompressedRead,
    ) -> ReadDispatchInfo<F::AssociatedExtraData>;

    fn maybe_swap_bucket<'y: 'x>(&mut self, global_data: &F::GlobalExtraData<'y>);

    fn process_group<'y: 'x>(
        &mut self,
        global_data: &F::GlobalExtraData<'y>,
        reader: LockFreeBinaryReader,
    );

    fn finalize<'y: 'x>(self, global_data: &F::GlobalExtraData<'y>);
}

pub struct KmersTransform<'a, F: KmersTransformExecutorFactory> {
    buckets_count: usize,
    buckets_total_size: u64,
    buffer_files_counter: Arc<ResourceCounter>,

    global_extra_data: F::GlobalExtraData<'a>,

    files_queue: ArrayQueue<PathBuf>,

    current_bucket: RwLock<Weak<BucketProcessData>>,

    process_queue: Arc<SegQueue<ProcessQueueItem>>,
    reprocess_queue: Arc<SegQueue<u8>>,

    last_info_log: Mutex<Instant>,
    _phantom: PhantomData<F>,
}

impl<'a, F: KmersTransformExecutorFactory> KmersTransform<'a, F> {
    pub fn new(
        file_inputs: Vec<PathBuf>,
        buckets_count: usize,
        extra_buffers_count: usize,
        threads_count: usize,
        global_extra_data: F::GlobalExtraData<'a>,
    ) -> Self {
        let files_queue = ArrayQueue::new(file_inputs.len());

        let mut buckets_total_size = 0;

        file_inputs.into_iter().for_each(|f| {
            buckets_total_size += MemoryFs::get_file_size(&f).unwrap_or(0);
            files_queue.push(f).unwrap()
        });

        Self {
            buckets_count,
            buckets_total_size: 0,
            buffer_files_counter: ResourceCounter::new(
                (SECOND_BUCKETS_COUNT + extra_buffers_count) as u64,
            ),
            global_extra_data,
            files_queue,
            current_bucket: RwLock::new(Weak::new()),
            process_queue: Arc::new(SegQueue::new()),
            reprocess_queue: Arc::new(SegQueue::new()),
            last_info_log: Mutex::new(Instant::now()),
            _phantom: Default::default(),
        }
    }

    fn get_current_bucket(&self) -> Option<Arc<BucketProcessData>> {
        fn get_valid_bucket(bucket: &Weak<BucketProcessData>) -> Option<Arc<BucketProcessData>> {
            if let Some(bucket) = bucket.upgrade() {
                if !bucket.reader.is_finished() {
                    return Some(bucket);
                }
            }
            return None;
        }

        loop {
            let bucket = self.current_bucket.read();

            if let Some(bucket) = get_valid_bucket(&bucket) {
                return Some(bucket);
            }

            drop(bucket);
            let mut bucket = self.current_bucket.write();

            if let Some(bucket) = get_valid_bucket(&bucket) {
                return Some(bucket);
            }

            let file = self.files_queue.pop()?;

            let new_bucket = Arc::new(structs::BucketProcessData::new_blocking(
                file,
                self.process_queue.clone(),
                self.buffer_files_counter.clone(),
            ));

            *bucket = Arc::downgrade(&new_bucket);

            let mut last_info_log = self.last_info_log.lock();
            if last_info_log.elapsed() > MINIMUM_LOG_DELTA_TIME {
                let monitor = PHASES_TIMES_MONITOR.read();

                let processed_count = self.buckets_count - self.files_queue.len();

                let eta = Duration::from_secs(
                    (monitor.get_phase_timer().as_secs_f64() / (processed_count as f64)
                        * (self.files_queue.len() as f64)) as u64,
                );

                let est_tot = Duration::from_secs(
                    (monitor.get_phase_timer().as_secs_f64() / (processed_count as f64)
                        * (self.buckets_count as f64)) as u64,
                );

                println!(
                    "Processing bucket {} of {} {} phase eta: {:.0?} est.tot: {:.0?}",
                    processed_count,
                    self.buckets_count,
                    monitor.get_formatted_counter_without_memory(),
                    eta,
                    est_tot
                );
                *last_info_log = Instant::now();
            }
            drop(last_info_log);

            return Some(new_bucket);
        }
    }

    fn read_bucket(&self, executor: &mut F::ExecutorType<'a>, bucket: &BucketProcessData) {
        let mut continue_read = true;

        if bucket.reader.is_finished() {
            return;
        }

        // TODO: Reuse the dispatcher
        let mut cmp_reads =
            BucketsThreadDispatcher::new(DEFAULT_PER_CPU_BUFFER_SIZE, &bucket.buckets);

        while continue_read {
            continue_read = bucket.reader.decode_bucket_items_parallel::<CompressedReadsBucketHelper<F::AssociatedExtraData, F::FLAGS_COUNT>, _>(Vec::new(), |(flags, read_extra_data, read)| {
                    let preprocess_info = executor.preprocess_bucket(
                        &self.global_extra_data,
                        flags,
                        read_extra_data,
                        read,
                    );

                    cmp_reads.add_element(
                        preprocess_info.bucket,
                        &preprocess_info.extra_data,
                        &ReadRef::<_, F::FLAGS_COUNT> {
                            flags,
                            read,
                            _phantom: Default::default()
                        },
                    );
                },
            );
        }
    }

    fn process_buffers(&self, executor: &mut F::ExecutorType<'a>, typical_sub_bucket_size: usize) {
        while let Some(ProcessQueueItem {
            ref path,
            ref can_resplit,
            ..
        }) = self.process_queue.pop()
        {
            let mut reader =
                LockFreeBinaryReader::new(path, RemoveFileMode::Remove { remove_fs: true });

            executor.maybe_swap_bucket(&self.global_extra_data);

            let is_outlier = false; //reader > typical_sub_bucket_size * 512;

            process_subbucket::process_subbucket::<F>(
                &self.global_extra_data,
                reader,
                executor,
                path,
                *can_resplit,
                is_outlier,
            );
        }
    }

    pub fn parallel_kmers_transform(&self, threads_count: usize) {
        let typical_sub_bucket_size =
            self.buckets_total_size as usize / (FIRST_BUCKETS_COUNT * SECOND_BUCKETS_COUNT);

        crossbeam::thread::scope(|s| {
            for _ in 0..min(self.buckets_count, threads_count) {
                s.builder()
                    .name("kmers-transform".to_string())
                    .spawn(|_| {
                        let mut executor = F::new(&self.global_extra_data);
                        loop {
                            self.process_buffers(&mut executor, typical_sub_bucket_size);

                            // if resplit_all(&gdata, &mut queue, &mut rqueue) > 0 {
                            //     continue;
                            // }

                            let bucket = match self.get_current_bucket() {
                                None => break,
                                Some(x) => x,
                            };

                            self.read_bucket(&mut executor, &bucket);
                        }
                        executor.finalize(&self.global_extra_data);
                    })
                    .unwrap();
            }
        })
        .unwrap();
    }
}
