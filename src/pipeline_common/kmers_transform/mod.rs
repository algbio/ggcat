pub mod structs;

use crate::config::{
    BucketIndexType, SortingHashType, DEFAULT_OUTPUT_BUFFER_SIZE, DEFAULT_PREFETCH_AMOUNT,
    MINIMUM_LOG_DELTA_TIME, MINIMUM_RESPLIT_SIZE, OUTLIER_MAX_NUMBER_RATIO, OUTLIER_MAX_SIZE_RATIO,
    OUTLIER_MIN_DIFFERENCE, RESPLIT_MINIMIZER_MASK, SECOND_BUCKETS_COUNT,
    SUBBUCKET_OUTLIER_DIVISOR,
};
use crate::hashes::HashableSequence;
use crate::io::concurrent::temp_reads::creads_utils::CompressedReadsBucketHelper;
use crate::io::concurrent::temp_reads::extra_data::SequenceExtraData;
use crate::pipeline_common::kmers_transform::structs::{BucketProcessData, ProcessQueueItem};
use crate::pipeline_common::minimizer_bucketing::MinimizerBucketingExecutor;
use crate::pipeline_common::minimizer_bucketing::MinimizerBucketingExecutorFactory;
use crate::utils::compressed_read::CompressedRead;
use crate::utils::resource_counter::ResourceCounter;
use crate::KEEP_FILES;
use crossbeam::queue::SegQueue;
use parallel_processor::buckets::concurrent::{BucketsThreadBuffer, BucketsThreadDispatcher};
use parallel_processor::buckets::readers::async_binary_reader::{
    AsyncBinaryReader, AsyncReaderThread,
};
use parallel_processor::buckets::readers::compressed_binary_reader::CompressedStreamDecoder;
use parallel_processor::buckets::readers::generic_binary_reader::ChunkDecoder;
use parallel_processor::buckets::readers::lock_free_binary_reader::LockFreeStreamDecoder;
use parallel_processor::buckets::readers::BucketReader;
use parallel_processor::memory_fs::file::reader::FileReader;
use parallel_processor::memory_fs::{MemoryFs, RemoveFileMode};
use parallel_processor::phase_times_monitor::PHASES_TIMES_MONITOR;
use parallel_processor::utils::scoped_thread_local::ScopedThreadLocal;
use parking_lot::{Mutex, RwLock};
use std::cmp::min;
use std::marker::PhantomData;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Weak};
use std::time::{Duration, Instant};

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

    fn process_group<'y: 'x, R: BucketReader>(
        &mut self,
        global_data: &F::GlobalExtraData<'y>,
        reader: R,
    );

    fn finalize<'y: 'x>(self, global_data: &F::GlobalExtraData<'y>);
}

pub struct KmersTransform<'a, F: KmersTransformExecutorFactory> {
    buckets_count: usize,
    buffer_files_counter: Arc<ResourceCounter>,

    global_extra_data: F::GlobalExtraData<'a>,

    // files_queue: ArrayQueue<(PathBuf, bool)>,
    current_bucket: RwLock<Weak<BucketProcessData<CompressedStreamDecoder>>>,
    current_resplit_bucket: RwLock<Weak<BucketProcessData<LockFreeStreamDecoder>>>,

    process_queue: Arc<SegQueue<ProcessQueueItem>>,
    reprocess_queue: Arc<SegQueue<PathBuf>>,

    resplit_buckets_index: AtomicU32,

    async_readers: ScopedThreadLocal<Arc<AsyncReaderThread>>,

    last_info_log: Mutex<Instant>,
    _phantom: PhantomData<F>,
}

impl<'a, F: KmersTransformExecutorFactory> KmersTransform<'a, F> {
    pub fn new(
        file_inputs: Vec<PathBuf>,
        buckets_count: usize,
        extra_buffers_count: usize,
        global_extra_data: F::GlobalExtraData<'a>,
    ) -> Self {
        // let files_queue = ArrayQueue::new(file_inputs.len());
        let process_queue = SegQueue::new();

        let mut files_with_sizes: Vec<_> = file_inputs
            .into_iter()
            .map(|f| {
                let file_size = MemoryFs::get_file_size(&f).unwrap_or(0);
                (f, file_size, false)
            })
            .collect();

        files_with_sizes.sort_by_key(|x| x.1);
        files_with_sizes.reverse();

        let max_size = files_with_sizes[0].1;

        /*
         * Bucket outlier definition:
         * - More than 30% difference from the next smaller
         * - less than 50% difference from the biggest bucket
         * -
         */
        for idx in 0..(files_with_sizes.len() / OUTLIER_MAX_NUMBER_RATIO) {
            let crt_size = files_with_sizes[idx].1;
            let next_size = files_with_sizes[idx + 1].1;
            let distance_req = crt_size as f64 > next_size as f64 * (1.0 + OUTLIER_MIN_DIFFERENCE);
            let maxim_req = crt_size as f64 > max_size as f64 * OUTLIER_MAX_SIZE_RATIO;
            println!(
                "Outlier test: {} {} {} {}/{}",
                idx, crt_size, next_size, distance_req, maxim_req
            );
            if distance_req && maxim_req {
                for midx in 0..=idx {
                    files_with_sizes[midx].2 = true;
                }
            }
        }

        {
            let dummy_counter = ResourceCounter::new(1024);

            let mut start_idx = 0;
            let mut end_idx = files_with_sizes.len();

            let mut matched_size = 0i64;

            while start_idx != end_idx {
                let file_entry = if matched_size <= 0 {
                    let target_file = &files_with_sizes[start_idx];
                    let entry = (target_file.0.clone(), target_file.2);
                    matched_size = target_file.1 as i64;
                    start_idx += 1;
                    entry
                } else {
                    let target_file = &files_with_sizes[end_idx - 1];
                    let entry = (target_file.0.clone(), target_file.2);
                    matched_size -= target_file.1 as i64;
                    end_idx -= 1;
                    entry
                };

                process_queue.push(ProcessQueueItem {
                    path: file_entry.0,
                    can_resplit: false,
                    potential_outlier: !file_entry.1,
                    main_bucket_size: 0,
                    buffers_counter: dummy_counter.clone(),
                })
                // .unwrap();
            }
        }

        Self {
            buckets_count,
            buffer_files_counter: ResourceCounter::new(
                (SECOND_BUCKETS_COUNT + extra_buffers_count) as u64,
            ),
            global_extra_data,
            // files_queue,
            current_bucket: RwLock::new(Weak::new()),
            current_resplit_bucket: RwLock::new(Weak::new()),
            // process_queue: Arc::new(SegQueue::new()),
            process_queue: Arc::new(process_queue),
            reprocess_queue: Arc::new(SegQueue::new()),
            resplit_buckets_index: AtomicU32::new(0),
            async_readers: ScopedThreadLocal::new(|| {
                AsyncReaderThread::new(DEFAULT_OUTPUT_BUFFER_SIZE / 2, 4)
            }),
            last_info_log: Mutex::new(Instant::now()),
            _phantom: Default::default(),
        }
    }

    fn do_logging(&self) {
        let mut last_info_log = match self.last_info_log.try_lock() {
            None => return,
            Some(x) => x,
        };
        if last_info_log.elapsed() > MINIMUM_LOG_DELTA_TIME {
            let monitor = PHASES_TIMES_MONITOR.read();

            let processed_count = self.buckets_count - self.process_queue.len();

            let eta = Duration::from_secs(
                (monitor.get_phase_timer().as_secs_f64() / (processed_count as f64)
                    * (self.process_queue.len() as f64)) as u64,
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
    }

    fn get_current_bucket<
        FileType: ChunkDecoder,
        Allocator: Fn() -> Option<Arc<BucketProcessData<FileType>>>,
    >(
        current_bucket: &RwLock<Weak<BucketProcessData<FileType>>>,
        alloc_fn: Allocator,
    ) -> Option<Arc<BucketProcessData<FileType>>> {
        fn get_valid_bucket<FileType: ChunkDecoder>(
            bucket: &Weak<BucketProcessData<FileType>>,
        ) -> Option<Arc<BucketProcessData<FileType>>> {
            if let Some(bucket) = bucket.upgrade() {
                if !bucket.reader.is_finished() {
                    return Some(bucket);
                }
            }
            return None;
        }

        loop {
            let bucket = current_bucket.read();

            if let Some(bucket) = get_valid_bucket(&bucket) {
                return Some(bucket);
            }

            drop(bucket);
            let mut bucket = current_bucket.write();

            if let Some(bucket) = get_valid_bucket(&bucket) {
                return Some(bucket);
            }

            let new_bucket = alloc_fn()?;

            *bucket = Arc::downgrade(&new_bucket);

            return Some(new_bucket);
        }
    }

    fn read_bucket(
        &self,
        executor: &mut F::ExecutorType<'a>,
        bucket: &BucketProcessData<CompressedStreamDecoder>,
        buffer: &mut BucketsThreadBuffer,
    ) {
        let mut continue_read = true;

        if bucket.reader.is_finished() {
            return;
        }

        let mut cmp_reads = BucketsThreadDispatcher::new(&bucket.buckets, buffer);

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
                        &CompressedReadsBucketHelper::<_, F::FLAGS_COUNT>::new_packed(read, flags),
                    );
                },
            );
        }
    }

    fn resplit_buckets<'b>(
        &self,
        resplitter: &mut <F::SequencesResplitterFactory as MinimizerBucketingExecutorFactory>::ExecutorType<'b>,
        resplit_buffer: &mut BucketsThreadBuffer,
    ) -> bool {
        let mut did_resplit = false;
        let mut preproc_info = <F::SequencesResplitterFactory as MinimizerBucketingExecutorFactory>::PreprocessInfo::default();

        if let Some(resplit_bucket) = Self::get_current_bucket(&self.current_resplit_bucket, || {
            let path = self.reprocess_queue.pop()?;
            let file_name = path.file_name().unwrap().to_os_string();
            Some(Arc::new(
                BucketProcessData::<LockFreeStreamDecoder>::new_blocking(
                    path,
                    file_name.to_str().unwrap(),
                    self.process_queue.clone(),
                    self.buffer_files_counter.clone(),
                    true,
                    false,
                ),
            ))
        }) {
            did_resplit = true;

            let mut thread_buckets =
                BucketsThreadDispatcher::new(&resplit_bucket.buckets, resplit_buffer);

            while resplit_bucket
                .reader
                .decode_bucket_items_parallel::<CompressedReadsBucketHelper<F::AssociatedExtraData, F::FLAGS_COUNT>, _>(
                    Vec::new(),
                    |(flags, extra, read)| {
                        resplitter.reprocess_sequence(flags, &extra, &mut preproc_info);
                        resplitter.process_sequence::<_, _, { RESPLIT_MINIMIZER_MASK }>(
                            &preproc_info,
                            read,
                            0..read.bases_count(),
                            |bucket, seq, flags, extra| {
                                thread_buckets.add_element(
                                    bucket % (SECOND_BUCKETS_COUNT as BucketIndexType),
                                    &extra,
                                    &CompressedReadsBucketHelper::<F::AssociatedExtraData, F::FLAGS_COUNT>::new_packed(seq, flags)
                                );
                            },
                        );
                    },
                )
            {
                continue;
            }

            thread_buckets.finalize();
        }

        did_resplit
    }

    fn process_buffers(
        &self,
        executor: &mut F::ExecutorType<'a>,
        reader_thread: Arc<AsyncReaderThread>,
    ) {
        while let Some(ProcessQueueItem {
            ref path,
            ref can_resplit,
            ref potential_outlier,
            ref main_bucket_size,
            ..
        }) = self.process_queue.pop()
        {
            executor.maybe_swap_bucket(&self.global_extra_data);

            let file_size = FileReader::open(path, None).unwrap().total_file_size();
            let subbucket_outlier = file_size > main_bucket_size / SUBBUCKET_OUTLIER_DIVISOR;

            if !*potential_outlier
                || !subbucket_outlier
                || !*can_resplit
                || (file_size < MINIMUM_RESPLIT_SIZE)
            {
                let reader = AsyncBinaryReader::new(
                    path,
                    reader_thread.clone(),
                    true,
                    RemoveFileMode::Remove {
                        remove_fs: !KEEP_FILES.load(Ordering::Relaxed),
                    },
                    DEFAULT_PREFETCH_AMOUNT,
                );
                executor.process_group(&self.global_extra_data, reader);
            } else {
                println!(
                    "Resplitting bucket {} size: {} / {} [{}]",
                    path.display(),
                    file_size,
                    main_bucket_size / SUBBUCKET_OUTLIER_DIVISOR,
                    main_bucket_size
                );
                self.reprocess_queue.push(path.clone());
            }
            self.do_logging();
        }
    }

    pub fn parallel_kmers_transform(&self, threads_count: usize) {
        crossbeam::thread::scope(|s| {
            for _ in 0..min(self.buckets_count, threads_count) {
                s.builder()
                    .name("kmers-transform".to_string())
                    .spawn(|_| {
                        let async_reader = self.async_readers.get();
                        let mut executor = F::new(&self.global_extra_data);
                        // let mut splitter = F::new_resplitter(&self.global_extra_data);
                        // let mut local_buffer = BucketsThreadBuffer::new(
                        //     DEFAULT_PER_CPU_BUFFER_SIZE,
                        //     SECOND_BUCKETS_COUNT,
                        // );

                        loop {
                            self.process_buffers(&mut executor, Arc::clone(&*async_reader));
                            if self.process_queue.len() == 0 {
                                break;
                            }

                            // if self.resplit_buckets(&mut splitter, &mut local_buffer) {
                            //     continue;
                            // } else {
                            //     break;
                            // }

                            // let bucket =
                            //     match Self::get_current_bucket(&self.current_bucket, || {
                            //         let (file, outlier) = self.files_queue.pop()?;
                            //
                            //         Some(Arc::new(BucketProcessData::new_blocking(
                            //             file,
                            //             &format!(
                            //                 "vec{}",
                            //                 self.resplit_buckets_index
                            //                     .fetch_add(1, Ordering::Relaxed)
                            //             ),
                            //             self.process_queue.clone(),
                            //             self.buffer_files_counter.clone(),
                            //             false,
                            //             outlier,
                            //         )))
                            //     }) {
                            //         None => {
                            //             if self.process_queue.is_empty()
                            //                 && self.reprocess_queue.is_empty()
                            //             {
                            //                 break;
                            //             } else {
                            //                 continue;
                            //             }
                            //         }
                            //         Some(x) => x,
                            //     };
                            //
                            //
                            // self.read_bucket(&mut executor, &bucket, &mut local_buffer);
                        }
                        executor.finalize(&self.global_extra_data);
                    })
                    .unwrap();
            }
        })
        .unwrap();
    }
}
