use crate::config::{
    BucketIndexType, DEFAULT_OUTPUT_BUFFER_SIZE, DEFAULT_PREFETCH_AMOUNT, MINIMUM_LOG_DELTA_TIME,
    USE_SECOND_BUCKET,
};
use crate::io::concurrent::temp_reads::creads_utils::CompressedReadsBucketHelper;
use crate::io::concurrent::temp_reads::extra_data::SequenceExtraData;
use crate::pipeline_common::kmers_transform::reads_buffer::{ReadsBuffer, ReadsMode};
use crate::pipeline_common::minimizer_bucketing::MinimizerBucketingExecutorFactory;
use crate::utils::compressed_read::CompressedReadIndipendent;
use crate::{CompressedRead, KEEP_FILES};
use crossbeam::channel::{bounded, unbounded, Receiver};
use crossbeam::queue::ArrayQueue;
use parallel_processor::buckets::readers::async_binary_reader::{
    AsyncBinaryReader, AsyncReaderThread,
};
use parallel_processor::memory_fs::{MemoryFs, RemoveFileMode};
use parallel_processor::phase_times_monitor::PHASES_TIMES_MONITOR;
use parallel_processor::utils::scoped_thread_local::ScopedThreadLocal;
use parking_lot::Mutex;
use std::marker::PhantomData;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

pub trait KmersTransformExecutorFactory: Sized + 'static + Sync + Send {
    type SequencesResplitterFactory: MinimizerBucketingExecutorFactory<
        ExtraData = Self::AssociatedExtraData,
    >;
    type GlobalExtraData<'a>: Send + Sync + 'a;
    type AssociatedExtraData: SequenceExtraData;
    type ExecutorType<'a>: KmersTransformExecutor<'a, Self>;
    type PreprocessorType<'a>: KmersTransformPreprocessor<'a, Self>;

    #[allow(non_camel_case_types)]
    type FLAGS_COUNT: typenum::uint::Unsigned;

    fn new_resplitter<'a, 'b: 'a>(
        global_data: &'a Self::GlobalExtraData<'b>,
    ) -> <Self::SequencesResplitterFactory as MinimizerBucketingExecutorFactory>::ExecutorType<'a>;

    fn new_preprocessor<'a>(global_data: &Self::GlobalExtraData<'a>) -> Self::PreprocessorType<'a>;
    fn new_executor<'a>(global_data: &Self::GlobalExtraData<'a>) -> Self::ExecutorType<'a>;
}

pub trait KmersTransformPreprocessor<'x, F: KmersTransformExecutorFactory> {
    fn get_sequence_bucket<'y: 'x, 'a, C>(
        &self,
        global_data: &F::GlobalExtraData<'y>,
        seq_data: &(u8, u8, C, CompressedRead),
    ) -> BucketIndexType;
}

pub trait KmersTransformExecutor<'x, F: KmersTransformExecutorFactory> {
    fn process_group_start<'y: 'x>(&mut self, global_data: &F::GlobalExtraData<'y>);
    fn process_group_batch_sequences<'y: 'x>(
        &mut self,
        global_data: &F::GlobalExtraData<'y>,
        batch: &Vec<(u8, F::AssociatedExtraData, CompressedReadIndipendent)>,
        ref_sequences: &Vec<u8>,
    );
    fn process_group_finalize<'y: 'x>(&mut self, global_data: &F::GlobalExtraData<'y>);

    fn finalize<'y: 'x>(self, global_data: &F::GlobalExtraData<'y>);
}

mod reads_buffer {
    use crate::utils::compressed_read::CompressedReadIndipendent;

    pub struct ReadsBuffer<E> {
        pub reads: Vec<(u8, E, CompressedReadIndipendent)>,
        pub reads_buffer: Vec<u8>,
    }

    pub enum ReadsMode<E> {
        Start(),
        AddBatch(ReadsBuffer<E>),
        Finalize(),
    }

    impl<E> ReadsBuffer<E> {
        pub fn new() -> Self {
            Self {
                reads: Vec::with_capacity(32 * 1024),
                reads_buffer: vec![],
            }
        }
        pub fn is_full(&self) -> bool {
            self.reads.len() == self.reads.capacity()
        }
    }
}

pub struct KmersTransform<'a, F: KmersTransformExecutorFactory> {
    buckets_count: usize,
    processed_buckets: AtomicUsize,

    global_extra_data: F::GlobalExtraData<'a>,
    files_queue: Arc<ArrayQueue<PathBuf>>,
    async_readers: ScopedThreadLocal<Arc<AsyncReaderThread>>,

    last_info_log: Mutex<Instant>,
    _phantom: PhantomData<F>,
}

impl<'a, F: KmersTransformExecutorFactory> KmersTransform<'a, F> {
    pub fn new(
        file_inputs: Vec<PathBuf>,
        buckets_count: usize,
        global_extra_data: F::GlobalExtraData<'a>,
    ) -> Self {
        let files_queue = ArrayQueue::new(file_inputs.len());

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

                files_queue.push(file_entry).unwrap();
            }
        }

        Self {
            buckets_count,
            processed_buckets: AtomicUsize::new(0),
            global_extra_data,
            files_queue: Arc::new(files_queue),
            async_readers: ScopedThreadLocal::new(|| {
                AsyncReaderThread::new(DEFAULT_OUTPUT_BUFFER_SIZE / 2, 4)
            }),
            last_info_log: Mutex::new(Instant::now()),
            _phantom: Default::default(),
        }
    }

    fn log_completed_bucket(&self) {
        self.processed_buckets.fetch_add(1, Ordering::Relaxed);

        let mut last_info_log = match self.last_info_log.try_lock() {
            None => return,
            Some(x) => x,
        };
        if last_info_log.elapsed() > MINIMUM_LOG_DELTA_TIME {
            let monitor = PHASES_TIMES_MONITOR.read();

            let processed_count = self.processed_buckets.load(Ordering::Relaxed);
            let remaining = self.buckets_count - processed_count;

            let eta = Duration::from_secs(
                (monitor.get_phase_timer().as_secs_f64() / (processed_count as f64)
                    * (remaining as f64)) as u64,
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

    pub fn parallel_kmers_transform(&self, threads_count: usize) {
        crossbeam::thread::scope(|s| {
            const THREADS_COUNT: usize = 2;

            for _ in 0..THREADS_COUNT {
                // min(self.buckets_count, threads_count) {
                s.builder()
                    .name("kmers-transform".to_string())
                    .spawn(|ns| {
                        const EXECUTORS_COUNT: usize = 16 / THREADS_COUNT;
                        const READER_THREADS: usize = 4;
                        const BUFFERS_POOL_SIZE: usize = (EXECUTORS_COUNT + READER_THREADS) * 8;

                        let buffers_pool = bounded(BUFFERS_POOL_SIZE);

                        for _ in 0..BUFFERS_POOL_SIZE {
                            buffers_pool.0.send(ReadsBuffer::new()).unwrap();
                        }

                        let read_input_queues: Vec<_> = (0..READER_THREADS)
                            .into_iter()
                            .map(|_| bounded(1))
                            .collect();

                        let read_output_queue = unbounded();

                        let queues: Vec<_> = (0..EXECUTORS_COUNT)
                            .into_iter()
                            .map(|_| unbounded())
                            .collect();

                        for i in 0..READER_THREADS {
                            let input_queue: Receiver<AsyncBinaryReader> =
                                read_input_queues[i].1.clone();
                            let output_queue = read_output_queue.0.clone();
                            let buffers_pool = buffers_pool.clone();
                            let queues = queues.clone();
                            ns.builder()
                                .name("r-kmers-transform".to_string())
                                .spawn(move |_| {
                                    let mut reads_mt_buffers: Vec<_> = (0..EXECUTORS_COUNT)
                                        .into_iter()
                                        .map(|_| buffers_pool.1.recv().unwrap())
                                        .collect();

                                    let async_reader = self.async_readers.get();
                                    let preprocessor = F::new_preprocessor(&self.global_extra_data);

                                    while let Ok(reader) = input_queue.recv() {
                                        reader
                                            .decode_all_bucket_items::<CompressedReadsBucketHelper<
                                                F::AssociatedExtraData,
                                                F::FLAGS_COUNT,
                                                { USE_SECOND_BUCKET },
                                                false,
                                            >, _>(
                                                async_reader.clone(),
                                                Vec::new(),
                                                |read_info| {
                                                    let bucket = preprocessor.get_sequence_bucket(
                                                        &self.global_extra_data,
                                                        &read_info,
                                                    )
                                                        as usize
                                                        % EXECUTORS_COUNT;

                                                    let (flags, _second_bucket, extra_data, read) =
                                                        read_info;

                                                    let ind_read =
                                                        CompressedReadIndipendent::from_read(
                                                            &read,
                                                            &mut reads_mt_buffers[bucket]
                                                                .reads_buffer,
                                                        );
                                                    reads_mt_buffers[bucket]
                                                        .reads
                                                        .push((flags, extra_data, ind_read));
                                                    if reads_mt_buffers[bucket].reads.len()
                                                        == reads_mt_buffers[bucket].reads.capacity()
                                                    {
                                                        queues[bucket]
                                                            .0
                                                            .send(ReadsMode::AddBatch(
                                                                std::mem::replace(
                                                                    &mut reads_mt_buffers[bucket],
                                                                    buffers_pool.1.recv().unwrap(),
                                                                ),
                                                            ))
                                                            .unwrap();
                                                        reads_mt_buffers[bucket].reads.clear();
                                                        reads_mt_buffers[bucket]
                                                            .reads_buffer
                                                            .clear();
                                                    }
                                                },
                                            );

                                        for e in 0..EXECUTORS_COUNT {
                                            if reads_mt_buffers[e].reads.len() > 0 {
                                                queues[e]
                                                    .0
                                                    .send(ReadsMode::AddBatch(std::mem::replace(
                                                        &mut reads_mt_buffers[e],
                                                        buffers_pool.1.recv().unwrap(),
                                                    )))
                                                    .unwrap();
                                                reads_mt_buffers[e].reads.clear();
                                                reads_mt_buffers[e].reads_buffer.clear();
                                            }
                                        }
                                        output_queue.send(());
                                    }
                                });
                        }

                        for i in 0..EXECUTORS_COUNT {
                            let pool_returner = buffers_pool.0.clone();
                            let receiver = queues[i].1.clone();
                            ns.builder()
                                .name("ex-kmers-transform".to_string())
                                .spawn(move |_| {
                                    let mut executor = F::new_executor(&self.global_extra_data);
                                    let mut resplitter = F::new_resplitter(&self.global_extra_data);

                                    while let Ok(data) = receiver.recv() {
                                        match data {
                                            ReadsMode::Start() => {
                                                executor
                                                    .process_group_start(&self.global_extra_data);
                                            }
                                            ReadsMode::AddBatch(mut batch) => {
                                                executor.process_group_batch_sequences(
                                                    &self.global_extra_data,
                                                    &batch.reads,
                                                    &batch.reads_buffer,
                                                );
                                                batch.reads.clear();
                                                batch.reads_buffer.clear();
                                                pool_returner.send(batch);
                                            }
                                            ReadsMode::Finalize() => {
                                                executor.process_group_finalize(
                                                    &self.global_extra_data,
                                                );
                                            }
                                        }
                                    }
                                    executor.finalize(&self.global_extra_data);
                                });
                        }

                        while let Some(path) = self.files_queue.pop() {
                            let reader = AsyncBinaryReader::new(
                                &path,
                                true,
                                RemoveFileMode::Remove {
                                    remove_fs: !KEEP_FILES.load(Ordering::Relaxed),
                                },
                                DEFAULT_PREFETCH_AMOUNT,
                            );

                            for queue in &queues {
                                queue.0.send(ReadsMode::Start()).unwrap();
                            }

                            for i in 0..READER_THREADS {
                                read_input_queues[i].0.send(reader.clone());
                            }

                            // Wait for all reader threads to complete
                            for _ in 0..READER_THREADS {
                                read_output_queue.1.recv();
                            }

                            for e in 0..EXECUTORS_COUNT {
                                queues[e].0.send(ReadsMode::Finalize()).unwrap();
                            }

                            self.log_completed_bucket();
                        }
                    })
                    .unwrap();
            }
        })
        .unwrap();
    }
}
