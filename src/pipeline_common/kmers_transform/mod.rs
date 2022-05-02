mod reader;

use crate::config::{
    BucketIndexType, DEFAULT_OUTPUT_BUFFER_SIZE, DEFAULT_PREFETCH_AMOUNT, MINIMUM_LOG_DELTA_TIME,
    USE_SECOND_BUCKET,
};
use crate::io::concurrent::temp_reads::creads_utils::CompressedReadsBucketHelper;
use crate::io::concurrent::temp_reads::extra_data::SequenceExtraData;
use crate::pipeline_common::kmers_transform::reads_buffer::ReadsBuffer;
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
use parallel_processor::memory_fs::{MemoryFs, RemoveFileMode};
use parallel_processor::phase_times_monitor::PHASES_TIMES_MONITOR;
use parallel_processor::utils::scoped_thread_local::ScopedThreadLocal;
use parking_lot::Mutex;
use std::marker::PhantomData;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

mod reads_buffer;
mod resplitter;
mod sequences_finalizer;
mod sequences_processor;

pub trait KmersTransformExecutorFactory: Sized + 'static + Sync + Send {
    type SequencesResplitterFactory: MinimizerBucketingExecutorFactory<
        ExtraData = Self::AssociatedExtraData,
    >;
    type GlobalExtraData: Sync + Send;
    type AssociatedExtraData: SequenceExtraData;
    type ExecutorType: KmersTransformExecutor<Self>;
    type PreprocessorType: KmersTransformPreprocessor<Self>;

    #[allow(non_camel_case_types)]
    type FLAGS_COUNT: typenum::uint::Unsigned;

    fn new_resplitter(
        global_data: &Arc<Self::GlobalExtraData>,
    ) -> <Self::SequencesResplitterFactory as MinimizerBucketingExecutorFactory>::ExecutorType;

    fn new_preprocessor(global_data: &Arc<Self::GlobalExtraData>) -> Self::PreprocessorType;
    fn new_executor(global_data: &Arc<Self::GlobalExtraData>) -> Self::ExecutorType;
}

pub trait KmersTransformPreprocessor<F: KmersTransformExecutorFactory> {
    fn get_sequence_bucket<C>(
        &self,
        global_data: &F::GlobalExtraData,
        seq_data: &(u8, u8, C, CompressedRead),
    ) -> BucketIndexType;
}

pub trait KmersTransformExecutor<F: KmersTransformExecutorFactory> {
    fn process_group_start(&mut self, global_data: &F::GlobalExtraData);
    fn process_group_batch_sequences(
        &mut self,
        global_data: &F::GlobalExtraData,
        batch: &Vec<(u8, F::AssociatedExtraData, CompressedReadIndipendent)>,
        ref_sequences: &Vec<u8>,
    );
    fn process_group_finalize(&mut self, global_data: &F::GlobalExtraData);

    fn finalize(self, global_data: &F::GlobalExtraData);
}

pub struct KmersTransform<F: KmersTransformExecutorFactory> {
    execution_context: Arc<KmersTransformContext<F>>,
    files_queue: Arc<ArrayQueue<PathBuf>>,

    last_info_log: Mutex<Instant>,
    _phantom: PhantomData<F>,
}

pub struct KmersTransformContext<F: KmersTransformExecutorFactory> {
    buckets_count: usize,
    processed_buckets: AtomicUsize,

    global_extra_data: Arc<F::GlobalExtraData>,
    async_readers: ScopedThreadLocal<Arc<AsyncReaderThread>>,
    counters: CountersAnalyzer,
}

impl<F: KmersTransformExecutorFactory> KmersTransform<F> {
    pub fn new(
        file_inputs: Vec<PathBuf>,
        buckets_counters_path: PathBuf,
        buckets_count: usize,
        global_extra_data: Arc<F::GlobalExtraData>,
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

        let execution_context = Arc::new(KmersTransformContext {
            buckets_count,
            processed_buckets: AtomicUsize::new(0),
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
            files_queue: Arc::new(files_queue),
            last_info_log: Mutex::new(Instant::now()),
            _phantom: Default::default(),
        }
    }

    fn log_completed_bucket(&self) {
        self.execution_context
            .processed_buckets
            .fetch_add(1, Ordering::Relaxed);

        let mut last_info_log = match self.last_info_log.try_lock() {
            None => return,
            Some(x) => x,
        };
        if last_info_log.elapsed() > MINIMUM_LOG_DELTA_TIME {
            *last_info_log = Instant::now();
            drop(last_info_log);

            let monitor = PHASES_TIMES_MONITOR.read();

            let processed_count = self
                .execution_context
                .processed_buckets
                .load(Ordering::Relaxed);
            let remaining = self.execution_context.buckets_count - processed_count;

            let eta = Duration::from_secs(
                (monitor.get_phase_timer().as_secs_f64() / (processed_count as f64)
                    * (remaining as f64)) as u64,
            );

            let est_tot = Duration::from_secs(
                (monitor.get_phase_timer().as_secs_f64() / (processed_count as f64)
                    * (self.execution_context.buckets_count as f64)) as u64,
            );

            println!(
                "Processing bucket {} of {} {} phase eta: {:.0?} est.tot: {:.0?}",
                processed_count,
                self.execution_context.buckets_count,
                monitor.get_formatted_counter_without_memory(),
                eta,
                est_tot
            );
        }
    }

    pub fn parallel_kmers_transform(&self, threads_count: usize) {
        crossbeam::thread::scope(|s| {
            const THREADS_COUNT: usize = 1;

            for _ in 0..THREADS_COUNT {
                // min(self.execution_context.buckets_count, threads_count) {
                s.builder()
                    .name("kmers-transform".to_string())
                    .spawn(|ns| {
                        let executors_count: usize = threads_count / THREADS_COUNT;
                        let reader_threads: usize = 4;

                        for i in 0..reader_threads {}

                        for i in 0..executors_count {
                            ns.builder()
                                .name("ex-kmers-transform".to_string())
                                .spawn(move |_| {
                                    let mut executor =
                                        F::new_executor(&self.execution_context.global_extra_data);
                                    let mut resplitter = F::new_resplitter(
                                        &self.execution_context.global_extra_data,
                                    );

                                    let mut outlier = false;

                                    // while let Ok(data) = receiver.recv() {
                                    //     match data {
                                    //         ReadsMode::Start { is_outlier } => {
                                    //             outlier = is_outlier;
                                    //             if !is_outlier {
                                    //                 executor.process_group_start(
                                    //                     &self.execution_context.global_extra_data,
                                    //                 );
                                    //             }
                                    //         }
                                    //         ReadsMode::AddBatch(mut batch) => {
                                    //             if outlier {
                                    //             } else {
                                    //                 executor.process_group_batch_sequences(
                                    //                     &self.execution_context.global_extra_data,
                                    //                     &batch.reads,
                                    //                     &batch.reads_buffer,
                                    //                 );
                                    //             }
                                    //             batch.reads.clear();
                                    //             batch.reads_buffer.clear();
                                    //             pool_returner.send(batch);
                                    //         }
                                    //         ReadsMode::Finalize() => {
                                    //             executor.process_group_finalize(
                                    //                 &self.execution_context.global_extra_data,
                                    //             );
                                    //         }
                                    //     }
                                    // }
                                    executor.finalize(&self.execution_context.global_extra_data);
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

                            // for queue in &queues {
                            //     queue
                            //         .0
                            //         .send(ReadsMode::Start { is_outlier: false })
                            //         .unwrap();
                            // }
                            //
                            // for i in 0..reader_threads {
                            //     read_input_queues[i].0.send(reader.clone());
                            // }

                            // // Wait for all reader threads to complete
                            // for _ in 0..reader_threads {
                            //     read_output_queue.1.recv();
                            // }
                            //
                            // for e in 0..executors_count {
                            //     queues[e].0.send(ReadsMode::Finalize()).unwrap();
                            // }

                            self.log_completed_bucket();
                        }
                    })
                    .unwrap();
            }
        })
        .unwrap();
    }
}
