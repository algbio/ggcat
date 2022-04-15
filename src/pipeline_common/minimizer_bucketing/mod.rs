mod queue_data;
mod reader;
mod sequences_splitter;

use crate::config::{
    BucketIndexType, MinimizerType, SwapPriority, DEFAULT_LZ4_COMPRESSION_LEVEL,
    DEFAULT_MINIMIZER_MASK, DEFAULT_PER_CPU_BUFFER_SIZE, HYPER_LOG_LOG_BUCKETS,
    HYPER_LOG_LOG_CORRECTION_FACTOR, HYPER_LOG_LOG_SAMPLE_RC_PROB,
    MINIMIZER_BUCKETS_CHECKPOINT_SIZE, READ_INTERMEDIATE_CHUNKS_SIZE,
    READ_INTERMEDIATE_QUEUE_MULTIPLIER,
};
use crate::hashes::HashableSequence;
use crate::io::concurrent::temp_reads::creads_utils::CompressedReadsBucketHelper;
use crate::io::concurrent::temp_reads::extra_data::SequenceExtraData;
use crate::io::sequences_reader::FastaSequence;
use crate::pipeline_common::minimizer_bucketing::queue_data::MinimizerBucketingQueueData;
use crate::pipeline_common::minimizer_bucketing::reader::minb_reader;
use crate::pipeline_common::minimizer_bucketing::sequences_splitter::SequencesSplitter;
use crate::utils::fast_rand_bool::FastRandBool;
use crate::utils::get_memory_mode;
use crate::utils::hyper_loglog_est::HyperLogLogEstimator;
use parallel_processor::buckets::concurrent::{BucketsThreadBuffer, BucketsThreadDispatcher};
use parallel_processor::buckets::writers::compressed_binary_writer::CompressedBinaryWriter;
use parallel_processor::buckets::MultiThreadBuckets;
use parallel_processor::phase_times_monitor::PHASES_TIMES_MONITOR;
use parallel_processor::threadpools_chain::{
    ObjectsPoolManager, ThreadPoolDefinition, ThreadPoolsChain,
};
use parking_lot::Mutex;
use std::cmp::max;
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

pub trait MinimizerInputSequence: HashableSequence + Copy {
    fn get_subslice(&self, range: Range<usize>) -> Self;
    fn seq_len(&self) -> usize;
    fn debug_to_string(&self) -> String;
}

impl MinimizerInputSequence for &[u8] {
    #[inline(always)]
    fn get_subslice(&self, range: Range<usize>) -> Self {
        &self[range]
    }

    fn seq_len(&self) -> usize {
        self.len()
    }

    fn debug_to_string(&self) -> String {
        std::str::from_utf8(self).unwrap().to_string()
    }
}

pub trait MinimizerBucketingExecutorFactory: Sized {
    type GlobalData: Sync + Send;
    type ExtraData: SequenceExtraData;
    type PreprocessInfo: Default;
    type FileInfo: Clone + Sync + Send + Default;

    #[allow(non_camel_case_types)]
    type FLAGS_COUNT: typenum::uint::Unsigned;

    type ExecutorType<'a>: MinimizerBucketingExecutor<'a, Self>;

    fn new<'a>(
        global_data: &'a MinimizerBucketingCommonData<Self::GlobalData>,
    ) -> Self::ExecutorType<'a>;
}

pub trait MinimizerBucketingExecutor<'a, FACTORY: MinimizerBucketingExecutorFactory> {
    fn preprocess_fasta(
        &mut self,
        file_info: &FACTORY::FileInfo,
        read_index: u64,
        preprocess_info: &mut FACTORY::PreprocessInfo,
        sequence: &FastaSequence,
    );

    fn reprocess_sequence(
        &mut self,
        flags: u8,
        intermediate_data: &FACTORY::ExtraData,
        preprocess_info: &mut FACTORY::PreprocessInfo,
    );

    fn process_sequence<
        S: MinimizerInputSequence,
        F: FnMut(BucketIndexType, S, u8, FACTORY::ExtraData),
        const MINIMIZER_MASK: MinimizerType,
        const FREQ_ESTIMATE: bool,
    >(
        &mut self,
        preprocess_info: &FACTORY::PreprocessInfo,
        sequence: S,
        range: Range<usize>,
        push_sequence: F,
    );

    fn get_buckets_estimators(&self) -> &[HyperLogLogEstimator<{ HYPER_LOG_LOG_BUCKETS }>];
}

pub struct MinimizerBucketingCommonData<GlobalData> {
    pub k: usize,
    pub m: usize,
    pub buckets_count: usize,
    pub global_data: GlobalData,
}

pub struct MinimizerBucketingExecutionContext<GlobalData> {
    pub buckets: MultiThreadBuckets<CompressedBinaryWriter>,
    pub freq_estimators: Mutex<Vec<Vec<HyperLogLogEstimator<HYPER_LOG_LOG_BUCKETS>>>>,
    pub common: MinimizerBucketingCommonData<GlobalData>,
    pub current_file: AtomicUsize,
    pub total_files: usize,
}

pub struct GenericMinimizerBucketing;

static SEQ_COUNT: AtomicU64 = AtomicU64::new(0);
static LAST_TOTAL_COUNT: AtomicU64 = AtomicU64::new(0);
static TOT_BASES_COUNT: AtomicU64 = AtomicU64::new(0);
static VALID_BASES_COUNT: AtomicU64 = AtomicU64::new(0);

fn worker<E: MinimizerBucketingExecutorFactory>(
    context: &MinimizerBucketingExecutionContext<E::GlobalData>,
    manager: ObjectsPoolManager<(), MinimizerBucketingQueueData<E::FileInfo>>,
) {
    let mut buffer = BucketsThreadBuffer::new(DEFAULT_PER_CPU_BUFFER_SIZE, context.buckets.count());

    let mut tmp_reads_buffer = BucketsThreadDispatcher::new(&context.buckets, &mut buffer);

    let mut buckets_processor = E::new(&context.common);

    let mut freq_prob_gen = FastRandBool::<HYPER_LOG_LOG_SAMPLE_RC_PROB>::new();

    while let Some(data) = manager.recv_obj() {
        let mut total_bases = 0;
        let mut sequences_splitter = SequencesSplitter::new(context.common.k);

        let mut preprocess_info = E::PreprocessInfo::default();

        let mut sequences_count = 0;

        for (index, x) in data.iter_sequences().enumerate() {
            total_bases += x.seq.len() as u64;
            buckets_processor.preprocess_fasta(
                &data.file_info,
                data.start_read_index + index as u64,
                &mut preprocess_info,
                &x,
            );

            sequences_splitter.process_sequences(&x, None, &mut |sequence: &[u8], range| {
                if freq_prob_gen.get_randbool() {
                    buckets_processor.process_sequence::<_, _, { DEFAULT_MINIMIZER_MASK }, true>(
                        &preprocess_info,
                        sequence,
                        range,
                        |bucket, seq, flags, extra| {
                            tmp_reads_buffer.add_element(
                                bucket,
                                &extra,
                                &CompressedReadsBucketHelper::<_, E::FLAGS_COUNT>::new(seq, flags),
                            );
                        },
                    );
                } else {
                    buckets_processor.process_sequence::<_, _, { DEFAULT_MINIMIZER_MASK }, false>(
                        &preprocess_info,
                        sequence,
                        range,
                        |bucket, seq, flags, extra| {
                            tmp_reads_buffer.add_element(
                                bucket,
                                &extra,
                                &CompressedReadsBucketHelper::<_, E::FLAGS_COUNT>::new(seq, flags),
                            );
                        },
                    )
                }
            });

            sequences_count += 1;
        }

        SEQ_COUNT.fetch_add(sequences_count, Ordering::Relaxed);
        let total_bases_count =
            TOT_BASES_COUNT.fetch_add(total_bases, Ordering::Relaxed) + total_bases;
        VALID_BASES_COUNT.fetch_add(sequences_splitter.valid_bases, Ordering::Relaxed);

        const TOTAL_BASES_DIFF_LOG: u64 = 10000000000;

        let do_print_log = LAST_TOTAL_COUNT
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |x| {
                if total_bases_count - x > TOTAL_BASES_DIFF_LOG {
                    Some(total_bases_count)
                } else {
                    None
                }
            })
            .is_ok();

        if do_print_log {
            let current_file = context.current_file.load(Ordering::Relaxed);

            println!(
                "Elaborated {} sequences! [{} | {:.2}% qb] ({}/{} => {:.2}%) {}",
                SEQ_COUNT.load(Ordering::Relaxed),
                VALID_BASES_COUNT.load(Ordering::Relaxed),
                (VALID_BASES_COUNT.load(Ordering::Relaxed) as f64)
                    / (max(1, TOT_BASES_COUNT.load(Ordering::Relaxed)) as f64)
                    * 100.0,
                current_file,
                context.total_files,
                current_file as f64 / max(1, context.total_files) as f64 * 100.0,
                PHASES_TIMES_MONITOR
                    .read()
                    .get_formatted_counter_without_memory()
            );
        }

        manager.return_obj(data);
    }

    let estimators = Vec::from(buckets_processor.get_buckets_estimators());
    context.freq_estimators.lock().push(estimators);

    tmp_reads_buffer.finalize();
}

impl GenericMinimizerBucketing {
    pub fn do_bucketing<E: MinimizerBucketingExecutorFactory>(
        mut input_files: Vec<(PathBuf, E::FileInfo)>,
        output_path: &Path,
        buckets_count: usize,
        threads_count: usize,
        k: usize,
        m: usize,
        global_data: E::GlobalData,
    ) -> Vec<PathBuf> {
        let buckets = MultiThreadBuckets::<CompressedBinaryWriter>::new(
            buckets_count,
            output_path.join("bucket"),
            &(
                get_memory_mode(SwapPriority::MinimizerBuckets),
                MINIMIZER_BUCKETS_CHECKPOINT_SIZE,
                DEFAULT_LZ4_COMPRESSION_LEVEL,
            ),
        );

        input_files.sort_by_cached_key(|(file, _)| {
            std::fs::metadata(file)
                .expect(&format!("Error while opening file {}", file.display()))
                .len()
        });
        input_files.reverse();

        let mut execution_context = MinimizerBucketingExecutionContext {
            buckets,
            current_file: AtomicUsize::new(0),
            total_files: input_files.len(),
            common: MinimizerBucketingCommonData {
                k,
                m,
                buckets_count,
                global_data,
            },
            freq_estimators: Mutex::new(Vec::new()),
        };

        ThreadPoolsChain::run_double(
            input_files,
            ThreadPoolDefinition::new(
                &execution_context,
                READ_INTERMEDIATE_CHUNKS_SIZE,
                String::from("r-assembler-minimizer"),
                max(1, threads_count / 2),
                &AtomicUsize::new(threads_count),
                threads_count * READ_INTERMEDIATE_QUEUE_MULTIPLIER.load(Ordering::Relaxed),
                minb_reader,
            ),
            ThreadPoolDefinition::new(
                &execution_context,
                (),
                String::from("w-assembler-minimizer"),
                threads_count,
                &AtomicUsize::new(threads_count),
                threads_count * READ_INTERMEDIATE_QUEUE_MULTIPLIER.load(Ordering::Relaxed),
                worker::<E>,
            ),
        );

        let frequencies = HyperLogLogEstimator::estimate_batch_elements(
            &execution_context.freq_estimators.lock(),
            HYPER_LOG_LOG_CORRECTION_FACTOR,
            (0.5f64).powf(HYPER_LOG_LOG_SAMPLE_RC_PROB as f64),
        );

        for (idx, (f, cnt)) in frequencies.iter().enumerate() {
            println!(
                "Size estimation bucket{}: {} unique vs {} total",
                idx, *f, *cnt
            );
        }

        execution_context.buckets.finalize()
    }
}
