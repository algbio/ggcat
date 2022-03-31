mod queue_data;
mod reader;
mod sequences_splitter;

use crate::config::{
    BucketIndexType, MinimizerType, SwapPriority, DEFAULT_LZ4_COMPRESSION_LEVEL,
    DEFAULT_MINIMIZER_MASK, DEFAULT_PER_CPU_BUFFER_SIZE, MINIMIZER_BUCKETS_CHECKPOINT_SIZE,
    READ_INTERMEDIATE_CHUNKS_SIZE, READ_INTERMEDIATE_QUEUE_MULTIPLIER,
};
use crate::hashes::HashableSequence;
use crate::io::concurrent::temp_reads::creads_utils::CompressedReadsBucketHelper;
use crate::io::concurrent::temp_reads::extra_data::SequenceExtraData;
use crate::io::sequences_reader::FastaSequence;
use crate::pipeline_common::minimizer_bucketing::queue_data::MinimizerBucketingQueueData;
use crate::pipeline_common::minimizer_bucketing::reader::minb_reader;
use crate::pipeline_common::minimizer_bucketing::sequences_splitter::SequencesSplitter;
use crate::utils::get_memory_mode;
use parallel_processor::buckets::concurrent::BucketsThreadDispatcher;
use parallel_processor::buckets::writers::compressed_binary_writer::CompressedBinaryWriter;
use parallel_processor::buckets::writers::lock_free_binary_writer::LockFreeBinaryWriter;
use parallel_processor::buckets::MultiThreadBuckets;
use parallel_processor::memory_fs::file::internal::MemoryFileMode;
use parallel_processor::phase_times_monitor::PHASES_TIMES_MONITOR;
use parallel_processor::threadpools_chain::{
    ObjectsPoolManager, ThreadPoolDefinition, ThreadPoolsChain,
};
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
    >(
        &mut self,
        preprocess_info: &FACTORY::PreprocessInfo,
        sequence: S,
        range: Range<usize>,
        push_sequence: F,
    );
}

pub struct MinimizerBucketingCommonData<GlobalData> {
    pub k: usize,
    pub m: usize,
    pub buckets_count: usize,
    pub global_data: GlobalData,
}

pub struct MinimizerBucketingExecutionContext<GlobalData> {
    pub buckets: MultiThreadBuckets<CompressedBinaryWriter>,
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
    let mut tmp_reads_buffer =
        BucketsThreadDispatcher::new(DEFAULT_PER_CPU_BUFFER_SIZE, &context.buckets);

    let mut buckets_processor = E::new(&context.common);

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
                buckets_processor.process_sequence::<_, _, { DEFAULT_MINIMIZER_MASK }>(
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
        };

        ThreadPoolsChain::run_double(
            input_files,
            ThreadPoolDefinition::new(
                &execution_context,
                READ_INTERMEDIATE_CHUNKS_SIZE,
                String::from("assembler-minimizer-bucketing-reader"),
                max(1, threads_count / 2),
                &AtomicUsize::new(threads_count),
                threads_count * READ_INTERMEDIATE_QUEUE_MULTIPLIER.load(Ordering::Relaxed),
                minb_reader,
            ),
            ThreadPoolDefinition::new(
                &execution_context,
                (),
                String::from("assembler-minimizer-bucketing-writer"),
                threads_count,
                &AtomicUsize::new(threads_count),
                threads_count * READ_INTERMEDIATE_QUEUE_MULTIPLIER.load(Ordering::Relaxed),
                worker::<E>,
            ),
        );

        execution_context.buckets.finalize()
    }
}
