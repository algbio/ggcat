mod queue_data;
mod reader;
mod sequences_splitter;

use crate::assemble_pipeline::parallel_kmers_merge::KmersFlags;
use crate::hashes::HashFunctionFactory;
use crate::io::concurrent::intermediate_storage::{
    IntermediateReadsWriter, IntermediateSequencesStorage, SequenceExtraData,
};
use crate::io::sequences_reader::FastaSequence;
use crate::io::DataWriter;
use crate::pipeline_common::minimizer_bucketing::queue_data::MinimizerBucketingQueueData;
use crate::pipeline_common::minimizer_bucketing::reader::minb_reader;
use crate::pipeline_common::minimizer_bucketing::sequences_splitter::SequencesSplitter;
use crate::rolling::minqueue::RollingMinQueue;
use crate::config::BucketIndexType;
use parallel_processor::multi_thread_buckets::MultiThreadBuckets;
use parallel_processor::phase_times_monitor::PHASES_TIMES_MONITOR;
use parallel_processor::threadpools_chain::{
    ObjectsPoolManager, ThreadPoolDefinition, ThreadPoolsChain,
};
use std::cmp::max;
use std::io::Write;
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

pub trait MinimizerBucketingExecutor {
    type GlobalData: Sync + Send;
    type ExtraData: SequenceExtraData;
    type PreprocessInfo: Default;
    type FileInfo: Clone + Sync + Send + Default;

    fn new<C, W: DataWriter>(
        global_data: &MinimizerBucketingExecutionContext<Self::ExtraData, C, Self::GlobalData, W>,
    ) -> Self;

    fn preprocess_fasta<C, W: DataWriter>(
        &mut self,
        global_data: &MinimizerBucketingExecutionContext<Self::ExtraData, C, Self::GlobalData, W>,
        file_info: &Self::FileInfo,
        read_index: u64,
        preprocess_info: &mut Self::PreprocessInfo,
        sequence: &FastaSequence,
    );
    fn process_sequence<C, F: FnMut(BucketIndexType, &[u8], Self::ExtraData), W: DataWriter>(
        &mut self,
        global_data: &MinimizerBucketingExecutionContext<Self::ExtraData, C, Self::GlobalData, W>,
        preprocess_info: &Self::PreprocessInfo,
        sequence: &[u8],
        range: Range<usize>,
        push_sequence: F,
    );
}

pub struct MinimizerBucketingExecutionContext<
    ReadAssociatedData: SequenceExtraData,
    ExtraData,
    GlobalData,
    W: DataWriter,
> {
    pub k: usize,
    pub m: usize,
    pub buckets_count: usize,
    pub buckets: MultiThreadBuckets<IntermediateReadsWriter<ReadAssociatedData, W>>,
    pub extra: ExtraData,
    pub global_data: GlobalData,
    pub current_file: AtomicUsize,
    pub total_files: usize,
}

pub struct GenericMinimizerBucketing;

static SEQ_COUNT: AtomicU64 = AtomicU64::new(0);
static LAST_TOTAL_COUNT: AtomicU64 = AtomicU64::new(0);
static TOT_BASES_COUNT: AtomicU64 = AtomicU64::new(0);
static VALID_BASES_COUNT: AtomicU64 = AtomicU64::new(0);

struct ContextExtraData {
    quality_threshold: Option<f64>,
}

const CHUNKS_SIZE: usize = 1024 * 1024 * 16;
const MAX_READING_THREADS: usize = 2;
const WATERMARK_HIGH: usize = 64;

fn worker<E: MinimizerBucketingExecutor, W: DataWriter>(
    context: &MinimizerBucketingExecutionContext<E::ExtraData, ContextExtraData, E::GlobalData, W>,
    manager: ObjectsPoolManager<(), MinimizerBucketingQueueData<E::FileInfo>>,
) {
    let mut tmp_reads_buffer =
        IntermediateSequencesStorage::new(context.buckets_count, &context.buckets);

    let mut buckets_processor = E::new(context);

    while let Some(data) = manager.recv_obj() {
        let mut total_bases = 0;
        let mut sequences_splitter = SequencesSplitter::new(context.k);

        let mut preprocess_info = E::PreprocessInfo::default();

        let mut sequences_count = 0;

        for (index, x) in data.iter_sequences().enumerate() {
            total_bases += x.seq.len() as u64;
            buckets_processor.preprocess_fasta(
                context,
                &data.file_info,
                data.start_read_index + index as u64,
                &mut preprocess_info,
                &x,
            );

            sequences_splitter.process_sequences(&x, None, &mut |sequence: &[u8], range| {
                buckets_processor.process_sequence(
                    context,
                    &preprocess_info,
                    sequence,
                    range,
                    |bucket, seq, extra| {
                        // println!(
                        //     "Sequence: {} /\t {}",
                        //     std::str::from_utf8(seq).unwrap(),
                        //     seq.len()
                        // );
                        tmp_reads_buffer.add_read(extra, seq, bucket);
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
    pub fn do_bucketing<E: MinimizerBucketingExecutor, W: DataWriter>(
        mut input_files: Vec<(PathBuf, E::FileInfo)>,
        output_path: &Path,
        buckets_count: usize,
        threads_count: usize,
        k: usize,
        m: usize,
        quality_threshold: Option<f64>,
        global_data: E::GlobalData,
    ) -> Vec<PathBuf> {
        let mut buckets = MultiThreadBuckets::<IntermediateReadsWriter<E::ExtraData, W>>::new(
            buckets_count,
            &output_path.join("bucket"),
            None,
        );

        input_files.sort_by_cached_key(|(file, _)| {
            std::fs::metadata(file)
                .expect(&format!("Error while opening file {}", file.display()))
                .len()
        });
        input_files.reverse();

        let mut execution_context = MinimizerBucketingExecutionContext {
            k,
            m,
            buckets_count,
            buckets,
            extra: ContextExtraData { quality_threshold },
            global_data,
            current_file: AtomicUsize::new(0),
            total_files: input_files.len(),
        };

        ThreadPoolsChain::run_double(
            input_files,
            ThreadPoolDefinition::new(
                &execution_context,
                CHUNKS_SIZE,
                String::from("assembler-minimizer-bucketing-reader"),
                threads_count,
                &AtomicUsize::new(threads_count),
                WATERMARK_HIGH,
                minb_reader,
            ),
            ThreadPoolDefinition::new(
                &execution_context,
                (),
                String::from("assembler-minimizer-bucketing-writer"),
                threads_count,
                &AtomicUsize::new(threads_count),
                WATERMARK_HIGH,
                worker::<E, W>,
            ),
        );

        execution_context.buckets.finalize()
    }
}
