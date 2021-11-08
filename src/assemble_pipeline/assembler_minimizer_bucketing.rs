use crate::assemble_pipeline::current_kmers_merge::KmersFlags;
use crate::assemble_pipeline::AssemblePipeline;
use crate::colors::colors_manager::{ColorsManager, MinimizerBucketingSeqColorData};
use crate::hashes::ExtendableHashTraitType;
use crate::hashes::HashFunction;
use crate::hashes::HashFunctionFactory;
use crate::io::concurrent::intermediate_storage::{
    IntermediateReadsWriter, IntermediateSequencesStorage, SequenceExtraData,
};
use crate::io::sequences_reader::{FastaSequence, SequencesReader};
use crate::pipeline_common::common_minimizer_bucketing::{
    minb_reader, MinimizerBucketingExecutionContext, MinimizerBucketingQueueData,
};
use crate::rolling::kseq_iterator::{RollingKseqImpl, RollingKseqIterator};
use crate::rolling::minqueue::RollingMinQueue;
use crate::rolling::quality_check::{RollingQualityCheck, LOGPROB_MULTIPLIER, SCORES_INDEX};
use crate::types::BucketIndexType;
use crate::KEEP_FILES;
use bstr::ByteSlice;
use crossbeam::channel::*;
use crossbeam::queue::{ArrayQueue, SegQueue};
use crossbeam::{scope, thread};
use hashbrown::HashMap;
use itertools::Itertools;
use nix::sys::ptrace::cont;
use object_pool::Pool;
use parallel_processor::multi_thread_buckets::MultiThreadBuckets;
use parallel_processor::phase_times_monitor::PHASES_TIMES_MONITOR;
use parallel_processor::threadpools_chain::{
    ObjectsPoolManager, ThreadChainObject, ThreadPoolDefinition, ThreadPoolsChain,
};
use rayon::iter::ParallelIterator;
use rayon::iter::{IntoParallelIterator, IntoParallelRefIterator};
use std::cmp::{max, min};
use std::intrinsics::unlikely;
use std::mem::swap;
use std::ops::Deref;
use std::path::{Path, PathBuf};
use std::process::exit;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::{sleep, Thread};
use std::time::{Duration, Instant};

static SEQ_COUNT: AtomicU64 = AtomicU64::new(0);
static TOT_BASES_COUNT: AtomicU64 = AtomicU64::new(0);
static VALID_BASES_COUNT: AtomicU64 = AtomicU64::new(0);

struct ContextExtraData {
    quality_threshold: Option<f64>,
}

const CHUNKS_SIZE: usize = 1024 * 1024 * 16;
const MAX_READING_THREADS: usize = 2;
const WATERMARK_HIGH: usize = 64;

fn worker<H: HashFunctionFactory, CX: ColorsManager>(
    context: &MinimizerBucketingExecutionContext<CX, ContextExtraData>,
    manager: ObjectsPoolManager<(), MinimizerBucketingQueueData>,
) {
    let mut minimizer_queue = RollingMinQueue::<H>::new(context.k - context.m);

    let mut tmp_reads_buffer =
        IntermediateSequencesStorage::new(context.buckets_count, &context.buckets);
    let mut quality_check = RollingQualityCheck::new();

    let quality_log_threshold: u64 = RollingQualityCheck::get_log_for_correct_probability(
        context.extra.quality_threshold.unwrap_or(0.0),
    ); //0.9967);

    while let Some(data) = manager.recv_obj() {
        let mut total_bases = 0;
        let mut valid_bases = 0;

        for x in data.iter_sequences() {
            let mut start = 0;
            let mut end = 0;
            total_bases += x.seq.len() as u64;

            let mut process_sequences = |process_fn: &mut dyn FnMut(&[u8])| {
                while end < x.seq.len() {
                    start = end;
                    // Skip all not recognized characters
                    while start < x.seq.len() && x.seq[start] == b'N' {
                        start += 1;
                    }
                    end = start;
                    // Find the last valid character in this sequence
                    while end < x.seq.len() && x.seq[end] != b'N' {
                        end += 1;
                    }
                    // If the length of the read is long enough, return it
                    if end - start >= context.k {
                        if context.extra.quality_threshold.is_some() && x.qual.is_some() {
                            let q = x.qual.unwrap();

                            for (valid, mut el) in &RollingKseqIterator::iter_seq(
                                &q[start..end],
                                context.k,
                                &mut quality_check,
                            )
                            .enumerate()
                            .group_by(|(_, x)| *x < quality_log_threshold)
                            {
                                if !valid {
                                    continue;
                                }
                                let first_el = el.next().unwrap();
                                let last_el = el.last().unwrap_or(first_el);

                                let start_index = start + first_el.0;
                                let end_index = start + last_el.0 + context.k;

                                valid_bases += (end_index - start_index) as u64;
                                process_fn(&x.seq[start_index..end_index]);
                            }
                        } else {
                            valid_bases += (end - start) as u64;
                            process_fn(&x.seq[start..end]);
                        }
                    }
                }
            };

            process_sequences(&mut |seq| {
                let hashes = H::new(&seq[..], context.m);

                let mut rolling_iter =
                    minimizer_queue.make_iter(hashes.iter().map(|x| x.to_unextendable()));

                let mut last_index = 0;
                let mut last_hash = rolling_iter.next().unwrap();
                let mut include_first = true;

                for (index, min_hash) in rolling_iter.enumerate() {
                    if min_hash != last_hash {
                        let bucket =
                            H::get_bucket(last_hash) % (context.buckets_count as BucketIndexType);

                        tmp_reads_buffer.add_read(
                            KmersFlags(
                                include_first as u8,
                                CX::MinimizerBucketingSeqColorDataType::create(data.file_index),
                            ),
                            &seq[(max(1, last_index) - 1)..(index + context.k)],
                            bucket,
                        );
                        last_index = index + 1;
                        last_hash = min_hash;
                        include_first = false;
                    }
                }

                let start_index = max(1, last_index) - 1;
                let include_last = true; // Always include the last element of the sequence in the last entry
                tmp_reads_buffer.add_read(
                    KmersFlags(
                        include_first as u8 | ((include_last as u8) << 1),
                        CX::MinimizerBucketingSeqColorDataType::create(data.file_index),
                    ),
                    &seq[start_index..seq.len()],
                    H::get_bucket(last_hash) % (context.buckets_count as BucketIndexType),
                );
            });

            if SEQ_COUNT.fetch_add(1, Ordering::Relaxed) % 100000000 == 0 {
                TOT_BASES_COUNT.fetch_add(total_bases, Ordering::Relaxed);
                VALID_BASES_COUNT.fetch_add(valid_bases, Ordering::Relaxed);
                total_bases = 0;
                valid_bases = 0;

                println!(
                    "Elaborated {} sequences! [{} | {:.2}%] quality bases {}",
                    SEQ_COUNT.load(Ordering::Relaxed),
                    VALID_BASES_COUNT.load(Ordering::Relaxed),
                    (VALID_BASES_COUNT.load(Ordering::Relaxed) as f64)
                        / (max(1, TOT_BASES_COUNT.load(Ordering::Relaxed)) as f64)
                        * 100.0,
                    PHASES_TIMES_MONITOR
                        .read()
                        .get_formatted_counter_without_memory()
                );
            }
        }

        TOT_BASES_COUNT.fetch_add(total_bases, Ordering::Relaxed);
        VALID_BASES_COUNT.fetch_add(valid_bases, Ordering::Relaxed);
        manager.return_obj(data);
    }
    tmp_reads_buffer.finalize();
}

impl AssemblePipeline {
    pub fn minimizer_bucketing<H: HashFunctionFactory, CX: ColorsManager>(
        input_files: Vec<PathBuf>,
        output_path: &Path,
        buckets_count: usize,
        threads_count: usize,
        k: usize,
        m: usize,
        quality_threshold: Option<f64>,
    ) -> Vec<PathBuf> {
        PHASES_TIMES_MONITOR
            .write()
            .start_phase("phase: reads bucketing".to_string());

        let mut buckets = MultiThreadBuckets::<
            IntermediateReadsWriter<KmersFlags<CX::MinimizerBucketingSeqColorDataType>>,
        >::new(buckets_count, &output_path.join("bucket"), None);

        let mut input_files: Vec<_> = input_files
            .into_iter()
            .enumerate()
            .map(|(i, f)| (f, i as u64))
            .collect();

        input_files.sort_by_cached_key(|(file, _)| {
            std::fs::metadata(file)
                .expect(&format!("Error while opening file {}", file.display()))
                .len()
        });
        input_files.reverse();

        let execution_context = MinimizerBucketingExecutionContext {
            k,
            m,
            buckets_count,
            buckets,
            extra: ContextExtraData { quality_threshold },
        };

        ThreadPoolsChain::run_double(
            input_files,
            ThreadPoolDefinition::new(
                &execution_context,
                CHUNKS_SIZE,
                String::from("minimizer-bucketing-reader"),
                threads_count,
                &AtomicUsize::new(threads_count),
                WATERMARK_HIGH,
                minb_reader,
            ),
            ThreadPoolDefinition::new(
                &execution_context,
                (),
                String::from("minimizer-bucketing-writer"),
                threads_count,
                &AtomicUsize::new(threads_count),
                WATERMARK_HIGH,
                worker::<H, CX>,
            ),
        );

        execution_context.buckets.finalize()
    }
}
