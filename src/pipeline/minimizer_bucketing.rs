use crate::hash::ExtendableHashTraitType;
use crate::hash::HashFunction;
use crate::hash::HashFunctionFactory;
use crate::intermediate_storage::{IntermediateReadsWriter, IntermediateSequencesStorage};
use crate::pipeline::kmers_merge::KmersFlags;
use crate::pipeline::Pipeline;
use crate::rolling_kseq_iterator::{RollingKseqImpl, RollingKseqIterator};
use crate::rolling_minqueue::RollingMinQueue;
use crate::rolling_quality_check::{RollingQualityCheck, LOGPROB_MULTIPLIER, SCORES_INDEX};
use crate::sequences_reader::{FastaSequence, SequencesReader};
use crate::types::BucketIndexType;
use crate::KEEP_FILES;
use bstr::ByteSlice;
use crossbeam::channel::*;
use crossbeam::queue::{ArrayQueue, SegQueue};
use crossbeam::{scope, thread};
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

struct QueueData {
    data: Vec<u8>,
    sequences: Vec<(usize, usize, usize, usize)>,
}

impl QueueData {
    fn new(capacity: usize) -> Self {
        Self {
            data: Vec::with_capacity(capacity),
            sequences: Vec::with_capacity(capacity / 512),
        }
    }

    fn push_sequences(&mut self, seq: FastaSequence) -> bool {
        let qual_len = seq.qual.map(|q| q.len()).unwrap_or(0);
        let ident_len = seq.ident.len();
        let seq_len = seq.seq.len();

        let tot_len = qual_len + ident_len + seq_len;

        if self.data.len() != 0 && (self.data.capacity() - self.data.len()) < tot_len {
            return false;
        }

        let mut start = self.data.len();
        self.data.extend_from_slice(seq.ident);
        self.data.extend_from_slice(seq.seq);
        if let Some(qual) = seq.qual {
            self.data.extend_from_slice(qual);
        }

        self.sequences.push((start, ident_len, seq_len, qual_len));

        true
    }

    fn iter_sequences(&self) -> impl Iterator<Item = FastaSequence> {
        self.sequences
            .iter()
            .map(move |&(start, id_len, seq_len, qual_len)| {
                let mut start = start;

                let ident = &self.data[start..start + id_len];
                start += id_len;

                let seq = &self.data[start..start + seq_len];
                start += seq_len;

                let qual = match qual_len {
                    0 => None,
                    _ => Some(&self.data[start..start + qual_len]),
                };

                FastaSequence { ident, seq, qual }
            })
    }
}

impl ThreadChainObject for QueueData {
    type InitData = usize;

    fn initialize(params: &Self::InitData) -> Self {
        Self::new(*params)
    }

    fn reset(&mut self) {
        self.data.clear();
        self.sequences.clear();
    }
}

struct ExecutionContext {
    k: usize,
    m: usize,
    buckets_count: usize,
    buckets: MultiThreadBuckets<IntermediateReadsWriter<KmersFlags>>,
    quality_threshold: Option<f64>,
}

struct CounterTracker<'a>(&'a AtomicUsize);
impl<'a> CounterTracker<'a> {
    fn track(counter: &'a AtomicUsize) -> CounterTracker {
        counter.fetch_add(1, Ordering::Relaxed);
        CounterTracker(counter)
    }
}
impl Drop for CounterTracker<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::Relaxed);
    }
}

const CHUNKS_SIZE: usize = 1024 * 1024 * 16;
const MAX_READING_THREADS: usize = 2;
const WATERMARK_HIGH: usize = 64;

fn worker<H: HashFunctionFactory>(
    context: &ExecutionContext,
    manager: ObjectsPoolManager<(), QueueData>,
) {
    let mut minimizer_queue = RollingMinQueue::<H>::new(context.k - context.m);

    let mut tmp_reads_buffer =
        IntermediateSequencesStorage::new(context.buckets_count, &context.buckets);
    let mut quality_check = RollingQualityCheck::new();

    let quality_log_threshold: u64 = RollingQualityCheck::get_log_for_correct_probability(
        context.quality_threshold.unwrap_or(0.0),
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
                        if context.quality_threshold.is_some() && x.qual.is_some() {
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
                            KmersFlags(include_first as u8),
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
                    KmersFlags(include_first as u8 | ((include_last as u8) << 1)),
                    &seq[start_index..seq.len()],
                    H::get_bucket(last_hash) % (context.buckets_count as BucketIndexType),
                );
            });

            if SEQ_COUNT.fetch_add(1, Ordering::Relaxed) % 10000000 == 0 {
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

fn reader(context: &ExecutionContext, manager: ObjectsPoolManager<QueueData, PathBuf>) {
    while let Some(input) = manager.recv_obj() {
        let mut data = manager.allocate();
        SequencesReader::process_file_extended(
            input,
            |x| {
                if x.seq.len() < context.k {
                    return;
                }

                if unsafe { unlikely(!data.push_sequences(x)) } {
                    let mut tmp_data = manager.allocate();
                    swap(&mut data, &mut tmp_data);
                    manager.send(tmp_data);

                    if !data.push_sequences(x) {
                        panic!("Out of memory!");
                    }
                }
            },
            false,
        );
        if data.sequences.len() > 0 {
            manager.send(data);
        }
    }
}

impl Pipeline {
    pub fn minimizer_bucketing<H: HashFunctionFactory>(
        mut input_files: Vec<PathBuf>,
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

        let mut buckets = MultiThreadBuckets::<IntermediateReadsWriter<KmersFlags>>::new(
            buckets_count,
            &output_path.join("bucket"),
            None,
        );

        input_files.sort_by_cached_key(|file| {
            std::fs::metadata(file)
                .expect(&format!("Error while opening file {}", file.display()))
                .len()
        });
        input_files.reverse();

        let execution_context = ExecutionContext {
            k,
            m,
            buckets_count,
            buckets,
            quality_threshold,
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
                reader,
            ),
            ThreadPoolDefinition::new(
                &execution_context,
                (),
                String::from("minimizer-bucketing-writer"),
                threads_count,
                &AtomicUsize::new(threads_count),
                WATERMARK_HIGH,
                worker::<H>,
            ),
        );

        execution_context.buckets.finalize()
    }
}
