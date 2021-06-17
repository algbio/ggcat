use crate::hash::HashFunction;
use crate::hash::HashFunctionFactory;
use crate::intermediate_storage::{IntermediateReadsWriter, IntermediateSequencesStorage};
use crate::pipeline::kmers_merge::KmersFlags;
use crate::pipeline::Pipeline;
use crate::rolling_minqueue::RollingMinQueue;
use crate::sequences_reader::{FastaSequence, SequencesReader};
use crate::types::BucketIndexType;
use crossbeam::channel::*;
use crossbeam::queue::{ArrayQueue, SegQueue};
use crossbeam::{scope, thread};
use nix::sys::ptrace::cont;
use object_pool::Pool;
use parallel_processor::multi_thread_buckets::MultiThreadBuckets;
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
    start_time: Instant,
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

    while let Some(data) = manager.recv_obj() {
        for x in data.iter_sequences() {
            let mut start = 0;
            let mut end = 0;

            let mut get_sequence = || {
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
                        return Some(&x.seq[start..end]);
                    }
                }
                None
            };

            while let Some(seq) = get_sequence() {
                // println!(
                //     "ABC Process read: {}",
                //     std::str::from_utf8(&seq[..]).unwrap()
                // );
                let hashes = H::new(&seq[..], context.m);

                let mut rolling_iter = minimizer_queue.make_iter(hashes.iter());

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
            }

            if SEQ_COUNT.fetch_add(1, Ordering::Relaxed) % 10000000 == 0 {
                println!(
                    "Elaborated {} sequences! Time: [{:?}]",
                    SEQ_COUNT.load(Ordering::Relaxed),
                    context.start_time.elapsed()
                );
            }
        }
        manager.return_obj(data);
    }
    tmp_reads_buffer.finalize();
}

fn reader(context: &ExecutionContext, manager: ObjectsPoolManager<QueueData, PathBuf>) {
    while let Some(input) = manager.recv_obj() {
        let mut data = manager.allocate();
        SequencesReader::process_file_extended(input, |x| {
            if x.seq.len() < context.k {
                return;
            }

            if unsafe { unlikely(!data.push_sequences(x)) } {
                let mut tmp_data = manager.allocate();
                swap(&mut data, &mut tmp_data);
                manager.send(tmp_data);
            }
        });
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
    ) -> Vec<PathBuf> {
        let start_time = Instant::now();
        let mut buckets = MultiThreadBuckets::<IntermediateReadsWriter<KmersFlags>>::new(
            buckets_count,
            &output_path.join("bucket"),
            None,
        );

        input_files.sort_by_cached_key(|file| std::fs::metadata(file).unwrap().len());
        input_files.reverse();

        let execution_context = ExecutionContext {
            k,
            m,
            buckets_count,
            buckets,
            start_time,
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
                |context, manager| {
                    reader(context, manager);
                },
            ),
            ThreadPoolDefinition::new(
                &execution_context,
                (),
                String::from("minimizer-bucketing-writer"),
                threads_count,
                &AtomicUsize::new(threads_count),
                WATERMARK_HIGH,
                |context, manager| {
                    worker::<H>(context, manager);
                },
            ),
        );

        execution_context.buckets.finalize()
    }
}
