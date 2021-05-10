use crate::intermediate_storage::{IntermediateReadsWriter, IntermediateSequencesStorage};
use crate::multi_thread_buckets::MultiThreadBuckets;
use crate::pipeline::Pipeline;
use crate::rolling_minqueue::RollingMinQueue;
use crate::sequences_reader::{FastaSequence, SequencesReader};
use crate::NtHashMinTransform;
use crossbeam::channel::*;
use crossbeam::queue::{ArrayQueue, SegQueue};
use crossbeam::{scope, thread};
use nix::sys::ptrace::cont;
use nthash::NtHashIterator;
use object_pool::Pool;
use rayon::iter::ParallelIterator;
use rayon::iter::{IntoParallelIterator, IntoParallelRefIterator};
use std::intrinsics::unlikely;
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

struct ExecutionContext {
    files: ArrayQueue<PathBuf>,
    pool: SegQueue<QueueData>,
    k: usize,
    m: usize,
    buckets_count: usize,
    buckets: MultiThreadBuckets<IntermediateReadsWriter<()>>,
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

fn worker(context: &ExecutionContext, receiver: &Receiver<QueueData>) {
    let mut minimizer_queue =
        RollingMinQueue::<u64, u32, NtHashMinTransform>::new(context.k - context.m + 1);

    let mut tmp_reads_buffer =
        IntermediateSequencesStorage::new(context.buckets_count, &context.buckets);

    while let Ok(data) = receiver.recv() {
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
                let hashes = NtHashIterator::new(seq, context.m).unwrap();

                let mut rolling_iter = minimizer_queue.make_iter(hashes.iter());

                let mut last_index = 0;
                let mut last_hash = rolling_iter.next().unwrap();

                for (index, min_hash) in rolling_iter.enumerate() {
                    if min_hash != last_hash {
                        let bucket = (last_hash as usize) % context.buckets_count;
                        tmp_reads_buffer.add_read(
                            (),
                            &seq[last_index..(index + context.k)],
                            bucket,
                        );
                        last_index = index + 1;
                        last_hash = min_hash;
                    }
                }
                tmp_reads_buffer.add_read(
                    (),
                    &seq[last_index..seq.len()],
                    (last_hash as usize) % context.buckets_count,
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
        let mut data = data;
        data.sequences.clear();
        data.data.clear();
        context.pool.push(data);
    }
    tmp_reads_buffer.finalize();
}

fn reader(context: &ExecutionContext, sender: &Sender<QueueData>) {
    while let Some(input) = context.files.pop() {
        let mut data = Some(context.pool.pop().unwrap_or(QueueData::new(CHUNKS_SIZE)));
        SequencesReader::process_file_extended(input, |x| {
            if x.seq.len() < context.k {
                return;
            }

            if unsafe { unlikely(!data.as_mut().unwrap_unchecked().push_sequences(x)) } {
                sender.send(unsafe { data.take().unwrap_unchecked() });
                data = Some(context.pool.pop().unwrap_or(QueueData::new(CHUNKS_SIZE)));
            }
        });
    }
}

impl Pipeline {
    pub fn minimizer_bucketing(
        mut input_files: Vec<PathBuf>,
        output_path: &Path,
        buckets_count: usize,
        k: usize,
        m: usize,
    ) -> Vec<PathBuf> {
        let start_time = Instant::now();
        let mut buckets = MultiThreadBuckets::<IntermediateReadsWriter<()>>::new(
            buckets_count,
            &output_path.join("bucket"),
        );

        input_files.sort_by_cached_key(|file| std::fs::metadata(file).unwrap().len());
        input_files.reverse();

        let files = ArrayQueue::new(input_files.len());
        for input in input_files {
            files.push(input);
        }

        let (sender, receiver) = bounded(WATERMARK_HIGH);

        let execution_context = ExecutionContext {
            files,
            pool: SegQueue::new(),
            k,
            m,
            buckets_count,
            buckets,
            start_time,
        };

        thread::scope(|s| {
            let mut readers = Vec::new();
            let sender_arc = Arc::new(sender);

            for i in 0..16 {
                let sender_arc = sender_arc.clone();
                let execution_context = &execution_context;
                readers.push(s.spawn(move |_| reader(execution_context, sender_arc.deref())));
            }
            drop(sender_arc);

            for i in 0..16 {
                s.spawn(|_| worker(&execution_context, &receiver));
            }

            readers.into_iter().for_each(|x| x.join().unwrap());
        });

        execution_context.buckets.finalize()
    }
}
