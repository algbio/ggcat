use crate::intermediate_storage::IntermediateReadsWriter;
use crate::multi_thread_buckets::MultiThreadBuckets;
use crate::pipeline::Pipeline;
use crate::rolling_minqueue::RollingMinQueue;
use crate::sequences_reader::SequencesReader;
use crate::NtHashMinTransform;
use nthash::NtHashIterator;
use rayon::iter::IntoParallelRefIterator;
use rayon::iter::ParallelIterator;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;
use std::time::Instant;

static SEQ_COUNT: AtomicU64 = AtomicU64::new(0);

impl Pipeline {
    pub fn minimizer_bucketing(
        mut input_files: Vec<String>,
        output_path: &Path,
        buckets_count: usize,
        k: usize,
        m: usize,
    ) -> Vec<PathBuf> {
        let start_time = Instant::now();

        const NONE: Option<Mutex<IntermediateReadsWriter>> = None;
        let mut buckets =
            MultiThreadBuckets::<IntermediateReadsWriter>::new(buckets_count, &output_path.join("bucket"));

        input_files.sort_by_cached_key(|file| std::fs::metadata(file).unwrap().len());
        input_files.reverse();

        input_files.par_iter().for_each(|input| {
            const VEC: Vec<u8> = Vec::new();
            const VEC_USZ: Vec<usize> = Vec::new();
            const ALLOWED_LEN: usize = 1024 * 1024;
            let mut temp_buffers = vec![VEC; buckets_count];
            let mut temp_indexes = vec![VEC_USZ; buckets_count];

            let mut flush_buffer =
                |temp_indexes: &mut [Vec<usize>], temp_buffers: &mut [Vec<u8>], bucket: usize| {
                    if temp_indexes.len() == 0 {
                        return;
                    }

                    temp_indexes[bucket].push(temp_buffers[bucket].len());
                    let mut last_index = 0;
                    buckets.flush(bucket, |reader| {
                        for index in temp_indexes[bucket].iter().skip(1) {
                            reader.add_acgt_read(&temp_buffers[bucket][last_index..*index]);
                            last_index = *index;
                        }
                    });
                    temp_buffers[bucket].clear();
                    temp_indexes[bucket].clear();
                };
            let mut minimizer_queue =
                RollingMinQueue::<u64, u32, NtHashMinTransform>::new(k - m + 1);

            SequencesReader::process_file_extended(input.to_string(), |x| {
                if x.seq.len() < k {
                    return;
                }

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
                        if end - start >= k {
                            return Some(&x.seq[start..end]);
                        }
                    }
                    None
                };

                while let Some(seq) = get_sequence() {
                    let hashes = NtHashIterator::new(seq, m).unwrap();

                    let mut rolling_iter = minimizer_queue.make_iter(hashes.iter());

                    let mut last_index = 0;

                    let mut add_buffer = |index: usize, bucket: usize| {
                        temp_indexes[bucket].push(temp_buffers[bucket].len());
                        temp_buffers[bucket].extend_from_slice(&seq[last_index..(index + k)]);
                        // assert_reads(
                        //     &seq[(max(1, last_index) - 1)..min(seq.len(), index + k + 1)],
                        //     bucket as u64,
                        // );

                        if temp_buffers[bucket].len() > ALLOWED_LEN {
                            flush_buffer(
                                temp_indexes.as_mut_slice(),
                                temp_buffers.as_mut_slice(),
                                bucket,
                            );
                        }

                        last_index = index + 1;
                    };

                    let mut last_hash = rolling_iter.next().unwrap();
                    for (index, min_hash) in rolling_iter.enumerate() {
                        if min_hash != last_hash {
                            let bucket = (last_hash as usize) % buckets_count;
                            add_buffer(index, bucket);
                            last_hash = min_hash;
                        }
                    }
                    add_buffer(seq.len() - k, (last_hash as usize) % buckets_count);
                }

                if SEQ_COUNT.fetch_add(1, Ordering::Relaxed) % 10000000 == 0 {
                    println!(
                        "Elaborated {} sequences! Time: [{:?}]",
                        SEQ_COUNT.load(Ordering::Relaxed),
                        start_time.elapsed()
                    );
                }
            });

            for i in 0..buckets_count {
                flush_buffer(&mut temp_indexes, &mut temp_buffers, i);
            }
        });
        buckets.finalize()
    }
}
