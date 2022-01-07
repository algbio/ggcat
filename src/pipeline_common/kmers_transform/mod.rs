mod process_subbucket;
pub mod structs;

use crate::config::{BucketIndexType, SortingHashType, DEFAULT_PER_CPU_BUFFER_SIZE};
use crate::hashes::HashableSequence;
use crate::io::concurrent::intermediate_storage::SequenceExtraData;
use crate::io::concurrent::intermediate_storage_single::IntermediateSequencesStorageSingleBucket;
use crate::io::varint::{encode_varint, encode_varint_flags};
use crate::io::DataReader;
use crate::utils::compressed_read::CompressedRead;
use crossbeam::queue::{ArrayQueue, SegQueue};
use parallel_processor::memory_data_size::MemoryDataSize;
use parallel_processor::memory_fs::file::reader::FileReader;
use parallel_processor::memory_fs::MemoryFs;
use parallel_processor::multi_thread_buckets::BucketsThreadDispatcher;
use parallel_processor::phase_times_monitor::PHASES_TIMES_MONITOR;
use parking_lot::{Condvar, Mutex, RwLock};
use std::cmp::min;
use std::path::{Path, PathBuf};
use std::process::exit;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use structs::ReadRef;

pub const MERGE_BUCKETS_COUNT: usize = 256;
const BUFFER_CHUNK_SIZE: usize = 1024 * 16;

pub struct ReadDispatchInfo<E: SequenceExtraData> {
    pub bucket: BucketIndexType,
    pub hash: SortingHashType,
    pub flags: u8,
    pub extra_data: E,
}

pub trait KmersTransformExecutorFactory: Sized + 'static {
    type GlobalExtraData<'a>: Send + Sync + 'a;
    type InputBucketExtraData: SequenceExtraData;
    type IntermediateExtraData: SequenceExtraData;
    type ExecutorType<'a>: KmersTransformExecutor<'a, Self>;

    const FLAGS_COUNT: usize;

    fn new<'a>(global_data: &Self::GlobalExtraData<'a>) -> Self::ExecutorType<'a>;
}

pub trait KmersTransformExecutor<'x, F: KmersTransformExecutorFactory> {
    fn preprocess_bucket(
        &mut self,
        global_data: &F::GlobalExtraData<'x>,
        input_extra_data: F::InputBucketExtraData,
        read: CompressedRead,
    ) -> ReadDispatchInfo<F::IntermediateExtraData>;

    fn maybe_swap_bucket(&mut self, global_data: &F::GlobalExtraData<'x>);

    fn process_group(
        &mut self,
        global_data: &F::GlobalExtraData<'x>,
        reads: &[ReadRef],
        memory: &[u8],
    );

    fn finalize(self, global_data: &F::GlobalExtraData<'x>);
}

pub struct KmersTransform;

impl KmersTransform {
    pub fn parallel_kmers_transform<'a, F: KmersTransformExecutorFactory>(
        file_inputs: Vec<PathBuf>,
        buckets_count: usize,
        threads_count: usize,
        extra_data: F::GlobalExtraData<'a>,
        save_memory: bool,
    ) {
        static CURRENT_BUCKETS_COUNT: AtomicU64 = AtomicU64::new(0);

        let files_queue = ArrayQueue::new(file_inputs.len());
        file_inputs
            .into_iter()
            .for_each(|f| files_queue.push(f).unwrap());

        let vecs_process_queue = Arc::new(SegQueue::new());

        let mut last_info_log = Mutex::new(Instant::now());

        const MINIMUM_LOG_DELTA_TIME: Duration = Duration::from_secs(15);

        let open_bucket = || {
            let file = files_queue.pop()?;

            let mut last_info_log = last_info_log.lock();
            if last_info_log.elapsed() > MINIMUM_LOG_DELTA_TIME {
                let monitor = PHASES_TIMES_MONITOR.read();

                let processed_count = buckets_count - files_queue.len();

                let eta = Duration::from_secs(
                    (monitor.get_phase_timer().as_secs_f64() / (processed_count as f64)
                        * (files_queue.len() as f64)) as u64,
                );

                let est_tot = Duration::from_secs(
                    (monitor.get_phase_timer().as_secs_f64() / (processed_count as f64)
                        * (buckets_count as f64)) as u64,
                );

                println!(
                    "Processing bucket {} of {} {} phase eta: {:.0?} est.tot: {:.0?}",
                    processed_count,
                    buckets_count,
                    monitor.get_formatted_counter_without_memory(),
                    eta,
                    est_tot
                );
                *last_info_log = Instant::now();
            }

            Some(Arc::new(structs::BucketProcessData::<
                F::InputBucketExtraData,
            >::new(
                file, vecs_process_queue.clone()
            )))
        };

        let current_bucket = RwLock::new(open_bucket());
        let reading_finished = AtomicBool::new(false);

        let data_wait_condvar = Condvar::new();
        let data_wait_mutex = Mutex::new(());

        crossbeam::thread::scope(|s| {
            for _ in 0..min(buckets_count, threads_count) {
                s.spawn(|_| {
                    let mut executor = F::new(&extra_data);
                    let mut temp_readref_vec = Vec::new();
                    let mut temp_data_vec = Vec::new();

                    let mut process_pending_reads = |executor: &mut F::ExecutorType<'a>| {
                        while let Some(queue_path) = vecs_process_queue.pop() {
                            let mut reader = FileReader::open(&queue_path).unwrap();

                            executor.maybe_swap_bucket(&extra_data);

                            process_subbucket::process_subbucket::<F>(
                                &extra_data,
                                &mut reader,
                                &mut temp_readref_vec,
                                &mut temp_data_vec,
                                executor,
                            );

                            drop(reader);
                            MemoryFs::remove_file(queue_path, true);
                        }
                    };

                    const HASH_SIZE: usize = std::mem::size_of::<SortingHashType>();

                    'outer_loop: loop {
                        process_pending_reads(&mut executor);

                        if reading_finished.load(Ordering::Relaxed) {
                            break 'outer_loop;
                        }
                        let mut bucket = {
                            let current_bucket_read_guard = current_bucket.read();

                            match current_bucket_read_guard.clone() {
                                None => {
                                    drop(current_bucket_read_guard);
                                    data_wait_condvar.wait(&mut data_wait_mutex.lock());
                                    continue;
                                }
                                Some(val) => val,
                            }
                        };

                        let mut cmp_reads = BucketsThreadDispatcher::new(
                            DEFAULT_PER_CPU_BUFFER_SIZE,
                            &bucket.buckets,
                        );

                        let mut continue_read = true;
                        let mut tmp_data = Vec::with_capacity(256);

                        while continue_read {
                            process_pending_reads(&mut executor);

                            const TMP_DATA_OFFSET: usize = 32;

                            continue_read = bucket.reader.read_parallel(|read_extra_data, read| {
                                unsafe {
                                    tmp_data.set_len(TMP_DATA_OFFSET);
                                }
                                let mut backward_offset = TMP_DATA_OFFSET;

                                let preprocess_info =
                                    executor.preprocess_bucket(&extra_data, read_extra_data, read);
                                let bases_slice = read.get_compr_slice();

                                let bucket_index = preprocess_info.bucket;

                                encode_varint_flags::<_, _>(
                                    |slice| tmp_data.extend_from_slice(slice),
                                    read.bases_count() as u64,
                                    F::FLAGS_COUNT,
                                    preprocess_info.flags,
                                );
                                tmp_data.extend_from_slice(bases_slice);
                                preprocess_info.extra_data.encode(&mut tmp_data);

                                let element_size = (tmp_data.len() - TMP_DATA_OFFSET) as u64;

                                encode_varint(
                                    |bytes| {
                                        let end_pos = backward_offset;
                                        backward_offset -= bytes.len();
                                        tmp_data[backward_offset..end_pos].copy_from_slice(bytes);
                                    },
                                    element_size,
                                );

                                tmp_data[(backward_offset - HASH_SIZE)..backward_offset]
                                    .copy_from_slice(&preprocess_info.hash.to_ne_bytes());
                                backward_offset -= HASH_SIZE;

                                cmp_reads.add_element(
                                    bucket_index,
                                    &(),
                                    &tmp_data[backward_offset..],
                                );
                            });
                        }

                        cmp_reads.finalize();

                        drop(bucket);

                        let mut writable_bucket = current_bucket.write();
                        if writable_bucket
                            .as_ref()
                            .map(|x| {
                                x.reader.is_finished()
                                    && (!save_memory || (Arc::strong_count(x) == 1))
                            })
                            .unwrap_or(false)
                        {
                            writable_bucket.take();
                            data_wait_condvar.notify_all();
                        }

                        drop(writable_bucket);

                        loop {
                            process_pending_reads(&mut executor);

                            writable_bucket = current_bucket.write();
                            if vecs_process_queue.len() > 0 {
                                drop(writable_bucket);
                                continue;
                            }

                            if writable_bucket.is_none() {
                                if let Some(bucket) = open_bucket() {
                                    *writable_bucket = Some(bucket);
                                    data_wait_condvar.notify_all();
                                } else {
                                    reading_finished.store(true, Ordering::Relaxed);
                                }
                            }
                            break;
                        }
                    }
                    executor.finalize(&extra_data);
                });
            }
        });
    }
}
