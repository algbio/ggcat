pub mod extra_data;

use std::{
    cmp::Reverse,
    future::Future,
    marker::PhantomData,
    path::Path,
    sync::atomic::{AtomicUsize, Ordering},
};

use crate::{
    MinimizerBucketMode, MinimizerBucketingExecutionContext, MinimizerBucketingExecutorFactory,
    queue_data::MinimizerBucketingQueueData,
};
use colors::non_colored::NonColoredManager;
use config::{
    BucketIndexType, DEFAULT_COMPACTION_MAP_SUBBUCKET_ELEMENTS,
    DEFAULT_COMPACTION_STORAGE_PER_BUCKET_SIZE, DEFAULT_OUTPUT_BUFFER_SIZE,
    DEFAULT_PREFETCH_AMOUNT, KEEP_FILES, MAX_COMPACTION_MAP_SUBBUCKET_ELEMENTS,
    MAXIMUM_SECOND_BUCKETS_COUNT, MINIMIZER_BUCKETS_CHECKPOINT_SIZE, MultiplicityCounterType,
    PRIORITY_SCHEDULING_HIGH, SwapPriority, USE_SECOND_BUCKET, WORKERS_PRIORITY_HIGH,
    get_compression_level_info, get_memory_mode,
};
use ggcat_logging::stats;
use hashbrown::{HashTable, hash_table::Entry};
use io::{
    compressed_read::CompressedReadIndipendent,
    concurrent::temp_reads::{
        creads_utils::{
            AssemblerMinimizerPosition, BucketModeFromBoolean, CompressedReadsBucketData,
            CompressedReadsBucketDataSerializer, NoSecondBucket, ReadsCheckpointData,
            WithMultiplicity,
        },
        extra_data::SequenceExtraDataTempBufferManagement,
    },
};
use io::{concurrent::temp_reads::creads_utils::DeserializedRead, creads_helper};
use parallel_processor::{
    buckets::{
        LockFreeBucket,
        readers::async_binary_reader::{AsyncBinaryReader, AsyncReaderThread},
        writers::compressed_binary_writer::CompressedBinaryWriter,
    },
    execution_manager::{
        executor::{AsyncExecutor, ExecutorReceiver},
        memory_tracker::MemoryTracker,
    },
    memory_fs::RemoveFileMode,
    mt_debug_counters::{
        counter::{AtomicCounter, SumMode},
        declare_counter_i64,
    },
};
use parallel_processor::{
    buckets::{
        bucket_writer::BucketItemSerializer,
        readers::async_binary_reader::AllowedCheckpointStrategy,
    },
    memory_fs::MemoryFs,
    scheduler::PriorityScheduler,
};
use utils::track;

pub struct MinimizerBucketingCompactor<E: MinimizerBucketingExecutorFactory + Sync + Send + 'static>
{
    _phantom: PhantomData<E>, // mem_tracker: MemoryTracker<Self>,
}

static ADDR_WAITING_COUNTER: AtomicCounter<SumMode> =
    declare_counter_i64!("mb_addr_wait_compactor", SumMode, false);

#[derive(Clone, Debug)]
pub struct CompactorInitData {
    pub bucket_index: u16,
}

impl<E: MinimizerBucketingExecutorFactory + Sync + Send + 'static> AsyncExecutor
    for MinimizerBucketingCompactor<E>
{
    type InputPacket = MinimizerBucketingQueueData<E::StreamInfo>;
    type OutputPacket = ();
    type GlobalParams = MinimizerBucketingExecutionContext<E::GlobalData>;
    type InitData = CompactorInitData;

    fn new() -> Self {
        Self {
            _phantom: PhantomData,
        }
    }

    fn async_executor_main<'a>(
        &'a mut self,
        global_params: &'a Self::GlobalParams,
        mut receiver: ExecutorReceiver<Self>,
        _memory_tracker: MemoryTracker<Self>,
    ) -> impl Future<Output = ()> + Send + 'a {
        async move {
            // Needed to correctly perform the splitting without recomputing the minimizers
            assert!(USE_SECOND_BUCKET);

            let read_thread = AsyncReaderThread::new(DEFAULT_OUTPUT_BUFFER_SIZE, 4);

            static COMPACTED_INDEX: AtomicUsize = AtomicUsize::new(0);

            const MAXIMUM_SEQUENCES: usize =
                MAXIMUM_SECOND_BUCKETS_COUNT * MAX_COMPACTION_MAP_SUBBUCKET_ELEMENTS;

            // Outline of the compaction algorithm:
            // OBJECTIVE: Compact the new buckets avoiding too much overhead in compaction
            // - An increasing in i/o factor of 1.2..1.5 is acceptable
            // - The compaction of non-compacted buckets has priority
            // - When compacting new buckets care must be taken in not reading again compressed buckets in a quadratic complexity
            // STRATEGY:
            // Compact buckets when one of the following applies:
            // - either there are no other compacted buckets
            // - the sum in sizes of the new buckets is larger than the smallest already compacted bucket
            // - the uncompacted buckets reach a minimum size threshold (20% of the total sizes of the buckets or ?= 64MB)
            // And the sizes of the bucket to compact is greater than a small threshold (1MB)
            // -----
            // To choose which buckets to compact, first take the uncompacted from the smallest to the largest,
            // then all the compacted from the smallest to the largest so that their size does not exceed 1/1.5 of the size of the non-compacted buckets

            struct SuperKmerEntry {
                read: CompressedReadIndipendent,
                multiplicity: MultiplicityCounterType,
                minimizer_pos: u16,
                flags: u8,
                is_window_duplicate: bool,
            }

            // Min 1MB size for compaction
            const MINIMUM_INPUT_SIZE: usize = 1024 * 1024 * 1;

            const COMPACT_THRESHOLD_RATIO: f64 = 0.2;
            const COMPACT_THRESHOLD_BYTES: usize = 1024 * 1024 * 64;
            const MAX_COMPACTED_IO_RATIO: f64 = 1.5;

            let mut super_kmers_hashmap: Vec<HashTable<SuperKmerEntry>> = (0
                ..MAXIMUM_SECOND_BUCKETS_COUNT)
                .map(|_| HashTable::with_capacity(DEFAULT_COMPACTION_MAP_SUBBUCKET_ELEMENTS))
                .collect();

            let mut super_kmers_buffer: Vec<Vec<SuperKmerEntry>> = (0
                ..MAXIMUM_SECOND_BUCKETS_COUNT)
                .map(|_| Vec::with_capacity(DEFAULT_COMPACTION_MAP_SUBBUCKET_ELEMENTS))
                .collect();

            let mut super_kmers_storage: Vec<Vec<u8>> = (0..MAXIMUM_SECOND_BUCKETS_COUNT)
                .map(|_| Vec::with_capacity(DEFAULT_COMPACTION_STORAGE_PER_BUCKET_SIZE))
                .collect();

            let thread_handle = PriorityScheduler::declare_thread(PRIORITY_SCHEDULING_HIGH);

            // use hashbrown::HashTable;
            fn process_superkmers(
                super_kmers_hashmap: &mut HashTable<SuperKmerEntry>,
                super_kmers_buffer: &mut Vec<SuperKmerEntry>,
                super_kmers_storage: &mut Vec<u8>,
                total_sequences: &mut usize,
            ) {
                debug_assert!(super_kmers_storage.len() > 0);
                let mut copy_pos = super_kmers_buffer[0].read.buffer_start_index();

                for SuperKmerEntry {
                    read,
                    multiplicity,
                    minimizer_pos,
                    flags,
                    is_window_duplicate,
                } in super_kmers_buffer.drain(..)
                {
                    let read_hash = read.compute_hash_aligned(super_kmers_storage);
                    let read_slice = read.get_packed_slice_aligned(super_kmers_storage);

                    match super_kmers_hashmap.entry(
                        read_hash,
                        |a| {
                            a.read.bases_count() == read.bases_count()
                                && a.read.get_packed_slice_aligned(super_kmers_storage)
                                    == read_slice
                                && a.flags == flags
                        },
                        |v| v.read.compute_hash_aligned(super_kmers_storage),
                    ) {
                        Entry::Occupied(mut entry) => {
                            entry.get_mut().multiplicity += multiplicity;
                        }
                        Entry::Vacant(position) => {
                            let bytes_count = read_slice.len();
                            let new_read = unsafe {
                                std::ptr::copy(
                                    super_kmers_storage.as_ptr().add(read.buffer_start_index()),
                                    super_kmers_storage.as_mut_ptr().add(copy_pos),
                                    bytes_count,
                                );

                                CompressedReadIndipendent::from_start_buffer_position(
                                    copy_pos,
                                    read.bases_count(),
                                )
                            };
                            copy_pos += bytes_count;

                            position.insert(SuperKmerEntry {
                                read: new_read,
                                multiplicity,
                                minimizer_pos,
                                flags,
                                is_window_duplicate,
                            });
                            *total_sequences += 1;
                        }
                    }
                }

                super_kmers_storage.truncate(copy_pos);
            }

            while let Ok((_, init_data)) = track!(
                receiver
                    .obtain_address_with_priority(WORKERS_PRIORITY_HIGH, &thread_handle)
                    .await,
                ADDR_WAITING_COUNTER
            ) {
                if !global_params.common.is_active.load(Ordering::Relaxed) {
                    continue;
                }

                let mut chosen_buckets = vec![];

                let bucket_index = init_data.bucket_index as usize;

                let should_skip = {
                    let mut ratios = global_params.compaction_ratios[bucket_index].lock();
                    ratios.skipped += 1;
                    ratios.skipped < ratios.skip_step
                };
                if should_skip {
                    continue;
                }

                let mut buckets = global_params.buckets.get_stored_buckets().lock();

                let mut total_uncompacted_size = 0;
                let mut total_compacted_size = 0;

                // Avoid crashing in case there are no chunks and this is called
                if buckets[bucket_index].chunks.is_empty() {
                    continue;
                }

                fn is_compacted(path: &Path) -> bool {
                    path.file_name()
                        .unwrap()
                        .to_str()
                        .unwrap()
                        .contains("compacted")
                }

                buckets[bucket_index].chunks.sort_by_cached_key(|c| {
                    let file_size = MemoryFs::get_file_size(c).unwrap();
                    let is_compacted = is_compacted(c);

                    if is_compacted {
                        total_compacted_size += file_size;
                    } else {
                        total_uncompacted_size += file_size;
                    }
                    // The first taken is the smallest non-compacted bucket
                    Reverse((is_compacted, file_size))
                });

                if total_uncompacted_size < COMPACT_THRESHOLD_BYTES
                    && (total_uncompacted_size as f64)
                        < COMPACT_THRESHOLD_RATIO * (total_compacted_size as f64)
                {
                    // Skip compaction if thresholds are not met
                    continue;
                }

                let mut chosen_size = 0;
                let mut taken_compacted_size = 0;

                // Choose buckets until one of two conditions is met:
                // 1. The next bucket would add up to a size of compacted buckets / uncompacted buckets > 1/1.5
                // 2. Four buckets were already selected and the number of sequences is greater than the maximum amount
                // The second condition is checked below, after the processing of each bucket
                while let Some(bucket) = buckets[bucket_index].chunks.pop() {
                    let bucket_size = MemoryFs::get_file_size(&bucket).unwrap();

                    if is_compacted(&bucket) {
                        if (taken_compacted_size + bucket_size) as f64 * MAX_COMPACTED_IO_RATIO
                            > total_uncompacted_size as f64
                        {
                            // Add back the last unused chunk
                            buckets[bucket_index].chunks.push(bucket);
                            break;
                        }
                        taken_compacted_size += bucket_size;
                    }

                    chosen_size += bucket_size;
                    chosen_buckets.push(bucket);
                }

                // Do not compact if we have only one bucket
                if chosen_buckets.len() == 1 || chosen_size < MINIMUM_INPUT_SIZE {
                    buckets[bucket_index]
                        .chunks
                        .extend(chosen_buckets.drain(..));
                }

                if chosen_buckets.is_empty() {
                    continue;
                }

                drop(buckets);

                chosen_buckets.reverse();

                stats!(
                    let stat_start_time = ggcat_logging::get_stat_opt!(stats.start_time).elapsed();
                    let mut pop_stats = vec![];
                );

                let mut input_files_size = 0;
                let new_path = global_params.output_path.join(format!(
                    "compacted-{}.dat",
                    COMPACTED_INDEX.fetch_add(1, Ordering::Relaxed)
                ));

                let mut sequences_deltas = vec![0i64; MAXIMUM_SECOND_BUCKETS_COUNT];

                // let used_hash_bits = global_params.buckets.count().ilog2() as usize;
                // let second_buckets_log_max = std::cmp::min(
                //     global_params.common.global_counters[bucket_index]
                //         .len()
                //         .ilog2() as usize,
                //     MAXIMUM_SECOND_BUCKETS_COUNT.ilog2() as usize,
                // );

                let mut total_sequences = 0;
                let mut processed_buckets = 0;

                while let Some(bucket_path) = chosen_buckets.pop() {
                    stats!(
                        let pop_time = ggcat_logging::get_stat_opt!(stats.start_time).elapsed();
                    );
                    // Reading the buckets
                    let reader = AsyncBinaryReader::new(
                        &bucket_path,
                        true,
                        RemoveFileMode::Remove {
                            remove_fs: !KEEP_FILES.load(Ordering::Relaxed),
                        },
                        DEFAULT_PREFETCH_AMOUNT,
                    );

                    processed_buckets += 1;

                    let format_data: MinimizerBucketMode = reader.get_data_format_info().unwrap();
                    let mut checkpoint_rewrite_bucket;

                    creads_helper! {
                        helper_read_bucket_with_opt_multiplicity::<
                            E::ExtraData,
                            E::FLAGS_COUNT,
                            BucketModeFromBoolean<USE_SECOND_BUCKET>,
                            AssemblerMinimizerPosition
                        >(
                            &reader,
                            read_thread.clone(),
                            matches!(format_data, MinimizerBucketMode::Compacted),
                            AllowedCheckpointStrategy::DecompressOnly,
                            |_passtrough| unreachable!(),
                            |checkpoint_data| { checkpoint_rewrite_bucket = checkpoint_data.map(|d| d.target_subbucket); } ,
                            |data, _extra_buffer| {

                                let DeserializedRead { read, extra: _, multiplicity, flags, second_bucket, minimizer_pos, is_window_duplicate } = data;

                                let rewrite_bucket = checkpoint_rewrite_bucket.unwrap_or(second_bucket as u16);

                                // Restore if needed!
                                // .unwrap_or_else(|| E::RewriteBucketCompute::get_rewrite_bucket(global_params.common.k,
                                //     global_params.common.m,
                                //     &data,
                                //     used_hash_bits,
                                //     second_buckets_log_max,
                                // ));
                                sequences_deltas[rewrite_bucket as usize] += 1;

                                let super_kmers_buffer = &mut super_kmers_buffer[rewrite_bucket as usize];
                                let super_kmers_storage = &mut super_kmers_storage[rewrite_bucket as usize];

                                let new_read = CompressedReadIndipendent::from_read(&read, super_kmers_storage);
                                super_kmers_buffer.push(SuperKmerEntry { read: new_read, multiplicity, minimizer_pos, flags, is_window_duplicate });

                                if super_kmers_buffer.len() == super_kmers_buffer.capacity() {
                                    let super_kmers_hashmap = &mut super_kmers_hashmap[rewrite_bucket as usize];
                                    process_superkmers(super_kmers_hashmap, super_kmers_buffer, super_kmers_storage, &mut total_sequences);
                                }
                            },
                            thread_handle,
                            global_params.common.k
                        );
                    }

                    input_files_size += MemoryFs::get_file_size(&bucket_path).unwrap();
                    stats!(
                        let end_process_time = ggcat_logging::get_stat_opt!(stats.start_time).elapsed();
                        pop_stats.push(
                            ggcat_logging::stats::InputFileStats {
                                file_name: bucket_path.clone(),
                                file_size: MemoryFs::get_file_size(&bucket_path).unwrap(),
                                start_time: pop_time.into(),
                                end_time: end_process_time.into(),
                            }
                        );
                    );

                    // Do not process more buckets if it will increase the maximum number of allowed sequences
                    if !global_params.common.is_active.load(Ordering::Relaxed)
                        || processed_buckets >= 4 && total_sequences > MAXIMUM_SEQUENCES
                    {
                        break;
                    }
                }

                // Flush all the buffers
                for i in 0..super_kmers_buffer.len() {
                    if super_kmers_buffer[i].len() > 0 {
                        process_superkmers(
                            &mut super_kmers_hashmap[i],
                            &mut super_kmers_buffer[i],
                            &mut super_kmers_storage[i],
                            &mut total_sequences,
                        );
                    }
                }

                let new_bucket = CompressedBinaryWriter::new(
                    &new_path,
                    &(
                        get_memory_mode(SwapPriority::MinimizerBuckets),
                        MINIMIZER_BUCKETS_CHECKPOINT_SIZE,
                        get_compression_level_info(),
                    ),
                    0,
                    &MinimizerBucketMode::Compacted,
                );

                let mut serializer = CompressedReadsBucketDataSerializer::<
                    NonColoredManager,
                    E::FLAGS_COUNT,
                    NoSecondBucket,
                    WithMultiplicity,
                    AssemblerMinimizerPosition,
                >::new(global_params.common.k);

                stats!(
                    let stat_subbucket_compactions = vec![];
                );

                let mut buffer = Vec::with_capacity(DEFAULT_OUTPUT_BUFFER_SIZE);

                // TODO: SUPPORT COLORS?
                let out_extra_buffer =
                    <NonColoredManager as SequenceExtraDataTempBufferManagement>::new_temp_buffer();
                let empty_extra = NonColoredManager::default();

                for (rewrite_bucket, super_kmers_hashmap) in
                    super_kmers_hashmap.iter_mut().enumerate()
                {
                    if buffer.len() > 0 {
                        new_bucket.write_data(&buffer);
                        buffer.clear();
                    }

                    if super_kmers_hashmap.is_empty() {
                        continue;
                    }

                    new_bucket.set_checkpoint_data(
                        Some(&ReadsCheckpointData {
                            target_subbucket: rewrite_bucket as BucketIndexType,
                            sequences_count: super_kmers_hashmap.len(),
                        }),
                        None,
                    );

                    // stats!(
                    //     let start_subbucket = ggcat_logging::get_stat_opt!(stats.start_time).elapsed();
                    //     let super_kmers_count = super_kmers_hashmap.len();
                    // );
                    let super_kmers_storage = &mut super_kmers_storage[rewrite_bucket];

                    for SuperKmerEntry {
                        read,
                        multiplicity,
                        minimizer_pos,
                        flags,
                        is_window_duplicate,
                    } in super_kmers_hashmap.drain()
                    {
                        let read = read.as_reference(super_kmers_storage);
                        sequences_deltas[rewrite_bucket as usize] -= 1;

                        serializer.write_to(
                            &CompressedReadsBucketData::new_packed_with_multiplicity(
                                read,
                                flags,
                                0,
                                multiplicity,
                                minimizer_pos,
                                is_window_duplicate,
                            ),
                            &mut buffer,
                            &empty_extra,
                            &out_extra_buffer,
                        );
                        if buffer.len() > DEFAULT_OUTPUT_BUFFER_SIZE {
                            new_bucket.write_data(&buffer);
                            buffer.clear();
                        }
                    }

                    super_kmers_storage.clear();

                    // stats!(let reset_capacity_start = ggcat_logging::get_stat_opt!(stats.start_time).elapsed(););

                    // Reset the hashmap capacity
                    if super_kmers_hashmap.capacity() > MAX_COMPACTION_MAP_SUBBUCKET_ELEMENTS {
                        *super_kmers_hashmap =
                            HashTable::with_capacity(DEFAULT_COMPACTION_MAP_SUBBUCKET_ELEMENTS);
                    }

                    // stats!(
                    //     let reset_capacity_end = ggcat_logging::get_stat_opt!(stats.start_time).elapsed();
                    //     stat_subbucket_compactions.push(
                    //         ggcat_logging::stats::SubbucketReport {
                    //             subbucket_index: rewrite_bucket,
                    //             super_kmers_count,
                    //             start_time: start_subbucket.into(),
                    //             reset_capacity_time: reset_capacity_end.into(),
                    //             end_reset_capacity_time: reset_capacity_start.into(),
                    //         }
                    //     )
                    // );
                }

                if buffer.len() > 0 {
                    new_bucket.write_data(&buffer);
                }

                let new_path = new_bucket.get_path();
                new_bucket.finalize();

                // Update the final buckets with new info
                let mut buckets = global_params.buckets.get_stored_buckets().lock();
                buckets[bucket_index].was_compacted = true;
                buckets[bucket_index].chunks.push(new_path.clone());

                for unused_bucket in chosen_buckets {
                    buckets[bucket_index].chunks.push(unused_bucket);
                }

                for (counter, global_counter) in sequences_deltas
                    .iter()
                    .zip(global_params.common.global_counters[bucket_index].iter())
                {
                    assert!(*counter >= 0);
                    global_counter.fetch_sub(*counter as u64, Ordering::Relaxed);
                }

                global_params.common.compaction_offsets[bucket_index]
                    .fetch_add(sequences_deltas.iter().sum::<i64>(), Ordering::Relaxed);

                let output_files_size = MemoryFs::get_file_size(&new_path).unwrap();
                let compression_ratio = input_files_size as f64 / output_files_size as f64;

                // Update the average compression ratio
                {
                    // Emprirically chosen to avoid too many compactions on non compactable datasets
                    const TARGET_COMPACTION_RATIO: f64 = 1.5;

                    let mut ratios = global_params.compaction_ratios[bucket_index].lock();
                    ratios.avg_ratio = (ratios.avg_ratio * (ratios.compactions as f64)
                        + compression_ratio)
                        / (ratios.compactions as f64 + 1.0);
                    ratios.compactions += 1;
                    ratios.skipped = 0;

                    if ratios.avg_ratio * 0.5 + compression_ratio * 0.5 < TARGET_COMPACTION_RATIO {
                        ratios.skip_step = 10.min(ratios.skip_step + 2);
                    } else {
                        ratios.skip_step = ratios.skip_step.saturating_sub(4);
                    }
                }

                stats!(
                    let end_time = ggcat_logging::get_stat_opt!(stats.start_time).elapsed();
                );

                stats!(
                    stats
                        .assembler
                        .compact_reports
                        .push(ggcat_logging::stats::CompactReport {
                            report_id: generate_stat_id!(),
                            bucket_index: init_data.bucket_index as usize,
                            input_files: pop_stats,
                            output_file: new_path,
                            start_time: stat_start_time.into(),
                            end_time: end_time.into(),
                            subbucket_reports: stat_subbucket_compactions,
                            input_total_size: input_files_size,
                            output_total_size: output_files_size,
                            compression_ratio,
                        })
                );
            }
        }
    }
}
