pub mod extra_data;

use std::{
    cmp::Reverse,
    marker::PhantomData,
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

use crate::{MinimizerBucketMode, MinimizerBucketingExecutorFactory, helper_read_bucket_with_type};
use config::{
    BucketIndexType, DEFAULT_COMPACTION_MAP_SUBBUCKET_ELEMENTS,
    DEFAULT_COMPACTION_STORAGE_PER_BUCKET_SIZE, DEFAULT_OUTPUT_BUFFER_SIZE,
    DEFAULT_PREFETCH_AMOUNT, KEEP_FILES, MAX_COMPACTION_MAP_SUBBUCKET_ELEMENTS,
    MINIMIZER_BUCKETS_COMPACTED_CHECKPOINT_SIZE, MultiplicityCounterType, SwapPriority,
    get_compression_level_info, get_memory_mode,
};
use ggcat_logging::stats;
use hashbrown::{HashTable, hash_table::Entry};
use io::{
    compressed_read::CompressedReadIndipendent,
    concurrent::temp_reads::{
        creads_utils::{
            AssemblerMinimizerPosition, CompressedReadsBucketData,
            CompressedReadsBucketDataSerializer, DeserializedRead, NoMultiplicity, NoSecondBucket,
            ReadsCheckpointData, WithMultiplicity,
        },
        extra_data::{
            SequenceExtraDataCombiner, SequenceExtraDataConsecutiveCompression,
            SequenceExtraDataTempBufferManagement,
        },
    },
};
use parallel_processor::{
    buckets::{
        BucketsCount, LockFreeBucket, MultiChunkBucket,
        readers::{
            compressed_decoder::CompressedStreamDecoder, typed_binary_reader::AsyncReaderThread,
        },
        writers::compressed_binary_writer::CompressedBinaryWriter,
    },
    execution_manager::executor::{AsyncExecutor, ExecutorReceiver},
    memory_fs::{RemoveFileMode, file::reader::FileReader},
    mt_debug_counters::{
        counter::{AtomicCounter, SumMode},
        declare_counter_i64,
    },
};
use parallel_processor::{
    buckets::{
        bucket_writer::BucketItemSerializer, readers::binary_reader::ChunkedBinaryReaderIndex,
    },
    memory_fs::MemoryFs,
};
use parking_lot::Mutex;
use utils::track;

pub struct MinimizerBucketingCompactor<
    SingleData: SequenceExtraDataConsecutiveCompression + Sync + Send + 'static,
    MultipleData: SequenceExtraDataCombiner<SingleDataType = SingleData> + Sync + Send + 'static,
    Executor: MinimizerBucketingExecutorFactory<ReadExtraData = SingleData> + Sync + Send + 'static,
> {
    _phantom: PhantomData<(Executor, SingleData, MultipleData)>, // mem_tracker: MemoryTracker<Self>,
}

static ADDR_WAITING_COUNTER: AtomicCounter<SumMode> =
    declare_counter_i64!("mb_addr_wait_compactor", SumMode, false);

struct SuperKmerEntry<E> {
    read: CompressedReadIndipendent,
    multiplicity: MultiplicityCounterType,
    extra: E,
    minimizer_pos: u16,
    flags: u8,
    is_window_duplicate: bool,
}

pub struct BucketsCompactor<
    SingleData: SequenceExtraDataConsecutiveCompression + Sync + Send + 'static,
    MultipleData: SequenceExtraDataCombiner<SingleDataType = SingleData> + Sync + Send + 'static,
    FlagsCount: typenum::Unsigned,
> {
    read_thread: Arc<AsyncReaderThread>,
    super_kmers_hashmap: Vec<HashTable<SuperKmerEntry<MultipleData>>>,
    super_kmers_buffer: Vec<Vec<SuperKmerEntry<MultipleData>>>,
    super_kmers_extra_buffer:
        Vec<<MultipleData as SequenceExtraDataTempBufferManagement>::TempBuffer>,
    super_kmers_storage: Vec<Vec<u8>>,
    k: usize,
    target_chunk_size: u64,
    _phantom: PhantomData<FlagsCount>,
}

impl<
    SingleData: SequenceExtraDataConsecutiveCompression + Sync + Send + 'static,
    MultipleData: SequenceExtraDataCombiner<SingleDataType = SingleData> + Sync + Send + 'static,
    FlagsCount: typenum::Unsigned,
> BucketsCompactor<SingleData, MultipleData, FlagsCount>
{
    pub fn new(k: usize, second_buckets: &BucketsCount, target_chunk_size: u64) -> Self {
        Self {
            read_thread: AsyncReaderThread::new(DEFAULT_OUTPUT_BUFFER_SIZE, 4),
            super_kmers_hashmap: (0..second_buckets.total_buckets_count)
                .map(|_| HashTable::with_capacity(DEFAULT_COMPACTION_MAP_SUBBUCKET_ELEMENTS))
                .collect(),
            super_kmers_buffer: (0..second_buckets.total_buckets_count)
                .map(|_| Vec::with_capacity(DEFAULT_COMPACTION_MAP_SUBBUCKET_ELEMENTS))
                .collect(),
            super_kmers_extra_buffer: (0..second_buckets.total_buckets_count)
                .map(|_| <MultipleData as SequenceExtraDataTempBufferManagement>::new_temp_buffer())
                .collect(),
            super_kmers_storage: (0..second_buckets.total_buckets_count)
                .map(|_| Vec::with_capacity(DEFAULT_COMPACTION_STORAGE_PER_BUCKET_SIZE))
                .collect(),
            k,
            target_chunk_size,
            _phantom: PhantomData,
        }
    }

    fn process_superkmers<
        E: SequenceExtraDataCombiner + SequenceExtraDataConsecutiveCompression,
    >(
        super_kmers_hashmap: &mut HashTable<SuperKmerEntry<E>>,
        super_kmers_buffer: &mut Vec<SuperKmerEntry<E>>,
        super_kmers_storage: &mut Vec<u8>,
        total_sequences: &mut usize,
        in_extra_buffer: &E::TempBuffer,
        out_extra_buffer: &mut E::TempBuffer,
    ) {
        debug_assert!(super_kmers_storage.len() > 0);
        let mut copy_pos = super_kmers_buffer[0].read.buffer_start_index();

        for SuperKmerEntry {
            read,
            multiplicity,
            minimizer_pos,
            extra,
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
                        && a.read.get_packed_slice_aligned(super_kmers_storage) == read_slice
                        && a.flags == flags
                },
                |v| v.read.compute_hash_aligned(super_kmers_storage),
            ) {
                Entry::Occupied(mut entry) => {
                    let entry = entry.get_mut();
                    entry.multiplicity += multiplicity;
                    entry
                        .extra
                        .combine_entries(out_extra_buffer, extra, in_extra_buffer);
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

                    let extra = E::copy_extra_from(extra, in_extra_buffer, out_extra_buffer);

                    position.insert(SuperKmerEntry {
                        read: new_read,
                        multiplicity,
                        minimizer_pos,
                        flags,
                        is_window_duplicate,
                        extra,
                    });
                    *total_sequences += 1;
                }
            }
        }

        super_kmers_storage.truncate(copy_pos);
    }

    #[inline(never)]
    pub fn compact_buckets(
        &mut self,
        uncompacted_bucket: &Mutex<MultiChunkBucket>,
        compacted_bucket: &Mutex<MultiChunkBucket>,
        _bucket_index: usize,
        output_path: &Path,
    ) {
        static COMPACTED_INDEX: AtomicUsize = AtomicUsize::new(0);

        const COMPACTED_VS_UNCOMPACTED_RATIO: f64 = 0.5;

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

        let mut chosen_chunks = vec![];

        let mut taken_uncompacted_size = 0;
        let mut taken_compacted_size = 0;

        struct ChosenChunk {
            _compacted: bool,
            path: PathBuf,
        }

        // Uncompacted
        {
            let mut bucket = uncompacted_bucket.lock();

            while let Some(chunk) = bucket.chunks.pop() {
                let chunk_size = MemoryFs::get_file_size(&chunk).unwrap();
                taken_uncompacted_size += chunk_size;
                chosen_chunks.push(ChosenChunk {
                    _compacted: false,
                    path: chunk,
                });
            }
        }

        // Abort if no uncompacted chunks are found
        if chosen_chunks.len() == 0 {
            return;
        }

        let force_advanced_compaction;

        // Compacted
        {
            let mut bucket = compacted_bucket.lock();

            let max_compacted =
                (taken_uncompacted_size as f64 * COMPACTED_VS_UNCOMPACTED_RATIO) as u64;

            let mut compactable_chunks_size = 0;

            let compactable_buckets_threshold = 2 * taken_uncompacted_size as u64;

            bucket.chunks.sort_by_cached_key(|f| {
                let is_single = f.file_name().unwrap().to_str().unwrap().contains("single");

                let real_file_size = MemoryFs::get_file_size(f).unwrap() as u64;

                let file_size = if is_single {
                    // Penalize single chunks in compaction
                    real_file_size * 2
                } else {
                    real_file_size
                };

                if file_size < compactable_buckets_threshold {
                    compactable_chunks_size += file_size;
                }
                // Do not compact single chunks
                Reverse(file_size)
            });

            // Force an advanced compaction step if there is enough data
            force_advanced_compaction = compactable_chunks_size > self.target_chunk_size;

            while let Some(chunk) = bucket.chunks.last() {
                let chunk_size = MemoryFs::get_file_size(&chunk).unwrap() as u64;

                let force_compactable =
                    force_advanced_compaction && chunk_size < compactable_buckets_threshold;

                if !force_compactable && taken_compacted_size + chunk_size > max_compacted {
                    break;
                }

                taken_compacted_size += chunk_size;
                chosen_chunks.push(ChosenChunk {
                    _compacted: true,
                    path: bucket.chunks.pop().unwrap(),
                });
            }
        }

        stats!(
            let stat_start_time = ggcat_logging::get_stat_opt!(stats.start_time).elapsed();
            let mut pop_stats = vec![];
        );

        let mut input_files_size = 0;

        let compact_index = COMPACTED_INDEX.fetch_add(1, Ordering::Relaxed);

        let new_path = output_path.join(format!("comp-mult-{}.dat", compact_index));
        let new_path_single = output_path.join(format!("comp-single-{}.dat", compact_index));

        let mut total_sequences = 0;

        let mut out_extra_buffer =
            <MultipleData as SequenceExtraDataTempBufferManagement>::new_temp_buffer();

        // {
        //     struct ReaderChunk<T> {
        //         file_reader: FileReader,
        //         length: usize,
        //         extra_data: T,
        //     }

        //     let bucket_chunks = vec![vec![]; MAX_SECOND_BUCKETS_COUNT];
        //     for chunk in &chosen_chunks[x..] {
        //         let file_chunks: Vec<ReaderChunk<ReadsCheckpointData>> =
        //             AsyncBinaryReader::list_chunks(chunk, remove_on_drop: true);

        //         for file_chunk in file_chunks {
        //             bucket_chunks[file_chunk.extra_data.target_subbucket as usize].push(file_chunk);
        //         }

        //         for subbucket_chunks in bucket_chunks.into_iter().enumerate() {
        //             let reader = AsyncBinaryReader::read_chunks(subbucket_chunks);
        //             for el in reader.get_elements() {
        //                 // Process the data
        //             }
        //         }
        //     }
        // }

        while let Some(bucket_path) = chosen_chunks.pop() {
            stats!(
                let pop_time = ggcat_logging::get_stat_opt!(stats.start_time).elapsed();
            );

            // Reading the buckets
            let bucket_file_index = ChunkedBinaryReaderIndex::from_file(
                &bucket_path.path,
                RemoveFileMode::Remove {
                    remove_fs: !KEEP_FILES.load(Ordering::Relaxed),
                },
                DEFAULT_PREFETCH_AMOUNT,
            );

            let file_size = bucket_file_index.get_file_size() as usize;
            input_files_size += file_size;

            let format_data: MinimizerBucketMode = bucket_file_index.get_data_format_info();

            for chunk in bucket_file_index.into_chunks() {
                let checkpoint_rewrite_bucket = chunk
                    .get_extra_data::<ReadsCheckpointData>()
                    .map(|d| d.target_subbucket);
                helper_read_bucket_with_type::<
                    SingleData,
                    MultipleData,
                    AssemblerMinimizerPosition,
                    FlagsCount,
                >(
                    vec![chunk],
                    Some(self.read_thread.clone()),
                    format_data,
                    #[inline(always)]
                    |data, in_extra_buffer| {
                        let DeserializedRead {
                            read,
                            extra,
                            multiplicity,
                            flags,
                            second_bucket,
                            minimizer_pos,
                            is_window_duplicate,
                        } = data;

                        let rewrite_bucket =
                            checkpoint_rewrite_bucket.unwrap_or(second_bucket as u16);

                        let super_kmers_buffer =
                            &mut self.super_kmers_buffer[rewrite_bucket as usize];
                        let super_kmers_storage =
                            &mut self.super_kmers_storage[rewrite_bucket as usize];
                        let super_kmers_extra_buffer =
                            &mut self.super_kmers_extra_buffer[rewrite_bucket as usize];

                        let new_read =
                            CompressedReadIndipendent::from_read(&read, super_kmers_storage);
                        super_kmers_buffer.push(SuperKmerEntry {
                            read: new_read,
                            multiplicity,
                            minimizer_pos,
                            flags,
                            is_window_duplicate,
                            extra: MultipleData::copy_extra_from(
                                extra,
                                in_extra_buffer,
                                super_kmers_extra_buffer,
                            ),
                        });

                        if super_kmers_buffer.len() == super_kmers_buffer.capacity() {
                            let super_kmers_hashmap =
                                &mut self.super_kmers_hashmap[rewrite_bucket as usize];
                            Self::process_superkmers::<MultipleData>(
                                super_kmers_hashmap,
                                super_kmers_buffer,
                                super_kmers_storage,
                                &mut total_sequences,
                                super_kmers_extra_buffer,
                                &mut out_extra_buffer,
                            );
                            MultipleData::clear_temp_buffer(super_kmers_extra_buffer);
                        }
                    },
                    self.k,
                );
            }

            stats!(
                let end_process_time = ggcat_logging::get_stat_opt!(stats.start_time).elapsed();
                pop_stats.push(
                    ggcat_logging::stats::InputFileStats {
                        file_name: bucket_path.path.clone(),
                        file_size,
                        start_time: pop_time.into(),
                        end_time: end_process_time.into(),
                    }
                );
            );
        }

        // Flush all the buffers
        for i in 0..self.super_kmers_buffer.len() {
            if self.super_kmers_buffer[i].len() > 0 {
                Self::process_superkmers(
                    &mut self.super_kmers_hashmap[i],
                    &mut self.super_kmers_buffer[i],
                    &mut self.super_kmers_storage[i],
                    &mut total_sequences,
                    &mut self.super_kmers_extra_buffer[i],
                    &mut out_extra_buffer,
                );
            }
        }

        let new_bucket = CompressedBinaryWriter::new(
            &new_path,
            &(
                get_memory_mode(SwapPriority::MinimizerBuckets),
                MINIMIZER_BUCKETS_COMPACTED_CHECKPOINT_SIZE,
                get_compression_level_info(),
            ),
            0,
            &MinimizerBucketMode::Compacted,
        );

        let new_bucket_single = CompressedBinaryWriter::new(
            &new_path_single,
            &(
                get_memory_mode(SwapPriority::MinimizerBuckets),
                MINIMIZER_BUCKETS_COMPACTED_CHECKPOINT_SIZE,
                get_compression_level_info(),
            ),
            0,
            &MinimizerBucketMode::SingleGrouped,
        );

        let mut serializer = CompressedReadsBucketDataSerializer::<
            MultipleData,
            NoSecondBucket,
            WithMultiplicity,
            AssemblerMinimizerPosition,
            FlagsCount,
        >::new(self.k);

        let mut serializer_single = CompressedReadsBucketDataSerializer::<
            SingleData,
            NoSecondBucket,
            NoMultiplicity,
            AssemblerMinimizerPosition,
            FlagsCount,
        >::new(self.k);

        stats!(
            let stat_subbucket_compactions = vec![];
        );

        let mut buffer = Vec::with_capacity(DEFAULT_OUTPUT_BUFFER_SIZE);
        let mut single_buffer = Vec::with_capacity(DEFAULT_OUTPUT_BUFFER_SIZE);

        let mut single_extra_buffer =
            <SingleData as SequenceExtraDataTempBufferManagement>::new_temp_buffer();

        let mut super_kmers_temp = [vec![], vec![]];

        for (rewrite_bucket, super_kmers_hashmap) in self.super_kmers_hashmap.iter_mut().enumerate()
        {
            if super_kmers_hashmap.is_empty() {
                continue;
            }

            super_kmers_temp[0].reserve(super_kmers_hashmap.len());
            super_kmers_temp[1].reserve(super_kmers_hashmap.len());

            for sk in super_kmers_hashmap.drain() {
                let mult_type = (sk.multiplicity > 1) as usize;
                super_kmers_temp[mult_type].push(sk);
            }

            new_bucket.set_checkpoint_data(
                Some(&ReadsCheckpointData {
                    target_subbucket: rewrite_bucket as BucketIndexType,
                    sequences_count: super_kmers_temp[1].len(),
                }),
                None,
            );

            new_bucket_single.set_checkpoint_data(
                Some(&ReadsCheckpointData {
                    target_subbucket: rewrite_bucket as BucketIndexType,
                    sequences_count: super_kmers_temp[0].len(),
                }),
                None,
            );

            // stats!(
            //     let start_subbucket = ggcat_logging::get_stat_opt!(stats.start_time).elapsed();
            //     let super_kmers_count = super_kmers_hashmap.len();
            // );
            let super_kmers_storage = &mut self.super_kmers_storage[rewrite_bucket];

            // Handle superkmers with multiplicity == 1
            for SuperKmerEntry {
                read,
                multiplicity: _,
                minimizer_pos,
                mut extra,
                flags,
                is_window_duplicate,
            } in super_kmers_temp[0].drain(..)
            {
                let read = read.as_reference(super_kmers_storage);
                extra.prepare_for_serialization(&mut out_extra_buffer);

                SingleData::clear_temp_buffer(&mut single_extra_buffer);
                let extra = extra.to_single(&out_extra_buffer, &mut single_extra_buffer);

                serializer_single.write_to(
                    &CompressedReadsBucketData::new_packed(
                        read,
                        flags,
                        0,
                        minimizer_pos,
                        is_window_duplicate,
                    ),
                    &mut single_buffer,
                    &extra,
                    &single_extra_buffer,
                );
                if single_buffer.len() > DEFAULT_OUTPUT_BUFFER_SIZE {
                    new_bucket_single.write_data(&single_buffer);
                    single_buffer.clear();
                }
            }

            // Handle superkmers with multiplicity > 1
            for SuperKmerEntry {
                read,
                multiplicity,
                minimizer_pos,
                mut extra,
                flags,
                is_window_duplicate,
            } in super_kmers_temp[1].drain(..)
            {
                let read = read.as_reference(super_kmers_storage);
                extra.prepare_for_serialization(&mut out_extra_buffer);

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
                    &extra,
                    &out_extra_buffer,
                );
                if buffer.len() > DEFAULT_OUTPUT_BUFFER_SIZE {
                    new_bucket.write_data(&buffer);
                    buffer.clear();
                }
            }

            super_kmers_temp[0].clear();
            super_kmers_temp[1].clear();
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
            if buffer.len() > 0 {
                new_bucket.write_data(&buffer);
                buffer.clear();
            }
            if single_buffer.len() > 0 {
                new_bucket_single.write_data(&single_buffer);
                single_buffer.clear();
            }
        }

        // println!(
        //     "Tot multiple: {} Multiple: {} Single: {} total ratio: real: {:.2} compr: {:.2} bucket: {}",
        //     tot_multiple,
        //     multiple,
        //     single,
        //     (tot_multiple as f64 / single as f64),
        //     (multiple as f64 / single as f64),
        //     _bucket_index
        // );

        let new_path = new_bucket.get_path();
        new_bucket.finalize();

        let new_path_single = new_bucket_single.get_path();
        new_bucket_single.finalize();

        // {
        //     let new_compacted_size = MemoryFs::get_file_size(&new_path).unwrap();
        //     let new_compacted_single_size = MemoryFs::get_file_size(&new_path_single).unwrap();

        //     println!(
        //         "Input size: C: {} U: {} output size: C: {} S: {} compr ratio: {} force: {}",
        //         taken_compacted_size,
        //         taken_uncompacted_size,
        //         new_compacted_size,
        //         new_compacted_single_size,
        //         input_files_size as f64 / (new_compacted_size + new_compacted_single_size) as f64,
        //         force_advanced_compaction
        //     )
        // }

        // Update the final buckets with new info
        let mut bucket = compacted_bucket.lock();
        bucket.chunks.push(new_path.clone());
        bucket.chunks.push(new_path_single.clone());

        let output_files_size = MemoryFs::get_file_size(&new_path).unwrap()
            + MemoryFs::get_file_size(&new_path_single).unwrap();
        let _compression_ratio = input_files_size as f64 / output_files_size as f64;

        stats!(
            let end_time = ggcat_logging::get_stat_opt!(stats.start_time).elapsed();
        );

        stats!(
            stats
                .assembler
                .compact_reports
                .push(ggcat_logging::stats::CompactReport {
                    report_id: generate_stat_id!(),
                    bucket_index: _bucket_index,
                    input_files: pop_stats,
                    output_file: new_path,
                    start_time: stat_start_time.into(),
                    end_time: end_time.into(),
                    subbucket_reports: stat_subbucket_compactions,
                    input_total_size: input_files_size,
                    output_total_size: output_files_size,
                    compression_ratio: _compression_ratio,
                })
        );
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use colors::non_colored::{NonColoredManager, NonColoredMultipleColors};
    use config::{DEFAULT_OUTPUT_BUFFER_SIZE, DEFAULT_PREFETCH_AMOUNT};
    use io::concurrent::temp_reads::creads_utils::AssemblerMinimizerPosition;
    use parallel_processor::{
        buckets::readers::{
            binary_reader::ChunkedBinaryReaderIndex,
            typed_binary_reader::{
                AllowedCheckpointStrategy, AsyncBinaryReader, AsyncReaderThread,
            },
        },
        memory_fs::RemoveFileMode,
    };

    use crate::{MinimizerBucketMode, helper_read_bucket_with_type};

    #[test]
    #[ignore]
    fn test_bucket_decoding() {
        // let read_thread = AsyncReaderThread::new(DEFAULT_OUTPUT_BUFFER_SIZE, 4);
        let bucket_path = PathBuf::from(
            // "../../.temp_files/build_graph_4f1a5aac-a6df-4559-a0b3-5d8b8c9357e0/compacted-8586.dat.0",
            "../../.temp_files/build_graph_4f1a5aac-a6df-4559-a0b3-5d8b8c9357e0/compacted-9800.dat.0",
        );

        let file_index = ChunkedBinaryReaderIndex::from_file(
            &bucket_path,
            RemoveFileMode::Keep,
            DEFAULT_PREFETCH_AMOUNT,
        );

        let format_data: MinimizerBucketMode = file_index.get_data_format_info();

        println!("Format: {:?}", format_data);

        helper_read_bucket_with_type::<
            NonColoredManager,
            NonColoredMultipleColors<NonColoredManager>,
            AssemblerMinimizerPosition,
            typenum::U2,
        >(
            file_index.into_chunks(),
            None,
            format_data,
            #[inline(always)]
            |_data, _in_extra_buffer| {},
            27,
        );
    }
}
