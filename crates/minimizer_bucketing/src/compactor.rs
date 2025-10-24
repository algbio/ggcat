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

use crate::{
    MinimizerBucketMode, MinimizerBucketingExecutorFactory, decode_helper::decode_sequences,
    split_buckets::SplittedBucket,
};
use config::{
    BucketIndexType, DEFAULT_COMPACTION_MAP_SUBBUCKET_ELEMENTS, DEFAULT_OUTPUT_BUFFER_SIZE,
    DEFAULT_PREFETCH_AMOUNT, KEEP_FILES, MINIMIZER_BUCKETS_CHECKPOINT_SIZE,
    MultiplicityCounterType, SwapPriority, get_memory_mode,
};
use ggcat_logging::stats;
use hashes::HashableSequence;
use io::{
    compressed_read::CompressedRead,
    concurrent::temp_reads::{
        creads_utils::{
            AssemblerMinimizerPosition, CompressedReadsBucketData,
            CompressedReadsBucketDataSerializer, DeserializedRead, NoAlignment,
            NoAlignmentWithOverflow, NoMultiplicity, NoSecondBucket, ReadsCheckpointData,
            WithFixedMultiplicity, WithMultiplicity, WithSecondBucket, helpers::helper_read_bucket,
        },
        extra_data::{
            SequenceExtraDataCombiner, SequenceExtraDataConsecutiveCompression,
            SequenceExtraDataTempBufferManagement,
        },
    },
    memstorage::{
        ReadMemStorage, memstorage_decode_reads, memstorage_decode_reads_changing,
        memstorage_encode_read,
    },
};
use nightly_quirks::branch_pred::likely;
use parallel_processor::{
    buckets::{
        BucketsCount, LockFreeBucket, MultiChunkBucket,
        readers::typed_binary_reader::AsyncReaderThread,
        writers::lock_free_binary_writer::LockFreeBinaryWriter,
    },
    memory_fs::RemoveFileMode,
};
use parallel_processor::{
    buckets::{
        bucket_writer::BucketItemSerializer, readers::binary_reader::ChunkedBinaryReaderIndex,
    },
    memory_fs::MemoryFs,
};
use parking_lot::Mutex;
use utils::{fuzzy_hashmap::FuzzyHashmap, resize_containers::ResizableVec};

pub struct MinimizerBucketingCompactor<
    SingleData: SequenceExtraDataConsecutiveCompression + Sync + Send + 'static,
    MultipleData: SequenceExtraDataCombiner<SingleDataType = SingleData> + Sync + Send + 'static,
    Executor: MinimizerBucketingExecutorFactory<ReadExtraData = SingleData> + Sync + Send + 'static,
> {
    _phantom: PhantomData<(Executor, SingleData, MultipleData)>, // mem_tracker: MemoryTracker<Self>,
}

struct SuperKmerEntryRef<'a, E> {
    read: CompressedRead<'a>,
    multiplicity: MultiplicityCounterType,
    extra: E,
    minimizer_pos: u16,
    flags: u8,
}

pub struct BucketsCompactor<
    SingleData: SequenceExtraDataConsecutiveCompression + Sync + Send + 'static,
    MultipleData: SequenceExtraDataCombiner<SingleDataType = SingleData> + Sync + Send + Copy + 'static,
    FlagsCount: typenum::Unsigned,
> {
    read_thread: Arc<AsyncReaderThread>,

    // The single sub-bucket hashmap
    super_kmers_hashmap: FuzzyHashmap<u8, 0>,

    // The single sub-bucket extra buffer with multiplicity
    super_kmers_extra_buffer: <MultipleData as SequenceExtraDataTempBufferManagement>::TempBuffer,

    // The uncompacted sk buffer
    uncompacted_super_kmers_buffer: Vec<
        ReadMemStorage<
            ResizableVec<u8, DEFAULT_COMPACTION_MAP_SUBBUCKET_ELEMENTS>,
            MultipleData,
            WithMultiplicity,
            AssemblerMinimizerPosition,
        >,
    >,

    // The uncompacted extra buffer
    uncompacted_super_kmers_extra_buffer:
        <MultipleData as SequenceExtraDataTempBufferManagement>::TempBuffer,

    k: usize,
    target_chunk_size: u64,
    _phantom: PhantomData<FlagsCount>,
}

impl<
    SingleData: SequenceExtraDataConsecutiveCompression + Sync + Send + Copy + 'static,
    MultipleData: SequenceExtraDataCombiner<SingleDataType = SingleData> + Sync + Send + Copy + 'static,
    FlagsCount: typenum::Unsigned,
> BucketsCompactor<SingleData, MultipleData, FlagsCount>
{
    pub fn new(k: usize, second_buckets: &BucketsCount, target_chunk_size: u64) -> Self {
        Self {
            read_thread: AsyncReaderThread::new(DEFAULT_OUTPUT_BUFFER_SIZE, 4),
            super_kmers_hashmap: FuzzyHashmap::new(DEFAULT_COMPACTION_MAP_SUBBUCKET_ELEMENTS),
            super_kmers_extra_buffer:
                <MultipleData as SequenceExtraDataTempBufferManagement>::new_temp_buffer(),
            uncompacted_super_kmers_buffer: (0..second_buckets.total_buckets_count)
                .map(|_| ReadMemStorage::new(ResizableVec::new()))
                .collect(),
            uncompacted_super_kmers_extra_buffer:
                <MultipleData as SequenceExtraDataTempBufferManagement>::new_temp_buffer(),
            k,
            target_chunk_size,
            _phantom: PhantomData,
        }
    }

    #[inline(always)]
    fn process_superkmer<
        E: SequenceExtraDataCombiner + SequenceExtraDataConsecutiveCompression + Copy,
    >(
        super_kmer: SuperKmerEntryRef<E>,
        super_kmers_hashmap: &mut FuzzyHashmap<u8, 0>,
        total_sequences: &mut usize,
        in_extra_buffer: &E::TempBuffer,
        out_extra_buffer: &mut E::TempBuffer,
    ) {
        let SuperKmerEntryRef {
            read,
            multiplicity,
            minimizer_pos,
            extra,
            flags,
        } = super_kmer;

        let read_hash = unsafe { read.compute_hash_aligned_overflow16() };
        let read_slice = read.get_packed_slice();

        let elements = super_kmers_hashmap.get_elements_mut(read_hash);

        let found = memstorage_decode_reads_changing::<E, AssemblerMinimizerPosition>(
            elements.as_mut_ptr(),
            elements.len(),
            |entry, entry_extra, entry_multiplicity| {
                if likely({
                    entry.read.bases_count() == read.bases_count()
                        && entry.read.get_packed_slice() == read_slice
                        && entry.flags == flags
                }) {
                    unsafe {
                        std::ptr::write_unaligned(
                            entry_multiplicity,
                            std::ptr::read_unaligned(entry_multiplicity) + multiplicity,
                        );
                        let mut new_extra = std::ptr::read_unaligned(entry_extra);
                        new_extra.combine_entries(out_extra_buffer, extra, in_extra_buffer);
                        std::ptr::write_unaligned(entry_extra, new_extra);
                    }
                    return true;
                }
                false
            },
        );

        if found {
            return;
        }

        let extra = E::copy_extra_from(extra, in_extra_buffer, out_extra_buffer);

        memstorage_encode_read::<E, WithFixedMultiplicity, AssemblerMinimizerPosition>(
            &DeserializedRead {
                read,
                multiplicity,
                minimizer_pos,
                flags,
                extra,
                second_bucket: 0,
            },
            |needed, reserved| {
                // TODO: Manage reserved
                super_kmers_hashmap.allocator_reserve_additional(reserved);
                super_kmers_hashmap.allocate_elements(read_hash, needed)
            },
        );
        // super_kmers_storage.reserve(HASH_MAX_OVERREAD);
        *total_sequences += 1;
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
        const COMPACTED_VS_UNCOMPACTED_RATIO_FORCED: f64 = 2.0;

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

        let mut uncompacted_chosen_chunks = vec![];
        let mut compacted_chosen_chunks = vec![];

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
                uncompacted_chosen_chunks.push(ChosenChunk {
                    _compacted: false,
                    path: chunk,
                });
            }
        }

        // Abort if no uncompacted chunks are found
        if uncompacted_chosen_chunks.len() == 0 {
            return;
        }

        let force_advanced_compaction;

        // Compacted
        {
            let mut bucket = compacted_bucket.lock();

            let max_compacted =
                (taken_uncompacted_size as f64 * COMPACTED_VS_UNCOMPACTED_RATIO) as u64;

            let max_compacted_forced =
                (taken_uncompacted_size as f64 * COMPACTED_VS_UNCOMPACTED_RATIO_FORCED) as u64;

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

                let force_compactable = force_advanced_compaction
                    && chunk_size < compactable_buckets_threshold
                    && taken_compacted_size + chunk_size < max_compacted_forced;

                if !force_compactable && taken_compacted_size + chunk_size > max_compacted {
                    break;
                }

                taken_compacted_size += chunk_size;
                compacted_chosen_chunks.push(ChosenChunk {
                    _compacted: true,
                    path: bucket.chunks.pop().unwrap(),
                });
            }
        }

        stats!(
            let stat_start_time = ggcat_logging::get_stat_opt!(stats.start_time).elapsed();
            let pop_stats = vec![];
        );

        let mut input_files_size = 0;

        // Save in memory the uncompacted data
        for bucket in uncompacted_chosen_chunks {
            let bucket_file_index = ChunkedBinaryReaderIndex::from_file(
                &bucket.path,
                RemoveFileMode::Remove {
                    remove_fs: !KEEP_FILES.load(Ordering::Relaxed),
                },
                DEFAULT_PREFETCH_AMOUNT,
            );
            input_files_size += bucket_file_index.get_file_size() as usize;

            helper_read_bucket::<
                SingleData,
                WithSecondBucket,
                NoMultiplicity,
                AssemblerMinimizerPosition,
                FlagsCount,
                NoAlignment,
            >(
                bucket_file_index.into_chunks(),
                None,
                |read, extra_buffer| {
                    self.uncompacted_super_kmers_buffer[read.second_bucket as usize].encode_read(
                        &DeserializedRead {
                            read: read.read,
                            multiplicity: read.multiplicity,
                            minimizer_pos: read.minimizer_pos,
                            flags: read.flags,
                            extra: MultipleData::from_single_entry(
                                &mut self.uncompacted_super_kmers_extra_buffer,
                                read.extra,
                                extra_buffer,
                            )
                            .0,
                            second_bucket: 0,
                        },
                    );
                },
                self.k,
            );
        }

        // self.uncompacted_super_kmers_storage
        //     .reserve(HASH_MAX_OVERREAD);

        let compact_index = COMPACTED_INDEX.fetch_add(1, Ordering::Relaxed);

        let new_path_multi = output_path.join(format!("comp-mult-{}.dat", compact_index));
        let new_path_single = output_path.join(format!("comp-single-{}.dat", compact_index));

        let new_bucket_multi = LockFreeBinaryWriter::new(
            &new_path_multi,
            &(
                get_memory_mode(SwapPriority::MinimizerBuckets),
                MINIMIZER_BUCKETS_CHECKPOINT_SIZE,
            ),
            0,
            &MinimizerBucketMode::Compacted,
        );

        let new_bucket_single = LockFreeBinaryWriter::new(
            &new_path_single,
            &(
                get_memory_mode(SwapPriority::MinimizerBuckets),
                MINIMIZER_BUCKETS_CHECKPOINT_SIZE,
            ),
            0,
            &MinimizerBucketMode::SingleGrouped,
        );

        let mut serializer_multi = CompressedReadsBucketDataSerializer::<
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

        let mut single_to_multiple_extra_buffer =
            <MultipleData as SequenceExtraDataTempBufferManagement>::new_temp_buffer();
        let mut multiple_to_single_extra_buffer =
            <SingleData as SequenceExtraDataTempBufferManagement>::new_temp_buffer();

        let sub_buckets = SplittedBucket::generate(
            compacted_chosen_chunks.iter().map(|c| &c.path),
            RemoveFileMode::Remove {
                remove_fs: !KEEP_FILES.load(Ordering::Relaxed),
            },
            DEFAULT_PREFETCH_AMOUNT,
            self.uncompacted_super_kmers_buffer.len(),
        );

        let mut super_kmers_temp = [ReadMemStorage::<_, MultipleData, WithFixedMultiplicity, AssemblerMinimizerPosition>::new(vec![]), ReadMemStorage::new(vec![])];
        let mut multi_buffer = Vec::with_capacity(DEFAULT_OUTPUT_BUFFER_SIZE);
        let mut single_buffer = Vec::with_capacity(DEFAULT_OUTPUT_BUFFER_SIZE);

        for (sub_bucket_index, (compacted_sub_bucket, uncompacted_buffer)) in sub_buckets
            .into_iter()
            .zip(&mut self.uncompacted_super_kmers_buffer)
            .enumerate()
        {
            let mut total_sequences = 0;

            // stats!(
            //     let pop_time = ggcat_logging::get_stat_opt!(stats.start_time).elapsed();
            // );

            if uncompacted_buffer.sequences_count() == 0 && compacted_sub_bucket.is_none() {
                // Skip the sub-bucket if it has no data
                continue;
            }

            let total_sequences_count = uncompacted_buffer.sequences_count()
                + compacted_sub_bucket
                    .as_ref()
                    .map(|c| c.sequences_count)
                    .unwrap_or(0) as usize;

            // Clear all temp data
            MultipleData::clear_temp_buffer(&mut self.super_kmers_extra_buffer);
            serializer_single.reset();
            serializer_multi.reset();
            // Clear and reset the hashmap capacity (double it to allow less collisions)
            self.super_kmers_hashmap
                .initialize(total_sequences_count.next_power_of_two() * 2);

            if let Some(mut sub_bucket) = compacted_sub_bucket {
                input_files_size += sub_bucket.total_size as usize;

                decode_sequences::<SingleData, MultipleData, FlagsCount, NoAlignmentWithOverflow>(
                    self.read_thread.clone(),
                    &mut single_to_multiple_extra_buffer,
                    &mut sub_bucket,
                    self.k,
                    #[inline(always)]
                    |data, in_extra_buffer| {
                        let DeserializedRead {
                            read,
                            extra,
                            multiplicity,
                            flags,
                            second_bucket: _,
                            minimizer_pos,
                        } = data;

                        Self::process_superkmer::<MultipleData>(
                            SuperKmerEntryRef {
                                read,
                                multiplicity,
                                minimizer_pos,
                                flags,
                                extra,
                            },
                            &mut self.super_kmers_hashmap,
                            &mut total_sequences,
                            &in_extra_buffer,
                            &mut self.super_kmers_extra_buffer,
                        );
                    },
                );
            }

            uncompacted_buffer.decode_reads(|entry| {
                Self::process_superkmer::<MultipleData>(
                    SuperKmerEntryRef {
                        read: entry.read,
                        multiplicity: entry.multiplicity,
                        minimizer_pos: entry.minimizer_pos,
                        flags: entry.flags,
                        extra: entry.extra,
                    },
                    &mut self.super_kmers_hashmap,
                    &mut total_sequences,
                    &self.uncompacted_super_kmers_extra_buffer,
                    &mut self.super_kmers_extra_buffer,
                );
            });
            uncompacted_buffer.clear();
            // for entry in uncompacted_buffer.drain(..) {}

            // super_kmers_temp[0].reserve(total_sequences);
            // super_kmers_temp[1].reserve(total_sequences);

            // Split between single (multiplicity = 1) and multiple superkmers
            self.super_kmers_hashmap.process_elements(
                |sks| {
                    memstorage_decode_reads::<
                        MultipleData,
                        WithFixedMultiplicity,
                        AssemblerMinimizerPosition,
                    >(sks.as_ptr(), sks.len(), |sk| {
                        let mult_type = (sk.multiplicity > 1) as usize;
                        super_kmers_temp[mult_type].encode_read(&sk);
                    })
                },
                false,
            );

            new_bucket_multi.set_checkpoint_data(
                Some(&ReadsCheckpointData {
                    target_subbucket: sub_bucket_index as BucketIndexType,
                    sequences_count: super_kmers_temp[1].sequences_count(),
                }),
                None,
            );

            new_bucket_single.set_checkpoint_data(
                Some(&ReadsCheckpointData {
                    target_subbucket: sub_bucket_index as BucketIndexType,
                    sequences_count: super_kmers_temp[0].sequences_count(),
                }),
                None,
            );

            // Handle superkmers with multiplicity == 1
            super_kmers_temp[0].decode_reads(
                |DeserializedRead {
                     read,
                     extra,
                     minimizer_pos,
                     flags,
                     ..
                 }| {
                    // Not needed because the entry has a single value
                    // extra.prepare_for_serialization(&mut self.super_kmers_extra_buffer);

                    SingleData::clear_temp_buffer(&mut multiple_to_single_extra_buffer);
                    let extra = extra.to_single(
                        &self.super_kmers_extra_buffer,
                        &mut multiple_to_single_extra_buffer,
                    );

                    serializer_single.write_to(
                        &CompressedReadsBucketData::new_packed(read, flags, 0, minimizer_pos),
                        &mut single_buffer,
                        &extra,
                        &multiple_to_single_extra_buffer,
                    );

                    if single_buffer.len() > DEFAULT_OUTPUT_BUFFER_SIZE {
                        new_bucket_single.write_data(&single_buffer);
                        single_buffer.clear();
                    }
                },
            );

            // Handle superkmers with multiplicity > 1
            super_kmers_temp[1].decode_reads(
                |DeserializedRead {
                     read,
                     mut extra,
                     multiplicity,
                     minimizer_pos,
                     flags,
                     ..
                 }| {
                    extra.prepare_for_serialization(&mut self.super_kmers_extra_buffer);

                    serializer_multi.write_to(
                        &CompressedReadsBucketData::new_packed_with_multiplicity(
                            read,
                            flags,
                            0,
                            multiplicity,
                            minimizer_pos,
                        ),
                        &mut multi_buffer,
                        &extra,
                        &self.super_kmers_extra_buffer,
                    );
                    if multi_buffer.len() > DEFAULT_OUTPUT_BUFFER_SIZE {
                        new_bucket_multi.write_data(&multi_buffer);
                        multi_buffer.clear();
                    }
                },
            );

            super_kmers_temp[0].clear();
            super_kmers_temp[1].clear();
            self.super_kmers_hashmap.reset_allocator();

            if multi_buffer.len() > 0 {
                new_bucket_multi.write_data(&multi_buffer);
                multi_buffer.clear();
            }
            if single_buffer.len() > 0 {
                new_bucket_single.write_data(&single_buffer);
                single_buffer.clear();
            }
        }

        // Final clearing of all buffers
        {
            MultipleData::clear_temp_buffer(&mut self.super_kmers_extra_buffer);
            MultipleData::clear_temp_buffer(&mut self.uncompacted_super_kmers_extra_buffer);
        }

        let new_path = new_bucket_multi.get_path();
        new_bucket_multi.finalize();

        let new_path_single = new_bucket_single.get_path();
        new_bucket_single.finalize();

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

        // let compacted_time = start_time.elapsed();

        // static COMPACTED_TIME: AtomicU64 = AtomicU64::new(0);
        // static UNCOMPACTED_TIME: AtomicU64 = AtomicU64::new(0);

        // let compacted_micros =
        //     COMPACTED_TIME.fetch_add(compacted_time.as_micros() as u64, Ordering::Relaxed);
        // let uncompacted_micros =
        //     UNCOMPACTED_TIME.fetch_add(uncompacted_time.as_micros() as u64, Ordering::Relaxed);

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
                    subbucket_reports: vec![],
                    input_total_size: input_files_size,
                    output_total_size: output_files_size,
                    compression_ratio: _compression_ratio,
                })
        );
    }
}
