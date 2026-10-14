use std::{
    any::TypeId,
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
use colors::non_colored::NonColoredManager;
use config::{
    BucketIndexType, DEFAULT_COMPACTION_MAP_SUBBUCKET_ELEMENTS, DEFAULT_OUTPUT_BUFFER_SIZE,
    KEEP_FILES, MINIMIZER_BUCKETS_CHECKPOINT_SIZE, MINIMIZER_BUCKETS_COMPACTED_CHECKPOINT_SIZE,
    MultiplicityCounterType, SwapPriority, get_compression_level_info, get_memory_mode,
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
            SequenceExtraDataCombiner, SequenceExtraDataConsecutiveCompression, TempBuffer,
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
        writers::{
            compressed_binary_writer::CompressedBinaryWriter,
            lock_free_binary_writer::LockFreeBinaryWriter,
        },
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

/// Whether a compacted chunk came out of a light compaction.
///
/// The kind lives in the file name rather than beside it, so a compaction that
/// picks chunks up can tell what produced them without any state surviving in
/// memory between calls.
fn is_light_chunk(path: &Path) -> bool {
    path.file_name()
        .and_then(|name| name.to_str())
        .is_some_and(|name| name.contains("light-"))
}

pub struct BucketsCompactor<
    // `Copy` because narrow records are staged by value, unwidened.
    SingleData: SequenceExtraDataConsecutiveCompression + Sync + Send + Copy + 'static,
    MultipleData: SequenceExtraDataCombiner<SingleDataType = SingleData> + Sync + Send + Copy + 'static,
    FlagsCount: typenum::Unsigned,
> {
    read_thread: Arc<AsyncReaderThread>,

    // The single sub-bucket hashmap
    super_kmers_hashmap: FuzzyHashmap<u8, 0>,

    // The single sub-bucket extra buffer with multiplicity
    super_kmers_extra_buffer: TempBuffer<MultipleData>,

    // The uncompacted sk buffer
    uncompacted_super_kmers_buffer: Vec<
        ReadMemStorage<
            ResizableVec<u8, DEFAULT_COMPACTION_MAP_SUBBUCKET_ELEMENTS>,
            MultipleData,
            WithMultiplicity,
            AssemblerMinimizerPosition,
            true,
        >,
    >,

    /// Records that arrived narrow, staged narrow.
    ///
    /// A bypassed record is never folded with anything, so widening it on the
    /// way in only to narrow it again on the way out is pure cost. Keeping it
    /// as it arrived is what the compactor did before the deduplicator's wide
    /// format was imposed on every record, folded or not.
    uncompacted_narrow_buffer: Vec<
        ReadMemStorage<
            ResizableVec<u8, DEFAULT_COMPACTION_MAP_SUBBUCKET_ELEMENTS>,
            SingleData,
            NoMultiplicity,
            AssemblerMinimizerPosition,
            true,
        >,
    >,

    /// Holds what the narrow staging's extra data points at.
    ///
    /// An extra data entry is stored by value, and for the colour types it is a
    /// range into a buffer rather than the colours themselves, so the bytes it
    /// refers to have to be copied somewhere that outlives the decode.
    uncompacted_narrow_extra_buffer: TempBuffer<SingleData>,

    // The uncompacted extra buffer
    uncompacted_super_kmers_extra_buffer: TempBuffer<MultipleData>,

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
            super_kmers_extra_buffer: MultipleData::new_temp_buffer(),
            uncompacted_super_kmers_buffer: (0..second_buckets.total_buckets_count)
                .map(|_| ReadMemStorage::new(ResizableVec::new()))
                .collect(),
            uncompacted_narrow_buffer: (0..second_buckets.total_buckets_count)
                .map(|_| ReadMemStorage::new(ResizableVec::new()))
                .collect(),
            uncompacted_narrow_extra_buffer: SingleData::new_temp_buffer(),
            uncompacted_super_kmers_extra_buffer: MultipleData::new_temp_buffer(),
            k,
            target_chunk_size,
            _phantom: PhantomData,
        }
    }

    #[inline(always)]
    fn process_compactable_superkmer<
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

        let found = memstorage_decode_reads_changing::<E, AssemblerMinimizerPosition, true>(
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

        memstorage_encode_read::<E, WithFixedMultiplicity, AssemblerMinimizerPosition, true>(
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
    /// `narrow_bucket` holds the chunks a bypassing deduplicator wrote in the
    /// bucketing threads' own format. They are compacted together with the wide
    /// ones, as a single uncompacted blob.
    pub fn compact_buckets(
        &mut self,
        uncompacted_bucket: &Mutex<MultiChunkBucket>,
        narrow_bucket: Option<&Mutex<MultiChunkBucket>>,
        compacted_bucket: &Mutex<MultiChunkBucket>,
        _bucket_index: usize,
        output_path: &Path,
    ) {
        let file_mode = get_memory_mode(SwapPriority::MinimizerBuckets);
        let plain_init = (file_mode, MINIMIZER_BUCKETS_CHECKPOINT_SIZE);
        let compressed_init = (
            file_mode,
            MINIMIZER_BUCKETS_COMPACTED_CHECKPOINT_SIZE,
            get_compression_level_info(),
        );
        if TypeId::of::<SingleData>() == TypeId::of::<NonColoredManager>() {
            self.compact_buckets_with_writers::<LockFreeBinaryWriter, LockFreeBinaryWriter>(
                uncompacted_bucket,
                narrow_bucket,
                compacted_bucket,
                output_path,
                _bucket_index,
                &plain_init,
                &plain_init,
            );
        } else {
            // Layer LZ4 over both colored outputs, including run-length encoded color sets.
            self.compact_buckets_with_writers::<CompressedBinaryWriter, CompressedBinaryWriter>(
                uncompacted_bucket,
                narrow_bucket,
                compacted_bucket,
                output_path,
                _bucket_index,
                &compressed_init,
                &compressed_init,
            );
        }
    }

    fn compact_buckets_with_writers<MultiWriter: LockFreeBucket, SingleWriter: LockFreeBucket>(
        &mut self,
        uncompacted_bucket: &Mutex<MultiChunkBucket>,
        narrow_bucket: Option<&Mutex<MultiChunkBucket>>,
        compacted_bucket: &Mutex<MultiChunkBucket>,
        output_path: &Path,
        _bucket_index: usize,
        multi_writer_init: &MultiWriter::InitData,
        single_writer_init: &SingleWriter::InitData,
    ) {
        static COMPACTED_INDEX: AtomicUsize = AtomicUsize::new(0);

        /// How far the light chunks have to outweigh the smallest extended one
        /// before an extended compaction folds them in.
        const LIGHT_PROMOTION_RATIO: f64 = 2.0;

        const COMPACTED_VS_UNCOMPACTED_RATIO: f64 = 0.5;
        const COMPACTED_VS_UNCOMPACTED_RATIO_FORCED: f64 = 2.0;

        // Allow compaction only if the extra data can be combined
        // otherwise the compaction is needed only for splitting in sub-buckets
        let allow_compaction = MultipleData::ALLOW_COMBINE;

        // There are two kinds of compaction. A *light* one folds this bucket's
        // uncompacted chunks together and stops there. An *extended* one also
        // re-reads chunks that were compacted before, which is what merges
        // super-kmers across compactions -- and what costs a second pass over
        // data that has already been read once.
        //
        // The extended kind runs only when the light chunks that have piled up
        // since outweigh the smallest extended chunk, which is the point at
        // which folding them in is worth a pass over it. Until the first
        // extended chunk exists any light chunk is enough, so the first light
        // output is promoted immediately. The uncompacted chunks are not part of
        // this: they are processed either way, so they say nothing about which
        // kind to run.
        let extended_compaction = allow_compaction && {
            let bucket = compacted_bucket.lock();
            let mut light_total = 0u64;
            let mut smallest_extended: Option<u64> = None;
            for chunk in &bucket.chunks {
                let size = MemoryFs::get_file_size(chunk).unwrap() as u64;
                if is_light_chunk(chunk) {
                    light_total += size;
                } else {
                    smallest_extended =
                        Some(smallest_extended.map_or(size, |smallest| smallest.min(size)));
                }
            }
            light_total as f64 > LIGHT_PROMOTION_RATIO * smallest_extended.unwrap_or(0) as f64
        };

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
            /// Written by a bypassing deduplicator, so it holds the narrow
            /// records rather than the wide ones.
            narrow: bool,
            path: PathBuf,
        }

        // Uncompacted, both flavours. They go into one list because they are one
        // blob as far as compaction is concerned: only the decoder differs.
        //
        // A narrow chunk holds more records per byte than a wide one, so this
        // size slightly under-counts the work it stands for. It only bounds how
        // much already-compacted data may be read back alongside, so erring low
        // is the safe direction.
        for (bucket, narrow) in [
            Some((uncompacted_bucket, false)),
            narrow_bucket.map(|b| (b, true)),
        ]
        .into_iter()
        .flatten()
        {
            let mut bucket = bucket.lock();

            while let Some(chunk) = bucket.chunks.pop() {
                let chunk_size = MemoryFs::get_file_size(&chunk).unwrap();
                taken_uncompacted_size += chunk_size;
                uncompacted_chosen_chunks.push(ChosenChunk {
                    narrow,
                    path: chunk,
                });
            }
        }

        // Abort if no uncompacted chunks are found
        if uncompacted_chosen_chunks.len() == 0 {
            return;
        }

        let force_advanced_compaction;

        // Compacted -- only an extended compaction reads these back.
        if extended_compaction {
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
                    // Unread for compacted chunks: those are routed by the
                    // format tag in their own header, not by this flag.
                    narrow: false,
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
            );
            input_files_size += bucket_file_index.get_file_size() as usize;

            debug_assert_eq!(
                bucket_file_index.get_data_format_info::<MinimizerBucketMode>(),
                if bucket.narrow {
                    MinimizerBucketMode::UncompactedNarrow
                } else {
                    MinimizerBucketMode::Single
                },
                "an uncompacted chunk was taken from the wrong list"
            );

            // Both flavours land in the same staging buffer, keyed by the
            // in-band second bucket byte, so everything downstream of here sees
            // one uncompacted blob.
            let chunks = bucket_file_index.into_chunks();
            if bucket.narrow {
                // The bucketing threads' own format: one extra data entry and an
                // implicit multiplicity of one. Widening has to happen inside
                // the callback, because `helper_read_bucket` clears the
                // decoder's buffer after every record.
                helper_read_bucket::<
                    SingleData,
                    WithSecondBucket,
                    NoMultiplicity,
                    AssemblerMinimizerPosition,
                    FlagsCount,
                    NoAlignment,
                >(
                    chunks,
                    None,
                    |read, extra_buffer| {
                        // Not widened: it is widened only if this compaction
                        // actually folds, and not at all if it just groups
                        // records into sub-buckets. What it points at still has
                        // to cross into this compactor's own buffer, because
                        // the decoder's is cleared after every record.
                        let extra = SingleData::copy_extra_from(
                            read.extra,
                            extra_buffer,
                            &mut self.uncompacted_narrow_extra_buffer,
                        );
                        self.uncompacted_narrow_buffer[read.second_bucket as usize].encode_read(
                            &DeserializedRead {
                                read: read.read,
                                multiplicity: read.multiplicity,
                                minimizer_pos: read.minimizer_pos,
                                flags: read.flags,
                                extra,
                                second_bucket: 0,
                            },
                        );
                    },
                    self.k,
                );
            } else {
                // Drained by a deduplicator, so the entries already carry the
                // combinable extra data and a multiplicity and only have to
                // cross into this compactor's arena.
                helper_read_bucket::<
                    MultipleData,
                    WithSecondBucket,
                    WithMultiplicity,
                    AssemblerMinimizerPosition,
                    FlagsCount,
                    NoAlignment,
                >(
                    chunks,
                    None,
                    |read, extra_buffer| {
                        self.uncompacted_super_kmers_buffer[read.second_bucket as usize]
                            .encode_read(&DeserializedRead {
                                read: read.read,
                                multiplicity: read.multiplicity,
                                minimizer_pos: read.minimizer_pos,
                                flags: read.flags,
                                extra: MultipleData::copy_extra_from(
                                    read.extra,
                                    extra_buffer,
                                    &mut self.uncompacted_super_kmers_extra_buffer,
                                ),
                                second_bucket: 0,
                            });
                    },
                    self.k,
                );
            }
        }

        // self.uncompacted_super_kmers_storage
        //     .reserve(HASH_MAX_OVERREAD);

        let compact_index = COMPACTED_INDEX.fetch_add(1, Ordering::Relaxed);

        // The kind is carried in the name, which is where the next compaction
        // reads it back from: nothing else has to remember what produced a
        // chunk. `comp-mult-*` and `comp-single-*` still prefix both kinds, so
        // everything that already keys off those names keeps working.
        let kind = if extended_compaction { "" } else { "light-" };
        let new_path_multi = if allow_compaction {
            Some(output_path.join(format!("comp-mult-{}{}.dat", kind, compact_index)))
        } else {
            None
        };
        let new_path_single =
            output_path.join(format!("comp-single-{}{}.dat", kind, compact_index));

        let new_bucket_multi = new_path_multi.as_ref().map(|new_path_multi| {
            MultiWriter::new(
                &new_path_multi,
                multi_writer_init,
                0,
                &MinimizerBucketMode::Compacted,
            )
        });

        let new_bucket_single = SingleWriter::new(
            &new_path_single,
            single_writer_init,
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

        let mut single_to_multiple_extra_buffer = MultipleData::new_temp_buffer();
        let mut multiple_to_single_extra_buffer = SingleData::new_temp_buffer();

        let sub_buckets = SplittedBucket::generate(
            compacted_chosen_chunks.iter().map(|c| &c.path),
            RemoveFileMode::Remove {
                remove_fs: !KEEP_FILES.load(Ordering::Relaxed),
            },
            self.uncompacted_super_kmers_buffer.len(),
        );

        let mut super_kmers_temp = [
            ReadMemStorage::<
                _,
                MultipleData,
                WithFixedMultiplicity,
                AssemblerMinimizerPosition,
                true,
            >::new(vec![]),
            ReadMemStorage::new(vec![]),
        ];
        let mut multi_buffer = Vec::with_capacity(DEFAULT_OUTPUT_BUFFER_SIZE);
        let mut single_buffer = Vec::with_capacity(DEFAULT_OUTPUT_BUFFER_SIZE);

        for (sub_bucket_index, ((compacted_sub_bucket, uncompacted_buffer), narrow_buffer)) in
            sub_buckets
                .into_iter()
                .zip(&mut self.uncompacted_super_kmers_buffer)
                .zip(&mut self.uncompacted_narrow_buffer)
                .enumerate()
        {
            let mut total_sequences = 0;

            // stats!(
            //     let pop_time = ggcat_logging::get_stat_opt!(stats.start_time).elapsed();
            // );

            if uncompacted_buffer.sequences_count() == 0
                && narrow_buffer.sequences_count() == 0
                && compacted_sub_bucket.is_none()
            {
                // Skip the sub-bucket if it has no data
                continue;
            }

            let total_sequences_count = uncompacted_buffer.sequences_count()
                + narrow_buffer.sequences_count()
                + compacted_sub_bucket
                    .as_ref()
                    .map(|c| c.sequences_count)
                    .unwrap_or(0) as usize;

            // Clear all temp data
            serializer_single.reset();

            if allow_compaction {
                MultipleData::clear_temp_buffer(&mut self.super_kmers_extra_buffer);
                serializer_multi.reset();
                // Clear and reset the hashmap capacity (double it to allow less collisions)
                self.super_kmers_hashmap
                    .initialize(total_sequences_count.next_power_of_two() * 2);

                if let Some(mut sub_bucket) = compacted_sub_bucket {
                    input_files_size += sub_bucket.total_size as usize;

                    decode_sequences::<SingleData, MultipleData, FlagsCount, NoAlignmentWithOverflow>(
                        Some(self.read_thread.clone()),
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

                            Self::process_compactable_superkmer::<MultipleData>(
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
                    Self::process_compactable_superkmer::<MultipleData>(
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

                // Records that arrived narrow are widened here rather than on
                // the way into staging, so the cost falls only on a compaction
                // that actually folds.
                narrow_buffer.decode_reads(|entry| {
                    let extra = MultipleData::from_single_entry(
                        &mut self.uncompacted_super_kmers_extra_buffer,
                        entry.extra,
                        &self.uncompacted_narrow_extra_buffer,
                    )
                    .0;
                    Self::process_compactable_superkmer::<MultipleData>(
                        SuperKmerEntryRef {
                            read: entry.read,
                            multiplicity: entry.multiplicity,
                            minimizer_pos: entry.minimizer_pos,
                            flags: entry.flags,
                            extra,
                        },
                        &mut self.super_kmers_hashmap,
                        &mut total_sequences,
                        &self.uncompacted_super_kmers_extra_buffer,
                        &mut self.super_kmers_extra_buffer,
                    );
                });
                narrow_buffer.clear();

                // Split between single (multiplicity = 1) and multiple superkmers
                self.super_kmers_hashmap.process_elements(
                    |sks| {
                        memstorage_decode_reads::<
                            MultipleData,
                            WithFixedMultiplicity,
                            AssemblerMinimizerPosition,
                            true,
                        >(sks.as_ptr(), sks.len(), |sk| {
                            let mult_type = (sk.multiplicity > 1) as usize;
                            super_kmers_temp[mult_type].encode_read(&sk);
                        })
                    },
                    false,
                );

                // Create the new sub-bucket checkpoints
                {
                    if let Some(ref new_bucket_multi) = new_bucket_multi {
                        new_bucket_multi.set_checkpoint_data(
                            Some(&ReadsCheckpointData {
                                target_subbucket: sub_bucket_index as BucketIndexType,
                                sequences_count: super_kmers_temp[1].sequences_count(),
                            }),
                            None,
                        );
                    }

                    new_bucket_single.set_checkpoint_data(
                        Some(&ReadsCheckpointData {
                            target_subbucket: sub_bucket_index as BucketIndexType,
                            sequences_count: super_kmers_temp[0].sequences_count(),
                        }),
                        None,
                    );
                }

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

                let new_bucket_multi = new_bucket_multi.as_ref().unwrap();

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
            } else {
                // Not compacting, just write the uncompacted buffer into the output bucket
                new_bucket_single.set_checkpoint_data(
                    Some(&ReadsCheckpointData {
                        target_subbucket: sub_bucket_index as BucketIndexType,
                        sequences_count: uncompacted_buffer.sequences_count(),
                    }),
                    None,
                );

                uncompacted_buffer.decode_reads(|entry| {
                    SingleData::clear_temp_buffer(&mut multiple_to_single_extra_buffer);
                    let extra = entry.extra.to_single(
                        &self.uncompacted_super_kmers_extra_buffer,
                        &mut multiple_to_single_extra_buffer,
                    );

                    serializer_single.write_to(
                        &CompressedReadsBucketData::new_packed(
                            entry.read,
                            entry.flags,
                            0,
                            entry.minimizer_pos,
                        ),
                        &mut single_buffer,
                        &extra,
                        &multiple_to_single_extra_buffer,
                    );

                    if single_buffer.len() > DEFAULT_OUTPUT_BUFFER_SIZE {
                        new_bucket_single.write_data(&single_buffer);
                        single_buffer.clear();
                    }
                });
                uncompacted_buffer.clear();

                // Already in the single form the output asks for.
                narrow_buffer.decode_reads(|entry| {
                    serializer_single.write_to(
                        &CompressedReadsBucketData::new_packed(
                            entry.read,
                            entry.flags,
                            0,
                            entry.minimizer_pos,
                        ),
                        &mut single_buffer,
                        &entry.extra,
                        &self.uncompacted_narrow_extra_buffer,
                    );

                    if single_buffer.len() > DEFAULT_OUTPUT_BUFFER_SIZE {
                        new_bucket_single.write_data(&single_buffer);
                        single_buffer.clear();
                    }
                });
                narrow_buffer.clear();
            }

            if multi_buffer.len() > 0 {
                new_bucket_multi
                    .as_ref()
                    .map(|b| b.write_data(&multi_buffer));
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
            SingleData::clear_temp_buffer(&mut self.uncompacted_narrow_extra_buffer);
        }

        let new_path = new_bucket_multi.as_ref().map(|b| b.get_path());
        new_bucket_multi.map(|b| b.finalize());

        let new_path_single = new_bucket_single.get_path();
        new_bucket_single.finalize();

        // Update the final buckets with new info
        let mut bucket = compacted_bucket.lock();
        if let Some(ref new_path) = new_path {
            bucket.chunks.push(new_path.clone());
        }
        bucket.chunks.push(new_path_single.clone());

        let output_files_size = new_path
            .map(|new_path| MemoryFs::get_file_size(&new_path).unwrap())
            .unwrap_or(0)
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
