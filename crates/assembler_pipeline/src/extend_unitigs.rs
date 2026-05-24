use byteorder::ReadBytesExt;
use colors::colors_manager::ColorsMergeManager;
use colors::colors_manager::color_types::{PartialUnitigsColorStructure, TempUnitigColorStructure};
use colors::colors_manager::{ColorsManager, color_types};
use config::{
    BucketIndexType, DEFAULT_COMPACTION_MAP_SUBBUCKET_ELEMENTS, DEFAULT_OUTPUT_BUFFER_SIZE,
    DEFAULT_PER_CPU_BUFFER_SIZE, KEEP_FILES, MAX_EXTREMITIES_HASHMAP_SIZE, MAX_SUBPARTITIONS_COUNT,
    MAX_SUBSUBPARTITION_SIZE, MINIMUM_LOG_DELTA_TIME, PARTIAL_UNITIGS_COMPACTED_CHECKPOINT_SIZE,
    SwapPriority, get_compression_level_info, get_memory_mode,
};
use dashmap::DashMap;
use ggcat_logging::info;
use hashbrown::hash_table::Entry;
use hashbrown::{HashMap, HashTable};
use hashes::extremal::{DelayedHashComputation, HashGenerator};
use hashes::{ExtendableHashTraitType, HashFunctionFactory, HashableSequence};
use io::compressed_read::{CompressedRead, CompressedReadIndipendent};
use io::concurrent::structured_sequences::concurrent::FastaWriterConcurrentBuffer;
use io::concurrent::structured_sequences::{
    IdentSequenceWriter, SequenceAbundanceType, StructuredSequenceBackend, StructuredSequenceWriter,
};
use io::concurrent::temp_reads::creads_utils::{
    AlignToMinimizerByteBoundary, AssemblerMinimizerPosition, CompressedReadsBucketData,
    CompressedReadsBucketDataSerializer, DeserializedRead, NoMinimizerPosition, NoMultiplicity,
    NoSecondBucket, ReadData, ToReadData,
};
use io::concurrent::temp_reads::extra_data::{
    self, SequenceExtraData, SequenceExtraDataConsecutiveCompression,
    SequenceExtraDataTempBufferManagement,
};
use io::structs::unitig_link::{UnitigFlags, UnitigIndex, UnitigLinkSerializer};
use io::varint::{VARINT_MAX_SIZE, decode_varint, encode_varint};
use nightly_quirks::branch_pred::unlikely;
use nightly_quirks::slice_group_by::SliceGroupBy;
use parallel_processor::buckets::concurrent::{BucketsThreadBuffer, BucketsThreadDispatcher};
use parallel_processor::buckets::readers::binary_reader::ChunkedBinaryReaderIndex;
use parallel_processor::buckets::readers::typed_binary_reader::TypedStreamReader;
use parallel_processor::buckets::writers::compressed_binary_writer::CompressedBinaryWriter;
use parallel_processor::buckets::{BucketsCount, ExtraBuckets, MultiThreadBuckets, SingleBucket};
use parallel_processor::memory_fs::{MemoryFs, RemoveFileMode};
use parallel_processor::phase_times_monitor::PHASES_TIMES_MONITOR;
use parallel_processor::utils::scoped_thread_local::ScopedThreadLocal;
use parking_lot::Mutex;
use rayon::{current_num_threads, prelude::*};
use std::cmp::Reverse;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::{Duration, Instant};
use structs::partial_unitigs_extra_data::PartialUnitigExtraData;
use typenum::U4;
use utils::fast_rand_bool::FastRandBool;
use utils::fuzzy_buckets::FuzzyBuckets;
use utils::fuzzy_hashmap::FuzzyHashmap;

const HASH_ENDING_FLAG_MASK: u8 = 1;
const OTHER_END_FLAG_MASK: u8 = 2;
use structs::partial_unitigs_extra_data::INDIRECT_UNITIG_FLAG_MASK;

/// The chosen extremity is at the beginning of (the current orientation of) the sequence
fn chosen_extremity_at_beginning(flags: u8) -> bool {
    flags & HASH_ENDING_FLAG_MASK == 0
}

/// The chosen extremity is at the end of (the current orientation of) the sequence
fn chosen_extremity_at_end(flags: u8) -> bool {
    flags & HASH_ENDING_FLAG_MASK != 0
}

/// Both the extremities are open and need to be processed
fn both_extremities_are_open(flags: u8) -> bool {
    flags & OTHER_END_FLAG_MASK != 0
}

struct JoinedRead<'a, E> {
    read: CompressedRead<'a>,
    extra: E,
    flags: u8,
    completed: bool,
}

fn join_partial_unitig_data<CX: ColorsManager>(
    final_unitig_color: &mut <<CX as ColorsManager>::ColorsMergeManagerType as ColorsMergeManager>::TempUnitigColorStructure,
    final_extra_buffer: &mut <PartialUnitigsColorStructure<CX> as SequenceExtraDataTempBufferManagement>::TempBuffer,
    first_extra: &PartialUnitigExtraData<PartialUnitigsColorStructure<CX>>,
    first_buffer: &<PartialUnitigsColorStructure<CX> as SequenceExtraDataTempBufferManagement>::TempBuffer,
    second_extra: &PartialUnitigExtraData<PartialUnitigsColorStructure<CX>>,
    second_buffer: &<PartialUnitigsColorStructure<CX> as SequenceExtraDataTempBufferManagement>::TempBuffer,
) -> PartialUnitigExtraData<PartialUnitigsColorStructure<CX>> {
    match (first_extra, second_extra) {
        (
            PartialUnitigExtraData::Inline {
                colors: first_colors,
                #[cfg(feature = "support_kmer_counters")]
                    counters: first_counters,
            },
            PartialUnitigExtraData::Inline {
                colors: second_colors,
                #[cfg(feature = "support_kmer_counters")]
                    counters: second_counters,
            },
        ) => {
            #[cfg(feature = "support_kmer_counters")]
            let counters = io::concurrent::structured_sequences::SequenceAbundance {
                first: first_counters.first,
                sum: first_counters.sum + second_counters.sum - second_counters.first,
                last: second_counters.last,
            };

            CX::ColorsMergeManagerType::reset_unitig_color_structure(final_unitig_color);
            CX::ColorsMergeManagerType::join_structures::<false>(
                final_unitig_color,
                first_colors,
                first_buffer,
                0,
                None,
            );
            CX::ColorsMergeManagerType::join_structures::<false>(
                final_unitig_color,
                second_colors,
                second_buffer,
                1,
                None,
            );

            let writable_color = CX::ColorsMergeManagerType::encode_part_unitigs_colors(
                final_unitig_color,
                final_extra_buffer,
            );

            PartialUnitigExtraData::Inline {
                colors: writable_color,
                #[cfg(feature = "support_kmer_counters")]
                counters,
            }
        }
        _ => {
            unimplemented!()
        }
    }
}

fn get_color_and_counters<CX: ColorsManager>(
    extra: &PartialUnitigExtraData<PartialUnitigsColorStructure<CX>>,
) -> (
    PartialUnitigsColorStructure<CX>,
    io::concurrent::structured_sequences::SequenceAbundanceType,
) {
    match extra {
        PartialUnitigExtraData::Indirect { .. } => unimplemented!(),
        PartialUnitigExtraData::Inline {
            colors,
            #[cfg(feature = "support_kmer_counters")]
            counters,
        } => (
            *colors,
            match () {
                #[cfg(feature = "support_kmer_counters")]
                () => *counters,
                #[cfg(not(feature = "support_kmer_counters"))]
                () => (),
            },
        ),
    }
}

struct SubsplitBestOrientationAndFlags {
    should_rc: bool,
    new_flags: u8,
    new_bucket: BucketIndexType,
    new_subpartition: BucketIndexType,
    last_align: u16,
    is_circular: bool,
}

struct SubsplitGlobalData<MH: HashFunctionFactory> {
    k: usize,
    extremities_hashmaps_presence: Vec<Mutex<HashMap<MH::HashTypeUnextendable, bool>>>,
    extremities_hashmaps_symmetric_at_end: Vec<Mutex<HashMap<MH::HashTypeUnextendable, bool>>>,
    hashmap_size: AtomicUsize,
    input_buckets_count: BucketsCount,
    subpartitions_count: BucketsCount,
}

#[inline(always)]
fn find_best_orientation_and_flags<MH: HashFunctionFactory>(
    global_data: &SubsplitGlobalData<MH>,
    read: CompressedRead<'_>,
    mut flags: u8,
    fast_rand: &mut FastRandBool<1>,
    only_random: bool,
) -> SubsplitBestOrientationAndFlags {
    #[derive(Debug)]
    struct ExtremityData<MH: HashFunctionFactory> {
        bucket: BucketIndexType,
        subpartition: BucketIndexType,
        hash: MH::HashTypeExtendable,
    }

    #[inline(always)]
    fn compute_extremity_data<MH: HashFunctionFactory>(
        global_data: &SubsplitGlobalData<MH>,
        read: CompressedRead<'_>,
        beginning: bool,
    ) -> ExtremityData<MH> {
        let hash = HashGenerator::<MH>::get_extremal_hash(
            &DelayedHashComputation,
            read,
            global_data.k,
            beginning,
        );

        ExtremityData {
            bucket: MH::get_bucket(
                0,
                global_data.input_buckets_count.normal_buckets_count_log,
                hash.to_unextendable(),
            ),
            subpartition: MH::get_bucket(
                global_data.input_buckets_count.normal_buckets_count_log,
                global_data.subpartitions_count.normal_buckets_count_log,
                hash.to_unextendable(),
            ),
            hash,
        }
    }

    let lock_presence_hmaps_pair = |first_idx: usize, second_idx: usize| {
        // Avoid deadlocks by always locking in the same order (smaller first)
        if first_idx < second_idx {
            let first = global_data.extremities_hashmaps_presence[first_idx].lock();
            let second = global_data.extremities_hashmaps_presence[second_idx].lock();
            (first, Some(second))
        } else if second_idx < first_idx {
            let second = global_data.extremities_hashmaps_presence[second_idx].lock();
            let first = global_data.extremities_hashmaps_presence[first_idx].lock();
            (first, Some(second))
        } else {
            let first = global_data.extremities_hashmaps_presence[first_idx].lock();
            (first, None)
        }
    };

    let extremity_data =
        compute_extremity_data(global_data, read, chosen_extremity_at_beginning(flags));

    let (chosen_extremity, change_extremity, is_circular) = if both_extremities_are_open(flags) {
        let other_extremity_data =
            compute_extremity_data(global_data, read, !chosen_extremity_at_beginning(flags));

        // Compute the next bucket to put this read into
        if extremity_data.bucket < other_extremity_data.bucket {
            (extremity_data, false, false)
        } else if other_extremity_data.bucket < extremity_data.bucket {
            (other_extremity_data, true, false)
        } else {
            let change_extremity = if only_random
                || global_data.hashmap_size.load(Ordering::Relaxed) >= MAX_EXTREMITIES_HASHMAP_SIZE
            {
                fast_rand.get_randbool()
            } else {
                global_data.hashmap_size.fetch_add(1, Ordering::Relaxed);
                let mut change_extremity = false;

                let (mut hmap, mut other_hmap) = lock_presence_hmaps_pair(
                    extremity_data.subpartition as usize,
                    other_extremity_data.subpartition as usize,
                );

                // Choose the bucket that has the other entry or avoid the one that surely does not have it.
                if let Some(inserted_here) = hmap.get(&extremity_data.hash.to_unextendable()) {
                    change_extremity = !*inserted_here;
                } else if let Some(inserted_here) = other_hmap
                    .as_mut()
                    .unwrap_or(&mut hmap)
                    .get(&other_extremity_data.hash.to_unextendable())
                {
                    change_extremity = *inserted_here;
                }

                hmap.entry(extremity_data.hash.to_unextendable())
                    .or_insert(!change_extremity);

                other_hmap
                    .as_mut()
                    .unwrap_or(&mut hmap)
                    .entry(other_extremity_data.hash.to_unextendable())
                    .or_insert(change_extremity);
                change_extremity
            };

            let is_circular = extremity_data.hash.to_unextendable()
                == other_extremity_data.hash.to_unextendable();

            if change_extremity {
                (other_extremity_data, true, is_circular)
            } else {
                (extremity_data, false, is_circular)
            }
        }
    } else {
        (extremity_data, false, false)
    };

    let should_rc = if chosen_extremity.hash.is_rc_symmetric() {
        if only_random
            || global_data.hashmap_size.load(Ordering::Relaxed) >= MAX_EXTREMITIES_HASHMAP_SIZE
        {
            fast_rand.get_randbool()
        } else {
            global_data.hashmap_size.fetch_add(1, Ordering::Relaxed);

            // Case where there is only one ending but it is rc-symmetric, choose only if we need to change orientation
            let mut should_rc = false;
            global_data.extremities_hashmaps_symmetric_at_end
                [chosen_extremity.subpartition as usize]
                .lock()
                .entry(chosen_extremity.hash.to_unextendable())
                .and_modify(|other_is_at_end| {
                    should_rc =
                        chosen_extremity_at_beginning(flags) ^ change_extremity ^ *other_is_at_end;
                })
                .or_insert(chosen_extremity_at_end(flags) ^ change_extremity);
            should_rc
        }
    } else {
        !chosen_extremity.hash.is_forward()
    };

    // Change the extremity if needed
    if change_extremity ^ should_rc {
        flags ^= HASH_ENDING_FLAG_MASK;
    }
    return SubsplitBestOrientationAndFlags {
        should_rc,
        new_flags: flags,
        new_bucket: chosen_extremity.bucket,
        new_subpartition: chosen_extremity.subpartition,
        last_align: if chosen_extremity_at_beginning(flags) {
            0
        } else {
            (read.bases_count() - global_data.k) % 4
        } as u16,
        is_circular,
    };
}

#[inline(always)]
fn join_reads<'a, MH: HashFunctionFactory, CX: ColorsManager>(
    global_data: &SubsplitGlobalData<MH>,
    first_read: CompressedRead<'_>,
    first_buffer: &<PartialUnitigExtraData<PartialUnitigsColorStructure<CX>> as SequenceExtraDataTempBufferManagement>::TempBuffer,
    first_extra: &PartialUnitigExtraData<PartialUnitigsColorStructure<CX>>,
    first_flags: u8,
    second_read: CompressedRead<'_>,
    second_buffer: &<PartialUnitigExtraData<PartialUnitigsColorStructure<CX>> as SequenceExtraDataTempBufferManagement>::TempBuffer,
    second_extra: &PartialUnitigExtraData<PartialUnitigsColorStructure<CX>>,
    second_flags: u8,
    join_buffer: &'a mut Vec<u8>,
    final_unitig_color: &mut TempUnitigColorStructure<CX>,
    final_extra_buffer: &mut <PartialUnitigsColorStructure<CX> as SequenceExtraDataTempBufferManagement>::TempBuffer,
) -> JoinedRead<'a, PartialUnitigExtraData<PartialUnitigsColorStructure<CX>>> {
    let first_glue_beginning = chosen_extremity_at_beginning(first_flags);
    let second_glue_beginning = chosen_extremity_at_beginning(second_flags);

    // Wrong orientation, can happen only if the k-mer is rc-symmetric
    if first_glue_beginning == second_glue_beginning {
        // Cannot join if the reads are not oriented correctly, it should never happen in the new implementation
        unreachable!()
    }

    let (
        (first_read, first_flags, first_extra, first_buffer),
        (second_read, second_flags, second_extra, second_buffer),
    ) = if first_glue_beginning {
        (
            (second_read, second_flags, second_extra, second_buffer),
            (first_read, first_flags, first_extra, first_buffer),
        )
    } else {
        (
            (first_read, first_flags, first_extra, first_buffer),
            (second_read, second_flags, second_extra, second_buffer),
        )
    };

    // Check correctness
    // {
    //     let first_link = first_read
    //         .sub_slice(first_read.bases_count() - global_data.k..first_read.bases_count());
    //     let second_link = second_read.sub_slice(0..global_data.k);

    //     assert!(first_link.equality_compare_start_zero(&second_link));
    // }

    join_buffer.clear();
    join_buffer.extend_from_slice(
        first_read
            .sub_slice(0..first_read.bases_count() - global_data.k)
            .get_packed_slice(),
    );
    join_buffer.extend_from_slice(second_read.get_packed_slice());

    let extra = join_partial_unitig_data::<CX>(
        final_unitig_color,
        final_extra_buffer,
        first_extra,
        first_buffer,
        second_extra,
        second_buffer,
    );

    let total_bases = first_read.bases_count() + second_read.bases_count() - global_data.k;
    let read = CompressedRead::new_offset(join_buffer, first_read.start as usize, total_bases);

    let beginning_open = both_extremities_are_open(first_flags);
    let ending_open = both_extremities_are_open(second_flags);

    let both_open = beginning_open && ending_open;
    let none_open = !beginning_open && !ending_open;

    let mut flags = 0;
    if both_open {
        flags |= OTHER_END_FLAG_MASK;
    }
    if !beginning_open {
        flags |= HASH_ENDING_FLAG_MASK
    }

    JoinedRead {
        read,
        flags,
        extra,
        completed: none_open,
    }
}

pub fn extend_unitigs<
    MH: HashFunctionFactory,
    CX: ColorsManager,
    BK: StructuredSequenceBackend<PartialUnitigsColorStructure<CX>, ()>,
>(
    read_buckets_files: Vec<SingleBucket>,
    temp_path: &Path,
    out_file: &StructuredSequenceWriter<PartialUnitigsColorStructure<CX>, (), BK>,
    circular_out_file: Option<&StructuredSequenceWriter<PartialUnitigsColorStructure<CX>, (), BK>>,
    k: usize,
) {
    PHASES_TIMES_MONITOR
        .write()
        .start_phase("phase: unitigs joining".to_string());

    let current_buckets: Vec<_> = read_buckets_files;

    #[derive(Copy, Clone)]
    struct ReadsVecEntry<E> {
        read: CompressedReadIndipendent,
        extra: E,
        flags: u8,
    }

    let extra_buffer =
        ScopedThreadLocal::new(move || PartialUnitigsColorStructure::<CX>::new_temp_buffer());

    let input_buckets_count =
        BucketsCount::new(current_buckets.len().ilog2() as usize, ExtraBuckets::None);

    let mut log_timer = Instant::now();

    /* TODO: for each result file:
     *  - read in parallel the result file, splitting in sub-buckets
     *  - process each sub-bucket using an hashmap
     *  - if an unitig has the same number in both extensions, assign a duplicate number and put it in both directions.
     *  - The first taken wins and keeps its place in the hashmap. When the second one asks to be merged the first merged one is used instead.
     *      In this way the chain of links can be unlimited. if buckets == 1 this algorithm is serialized and becomes hash based
     *
     */

    let additional_buckets = Arc::new(MultiThreadBuckets::<CompressedBinaryWriter>::new(
        input_buckets_count,
        temp_path.join("extadd"),
        None,
        &(
            get_memory_mode(SwapPriority::ResultBuckets),
            PARTIAL_UNITIGS_COMPACTED_CHECKPOINT_SIZE,
            get_compression_level_info(),
        ),
        &(),
    ));

    let additional_buckets_buffers = ScopedThreadLocal::new(move || {
        BucketsThreadBuffer::new(DEFAULT_PER_CPU_BUFFER_SIZE, &input_buckets_count)
        // FuzzyBuckets::<usize>::new(DEFAULT_COMPACTION_MAP_SUBBUCKET_ELEMENTS)
    });

    for (bucket_index, unitigs_bucket) in current_buckets.iter().enumerate() {
        println!("Path: {}", unitigs_bucket.path.display());

        let reads_map = ScopedThreadLocal::new(move || {
            HashTable::<usize>::with_capacity(DEFAULT_COMPACTION_MAP_SUBBUCKET_ELEMENTS)
            // FuzzyBuckets::<usize>::new(DEFAULT_COMPACTION_MAP_SUBBUCKET_ELEMENTS)
        });

        let reads_storage = ScopedThreadLocal::new(move || {
            Vec::<u8>::with_capacity(DEFAULT_COMPACTION_MAP_SUBBUCKET_ELEMENTS * k)
        });

        let reads_vec = ScopedThreadLocal::new(move || {
            Vec::<ReadsVecEntry<PartialUnitigExtraData<PartialUnitigsColorStructure<CX>>>>::with_capacity(
            DEFAULT_COMPACTION_MAP_SUBBUCKET_ELEMENTS * k,
        )
        });

        let unitigs_bucket_chunks = ChunkedBinaryReaderIndex::from_file(
            &unitigs_bucket.path,
            RemoveFileMode::Remove {
                remove_fs: !KEEP_FILES.load(Ordering::Relaxed),
            },
        )
        .into_parallel_chunks();

        let additional_bucket_path = additional_buckets
            .take_bucket(bucket_index as u16)
            .into_single()
            .path;
        let additional_bucket_chunks = ChunkedBinaryReaderIndex::from_file(
            &additional_bucket_path,
            RemoveFileMode::Remove {
                remove_fs: !KEEP_FILES.load(Ordering::Relaxed),
            },
        )
        .into_parallel_chunks();

        // Add the additional unitigs into the main chunks
        unitigs_bucket_chunks
            .lock()
            .extend(additional_bucket_chunks.into_inner());

        let min_subpartitions = current_num_threads().next_power_of_two() * 2;
        let subpartitions_count = ((MemoryFs::get_file_size(&unitigs_bucket.path).unwrap()
            + MemoryFs::get_file_size(&additional_bucket_path).unwrap())
            as usize
            / MAX_SUBSUBPARTITION_SIZE)
            .max(min_subpartitions)
            .min(MAX_SUBPARTITIONS_COUNT as usize);
        let mut subpartitions_count = BucketsCount::from_power_of_two(
            subpartitions_count.next_power_of_two(),
            ExtraBuckets::None,
        );

        let subpartition_buffers = ScopedThreadLocal::new(move || {
            BucketsThreadBuffer::new(DEFAULT_PER_CPU_BUFFER_SIZE, &subpartitions_count)
            // FuzzyBuckets::<usize>::new(DEFAULT_COMPACTION_MAP_SUBBUCKET_ELEMENTS)
        });

        let subpartitions = Arc::new(MultiThreadBuckets::<CompressedBinaryWriter>::new(
            subpartitions_count,
            temp_path.join(format!("subpart-{}.0", bucket_index)),
            None,
            &(
                get_memory_mode(SwapPriority::ResultBuckets),
                CompressedBinaryWriter::CHECKPOINT_SIZE_UNLIMITED,
                get_compression_level_info(),
            ),
            &(),
        ));

        let mut global_data = SubsplitGlobalData {
            k,
            // These hashmaps are used for two purposes:
            // - the first hashmap is to ensure that extremities are paired together when possible, so that between two adjacent unitigs at least one gets merged.
            // - the second hashmap is to handle symmetric k-mer endings, ensuring the orientation of the reads is matched correctly
            extremities_hashmaps_presence: (0..subpartitions
                .get_buckets_count()
                .total_buckets_count)
                .map(|_| Mutex::new(HashMap::<MH::HashTypeUnextendable, bool>::new()))
                .collect(),
            extremities_hashmaps_symmetric_at_end: (0..subpartitions
                .get_buckets_count()
                .total_buckets_count)
                .map(|_| Mutex::new(HashMap::<MH::HashTypeUnextendable, bool>::new()))
                .collect(),
            hashmap_size: AtomicUsize::new(0),
            input_buckets_count,
            subpartitions_count,
        };

        (0..rayon::current_num_threads())
            .into_par_iter()
            .for_each(|_thread_index| {
                let mut fast_rand = FastRandBool::new();
                let mut subpartition_buffer = subpartition_buffers.get();

                let mut subpartition_buckets: BucketsThreadDispatcher<
                    CompressedBinaryWriter,
                    CompressedReadsBucketDataSerializer<
                        PartialUnitigExtraData<PartialUnitigsColorStructure<CX>>,
                        NoSecondBucket,
                        NoMultiplicity,
                        AssemblerMinimizerPosition,
                        U4,
                        AlignToMinimizerByteBoundary,
                    >,
                > = BucketsThreadDispatcher::new(&subpartitions, subpartition_buffer.take(), k);

                TypedStreamReader::get_items_parallel::<
                    CompressedReadsBucketDataSerializer<
                        PartialUnitigExtraData<color_types::PartialUnitigsColorStructure<CX>>,
                        NoSecondBucket,
                        NoMultiplicity,
                        AssemblerMinimizerPosition,
                        U4,
                        AlignToMinimizerByteBoundary,
                    >,
                >(
                    k,
                    &unitigs_bucket_chunks,
                    |DeserializedRead {
                         read,
                         extra,
                         minimizer_pos: _,
                         flags,
                         ..
                     },
                     extra_buffer| {
                        let info = find_best_orientation_and_flags::<MH>(
                            &global_data,
                            read,
                            flags,
                            &mut fast_rand,
                            true,
                        );
                        subpartition_buckets.add_element_extended(
                            info.new_subpartition,
                            &extra,
                            extra_buffer,
                            &CompressedReadsBucketData {
                                read: ReadData::Packed(read).reverse_complement(info.should_rc),
                                multiplicity: 0,
                                minimizer_pos: info.last_align,
                                extra_bucket: 0,
                                flags: info.new_flags,
                            },
                        );

                        PartialUnitigsColorStructure::<CX>::clear_temp_buffer(extra_buffer);
                    },
                );

                subpartition_buffer.put_back(subpartition_buckets.finalize().0);
            });
        let queued_buckets = Mutex::new(subpartitions.finalize_single());

        let mut subloop_index = 1;
        loop {
            let has_joinable_unitigs = AtomicBool::new(false);

            global_data.subpartitions_count = subpartitions_count;
            // Read the subpartitioned unitigs
            let subpartitions_next = Arc::new(MultiThreadBuckets::<CompressedBinaryWriter>::new(
                subpartitions_count,
                temp_path.join(format!("subpart-{}.{}", bucket_index, subloop_index)),
                None,
                &(
                    get_memory_mode(SwapPriority::ResultBuckets),
                    CompressedBinaryWriter::CHECKPOINT_SIZE_UNLIMITED,
                    get_compression_level_info(),
                ),
                &(),
            ));

            global_data.hashmap_size.store(0, Ordering::Relaxed);
            global_data
                .extremities_hashmaps_presence
                .par_iter()
                .for_each(|h| h.lock().clear());

            global_data
                .extremities_hashmaps_symmetric_at_end
                .par_iter()
                .for_each(|h| h.lock().clear());

            (0..rayon::current_num_threads())
                .into_par_iter()
                .for_each(|_thread_index| {
                    let mut additional_buffer = additional_buckets_buffers.get();
                    let mut fast_rand = FastRandBool::new();

                    let mut additional_buckets: BucketsThreadDispatcher<
                        CompressedBinaryWriter,
                        CompressedReadsBucketDataSerializer<
                            PartialUnitigExtraData<PartialUnitigsColorStructure<CX>>,
                            NoSecondBucket,
                            NoMultiplicity,
                            AssemblerMinimizerPosition,
                            U4,
                            AlignToMinimizerByteBoundary,
                        >,
                    > = BucketsThreadDispatcher::new(
                        &additional_buckets,
                        additional_buffer.take(),
                        k,
                    );

                    let mut subpartition_buffer = subpartition_buffers.get();

                    let mut next_subpartition_buckets: BucketsThreadDispatcher<
                        CompressedBinaryWriter,
                        CompressedReadsBucketDataSerializer<
                            PartialUnitigExtraData<PartialUnitigsColorStructure<CX>>,
                            NoSecondBucket,
                            NoMultiplicity,
                            AssemblerMinimizerPosition,
                            U4,
                            AlignToMinimizerByteBoundary,
                        >,
                    > = BucketsThreadDispatcher::new(
                        &subpartitions_next,
                        subpartition_buffer.take(),
                        k,
                    );

                    let mut reads = reads_map.get();
                    let mut reads_storage = reads_storage.get();
                    let mut reads_vec = reads_vec.get();
                    let mut extra_buffer = extra_buffer.get();

                    let mut tmp_final_unitigs_buffer = FastaWriterConcurrentBuffer::new(
                        out_file,
                        DEFAULT_OUTPUT_BUFFER_SIZE,
                        true,
                        k,
                    );

                    let mut tmp_final_circular_unitigs_buffer =
                        circular_out_file.map(|circular_out_file| {
                            FastaWriterConcurrentBuffer::new(
                                circular_out_file,
                                DEFAULT_OUTPUT_BUFFER_SIZE,
                                true,
                                k,
                            )
                        });

                    let mut join_extra_buffer =
                        PartialUnitigsColorStructure::<CX>::new_temp_buffer();

                    let mut join_colors_structure =
                        CX::ColorsMergeManagerType::alloc_unitig_color_structure();

                    let mut join_buffer: Vec<u8> =
                        Vec::with_capacity(DEFAULT_PER_CPU_BUFFER_SIZE.as_bytes());

                    let mut more_steps_needed = false;

                    #[inline]
                    fn get_extremity(read: CompressedRead, flags: u8, k: usize) -> CompressedRead {
                        let glue_offset = if flags & HASH_ENDING_FLAG_MASK != 0 {
                            read.bases_count() - k
                        } else {
                            0
                        };
                        read.sub_slice(glue_offset..glue_offset + k)
                    }

                    #[inline]
                    fn compute_hash(read: CompressedRead, flags: u8, k: usize) -> u64 {
                        unsafe { get_extremity(read, flags, k).compute_hash_aligned_overflow16() }
                    }

                    while let Some(bucket) = {
                        let mut buckets = queued_buckets.lock();
                        buckets.pop()
                    } {
                        let estimated_size = MemoryFs::get_file_size(&bucket.path).unwrap() / k * 2;
                        reads.reserve(estimated_size, |_| unreachable!());

                        let mut reads_count = 0;
                        TypedStreamReader::get_items::<
                            CompressedReadsBucketDataSerializer<
                                PartialUnitigExtraData<PartialUnitigsColorStructure<CX>>,
                                NoSecondBucket,
                                NoMultiplicity,
                                AssemblerMinimizerPosition,
                                U4,
                                AlignToMinimizerByteBoundary,
                            >,
                        >(
                            None,
                            k,
                            ChunkedBinaryReaderIndex::from_file(
                                &bucket.path,
                                RemoveFileMode::Remove {
                                    remove_fs: !KEEP_FILES.load(Ordering::Relaxed),
                                },
                            )
                            .into_chunks(),
                            |DeserializedRead {
                                 read,
                                 extra,
                                 minimizer_pos: _,
                                 flags,
                                 ..
                             },
                             src_extra_buffer| {
                                reads_count += 1;
                                let hash = compute_hash(read, flags, k);
                                let current_extremity = get_extremity(read, flags, k);

                                let element_entry = reads.entry(
                                    hash,
                                    |index| {
                                        let target_read = &reads_vec[*index];
                                        let target_extremity = get_extremity(
                                            target_read.read.as_reference(&reads_storage),
                                            target_read.flags,
                                            k,
                                        );
                                        target_extremity
                                            .equality_compare_start_zero(&current_extremity)
                                    },
                                    |index| {
                                        compute_hash(
                                            reads_vec[*index].read.as_reference(&reads_storage),
                                            reads_vec[*index].flags,
                                            k,
                                        )
                                    },
                                );

                                // The current bucket is empty, fill it with the read
                                match element_entry {
                                    Entry::Vacant(entry) => {
                                        let saved_read = CompressedReadIndipendent::from_read::<
                                            false,
                                        >(
                                            &read, &mut reads_storage
                                        );

                                        let copied_extra = PartialUnitigExtraData::<
                                            PartialUnitigsColorStructure<CX>,
                                        >::copy_extra_from(
                                            extra,
                                            src_extra_buffer,
                                            &mut extra_buffer,
                                        );

                                        entry.insert(reads_vec.len());
                                        reads_vec.push(ReadsVecEntry {
                                            read: saved_read,
                                            extra: copied_extra,
                                            flags,
                                        });
                                    }
                                    Entry::Occupied(entry) => {
                                        // Join the reads

                                        let other_read = &reads_vec[*entry.get()];

                                        // After joining, if there is only one ending, put the unitig in the corresponding bucket (either next turn or following rounds)
                                        // If the new unitig contains at least one ending in the current round, put it back
                                        // If the new unitig has two endings in the current round, check the hashmap for a correspondence, if there is none cluster randomly,
                                        // otherwise cluster in the same bucket as the found entry in the hashmap,
                                        // to guarantee joining in the next round

                                        let joined = join_reads::<MH, CX>(
                                            &global_data,
                                            read,
                                            src_extra_buffer,
                                            &extra,
                                            flags,
                                            other_read.read.as_reference(&reads_storage),
                                            &extra_buffer,
                                            &other_read.extra,
                                            other_read.flags,
                                            &mut join_buffer,
                                            &mut join_colors_structure,
                                            &mut join_extra_buffer,
                                        );

                                        // Reset the current bucket
                                        entry.remove();

                                        if joined.completed {
                                            let (color, _counters) =
                                                get_color_and_counters::<CX>(&joined.extra);

                                            tmp_final_unitigs_buffer.add_read(
                                                joined.read.into_bases_iter(),
                                                None,
                                                color,
                                                &join_extra_buffer,
                                                (),
                                                &(),
                                                #[cfg(feature = "support_kmer_counters")]
                                                _counters,
                                            );
                                        } else {
                                            let info = find_best_orientation_and_flags::<MH>(
                                                &global_data,
                                                joined.read,
                                                joined.flags,
                                                &mut fast_rand,
                                                false,
                                            );

                                            if info.new_bucket as usize == bucket_index {
                                                more_steps_needed = true;
                                                next_subpartition_buckets.add_element_extended(
                                                    info.new_subpartition,
                                                    &joined.extra,
                                                    &join_extra_buffer,
                                                    &CompressedReadsBucketData {
                                                        read: ReadData::Packed(joined.read)
                                                            .reverse_complement(info.should_rc),
                                                        multiplicity: 0,
                                                        minimizer_pos: info.last_align,
                                                        extra_bucket: 0,
                                                        flags: info.new_flags,
                                                    },
                                                );
                                            } else {
                                                additional_buckets.add_element_extended(
                                                    info.new_bucket,
                                                    &joined.extra,
                                                    &join_extra_buffer,
                                                    &CompressedReadsBucketData {
                                                        read: ReadData::Packed(joined.read)
                                                            .reverse_complement(info.should_rc),
                                                        multiplicity: 0,
                                                        minimizer_pos: info.last_align,
                                                        extra_bucket: 0,
                                                        flags: info.new_flags,
                                                    },
                                                );
                                            }
                                        }
                                    }
                                }

                                PartialUnitigsColorStructure::<CX>::clear_temp_buffer(
                                    src_extra_buffer,
                                );
                            },
                        );

                        // {
                        //     println!(
                        //         "Memory usage - reads: {} KiB [{} elements], reads_storage: {} KiB [{} elements], reads_vec: {} KiB [{} elements] total reads: {}",
                        //         reads.capacity() * std::mem::size_of::<usize>() / 1024,
                        //         reads.capacity(),
                        //         reads_storage.capacity() * (if reads_storage.is_empty() { 0 } else { std::mem::size_of_val(&reads_storage[0]) }) / 1024,
                        //         reads_storage.capacity(),
                        //         reads_vec.capacity() * (if reads_vec.is_empty() { 0 } else { std::mem::size_of_val(&reads_vec[0]) }) / 1024,
                        //         reads_vec.capacity(),
                        //         reads_count
                        //     )
                        // }

                        for index in reads.drain() {
                            let read_struct = &reads_vec[index];
                            let read = read_struct.read.as_reference(&reads_storage);

                            let info = find_best_orientation_and_flags::<MH>(
                                &global_data,
                                read,
                                read_struct.flags,
                                &mut fast_rand,
                                false,
                            );

                            if info.is_circular {
                                // Write a circular unitig

                                let circular_unitigs_buffer = tmp_final_circular_unitigs_buffer
                                    .as_mut()
                                    .unwrap_or(&mut tmp_final_unitigs_buffer);
                                let bases_count = read.bases_count() - 1;

                                let (color, _counters) =
                                    get_color_and_counters::<CX>(&read_struct.extra);

                                CX::ColorsMergeManagerType::reset_unitig_color_structure(
                                    &mut join_colors_structure,
                                );
                                CX::ColorsMergeManagerType::join_structures::<false>(
                                    &mut join_colors_structure,
                                    &color,
                                    &extra_buffer,
                                    0,
                                    Some(bases_count - k + 1),
                                );
                                let writable_color =
                                    CX::ColorsMergeManagerType::encode_part_unitigs_colors(
                                        &mut join_colors_structure,
                                        &mut join_extra_buffer,
                                    );

                                circular_unitigs_buffer.add_read(
                                    read.subslice(0, bases_count).into_bases_iter(),
                                    None,
                                    writable_color,
                                    &join_extra_buffer,
                                    (),
                                    &(),
                                    #[cfg(feature = "support_kmer_counters")]
                                    _counters,
                                );
                            } else {
                                more_steps_needed = true;
                                assert_eq!(info.new_bucket as usize, bucket_index);
                                next_subpartition_buckets.add_element_extended(
                                    info.new_subpartition,
                                    &read_struct.extra,
                                    &extra_buffer,
                                    &CompressedReadsBucketData {
                                        read: ReadData::Packed(read)
                                            .reverse_complement(info.should_rc),
                                        multiplicity: 0,
                                        minimizer_pos: info.last_align as u16,
                                        extra_bucket: 0,
                                        flags: info.new_flags,
                                    },
                                );
                            }
                        }

                        if more_steps_needed {
                            has_joinable_unitigs.store(true, Ordering::Relaxed);
                        }

                        reads_storage.clear();
                        reads_vec.clear();
                        PartialUnitigsColorStructure::<CX>::clear_temp_buffer(&mut extra_buffer);
                    }

                    additional_buffer.put_back(additional_buckets.finalize().0);
                    subpartition_buffer.put_back(next_subpartition_buckets.finalize().0);
                    tmp_final_unitigs_buffer.finalize();
                });

            subloop_index += 1;

            let do_logging = if log_timer.elapsed() > MINIMUM_LOG_DELTA_TIME {
                log_timer = Instant::now();
                true
            } else {
                false
            };

            if do_logging {
                let monitor = PHASES_TIMES_MONITOR.read();

                let processed = bucket_index + 1;
                let total = input_buckets_count.total_buckets_count;

                let eta = Duration::from_secs(
                    (monitor.get_phase_timer().as_secs_f64() / (processed as f64)
                        * ((total - processed) as f64)) as u64,
                );

                let est_tot = Duration::from_secs(
                    (monitor.get_phase_timer().as_secs_f64() / (processed as f64) * (total as f64))
                        as u64,
                );

                ggcat_logging::info!(
                    "Processed bucket: {}/{} (substep: {}) {} phase eta: {:.0?} est. tot: {:.0?}",
                    processed,
                    total,
                    subloop_index,
                    PHASES_TIMES_MONITOR
                        .read()
                        .get_formatted_counter_without_memory(),
                    eta,
                    est_tot,
                );
            }

            if !has_joinable_unitigs.into_inner() {
                // Remove the unuzed buckets (they are all empty)
                subpartitions_next.finalize_single().iter().for_each(|s| {
                    MemoryFs::remove_file(&s.path, RemoveFileMode::Remove { remove_fs: true })
                        .unwrap()
                });
                break;
            }

            let next_subpartitions = subpartitions_next.finalize_single();

            let next_subpartitions_size = next_subpartitions
                .iter()
                .map(|s| MemoryFs::get_file_size(&s.path).unwrap())
                .sum::<usize>();

            subpartitions_count = BucketsCount::from_power_of_two(
                (next_subpartitions_size / MAX_SUBSUBPARTITION_SIZE)
                    .max(min_subpartitions)
                    .min(MAX_SUBPARTITIONS_COUNT as usize)
                    .next_power_of_two(),
                ExtraBuckets::None,
            );

            // Add the new buckets to the processing queue
            queued_buckets.lock().extend(next_subpartitions);
        }
    }
}
