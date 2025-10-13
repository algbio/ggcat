use colors::colors_manager::ColorsMergeManager;
use colors::colors_manager::color_types::{PartialUnitigsColorStructure, TempUnitigColorStructure};
use colors::colors_manager::{ColorsManager, color_types};
use config::{
    BucketIndexType, DEFAULT_COMPACTION_MAP_SUBBUCKET_ELEMENTS, DEFAULT_OUTPUT_BUFFER_SIZE,
    DEFAULT_PER_CPU_BUFFER_SIZE, DEFAULT_PREFETCH_AMOUNT, KEEP_FILES, MINIMUM_LOG_DELTA_TIME,
    SwapPriority, get_compression_level_info, get_memory_mode,
};
use ggcat_logging::info;
use hashbrown::hash_table::Entry;
use hashbrown::{HashMap, HashTable};
use hashes::extremal::{DelayedHashComputation, HashGenerator};
use hashes::{ExtendableHashTraitType, HashFunctionFactory, HashableSequence};
use io::compressed_read::{CompressedRead, CompressedReadIndipendent};
use io::concurrent::structured_sequences::concurrent::FastaWriterConcurrentBuffer;
use io::concurrent::structured_sequences::{
    IdentSequenceWriter, StructuredSequenceBackend, StructuredSequenceWriter,
};
use io::concurrent::temp_reads::creads_utils::{
    AlignToMinimizerByteBoundary, AssemblerMinimizerPosition, CompressedReadsBucketData,
    CompressedReadsBucketDataSerializer, DeserializedRead, NoMinimizerPosition, NoMultiplicity,
    NoSecondBucket, ReadData, ToReadData,
};
use io::concurrent::temp_reads::extra_data::{
    self, SequenceExtraData, SequenceExtraDataTempBufferManagement,
};
use io::structs::unitig_link::{UnitigFlags, UnitigIndex, UnitigLinkSerializer};
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
use rayon::prelude::*;
use std::cmp::Reverse;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Instant;
use structs::partial_unitigs_extra_data::PartialUnitigExtraData;
use typenum::U2;
use utils::fast_rand_bool::FastRandBool;
use utils::fuzzy_buckets::FuzzyBuckets;
use utils::fuzzy_hashmap::FuzzyHashmap;

#[cfg(feature = "support_kmer_counters")]
use io::concurrent::structured_sequences::SequenceAbundance;

const HASH_ENDING_FLAG_MASK: u8 = 1;
const OTHER_END_FLAG_MASK: u8 = 2;

struct JoinedRead<'a, H, E> {
    read: CompressedRead<'a>,
    extremity_hash: Option<H>,
    extra: E,
    flags: u8,
    should_rc: bool,
}

#[inline(always)]
fn try_join<'a, MH: HashFunctionFactory, CX: ColorsManager>(
    first_read: CompressedRead<'_>,
    first_buffer: &<PartialUnitigsColorStructure<CX> as SequenceExtraDataTempBufferManagement>::TempBuffer,
    first_extra: &PartialUnitigsColorStructure<CX>,
    first_flags: u8,
    second_read: CompressedRead<'_>,
    second_buffer: &<PartialUnitigsColorStructure<CX> as SequenceExtraDataTempBufferManagement>::TempBuffer,
    second_extra: &PartialUnitigsColorStructure<CX>,
    second_flags: u8,
    fast_random: &mut FastRandBool<1>,
    join_buffer: &'a mut Vec<u8>,
    final_unitig_color: &mut TempUnitigColorStructure<CX>,
    final_extra_buffer: &mut <PartialUnitigsColorStructure<CX> as SequenceExtraDataTempBufferManagement>::TempBuffer,
    k: usize,
) -> Option<JoinedRead<'a, MH::HashTypeUnextendable, PartialUnitigsColorStructure<CX>>> {
    let first_glue_beginning = first_flags & HASH_ENDING_FLAG_MASK == 0;
    let second_glue_beginning = second_flags & HASH_ENDING_FLAG_MASK == 0;

    // Wrong orientation, can happen only if the k-mer is rc-symmetric
    if first_glue_beginning == second_glue_beginning {
        // Cannot join if the reads are not oriented correctly
        return None;
    }

    let ((first_read, first_flags), (second_read, second_flags)) = if first_glue_beginning {
        ((second_read, second_flags), (first_read, first_flags))
    } else {
        ((first_read, first_flags), (second_read, second_flags))
    };

    // Check correctness
    // {
    //     let first_link =
    //         first_read.sub_slice(first_read.bases_count() - k..first_read.bases_count());
    //     let second_link = second_read.sub_slice(0..k);

    //     assert!(first_link.equality_compare_start_zero(&second_link));
    // }

    join_buffer.clear();
    join_buffer.extend_from_slice(
        first_read
            .sub_slice(0..first_read.bases_count() - k)
            .get_packed_slice(),
    );
    join_buffer.extend_from_slice(second_read.get_packed_slice());

    CX::ColorsMergeManagerType::reset_unitig_color_structure(final_unitig_color);
    CX::ColorsMergeManagerType::join_structures::<false>(
        final_unitig_color,
        first_extra,
        first_buffer,
        0,
        None,
    );
    CX::ColorsMergeManagerType::join_structures::<false>(
        final_unitig_color,
        second_extra,
        second_buffer,
        1,
        None,
    );

    let total_bases = first_read.bases_count() + second_read.bases_count() - k;

    let read = CompressedRead::new_offset(join_buffer, first_read.start as usize, total_bases);

    let beginning_open = first_flags & OTHER_END_FLAG_MASK != 0;
    let ending_open = second_flags & OTHER_END_FLAG_MASK != 0;

    let dest_bucket_beginning = if beginning_open {
        Some(if ending_open {
            fast_random.get_randbool()
        } else {
            true
        })
    } else if ending_open {
        Some(false)
    } else {
        None
    };

    let (hash, should_rc, flags) = match dest_bucket_beginning {
        Some(beginning) => {
            let hash =
                HashGenerator::<MH>::get_extremal_hash(&DelayedHashComputation, read, k, beginning);
            let should_rc = !hash.is_forward();
            let both_extremities_open = beginning_open && ending_open;
            let hash_ending = !beginning ^ should_rc;
            (
                Some(hash.to_unextendable()),
                should_rc,
                ((both_extremities_open as u8) << 1) | (hash_ending as u8),
            )
        }
        None => (None, false, 0),
    };

    let writable_color = CX::ColorsMergeManagerType::encode_part_unitigs_colors(
        final_unitig_color,
        final_extra_buffer,
    );

    Some(JoinedRead {
        read,
        extremity_hash: hash,
        flags,
        should_rc,
        extra: writable_color,
    })
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

    let mut inputs: Vec<_> = read_buckets_files;
    inputs.sort_by_cached_key(|f| MemoryFs::get_file_size(&f.path).unwrap());

    let reads_map = ScopedThreadLocal::new(move || {
        HashTable::<usize>::with_capacity(DEFAULT_COMPACTION_MAP_SUBBUCKET_ELEMENTS)
        // FuzzyBuckets::<usize>::new(DEFAULT_COMPACTION_MAP_SUBBUCKET_ELEMENTS)
    });

    let reads_storage = ScopedThreadLocal::new(move || {
        Vec::<u8>::with_capacity(DEFAULT_COMPACTION_MAP_SUBBUCKET_ELEMENTS * k)
    });

    #[derive(Copy, Clone)]
    struct ReadsVecEntry<E> {
        read: CompressedReadIndipendent,
        extra: E,
        flags: u8,
    }

    let reads_vec = ScopedThreadLocal::new(move || {
        Vec::<ReadsVecEntry<PartialUnitigsColorStructure<CX>>>::with_capacity(
            DEFAULT_COMPACTION_MAP_SUBBUCKET_ELEMENTS * k,
        )
    });

    let extra_buffer =
        ScopedThreadLocal::new(move || PartialUnitigsColorStructure::<CX>::new_temp_buffer());

    let mut iter_count = 1;
    let buckets_count = BucketsCount::new(inputs.len().ilog2() as usize, ExtraBuckets::None);
    let mut current_buckets = inputs;

    let buckets_buffer = ScopedThreadLocal::new(move || {
        BucketsThreadBuffer::new(DEFAULT_PER_CPU_BUFFER_SIZE, &buckets_count)
    });

    let mut log_timer = Instant::now();

    loop {
        let new_buckets = Arc::new(MultiThreadBuckets::<CompressedBinaryWriter>::new(
            buckets_count,
            temp_path.join(format!("extended-{}", iter_count)),
            None,
            &(
                get_memory_mode(SwapPriority::ResultBuckets),
                CompressedBinaryWriter::CHECKPOINT_SIZE_UNLIMITED,
                get_compression_level_info(),
            ),
            &(),
        ));

        let has_joinable_unitigs = AtomicBool::new(false);
        let total_remaining_count = AtomicUsize::new(0);

        rayon::scope(|_s| {
            let inputs_count = current_buckets.len();
            let inputs = Mutex::new(current_buckets);

            (0..inputs_count).into_par_iter().for_each(|_| {
                // Guarantee processing order to ensure bigger buckets are processed earlier
                let read_file: SingleBucket = inputs.lock().pop().unwrap();
                let bucket_index = read_file.index as BucketIndexType;

                let mut tmp_final_unitigs_buffer =
                    FastaWriterConcurrentBuffer::new(out_file, DEFAULT_OUTPUT_BUFFER_SIZE, true, k);

                let mut tmp_final_circular_unitigs_buffer =
                    circular_out_file.map(|circular_out_file| {
                        FastaWriterConcurrentBuffer::new(
                            circular_out_file,
                            DEFAULT_OUTPUT_BUFFER_SIZE,
                            true,
                            k,
                        )
                    });

                let estimated_size = MemoryFs::get_file_size(&read_file.path).unwrap() / k * 4;

                let mut reads = reads_map.get();
                let mut reads_storage = reads_storage.get();
                let mut reads_vec = reads_vec.get();
                let mut extra_buffer = extra_buffer.get();
                let mut buckets_buffer = buckets_buffer.get();

                let mut join_extra_buffer = PartialUnitigsColorStructure::<CX>::new_temp_buffer();
                let mut join_colors_structure =
                    CX::ColorsMergeManagerType::alloc_unitig_color_structure();

                let mut joined_unitigs_buckets: BucketsThreadDispatcher<
                    CompressedBinaryWriter,
                    CompressedReadsBucketDataSerializer<
                        PartialUnitigExtraData<color_types::PartialUnitigsColorStructure<CX>>,
                        NoSecondBucket,
                        NoMultiplicity,
                        AssemblerMinimizerPosition,
                        U2,
                        AlignToMinimizerByteBoundary,
                    >,
                > = BucketsThreadDispatcher::new(&new_buckets, buckets_buffer.take(), k);

                let mut join_buffer: Vec<u8> =
                    Vec::with_capacity(DEFAULT_PER_CPU_BUFFER_SIZE.as_bytes());
                reads.reserve(estimated_size, |_| unreachable!());

                let mut fast_random = FastRandBool::<1>::new();

                let mut remaining_count = 0;
                let mut has_written = false;

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

                TypedStreamReader::get_items::<
                    CompressedReadsBucketDataSerializer<
                        PartialUnitigExtraData<color_types::PartialUnitigsColorStructure<CX>>,
                        NoSecondBucket,
                        NoMultiplicity,
                        AssemblerMinimizerPosition,
                        U2,
                        AlignToMinimizerByteBoundary,
                    >,
                >(
                    None,
                    k,
                    ChunkedBinaryReaderIndex::from_file(
                        &read_file.path,
                        RemoveFileMode::Remove {
                            remove_fs: !KEEP_FILES.load(Ordering::Relaxed),
                        },
                        DEFAULT_PREFETCH_AMOUNT,
                    )
                    .into_chunks(),
                    |DeserializedRead {
                         read, extra, flags, ..
                     },
                     color_extra_buffer| {
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
                                target_extremity.equality_compare_start_zero(&current_extremity)
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
                                let saved_read = CompressedReadIndipendent::from_read::<false>(
                                    &read,
                                    &mut reads_storage,
                                );

                                let extra = PartialUnitigsColorStructure::<CX>::copy_extra_from(
                                    extra.colors,
                                    color_extra_buffer,
                                    &mut extra_buffer,
                                );

                                entry.insert(reads_vec.len());
                                reads_vec.push(ReadsVecEntry {
                                    read: saved_read,
                                    extra,
                                    flags,
                                });
                            }
                            Entry::Occupied(entry) => {
                                // Try to join the reads

                                let other_read = &reads_vec[*entry.get()];

                                let joined = try_join::<MH, CX>(
                                    read,
                                    color_extra_buffer,
                                    &extra.colors,
                                    flags,
                                    other_read.read.as_reference(&reads_storage),
                                    &extra_buffer,
                                    &other_read.extra,
                                    other_read.flags,
                                    &mut fast_random,
                                    &mut join_buffer,
                                    &mut join_colors_structure,
                                    &mut join_extra_buffer,
                                    k,
                                );

                                // Reset the current bucket
                                if let Some(joined) = joined {
                                    entry.remove();

                                    if let Some(extremity_hash) = joined.extremity_hash {
                                        has_written = true;

                                        let last_align =
                                            if joined.flags & HASH_ENDING_FLAG_MASK == 0 {
                                                0
                                            } else {
                                                (joined.read.bases_count() - k) % 4
                                            };

                                        // println!(
                                        //     "Pushing read {} to bucket {} with start: {} and rc: {}",
                                        //     joined.read.to_string(),
                                        //     MH::get_bucket(
                                        //         0,
                                        //         joined_unitigs_buckets
                                        //             .get_buckets_count()
                                        //             .normal_buckets_count_log,
                                        //         extremity_hash,
                                        //     ),
                                        //     joined.read.start,
                                        //     joined.should_rc
                                        // );
                                        remaining_count += 1;
                                        joined_unitigs_buckets.add_element_extended(
                                            MH::get_bucket(
                                                0,
                                                joined_unitigs_buckets
                                                    .get_buckets_count()
                                                    .normal_buckets_count_log,
                                                extremity_hash,
                                            ),
                                            &PartialUnitigExtraData {
                                                colors: joined.extra,
                                                #[cfg(feature = "support_kmer_counters")]
                                                counters,
                                            },
                                            &join_extra_buffer,
                                            &CompressedReadsBucketData {
                                                read: ReadData::Packed(joined.read)
                                                    .reverse_complement(joined.should_rc),
                                                multiplicity: 0,
                                                minimizer_pos: last_align as u16,
                                                extra_bucket: 0,
                                                flags: joined.flags,
                                                is_window_duplicate: false,
                                            },
                                        );
                                    } else {
                                        tmp_final_unitigs_buffer.add_read(
                                            joined.read.into_bases_iter(),
                                            None,
                                            joined.extra,
                                            &join_extra_buffer,
                                            (),
                                            &(),
                                            #[cfg(feature = "support_kmer_counters")]
                                            counters,
                                        );
                                    }
                                } else {
                                    // Cannot join, this means the current reads are oriented in a wrong way
                                    // let hash1 = compute_hash(read, flags, k);
                                    // let hash2 = compute_hash(
                                    //     other_read.read.as_reference(&reads_storage),
                                    //     other_read.flags,
                                    //     k,
                                    // );

                                    // println!(
                                    //     "Cannot join {} with {} => {} => {} == {}",
                                    //     read.to_string(),
                                    //     other_read.read.as_reference(&reads_storage).to_string(),
                                    //     read.equality_compare_start_zero(
                                    //         &other_read.read.as_reference(&reads_storage),
                                    //     ),
                                    //     hash1,
                                    //     hash2
                                    // );
                                    // TODO: Test this branch

                                    let hash_beginning = flags & HASH_ENDING_FLAG_MASK == 0;
                                    let hash = HashGenerator::<MH>::get_extremal_hash(
                                        &DelayedHashComputation,
                                        read,
                                        k,
                                        hash_beginning,
                                    );

                                    let target_bucket = MH::get_bucket(
                                        0,
                                        joined_unitigs_buckets
                                            .get_buckets_count()
                                            .normal_buckets_count_log,
                                        hash.to_unextendable(),
                                    );

                                    // Write the current unitig reverse complementing it
                                    let last_align = if !hash_beginning {
                                        0
                                    } else {
                                        (read.bases_count() - k) % 4
                                    };

                                    remaining_count += 1;
                                    joined_unitigs_buckets.add_element_extended(
                                        target_bucket,
                                        &PartialUnitigExtraData {
                                            colors: extra.colors,
                                            #[cfg(feature = "support_kmer_counters")]
                                            counters,
                                        },
                                        &color_extra_buffer,
                                        &CompressedReadsBucketData {
                                            read: ReadData::PackedRc(read),
                                            multiplicity: 0,
                                            minimizer_pos: last_align as u16,
                                            extra_bucket: 0,
                                            flags: flags ^ HASH_ENDING_FLAG_MASK,
                                            is_window_duplicate: false,
                                        },
                                    );
                                }
                            }
                        }
                    },
                );

                for index in reads.drain() {
                    let read_struct = &reads_vec[index];
                    let read = read_struct.read.as_reference(&reads_storage);
                    let hash_beginning = read_struct.flags & HASH_ENDING_FLAG_MASK == 0;
                    let both_ends = read_struct.flags & OTHER_END_FLAG_MASK != 0;

                    let should_change = if both_ends {
                        fast_random.get_randbool()
                    } else {
                        false
                    };

                    let hash_beginning = hash_beginning ^ should_change;

                    let hash = HashGenerator::<MH>::get_extremal_hash(
                        &DelayedHashComputation,
                        read,
                        k,
                        hash_beginning,
                    );

                    let target_bucket = MH::get_bucket(
                        0,
                        joined_unitigs_buckets
                            .get_buckets_count()
                            .normal_buckets_count_log,
                        hash.to_unextendable(),
                    );

                    let could_be_circular =
                        both_ends && should_change && (target_bucket == bucket_index);

                    let is_circular = if unlikely(could_be_circular) {
                        let other_hash = HashGenerator::<MH>::get_extremal_hash(
                            &DelayedHashComputation,
                            read,
                            k,
                            !hash_beginning,
                        )
                        .to_unextendable();
                        // If beginning and ending hashes are equal, assume this unitig is circular
                        hash.to_unextendable() == other_hash
                    } else {
                        false
                    };

                    if is_circular {
                        let circular_unitigs_buffer = tmp_final_circular_unitigs_buffer
                            .as_mut()
                            .unwrap_or(&mut tmp_final_unitigs_buffer);
                        let bases_count = read.bases_count() - 1;

                        CX::ColorsMergeManagerType::reset_unitig_color_structure(
                            &mut join_colors_structure,
                        );
                        CX::ColorsMergeManagerType::join_structures::<false>(
                            &mut join_colors_structure,
                            &read_struct.extra,
                            &extra_buffer,
                            0,
                            Some(bases_count),
                        );
                        let writable_color = CX::ColorsMergeManagerType::encode_part_unitigs_colors(
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
                            counters,
                        );

                        // Write a circular unitig
                    } else {
                        has_written = true;
                        let should_rc = !hash.is_forward();
                        let hash_beginning = hash_beginning ^ should_rc;
                        let last_align = if hash_beginning {
                            0
                        } else {
                            (read.bases_count() - k) % 4
                        };

                        remaining_count += 1;
                        joined_unitigs_buckets.add_element_extended(
                            target_bucket,
                            &PartialUnitigExtraData {
                                colors: read_struct.extra,
                                #[cfg(feature = "support_kmer_counters")]
                                counters,
                            },
                            &extra_buffer,
                            &CompressedReadsBucketData {
                                read: ReadData::Packed(read).reverse_complement(should_rc),
                                multiplicity: 0,
                                minimizer_pos: last_align as u16,
                                extra_bucket: 0,
                                flags: (!hash_beginning as u8) | ((both_ends as u8) << 1),
                                is_window_duplicate: false,
                            },
                        );
                    }
                }

                if has_written {
                    has_joinable_unitigs.store(true, Ordering::Relaxed);
                }

                total_remaining_count.fetch_add(remaining_count, Ordering::Relaxed);

                reads_storage.clear();
                reads_vec.clear();
                PartialUnitigsColorStructure::<CX>::clear_temp_buffer(&mut extra_buffer);
                buckets_buffer.put_back(joined_unitigs_buckets.finalize().0);

                tmp_final_unitigs_buffer.finalize();
            });
        });

        if !has_joinable_unitigs.into_inner() {
            break;
        }

        let do_logging = if log_timer.elapsed() > MINIMUM_LOG_DELTA_TIME {
            log_timer = Instant::now();
            true
        } else {
            false
        };

        if do_logging {
            ggcat_logging::info!("Iteration: {}", iter_count);
        }

        if do_logging {
            ggcat_logging::info!(
                "Remaining: {} {}",
                total_remaining_count.into_inner(),
                PHASES_TIMES_MONITOR
                    .read()
                    .get_formatted_counter_without_memory()
            );
        }

        current_buckets = new_buckets.finalize_single();
        iter_count += 1;
    }
    info!("Completed unitigs extension in {} iterations", iter_count);
}
