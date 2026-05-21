use byteorder::ReadBytesExt;
use colors::colors_manager::ColorsMergeManager;
use colors::colors_manager::color_types::{PartialUnitigsColorStructure, TempUnitigColorStructure};
use colors::colors_manager::{ColorsManager, color_types};
use config::{
    BucketIndexType, DEFAULT_COMPACTION_MAP_SUBBUCKET_ELEMENTS, DEFAULT_OUTPUT_BUFFER_SIZE,
    DEFAULT_PER_CPU_BUFFER_SIZE, DEFAULT_PREFETCH_AMOUNT, KEEP_FILES, MAX_SUBPARTITIONS_COUNT,
    MAX_SUBSUBPARTITION_SIZE, MINIMUM_LOG_DELTA_TIME, PARTIAL_UNITIGS_COMPACTED_CHECKPOINT_SIZE,
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
use std::time::Instant;
use structs::partial_unitigs_extra_data::PartialUnitigExtraData;
use typenum::U4;
use utils::fast_rand_bool::FastRandBool;
use utils::fuzzy_buckets::FuzzyBuckets;
use utils::fuzzy_hashmap::FuzzyHashmap;

const HASH_ENDING_FLAG_MASK: u8 = 1;
const OTHER_END_FLAG_MASK: u8 = 2;
const SAME_BUCKET_ENDING_FLAG_MASK: u8 = 4;
use structs::partial_unitigs_extra_data::INDIRECT_UNITIG_FLAG_MASK;

struct JoinedRead<'a, H, E> {
    read: CompressedRead<'a>,
    extremity_hash: Option<H>,
    extra: E,
    flags: u8,
    should_rc: bool,
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

#[inline(always)]
fn try_join<'a, MH: HashFunctionFactory, CX: ColorsManager>(
    first_read: CompressedRead<'_>,
    first_buffer: &<PartialUnitigExtraData<PartialUnitigsColorStructure<CX>> as SequenceExtraDataTempBufferManagement>::TempBuffer,
    first_extra: &PartialUnitigExtraData<PartialUnitigsColorStructure<CX>>,
    first_flags: u8,
    second_read: CompressedRead<'_>,
    second_buffer: &<PartialUnitigExtraData<PartialUnitigsColorStructure<CX>> as SequenceExtraDataTempBufferManagement>::TempBuffer,
    second_extra: &PartialUnitigExtraData<PartialUnitigsColorStructure<CX>>,
    second_flags: u8,
    fast_random: &mut FastRandBool<1>,
    join_buffer: &'a mut Vec<u8>,
    final_unitig_color: &mut TempUnitigColorStructure<CX>,
    final_extra_buffer: &mut <PartialUnitigsColorStructure<CX> as SequenceExtraDataTempBufferManagement>::TempBuffer,
    k: usize,
) -> Option<
    JoinedRead<
        'a,
        MH::HashTypeUnextendable,
        PartialUnitigExtraData<PartialUnitigsColorStructure<CX>>,
    >,
> {
    let first_glue_beginning = first_flags & HASH_ENDING_FLAG_MASK == 0;
    let second_glue_beginning = second_flags & HASH_ENDING_FLAG_MASK == 0;

    // Wrong orientation, can happen only if the k-mer is rc-symmetric
    if first_glue_beginning == second_glue_beginning {
        // Cannot join if the reads are not oriented correctly
        return None;
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

    let extra = join_partial_unitig_data::<CX>(
        final_unitig_color,
        final_extra_buffer,
        first_extra,
        first_buffer,
        second_extra,
        second_buffer,
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

    Some(JoinedRead {
        read,
        extremity_hash: hash,

        flags,
        should_rc,
        extra,
    })
}

#[derive(Debug, Clone)]
struct SubPartitionExtra<CX: ColorsManager> {
    extra: PartialUnitigExtraData<PartialUnitigsColorStructure<CX>>,
    same_bucket_id: usize,
}

impl<CX: ColorsManager> SequenceExtraDataTempBufferManagement for SubPartitionExtra<CX> {
    type TempBuffer = <PartialUnitigExtraData<PartialUnitigsColorStructure<CX>> as SequenceExtraDataTempBufferManagement>::TempBuffer;

    fn new_temp_buffer() -> Self::TempBuffer {
        <PartialUnitigExtraData<PartialUnitigsColorStructure<CX>> as SequenceExtraDataTempBufferManagement>::new_temp_buffer()
    }

    fn clear_temp_buffer(buffer: &mut Self::TempBuffer) {
        <PartialUnitigExtraData<PartialUnitigsColorStructure<CX>> as SequenceExtraDataTempBufferManagement>::clear_temp_buffer(buffer);
    }

    fn copy_temp_buffer(dest: &mut Self::TempBuffer, src: &Self::TempBuffer) {
        <PartialUnitigExtraData<PartialUnitigsColorStructure<CX>> as SequenceExtraDataTempBufferManagement>::copy_temp_buffer(dest, src);
    }

    fn copy_extra_from(extra: Self, src: &Self::TempBuffer, dst: &mut Self::TempBuffer) -> Self {
        Self {
            extra: <PartialUnitigExtraData<PartialUnitigsColorStructure<CX>> as SequenceExtraDataTempBufferManagement>::copy_extra_from(extra.extra, src, dst),
            same_bucket_id: extra.same_bucket_id,
        }
    }
}

impl<CX: ColorsManager> SequenceExtraDataConsecutiveCompression for SubPartitionExtra<CX> {
    type LastData = <PartialUnitigExtraData<PartialUnitigsColorStructure<CX>> as SequenceExtraDataConsecutiveCompression>::LastData;

    fn decode_extended(
        buffer: &mut Self::TempBuffer,
        reader: &mut impl std::io::prelude::Read,
        last_data: Self::LastData,
        read_flags: u8,
    ) -> Option<Self> {
        let same_bucket_id = if read_flags & SAME_BUCKET_ENDING_FLAG_MASK != 0 {
            decode_varint(|| reader.read_u8().ok())?
        } else {
            0
        } as usize;

        let extra = PartialUnitigExtraData::<PartialUnitigsColorStructure<CX>>::decode_extended(
            buffer, reader, last_data, read_flags,
        )?;
        Some(Self {
            extra,
            same_bucket_id,
        })
    }

    fn encode_extended(
        &self,
        buffer: &Self::TempBuffer,
        writer: &mut impl std::io::prelude::Write,
        last_data: Self::LastData,
        reverse_complement: bool,
        read_flags: u8,
    ) {
        if read_flags & SAME_BUCKET_ENDING_FLAG_MASK != 0 {
            encode_varint(|b| writer.write_all(b), self.same_bucket_id as u64).unwrap();
        }

        self.extra
            .encode_extended(buffer, writer, last_data, reverse_complement, read_flags);
    }

    fn max_size(&self) -> usize {
        self.extra.max_size() + VARINT_MAX_SIZE
    }

    fn obtain_last_data(
        &self,
        last_data: Self::LastData,
        reverse_complement: bool,
    ) -> Self::LastData {
        self.extra.obtain_last_data(last_data, reverse_complement)
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
        Vec::<ReadsVecEntry<SubPartitionExtra<CX>>>::with_capacity(
            DEFAULT_COMPACTION_MAP_SUBBUCKET_ELEMENTS * k,
        )
    });

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

    let input_buckets_count_log = input_buckets_count.normal_buckets_count_log;

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

        let unitigs_bucket_chunks = ChunkedBinaryReaderIndex::from_file(
            &unitigs_bucket.path,
            RemoveFileMode::Remove {
                remove_fs: !KEEP_FILES.load(Ordering::Relaxed),
            },
            DEFAULT_PREFETCH_AMOUNT,
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
            DEFAULT_PREFETCH_AMOUNT,
        )
        .into_parallel_chunks();

        // Add the additional unitigs into the main chunks
        unitigs_bucket_chunks
            .lock()
            .extend(additional_bucket_chunks.into_inner());

        let min_subpartitions = current_num_threads().next_power_of_two() * 2;
        let subpartitions_count = (MemoryFs::get_file_size(&unitigs_bucket.path).unwrap() as usize
            / MAX_SUBSUBPARTITION_SIZE)
            .max(min_subpartitions)
            .min(MAX_SUBPARTITIONS_COUNT as usize);
        let subpartitions_count = BucketsCount::from_power_of_two(
            subpartitions_count.next_power_of_two(),
            ExtraBuckets::None,
        );

        let subpartition_buffers = ScopedThreadLocal::new(move || {
            BucketsThreadBuffer::new(DEFAULT_PER_CPU_BUFFER_SIZE, &subpartitions_count)
            // FuzzyBuckets::<usize>::new(DEFAULT_COMPACTION_MAP_SUBBUCKET_ELEMENTS)
        });

        let subpartitions = Arc::new(MultiThreadBuckets::<CompressedBinaryWriter>::new(
            subpartitions_count,
            temp_path.join(format!("subpart-{}", bucket_index)),
            None,
            &(
                get_memory_mode(SwapPriority::ResultBuckets),
                CompressedBinaryWriter::CHECKPOINT_SIZE_UNLIMITED,
                get_compression_level_info(),
            ),
            &(),
        ));

        let subpartitions_count_log = subpartitions.get_buckets_count().normal_buckets_count_log;

        let same_bucket_id_gen: AtomicUsize = AtomicUsize::new(0);

        (0..rayon::current_num_threads())
            .into_par_iter()
            .for_each(|_thread_index| {
                let mut subpartition_buffer = subpartition_buffers.get();

                let mut subpartition_buckets: BucketsThreadDispatcher<
                    CompressedBinaryWriter,
                    CompressedReadsBucketDataSerializer<
                        SubPartitionExtra<CX>,
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
                         minimizer_pos,
                         flags,
                         ..
                     },
                     extra_buffer| {
                        let compute_bucket =
                            |used_bits: usize, requested_bits: usize, beginning: bool| {
                                MH::get_bucket(
                                    used_bits,
                                    requested_bits,
                                    HashGenerator::<MH>::get_extremal_hash(
                                        &DelayedHashComputation,
                                        read,
                                        k,
                                        beginning,
                                    )
                                    .to_unextendable(),
                                )
                            };

                        let subbucket = compute_bucket(
                            input_buckets_count_log,
                            subpartitions_count_log,
                            flags & HASH_ENDING_FLAG_MASK == 0,
                        );

                        let both_ends_share_same_bucket = flags & SAME_BUCKET_ENDING_FLAG_MASK != 0;

                        let same_bucket_id = if both_ends_share_same_bucket {
                            same_bucket_id_gen.fetch_add(1, Ordering::Relaxed)
                        } else {
                            0
                        };

                        // let bucket_left = compute_bucket(0, input_buckets_count_log, true);
                        // let bucket_right = compute_bucket(0, input_buckets_count_log, false);

                        // let bucket_left = if flags & HASH_ENDING_FLAG_MASK == 0
                        //     || flags & OTHER_END_FLAG_MASK != 0
                        // {
                        //     bucket_left
                        // } else {
                        //     u16::MAX
                        // };

                        // let bucket_right = if flags & HASH_ENDING_FLAG_MASK != 0
                        //     || flags & OTHER_END_FLAG_MASK != 0
                        // {
                        //     bucket_right
                        // } else {
                        //     u16::MAX
                        // };

                        // assert_eq!(bucket_left.min(bucket_right), bucket_index as u16);

                        if both_ends_share_same_bucket {
                            let other_hash = HashGenerator::<MH>::get_extremal_hash(
                                &DelayedHashComputation,
                                read,
                                k,
                                flags & HASH_ENDING_FLAG_MASK != 0,
                            );

                            let other_subbucket = MH::get_bucket(
                                input_buckets_count_log,
                                subpartitions_count_log,
                                other_hash.to_unextendable(),
                            );

                            let should_rc = !other_hash.is_forward();
                            let mut new_flags = flags;

                            if !should_rc {
                                new_flags ^= HASH_ENDING_FLAG_MASK;
                            }

                            let new_align = if new_flags & HASH_ENDING_FLAG_MASK == 0 {
                                0
                            } else {
                                (read.bases_count() - k) % 4
                            };

                            subpartition_buckets.add_element_extended(
                                other_subbucket,
                                &SubPartitionExtra {
                                    extra: extra.clone(),
                                    same_bucket_id,
                                },
                                extra_buffer,
                                &CompressedReadsBucketData {
                                    read: ReadData::Packed(read).reverse_complement(should_rc),
                                    multiplicity: 0,
                                    minimizer_pos: new_align as u16,
                                    extra_bucket: 0,
                                    flags: new_flags,
                                },
                            );
                        }

                        subpartition_buckets.add_element_extended(
                            subbucket,
                            &SubPartitionExtra {
                                extra: extra,
                                same_bucket_id,
                            },
                            extra_buffer,
                            &CompressedReadsBucketData {
                                read: ReadData::Packed(read),
                                multiplicity: 0,
                                minimizer_pos,
                                extra_bucket: 0,
                                flags: flags,
                            },
                        );
                        PartialUnitigsColorStructure::<CX>::clear_temp_buffer(extra_buffer);
                    },
                );

                subpartition_buffer.put_back(subpartition_buckets.finalize().0);
            });
        let buckets = Mutex::new(subpartitions.finalize_single());

        let mut subloop_index = 1;
        loop {
            let has_joinable_unitigs = AtomicBool::new(false);

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

            (0..rayon::current_num_threads())
                .into_par_iter()
                .for_each(|_thread_index| {
                    let mut additional_buffer = additional_buckets_buffers.get();

                    let mut additional_buckets: BucketsThreadDispatcher<
                        CompressedBinaryWriter,
                        CompressedReadsBucketDataSerializer<
                            SubPartitionExtra<CX>,
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
                            SubPartitionExtra<CX>,
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

                    let mut has_written = true;

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
                        let mut buckets = buckets.lock();
                        buckets.pop()
                    } {
                        let estimated_size = MemoryFs::get_file_size(&bucket.path).unwrap() / k * 4;
                        reads.reserve(estimated_size, |_| unreachable!());

                        let mut count = 0;
                        let mut count_dupl = 0;
                        TypedStreamReader::get_items::<
                            CompressedReadsBucketDataSerializer<
                                SubPartitionExtra<CX>,
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
                                DEFAULT_PREFETCH_AMOUNT,
                            )
                            .into_chunks(),
                            |DeserializedRead {
                                 read,
                                 extra,
                                 minimizer_pos,
                                 flags,
                                 ..
                             },
                             src_extra_buffer| {
                                if flags & SAME_BUCKET_ENDING_FLAG_MASK != 0 {
                                    count_dupl += 1;
                                }

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

                                        let copied_extra = SubPartitionExtra::<CX>::copy_extra_from(
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
                                        // Try to join the reads

                                        let other_read = &reads_vec[*entry.get()];

                                        // After joining, if there is only one ending, put the unitig in the corresponding bucket (either next turn or following rounds)
                                        // If the new unitig contains at least one ending in the current round, put it back
                                        // If the new unitig has two endings in the current round, check the hashmap for a correspondence, if there is none cluster randomly,
                                        // otherwise cluster in the same bucket as the found entry in the hashmap,
                                        // to guarantee joining in the next round

                                        //                             let joined = try_join::<MH, CX>(
                                        //                                 read,
                                        //                                 src_extra_buffer,
                                        //                                 &extra,
                                        //                                 flags,
                                        //                                 other_read.read.as_reference(&reads_storage),
                                        //                                 &extra_buffer,
                                        //                                 &other_read.extra,
                                        //                                 other_read.flags,
                                        //                                 &mut fast_random,
                                        //                                 &mut join_buffer,
                                        //                                 &mut join_colors_structure,
                                        //                                 &mut join_extra_buffer,
                                        //                                 k,
                                        //                             );

                                        //                             // Reset the current bucket
                                        //                             if let Some(joined) = joined {
                                        //                                 entry.remove();

                                        //                                 if let Some(extremity_hash) = joined.extremity_hash {
                                        //                                     has_written = true;

                                        //                                     let last_align =
                                        //                                         if joined.flags & HASH_ENDING_FLAG_MASK == 0 {
                                        //                                             0
                                        //                                         } else {
                                        //                                             (joined.read.bases_count() - k) % 4
                                        //                                         };

                                        //                                     remaining_count += 1;
                                        //                                     next_subpartition_buckets.add_element_extended(
                                        //                                         MH::get_bucket(
                                        //                                             0,
                                        //                                             next_subpartition_buckets
                                        //                                                 .get_buckets_count()
                                        //                                                 .normal_buckets_count_log,
                                        //                                             extremity_hash,
                                        //                                         ),
                                        //                                         &joined.extra,
                                        //                                         &join_extra_buffer,
                                        //                                         &CompressedReadsBucketData {
                                        //                                             read: ReadData::Packed(joined.read)
                                        //                                                 .reverse_complement(joined.should_rc),
                                        //                                             multiplicity: 0,
                                        //                                             minimizer_pos: last_align as u16,
                                        //                                             extra_bucket: 0,
                                        //                                             flags: joined.flags,
                                        //                                         },
                                        //                                     );
                                        //                                 } else {
                                        //                                     let (color, counters) =
                                        //                                         get_color_and_counters::<CX>(&joined.extra);

                                        //                                     tmp_final_unitigs_buffer.add_read(
                                        //                                         joined.read.into_bases_iter(),
                                        //                                         None,
                                        //                                         color,
                                        //                                         &join_extra_buffer,
                                        //                                         (),
                                        //                                         &(),
                                        //                                         #[cfg(feature = "support_kmer_counters")]
                                        //                                         counters,
                                        //                                     );
                                        //                                 }
                                        //                             } else {
                                        //                                 // Cannot join, this means the current reads are oriented in a wrong way
                                        //                                 // let hash1 = compute_hash(read, flags, k);
                                        //                                 // let hash2 = compute_hash(
                                        //                                 //     other_read.read.as_reference(&reads_storage),
                                        //                                 //     other_read.flags,
                                        //                                 //     k,
                                        //                                 // );

                                        //                                 // println!(
                                        //                                 //     "Cannot join {} with {} => {} => {} == {}",
                                        //                                 //     read.to_string(),
                                        //                                 //     other_read.read.as_reference(&reads_storage).to_string(),
                                        //                                 //     read.equality_compare_start_zero(
                                        //                                 //         &other_read.read.as_reference(&reads_storage),
                                        //                                 //     ),
                                        //                                 //     hash1,
                                        //                                 //     hash2
                                        //                                 // );
                                        //                                 // TODO: Test this branch

                                        //                                 let hash_beginning = flags & HASH_ENDING_FLAG_MASK == 0;
                                        //                                 let hash = HashGenerator::<MH>::get_extremal_hash(
                                        //                                     &DelayedHashComputation,
                                        //                                     read,
                                        //                                     k,
                                        //                                     hash_beginning,
                                        //                                 );

                                        //                                 let target_bucket = MH::get_bucket(
                                        //                                     0,
                                        //                                     next_subpartition_buckets
                                        //                                         .get_buckets_count()
                                        //                                         .normal_buckets_count_log,
                                        //                                     hash.to_unextendable(),
                                        //                                 );

                                        //                                 // Write the current unitig reverse complementing it
                                        //                                 let last_align = if !hash_beginning {
                                        //                                     0
                                        //                                 } else {
                                        //                                     (read.bases_count() - k) % 4
                                        //                                 };

                                        //                                 remaining_count += 1;
                                        //                                 next_subpartition_buckets.add_element_extended(
                                        //                                     target_bucket,
                                        //                                     &extra,
                                        //                                     &src_extra_buffer,
                                        //                                     &CompressedReadsBucketData {
                                        //                                         read: ReadData::PackedRc(read),
                                        //                                         multiplicity: 0,
                                        //                                         minimizer_pos: last_align as u16,
                                        //                                         extra_bucket: 0,
                                        //                                         flags: flags ^ HASH_ENDING_FLAG_MASK,
                                        //                                     },
                                        //                                 );
                                        //                             }
                                    }
                                }

                                //             let mut fast_random = FastRandBool::<1>::new();

                                //             let mut remaining_count = 0;

                                count += 1;
                                PartialUnitigsColorStructure::<CX>::clear_temp_buffer(
                                    src_extra_buffer,
                                );
                            },
                        );

                        for index in reads.drain() {
                            //                 let read_struct = &reads_vec[index];
                            //                 let read = read_struct.read.as_reference(&reads_storage);
                            //                 let hash_beginning = read_struct.flags & HASH_ENDING_FLAG_MASK == 0;
                            //                 let both_ends = read_struct.flags & OTHER_END_FLAG_MASK != 0;

                            //                 let should_change = if both_ends {
                            //                     fast_random.get_randbool()
                            //                 } else {
                            //                     false
                            //                 };

                            //                 let hash_beginning = hash_beginning ^ should_change;

                            //                 let hash = HashGenerator::<MH>::get_extremal_hash(
                            //                     &DelayedHashComputation,
                            //                     read,
                            //                     k,
                            //                     hash_beginning,
                            //                 );

                            //                 let target_bucket = MH::get_bucket(
                            //                     0,
                            //                     next_subpartition_buckets
                            //                         .get_buckets_count()
                            //                         .normal_buckets_count_log,
                            //                     hash.to_unextendable(),
                            //                 );

                            //                 let could_be_circular =
                            //                     both_ends && should_change && (target_bucket == bucket_index);

                            //                 let is_circular = if unlikely(could_be_circular) {
                            //                     let other_hash = HashGenerator::<MH>::get_extremal_hash(
                            //                         &DelayedHashComputation,
                            //                         read,
                            //                         k,
                            //                         !hash_beginning,
                            //                     )
                            //                     .to_unextendable();
                            //                     // If beginning and ending hashes are equal, assume this unitig is circular
                            //                     hash.to_unextendable() == other_hash
                            //                 } else {
                            //                     false
                            //                 };

                            //                 if is_circular {
                            //                     // Write a circular unitig

                            //                     let circular_unitigs_buffer = tmp_final_circular_unitigs_buffer
                            //                         .as_mut()
                            //                         .unwrap_or(&mut tmp_final_unitigs_buffer);
                            //                     let bases_count = read.bases_count() - 1;

                            //                     let (color, _counters) = get_color_and_counters::<CX>(&read_struct.extra);

                            //                     CX::ColorsMergeManagerType::reset_unitig_color_structure(
                            //                         &mut join_colors_structure,
                            //                     );
                            //                     CX::ColorsMergeManagerType::join_structures::<false>(
                            //                         &mut join_colors_structure,
                            //                         &color,
                            //                         &extra_buffer,
                            //                         0,
                            //                         Some(bases_count - k + 1),
                            //                     );
                            //                     let writable_color = CX::ColorsMergeManagerType::encode_part_unitigs_colors(
                            //                         &mut join_colors_structure,
                            //                         &mut join_extra_buffer,
                            //                     );

                            //                     circular_unitigs_buffer.add_read(
                            //                         read.subslice(0, bases_count).into_bases_iter(),
                            //                         None,
                            //                         writable_color,
                            //                         &join_extra_buffer,
                            //                         (),
                            //                         &(),
                            //                         #[cfg(feature = "support_kmer_counters")]
                            //                         _counters,
                            //                     );
                            //                 } else {
                            //                     has_written = true;
                            //                     let should_rc = !hash.is_forward();
                            //                     let hash_beginning = hash_beginning ^ should_rc;
                            //                     let last_align = if hash_beginning {
                            //                         0
                            //                     } else {
                            //                         (read.bases_count() - k) % 4
                            //                     };

                            //                     remaining_count += 1;
                            //                     next_subpartition_buckets.add_element_extended(
                            //                         target_bucket,
                            //                         &read_struct.extra,
                            //                         &extra_buffer,
                            //                         &CompressedReadsBucketData {
                            //                             read: ReadData::Packed(read).reverse_complement(should_rc),
                            //                             multiplicity: 0,
                            //                             minimizer_pos: last_align as u16,
                            //                             extra_bucket: 0,
                            //                             flags: (!hash_beginning as u8) | ((both_ends as u8) << 1),
                            //                         },
                            //                     );
                            //                 }
                        }

                        if has_written {
                            has_joinable_unitigs.store(true, Ordering::Relaxed);
                        }

                        //             total_remaining_count.fetch_add(remaining_count, Ordering::Relaxed);

                        reads_storage.clear();
                        reads_vec.clear();
                        PartialUnitigsColorStructure::<CX>::clear_temp_buffer(&mut extra_buffer);

                        println!("COUNT: {:?} total duplicates: {}", count, count_dupl);
                    }

                    additional_buffer.put_back(additional_buckets.finalize().0);
                    subpartition_buffer.put_back(next_subpartition_buckets.finalize().0);
                    tmp_final_unitigs_buffer.finalize();
                });

            subloop_index += 1;

            if !has_joinable_unitigs.into_inner() {
                break;
            }
        }

        let do_logging = if log_timer.elapsed() > MINIMUM_LOG_DELTA_TIME {
            log_timer = Instant::now();
            true
        } else {
            false
        };

        if do_logging {
            ggcat_logging::info!(
                "Processing bucket: {}/{} {}",
                bucket_index,
                input_buckets_count.total_buckets_count,
                PHASES_TIMES_MONITOR
                    .read()
                    .get_formatted_counter_without_memory()
            );
        }
    }
}
