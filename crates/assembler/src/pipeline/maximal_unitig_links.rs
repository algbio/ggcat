mod mappings_loader;
mod maximal_hash_entry;
pub(crate) mod maximal_unitig_index;

use crate::pipeline::maximal_unitig_links::mappings_loader::{
    MaximalUnitigLinksMapping, MaximalUnitigLinksMappingsLoader,
};
use crate::pipeline::maximal_unitig_links::maximal_hash_entry::{
    MaximalHashCompare, MaximalHashEntry, MaximalHashEntrySerializer, MaximalUnitigPosition,
};
use crate::pipeline::maximal_unitig_links::maximal_unitig_index::{
    DoubleMaximalUnitigLinks, MaximalUnitigFlags, MaximalUnitigIndex, MaximalUnitigLink,
    MaximalUnitigLinkSerializer,
};
use colors::colors_manager::ColorsManager;
use colors::colors_manager::color_types::PartialUnitigsColorStructure;
use config::{
    BucketIndexType, DEFAULT_OUTPUT_BUFFER_SIZE, DEFAULT_PER_CPU_BUFFER_SIZE,
    DEFAULT_PREFETCH_AMOUNT, KEEP_FILES, SwapPriority, get_compression_level_info, get_memory_mode,
};
use dashmap::DashSet;
use hashbrown::HashSet;
use hashes::HashFunctionFactory;
use hashes::{ExtendableHashTraitType, HashFunction, HashableSequence};
use io::concurrent::structured_sequences::binary::SequenceDataWithAbundance;
use io::concurrent::structured_sequences::concurrent::FastaWriterConcurrentBuffer;
use io::concurrent::structured_sequences::{StructuredSequenceBackend, StructuredSequenceWriter};
use io::concurrent::temp_reads::creads_utils::{
    CompressedReadsBucketDataSerializer, DeserializedRead, NoMinimizerPosition, NoMultiplicity,
    NoSecondBucket,
};
use io::concurrent::temp_reads::extra_data::SequenceExtraDataTempBufferManagement;
use nightly_quirks::slice_group_by::SliceGroupBy;
use parallel_processor::buckets::concurrent::{BucketsThreadBuffer, BucketsThreadDispatcher};
use parallel_processor::buckets::readers::BucketReader;
use parallel_processor::buckets::readers::async_binary_reader::AllowedCheckpointStrategy;
use parallel_processor::buckets::readers::compressed_binary_reader::CompressedBinaryReader;
use parallel_processor::buckets::writers::compressed_binary_writer::CompressedBinaryWriter;
use parallel_processor::buckets::{DuplicatesBuckets, MultiThreadBuckets};
use parallel_processor::fast_smart_bucket_sort::fast_smart_radix_sort;
use parallel_processor::memory_fs::RemoveFileMode;
use parallel_processor::phase_times_monitor::PHASES_TIMES_MONITOR;
use parallel_processor::utils::scoped_thread_local::ScopedThreadLocal;
use rayon::prelude::*;
use std::cmp::max;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use utils::vec_slice::VecSlice;

pub fn build_maximal_unitigs_links<
    MH: HashFunctionFactory,
    CX: ColorsManager,
    BK: StructuredSequenceBackend<PartialUnitigsColorStructure<CX>, DoubleMaximalUnitigLinks>,
>(
    in_file: PathBuf,
    temp_dir: &Path,
    out_file: &StructuredSequenceWriter<
        PartialUnitigsColorStructure<CX>,
        DoubleMaximalUnitigLinks,
        BK,
    >,
    k: usize,
) {
    // TODO: Parametrize depending on the reads count!
    const DEFAULT_BUCKET_HASHES_SIZE_LOG: usize = 8;

    let buckets_count = 1 << DEFAULT_BUCKET_HASHES_SIZE_LOG;

    let self_complemental_unitigs = DashSet::new();

    // Hash all the extremities
    let (step_1_hash_files, unitigs_count) = {
        let unitigs_count = AtomicU64::new(0);

        PHASES_TIMES_MONITOR
            .write()
            .start_phase("phase: maximal unitigs links building [step 1]".to_string());

        let maximal_unitigs_reader_step1 =
            CompressedBinaryReader::new(&in_file, RemoveFileMode::Keep, DEFAULT_PREFETCH_AMOUNT);

        let maximal_unitigs_extremities_hashes_buckets =
            Arc::new(MultiThreadBuckets::<CompressedBinaryWriter>::new(
                buckets_count,
                temp_dir.join("mu-hashes"),
                None,
                &(
                    get_memory_mode(SwapPriority::HashBuckets),
                    CompressedBinaryWriter::CHECKPOINT_SIZE_UNLIMITED,
                    get_compression_level_info(),
                ),
                &(),
                DuplicatesBuckets::None,
            ));

        rayon::scope(|_s| {
            (0..rayon::current_num_threads())
                .into_par_iter()
                .for_each(|_| {
                    let mut unitigs_partial_count = 0;

                    let mut hashes_tmp = BucketsThreadDispatcher::<
                        _,
                        MaximalHashEntrySerializer<MH::HashTypeUnextendable>,
                    >::new(
                        &maximal_unitigs_extremities_hashes_buckets,
                        BucketsThreadBuffer::new(DEFAULT_PER_CPU_BUFFER_SIZE, buckets_count),
                        (),
                    );

                    while maximal_unitigs_reader_step1
                        .decode_bucket_items_parallel::<CompressedReadsBucketDataSerializer<
                            _,
                            typenum::consts::U0,
                            NoSecondBucket,
                            NoMultiplicity,
                            NoMinimizerPosition,
                        >, _>(
                            Vec::new(),
                            SequenceDataWithAbundance::<PartialUnitigsColorStructure<CX>, ()>::new_temp_buffer(),
                            AllowedCheckpointStrategy::DecompressOnly,
                            |DeserializedRead {
                                read,
                                extra: SequenceDataWithAbundance::<PartialUnitigsColorStructure<CX>, ()> { index, .. },
                                ..
                            },
                             _extra_buffer,
                             _checkpoint_data| {
                                let read_len = read.bases_count();
                                unitigs_partial_count += 1;

                                let first_hash = MH::new(read.sub_slice(0..(k - 1)), k - 1)
                                    .iter()
                                    .next()
                                    .unwrap();
                                let last_hash =
                                    MH::new(read.sub_slice((read_len - k + 1)..read_len), k - 1)
                                        .iter()
                                        .next()
                                        .unwrap();

                                let first_hash_unx = first_hash.to_unextendable();
                                let last_hash_unx = last_hash.to_unextendable();

                                let self_complemental = (first_hash_unx == last_hash_unx)
                                    && (first_hash.is_rc_symmetric()
                                        || (first_hash.is_forward() != last_hash.is_forward()));

                                if self_complemental {
                                    self_complemental_unitigs.insert(index);
                                }

                                hashes_tmp.add_element(
                                    MH::get_bucket(
                                        0,
                                        DEFAULT_BUCKET_HASHES_SIZE_LOG,
                                        first_hash_unx,
                                    ),
                                    &(),
                                    &MaximalHashEntry::new(
                                        first_hash_unx,
                                        index,
                                        MaximalUnitigPosition::Beginning,
                                        first_hash.is_forward(),
                                        0,
                                    ),
                                );

                                if first_hash.is_rc_symmetric() {
                                    hashes_tmp.add_element(
                                        MH::get_bucket(
                                            0,
                                            DEFAULT_BUCKET_HASHES_SIZE_LOG,
                                            first_hash_unx,
                                        ),
                                        &(),
                                        &MaximalHashEntry::new(
                                            first_hash_unx,
                                            index,
                                            MaximalUnitigPosition::Beginning,
                                            !first_hash.is_forward(),
                                            0,
                                        ),
                                    );
                                }

                                hashes_tmp.add_element(
                                    MH::get_bucket(
                                        0,
                                        DEFAULT_BUCKET_HASHES_SIZE_LOG,
                                        last_hash_unx,
                                    ),
                                    &(),
                                    &MaximalHashEntry::new(
                                        last_hash_unx,
                                        index,
                                        MaximalUnitigPosition::Ending,
                                        !last_hash.is_forward(),
                                        read_len as u64 - k as u64 + 1,
                                    ),
                                );

                                if last_hash.is_rc_symmetric() {
                                    hashes_tmp.add_element(
                                        MH::get_bucket(
                                            0,
                                            DEFAULT_BUCKET_HASHES_SIZE_LOG,
                                            last_hash_unx,
                                        ),
                                        &(),
                                        &MaximalHashEntry::new(
                                            last_hash_unx,
                                            index,
                                            MaximalUnitigPosition::Ending,
                                            last_hash.is_forward(),
                                            read_len as u64 - k as u64 + 1,
                                        ),
                                    );
                                }
                            },
                            k,
                        )
                        .is_some()
                    {
                        continue;
                    }

                    unitigs_count.fetch_add(unitigs_partial_count, Ordering::Relaxed);
                    hashes_tmp.finalize();
                });
        });
        (
            maximal_unitigs_extremities_hashes_buckets.finalize_single(),
            unitigs_count.into_inner(),
        )
    };

    let entries_per_bucket =
        max(1, unitigs_count.next_power_of_two() / buckets_count as u64) as usize;
    let entries_per_bucket_log = entries_per_bucket.ilog2() as usize;

    // Sort the hashes
    let maximal_unitig_links_data_step2 = {
        PHASES_TIMES_MONITOR
            .write()
            .start_phase("phase: maximal unitigs links building [step 2]".to_string());

        let maximal_links_buckets = Arc::new(MultiThreadBuckets::<CompressedBinaryWriter>::new(
            buckets_count,
            temp_dir.join("maximal-links"),
            None,
            &(
                get_memory_mode(SwapPriority::LinksBuckets),
                CompressedBinaryWriter::CHECKPOINT_SIZE_UNLIMITED,
                get_compression_level_info(),
            ),
            &(),
            DuplicatesBuckets::None,
        ));

        let buckets_thread_buffers = ScopedThreadLocal::new(move || {
            BucketsThreadBuffer::new(DEFAULT_PER_CPU_BUFFER_SIZE, buckets_count)
        });

        step_1_hash_files.par_iter().for_each(|input| {
            let mut buffers = buckets_thread_buffers.get();
            let mut links_tmp = BucketsThreadDispatcher::<_, MaximalUnitigLinkSerializer>::new(
                &maximal_links_buckets,
                buffers.take(),
                (),
            );

            let mut hashes_vec = Vec::new();
            let mut tmp_links_vec = Vec::new();

            CompressedBinaryReader::new(
                &input.path,
                RemoveFileMode::Remove {
                    remove_fs: !KEEP_FILES.load(Ordering::Relaxed),
                },
                DEFAULT_PREFETCH_AMOUNT,
            )
            .decode_all_bucket_items::<MaximalHashEntrySerializer<MH::HashTypeUnextendable>, _>(
                (),
                &mut (),
                |h, _| {
                    hashes_vec.push(h);
                },
                (),
            );

            fast_smart_radix_sort::<_, MaximalHashCompare<MH>, false>(&mut hashes_vec[..]);

            for x in hashes_vec.nq_group_by_mut(|a, b| a.hash == b.hash) {
                if x.len() == 1 {
                    continue;
                }

                x.sort_unstable_by_key(|v| v.entry());

                for val in x.iter() {
                    tmp_links_vec.clear();

                    let indexes_slice = VecSlice::new_extend_iter(
                        &mut tmp_links_vec,
                        x.iter()
                            .filter(|v| v.direction() != val.direction())
                            .map(|v| {
                                MaximalUnitigIndex::new(
                                    v.entry(),
                                    v.overlap_start(),
                                    MaximalUnitigFlags::new_direction(
                                        val.position() == MaximalUnitigPosition::Beginning,
                                        v.position() == MaximalUnitigPosition::Ending,
                                    ),
                                )
                            }),
                    );

                    let link = MaximalUnitigLink::new(val.entry(), indexes_slice);

                    if link.entries.len() > 0 {
                        links_tmp.add_element(
                            (val.entry() / (1 << entries_per_bucket_log)) as BucketIndexType,
                            &tmp_links_vec,
                            &link,
                        );
                    }
                }
            }
            buffers.put_back(links_tmp.finalize().0);
        });
        maximal_links_buckets.finalize_single()
    };

    // Rewrite the output file to include found links
    {
        let self_complemental_unitigs = self_complemental_unitigs
            .into_iter()
            .collect::<HashSet<_>>();

        PHASES_TIMES_MONITOR
            .write()
            .start_phase("phase: maximal unitigs links building [step 3]".to_string());

        let mappings_loader = MaximalUnitigLinksMappingsLoader::new(
            maximal_unitig_links_data_step2,
            entries_per_bucket,
            rayon::current_num_threads(),
        );

        let maximal_unitigs_reader_step3 = CompressedBinaryReader::new(
            &in_file,
            RemoveFileMode::Remove {
                remove_fs: !KEEP_FILES.load(Ordering::Relaxed),
            },
            DEFAULT_PREFETCH_AMOUNT,
        );

        rayon::scope(|_s| {
            (0..rayon::current_num_threads())
                .into_par_iter()
                .for_each(|thread_index| {
                    let mut tmp_final_unitigs_buffer = FastaWriterConcurrentBuffer::new(
                        out_file,
                        DEFAULT_OUTPUT_BUFFER_SIZE,
                        false,
                        k,
                    );

                    let mut temp_sequence_buffer = Vec::new();

                    let mut current_mapping = Arc::new(MaximalUnitigLinksMapping::empty());

                    while maximal_unitigs_reader_step3
                        .decode_bucket_items_parallel::<CompressedReadsBucketDataSerializer<
                            _,
                            typenum::consts::U0,
                            NoSecondBucket,
                            NoMultiplicity,
                            NoMinimizerPosition,
                        >, _>(
                            Vec::new(),
                            SequenceDataWithAbundance::<PartialUnitigsColorStructure<CX>, ()>::new_temp_buffer(),
                            AllowedCheckpointStrategy::DecompressOnly,
                            |DeserializedRead {
                                read,
                                extra: SequenceDataWithAbundance::<_, ()> { index, color, .. },
                                ..
                            },
                             extra_buffer,
                             _checkpoint_data| {
                                temp_sequence_buffer.clear();
                                temp_sequence_buffer.extend(read.as_bases_iter());

                                if !current_mapping.has_mapping(index) {
                                    current_mapping =
                                        mappings_loader.get_mapping_for(index, thread_index);
                                }

                                let (mut links, links_buffer) = current_mapping.get_mapping(index);
                                links.is_self_complemental =
                                    self_complemental_unitigs.contains(&index);

                                tmp_final_unitigs_buffer.add_read(
                                    &temp_sequence_buffer,
                                    Some(index),
                                    color,
                                    &extra_buffer.0,
                                    links,
                                    links_buffer,
                                    #[cfg(feature = "support_kmer_counters")]
                                    _abundance,
                                );
                            },
                            k,
                        )
                        .is_some()
                    {
                        tmp_final_unitigs_buffer.flush();
                    }

                    mappings_loader.notify_thread_ending(thread_index);
                });
        });
    }
}
