use crate::pipeline::reorganize_reads::ReorganizedReadsExtraData;
use colors::colors_manager::color_types::PartialUnitigsColorStructure;
use colors::colors_manager::ColorsMergeManager;
use colors::colors_manager::{color_types, ColorsManager};
use config::{DEFAULT_OUTPUT_BUFFER_SIZE, DEFAULT_PREFETCH_AMOUNT, KEEP_FILES};
use hashbrown::HashMap;
use hashes::{HashFunctionFactory, HashableSequence, MinimizerHashFunctionFactory};
use io::compressed_read::CompressedReadIndipendent;
use io::concurrent::structured_sequences::concurrent::FastaWriterConcurrentBuffer;
use io::concurrent::structured_sequences::{StructuredSequenceBackend, StructuredSequenceWriter};
use io::concurrent::temp_reads::creads_utils::CompressedReadsBucketDataSerializer;
use io::concurrent::temp_reads::extra_data::SequenceExtraDataTempBufferManagement;
use io::get_bucket_index;
use io::structs::unitig_link::{UnitigFlags, UnitigIndex, UnitigLinkSerializer};
use nightly_quirks::slice_group_by::SliceGroupBy;
use parallel_processor::buckets::bucket_writer::BucketItemSerializer;
use parallel_processor::buckets::readers::compressed_binary_reader::CompressedBinaryReader;
use parallel_processor::buckets::readers::lock_free_binary_reader::LockFreeBinaryReader;
use parallel_processor::buckets::readers::BucketReader;
use parallel_processor::memory_fs::RemoveFileMode;
use parallel_processor::phase_times_monitor::PHASES_TIMES_MONITOR;
use rayon::prelude::*;
use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering;

#[cfg(feature = "support_kmer_counters")]
use io::concurrent::structured_sequences::SequenceAbundance;

#[derive(Copy, Clone, Debug)]
struct FinalUnitigInfo {
    is_start: bool,
    is_circular: bool,
    flags: UnitigFlags,
}

type CompressedReadsDataSerializerUnitigsBuilding<H, MH, CX> = CompressedReadsBucketDataSerializer<
    ReorganizedReadsExtraData<PartialUnitigsColorStructure<H, MH, CX>>,
    typenum::U0,
    false,
>;

pub fn build_unitigs<
    H: MinimizerHashFunctionFactory,
    MH: HashFunctionFactory,
    CX: ColorsManager,
    BK: StructuredSequenceBackend<PartialUnitigsColorStructure<H, MH, CX>, ()>,
>(
    mut read_buckets_files: Vec<PathBuf>,
    mut unitig_map_files: Vec<PathBuf>,
    _temp_path: &Path,
    out_file: &StructuredSequenceWriter<PartialUnitigsColorStructure<H, MH, CX>, (), BK>,
    k: usize,
) {
    PHASES_TIMES_MONITOR
        .write()
        .start_phase("phase: unitigs building".to_string());

    read_buckets_files.sort();
    unitig_map_files.sort();

    let inputs: Vec<_> = read_buckets_files
        .iter()
        .zip(unitig_map_files.iter())
        .collect();

    rayon::scope(|_s| {
        inputs
            .par_iter()
            .enumerate()
            .for_each(|(_index, (read_file, unitigs_map_file))| {
                let mut tmp_final_unitigs_buffer =
                    FastaWriterConcurrentBuffer::new(out_file, DEFAULT_OUTPUT_BUFFER_SIZE, true);

                assert_eq!(
                    get_bucket_index(read_file),
                    get_bucket_index(unitigs_map_file)
                );

                let bucket_index = get_bucket_index(read_file);

                let mut unitigs_map_reader = LockFreeBinaryReader::new(
                    &unitigs_map_file,
                    RemoveFileMode::Remove {
                        remove_fs: !KEEP_FILES.load(Ordering::Relaxed),
                    },
                    DEFAULT_PREFETCH_AMOUNT,
                );

                let mut unitigs_map_stream = unitigs_map_reader.get_single_stream();

                let mut unitigs_hashmap = HashMap::new();
                let mut unitigs_tmp_vec = Vec::new();

                let mut deserializer = UnitigLinkSerializer::new();

                let mut counter: usize = 0;
                while let Some(link) =
                                deserializer.read_from(&mut unitigs_map_stream, &mut unitigs_tmp_vec, &mut ())
                {
                    let start_unitig = UnitigIndex::new(
                        bucket_index,
                        link.entry() as usize,
                        link.flags().is_reverse_complemented(),
                    );

                    let is_circular = link
                        .entries
                        .get_slice(&unitigs_tmp_vec)
                        .last()
                        .map(|u| u == &start_unitig)
                        .unwrap_or(false);

                    assert!(!unitigs_hashmap.contains_key(&start_unitig));
                    unitigs_hashmap.insert(
                        start_unitig,
                        (
                            counter,
                            FinalUnitigInfo {
                                is_start: true,
                                is_circular,
                                flags: link.flags(),
                            },
                        ),
                    );

                    counter += 1;

                    for el in link.entries.get_slice(&unitigs_tmp_vec) {
                        if *el != start_unitig {
                            assert!(!unitigs_hashmap.contains_key(el));
                            unitigs_hashmap.insert(
                                *el,
                                (
                                    counter,
                                    FinalUnitigInfo {
                                        is_start: false,
                                        is_circular,
                                        flags: UnitigFlags::new_direction(
                                            /*unused*/ false,
                                            el.is_reverse_complemented(),
                                        ),
                                    },
                                ),
                            );
                            counter += 1;
                        }
                    }

                    unitigs_tmp_vec.clear();
                }

                drop(unitigs_map_stream);
                drop(unitigs_map_reader);

                let mut final_sequences = Vec::with_capacity(counter);
                let mut temp_storage = Vec::new();
                final_sequences.resize(counter, None);

                let mut color_extra_buffer = ReorganizedReadsExtraData::<
                    color_types::PartialUnitigsColorStructure<H, MH, CX>,
                >::new_temp_buffer();
                let mut final_color_extra_buffer =
                    color_types::PartialUnitigsColorStructure::<H, MH, CX>::new_temp_buffer();

                CompressedBinaryReader::new(
                    read_file,
                    RemoveFileMode::Remove {
                        remove_fs: !KEEP_FILES.load(Ordering::Relaxed),
                    },
                    DEFAULT_PREFETCH_AMOUNT,
                )
                .decode_all_bucket_items::<CompressedReadsDataSerializerUnitigsBuilding<H, MH, CX>, _>(
                    Vec::new(),
                    &mut color_extra_buffer,
                    |(_, _, index, seq), _color_extra_buffer| {
                        let &(findex, unitig_info) = unitigs_hashmap.get(&index.unitig).unwrap();
                        final_sequences[findex] = Some((
                            CompressedReadIndipendent::from_read(&seq, &mut temp_storage),
                            unitig_info,
                            index.colors,
                            #[cfg(feature = "support_kmer_counters")]
                            index.counters,
                        ));
                    },
                );

                let mut temp_sequence = Vec::new();

                let mut final_unitig_color =
                    CX::ColorsMergeManagerType::<H, MH>::alloc_unitig_color_structure();

                'uloop: for sequence in
                    final_sequences.nq_group_by(|_a, b| !b.as_ref().unwrap().1.is_start)
                {
                    let is_backwards = !sequence[0].as_ref().unwrap().1.flags.is_forward();
                    let is_circular = sequence[0].as_ref().unwrap().1.is_circular;

                    temp_sequence.clear();
                    CX::ColorsMergeManagerType::<H, MH>::reset_unitig_color_structure(
                        &mut final_unitig_color,
                    );
                    #[cfg(feature = "support_kmer_counters")]
                    let mut abundance = SequenceAbundance {
                        first: 0,
                        sum: 0,
                        last: 0,
                    };
                    #[cfg(feature = "support_kmer_counters")]
                    let mut prev_last = 0;

                    let mut is_first = true;

                    for upart in if is_backwards {
                        itertools::Either::Right(sequence.iter().rev())
                    } else {
                        itertools::Either::Left(sequence.iter())
                    } {
                        #[cfg(feature = "support_kmer_counters")]
                        let (read, FinalUnitigInfo { flags, .. }, color, counters) = upart.as_ref().unwrap();

                        #[cfg(not(feature = "support_kmer_counters"))]
                        let (read, FinalUnitigInfo { flags, .. }, color) = upart.as_ref().unwrap();

                        let compr_read = read.as_reference(&temp_storage);
                        if compr_read.bases_count() == 0 {
                            continue 'uloop;
                        }
                        if is_first {
                            if flags.is_reverse_complemented() {
                                temp_sequence.extend(compr_read.as_reverse_complement_bases_iter());
                                CX::ColorsMergeManagerType::<H, MH>::join_structures::<true>(
                                    &mut final_unitig_color,
                                    color,
                                    &color_extra_buffer.0,
                                    0,
                                );
                                #[cfg(feature = "support_kmer_counters")]
                                {
                                    abundance.first = counters.last;
                                    abundance.sum += counters.sum;
                                }
                            } else {
                                temp_sequence.extend(compr_read.as_bases_iter());
                                CX::ColorsMergeManagerType::<H, MH>::join_structures::<false>(
                                    &mut final_unitig_color,
                                    color,
                                    &color_extra_buffer.0,
                                    0,
                                );
                                #[cfg(feature = "support_kmer_counters")]
                                {
                                    abundance.first = counters.first;
                                    abundance.sum += counters.sum;
                                }
                            }
                            is_first = false;
                        } else {
                            if flags.is_reverse_complemented() {
                                temp_sequence.extend(
                                    compr_read
                                        // When two unitigs are merging, they share a full k length kmer
                                        .sub_slice(0..compr_read.bases_count() - k)
                                        .as_reverse_complement_bases_iter(),
                                );
                                CX::ColorsMergeManagerType::<H, MH>::join_structures::<true>(
                                    &mut final_unitig_color,
                                    color,
                                    &color_extra_buffer.0,
                                    1,
                                );
                                #[cfg(feature = "support_kmer_counters")] {
                                    abundance.sum += counters.sum - counters.last;
                                }
                            } else {
                                temp_sequence.extend(
                                    compr_read
                                        // When two unitigs are merging, they share a full k length kmer
                                        .sub_slice(k..compr_read.bases_count())
                                        .as_bases_iter(),
                                );
                                CX::ColorsMergeManagerType::<H, MH>::join_structures::<false>(
                                    &mut final_unitig_color,
                                    color,
                                    &color_extra_buffer.0,
                                    1,
                                );
                                #[cfg(feature = "support_kmer_counters")] {
                                    abundance.sum += counters.sum - counters.first;
                                }
                            }
                        }
                        #[cfg(feature = "support_kmer_counters")] {
                            prev_last = abundance.last;
                            abundance.last = counters.last;
                        }
                    }

                    // In case of circular unitigs, remove an extra ending base
                    if is_circular {
                        temp_sequence.pop();
                        #[cfg(feature = "support_kmer_counters")] {
                            abundance.sum -= abundance.last;
                            abundance.last = prev_last;
                        }

                        CX::ColorsMergeManagerType::<H, MH>::pop_base(&mut final_unitig_color);
                    }

                    let writable_color =
                        CX::ColorsMergeManagerType::<H, MH>::encode_part_unitigs_colors(
                            &mut final_unitig_color,
                            &mut final_color_extra_buffer,
                        );

                    tmp_final_unitigs_buffer.add_read(
                        temp_sequence.as_slice(),
                        None,
                        writable_color,
                        &final_color_extra_buffer,
                        (),
                        &(),
                        #[cfg(feature = "support_kmer_counters")]
                        abundance,
                    );

                    // write_fasta_entry::<H, MH, CX, _>(
                    //     &mut ident_buffer,
                    //     &mut tmp_final_unitigs_buffer,
                    //     writable_color,
                    //     &final_color_extra_buffer,
                    //     temp_sequence.as_slice(),
                    //     links_manager.get_unitig_index(bucket_index, unitig_index),
                    // );
                }

                // ReorganizedReadsExtraData::<
                //     color_types::PartialUnitigsColorStructure<H, MH, CX>,
                // >::clear_temp_buffer(&mut color_extra_buffer);

                tmp_final_unitigs_buffer.finalize();
            });
    });
}
