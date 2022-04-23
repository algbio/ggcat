use crate::assemble_pipeline::reorganize_reads::ReorganizedReadsExtraData;
use crate::assemble_pipeline::unitig_links_manager::UnitigLinksManager;
use crate::assemble_pipeline::AssemblePipeline;
use crate::colors::colors_manager::{color_types, ColorsManager, ColorsMergeManager};
use crate::config::{DEFAULT_OUTPUT_BUFFER_SIZE, DEFAULT_PREFETCH_AMOUNT};
use crate::hashes::{HashableHashFunctionFactory, HashableSequence};
use crate::io::concurrent::fasta_writer::FastaWriterConcurrentBuffer;
use crate::io::concurrent::temp_reads::creads_utils::CompressedReadsBucketHelper;
use crate::io::reads_writer::ReadsWriter;
use crate::io::sequences_reader::FastaSequence;
use crate::io::structs::unitig_link::{UnitigFlags, UnitigIndex, UnitigLink};
use crate::utils::compressed_read::CompressedReadIndipendent;
use crate::utils::Utils;
use crate::KEEP_FILES;
use hashbrown::HashMap;
use parallel_processor::buckets::bucket_writer::BucketItem;
use parallel_processor::buckets::readers::compressed_binary_reader::CompressedBinaryReader;
use parallel_processor::buckets::readers::lock_free_binary_reader::LockFreeBinaryReader;
use parallel_processor::buckets::readers::BucketReader;
use parallel_processor::memory_fs::RemoveFileMode;
use parallel_processor::phase_times_monitor::PHASES_TIMES_MONITOR;
use parking_lot::Mutex;
use rayon::prelude::*;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering;

#[derive(Copy, Clone, Debug)]
struct FinalUnitigInfo {
    is_start: bool,
    is_circular: bool,
    flags: UnitigFlags,
}

pub trait FastaCompatibleRead {
    type IntermediateData;
    fn write_unpacked_to_buffer(&self, buffer: &mut Vec<u8>) -> Self::IntermediateData;
    fn as_slice_from_buffer<'a>(
        &'a self,
        buffer: &'a Vec<u8>,
        data: Self::IntermediateData,
    ) -> &'a [u8];
    fn get_length(&self) -> usize;
}

impl FastaCompatibleRead for [u8] {
    type IntermediateData = ();

    #[inline(always)]
    fn write_unpacked_to_buffer(&self, _buffer: &mut Vec<u8>) -> Self::IntermediateData {}

    #[inline(always)]
    fn as_slice_from_buffer<'a>(&'a self, _: &'a Vec<u8>, _: ()) -> &'a [u8] {
        self
    }

    fn get_length(&self) -> usize {
        self.len()
    }
}

#[allow(unused_variables)]
pub fn write_fasta_entry<
    MH: HashableHashFunctionFactory,
    CX: ColorsManager,
    R: FastaCompatibleRead + ?Sized,
    // I: Iterator<Item = (bool, bool, usize)>,
>(
    temp_buffer: &mut Vec<u8>,
    writer: &mut FastaWriterConcurrentBuffer,
    color: color_types::PartialUnitigsColorStructure<MH, CX>,
    read: &R,
    index: usize,
    // links_iterator: I,
) {
    // KC:i:{} km:f:
    temp_buffer.clear();
    write!(temp_buffer, ">{} LN:i:{}", index, read.get_length()).unwrap();

    CX::ColorsMergeManagerType::<MH>::print_color_data(&color, temp_buffer);

    let ident_buffer_size = temp_buffer.len();

    let int_data = read.write_unpacked_to_buffer(temp_buffer);
    let read_slice = read.as_slice_from_buffer(temp_buffer, int_data);

    writer.add_read(FastaSequence {
        ident: &temp_buffer[..ident_buffer_size],
        seq: read_slice,
        qual: None,
    });
}

impl AssemblePipeline {
    pub fn build_unitigs<MH: HashableHashFunctionFactory, CX: ColorsManager>(
        mut read_buckets_files: Vec<PathBuf>,
        mut unitig_map_files: Vec<PathBuf>,
        _temp_path: &Path,
        out_file: &Mutex<ReadsWriter>,
        k: usize,
        links_manager: &UnitigLinksManager,
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
                        FastaWriterConcurrentBuffer::new(&out_file, DEFAULT_OUTPUT_BUFFER_SIZE);

                    assert_eq!(
                        Utils::get_bucket_index(read_file),
                        Utils::get_bucket_index(unitigs_map_file)
                    );

                    let bucket_index = Utils::get_bucket_index(read_file);

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

                    let mut counter: usize = 0;
                    while let Some(link) =
                        UnitigLink::read_from(&mut unitigs_map_stream, &mut unitigs_tmp_vec)
                    {
                        let start_unitig = UnitigIndex::new(
                            bucket_index,
                            link.entry as usize,
                            link.flags.is_reverse_complemented(),
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
                                    flags: link.flags,
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

                    CompressedBinaryReader::new(
                        read_file,
                        RemoveFileMode::Remove {
                            remove_fs: !KEEP_FILES.load(Ordering::Relaxed),
                        },
                        DEFAULT_PREFETCH_AMOUNT,
                    )
                    .decode_all_bucket_items::<CompressedReadsBucketHelper<
                        ReorganizedReadsExtraData<
                            color_types::PartialUnitigsColorStructure<MH, CX>,
                        >,
                        typenum::U0,
                        false,
                    >, _>(
                        Vec::new(),
                        |(_, _, index, seq)| {
                            let &(findex, unitig_info) =
                                unitigs_hashmap.get(&index.unitig).unwrap();
                            final_sequences[findex] = Some((
                                CompressedReadIndipendent::from_read(&seq, &mut temp_storage),
                                unitig_info,
                                index.color,
                            ));
                        },
                    );

                    let mut temp_sequence = Vec::new();
                    let mut ident_buffer = Vec::new();
                    let mut unitig_index = 0;

                    let mut final_unitig_color =
                        CX::ColorsMergeManagerType::<MH>::alloc_unitig_color_structure();

                    'uloop: for sequence in
                        final_sequences.group_by(|_a, b| !b.as_ref().unwrap().1.is_start)
                    {
                        let is_backwards = !sequence[0].as_ref().unwrap().1.flags.is_forward();
                        let is_circular = sequence[0].as_ref().unwrap().1.is_circular;

                        temp_sequence.clear();
                        CX::ColorsMergeManagerType::<MH>::reset_unitig_color_structure(
                            &mut final_unitig_color,
                        );

                        let mut is_first = true;

                        for upart in if is_backwards {
                            itertools::Either::Right(sequence.iter().rev())
                        } else {
                            itertools::Either::Left(sequence.iter())
                        } {
                            let (read, FinalUnitigInfo { flags, .. }, color) =
                                upart.as_ref().unwrap();

                            let compr_read = read.as_reference(&temp_storage);
                            if compr_read.bases_count() == 0 {
                                continue 'uloop;
                            }
                            if is_first {
                                if flags.is_reverse_complemented() {
                                    temp_sequence
                                        .extend(compr_read.as_reverse_complement_bases_iter());
                                    CX::ColorsMergeManagerType::<MH>::join_structures::<true>(
                                        &mut final_unitig_color,
                                        color,
                                        0,
                                    );
                                } else {
                                    temp_sequence.extend(compr_read.as_bases_iter());
                                    CX::ColorsMergeManagerType::<MH>::join_structures::<false>(
                                        &mut final_unitig_color,
                                        color,
                                        0,
                                    );
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
                                    CX::ColorsMergeManagerType::<MH>::join_structures::<true>(
                                        &mut final_unitig_color,
                                        color,
                                        1,
                                    );
                                } else {
                                    temp_sequence.extend(
                                        compr_read
                                            // When two unitigs are merging, they share a full k length kmer
                                            .sub_slice(k..compr_read.bases_count())
                                            .as_bases_iter(),
                                    );
                                    CX::ColorsMergeManagerType::<MH>::join_structures::<false>(
                                        &mut final_unitig_color,
                                        color,
                                        1,
                                    );
                                }
                            }
                        }

                        // In case of circular unitigs, remove an extra ending base
                        if is_circular {
                            temp_sequence.pop();
                            CX::ColorsMergeManagerType::<MH>::pop_base(&mut final_unitig_color);
                        }

                        let writable_color =
                            CX::ColorsMergeManagerType::<MH>::encode_part_unitigs_colors(
                                &mut final_unitig_color,
                            );

                        write_fasta_entry::<MH, CX, _>(
                            &mut ident_buffer,
                            &mut tmp_final_unitigs_buffer,
                            writable_color,
                            temp_sequence.as_slice(),
                            links_manager.get_unitig_index(bucket_index, unitig_index),
                        );
                        unitig_index += 1;
                    }

                    CX::ColorsMergeManagerType::<MH>::clear_deserialized_unitigs_colors();
                    tmp_final_unitigs_buffer.finalize();
                });
        });
    }
}
