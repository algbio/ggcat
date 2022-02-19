use crate::assemble_pipeline::reorganize_reads::ReorganizedReadsExtraData;
use crate::assemble_pipeline::AssemblePipeline;
use crate::colors::colors_manager::{ColorsManager, ColorsMergeManager};
use crate::config::DEFAULT_OUTPUT_BUFFER_SIZE;
use crate::hashes::{HashFunctionFactory, HashableSequence};
use crate::io::concurrent::fasta_writer::FastaWriterConcurrentBuffer;
use crate::io::concurrent::temp_reads::reads_reader::IntermediateReadsReader;
use crate::io::reads_writer::ReadsWriter;
use crate::io::sequences_reader::FastaSequence;
use crate::io::structs::unitig_link::{UnitigFlags, UnitigIndex, UnitigLink};
use crate::utils::compressed_read::CompressedReadIndipendent;
use crate::utils::Utils;
use crate::KEEP_FILES;
use hashbrown::HashMap;
use parallel_processor::memory_fs::file::reader::FileReader;
use parallel_processor::memory_fs::MemoryFs;
use parallel_processor::phase_times_monitor::PHASES_TIMES_MONITOR;
use parking_lot::Mutex;
use rayon::iter::IntoParallelRefIterator;
use rayon::iter::ParallelIterator;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering;

#[derive(Copy, Clone, Debug)]
struct FinalUnitigInfo {
    is_start: bool,
    is_circular: bool,
    flags: UnitigFlags,
}

impl AssemblePipeline {
    pub fn build_unitigs<MH: HashFunctionFactory, CX: ColorsManager>(
        mut read_buckets_files: Vec<PathBuf>,
        mut unitig_map_files: Vec<PathBuf>,
        _temp_path: &Path,
        out_file: &Mutex<ReadsWriter>,
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

        inputs.par_iter().for_each(|(read_file, unitigs_map_file)| {
            let mut tmp_final_unitigs_buffer =
                FastaWriterConcurrentBuffer::new(&out_file, DEFAULT_OUTPUT_BUFFER_SIZE);

            assert_eq!(
                Utils::get_bucket_index(read_file),
                Utils::get_bucket_index(unitigs_map_file)
            );

            let bucket_index = Utils::get_bucket_index(read_file);

            let mut reader = FileReader::open(&unitigs_map_file).unwrap();

            let mut unitigs_hashmap = HashMap::new();
            let mut unitigs_tmp_vec = Vec::new();

            let mut counter: usize = 0;
            while let Some(link) = UnitigLink::read_from(&mut reader, &mut unitigs_tmp_vec) {
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

            drop(reader);
            MemoryFs::remove_file(&unitigs_map_file, !KEEP_FILES.load(Ordering::Relaxed)).unwrap();


            let mut final_sequences = Vec::with_capacity(counter);
            let mut temp_storage = Vec::new();
            final_sequences.resize(counter, None);

            IntermediateReadsReader::<ReorganizedReadsExtraData<<CX::ColorsMergeManagerType<MH> as ColorsMergeManager<
                MH,
                CX,
            >>::PartialUnitigsColorStructure>>::new(
                read_file,
                !KEEP_FILES.load(Ordering::Relaxed),
            )
            .for_each::<_, typenum::U0>(|_, index, seq| {
                let &(findex, unitig_info) = unitigs_hashmap.get(&index.unitig).unwrap();
                final_sequences[findex] = Some((
                    CompressedReadIndipendent::from_read(&seq, &mut temp_storage),
                    unitig_info,
                    index.color
                ));
            });

            let mut temp_sequence = Vec::new();
            let mut ident_buffer = Vec::new();

            let mut final_unitig_color = CX::ColorsMergeManagerType::<MH>::alloc_unitig_color_structure();

            'uloop: for sequence in final_sequences.group_by(|_a, b| !b.as_ref().unwrap().1.is_start) {
                let is_backwards = !sequence[0].as_ref().unwrap().1.flags.is_forward();
                let is_circular = sequence[0].as_ref().unwrap().1.is_circular;

                temp_sequence.clear();
                CX::ColorsMergeManagerType::<MH>::reset_unitig_color_structure(&mut final_unitig_color);

                let mut is_first = true;

                for upart in if is_backwards {
                    itertools::Either::Right(sequence.iter().rev())
                } else {
                    itertools::Either::Left(sequence.iter())
                } {
                    let (read, FinalUnitigInfo { flags, .. }, color) = upart.as_ref().unwrap();

                    let compr_read = read.as_reference(&temp_storage);
                    if compr_read.bases_count() == 0 {
                        continue 'uloop;
                    }
                    if is_first {
                        if flags.is_reverse_complemented() {
                            temp_sequence.extend(compr_read.as_reverse_complement_bases_iter());
                            CX::ColorsMergeManagerType::<MH>::join_structures::<true>(&mut final_unitig_color, color, 0);
                        } else {
                            temp_sequence.extend(compr_read.as_bases_iter());
                            CX::ColorsMergeManagerType::<MH>::join_structures::<false>(&mut final_unitig_color, color, 0);
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
                            CX::ColorsMergeManagerType::<MH>::join_structures::<true>(&mut final_unitig_color, color, 1);
                        } else {
                            temp_sequence.extend(
                                compr_read
                                    // When two unitigs are merging, they share a full k length kmer
                                    .sub_slice(k..compr_read.bases_count())
                                    .as_bases_iter(),
                            );
                            CX::ColorsMergeManagerType::<MH>::join_structures::<false>(&mut final_unitig_color, color, 1);
                        }
                    }
                }

                // In case of circular unitigs, remove an extra ending base
                if is_circular {
                    temp_sequence.pop();
                    CX::ColorsMergeManagerType::<MH>::pop_base(&mut final_unitig_color);
                }

                ident_buffer.clear();
                write!(ident_buffer, "> {} J", bucket_index).unwrap();

                let writable_color = CX::ColorsMergeManagerType::<MH>::encode_part_unitigs_colors(&mut final_unitig_color);
                CX::ColorsMergeManagerType::<MH>::print_color_data(&writable_color, &mut ident_buffer);

                // <CX::ColorsMergeManagerType<MH> as ColorsMergeManager<
                //     MH,
                //     CX,
                // >>::debug_tucs(&final_unitig_color, temp_sequence.as_slice());


                tmp_final_unitigs_buffer.add_read(FastaSequence {
                    ident: ident_buffer.as_slice(),
                    seq: temp_sequence.as_slice(),
                    qual: None,
                });
            }

            CX::ColorsMergeManagerType::<MH>::clear_deserialized_unitigs_colors();
            tmp_final_unitigs_buffer.finalize();
        });
    }
}
