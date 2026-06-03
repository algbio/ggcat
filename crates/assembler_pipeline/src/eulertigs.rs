use colors::colors_manager::{ColorsManager, color_types};
use colors::colors_manager::{ColorsMergeManager, color_types::PartialUnitigsColorStructure};
use config::DEFAULT_OUTPUT_BUFFER_SIZE;
use dashmap::DashMap;
use hashes::HashFunctionFactory;
use hashes::{ExtendableHashTraitType, HashFunction, HashableSequence};
use io::compressed_read::{CompressedRead, CompressedReadIndipendent};
use io::concurrent::temp_reads::creads_utils::CompressedReadsBucketDataSerializer;
use io::concurrent::temp_reads::creads_utils::{
    DeserializedRead, NoMinimizerPosition, NoMultiplicity, NoSecondBucket, ToReadData,
};
use io::concurrent::temp_reads::extra_data::{SequenceExtraDataTempBufferManagement, TempBuffer};
use io::concurrent_filewriter::ConcurrentFileWriter;
use io::ident_writer::IdentSequenceWriter;
use io::partial_unitigs_extra_data::{
    PartialUnitigExtraData, PartialUnitigMode, SequenceAbundanceType,
};
use parallel_processor::buckets::readers::binary_reader::ChunkedBinaryReaderIndex;
use parallel_processor::buckets::readers::typed_binary_reader::TypedStreamReader;
use parallel_processor::memory_fs::RemoveFileMode;
use parallel_processor::phase_times_monitor::PHASES_TIMES_MONITOR;
use parking_lot::Mutex;
use rayon::iter::{IntoParallelIterator, ParallelIterator};
use rayon::slice::{ParallelSlice, ParallelSliceMut};
use sequence_output::indirect_reads_extractor::{
    ReadExtractWorkData, indirect_read_extract_all, indirect_read_extract_parts,
};
use sequence_output::sequences_joiner::{AlignedDynamicCompressedRead, IndirectSequencesJoiner};
use sequence_output::structured_sequences::binary::SequenceDataWithLinks;
use sequence_output::structured_sequences::concurrent::FastaWriterConcurrentBuffer;
use sequence_output::structured_sequences::{StructuredSequenceBackend, StructuredSequenceWriter};
use std::cmp::Reverse;
use std::mem::swap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};

#[derive(Clone, Copy, Debug)]
struct CircularUnitigPart {
    orig_index: usize,
    start_pos: usize,
    length: usize,
    rc: bool,
}

struct CircularUnitig {
    base_children: Vec<CircularUnitigPart>,
    used: AtomicBool,
}

impl Clone for CircularUnitig {
    fn clone(&self) -> Self {
        Self {
            base_children: self.base_children.clone(),
            used: AtomicBool::new(false),
        }
    }
}

impl CircularUnitig {
    pub fn new() -> Self {
        Self {
            base_children: vec![],
            used: AtomicBool::new(false),
        }
    }

    pub fn rotate_with_rc(&mut self, child: usize, position: usize, needed_rc: bool) {
        let split_index = self
            .base_children
            .iter()
            .position(|x| {
                (x.orig_index == child) && {
                    x.start_pos <= position && (x.start_pos + x.length > position)
                }
            })
            .expect("Could not find child in base_children, this is a bug");

        if split_index > 0 {
            self.base_children.rotate_left(split_index);
        }

        // Now the unitig part we want to split is at the beginning, so we should split it in two parts
        let target_child = &self.base_children[0];

        let mut first_part = CircularUnitigPart {
            orig_index: target_child.orig_index,
            start_pos: target_child.start_pos,
            length: position - target_child.start_pos,
            rc: target_child.rc,
        };

        let mut second_part = CircularUnitigPart {
            orig_index: target_child.orig_index,
            start_pos: position,
            length: target_child.length - (position - target_child.start_pos),
            rc: target_child.rc,
        };

        if target_child.rc {
            swap(&mut first_part, &mut second_part);
        }

        let wrong_orientation = target_child.rc != needed_rc;

        self.base_children[0] = second_part;
        self.base_children.push(first_part);

        if wrong_orientation {
            self.base_children.reverse();
            for child in &mut self.base_children {
                child.rc = !child.rc;
            }
        }
    }

    pub fn write_packed<'a, MH: HashFunctionFactory, CX: ColorsManager>(
        &self,
        unitigs_kmers: &Vec<u8>,
        unitigs: &DashMap<
            usize,
            (
                CompressedReadIndipendent,
                PartialUnitigsColorStructure<CX>,
                SequenceAbundanceType,
            ),
        >,
        writer: &'a mut AlignedDynamicCompressedRead,
        unitigs_colors_buffer: &TempBuffer<PartialUnitigsColorStructure<CX>>,
        colors_buffer: &mut TempBuffer<PartialUnitigsColorStructure<CX>>,
        k: usize,
        write_full: bool,
        #[allow(dead_code)] _out_abundance: &mut SequenceAbundanceType,
    ) -> CompressedRead<'a> {
        let children_count = self.base_children.len();

        #[cfg(feature = "support_kmer_counters")]
        {
            let first_unitig_entry = unitigs.get(&self.base_children[0].orig_index).unwrap();
            _out_abundance.first = first_unitig_entry.2.first;
        }

        writer.clear();

        for child in self.base_children.iter().take(children_count - 1) {
            let unitig_entry = unitigs.get(&child.orig_index).unwrap();
            let (unitig, src_color, _abundance) = unitig_entry.value();
            let unitig = unitig.as_reference(unitigs_kmers);

            #[cfg(feature = "support_kmer_counters")]
            {
                _out_abundance.sum += _abundance.sum;
            }

            let should_rc = child.rc;
            let rc_offset = if should_rc { k - 1 } else { 0 };
            // Skip the first k-1 bases as if they're already written when merging
            let unitig_slice =
                (child.start_pos + rc_offset)..(child.start_pos + rc_offset + child.length);

            let unitig_part = unitig.sub_slice(unitig_slice.clone());
            writer.append_read(unitig_part, should_rc);
            CX::ColorsMergeManagerType::join_structures_rc(
                colors_buffer,
                src_color,
                unitigs_colors_buffer,
                unitig.bases_count(),
                unitig_slice,
                should_rc,
            );
        }

        // Add the last part, adding k-1 prefix or suffix depending on the rc status
        let last = self.base_children.last().unwrap();

        let end_offset = if !last.rc && !write_full { 0 } else { k - 1 };
        let start_offset = if last.rc && !write_full { k - 1 } else { 0 };

        let last_part_entry = unitigs.get(&last.orig_index).unwrap();

        #[cfg(feature = "support_kmer_counters")]
        {
            let abundance = &last_part_entry.2;
            _out_abundance.sum += abundance.sum;
            _out_abundance.last = if last.rc {
                abundance.first
            } else {
                abundance.last
            };
        }

        let last_part_slice =
            last.start_pos + start_offset..last.start_pos + last.length + end_offset;
        let last_part = last_part_entry
            .0
            .as_reference(unitigs_kmers)
            .sub_slice(last_part_slice.clone());

        writer.append_read(last_part, last.rc);
        CX::ColorsMergeManagerType::join_structures_rc(
            colors_buffer,
            &last_part_entry.1,
            unitigs_colors_buffer,
            last_part_entry.0.bases_count(),
            last_part_slice,
            last.rc,
        );
        writer.get_reference()
    }

    #[allow(dead_code)]
    pub fn debug_check_integrity<MH: HashFunctionFactory, CX: ColorsManager>(
        &mut self,
        k: usize,
        unitigs_kmers: &Vec<u8>,
        unitigs: &DashMap<
            usize,
            (
                CompressedReadIndipendent,
                PartialUnitigsColorStructure<CX>,
                SequenceAbundanceType,
            ),
        >,
        message: String,
    ) {
        let mut parts = vec![];
        for child in &self.base_children {
            let bases = unitigs
                .get(&child.orig_index)
                .unwrap()
                .0
                .as_reference(unitigs_kmers)
                .sub_slice(child.start_pos..(child.start_pos + child.length + k - 1));
            parts.push(if child.rc {
                bases.as_reverse_complement_bases_iter().collect()
            } else {
                bases.as_bases_iter().collect::<Vec<_>>()
            });
        }

        parts.push(parts[0].clone());
        for (i, pair) in parts.windows(2).enumerate() {
            let suffix = &pair[0][pair[0].len() - (k - 1)..];
            let prefix = &pair[1][..k - 1];
            if suffix != prefix {
                panic!(
                    "{} not contiguous to {} =>\nMESSAGE:{} at index: {}",
                    std::str::from_utf8(&pair[0]).unwrap(),
                    std::str::from_utf8(&pair[1]).unwrap(),
                    message,
                    i,
                );
            }
        }
    }

    #[allow(dead_code)]
    pub fn debug_to_string<MH: HashFunctionFactory, CX: ColorsManager>(
        &mut self,
        k: usize,
        unitigs_kmers: &Vec<u8>,
        unitigs: &DashMap<
            usize,
            (
                CompressedReadIndipendent,
                PartialUnitigsColorStructure<CX>,
                SequenceAbundanceType,
            ),
        >,
    ) -> String {
        let mut colors_buffer = color_types::PartialUnitigsColorStructure::<CX>::new_temp_buffer();
        let dummy_buffer = color_types::PartialUnitigsColorStructure::<CX>::new_temp_buffer();
        let mut packed_seq_bases = AlignedDynamicCompressedRead::new();
        let mut abundance = SequenceAbundanceType::default();
        let read = self.write_packed::<MH, CX>(
            unitigs_kmers,
            unitigs,
            &mut packed_seq_bases,
            &dummy_buffer,
            &mut colors_buffer,
            k,
            true,
            &mut abundance,
        );
        read.debug_to_string()
    }
}

struct CircularUnionFind {
    mappings: Vec<CircularUnitig>,
    parent: Vec<usize>,
    rank: Vec<usize>,
}

impl CircularUnionFind {
    pub fn new(capacity: usize) -> Self {
        CircularUnionFind {
            mappings: Vec::with_capacity(capacity),
            parent: Vec::with_capacity(capacity),
            rank: Vec::with_capacity(capacity),
        }
    }

    pub fn flatten_parents(&mut self) {
        for i in 0..self.parent.len() {
            self.find(i);
        }
    }

    pub fn find_flatten(&self, index: usize) -> usize {
        if self.parent[index] != usize::MAX {
            self.parent[index]
        } else {
            index
        }
    }

    pub fn find(&mut self, index: usize) -> usize {
        if self.parent[index] == usize::MAX {
            return index;
        }

        let parent = self.find(self.parent[index]);
        self.parent[index] = parent;
        parent
    }

    pub fn add_unitig(&mut self, length: usize) -> usize {
        let mut unitig = CircularUnitig::new();
        unitig.base_children.push(CircularUnitigPart {
            orig_index: self.mappings.len(),
            start_pos: 0,
            rc: false,
            length,
        });
        self.mappings.push(unitig);
        self.parent.push(usize::MAX);
        self.rank.push(0);
        self.mappings.len() - 1
    }

    // Join two circular unitigs placinga rotation of b inside a at a specific position
    pub fn union(
        &mut self,
        a: usize,
        b: usize,
        a_pos: usize,
        b_rot: usize,
        opposite_orientations: bool,
    ) -> usize {
        let a_parent = self.find(a);
        let b_parent = self.find(b);

        if a_parent == b_parent {
            return a_parent;
        }

        // Align the rotations such that the circular unitigs can be concatenated

        // Adjust rotation in case of reverse complement
        self.mappings[a_parent].rotate_with_rc(a, a_pos, false);
        self.mappings[b_parent].rotate_with_rc(b, b_rot, opposite_orientations);

        let b_children = std::mem::take(&mut self.mappings[b_parent].base_children);
        self.mappings[a_parent]
            .base_children
            .extend(b_children.into_iter());
        self.parent[b_parent] = a_parent;

        a_parent
    }
}

#[derive(Clone, Copy, Debug)]
struct KmerOffset(usize);

impl KmerOffset {
    const REVERSE_FLAG: usize = 1 << (usize::BITS - 1);
    const OFFSET_MASK: usize = !Self::REVERSE_FLAG;

    fn new(offset: usize, reverse: bool) -> Self {
        if reverse {
            KmerOffset(offset | Self::REVERSE_FLAG)
        } else {
            KmerOffset(offset)
        }
    }
    fn get_offset(&self) -> usize {
        self.0 & Self::OFFSET_MASK
    }

    fn is_rc(&self) -> bool {
        (self.0 & Self::REVERSE_FLAG) != 0
    }
}

pub fn build_eulertigs<
    MH: HashFunctionFactory,
    CX: ColorsManager,
    L: IdentSequenceWriter + Default,
    BK: StructuredSequenceBackend<CX, L>,
>(
    circ_in_file: PathBuf,
    flat_in_file: PathBuf,
    _temp_dir: &Path,
    out_file: &StructuredSequenceWriter<CX, L, BK>,
    k: usize,
    indirect_file: &ConcurrentFileWriter,
) {
    PHASES_TIMES_MONITOR
        .write()
        .start_phase("phase: eulertigs building part 1".to_string());

    let circular_unitigs_reader_index = ChunkedBinaryReaderIndex::from_file(
        &circ_in_file,
        RemoveFileMode::Remove { remove_fs: true },
    );

    let joined = Mutex::new(CircularUnionFind::new(DEFAULT_OUTPUT_BUFFER_SIZE));

    let unitigs_bases = Mutex::new(vec![]);
    let unitig_mapping = DashMap::<
        usize,
        (
            CompressedReadIndipendent,
            PartialUnitigsColorStructure<CX>,
            SequenceAbundanceType,
        ),
    >::new();

    let circular_unitigs_kmers = Mutex::new(vec![]);
    let unitigs_color_buffer = Mutex::new(<PartialUnitigsColorStructure<CX>>::new_temp_buffer());

    let circular_unitigs_reader_parallel_chunks =
        circular_unitigs_reader_index.into_parallel_chunks();

    rayon::scope(|_s| {
        (0..rayon::current_num_threads())
            .into_par_iter()
            .for_each(|_thread_index| {
                let mut extract_workdata = ReadExtractWorkData::<CX>::new();

                let mut kmers = vec![];
                TypedStreamReader::get_items_parallel::<
                    CompressedReadsBucketDataSerializer<
                        _,
                        NoSecondBucket,
                        NoMultiplicity,
                        NoMinimizerPosition,
                        typenum::consts::U1,
                    >,
                >(
                    k,
                    &circular_unitigs_reader_parallel_chunks,
                    |DeserializedRead {
                         read,
                         extra:
                             SequenceDataWithLinks::<_, ()> {
                                 index: _,
                                 extra_data,
                                 link: _,
                             },
                         ..
                     },
                     input_extra_buffer| {
                        let unitig_index = joined.lock().add_unitig(read.get_length() - (k - 1));
                        let mut buffer = unitigs_bases.lock();

                        let (read, mut indirect_color, indirect_color_extra_buffer) =
                            indirect_read_extract_all(
                                &mut extract_workdata,
                                k,
                                read,
                                &extra_data,
                                &input_extra_buffer.0,
                                indirect_file,
                            );

                        // Save the current circular unitig for later usage
                        let copied_read =
                            CompressedReadIndipendent::from_read::<true>(&read, &mut buffer);

                        if CX::COLORS_ENABLED {
                            // Copy the color data to the temporary buffer
                            indirect_color = PartialUnitigsColorStructure::<CX>::copy_extra_from(
                                indirect_color,
                                indirect_color_extra_buffer,
                                &mut unitigs_color_buffer.lock(),
                            );
                        }

                        unitig_mapping.insert(
                            unitig_index,
                            (
                                copied_read,
                                indirect_color,
                                match () {
                                    #[cfg(feature = "support_kmer_counters")]
                                    () => extra_data.counters,
                                    #[cfg(not(feature = "support_kmer_counters"))]
                                    () => (),
                                },
                            ),
                        );

                        for (offset, hash) in
                            MH::new(read.sub_slice(0..(copied_read.bases_count() - 1)), k - 1)
                                .iter()
                                .enumerate()
                        {
                            kmers.push((
                                hash.to_unextendable(),
                                unitig_index,
                                KmerOffset::new(offset, !hash.is_forward()),
                            ));
                        }

                        SequenceDataWithLinks::<
                            PartialUnitigsColorStructure<CX>,
                            (),
                        >::clear_temp_buffer(input_extra_buffer);
                    },
                );

                kmers.sort_unstable_by_key(|x| x.0);
                circular_unitigs_kmers.lock().push(kmers);
            })
    });

    let unitigs_color_buffer = unitigs_color_buffer.into_inner();
    let mut circular_unitigs_kmers = circular_unitigs_kmers.into_inner();
    circular_unitigs_kmers.sort_unstable_by_key(|x| Reverse(x.len()));
    while circular_unitigs_kmers.len() > 1 {
        let kmers = circular_unitigs_kmers.pop().unwrap();
        circular_unitigs_kmers[0].extend(kmers.into_iter());
    }

    PHASES_TIMES_MONITOR
        .write()
        .start_phase("phase: eulertigs building part 2".to_string());

    let mut circular_unitigs_kmers = circular_unitigs_kmers.pop().unwrap();
    circular_unitigs_kmers.par_sort_by_key(|x| x.0);

    let circular_unitigs_kmers_map = DashMap::new();
    circular_unitigs_kmers
        .par_chunk_by(|a, b| a.0 == b.0)
        .for_each(|equal_kmers| {
            circular_unitigs_kmers_map
                .insert(equal_kmers[0].0, (equal_kmers[0].1, equal_kmers[0].2));

            if equal_kmers.len() == 1 {
                return;
            }

            // If the entry is different than the current unitig, find the parent of the entry
            // and join the current unitig with the parent rotating it by position
            for kmer in &equal_kmers[1..] {
                let a_pos = equal_kmers[0].2.get_offset();
                let b_rot = kmer.2.get_offset();

                let _joined_idx = joined.lock().union(
                    equal_kmers[0].1,
                    kmer.1,
                    a_pos,
                    b_rot,
                    equal_kmers[0].2.is_rc() ^ kmer.2.is_rc(),
                );
            }
        });

    ggcat_logging::info!(
        "Total circular unitigs kmers: {}",
        circular_unitigs_kmers.len()
    );
    ggcat_logging::info!("Total circular unitigs: {}", joined.lock().mappings.len());

    let mut joined = joined.into_inner();
    joined.flatten_parents();
    let unitigs_bases = unitigs_bases.into_inner();

    let flat_unitigs_reader_index = ChunkedBinaryReaderIndex::from_file(
        &flat_in_file,
        RemoveFileMode::Remove { remove_fs: true },
    );

    PHASES_TIMES_MONITOR
        .write()
        .start_phase("phase: eulertigs building part 3".to_string());

    let default_links_buffer = L::new_temp_buffer();

    let circular_unitigs_kmers_map = circular_unitigs_kmers_map.into_read_only();
    let flat_unitigs_reader_parallel_chunks = flat_unitigs_reader_index.into_parallel_chunks();

    // struct DebugKmerSet<MH: HashFunctionFactory> {
    //     set: HashSet<MH::HashTypeUnextendable>,
    //     sequences: Vec<String>,
    // }

    // impl<MH: HashFunctionFactory> DebugKmerSet<MH> {
    //     fn new() -> Self {
    //         Self {
    //             set: Default::default(),
    //             sequences: vec![],
    //         }
    //     }

    //     fn add_kmers<'a, S: ToReadData<'a> + Copy>(&mut self, k: usize, values: S) {
    //         self.set
    //             .extend(MH::new(values, k).iter().map(|h| h.to_unextendable()));
    //         self.sequences.push(values.debug_to_string());
    //     }

    //     fn compare_with(&self, other_set: DebugKmerSet<MH>, amount: usize, message: String) {
    //         let mut own: Vec<_> = self.set.iter().map(|h| MH::get_u64(*h)).collect();
    //         let mut other: Vec<_> = other_set.set.iter().map(|h| MH::get_u64(*h)).collect();

    //         own.sort();
    //         other.sort();

    //         if own != other {
    //             println!(
    //                 "Kmersets differ: {} / {} rot amount: {}",
    //                 own.len(),
    //                 other.len(),
    //                 amount
    //             );
    //             println!("Original: ");
    //             for seq in self.sequences.iter() {
    //                 println!("SEQ: {}", seq);
    //             }
    //             println!("Other: ");
    //             for seq in other_set.sequences.iter() {
    //                 println!("SEQ: {}", seq);
    //             }
    //             println!("MESSAGE: {}", message);
    //         }
    //     }
    // }

    rayon::scope(|_s| {
        (0..rayon::current_num_threads())
            .into_par_iter()
            .for_each(|_thread_index| {
                let mut final_unitig_buffer =
                    IndirectSequencesJoiner::<CX>::new(k, indirect_file.clone());
                let mut temp_kmers_buffer = AlignedDynamicCompressedRead::new();

                let mut writer =
                    FastaWriterConcurrentBuffer::new(out_file, DEFAULT_OUTPUT_BUFFER_SIZE, true, k);

                let mut extract_workdata = ReadExtractWorkData::<CX>::new();

                TypedStreamReader::get_items_parallel::<
                    CompressedReadsBucketDataSerializer<
                        _,
                        NoSecondBucket,
                        NoMultiplicity,
                        NoMinimizerPosition,
                        typenum::consts::U1,
                    >,
                >(
                    k,
                    &flat_unitigs_reader_parallel_chunks,
                    |DeserializedRead {
                         read,
                         extra:
                             SequenceDataWithLinks::<PartialUnitigsColorStructure<CX>, ()> {
                                 extra_data,
                                 ..
                             },
                         ..
                     },
                     extra_buffer| {
                        final_unitig_buffer.reset();

                        indirect_read_extract_parts::<CX, true, true>(
                            &mut extract_workdata.file_buffer,
                            &mut extract_workdata.file_color_buffer,
                            k,
                            read,
                            &extra_data,
                            &extra_buffer.0,
                            indirect_file,
                            |part, colors, _, is_rc, _| {
                                temp_kmers_buffer.clear();
                                let seq_ref = final_unitig_buffer.get_sequence_ref();
                                let sr_bases_count = seq_ref.bases_count;

                                if sr_bases_count >= k - 2 {
                                    temp_kmers_buffer.append_read(
                                        seq_ref.sub_slice(sr_bases_count - k + 2..sr_bases_count),
                                        false,
                                    );
                                }

                                temp_kmers_buffer.append_read(part, is_rc);
                                let mut last_offset = 0;

                                // if is_rc {
                                //     buffer.extend(part.as_reverse_complement_bases_iter());
                                // } else {
                                //     buffer.extend(part.as_bases_iter())
                                // }
                                // if buffer.len() > DEFAULT_OUTPUT_BUFFER_SIZE {
                                //     flush_callback(buffer)
                                // }

                                for (offset, hash) in
                                    MH::new(temp_kmers_buffer.get_reference(), k - 1)
                                        .iter()
                                        .enumerate()
                                {
                                    if let Some(kmer) =
                                        circular_unitigs_kmers_map.get(&hash.to_unextendable())
                                    {
                                        let should_rc_target_kmer =
                                            kmer.1.is_rc() ^ !hash.is_forward();
                                        let (index, rotation) = *kmer;

                                        let parent = joined.find_flatten(index);

                                        if joined.mappings[parent]
                                            .used
                                            .swap(true, Ordering::Relaxed)
                                        {
                                            continue;
                                        }

                                        // Found match
                                        let end_base_offset = offset;

                                        // FIXME!
                                        final_unitig_buffer.append_sequence(
                                            part.sub_slice(0..end_base_offset),
                                            &PartialUnitigExtraData {
                                                colors,
                                                mode: PartialUnitigMode::Inline,
                                            },
                                            last_offset,
                                            is_rc,
                                            &extra_buffer.0,
                                            Some(end_base_offset - last_offset),
                                        );

                                        // Dump the current circular unitig
                                        let mut circular_unitig = joined.mappings[parent].clone();

                                        circular_unitig.rotate_with_rc(
                                            index,
                                            rotation.get_offset(),
                                            should_rc_target_kmer,
                                        );

                                        let mut circular_abundance =
                                            SequenceAbundanceType::default();
                                        circular_unitig.write_packed::<MH, CX>(
                                            &unitigs_bases,
                                            &unitig_mapping,
                                            &mut final_unitig_buffer.sequence,
                                            &unitigs_color_buffer,
                                            &mut final_unitig_buffer.extra_buffer.0,
                                            k,
                                            false,
                                            &mut circular_abundance,
                                        );

                                        #[cfg(feature = "support_kmer_counters")]
                                        {
                                            _abundance.sum += circular_abundance.sum;
                                        }

                                        last_offset = end_base_offset;
                                    }
                                }

                                // Write the last bases of the unitig part
                                final_unitig_buffer.append_sequence(
                                    part.sub_slice(0..part.bases_count() - k + 1),
                                    &PartialUnitigExtraData {
                                        colors,
                                        mode: PartialUnitigMode::Inline,
                                    },
                                    last_offset,
                                    is_rc,
                                    &extra_buffer.0,
                                    Some(part.bases_count() - k + 1 - last_offset),
                                );
                            },
                        );

                        let final_sequence = final_unitig_buffer.get_sequence();

                        writer.add_read(
                            final_sequence.sequence,
                            None,
                            final_sequence.extra,
                            final_sequence.extra_buffer,
                            L::default(),
                            &default_links_buffer,
                            Some(indirect_file),
                        );
                    },
                );
                writer.finalize(Some(indirect_file));
            });
    });

    let mut writer =
        FastaWriterConcurrentBuffer::new(out_file, DEFAULT_OUTPUT_BUFFER_SIZE, true, k);

    // Write all not used circular kmers
    let mut packed_seq_buffer = AlignedDynamicCompressedRead::new();
    let mut final_color_extra_buffer = (
        color_types::PartialUnitigsColorStructure::<CX>::new_temp_buffer(),
        vec![],
    );

    for index in 0..joined.mappings.len() {
        let parent = joined.find_flatten(index);
        let unitig = &mut joined.mappings[parent];

        if unitig.used.swap(true, Ordering::Relaxed) {
            continue;
        }

        PartialUnitigExtraData::<PartialUnitigsColorStructure<CX>>::clear_temp_buffer(
            &mut final_color_extra_buffer,
        );

        packed_seq_buffer.clear();

        let mut abundance = SequenceAbundanceType::default();

        let full_unitig = unitig.write_packed::<MH, CX>(
            &unitigs_bases,
            &unitig_mapping,
            &mut packed_seq_buffer,
            &unitigs_color_buffer,
            &mut final_color_extra_buffer.0,
            k,
            true,
            &mut abundance,
        );

        let writable_color = CX::ColorsMergeManagerType::get_color_from_fully_joined_buffer(
            &final_color_extra_buffer.0,
        );

        writer.add_read(
            full_unitig,
            None,
            PartialUnitigExtraData {
                colors: writable_color,
                mode: PartialUnitigMode::Inline,
                #[cfg(feature = "support_kmer_counters")]
                counters: abundance,
            },
            &final_color_extra_buffer,
            L::default(),
            &default_links_buffer,
            Some(indirect_file),
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use colors::non_colored::NonColoredManager;
    use hashbrown::HashMap;
    use hashes::cn_seqhash::u128::CanonicalSeqHashFactory;
    use io::compressed_read::CompressedRead;

    #[test]
    fn test_rc_rotation() {
        let k = 31;
        let mut stream = Vec::with_capacity(10000);
        let circular_unitig1 = b"CCTGCATCAGCTAGTATGCATCAGCTACGCCATCGATCGCTAGCATCGCGCCGCATCCTCCGCGCCCGGGTACACCTGCATCAGCTAGTATGCATCAGCTACGC";
        CompressedRead::compress_from_plain(circular_unitig1, |b| stream.extend_from_slice(b));
        println!("STREAM: {:?}", stream);

        let circular_unitig2 = b"ACCCATATTCTGACGGGCTATCGCCCTACGATAAATACCCGGGCGCGGAGGATGCGGCGCGATGCTTCATCTGACGCTCACACCCATATTCTGACGGGCTATCGCCCTACG";
        let stream_start = stream.len();
        CompressedRead::compress_from_plain(circular_unitig2, |b| stream.extend_from_slice(b));
        let circular_unitig1 =
            CompressedRead::new_from_compressed(&stream[..stream_start], circular_unitig1.len());
        let circular_unitig2 =
            CompressedRead::new_from_compressed(&stream[stream_start..], circular_unitig2.len());

        let mut hashmap = HashMap::new();
        let unitigs_hashmap = DashMap::new();

        unitigs_hashmap.insert(
            0,
            (
                CompressedReadIndipendent::from_read_inplace(&circular_unitig1, &stream),
                NonColoredManager,
                SequenceAbundanceType::default(),
            ),
        );
        unitigs_hashmap.insert(
            1,
            (
                CompressedReadIndipendent::from_read_inplace(&circular_unitig2, &stream),
                NonColoredManager,
                SequenceAbundanceType::default(),
            ),
        );

        let mut unitigs = CircularUnionFind::new(1000);

        let u1_index = unitigs.add_unitig(circular_unitig1.bases_count() - (k - 1));
        let u2_index = unitigs.add_unitig(circular_unitig2.bases_count() - (k - 1));

        println!("First sequence: {}", circular_unitig1.to_string());
        for (offset, hash) in CanonicalSeqHashFactory::new(circular_unitig1, k - 1)
            .iter()
            .enumerate()
        {
            hashmap.insert(hash.to_unextendable(), offset);
        }

        println!("Second sequence: {}", circular_unitig2.to_string());
        for (offset, hash) in CanonicalSeqHashFactory::new(circular_unitig2, k - 1)
            .iter()
            .enumerate()
        {
            if let Some(offset1) = hashmap.get(&hash.to_unextendable()) {
                let offset2 = offset;
                println!("Found match: {} - {}", offset1, offset2);

                let result = unitigs.union(u1_index, u2_index, *offset1, offset2, true);

                println!(
                    "Joined unitig {} => {}",
                    result,
                    unitigs.mappings[result]
                        .debug_to_string::<CanonicalSeqHashFactory, NonColoredManager>(
                            k,
                            &stream,
                            &unitigs_hashmap
                        )
                );

                break;
            }
        }
    }
}
