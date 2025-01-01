use colors::colors_manager::{color_types, ColorsManager};
use colors::colors_manager::{color_types::PartialUnitigsColorStructure, ColorsMergeManager};
use config::{DEFAULT_OUTPUT_BUFFER_SIZE, DEFAULT_PREFETCH_AMOUNT};
use dashmap::DashMap;
use hashes::{ExtendableHashTraitType, HashFunction, HashableSequence};
use hashes::{HashFunctionFactory, MinimizerHashFunctionFactory};
use io::compressed_read::CompressedReadIndipendent;
use io::concurrent::structured_sequences::concurrent::FastaWriterConcurrentBuffer;
use io::concurrent::temp_reads::extra_data::SequenceExtraDataTempBufferManagement;
use io::concurrent::{
    structured_sequences::{
        IdentSequenceWriter, SequenceAbundanceType, StructuredSequenceBackend,
        StructuredSequenceWriter,
    },
    temp_reads::creads_utils::CompressedReadsBucketDataSerializer,
};
use parallel_processor::phase_times_monitor::PHASES_TIMES_MONITOR;
use parallel_processor::{
    buckets::readers::compressed_binary_reader::CompressedBinaryReader, memory_fs::RemoveFileMode,
};
use parking_lot::Mutex;
use rayon::iter::{IntoParallelIterator, ParallelIterator};
use rayon::slice::{ParallelSlice, ParallelSliceMut};
use std::cmp::Reverse;
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
    rc: bool,
    used: AtomicBool,
}

impl Clone for CircularUnitig {
    fn clone(&self) -> Self {
        Self {
            base_children: self.base_children.clone(),
            rc: self.rc,
            used: AtomicBool::new(false),
        }
    }
}

impl CircularUnitig {
    pub fn new() -> Self {
        Self {
            base_children: vec![],
            rc: false,
            used: AtomicBool::new(false),
        }
    }

    pub fn rotate(&mut self, child: usize, position: usize, rc: bool) {
        self.rc ^= rc;

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

        let first_part = CircularUnitigPart {
            orig_index: target_child.orig_index,
            start_pos: target_child.start_pos,
            length: position - target_child.start_pos,
            rc: target_child.rc,
        };

        let second_part = CircularUnitigPart {
            orig_index: target_child.orig_index,
            start_pos: position,
            length: target_child.length - (position - target_child.start_pos),
            rc: target_child.rc,
        };

        self.base_children[0] = second_part;
        self.base_children.push(first_part);
    }

    pub fn write_unpacked<
        H: MinimizerHashFunctionFactory,
        MH: HashFunctionFactory,
        CX: ColorsManager,
    >(
        &mut self,
        unitigs_kmers: &Vec<u8>,
        unitigs: &DashMap<usize, CompressedReadIndipendent>,
        writer: &mut Vec<u8>,
        colors_buffer: &mut <CX::ColorsMergeManagerType<H, MH> as ColorsMergeManager<H, MH>>::TempUnitigColorStructure,
        k: usize,
        write_full: bool,
    ) {
        if self.rc {
            self.base_children.reverse();
        }

        let children_count = self.base_children.len();

        for child in self.base_children.iter_mut().take(children_count - 1) {
            let unitig = unitigs.get(&child.orig_index).unwrap();
            let unitig = unitig.as_reference(unitigs_kmers);

            child.rc ^= self.rc;
            let should_rc = child.rc;
            let rc_offset = if should_rc { k - 1 } else { 0 };
            // Skip the first k-1 bases as if they're already written when merging
            let unitig_part = unitig.sub_slice(
                (child.start_pos + rc_offset)..(child.start_pos + rc_offset + child.length),
            );
            unitig_part.write_unpacked_to_vec(writer, should_rc);
            // C::join_structures::<false>(colors_buffer, src, src_buffer, 1, Some(5));
        }

        // Add the last part, adding k-1 prefix or suffix depending on the rc status
        let last = self.base_children.last_mut().unwrap();
        last.rc ^= self.rc;

        let end_offset = if !last.rc && !write_full { 0 } else { k - 1 };

        let last_part = unitigs
            .get(&last.orig_index)
            .unwrap()
            .as_reference(unitigs_kmers)
            .sub_slice(last.start_pos..last.start_pos + last.length + end_offset);

        last_part.write_unpacked_to_vec(writer, last.rc);

        self.rc = false;
    }

    pub fn debug_to_string<
        H: MinimizerHashFunctionFactory,
        MH: HashFunctionFactory,
        CX: ColorsManager,
    >(
        &mut self,
        k: usize,
        unitigs_kmers: &Vec<u8>,
        unitigs: &DashMap<usize, CompressedReadIndipendent>,
    ) -> String {
        let mut colors_buffer = CX::ColorsMergeManagerType::<H, MH>::alloc_unitig_color_structure();
        let mut writer = vec![];
        self.write_unpacked::<H, MH, CX>(
            unitigs_kmers,
            unitigs,
            &mut writer,
            &mut colors_buffer,
            k,
            true,
        );
        String::from_utf8(writer).unwrap()
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
    pub fn union(&mut self, a: usize, b: usize, a_pos: usize, b_rot: usize, b_rc: bool) -> usize {
        let a_parent = self.find(a);
        let b_parent = self.find(b);

        if a_parent == b_parent {
            return a_parent;
        }

        // Align the rotations such that the circular unitigs can be concatenated

        let a_rc = self.mappings[a_parent].rc;

        // Adjust rotation in case of reverse complement

        self.mappings[a_parent].rotate(a, a_pos, false);
        self.mappings[b_parent].rotate(b, b_rot, a_rc ^ b_rc);

        let mut b_children = std::mem::take(&mut self.mappings[b_parent].base_children);

        if self.mappings[b_parent].rc != a_rc {
            b_children.reverse();
            for child in &mut b_children {
                child.rc = !child.rc;
            }
        }

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
    H: MinimizerHashFunctionFactory,
    MH: HashFunctionFactory,
    CX: ColorsManager,
    L: IdentSequenceWriter + Default,
    BK: StructuredSequenceBackend<PartialUnitigsColorStructure<H, MH, CX>, L>,
>(
    circ_in_file: PathBuf,
    flat_in_file: PathBuf,
    _temp_dir: &Path,
    out_file: &StructuredSequenceWriter<PartialUnitigsColorStructure<H, MH, CX>, L, BK>,
    k: usize,
) {
    PHASES_TIMES_MONITOR
        .write()
        .start_phase("phase: eulertigs building part 1".to_string());

    let circular_unitigs_reader = CompressedBinaryReader::new(
        &circ_in_file,
        RemoveFileMode::Remove { remove_fs: true },
        DEFAULT_PREFETCH_AMOUNT,
    );

    let joined = Mutex::new(CircularUnionFind::new(DEFAULT_OUTPUT_BUFFER_SIZE));

    let unitigs_bases = Mutex::new(vec![]);
    let unitig_mapping = DashMap::<usize, CompressedReadIndipendent>::new();

    let circular_unitigs_kmers = Mutex::new(vec![]);

    rayon::scope(|_s| {
        (0..rayon::current_num_threads())
            .into_par_iter()
            .for_each(|_thread_index| {
                let mut kmers = vec![];
                while circular_unitigs_reader
                .decode_bucket_items_parallel::<CompressedReadsBucketDataSerializer<
                    _,
                    typenum::consts::U0,
                    false,
                >, _>(
                Vec::new(),
                <(u64, PartialUnitigsColorStructure<H, MH, CX>, (), SequenceAbundanceType)>::new_temp_buffer(
                ),
                |(_, _, (_index, _, _, _), read): (
                    _,
                    _,
                    (_, PartialUnitigsColorStructure<H, MH, CX>, (), SequenceAbundanceType),
                    _,
                ),
                _extra_buffer| {
                    let unitig_index = joined.lock().add_unitig(read.get_length() - (k - 1));
                    let mut buffer = unitigs_bases.lock();

                    // Save the current circular unitig for later usage
                    let copied_read = CompressedReadIndipendent::from_read(&read, &mut buffer);
                    unitig_mapping.insert(unitig_index, copied_read);

                    for (offset, hash) in MH::new(read.sub_slice(0..(copied_read.bases_count() - 1)), k - 1).iter().enumerate() {
                        kmers.push((hash.to_unextendable(), unitig_index, KmerOffset::new(offset,!hash.is_forward())));
                    }
                },
            ) {
                continue;
            }
            kmers.sort_unstable_by_key(|x| x.0);
            circular_unitigs_kmers.lock().push(kmers);
        })
    });

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

    let flat_unitigs_reader = CompressedBinaryReader::new(
        &flat_in_file,
        RemoveFileMode::Remove { remove_fs: true },
        DEFAULT_PREFETCH_AMOUNT,
    );

    PHASES_TIMES_MONITOR
        .write()
        .start_phase("phase: eulertigs building part 3".to_string());

    let default_links_buffer = L::new_temp_buffer();

    let circular_unitigs_kmers_map = circular_unitigs_kmers_map.into_read_only();

    rayon::scope(|_s| {
        (0..rayon::current_num_threads())
                .into_par_iter()
                .for_each(|_thread_index| {
            let mut output_unitigs_buffer = vec![];
            let mut final_unitig_color =
                CX::ColorsMergeManagerType::<H, MH>::alloc_unitig_color_structure();
            let mut final_color_extra_buffer = color_types::PartialUnitigsColorStructure::<H, MH, CX>::new_temp_buffer();
            let mut writer = FastaWriterConcurrentBuffer::new(out_file, DEFAULT_OUTPUT_BUFFER_SIZE, true);

            while flat_unitigs_reader
                .decode_bucket_items_parallel::<CompressedReadsBucketDataSerializer<
                    _,
                    typenum::consts::U0,
                    false,
                >, _>(
                Vec::new(),
                <(u64, PartialUnitigsColorStructure<H, MH, CX>, (), SequenceAbundanceType)>::new_temp_buffer(
                ),
                |(_, _, (_index, color, _, _), read): (
                    _,
                    _,
                    (_, PartialUnitigsColorStructure<H, MH, CX>, (), SequenceAbundanceType),
                    _,
                ),
                color_extra_buffer| {
                    output_unitigs_buffer.clear();

                    CX::ColorsMergeManagerType::<H, MH>::reset_unitig_color_structure(
                        &mut final_unitig_color,
                    );

                    let mut last_offset = 0;

                    for (offset, hash) in MH::new(read, k - 1).iter().enumerate() {

                        if let Some(kmer) = circular_unitigs_kmers_map.get(&hash.to_unextendable()) {

                            let should_rc = kmer.1.is_rc() ^ !hash.is_forward();
                            let (index, rotation) = *kmer;

                            let parent = joined.find_flatten(index);

                            if joined.mappings[parent].used.swap(true, Ordering::Relaxed) {
                                continue;
                            }

                            // Found match
                            let end_base_offset = offset;
                            read.sub_slice(last_offset..end_base_offset).write_unpacked_to_vec(&mut output_unitigs_buffer, false);
                            CX::ColorsMergeManagerType::<H, MH>::join_structures::<true>(
                                &mut final_unitig_color,
                                &color,
                                &color_extra_buffer.0,
                                last_offset,
                                Some(end_base_offset - last_offset),
                            );

                            // Dump the current circular unitig
                            let mut circular_unitig = joined.mappings[parent].clone();

                            circular_unitig.rotate(index, rotation.get_offset(), should_rc);

                            circular_unitig.write_unpacked::<H, MH, CX>(&unitigs_bases, &unitig_mapping, &mut output_unitigs_buffer, &mut final_unitig_color, k, false);
                            last_offset = end_base_offset;
                        }
                    }

                    // Write the last part of the unitig
                    read.sub_slice(last_offset..read.bases_count()).write_unpacked_to_vec(&mut output_unitigs_buffer, false);
                    CX::ColorsMergeManagerType::<H, MH>::join_structures::<true>(
                        &mut final_unitig_color,
                        &color,
                        &color_extra_buffer.0,
                        last_offset,
                        Some(read.bases_count() - last_offset),
                    );

                    let writable_color =
                        CX::ColorsMergeManagerType::<H, MH>::encode_part_unitigs_colors(
                            &mut final_unitig_color,
                            &mut final_color_extra_buffer,
                        );

                    writer.add_read(
                        &output_unitigs_buffer,
                        None,
                        writable_color,
                        &final_color_extra_buffer,
                        L::default(),
                        &default_links_buffer,
                    );
                },
            ) {
                continue;
            }
        });
    });

    let mut writer = FastaWriterConcurrentBuffer::new(out_file, DEFAULT_OUTPUT_BUFFER_SIZE, true);

    // Write all not used circular kmers
    let mut seq_buffer = vec![];
    let mut final_unitig_color =
        CX::ColorsMergeManagerType::<H, MH>::alloc_unitig_color_structure();
    let mut final_color_extra_buffer =
        color_types::PartialUnitigsColorStructure::<H, MH, CX>::new_temp_buffer();

    for index in 0..joined.mappings.len() {
        let parent = joined.find_flatten(index);
        let unitig = &mut joined.mappings[parent];

        if unitig.used.swap(true, Ordering::Relaxed) {
            continue;
        }

        CX::ColorsMergeManagerType::<H, MH>::reset_unitig_color_structure(&mut final_unitig_color);

        seq_buffer.clear();

        unitig.write_unpacked::<H, MH, CX>(
            &unitigs_bases,
            &unitig_mapping,
            &mut seq_buffer,
            &mut final_unitig_color,
            k,
            true,
        );

        let writable_color = CX::ColorsMergeManagerType::<H, MH>::encode_part_unitigs_colors(
            &mut final_unitig_color,
            &mut final_color_extra_buffer,
        );

        writer.add_read(
            &seq_buffer,
            None,
            writable_color,
            &final_color_extra_buffer,
            L::default(),
            &default_links_buffer,
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use colors::non_colored::NonColoredManager;
    use hashbrown::HashMap;
    use hashes::{
        cn_nthash::CanonicalNtHashIteratorFactory, cn_seqhash::u128::CanonicalSeqHashFactory,
    };
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
            CompressedReadIndipendent::from_read_inplace(&circular_unitig1, &stream),
        );
        unitigs_hashmap.insert(
            1,
            CompressedReadIndipendent::from_read_inplace(&circular_unitig2, &stream),
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
                    unitigs.mappings[result].debug_to_string::<CanonicalNtHashIteratorFactory, CanonicalSeqHashFactory, NonColoredManager>(
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
