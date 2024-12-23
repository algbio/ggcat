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

    pub fn rotate(&mut self, child: usize, position: usize) {
        let split_index = self
            .base_children
            .iter()
            .position(|x| {
                (x.orig_index == child)
                    && (x.start_pos <= position && (x.start_pos + x.length > position))
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
        };

        let second_part = CircularUnitigPart {
            orig_index: target_child.orig_index,
            start_pos: position,
            length: target_child.length - (position - target_child.start_pos),
        };

        self.base_children[0] = second_part;
        self.base_children.push(first_part);
    }

    pub fn write_unpacked<
        H: MinimizerHashFunctionFactory,
        MH: HashFunctionFactory,
        CX: ColorsManager,
    >(
        &self,
        unitigs_kmers: &Vec<u8>,
        unitigs: &DashMap<usize, CompressedReadIndipendent>,
        writer: &mut Vec<u8>,
        colors_buffer: &mut <CX::ColorsMergeManagerType<H, MH> as ColorsMergeManager<H, MH>>::TempUnitigColorStructure,
        k: usize,
        write_full: bool,
    ) {
        for child in &self.base_children {
            let unitig = unitigs.get(&child.orig_index).unwrap();
            let unitig = unitig.as_reference(unitigs_kmers);
            // Skip the first k-1 bases as if they're already written when merging
            let unitig_part = unitig.sub_slice(child.start_pos..(child.start_pos + child.length));
            unitig_part.write_unpacked_to_vec(writer);
            // C::join_structures::<false>(colors_buffer, src, src_buffer, 1, Some(5));
            // TODO: Avoid duplicate kmers
        }

        // Add the last part
        let last = self.base_children.last().unwrap();

        let last_part = unitigs
            .get(&last.orig_index)
            .unwrap()
            .as_reference(unitigs_kmers)
            .sub_slice(
                last.start_pos + last.length
                    ..last.start_pos + last.length + if write_full { k - 1 } else { 0 },
            );

        last_part.write_unpacked_to_vec(writer);
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
            length,
        });
        self.mappings.push(unitig);
        self.parent.push(usize::MAX);
        self.rank.push(0);
        self.mappings.len() - 1
    }

    // Join two circular unitigs placinga rotation of b inside a at a specific position
    pub fn union(&mut self, a: usize, b: usize, a_pos: usize, b_rot: usize) -> usize {
        let a_parent = self.find(a);
        let b_parent = self.find(b);

        if a_parent == b_parent {
            return a_parent;
        }

        // Align the rotations such that the circular unitigs can be concatenated

        // TODO: Handle rc
        self.mappings[a_parent].rotate(a, a_pos);
        self.mappings[b_parent].rotate(b, b_rot);

        let b_children = std::mem::take(&mut self.mappings[b_parent].base_children);
        self.mappings[a_parent]
            .base_children
            .extend(b_children.into_iter());
        self.parent[b_parent] = a_parent;

        a_parent
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
                        kmers.push((hash.to_unextendable(), unitig_index, offset));
                    }
                },
            ) {
                continue;
            }
            kmers.sort_unstable();
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
    circular_unitigs_kmers.par_sort();

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
                let _joined_idx =
                    joined
                        .lock()
                        .union(equal_kmers[0].1, kmer.1, equal_kmers[0].2, kmer.2);
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

                            let (index, rotation) = *kmer;

                            let parent = joined.find_flatten(index);

                            if joined.mappings[parent].used.swap(true, Ordering::Relaxed) {
                                continue;
                            }

                            // Found match
                            let end_base_offset = offset;
                            read.sub_slice(last_offset..end_base_offset).write_unpacked_to_vec(&mut output_unitigs_buffer);
                            CX::ColorsMergeManagerType::<H, MH>::join_structures::<true>(
                                &mut final_unitig_color,
                                &color,
                                &color_extra_buffer.0,
                                last_offset,
                                Some(end_base_offset - last_offset),
                            );

                            // Dump the current circular unitig
                            let mut circular_unitig = joined.mappings[parent].clone();

                            circular_unitig.rotate(index, rotation);
                            circular_unitig.write_unpacked::<H, MH, CX>(&unitigs_bases, &unitig_mapping, &mut output_unitigs_buffer, &mut final_unitig_color, k, false);
                            last_offset = end_base_offset;
                        }
                    }

                    // Write the last part of the unitig
                    read.sub_slice(last_offset..read.bases_count()).write_unpacked_to_vec(&mut output_unitigs_buffer);
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
