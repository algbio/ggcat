use std::path::{Path, PathBuf};

use colors::colors_manager::{color_types::PartialUnitigsColorStructure, ColorsManager};
use config::{DEFAULT_OUTPUT_BUFFER_SIZE, DEFAULT_PREFETCH_AMOUNT};
use dashmap::{DashMap, DashSet};
use hashes::{ExtendableHashTraitType, HashFunction, HashableSequence};
use hashes::{HashFunctionFactory, MinimizerHashFunctionFactory};
use io::concurrent::temp_reads::extra_data::SequenceExtraDataTempBufferManagement;
use io::concurrent::{
    structured_sequences::{
        IdentSequenceWriter, SequenceAbundanceType, StructuredSequenceBackend,
        StructuredSequenceWriter,
    },
    temp_reads::creads_utils::CompressedReadsBucketDataSerializer,
};
use parallel_processor::{
    buckets::readers::compressed_binary_reader::CompressedBinaryReader, memory_fs::RemoveFileMode,
};

struct CircularUnitigPart {
    orig_index: usize,
    start_pos: usize,
    length: usize,
}

struct CircularUnitig {
    base_children: Vec<CircularUnitigPart>,
    used: bool,
}

impl CircularUnitig {
    pub fn new() -> Self {
        Self { base_children: vec![], used: false }
    }

    pub fn rotate(&mut self, child: usize, position: usize) {
        let split_index = self.base_children
            .iter()
            .position(|x| (x.orig_index == child) && (x.start_pos <= position && (x.start_pos + x.length > position)))
            .expect("Could not find child in base_children, this is a bug");

        if split_index > 0 {
            self.base_children.rotate_left(split_index - 1);
        }
        // TODO: FIX
    }

    pub fn join(&mut self, other: CircularUnitig) {
        self.base_children.extend(other.base_children.into_iter());
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

    pub fn find(&mut self, index: usize) -> usize {

        if self.parent[index] != usize::MAX {
            return self.parent[index];
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

        // Align the rotations such that the circular unitigs can be concatenated

        // TODO: Handle rc
        self.mappings[a_parent].rotate(a, a_pos);
        self.mappings[b_parent].rotate(b, b_rot);


        let b_children = std::mem::take(&mut self.mappings[b_parent].base_children);
        self.mappings[a_parent].base_children.extend(b_children.into_iter());
        self.parent[b_parent] = a_parent;

        a_parent
    }
}


pub fn build_eulertigs<
    H: MinimizerHashFunctionFactory,
    MH: HashFunctionFactory,
    CX: ColorsManager,
    L: IdentSequenceWriter,
    BK: StructuredSequenceBackend<PartialUnitigsColorStructure<H, MH, CX>, L>,
>(
    circ_in_file: PathBuf,
    flat_in_file: PathBuf,
    temp_dir: &Path,
    out_file: &StructuredSequenceWriter<PartialUnitigsColorStructure<H, MH, CX>, L, BK>,
    k: usize,
) {
    let circular_unitigs_reader = CompressedBinaryReader::new(
        &circ_in_file,
        RemoveFileMode::Remove { remove_fs: true },
        DEFAULT_PREFETCH_AMOUNT,
    );

    let circular_unitigs_kmers = DashMap::new();
    let used_circular = DashSet::new();
    let joined = CircularUnionFind::new(DEFAULT_OUTPUT_BUFFER_SIZE);

    while circular_unitigs_reader
        .decode_bucket_items_parallel::<CompressedReadsBucketDataSerializer<
            _,
            typenum::consts::U0,
            false,
        >, _>(
        Vec::new(),
        <(u64, PartialUnitigsColorStructure<H, MH, CX>, (), SequenceAbundanceType)>::new_temp_buffer(
        ),
        |(_, _, (index, _, _, _), read): (
            _,
            _,
            (_, PartialUnitigsColorStructure<H, MH, CX>, (), SequenceAbundanceType),
            _,
        ),
        _extra_buffer| {
            println!("Processing circular unitig: {}", read.to_string());
            for (idx, hash) in MH::new(read.sub_slice(0..(read.bases_count() - 1)), k - 1).iter().enumerate() {
                match circular_unitigs_kmers.entry(hash.to_unextendable()) {
                    dashmap::Entry::Occupied(occupied_entry) => {
                        // occupied_entry.

                        // If the entry is different than the current unitig, find the parent of the entry
                        // and join the current unitig with the parent rotating it by position
                        println!("Duplicate entry kmer: {}", read.sub_slice(idx..idx + k - 1).to_string());

                        // joined
                    },
                    dashmap::Entry::Vacant(vacant_entry) => {
                        vacant_entry.insert((index, idx));
                    },
                }
            }
        },
    ) {
        continue;
    }

    let flat_unitigs_reader = CompressedBinaryReader::new(
        &flat_in_file,
        RemoveFileMode::Remove { remove_fs: true },
        DEFAULT_PREFETCH_AMOUNT,
    );

    while flat_unitigs_reader
        .decode_bucket_items_parallel::<CompressedReadsBucketDataSerializer<
            _,
            typenum::consts::U0,
            false,
        >, _>(
        Vec::new(),
        <(u64, PartialUnitigsColorStructure<H, MH, CX>, (), SequenceAbundanceType)>::new_temp_buffer(
        ),
        |(_, _, (index, _, _, _), read): (
            _,
            _,
            (_, PartialUnitigsColorStructure<H, MH, CX>, (), SequenceAbundanceType),
            _,
        ),
        _extra_buffer| {
            for (_idx, hash) in MH::new(read, k - 1).iter().enumerate() {
                if let Some(kmer) = circular_unitigs_kmers.get(&hash.to_unextendable()) {
                    let (index, _pos) = *kmer;
                    
                    // Found match
                    if let Some(_idx) = used_circular.get(&index) {
                        continue;
                    } else {
                        used_circular.insert(index);
                    }
                }
            }
        },
    ) {
        continue;
    }
}
