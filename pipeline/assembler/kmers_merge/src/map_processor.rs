use crate::ParallelKmersMergeFactory;
use colors::colors_manager::color_types::MinimizerBucketingSeqColorDataType;
use colors::colors_manager::{color_types, ColorsManager};
use colors::colors_manager::{ColorsMergeManager, MinimizerBucketingSeqColorData};
use config::{READ_FLAG_INCL_BEGIN, READ_FLAG_INCL_END};
use hashbrown::HashMap;
use hashes::ExtendableHashTraitType;
use hashes::HashFunction;
use hashes::HashableSequence;
use hashes::{HashFunctionFactory, MinimizerHashFunctionFactory};
use io::compressed_read::CompressedReadIndipendent;
use io::concurrent::temp_reads::extra_data::SequenceExtraData;
use io::varint::encode_varint;
use kmers_transform::processor::KmersTransformProcessor;
use kmers_transform::{KmersTransformExecutorFactory, KmersTransformMapProcessor};
use parallel_processor::counter_stats::counter::{AtomicCounter, AvgMode, MaxMode};
use parallel_processor::counter_stats::{declare_avg_counter_i64, declare_counter_i64};
use parallel_processor::execution_manager::memory_tracker::MemoryTracker;
use parallel_processor::execution_manager::objects_pool::PoolObjectTrait;
use parallel_processor::execution_manager::packet::{Packet, PacketTrait};
use parking_lot::RwLock;
use std::cmp::{max, min};
use std::mem::size_of;
use std::ops::{Deref, DerefMut};
use std::path::PathBuf;
use std::sync::atomic::Ordering;
use structs::map_entry::MapEntry;

instrumenter::use_instrumenter!();

pub(crate) static KMERGE_TEMP_DIR: RwLock<Option<PathBuf>> = RwLock::new(None);

pub struct ParallelKmersMergeMapPacket<
    H: MinimizerHashFunctionFactory,
    MH: HashFunctionFactory,
    CX: ColorsManager,
> {
    pub rhash_map:
        HashMap<MH::HashTypeUnextendable, MapEntry<color_types::HashMapTempColorIndex<H, MH, CX>>>,
    pub saved_reads: Vec<u8>,
    pub encoded_saved_reads_indexes: Vec<u8>,
    pub temp_colors: color_types::ColorsBufferTempStructure<H, MH, CX>,
    average_hasmap_size: u64,
    average_sequences_size: u64,
}

#[inline]
fn clear_hashmap<K, V>(hashmap: &mut HashMap<K, V>, suggested_size: usize) {
    let suggested_capacity = (suggested_size / 2).next_power_of_two();

    if hashmap.capacity() < suggested_capacity {
        hashmap.clear();
    } else {
        // Reset the hashmap if it gets too big
        *hashmap = HashMap::with_capacity(suggested_capacity);
    }
}

impl<H: MinimizerHashFunctionFactory, MH: HashFunctionFactory, CX: ColorsManager> PoolObjectTrait
    for ParallelKmersMergeMapPacket<H, MH, CX>
{
    type InitData = ();

    fn allocate_new(_init_data: &Self::InitData) -> Self {
        Self {
            rhash_map: HashMap::with_capacity(4096),
            saved_reads: vec![],
            encoded_saved_reads_indexes: vec![],
            temp_colors: CX::ColorsMergeManagerType::<H, MH>::allocate_temp_buffer_structure(
                KMERGE_TEMP_DIR.read().deref().as_ref().unwrap(),
            ),
            average_hasmap_size: 0,
            average_sequences_size: 0,
        }
    }

    fn reset(&mut self) {
        clear_hashmap(
            &mut self.rhash_map,
            max(8192, self.average_hasmap_size as usize),
        );

        let saved_reads_suggested_size = (self.average_sequences_size).next_power_of_two() as usize;

        if self.saved_reads.capacity() < saved_reads_suggested_size {
            self.saved_reads.clear();
        } else {
            self.saved_reads = Vec::with_capacity(saved_reads_suggested_size)
        }

        if self.encoded_saved_reads_indexes.capacity() < saved_reads_suggested_size {
            self.encoded_saved_reads_indexes.clear();
        } else {
            self.encoded_saved_reads_indexes = Vec::with_capacity(saved_reads_suggested_size)
        }

        CX::ColorsMergeManagerType::<H, MH>::reinit_temp_buffer_structure(&mut self.temp_colors);
    }
}

impl<H: MinimizerHashFunctionFactory, MH: HashFunctionFactory, CX: ColorsManager> PacketTrait
    for ParallelKmersMergeMapPacket<H, MH, CX>
{
    fn get_size(&self) -> usize {
        self.rhash_map.len()
            * (size_of::<(
                MH::HashTypeUnextendable,
                MapEntry<color_types::HashMapTempColorIndex<H, MH, CX>>,
            )>() + 1)
            + self.saved_reads.len()
    }
}

pub struct ParallelKmersMergeMapProcessor<
    H: MinimizerHashFunctionFactory,
    MH: HashFunctionFactory,
    CX: ColorsManager,
> {
    map_packet: Option<
        Packet<
            <Self as KmersTransformMapProcessor<ParallelKmersMergeFactory<H, MH, CX>>>::MapStruct,
        >,
    >,
    last_saved_len: usize,
    mem_tracker: MemoryTracker<KmersTransformProcessor<ParallelKmersMergeFactory<H, MH, CX>>>,
}

impl<H: MinimizerHashFunctionFactory, MH: HashFunctionFactory, CX: ColorsManager>
    ParallelKmersMergeMapProcessor<H, MH, CX>
{
    pub fn new(
        mem_tracker: MemoryTracker<KmersTransformProcessor<ParallelKmersMergeFactory<H, MH, CX>>>,
    ) -> Self {
        Self {
            map_packet: None,
            last_saved_len: 0,
            mem_tracker,
        }
    }
}

impl<H: MinimizerHashFunctionFactory, MH: HashFunctionFactory, CX: ColorsManager>
    KmersTransformMapProcessor<ParallelKmersMergeFactory<H, MH, CX>>
    for ParallelKmersMergeMapProcessor<H, MH, CX>
{
    type MapStruct = ParallelKmersMergeMapPacket<H, MH, CX>;

    fn process_group_start(
        &mut self,
        map_struct: Packet<Self::MapStruct>,
        _global_data: &<ParallelKmersMergeFactory<H, MH, CX> as KmersTransformExecutorFactory>::GlobalExtraData,
    ) {
        self.map_packet = Some(map_struct);
        self.last_saved_len = 0;
    }

    #[instrumenter::track]
    fn process_group_batch_sequences(
        &mut self,
        global_data: &<ParallelKmersMergeFactory<H, MH, CX> as KmersTransformExecutorFactory>::GlobalExtraData,
        batch: &Vec<(
            u8,
            MinimizerBucketingSeqColorDataType<CX>,
            CompressedReadIndipendent,
        )>,
        extra_data_buffer: &<MinimizerBucketingSeqColorDataType<CX> as SequenceExtraData>::TempBuffer,
        ref_sequences: &Vec<u8>,
    ) {
        let k = global_data.k;

        let map_packet = self.map_packet.as_mut().unwrap().deref_mut();

        for (flags, color, read) in batch.iter() {
            let read = read.as_reference(ref_sequences);

            let hashes = MH::new(read, k);

            let last_hash_pos = read.bases_count() - k;
            let mut min_idx = usize::MAX;
            let mut max_idx = 0;

            for ((idx, hash), kmer_color) in hashes
                .iter_enumerate()
                .zip(color.get_iterator(extra_data_buffer))
            {
                let begin_ignored = flags & READ_FLAG_INCL_BEGIN == 0 && idx == 0;
                let end_ignored = flags & READ_FLAG_INCL_END == 0 && idx == last_hash_pos;

                let is_forward = hash.is_forward();

                let entry = map_packet
                    .rhash_map
                    .entry(hash.to_unextendable())
                    .or_insert(MapEntry::new(
                        CX::ColorsMergeManagerType::<H, MH>::new_color_index(),
                    ));

                entry.update_flags(
                    ((begin_ignored as u8) << ((!is_forward) as u8))
                        | ((end_ignored as u8) << (is_forward as u8)),
                );

                entry.incr();

                CX::ColorsMergeManagerType::<H, MH>::add_temp_buffer_structure_el(
                    &mut map_packet.temp_colors,
                    &kmer_color,
                    (idx, hash.to_unextendable()),
                    entry,
                );

                if entry.get_counter() == global_data.min_multiplicity {
                    min_idx = min(min_idx, idx / 4);
                    max_idx = max(max_idx, idx);
                }
            }

            CX::ColorsMergeManagerType::<H, MH>::add_temp_buffer_sequence(
                &mut map_packet.temp_colors,
                read,
                global_data.k,
                global_data.m,
                *flags,
            );

            if min_idx != usize::MAX {
                encode_varint(
                    |b| {
                        map_packet.encoded_saved_reads_indexes.extend_from_slice(b);
                    },
                    (map_packet.saved_reads.len() - self.last_saved_len) as u64,
                );
                self.last_saved_len = map_packet.saved_reads.len();
                map_packet
                    .saved_reads
                    .extend_from_slice(&read.get_packed_slice()[min_idx..((max_idx + k + 3) / 4)]);
            }
        }
        self.mem_tracker
            .update_memory_usage(&[map_packet.get_size(), 0])
    }

    #[instrumenter::track]
    fn process_group_finalize(
        &mut self,
        global_data: &<ParallelKmersMergeFactory<H, MH, CX> as KmersTransformExecutorFactory>::GlobalExtraData,
    ) -> Packet<Self::MapStruct> {
        static COUNTER_KMERS_MAX: AtomicCounter<MaxMode> =
            declare_counter_i64!("kmers_cardinality_max", MaxMode, false);
        static COUNTER_READS_AVG: AtomicCounter<AvgMode> =
            declare_avg_counter_i64!("correct_reads_avg", false);

        let mut map_packet = self.map_packet.take().unwrap();

        let sequences_sizes = map_packet.saved_reads.len() as u64;
        let all_kmers = map_packet.rhash_map.len() as u64;

        let kmers_total = global_data
            .hasnmap_kmers_total
            .fetch_add(all_kmers, Ordering::Relaxed)
            + all_kmers;
        let sequences_size_total = global_data
            .sequences_size_total
            .fetch_add(sequences_sizes, Ordering::Relaxed)
            + sequences_sizes;
        let batches_count = global_data
            .kmer_batches_count
            .fetch_add(1, Ordering::Relaxed)
            + 1;

        map_packet.average_hasmap_size = kmers_total / batches_count;
        map_packet.average_sequences_size = sequences_size_total / batches_count;

        COUNTER_KMERS_MAX.max(all_kmers as i64);
        COUNTER_READS_AVG.add_value(all_kmers as i64);
        self.mem_tracker.update_memory_usage(&[0, 0]);

        map_packet
    }
}
