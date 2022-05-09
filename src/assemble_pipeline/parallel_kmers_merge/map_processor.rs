use crate::assemble_pipeline::parallel_kmers_merge::structs::MapEntry;
use crate::assemble_pipeline::parallel_kmers_merge::{
    ParallelKmersMergeFactory, READ_FLAG_INCL_BEGIN, READ_FLAG_INCL_END,
};
use crate::colors::colors_manager::ColorsMergeManager;
use crate::colors::colors_manager::{color_types, ColorsManager};
use crate::hashes::ExtendableHashTraitType;
use crate::hashes::HashFunction;
use crate::hashes::HashableSequence;
use crate::hashes::{HashFunctionFactory, MinimizerHashFunctionFactory};
use crate::pipeline_common::kmers_transform::processor::KmersTransformProcessor;
use crate::pipeline_common::kmers_transform::{
    KmersTransformExecutorFactory, KmersTransformMapProcessor,
};
use crate::utils::compressed_read::CompressedReadIndipendent;
use hashbrown::HashMap;
use parallel_processor::counter_stats::counter::{AtomicCounter, AvgMode, MaxMode};
use parallel_processor::counter_stats::{declare_avg_counter_i64, declare_counter_i64};
use parallel_processor::execution_manager::memory_tracker::MemoryTracker;
use parallel_processor::execution_manager::objects_pool::PoolObjectTrait;
use parallel_processor::execution_manager::packet::{Packet, PacketTrait};
use std::mem::size_of;
use std::ops::DerefMut;

pub struct ParallelKmersMergeMapPacket<MH: HashFunctionFactory, CX: ColorsManager> {
    pub rhash_map:
        HashMap<MH::HashTypeUnextendable, MapEntry<color_types::HashMapTempColorIndex<MH, CX>>>,
    pub saved_reads: Vec<u8>,
    pub rcorrect_reads: Vec<usize>,
    pub temp_colors: color_types::ColorsBufferTempStructure<MH, CX>,
}

#[inline]
fn clear_hashmap<K, V>(hashmap: &mut HashMap<K, V>) {
    if hashmap.capacity() < 8192 {
        hashmap.clear();
    } else {
        // Reset the hashmap if it gets too big
        *hashmap = HashMap::with_capacity(4096);
    }
}

impl<MH: HashFunctionFactory, CX: ColorsManager> PoolObjectTrait
    for ParallelKmersMergeMapPacket<MH, CX>
{
    type InitData = ();

    fn allocate_new(_init_data: &Self::InitData) -> Self {
        Self {
            rhash_map: HashMap::with_capacity(4096),
            saved_reads: vec![],
            rcorrect_reads: Vec::new(),
            temp_colors: <CX::ColorsMergeManagerType<MH> as ColorsMergeManager<MH, CX>>::allocate_temp_buffer_structure()
        }
    }

    fn reset(&mut self) {
        clear_hashmap(&mut self.rhash_map);
        self.saved_reads.clear();
        CX::ColorsMergeManagerType::<MH>::reinit_temp_buffer_structure(&mut self.temp_colors);
        self.rcorrect_reads.clear();
    }
}

impl<MH: HashFunctionFactory, CX: ColorsManager> PacketTrait
    for ParallelKmersMergeMapPacket<MH, CX>
{
    fn get_size(&self) -> usize {
        self.rhash_map.len()
            * (size_of::<(
                MH::HashTypeUnextendable,
                MapEntry<color_types::HashMapTempColorIndex<MH, CX>>,
            )>() + 1)
            + self.saved_reads.len()
        // + self.rcorrect_reads.len() * size_of::<usize>()
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
            mem_tracker,
        }
    }
}

impl<H: MinimizerHashFunctionFactory, MH: HashFunctionFactory, CX: ColorsManager>
    KmersTransformMapProcessor<ParallelKmersMergeFactory<H, MH, CX>>
    for ParallelKmersMergeMapProcessor<H, MH, CX>
{
    type MapStruct = ParallelKmersMergeMapPacket<MH, CX>;

    fn process_group_start(
        &mut self,
        map_struct: Packet<Self::MapStruct>,
        _global_data: &<ParallelKmersMergeFactory<H, MH, CX> as KmersTransformExecutorFactory>::GlobalExtraData,
    ) {
        self.map_packet = Some(map_struct);
    }

    fn process_group_batch_sequences(
        &mut self,
        global_data: &<ParallelKmersMergeFactory<H, MH, CX> as KmersTransformExecutorFactory>::GlobalExtraData,
        batch: &Vec<(
            u8,
            CX::MinimizerBucketingSeqColorDataType,
            CompressedReadIndipendent,
        )>,
        ref_sequences: &Vec<u8>,
    ) {
        let k = global_data.k;

        let map_packet = self.map_packet.as_mut().unwrap().deref_mut();

        for (flags, color, read) in batch.iter() {
            let read = read.as_reference(ref_sequences);

            let hashes = MH::new(read, k);

            let last_hash_pos = read.bases_count() - k;
            let mut saved_read_offset = None;

            for (idx, hash) in hashes.iter_enumerate() {
                let begin_ignored = flags & READ_FLAG_INCL_BEGIN == 0 && idx == 0;
                let end_ignored = flags & READ_FLAG_INCL_END == 0 && idx == last_hash_pos;

                let is_forward = hash.is_forward();

                let entry = map_packet
                    .rhash_map
                    .entry(hash.to_unextendable())
                    .or_insert(MapEntry::new(
                        CX::ColorsMergeManagerType::<MH>::new_color_index(),
                    ));

                entry.update_flags(
                    ((begin_ignored as u8) << ((!is_forward) as u8))
                        | ((end_ignored as u8) << (is_forward as u8)),
                );

                entry.incr();

                CX::ColorsMergeManagerType::<MH>::add_temp_buffer_structure_el(
                    &mut map_packet.temp_colors,
                    &color,
                    (idx, hash.to_unextendable()),
                );

                if entry.get_counter() == global_data.min_multiplicity {
                    if saved_read_offset.is_none() {
                        saved_read_offset = Some(map_packet.saved_reads.len());
                        map_packet
                            .saved_reads
                            .extend_from_slice(read.get_packed_slice())
                    }
                    map_packet
                        .rcorrect_reads
                        .push(saved_read_offset.unwrap() * 4 + idx);
                }
            }
        }
        self.mem_tracker
            .update_memory_usage(&[map_packet.get_size(), 0])
    }

    fn process_group_finalize(
        &mut self,
        _global_data: &<ParallelKmersMergeFactory<H, MH, CX> as KmersTransformExecutorFactory>::GlobalExtraData,
    ) -> Packet<Self::MapStruct> {
        static COUNTER_KMERS_MAX: AtomicCounter<MaxMode> =
            declare_counter_i64!("kmers_cardinality_max", MaxMode, false);
        static COUNTER_READS_MAX: AtomicCounter<MaxMode> =
            declare_counter_i64!("correct_reads_max", MaxMode, false);
        static COUNTER_READS_MAX_LAST: AtomicCounter<MaxMode> =
            declare_counter_i64!("correct_reads_max_last", MaxMode, true);
        static COUNTER_READS_AVG: AtomicCounter<AvgMode> =
            declare_avg_counter_i64!("correct_reads_avg", false);

        let map_packet = self.map_packet.take().unwrap();

        let len = map_packet.rcorrect_reads.len() as i64;
        COUNTER_KMERS_MAX.max(map_packet.rhash_map.len() as i64);
        COUNTER_READS_MAX.max(len);
        COUNTER_READS_MAX_LAST.max(len);
        COUNTER_READS_AVG.add_value(len);
        self.mem_tracker.update_memory_usage(&[0, 0]);

        map_packet
    }
}
