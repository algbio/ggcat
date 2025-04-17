use crate::ParallelKmersMergeFactory;
use colors::colors_manager::color_types::MinimizerBucketingSeqColorDataType;
use colors::colors_manager::{color_types, ColorsManager};
use colors::colors_manager::{ColorsMergeManager, MinimizerBucketingSeqColorData};
use config::{READ_FLAG_INCL_BEGIN, READ_FLAG_INCL_END};
use ggcat_logging::stats;
use ggcat_logging::stats::KmersMergeBucketReport;
use hashes::ExtendableHashTraitType;
use hashes::HashFunction;
use hashes::HashFunctionFactory;
use hashes::HashableSequence;
use io::concurrent::temp_reads::extra_data::SequenceExtraDataTempBufferManagement;
use io::varint::encode_varint;
use kmers_transform::processor::KmersTransformProcessor;
use kmers_transform::reads_buffer::ReadsVector;
use kmers_transform::{
    GroupProcessStats, KmersTransformExecutorFactory, KmersTransformMapProcessor,
};
use parallel_processor::execution_manager::memory_tracker::MemoryTracker;
use parallel_processor::execution_manager::objects_pool::PoolObjectTrait;
use parallel_processor::execution_manager::packet::{Packet, PacketTrait};
use parallel_processor::mt_debug_counters::counter::{AtomicCounter, AvgMode, MaxMode};
use parallel_processor::mt_debug_counters::{declare_avg_counter_i64, declare_counter_i64};
use parking_lot::RwLock;
use rustc_hash::{FxBuildHasher, FxHashMap};
use std::cmp::{max, min};
use std::mem::size_of;
use std::ops::{Deref, DerefMut};
use std::path::PathBuf;
use std::sync::atomic::Ordering;
use structs::map_entry::MapEntry;

instrumenter::use_instrumenter!();

pub(crate) static KMERGE_TEMP_DIR: RwLock<Option<PathBuf>> = RwLock::new(None);

pub struct ParallelKmersMergeMapPacket<MH: HashFunctionFactory, CX: ColorsManager> {
    pub detailed_stats: KmersMergeBucketReport,
    pub rhash_map:
        FxHashMap<MH::HashTypeUnextendable, MapEntry<color_types::HashMapTempColorIndex<CX>>>,
    pub saved_reads: Vec<u8>,
    pub encoded_saved_reads_indexes: Vec<u8>,
    pub temp_colors: color_types::ColorsBufferTempStructure<CX>,
    average_hasmap_size: u64,
    average_sequences_size: u64,
}

#[inline]
fn clear_hashmap<K, V>(hashmap: &mut FxHashMap<K, V>, suggested_size: usize) {
    let suggested_capacity = (suggested_size / 2).next_power_of_two();

    if hashmap.capacity() < suggested_capacity {
        hashmap.clear();
    } else {
        // Reset the hashmap if it gets too big
        *hashmap = FxHashMap::with_capacity_and_hasher(suggested_capacity, FxBuildHasher);
    }
}

impl<MH: HashFunctionFactory, CX: ColorsManager> PoolObjectTrait
    for ParallelKmersMergeMapPacket<MH, CX>
{
    type InitData = ();

    fn allocate_new(_init_data: &Self::InitData) -> Self {
        Self {
            detailed_stats: Default::default(),
            rhash_map: FxHashMap::with_capacity_and_hasher(4096, FxBuildHasher),
            saved_reads: vec![],
            encoded_saved_reads_indexes: vec![],
            temp_colors: CX::ColorsMergeManagerType::allocate_temp_buffer_structure(
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

        CX::ColorsMergeManagerType::reinit_temp_buffer_structure(&mut self.temp_colors);
    }
}

impl<MH: HashFunctionFactory, CX: ColorsManager> PacketTrait
    for ParallelKmersMergeMapPacket<MH, CX>
{
    fn get_size(&self) -> usize {
        self.rhash_map.len()
            * (size_of::<(
                MH::HashTypeUnextendable,
                MapEntry<color_types::HashMapTempColorIndex<CX>>,
            )>() + 1)
            + self.saved_reads.len()
    }
}

pub struct ParallelKmersMergeMapProcessor<
    MH: HashFunctionFactory,
    CX: ColorsManager,
    const COMPUTE_SIMPLITIGS: bool,
> {
    map_packet: Option<
        Packet<
            <Self as KmersTransformMapProcessor<
                ParallelKmersMergeFactory<MH, CX, COMPUTE_SIMPLITIGS>,
            >>::MapStruct,
        >,
    >,
    last_saved_len: usize,
    mem_tracker: MemoryTracker<
        KmersTransformProcessor<ParallelKmersMergeFactory<MH, CX, COMPUTE_SIMPLITIGS>>,
    >,
}

impl<MH: HashFunctionFactory, CX: ColorsManager, const COMPUTE_SIMPLITIGS: bool>
    ParallelKmersMergeMapProcessor<MH, CX, COMPUTE_SIMPLITIGS>
{
    pub fn new(
        mem_tracker: MemoryTracker<
            KmersTransformProcessor<ParallelKmersMergeFactory<MH, CX, COMPUTE_SIMPLITIGS>>,
        >,
    ) -> Self {
        Self {
            map_packet: None,
            last_saved_len: 0,
            mem_tracker,
        }
    }
}

impl<MH: HashFunctionFactory, CX: ColorsManager, const COMPUTE_SIMPLITIGS: bool>
    KmersTransformMapProcessor<ParallelKmersMergeFactory<MH, CX, COMPUTE_SIMPLITIGS>>
    for ParallelKmersMergeMapProcessor<MH, CX, COMPUTE_SIMPLITIGS>
{
    type MapStruct = ParallelKmersMergeMapPacket<MH, CX>;
    const MAP_SIZE: usize = size_of::<MH::HashTypeUnextendable>()
        + size_of::<MapEntry<color_types::HashMapTempColorIndex<CX>>>();

    fn process_group_start(
        &mut self,
        map_struct: Packet<Self::MapStruct>,
        _global_data: &<ParallelKmersMergeFactory<MH, CX, COMPUTE_SIMPLITIGS> as KmersTransformExecutorFactory>::GlobalExtraData,
    ) {
        stats!(
            let mut map_struct = map_struct;
            map_struct.detailed_stats = Default::default();
            map_struct.detailed_stats.report_id = ggcat_logging::generate_stat_id!();
            map_struct.detailed_stats.start_time = ggcat_logging::get_stat_opt!(stats.start_time).elapsed().into();
        );
        self.map_packet = Some(map_struct);
        self.last_saved_len = 0;
    }

    #[instrumenter::track]
    fn process_group_batch_sequences(
        &mut self,
        global_data: &<ParallelKmersMergeFactory<MH, CX, COMPUTE_SIMPLITIGS> as KmersTransformExecutorFactory>::GlobalExtraData,
        batch: &ReadsVector<MinimizerBucketingSeqColorDataType<CX>>,
        extra_data_buffer: &<MinimizerBucketingSeqColorDataType<CX> as SequenceExtraDataTempBufferManagement>::TempBuffer,
        ref_sequences: &Vec<u8>,
    ) -> GroupProcessStats {
        let k = global_data.k;

        let map_packet = self.map_packet.as_mut().unwrap().deref_mut();

        let mut kmers_count = 0;
        let mut unique_kmers_count = 0;

        stats!(
            let start_batch_time = std::time::Instant::now();
        );

        for (flags, color, read, multiplicity) in batch.iter() {
            let read = read.as_reference(ref_sequences);

            let hashes = MH::new(read, k);

            kmers_count += (read.bases_count() - k + 1) as u64;

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
                    .or_insert_with(|| {
                        unique_kmers_count += 1;
                        MapEntry::new(CX::ColorsMergeManagerType::new_color_index())
                    });

                entry.update_flags(
                    ((begin_ignored as u8) << ((!is_forward) as u8))
                        | ((end_ignored as u8) << (is_forward as u8)),
                );

                let crossed_min_abundance =
                    entry.incr_by_and_check(multiplicity, global_data.min_multiplicity);

                CX::ColorsMergeManagerType::add_temp_buffer_structure_el::<MH>(
                    &mut map_packet.temp_colors,
                    &kmer_color,
                    (idx, hash.to_unextendable()),
                    entry,
                );

                // Update the valid indexes to allow saving reads that cross the min abundance threshold
                if !MH::INVERTIBLE && crossed_min_abundance {
                    min_idx = min(min_idx, idx / 4);
                    max_idx = max(max_idx, idx);
                }
            }

            CX::ColorsMergeManagerType::add_temp_buffer_sequence(
                &mut map_packet.temp_colors,
                read,
                global_data.k,
                global_data.m,
                flags,
            );

            if !MH::INVERTIBLE {
                if min_idx != usize::MAX {
                    encode_varint(
                        |b| {
                            map_packet.encoded_saved_reads_indexes.extend_from_slice(b);
                        },
                        (map_packet.saved_reads.len() - self.last_saved_len) as u64,
                    );
                    self.last_saved_len = map_packet.saved_reads.len();
                    map_packet.saved_reads.extend_from_slice(
                        &read.get_packed_slice()[min_idx..((max_idx + k + 3) / 4)],
                    );
                }
            }
        }

        self.mem_tracker
            .update_memory_usage(&[map_packet.get_size(), 0]);

        stats!(
            map_packet.detailed_stats.elapsed_processor_time += start_batch_time.elapsed();
        );

        GroupProcessStats {
            total_kmers: kmers_count,
            unique_kmers: unique_kmers_count,
        }
    }

    #[instrumenter::track]
    fn process_group_finalize(
        &mut self,
        global_data: &<ParallelKmersMergeFactory<MH, CX, COMPUTE_SIMPLITIGS> as KmersTransformExecutorFactory>::GlobalExtraData,
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
        map_packet.average_sequences_size = max(256, sequences_size_total / batches_count);

        COUNTER_KMERS_MAX.max(all_kmers as i64);
        COUNTER_READS_AVG.add_value(all_kmers as i64);
        self.mem_tracker.update_memory_usage(&[0, 0]);

        stats!(
            map_packet.detailed_stats.end_processor_time = ggcat_logging::get_stat_opt!(stats.start_time).elapsed().into();
            map_packet.detailed_stats.sequences_sizes = sequences_sizes;
            map_packet.detailed_stats.all_kmers_count = all_kmers;
        );

        map_packet
    }
}
