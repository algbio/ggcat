use crate::ParallelKmersMergeFactory;
use crate::unitigs_extender::hashmap::HashMapUnitigsExtender;
use crate::unitigs_extender::{GlobalExtenderParams, UnitigsExtenderTrait};
use colors::colors_manager::color_types::MinimizerBucketingSeqColorDataType;
use colors::colors_manager::{ColorsManager, color_types};
use ggcat_logging::stats;
use ggcat_logging::stats::KmersMergeBucketReport;
use hashes::HashFunctionFactory;
use io::concurrent::temp_reads::extra_data::SequenceExtraDataTempBufferManagement;
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
use std::cmp::max;
use std::mem::size_of;
use std::ops::DerefMut;
use std::path::PathBuf;
use std::sync::atomic::Ordering;
use structs::map_entry::MapEntry;

instrumenter::use_instrumenter!();

pub(crate) static KMERGE_TEMP_DIR: RwLock<Option<PathBuf>> = RwLock::new(None);

pub struct ParallelKmersMergeMapPacket<MH: HashFunctionFactory, CX: ColorsManager> {
    pub detailed_stats: KmersMergeBucketReport,
    pub extender: HashMapUnitigsExtender<MH, CX>,
}

impl<MH: HashFunctionFactory, CX: ColorsManager> PoolObjectTrait
    for ParallelKmersMergeMapPacket<MH, CX>
{
    type InitData = GlobalExtenderParams;

    fn allocate_new(sizes: &Self::InitData) -> Self {
        Self {
            detailed_stats: Default::default(),
            extender: HashMapUnitigsExtender::new(sizes),
        }
    }

    fn reset(&mut self) {
        self.extender.reset();
    }
}

impl<MH: HashFunctionFactory, CX: ColorsManager> PacketTrait
    for ParallelKmersMergeMapPacket<MH, CX>
{
    fn get_size(&self) -> usize {
        self.extender.get_memory_usage()
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
    }

    #[instrumenter::track]
    fn process_group_batch_sequences(
        &mut self,
        _global_data: &<ParallelKmersMergeFactory<MH, CX, COMPUTE_SIMPLITIGS> as KmersTransformExecutorFactory>::GlobalExtraData,
        batch: &ReadsVector<MinimizerBucketingSeqColorDataType<CX>>,
        extra_data_buffer: &<MinimizerBucketingSeqColorDataType<CX> as SequenceExtraDataTempBufferManagement>::TempBuffer,
        ref_sequences: &Vec<u8>,
    ) {
        let map_packet = self.map_packet.as_mut().unwrap().deref_mut();

        // let mut kmers_count = 0;
        // let mut unique_kmers_count = 0;

        stats!(
            let start_batch_time = std::time::Instant::now();
        );

        for sequence in batch.iter() {
            map_packet
                .extender
                .add_sequence(ref_sequences, extra_data_buffer, sequence);
        }

        self.mem_tracker
            .update_memory_usage(&[map_packet.get_size(), 0]);

        stats!(
            map_packet.detailed_stats.elapsed_processor_time += start_batch_time.elapsed();
        );

        // GroupProcessStats {
        //     total_kmers: kmers_count,
        //     unique_kmers: unique_kmers_count,
        // }
    }

    fn get_stats(&self) -> GroupProcessStats {
        self.map_packet.as_ref().unwrap().extender.get_stats()
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

        let stats = map_packet.extender.get_stats();

        let sequences_sizes = stats.saved_read_bytes;
        let all_kmers = stats.unique_kmers;

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

        map_packet.extender.set_suggested_sizes(
            kmers_total / batches_count,
            max(256, sequences_size_total / batches_count),
        );

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
