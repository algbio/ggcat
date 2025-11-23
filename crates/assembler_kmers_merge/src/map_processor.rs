use crate::unitigs_extender::hashmap::HashMapUnitigsExtender;
use crate::unitigs_extender::{GlobalExtenderParams, UnitigsExtenderTrait};
use crate::{GlobalMergeData, ParallelKmersMergeFactory};
use colors::colors_manager::{ColorsManager, color_types};
use config::{MAX_RESPLIT_SUBBUCKET_AVERAGE_MULTIPLIER, MAX_SUBBUCKET_AVERAGE_MULTIPLIER};
use ggcat_logging::stats;
use ggcat_logging::stats::KmersMergeBucketReport;
use hashes::HashFunctionFactory;
use io::DUPLICATES_BUCKET_EXTRA;
use io::concurrent::structured_sequences::StructuredSequenceBackendWrapper;
use io::concurrent::temp_reads::creads_utils::{
    AssemblerMinimizerPosition, DeserializedRead, WithMultiplicity,
};
use io::concurrent::temp_reads::extra_data::SequenceExtraDataTempBufferManagement;
use io::memstorage::memstorage_encode_read;
use kmers_transform::{
    GroupProcessStats, KmersTransformExecutorFactory, KmersTransformMapProcessor,
};
use parallel_processor::buckets::ExtraBucketData;
use parallel_processor::execution_manager::objects_pool::PoolObjectTrait;
use parallel_processor::execution_manager::packet::{Packet, PacketTrait};
use parallel_processor::mt_debug_counters::counter::{AtomicCounter, AvgMode, MaxMode};
use parallel_processor::mt_debug_counters::{declare_avg_counter_i64, declare_counter_i64};
use parking_lot::RwLock;
use rustc_hash::FxHasher;
use std::hash::Hasher;
use std::mem::size_of;
use std::ops::DerefMut;
use std::path::PathBuf;
use std::sync::atomic::Ordering;
use structs::map_entry::MapEntry;
use utils::fuzzy_hashmap::FuzzyHashmap;

instrumenter::use_instrumenter!();

pub(crate) static KMERGE_TEMP_DIR: RwLock<Option<PathBuf>> = RwLock::new(None);

pub struct ParallelKmersMergeMapPacket<
    MH: HashFunctionFactory,
    CX: ColorsManager,
    OM: StructuredSequenceBackendWrapper,
> {
    pub detailed_stats: KmersMergeBucketReport,
    pub extender: HashMapUnitigsExtender<MH, CX>,

    pub minimizer_superkmers: FuzzyHashmap<u8, 0>,
    pub superkmers_extra_buffer:
        <<ParallelKmersMergeFactory<MH, CX, OM, false> as KmersTransformExecutorFactory>::AssociatedExtraDataWithMultiplicity as SequenceExtraDataTempBufferManagement>::TempBuffer,

    pub is_duplicate: bool,
    pub is_resplitted: bool,
    pub is_outlier: bool,
    pub average_sequences: u64,
    pub m: usize,
    pub k: usize,
}

impl<MH: HashFunctionFactory, CX: ColorsManager, OM: StructuredSequenceBackendWrapper>
    PoolObjectTrait for ParallelKmersMergeMapPacket<MH, CX, OM>
{
    type InitData = GlobalExtenderParams;

    fn allocate_new(sizes: &Self::InitData) -> Self {
        const MINIMIZER_MAP_DEFAULT_SIZE: usize = 1024;
        Self {
            detailed_stats: Default::default(),
            extender: HashMapUnitigsExtender::new(sizes),
            minimizer_superkmers: FuzzyHashmap::new(MINIMIZER_MAP_DEFAULT_SIZE),
            superkmers_extra_buffer: Default::default(),
            m: sizes.m,
            k: sizes.k,
            is_duplicate: false,
            is_resplitted: false,
            is_outlier: false,
            average_sequences: 0,
        }
    }

    fn reset(&mut self) {
        self.extender.reset();
        <<ParallelKmersMergeFactory<MH, CX, OM, false> as KmersTransformExecutorFactory>::AssociatedExtraDataWithMultiplicity as SequenceExtraDataTempBufferManagement>
            ::clear_temp_buffer(&mut self.superkmers_extra_buffer);
    }
}

impl<MH: HashFunctionFactory, CX: ColorsManager, OM: StructuredSequenceBackendWrapper> PacketTrait
    for ParallelKmersMergeMapPacket<MH, CX, OM>
{
    fn get_size(&self) -> usize {
        self.extender.get_memory_usage()
    }
}

pub struct ParallelKmersMergeMapProcessor<
    MH: HashFunctionFactory,
    CX: ColorsManager,
    OM: StructuredSequenceBackendWrapper,
    const COMPUTE_SIMPLITIGS: bool,
> {
    map_packet: Option<
        Packet<
            <Self as KmersTransformMapProcessor<
                ParallelKmersMergeFactory<MH, CX, OM, COMPUTE_SIMPLITIGS>,
            >>::MapStruct,
        >,
    >,
}

pub fn hash_integer(value: u64) -> u64 {
    let mut hasher = FxHasher::default();
    hasher.write_u64(value);
    hasher.finish()
}

impl<
    MH: HashFunctionFactory,
    CX: ColorsManager,
    OM: StructuredSequenceBackendWrapper,
    const COMPUTE_SIMPLITIGS: bool,
> ParallelKmersMergeMapProcessor<MH, CX, OM, COMPUTE_SIMPLITIGS>
{
    pub fn new(_context: &GlobalMergeData<CX, OM>) -> Self {
        Self { map_packet: None }
    }
}

fn add_read<
    MH: HashFunctionFactory,
    CX: ColorsManager,
    OM: StructuredSequenceBackendWrapper,
    const COMPUTE_SIMPLITIGS: bool,
>(
    map_packet: &mut ParallelKmersMergeMapPacket<MH, CX, OM>,
    read: &DeserializedRead<'_, <ParallelKmersMergeFactory<MH, CX, OM, COMPUTE_SIMPLITIGS> as KmersTransformExecutorFactory>::AssociatedExtraDataWithMultiplicity>,
    extra_buffer: &<<ParallelKmersMergeFactory<MH, CX, OM, COMPUTE_SIMPLITIGS> as KmersTransformExecutorFactory>::AssociatedExtraDataWithMultiplicity as SequenceExtraDataTempBufferManagement>::TempBuffer,
) {
    let minimizer_pos = read.minimizer_pos as usize;

    let minimizer_hash = unsafe {
        read.read
            .sub_slice(minimizer_pos..minimizer_pos + map_packet.m)
            .compute_hash_aligned_overflow16()
    };

    let hash = hash_integer(minimizer_hash);

    memstorage_encode_read::<
        <ParallelKmersMergeFactory<MH, CX, OM, false> as KmersTransformExecutorFactory>::AssociatedExtraDataWithMultiplicity, WithMultiplicity,
        AssemblerMinimizerPosition,
        false>(
        &DeserializedRead {
            read: read.read,
            extra: <ParallelKmersMergeFactory<MH, CX, OM, COMPUTE_SIMPLITIGS> as KmersTransformExecutorFactory>::AssociatedExtraDataWithMultiplicity
                ::copy_extra_from(read.extra, extra_buffer, &mut map_packet.superkmers_extra_buffer),
            multiplicity: read.multiplicity,
            minimizer_pos: read.minimizer_pos,
            flags: read.flags,
            second_bucket: read.second_bucket,
        },
        |needed, reserved| {
            // TODO: Manage reserved
            map_packet.minimizer_superkmers.allocator_reserve_additional(reserved);
            map_packet.minimizer_superkmers.allocate_elements(hash, needed)
        },
    );
}

impl<
    MH: HashFunctionFactory,
    CX: ColorsManager,
    OM: StructuredSequenceBackendWrapper,
    const COMPUTE_SIMPLITIGS: bool,
> KmersTransformMapProcessor<ParallelKmersMergeFactory<MH, CX, OM, COMPUTE_SIMPLITIGS>>
    for ParallelKmersMergeMapProcessor<MH, CX, OM, COMPUTE_SIMPLITIGS>
{
    type MapStruct = ParallelKmersMergeMapPacket<MH, CX, OM>;
    const MAP_SIZE: usize = size_of::<MH::HashTypeUnextendable>()
        + size_of::<MapEntry<color_types::HashMapTempColorIndex<CX>>>();

    type ProcessSequencesContext = Self::MapStruct;

    fn process_group_start(
        &mut self,
        mut map_struct: Packet<Self::MapStruct>,
        global_data: &<ParallelKmersMergeFactory<MH, CX, OM, COMPUTE_SIMPLITIGS> as KmersTransformExecutorFactory>::GlobalExtraData,
        extra_bucket_data: Option<ExtraBucketData>,
        is_resplitted: bool,
        average_sequences: u64,
    ) {
        stats!(
            map_struct.detailed_stats = Default::default();
            map_struct.detailed_stats.report_id = ggcat_logging::generate_stat_id!();
            map_struct.detailed_stats.start_time = ggcat_logging::get_stat_opt!(stats.start_time).elapsed().into();
        );
        map_struct.is_duplicate = extra_bucket_data == Some(DUPLICATES_BUCKET_EXTRA);
        map_struct.is_resplitted = is_resplitted;

        map_struct.average_sequences = average_sequences;
        map_struct.is_outlier = false;
        if is_resplitted {
            map_struct.m = global_data.global_resplit_data.m;
        } else {
            map_struct.m = global_data.m;
        }
        self.map_packet = Some(map_struct);
    }

    #[instrumenter::track]
    fn process_group_sequences(
        &mut self,
        sequences_count: u64,
        process_reads_callback: impl FnOnce(&mut Self::MapStruct, fn(
                    context: &mut Self::MapStruct,
                    read: &DeserializedRead<'_, <ParallelKmersMergeFactory<MH, CX, OM, COMPUTE_SIMPLITIGS> as KmersTransformExecutorFactory>::AssociatedExtraDataWithMultiplicity>,
                    extra_buffer: &<<ParallelKmersMergeFactory<MH, CX, OM, COMPUTE_SIMPLITIGS> as KmersTransformExecutorFactory>::AssociatedExtraDataWithMultiplicity as SequenceExtraDataTempBufferManagement>::TempBuffer
                )
            ),
    ) {
        let map_packet = self.map_packet.as_mut().unwrap().deref_mut();

        map_packet.is_outlier = if map_packet.is_resplitted {
            sequences_count
                > MAX_RESPLIT_SUBBUCKET_AVERAGE_MULTIPLIER * map_packet.average_sequences
        } else {
            sequences_count > MAX_SUBBUCKET_AVERAGE_MULTIPLIER * map_packet.average_sequences
        };

        if map_packet.is_duplicate || map_packet.is_outlier {
            process_reads_callback(
                map_packet,
                #[inline(always)]
                |map_packet, read, extra_buffer| {
                    map_packet.extender.add_sequence(read, extra_buffer);
                },
            )
        } else {
            let map_size = sequences_count.next_power_of_two().max(4) as usize;
            map_packet.minimizer_superkmers.initialize(map_size);

            process_reads_callback(
                map_packet,
                #[inline(always)]
                |map_packet, read, extra_buffer| {
                    add_read::<_, _, _, COMPUTE_SIMPLITIGS>(map_packet, read, extra_buffer)
                },
            );
        }
    }

    fn get_stats(&self) -> GroupProcessStats {
        self.map_packet.as_ref().unwrap().extender.get_stats()
    }

    #[instrumenter::track]
    fn process_group_finalize(
        &mut self,
        global_data: &<ParallelKmersMergeFactory<MH, CX, OM, COMPUTE_SIMPLITIGS> as KmersTransformExecutorFactory>::GlobalExtraData,
    ) -> Packet<Self::MapStruct> {
        static COUNTER_KMERS_MAX: AtomicCounter<MaxMode> =
            declare_counter_i64!("kmers_cardinality_max", MaxMode, false);
        static COUNTER_READS_AVG: AtomicCounter<AvgMode> =
            declare_avg_counter_i64!("correct_reads_avg", false);

        let mut map_packet = self.map_packet.take().unwrap();

        let stats = map_packet.extender.get_stats();

        let all_kmers = stats.unique_kmers;

        let kmers_total = global_data
            .hasnmap_kmers_total
            .fetch_add(all_kmers, Ordering::Relaxed)
            + all_kmers;
        let batches_count = global_data
            .kmer_batches_count
            .fetch_add(1, Ordering::Relaxed)
            + 1;

        map_packet
            .extender
            .set_suggested_sizes(kmers_total / batches_count, 256);

        COUNTER_KMERS_MAX.max(all_kmers as i64);
        COUNTER_READS_AVG.add_value(all_kmers as i64);

        stats!(
            map_packet.detailed_stats.end_processor_time = ggcat_logging::get_stat_opt!(stats.start_time).elapsed().into();
            map_packet.detailed_stats.sequences_sizes = 0;
            map_packet.detailed_stats.all_kmers_count = all_kmers;
        );

        map_packet
    }
}
