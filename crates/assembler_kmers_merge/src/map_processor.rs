use crate::ParallelKmersMergeFactory;
use crate::unitigs_extender::hashmap::HashMapUnitigsExtender;
use crate::unitigs_extender::{GlobalExtenderParams, UnitigsExtenderTrait};
use colors::colors_manager::color_types::MinimizerBucketingMultipleSeqColorDataType;
use colors::colors_manager::{ColorsManager, color_types};
use ggcat_logging::stats;
use ggcat_logging::stats::KmersMergeBucketReport;
use hashbrown::HashTable;
use hashbrown::hash_table::Entry;
use hashes::{HashFunctionFactory, HashableSequence};
use io::DUPLICATES_BUCKET_EXTRA;
use io::compressed_read::CompressedReadIndipendent;
use io::concurrent::temp_reads::extra_data::SequenceExtraDataTempBufferManagement;
use kmers_transform::reads_buffer::ReadsVector;
use kmers_transform::{
    GroupProcessStats, KmersTransformExecutorFactory, KmersTransformMapProcessor,
};
use parallel_processor::buckets::ExtraBucketData;
use parallel_processor::execution_manager::objects_pool::PoolObjectTrait;
use parallel_processor::execution_manager::packet::{Packet, PacketTrait};
use parallel_processor::mt_debug_counters::counter::{AtomicCounter, AvgMode, MaxMode};
use parallel_processor::mt_debug_counters::{declare_avg_counter_i64, declare_counter_i64};
use parking_lot::RwLock;
use rustc_hash::FxHashMap;
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
    pub superkmers_hashmap: HashTable<(CompressedReadIndipendent, u64)>,
    pub superkmers_storage: Box<Vec<u8>>,
    pub minimizer_collisions: FxHashMap<u64, u64>,
    pub extra_bucket_data: Option<ExtraBucketData>,
}

impl<MH: HashFunctionFactory, CX: ColorsManager> PoolObjectTrait
    for ParallelKmersMergeMapPacket<MH, CX>
{
    type InitData = GlobalExtenderParams;

    fn allocate_new(sizes: &Self::InitData) -> Self {
        Self {
            detailed_stats: Default::default(),
            extender: HashMapUnitigsExtender::new(sizes),
            superkmers_hashmap: HashTable::default(),
            superkmers_storage: Box::new(vec![]),
            minimizer_collisions: FxHashMap::default(),
            extra_bucket_data: None,
        }
    }

    fn reset(&mut self) {
        self.extender.reset();
        self.superkmers_hashmap.clear();
        self.superkmers_storage.clear();
        self.minimizer_collisions.clear();
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
}

impl<MH: HashFunctionFactory, CX: ColorsManager, const COMPUTE_SIMPLITIGS: bool>
    ParallelKmersMergeMapProcessor<MH, CX, COMPUTE_SIMPLITIGS>
{
    pub fn new() -> Self {
        Self { map_packet: None }
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
        global_data: &<ParallelKmersMergeFactory<MH, CX, COMPUTE_SIMPLITIGS> as KmersTransformExecutorFactory>::GlobalExtraData,
        batch: &ReadsVector<MinimizerBucketingMultipleSeqColorDataType<CX>>,
        extra_data_buffer: &<MinimizerBucketingMultipleSeqColorDataType<CX> as SequenceExtraDataTempBufferManagement>::TempBuffer,
        ref_sequences: &Vec<u8>,
    ) {
        let map_packet = self.map_packet.as_mut().unwrap().deref_mut();
        map_packet.extra_bucket_data = batch.extra_bucket_data;
        stats!(
            let start_batch_time = std::time::Instant::now();
        );

        if true || batch.extra_bucket_data == Some(DUPLICATES_BUCKET_EXTRA) {
            for sequence in batch.iter() {
                map_packet
                    .extender
                    .add_sequence(ref_sequences, extra_data_buffer, sequence);
            }
        } else {
            for sequence in batch.iter() {
                // TODO:
                // Save the start offset of the minimizer in the bucket

                // first pass: collapse unique super-kmers
                // index k-mers based on (loe 32-bit) 2-fold minimizer values + distance between minimizers
                // Betore adding a new super-kmer, check if there are k-mers already present in the current dataset
                // In case, define a plan to increase that k-mer counter in the end, and mask out the current k-mer.
                // Iterate each super-kmer, cnecking for duplicate/branching k-mers by using the minimizer pair hashtable

                // TODO:
                // 0) Track unicity of minimizers & improve first hash computation (x4 speed improvement)
                // 1) Add a flag to each read to determine if the minimizer is unique or not
                // 2) Find the one (or multiple) minimizers in each super-kmer and compute the left and/or right extended minimizer of size m+(k-m+1?)/2
                // 3) Dedup and group the super-kmers by their extended minimizers (common storage + specialized vec based hashmap)
                // 4) For each super-kmer extended minimizer group build the maximal unitigs and join them using an hashmap
                // 5) Improve the kmers hashmap storage, reducing accesses from 1 to 4
                // 5) Optionally increase the threshold for resplitting
                {
                    let read_slice = sequence.read.get_packed_slice_aligned(&ref_sequences);
                    match map_packet.superkmers_hashmap.entry(
                        sequence.read.compute_hash_aligned(&ref_sequences),
                        |a| {
                            a.0.bases_count() == sequence.read.bases_count()
                                && a.0.get_packed_slice_aligned(&map_packet.superkmers_storage)
                                    == read_slice
                        },
                        |v| v.0.compute_hash_aligned(&map_packet.superkmers_storage),
                    ) {
                        Entry::Occupied(mut occupied_entry) => {
                            occupied_entry.get_mut().1 += sequence.multiplicity as u64;
                        }
                        Entry::Vacant(vacant_entry) => {
                            let read = sequence.read.as_reference(&ref_sequences);
                            let new_read = CompressedReadIndipendent::from_read(
                                &read,
                                &mut map_packet.superkmers_storage,
                            );
                            // assert!(!map_packet.superkmers_hashmap.contains_key(read.get_borrowable()));
                            // assert!(!map_packet.superkmers_hashmap.contains_key(&SuperKmerEntry::new(&kmers_storage, new_read)));
                            vacant_entry.insert((new_read, sequence.multiplicity as u64));

                            let minimizer_pos = sequence.minimizer_pos as usize;

                            if !sequence.is_window_duplicate && batch.extra_bucket_data.is_none() {
                                let first_hash = read
                                    .sub_slice(minimizer_pos..minimizer_pos + batch.minimizer_size)
                                    .get_hash();
                            }
                            if read.bases_count() >= minimizer_pos + global_data.k / 2 {
                                let extra_hash = read
                                    .sub_slice(minimizer_pos..minimizer_pos + global_data.k / 2)
                                    .get_hash();

                                *map_packet
                                    .minimizer_collisions
                                    .entry(extra_hash)
                                    .or_insert(0) += 1;
                            }

                            let end = minimizer_pos + batch.minimizer_size;
                            if end >= global_data.k / 2 {
                                let extra_hash =
                                    read.sub_slice((end - global_data.k / 2)..end).get_hash();
                                *map_packet
                                    .minimizer_collisions
                                    .entry(extra_hash)
                                    .or_insert(0) += 1;
                            }
                        }
                    }
                }
            }
        }

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

        stats!(
            map_packet.detailed_stats.end_processor_time = ggcat_logging::get_stat_opt!(stats.start_time).elapsed().into();
            map_packet.detailed_stats.sequences_sizes = sequences_sizes;
            map_packet.detailed_stats.all_kmers_count = all_kmers;
        );

        map_packet
    }
}
