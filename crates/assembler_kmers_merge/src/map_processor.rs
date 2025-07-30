use crate::unitigs_extender::hashmap::HashMapUnitigsExtender;
use crate::unitigs_extender::{GlobalExtenderParams, UnitigsExtenderTrait};
use crate::{GlobalMergeData, ParallelKmersMergeFactory};
use assembler_minimizer_bucketing::rewrite_bucket::get_superkmer_minimizer;
use colors::colors_manager::{ColorsManager, color_types};
use config::DEFAULT_OUTPUT_BUFFER_SIZE;
use ggcat_logging::stats;
use ggcat_logging::stats::KmersMergeBucketReport;
use hashbrown::HashTable;
use hashbrown::hash_table::Entry;
use hashes::default::MNHFactory;
use hashes::{HashFunction, HashFunctionFactory, HashableSequence};
use io::DUPLICATES_BUCKET_EXTRA;
use io::compressed_read::CompressedReadIndipendent;
use io::concurrent::temp_reads::creads_utils::{DeserializedRead, DeserializedReadIndependent};
use io::concurrent::temp_reads::extra_data::SequenceExtraDataTempBufferManagement;
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
use utils::inline_vec::Allocator;

instrumenter::use_instrumenter!();

pub(crate) static KMERGE_TEMP_DIR: RwLock<Option<PathBuf>> = RwLock::new(None);

pub struct ParallelKmersMergeMapPacket<MH: HashFunctionFactory, CX: ColorsManager> {
    pub detailed_stats: KmersMergeBucketReport,
    pub extender: HashMapUnitigsExtender<MH, CX>,

    pub superkmers_storage: Box<Vec<u8>>,
    // pub minimizer_superkmers: FxHashMap<u64, InlineVec<DeserializedReadIndependent<<
    // ParallelKmersMergeFactory<MH, CX, false> as KmersTransformExecutorFactory>::AssociatedExtraDataWithMultiplicity>, 0>>,
    pub minimizer_superkmers: FuzzyHashmap<DeserializedReadIndependent<<
        ParallelKmersMergeFactory<MH, CX, false> as KmersTransformExecutorFactory>::AssociatedExtraDataWithMultiplicity>, 0>,

    pub resplitting_map: FuzzyHashmap<DeserializedReadIndependent<<
        ParallelKmersMergeFactory<MH, CX, false> as KmersTransformExecutorFactory>::AssociatedExtraDataWithMultiplicity>, 0>,


    pub is_duplicate: bool,
    pub m: usize,
}

impl<MH: HashFunctionFactory, CX: ColorsManager> PoolObjectTrait
    for ParallelKmersMergeMapPacket<MH, CX>
{
    type InitData = GlobalExtenderParams;

    fn allocate_new(sizes: &Self::InitData) -> Self {
        const MINIMIZER_MAP_DEFAULT_SIZE: usize = 1024;
        Self {
            detailed_stats: Default::default(),
            extender: HashMapUnitigsExtender::new(sizes),
            superkmers_storage: Box::new(vec![]),
            minimizer_superkmers: FuzzyHashmap::new(MINIMIZER_MAP_DEFAULT_SIZE),
            resplitting_map: FuzzyHashmap::new(MINIMIZER_MAP_DEFAULT_SIZE),
            m: sizes.m,
            is_duplicate: false,
        }
    }

    fn reset(&mut self) {
        self.extender.reset();
        self.superkmers_storage.clear();
        // self.minimizer_superkmers.clear();
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
    k: usize,
}

fn hash_integer(value: u64) -> u64 {
    let mut hasher = FxHasher::default();
    hasher.write_u64(value);
    hasher.finish()
}

impl<MH: HashFunctionFactory, CX: ColorsManager, const COMPUTE_SIMPLITIGS: bool>
    ParallelKmersMergeMapProcessor<MH, CX, COMPUTE_SIMPLITIGS>
{
    pub fn new(context: &GlobalMergeData<CX>) -> Self {
        Self {
            map_packet: None,
            k: context.k,
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

    type ProcessSequencesContext = Self::MapStruct;

    fn process_group_start(
        &mut self,
        mut map_struct: Packet<Self::MapStruct>,
        global_data: &<ParallelKmersMergeFactory<MH, CX, COMPUTE_SIMPLITIGS> as KmersTransformExecutorFactory>::GlobalExtraData,
        extra_bucket_data: Option<ExtraBucketData>,
        is_resplitted: bool,
    ) {
        stats!(
            map_struct.detailed_stats = Default::default();
            map_struct.detailed_stats.report_id = ggcat_logging::generate_stat_id!();
            map_struct.detailed_stats.start_time = ggcat_logging::get_stat_opt!(stats.start_time).elapsed().into();
        );
        map_struct.is_duplicate = extra_bucket_data == Some(DUPLICATES_BUCKET_EXTRA);
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
                    read: &DeserializedRead<'_, <ParallelKmersMergeFactory<MH, CX, COMPUTE_SIMPLITIGS> as KmersTransformExecutorFactory>::AssociatedExtraDataWithMultiplicity>,
                    extra_buffer: &<<ParallelKmersMergeFactory<MH, CX, COMPUTE_SIMPLITIGS> as KmersTransformExecutorFactory>::AssociatedExtraDataWithMultiplicity as SequenceExtraDataTempBufferManagement>::TempBuffer
                )
            ),
    ) {
        let map_packet = self.map_packet.as_mut().unwrap().deref_mut();

        let map_size = sequences_count.next_power_of_two().max(4);
        map_packet
            .minimizer_superkmers
            .initialize(sequences_count as usize);

        if map_packet.is_duplicate {
            process_reads_callback(
                map_packet,
                #[inline(always)]
                |map_packet, read, extra_buffer| {
                    map_packet.extender.add_sequence(read, extra_buffer);
                },
            )
        } else {
            process_reads_callback(
                map_packet,
                #[inline(always)]
                |map_packet, read, extra_buffer| {
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

                    let minimizer_pos = read.minimizer_pos as usize;

                    let minimizer_hash = unsafe {
                        read.read
                            .sub_slice(minimizer_pos..minimizer_pos + map_packet.m)
                            .compute_hash_aligned_overflow16()
                    };

                    let new_read = CompressedReadIndipendent::from_read::<false>(
                        &read.read,
                        &mut map_packet.superkmers_storage,
                    );

                    let hash = hash_integer(minimizer_hash);

                    map_packet.minimizer_superkmers.add_element(
                        hash,
                        // map_packet
                        //     .minimizer_superkmers
                        //     .entry(minimizer_hash)
                        //     .or_insert(InlineVec::default()),
                        DeserializedReadIndependent {
                            read: new_read,
                            extra: read.extra,
                            multiplicity: read.multiplicity,
                            minimizer_pos: read.minimizer_pos,
                            flags: read.flags,
                            second_bucket: read.second_bucket,
                            is_window_duplicate: read.is_window_duplicate,
                        },
                    );

                    // assert!(
                    //     map_packet.minimizer_superkmers
                    //         [(minimizer_hash as usize) & map_size_mask]
                    //         .len()
                    //         != 1239,
                    // );

                    //     let mut dupl_count = 0;
                    //     let read_slice = read.read.get_packed_slice();
                    //     match map_packet.superkmers_hashmap.entry(
                    //         unsafe { read.read.compute_hash_aligned_overflow16() },
                    //         |a| {
                    //             a.read.bases_count() == read.read.bases_count()
                    //                 && a.read
                    //                     .get_packed_slice_aligned(&map_packet.superkmers_storage)
                    //                     == read_slice
                    //         },
                    //         |v| unsafe {
                    //             v.read
                    //                 .compute_hash_aligned_overflow16(&map_packet.superkmers_storage)
                    //         },
                    //     ) {
                    //         Entry::Occupied(mut occupied_entry) => {
                    //             occupied_entry.get_mut().multiplicity += read.multiplicity as u32;
                    //             dupl_count += 1;
                    //         }
                    //         Entry::Vacant(vacant_entry) => {

                    //             // Make room for possible overflow
                    //             map_packet.superkmers_storage.reserve(16);

                    //             vacant_entry.insert(DeserializedReadIndependent {
                    //                 read: new_read,
                    //                 extra: read.extra,
                    //                 multiplicity: read.multiplicity,
                    //                 minimizer_pos: read.minimizer_pos,
                    //                 flags: read.flags,
                    //                 second_bucket: read.second_bucket,
                    //                 is_window_duplicate: read.is_window_duplicate,
                    //             });

                    //         }
                    //     }

                    //     if dupl_count > 10 {
                    //         println!(
                    //             "Dupl count: {} over unique: {}",
                    //             dupl_count,
                    //             map_packet.superkmers_hashmap.len()
                    //         );
                    //     }
                },
            )
        }
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
        // let map_packet_ref = map_packet.deref_mut();

        // for read in map_packet_ref.superkmers_hashmap.drain() {
        //     // assert!(!map_packet_ref.superkmers_hashmap.contains_key(read.get_borrowable()));
        //     // assert!(!map_packet_ref.superkmers_hashmap.contains_key(&SuperKmerEntry::new(&kmers_storage, new_read)));

        //     let minimizer_pos = read.minimizer_pos as usize;

        //     let minimizer_hash = read
        //         .read
        //         .as_reference(&map_packet_ref.superkmers_storage)
        //         .sub_slice(minimizer_pos..minimizer_pos + self.m)
        //         .get_hash_aligned();

        //     /*
        //         TODO:
        //         - If less than x (8?) sk, process inline
        //         - Else split the kmers with rolling hashes of (k - 4) bases, ex for k=27 run up to (k - m + 1) / 4 distinct hash computations
        //           and cluster the results
        //         - For every sub-hash, dedup its skmers, and if there is only one skmer, mark it a partial unitig
        //         - If there is more than one sub-hash, make 16-bit hashes that represent the 4 bases that make up the skmer, along with the offset position
        //         - Extend the unitigs using the hashes and put the endings in an hashmap, flagging every kmer-part as hashmap joinable

        //     */
        //     // use hashes::ExtendableHashTraitType;
        //     // let original_minimizer = MNHFactory::new(
        //     //     read.read.sub_slice(minimizer_pos..minimizer_pos + self.m),
        //     //     self.m,
        //     // )
        //     // .iter()
        //     // .next()
        //     // .unwrap()
        //     // .to_unextendable();

        //     // let computed_minimizer =
        //     //     get_superkmer_minimizer(self.k, self.m, read.flags, &read.read).1;

        //     // if original_minimizer != computed_minimizer && !read.is_window_duplicate {
        //     //     panic!(
        //     //         "Minimizers: {:?} Orig: {} Computed: {} orig_pos: {}",
        //     //         MNHFactory::new(read.read, self.m)
        //     //             .iter()
        //     //             .map(|h| h.to_unextendable())
        //     //             .collect::<Vec<_>>(),
        //     //         original_minimizer,
        //     //         computed_minimizer,
        //     //         minimizer_pos
        //     //     );
        //     // }

        //     map_packet_ref.allocator.push_vec(
        //         map_packet_ref
        //             .minimizer_superkmers
        //             .entry(minimizer_hash)
        //             .or_insert(InlineVec::default()),
        //         read,
        //     );
        // }

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
            256,
            // max(256, sequences_size_total / batches_count),
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
