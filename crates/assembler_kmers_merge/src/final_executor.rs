use crate::map_processor::ParallelKmersMergeMapPacket;
use crate::sorting::radix_sort_reads;
use crate::unitigs_extender::sorting::SortingExtender;
use crate::unitigs_extender::{UnitigExtensionColorsData, UnitigsExtenderTrait};
use crate::{GlobalMergeData, ParallelKmersMergeFactory, ResultsBucket};
use binary_heap_plus::BinaryHeap;
use colors::colors_manager::ColorsMergeManager;
use colors::colors_manager::color_types::PartialUnitigsColorStructure;
use colors::colors_manager::{ColorsManager, color_types};
use config::{
    DEFAULT_OUTPUT_BUFFER_SIZE, DEFAULT_PER_CPU_BUFFER_SIZE, READ_FLAG_INCL_BEGIN,
    READ_FLAG_INCL_END,
};
use ggcat_logging::stats;
use hashes::{ExtendableHashTraitType, HashFunction, HashFunctionFactory, HashableSequence};
use instrumenter::local_setup_instrumenter;
use io::compressed_read::CompressedRead;
use io::concurrent::temp_reads::creads_utils::{
    DeserializedRead, DeserializedReadIndependent, ToReadData,
};
use io::concurrent::temp_reads::extra_data::SequenceExtraDataTempBufferManagement;
use io::structs::hash_entry::{Direction, HashEntrySerializer};
use kmers_transform::{KmersTransformExecutorFactory, KmersTransformFinalExecutor};
use parallel_processor::buckets::concurrent::{BucketsThreadBuffer, BucketsThreadDispatcher};
use parallel_processor::buckets::writers::lock_free_binary_writer::LockFreeBinaryWriter;
use parallel_processor::execution_manager::packet::Packet;
use std::cmp::Reverse;
use std::iter::repeat;
use std::marker::PhantomData;
use std::mem::take;
use std::ops::DerefMut;
use std::time::Instant;
use structs::partial_unitigs_extra_data::PartialUnitigExtraData;
#[cfg(feature = "support_kmer_counters")]
use structs::unitigs_counters::UnitigsCounters;
use utils::fuzzy_hashmap::FuzzyHashmap;

local_setup_instrumenter!();

pub struct ParallelKmersMergeFinalExecutor<
    MH: HashFunctionFactory,
    CX: ColorsManager,
    const COMPUTE_SIMPLITIGS: bool,
> {
    hashes_tmp: BucketsThreadDispatcher<
        LockFreeBinaryWriter,
        HashEntrySerializer<MH::HashTypeUnextendable>,
    >,

    current_bucket: Option<ResultsBucket<color_types::PartialUnitigsColorStructure<CX>>>,

    colors_data: UnitigExtensionColorsData<CX>,
    bucket_counter: usize,
    bucket_change_threshold: usize,
}

impl<MH: HashFunctionFactory, CX: ColorsManager, const COMPUTE_SIMPLITIGS: bool>
    ParallelKmersMergeFinalExecutor<MH, CX, COMPUTE_SIMPLITIGS>
{
    pub fn new(global_data: &GlobalMergeData<CX>) -> Self {
        let hashes_buffer =
            BucketsThreadBuffer::new(DEFAULT_PER_CPU_BUFFER_SIZE, &global_data.buckets_count);

        Self {
            hashes_tmp: BucketsThreadDispatcher::new(
                &global_data.hashes_buckets,
                hashes_buffer,
                (),
            ),
            current_bucket: None,
            colors_data: UnitigExtensionColorsData {
                colors_global_table: global_data.colors_global_table.clone(),
                unitigs_temp_colors: CX::ColorsMergeManagerType::alloc_unitig_color_structure(),
                temp_color_buffer: <PartialUnitigsColorStructure<CX> as SequenceExtraDataTempBufferManagement>::new_temp_buffer()
            },
            bucket_counter: 0,
            bucket_change_threshold: 16, // TODO: Parametrize
        }
    }
}

// static DEBUG_MAPS_HOLDER: Mutex<Vec<Box<dyn Any + Sync + Send>>> = const_mutex(Vec::new());

trait HashGenerator<MH: HashFunctionFactory> {
    fn get_extremal_hash<'a, R: ToReadData<'a>>(
        &self,
        seq: R,
        k: usize,
        beginning: bool,
    ) -> MH::HashTypeUnextendable;
}

pub struct PrecomputedHash<MH: HashFunctionFactory>(pub MH::HashTypeUnextendable);

impl<MH: HashFunctionFactory> HashGenerator<MH> for PrecomputedHash<MH> {
    fn get_extremal_hash<'a, R: ToReadData<'a>>(
        &self,
        _seq: R,
        _k: usize,
        _beginning: bool,
    ) -> MH::HashTypeUnextendable {
        self.0
    }
}

pub struct DelayedHashComputation;

impl<MH: HashFunctionFactory> HashGenerator<MH> for DelayedHashComputation {
    #[inline(always)]
    fn get_extremal_hash<'a, R: ToReadData<'a>>(
        &self,
        seq: R,
        k: usize,
        beginning: bool,
    ) -> <MH as HashFunctionFactory>::HashTypeUnextendable {
        if beginning {
            let hash = MH::new(seq, k);
            hash.iter().next().unwrap().to_unextendable()
        } else {
            let hash = MH::new(seq.subslice(seq.bases_count() - k, seq.bases_count()), k);
            hash.iter().next().unwrap().to_unextendable()
        }
    }
}

impl<MH: HashFunctionFactory, CX: ColorsManager, const COMPUTE_SIMPLITIGS: bool>
    ParallelKmersMergeFinalExecutor<MH, CX, COMPUTE_SIMPLITIGS>
{
    #[inline]
    fn output_sequence<'a, R: ToReadData<'a> + Copy, H: HashGenerator<MH>>(
        current_bucket: &mut ResultsBucket<color_types::PartialUnitigsColorStructure<CX>>,
        hashes_tmp: &mut BucketsThreadDispatcher<
            LockFreeBinaryWriter,
            HashEntrySerializer<MH::HashTypeUnextendable>,
        >,
        colors_data: &mut UnitigExtensionColorsData<CX>,
        out_seq: R,
        forward_linked: Option<H>,
        backward_linked: Option<H>,
        k: usize,
        normal_buckets_count_log: usize,
    ) {
        stats!(
            stat_output_kmers_count += 1;
        );

        let colors = color_types::ColorsMergeManagerType::<CX>::encode_part_unitigs_colors(
            &mut colors_data.unitigs_temp_colors,
            &mut colors_data.temp_color_buffer,
        );

        let extra_data = PartialUnitigExtraData {
            colors,

            #[cfg(feature = "support_kmer_counters")]
            counters,
        };

        let read_index =
            current_bucket.add_read(extra_data, out_seq, &colors_data.temp_color_buffer);

        color_types::PartialUnitigsColorStructure::<CX>::clear_temp_buffer(
            &mut colors_data.temp_color_buffer,
        );

        if let Some(fw_hash) = forward_linked {
            Self::write_hashes(
                hashes_tmp,
                fw_hash.get_extremal_hash(out_seq, k, false),
                current_bucket.get_bucket_index(),
                read_index,
                Direction::Forward,
                normal_buckets_count_log,
            );
        }

        if let Some(bw_hash) = backward_linked {
            Self::write_hashes(
                hashes_tmp,
                bw_hash.get_extremal_hash(out_seq, k, true),
                current_bucket.get_bucket_index(),
                read_index,
                Direction::Backward,
                normal_buckets_count_log,
            );
        }
    }
}

impl<MH: HashFunctionFactory, CX: ColorsManager, const COMPUTE_SIMPLITIGS: bool>
    KmersTransformFinalExecutor<ParallelKmersMergeFactory<MH, CX, COMPUTE_SIMPLITIGS>>
    for ParallelKmersMergeFinalExecutor<MH, CX, COMPUTE_SIMPLITIGS>
{
    type MapStruct = ParallelKmersMergeMapPacket<MH, CX>;

    #[instrumenter::track(fields(map_capacity = map_struct_packet.rhash_map.capacity(), map_size = map_struct_packet.rhash_map.len()))]
    fn process_map(
        &mut self,
        global_data: &<ParallelKmersMergeFactory<MH, CX, COMPUTE_SIMPLITIGS> as KmersTransformExecutorFactory>::GlobalExtraData,
        mut map_struct_packet: Packet<Self::MapStruct>,
    ) -> Packet<Self::MapStruct> {
        if self.current_bucket.is_none() {
            self.current_bucket = Some(global_data.output_results_buckets.pop().unwrap());
        }

        stats!(
            map_struct_packet.detailed_stats.start_finalize_time = ggcat_logging::get_stat_opt!(stats.start_time).elapsed().into();
            let mut stat_output_kmers_count = 0;
        );

        let map_struct = map_struct_packet.deref_mut();

        let buckets_count = global_data.buckets_count;

        let current_bucket = self.current_bucket.as_mut().unwrap();

        let mut sorting_extender = SortingExtender::default();

        if !map_struct.is_duplicate {
            map_struct.minimizer_superkmers.process_elements(
                #[inline(always)]
                |minimizer_elements| {
                    let has_duplicate_kmers =
                        minimizer_elements.iter().any(|m| m.is_window_duplicate);

                    if has_duplicate_kmers || minimizer_elements.len() <= 1 {
                        map_struct.extender.reset();

                        for element in minimizer_elements {
                            map_struct.extender.add_sequence(
                                &DeserializedRead {
                                    read: element.read.as_reference(&map_struct.superkmers_storage),
                                    extra: element.extra,
                                    multiplicity: element.multiplicity,
                                    minimizer_pos: element.minimizer_pos,
                                    flags: element.flags,
                                    second_bucket: element.second_bucket,
                                    is_window_duplicate: element.is_window_duplicate,
                                },
                                &map_struct.superkmers_extra_buffer,
                            );
                        }

                        map_struct.extender.compute_unitigs::<COMPUTE_SIMPLITIGS>(
                            &mut self.colors_data,
                            #[inline(always)]
                            |colors_data, out_seq, fw_hash, bw_hash| {
                                Self::output_sequence(
                                    current_bucket,
                                    &mut self.hashes_tmp,
                                    colors_data,
                                    out_seq,
                                    fw_hash,
                                    bw_hash,
                                    global_data.k,
                                    buckets_count.normal_buckets_count_log,
                                )
                            },
                        );

                        return;
                    }

                    if minimizer_elements.len() <= 1 {
                        let read = &minimizer_elements[0];
                        Self::output_sequence(
                            current_bucket,
                            &mut self.hashes_tmp,
                            &mut self.colors_data,
                            read.read.as_reference(&map_struct.superkmers_storage),
                            if read.flags & READ_FLAG_INCL_END == 0 {
                                Some(DelayedHashComputation)
                            } else {
                                None
                            },
                            if read.flags & READ_FLAG_INCL_BEGIN == 0 {
                                Some(DelayedHashComputation)
                            } else {
                                None
                            },
                            global_data.k,
                            buckets_count.normal_buckets_count_log,
                        );

                        return;
                    }

                    sorting_extender.clear_supertigs();
                    sorting_extender.process_reads(
                        minimizer_elements,
                        &map_struct.superkmers_storage,
                        global_data.k,
                        global_data.min_multiplicity,
                        |read, fw_linked, bw_linked| {
                            Self::output_sequence(
                                current_bucket,
                                &mut self.hashes_tmp,
                                &mut self.colors_data,
                                read,
                                fw_linked,
                                bw_linked,
                                global_data.k,
                                buckets_count.normal_buckets_count_log,
                            )
                        },
                    );
                },
            );
        } else {
            map_struct.extender.compute_unitigs::<COMPUTE_SIMPLITIGS>(
                &mut self.colors_data,
                #[inline(always)]
                |colors_data, out_seq, fw_hash, bw_hash| {
                    Self::output_sequence(
                        current_bucket,
                        &mut self.hashes_tmp,
                        colors_data,
                        out_seq,
                        fw_hash,
                        bw_hash,
                        global_data.k,
                        buckets_count.normal_buckets_count_log,
                    )
                },
            );
        }

        self.bucket_counter += 1;
        if self.bucket_counter >= self.bucket_change_threshold {
            self.bucket_counter = 0;
            let _ = global_data
                .output_results_buckets
                .push(self.current_bucket.take().unwrap());
        }

        stats!(
            map_struct_packet.detailed_stats.end_finalize_time = ggcat_logging::get_stat_opt!(stats.start_time).elapsed().into();
            map_struct_packet.detailed_stats.output_kmers_count = stat_output_kmers_count;
        );

        stats!(stats.assembler.kmers_merge_stats.push(map_struct_packet.detailed_stats.clone()););

        // DEBUG_MAPS_HOLDER.lock().push(Box::new(map_struct_packet));
        map_struct_packet
    }

    fn finalize(
        self,
        _global_data: &<ParallelKmersMergeFactory<MH, CX, COMPUTE_SIMPLITIGS> as KmersTransformExecutorFactory>::GlobalExtraData,
    ) {
        self.hashes_tmp.finalize();
    }
}
