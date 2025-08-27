use crate::map_processor::ParallelKmersMergeMapPacket;
use crate::sorting::radix_sort_reads;
use crate::unitigs_extender::sorting::SortingExtender;
use crate::unitigs_extender::{UnitigExtensionColorsData, UnitigsExtenderTrait};
use crate::{GlobalMergeData, ParallelKmersMergeFactory, ResultsBucket};
use binary_heap_plus::BinaryHeap;
use colors::colors_manager::ColorsMergeManager;
use colors::colors_manager::color_types::PartialUnitigsColorStructure;
use colors::colors_manager::{ColorsManager, color_types};
use config::{DEFAULT_OUTPUT_BUFFER_SIZE, DEFAULT_PER_CPU_BUFFER_SIZE};
use ggcat_logging::stats;
use hashes::{HashFunctionFactory, HashableSequence};
use instrumenter::local_setup_instrumenter;
use io::compressed_read::CompressedRead;
use io::concurrent::temp_reads::creads_utils::{DeserializedRead, DeserializedReadIndependent};
use io::concurrent::temp_reads::extra_data::SequenceExtraDataTempBufferManagement;
use io::structs::hash_entry::{Direction, HashEntrySerializer};
use kmers_transform::{KmersTransformExecutorFactory, KmersTransformFinalExecutor};
use parallel_processor::buckets::concurrent::{BucketsThreadBuffer, BucketsThreadDispatcher};
use parallel_processor::buckets::writers::lock_free_binary_writer::LockFreeBinaryWriter;
use parallel_processor::execution_manager::packet::Packet;
use std::cmp::Reverse;
use std::iter::repeat;
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

impl<MH: HashFunctionFactory, CX: ColorsManager, const COMPUTE_SIMPLITIGS: bool>
    ParallelKmersMergeFinalExecutor<MH, CX, COMPUTE_SIMPLITIGS>
{
    fn output_sequence(
        current_bucket: &mut ResultsBucket<color_types::PartialUnitigsColorStructure<CX>>,
        colors_data: &mut UnitigExtensionColorsData<CX>,
        out_seq: &[u8],
        // fw_hash: Option<MH::HashTypeUnextendable>,
        // bw_hash: Option<MH::HashTypeUnextendable>,
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

        // if let Some(fw_hash) = fw_hash {
        //     Self::write_hashes(
        //         &mut self.hashes_tmp,
        //         fw_hash,
        //         bucket_index,
        //         read_index,
        //         Direction::Forward,
        //         buckets_count.normal_buckets_count_log,
        //     );
        // }

        // if let Some(bw_hash) = bw_hash {
        //     Self::write_hashes(
        //         &mut self.hashes_tmp,
        //         bw_hash,
        //         bucket_index,
        //         read_index,
        //         Direction::Backward,
        //         buckets_count.normal_buckets_count_log,
        //     );
        // }
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
        let bucket_index = current_bucket.get_bucket_index();

        let mut sorting_extender = SortingExtender::default();

        let temp_buffer = Default::default();

        if !map_struct.is_duplicate {
            map_struct.minimizer_superkmers.process_elements(
                #[inline(always)]
                |minimizer_elements| {
                    let has_duplicate_kmers =
                        minimizer_elements.iter().any(|m| m.is_window_duplicate);

                    if has_duplicate_kmers {
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
                            |colors_data, out_seq, _fw_hash, _bw_hash| {
                                Self::output_sequence(current_bucket, colors_data, out_seq)
                            },
                        );

                        return;
                    }

                    if minimizer_elements.len() <= 1 {
                        let read_index = current_bucket.add_compressed_read(
                            PartialUnitigExtraData {
                                colors: Default::default(),
                            },
                            minimizer_elements[0]
                                .read
                                .as_reference(&map_struct.superkmers_storage),
                            &temp_buffer,
                        );
                        return;
                    }

                    sorting_extender.clear_supertigs();
                    sorting_extender.process_reads(
                        minimizer_elements,
                        &map_struct.superkmers_storage,
                        global_data.k,
                        global_data.min_multiplicity,
                        |read, mult| {
                            let read_index = current_bucket.add_compressed_read(
                                PartialUnitigExtraData {
                                    colors: Default::default(),
                                },
                                read,
                                &temp_buffer,
                            );
                        },
                    );
                },
            );
        } else {
            map_struct.extender.compute_unitigs::<COMPUTE_SIMPLITIGS>(
                &mut self.colors_data,
                |colors_data, out_seq, _fw_hash, _bw_hash| {
                    Self::output_sequence(current_bucket, colors_data, out_seq)
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
