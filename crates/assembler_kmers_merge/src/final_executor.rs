use crate::map_processor::ParallelKmersMergeMapPacket;
use crate::unitigs_extender::sorting::SortingExtender;
use crate::unitigs_extender::{UnitigExtensionColorsData, UnitigsExtenderTrait};
use crate::{GlobalMergeData, ParallelKmersMergeFactory};
use colors::colors_manager::color_types::PartialUnitigsColorStructure;
use colors::colors_manager::{ColorsManager, color_types};
use colors::colors_manager::{ColorsMergeManager, MinimizerBucketingSeqColorData};
use config::{
    DEFAULT_OUTPUT_BUFFER_SIZE, DEFAULT_PER_CPU_BUFFER_SIZE, READ_FLAG_INCL_BEGIN,
    READ_FLAG_INCL_END,
};
use ggcat_logging::stats;
use hashes::extremal::{DelayedHashComputation, HashGenerator};
use hashes::{ExtendableHashTraitType, HashFunctionFactory};
use instrumenter::local_setup_instrumenter;
use io::concurrent::structured_sequences::StructuredSequenceBackendWrapper;
use io::concurrent::structured_sequences::concurrent::FastaWriterConcurrentBuffer;
use io::concurrent::temp_reads::creads_utils::{
    AlignToMinimizerByteBoundary, AssemblerMinimizerPosition, CompressedReadsBucketData,
    CompressedReadsBucketDataSerializer, DeserializedReadIndependent, NoMultiplicity,
    NoSecondBucket, ToReadData,
};
use io::concurrent::temp_reads::extra_data::SequenceExtraDataTempBufferManagement;
use kmers_transform::{KmersTransformExecutorFactory, KmersTransformFinalExecutor};
use parallel_processor::buckets::concurrent::{BucketsThreadBuffer, BucketsThreadDispatcher};
use parallel_processor::buckets::writers::compressed_binary_writer::CompressedBinaryWriter;
use parallel_processor::execution_manager::packet::Packet;
use std::marker::PhantomData;
use std::ops::DerefMut;
use std::sync::Arc;
use structs::partial_unitigs_extra_data::PartialUnitigExtraData;
use typenum::U2;

local_setup_instrumenter!();

pub struct ParallelKmersMergeFinalExecutor<
    MH: HashFunctionFactory,
    CX: ColorsManager,
    OM: StructuredSequenceBackendWrapper,
    const COMPUTE_SIMPLITIGS: bool,
> {
    unitigs_tmp: BucketsThreadDispatcher<
        CompressedBinaryWriter,
        CompressedReadsBucketDataSerializer<
            PartialUnitigExtraData<color_types::PartialUnitigsColorStructure<CX>>,
            NoSecondBucket,
            NoMultiplicity,
            AssemblerMinimizerPosition,
            U2,
            AlignToMinimizerByteBoundary,
        >,
    >,

    colors_data: UnitigExtensionColorsData<CX>,
    _phantom: PhantomData<(MH, OM)>,
}

impl<
    MH: HashFunctionFactory,
    CX: ColorsManager,
    OM: StructuredSequenceBackendWrapper,
    const COMPUTE_SIMPLITIGS: bool,
> ParallelKmersMergeFinalExecutor<MH, CX, OM, COMPUTE_SIMPLITIGS>
{
    pub fn new(global_data: &GlobalMergeData<CX, OM>) -> Self {
        let unitigs_out_buffer =
            BucketsThreadBuffer::new(DEFAULT_PER_CPU_BUFFER_SIZE, &global_data.buckets_count);

        Self {
            unitigs_tmp: BucketsThreadDispatcher::new(
                &global_data.output_results_buckets,
                unitigs_out_buffer,
                global_data.k,
            ),
            colors_data: UnitigExtensionColorsData {
                colors_global_table: global_data.colors_global_table.clone(),
                unitigs_temp_colors: CX::ColorsMergeManagerType::alloc_unitig_color_structure(),
                temp_color_buffer: <PartialUnitigsColorStructure<CX> as SequenceExtraDataTempBufferManagement>::new_temp_buffer()
            },
            _phantom: PhantomData
        }
    }
}

// static DEBUG_MAPS_HOLDER: Mutex<Vec<Box<dyn Any + Sync + Send>>> = const_mutex(Vec::new());

impl<
    MH: HashFunctionFactory,
    CX: ColorsManager,
    OM: StructuredSequenceBackendWrapper,
    const COMPUTE_SIMPLITIGS: bool,
> ParallelKmersMergeFinalExecutor<MH, CX, OM, COMPUTE_SIMPLITIGS>
{
    #[inline]
    fn output_sequence<'a, 'b, R: ToReadData<'a> + Copy, H: HashGenerator<MH>>(
        lonely_unitigs: &mut FastaWriterConcurrentBuffer<
            'b,
            PartialUnitigsColorStructure<CX>,
            (),
            OM::Backend<PartialUnitigsColorStructure<CX>, ()>,
        >,
        lonely_circular_unitigs: Option<
            &mut FastaWriterConcurrentBuffer<
                'b,
                PartialUnitigsColorStructure<CX>,
                (),
                OM::Backend<PartialUnitigsColorStructure<CX>, ()>,
            >,
        >,
        unitigs_tmp: &mut BucketsThreadDispatcher<
            CompressedBinaryWriter,
            CompressedReadsBucketDataSerializer<
                PartialUnitigExtraData<color_types::PartialUnitigsColorStructure<CX>>,
                NoSecondBucket,
                NoMultiplicity,
                AssemblerMinimizerPosition,
                U2,
                AlignToMinimizerByteBoundary,
            >,
        >,
        colors_data: &mut UnitigExtensionColorsData<CX>,
        out_seq: R,
        forward_linked: Option<H>,
        backward_linked: Option<H>,
        k: usize,
        #[cfg(feature = "support_kmer_counters")]
        counters: io::concurrent::structured_sequences::SequenceAbundance,
        is_circular: bool,
    ) {
        let colors = color_types::ColorsMergeManagerType::<CX>::encode_part_unitigs_colors(
            &mut colors_data.unitigs_temp_colors,
            &mut colors_data.temp_color_buffer,
        );

        let extra_data = PartialUnitigExtraData {
            colors,
            #[cfg(feature = "support_kmer_counters")]
            counters,
        };

        if forward_linked.is_none() && backward_linked.is_none() {
            if is_circular {
                lonely_circular_unitigs.unwrap_or(lonely_unitigs).add_read(
                    out_seq.into_bases_iter(),
                    None,
                    extra_data.colors,
                    &colors_data.temp_color_buffer,
                    (),
                    &(),
                    #[cfg(feature = "support_kmer_counters")]
                    counters,
                );
            } else {
                lonely_unitigs.add_read(
                    out_seq.into_bases_iter(),
                    None,
                    extra_data.colors,
                    &colors_data.temp_color_buffer,
                    (),
                    &(),
                    #[cfg(feature = "support_kmer_counters")]
                    counters,
                );
            }
        } else {
            let hash_beginning = forward_linked.is_none();
            let both_ends = forward_linked.is_some() && backward_linked.is_some();
            let hash = forward_linked.or(backward_linked).unwrap();

            let extremal_hash = hash.get_extremal_hash(out_seq, k, hash_beginning);
            let should_rc = !extremal_hash.is_forward();

            let last_align = if hash_beginning ^ should_rc {
                0
            } else {
                (out_seq.bases_count() - k) % 4
            };

            unitigs_tmp.add_element_extended(
                MH::get_bucket(
                    0,
                    unitigs_tmp.get_buckets_count().normal_buckets_count_log,
                    extremal_hash.to_unextendable(),
                ),
                &extra_data,
                &colors_data.temp_color_buffer,
                &CompressedReadsBucketData {
                    read: out_seq.to_read_data().reverse_complement(should_rc),
                    multiplicity: 0,
                    minimizer_pos: last_align as u16,
                    extra_bucket: 0,
                    flags: (!hash_beginning ^ should_rc) as u8 | ((both_ends as u8) << 1),
                },
            );
        }
        color_types::PartialUnitigsColorStructure::<CX>::clear_temp_buffer(
            &mut colors_data.temp_color_buffer,
        );

        // TODO:
        // - write sequences to disk in buckets based on their extremal hash (randomly left or right)
        // - Group sequences using an hashmap on a fast extremity hash + join them
        // - Write sequences again using an hashmap
    }
}

impl<
    MH: HashFunctionFactory,
    CX: ColorsManager,
    OM: StructuredSequenceBackendWrapper,
    const COMPUTE_SIMPLITIGS: bool,
> KmersTransformFinalExecutor<ParallelKmersMergeFactory<MH, CX, OM, COMPUTE_SIMPLITIGS>>
    for ParallelKmersMergeFinalExecutor<MH, CX, OM, COMPUTE_SIMPLITIGS>
{
    type MapStruct = ParallelKmersMergeMapPacket<MH, CX, OM>;

    #[instrumenter::track(fields(map_capacity = map_struct_packet.rhash_map.capacity(), map_size = map_struct_packet.rhash_map.len()))]
    fn process_map(
        &mut self,
        global_data: &Arc<<ParallelKmersMergeFactory<MH, CX, OM, COMPUTE_SIMPLITIGS> as KmersTransformExecutorFactory>::GlobalExtraData>,
        mut map_struct_packet: Packet<Self::MapStruct>,
    ) -> Packet<Self::MapStruct> {
        stats!(
            map_struct_packet.detailed_stats.start_finalize_time = ggcat_logging::get_stat_opt!(stats.start_time).elapsed().into();
            let mut stat_output_kmers_count = 0;
        );

        let map_struct = map_struct_packet.deref_mut();

        let mut tmp_final_unitigs_buffer = FastaWriterConcurrentBuffer::new(
            &global_data.final_unitigs_file,
            DEFAULT_OUTPUT_BUFFER_SIZE,
            true,
            global_data.k,
        );

        let mut tmp_final_unitigs_circular_buffer =
            global_data.final_circular_unitigs_file.as_ref().map(|f| {
                FastaWriterConcurrentBuffer::new(f, DEFAULT_OUTPUT_BUFFER_SIZE, true, global_data.k)
            });

        let mut sorting_extender = SortingExtender::<CX>::default();
        let mut single_entry_colors = vec![];

        if !map_struct.is_duplicate {
            let mut process_fn = |minimizer_elements: &mut [DeserializedReadIndependent<<
                ParallelKmersMergeFactory<MH, CX, OM, false> as KmersTransformExecutorFactory>::AssociatedExtraDataWithMultiplicity>],
                superkmers_storage: &Vec<u8>,
                superkmers_extra_buffer: &<<ParallelKmersMergeFactory<MH, CX, OM, false> as KmersTransformExecutorFactory>::AssociatedExtraDataWithMultiplicity as SequenceExtraDataTempBufferManagement>::TempBuffer| {
                    if minimizer_elements.len() <= 1
                        && minimizer_elements[0].multiplicity as usize
                            >= global_data.min_multiplicity
                    {
                        let read = &minimizer_elements[0];
                        stats!(
                            stat_output_kmers_count += 1;
                        );

                        // Assign color for this entry
                        if CX::COLORS_ENABLED {
                            CX::ColorsMergeManagerType::reset_unitig_color_structure(
                                &mut self.colors_data.unitigs_temp_colors,
                            );

                            single_entry_colors.clear();
                            single_entry_colors.extend_from_slice(
                                read.extra
                                    .get_unique_color(superkmers_extra_buffer),
                            );
                            let color = CX::ColorsMergeManagerType::assign_color(
                                &self.colors_data.colors_global_table,
                                &mut single_entry_colors,
                            );
                            CX::ColorsMergeManagerType::extend_forward_with_color(
                                &mut self.colors_data.unitigs_temp_colors,
                                color,
                                read.read.bases_count() - global_data.k + 1,
                            );
                        }

                        Self::output_sequence(
                            &mut tmp_final_unitigs_buffer,
                            None,
                            &mut self.unitigs_tmp,
                            &mut self.colors_data,
                            read.read.as_reference(superkmers_storage),
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
                            #[cfg(feature = "support_kmer_counters")]
                            io::concurrent::structured_sequences::SequenceAbundance {
                                first: read.multiplicity as u64,
                                sum: read.multiplicity as u64
                                    * (read.read.bases_count() - global_data.k + 1) as u64,
                                last: read.multiplicity as u64,
                            },
                            // Can't be circular because the minimizer is unique
                            false,
                        );

                        return;
                    }

                    sorting_extender.clear_supertigs();
                    sorting_extender.process_reads::<COMPUTE_SIMPLITIGS>(
                        &mut self.colors_data,
                        minimizer_elements,
                        superkmers_extra_buffer,
                        superkmers_storage,
                        global_data.k,
                        global_data.min_multiplicity,
                        |colors_data, read, fw_linked, bw_linked, _abundance| {
                            stats!(
                                stat_output_kmers_count += 1;
                            );
                            Self::output_sequence(
                                &mut tmp_final_unitigs_buffer,
                                None,
                                &mut self.unitigs_tmp,
                                colors_data,
                                read,
                                fw_linked,
                                bw_linked,
                                global_data.k,
                                #[cfg(feature = "support_kmer_counters")]
                                _abundance,
                                // Can't be circular
                                false,
                            )
                        },
                    );
                };

            map_struct.minimizer_superkmers.process_elements(
                #[inline(always)]
                |minimizer_elements| {
                    process_fn(
                        minimizer_elements,
                        &map_struct.superkmers_storage,
                        &map_struct.superkmers_extra_buffer,
                    )
                },
            );
        } else {
            map_struct.extender.compute_unitigs::<COMPUTE_SIMPLITIGS>(
                &mut self.colors_data,
                #[inline(always)]
                |colors_data, out_seq, fw_hash, bw_hash, is_circular, _abundance| {
                    stats!(
                        stat_output_kmers_count += 1;
                    );
                    Self::output_sequence(
                        &mut tmp_final_unitigs_buffer,
                        tmp_final_unitigs_circular_buffer.as_mut(),
                        &mut self.unitigs_tmp,
                        colors_data,
                        out_seq,
                        fw_hash,
                        bw_hash,
                        global_data.k,
                        #[cfg(feature = "support_kmer_counters")]
                        _abundance,
                        is_circular,
                    )
                },
            );
        }

        stats!(
            map_struct_packet.detailed_stats.end_finalize_time = ggcat_logging::get_stat_opt!(stats.start_time).elapsed().into();
            map_struct_packet.detailed_stats.output_kmers_count = stat_output_kmers_count;
        );

        stats!(stats.assembler.kmers_merge_stats.push(map_struct_packet.detailed_stats.clone()););

        map_struct_packet
    }

    fn finalize(
        self,
        _global_data: &<ParallelKmersMergeFactory<MH, CX, OM, COMPUTE_SIMPLITIGS> as KmersTransformExecutorFactory>::GlobalExtraData,
    ) {
        self.unitigs_tmp.finalize();
    }
}
