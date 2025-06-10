use crate::map_processor::ParallelKmersMergeMapPacket;
use crate::structs::PartialUnitigExtraData;
use crate::unitigs_extender::{UnitigExtensionColorsData, UnitigsExtenderTrait};
use crate::{GlobalMergeData, ParallelKmersMergeFactory, ResultsBucket};
use colors::colors_manager::ColorsMergeManager;
use colors::colors_manager::color_types::PartialUnitigsColorStructure;
use colors::colors_manager::{ColorsManager, color_types};
use config::DEFAULT_PER_CPU_BUFFER_SIZE;
use ggcat_logging::stats;
use hashes::HashFunctionFactory;
use instrumenter::local_setup_instrumenter;
use io::DUPLICATES_BUCKET_EXTRA;
use io::concurrent::temp_reads::extra_data::SequenceExtraDataTempBufferManagement;
use io::structs::hash_entry::{Direction, HashEntrySerializer};
use kmers_transform::{KmersTransformExecutorFactory, KmersTransformFinalExecutor};
use parallel_processor::buckets::concurrent::{BucketsThreadBuffer, BucketsThreadDispatcher};
use parallel_processor::buckets::writers::lock_free_binary_writer::LockFreeBinaryWriter;
use parallel_processor::execution_manager::packet::Packet;
use std::ops::DerefMut;
#[cfg(feature = "support_kmer_counters")]
use structs::unitigs_counters::UnitigsCounters;

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

    unitigs_temp_colors: color_types::TempUnitigColorStructure<CX>,
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
            unitigs_temp_colors: CX::ColorsMergeManagerType::alloc_unitig_color_structure(),
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

        if true || map_struct.extra_bucket_data == Some(DUPLICATES_BUCKET_EXTRA) {
            map_struct.extender.compute_unitigs::<COMPUTE_SIMPLITIGS>(
                &mut self.colors_data,
                |colors_data, out_seq, fw_hash, bw_hash| {
                    stats!(
                        stat_output_kmers_count += 1;
                    );

                    let colors =
                        color_types::ColorsMergeManagerType::<CX>::encode_part_unitigs_colors(
                            &mut self.unitigs_temp_colors,
                            &mut colors_data.temp_color_buffer,
                        );

                    let extra_data = PartialUnitigExtraData {
                        colors,

                        #[cfg(feature = "support_kmer_counters")]
                        counters,
                    };

                    let read_index = current_bucket.add_read(
                        extra_data,
                        out_seq,
                        &colors_data.temp_color_buffer,
                    );

                    color_types::PartialUnitigsColorStructure::<CX>::clear_temp_buffer(
                        &mut colors_data.temp_color_buffer,
                    );

                    if let Some(fw_hash) = fw_hash {
                        Self::write_hashes(
                            &mut self.hashes_tmp,
                            fw_hash,
                            bucket_index,
                            read_index,
                            Direction::Forward,
                            buckets_count.normal_buckets_count_log,
                        );
                    }

                    if let Some(bw_hash) = bw_hash {
                        Self::write_hashes(
                            &mut self.hashes_tmp,
                            bw_hash,
                            bucket_index,
                            read_index,
                            Direction::Backward,
                            buckets_count.normal_buckets_count_log,
                        );
                    }
                },
            );
        } else {
            if true {
                let mut collisions: Vec<_> =
                    map_struct.minimizer_collisions.values().copied().collect();
                collisions.sort_unstable();
                let tot_collisions = collisions.iter().filter(|c| **c > 1).count();
                let average =
                    collisions.iter().copied().sum::<u64>() as f64 / collisions.len() as f64;
                let median = collisions[collisions.len() / 2] as f64;

                let total_sum = collisions.iter().copied().sum::<u64>();
                let tot_collisions_sum = collisions.iter().copied().filter(|c| *c > 1).sum::<u64>();

                let weighted_median = {
                    let mut weighted_values: Vec<_> = collisions
                        .iter()
                        .flat_map(|&value| std::iter::repeat(value).take(value as usize))
                        .collect();
                    weighted_values.sort_unstable();
                    weighted_values[weighted_values.len() / 2]
                };

                println!(
                    "Collisions: {}/{} [{}/{}] {:.2}% with average: {} and median: {} weighted median: {} max: {}",
                    tot_collisions,
                    collisions.len(),
                    tot_collisions_sum,
                    total_sum,
                    (tot_collisions as f64 / collisions.len() as f64) * 100.0,
                    average,
                    median,
                    weighted_median,
                    collisions.last().unwrap()
                );
            }

            todo!();
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
