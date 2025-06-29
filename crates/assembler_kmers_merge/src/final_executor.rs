use crate::map_processor::ParallelKmersMergeMapPacket;
use crate::unitigs_extender::{UnitigExtensionColorsData, UnitigsExtenderTrait};
use crate::{GlobalMergeData, ParallelKmersMergeFactory, ResultsBucket};
use colors::colors_manager::ColorsMergeManager;
use colors::colors_manager::color_types::PartialUnitigsColorStructure;
use colors::colors_manager::{ColorsManager, color_types};
use config::DEFAULT_PER_CPU_BUFFER_SIZE;
use ggcat_logging::stats;
use hashes::{HashFunctionFactory, HashableSequence};
use instrumenter::local_setup_instrumenter;
use io::DUPLICATES_BUCKET_EXTRA;
use io::concurrent::temp_reads::creads_utils::DeserializedRead;
use io::concurrent::temp_reads::extra_data::SequenceExtraDataTempBufferManagement;
use io::structs::hash_entry::{Direction, HashEntrySerializer};
use kmers_transform::{KmersTransformExecutorFactory, KmersTransformFinalExecutor};
use parallel_processor::buckets::concurrent::{BucketsThreadBuffer, BucketsThreadDispatcher};
use parallel_processor::buckets::writers::lock_free_binary_writer::LockFreeBinaryWriter;
use parallel_processor::execution_manager::packet::Packet;
use std::ops::DerefMut;
use structs::partial_unitigs_extra_data::PartialUnitigExtraData;
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

        // let mut skipped_count = 0;
        // let mut tot_counters = 0;
        // let mut hash_sum = 0;

        // let mut fully_unique_sets = 0;
        // let mut fully_unique_count = 0;
        // let mut total_count = 0;
        // let mut kmers_in_duplicate_sets = 0;
        // map_struct.minimizer_superkmers.clear();

        if false {
            let mut tot_sum = 0u64;

            let hash_len = global_data.k - 4;

            map_struct.minimizer_superkmers.process_elements(
                #[inline(always)]
                |minimizer_elements| {
                    if minimizer_elements.len() <= 1 {
                        return;
                    }

                    for read in minimizer_elements {
                        // let read_ref = read.read.as_reference(&map_struct.superkmers_storage);

                        // let mut start = read.minimizer_pos as usize % 4;
                        // // (read.minimizer_pos as usize)
                        // while start + hash_len <= read_ref.bases_count() {
                        //     let extra_hash = read_ref.sub_slice(start..(start + 4)).get_hash_aligned();
                        //     tot_sum = tot_sum.wrapping_add(extra_hash);
                        //     start += 4;
                        // }
                    }
                },
            );

            if tot_sum == 5109027987942643475 {
                println!("HASH: {}!", tot_sum);
            }
        }

        // for minimizer in map_struct.minimizer_superkmers.iter() {
        //     // if minimizer.1.len() < 1000 {
        //     //     continue;
        //     //     //     skipped_count += 1;
        //     // }
        //     // tot_counters += minimizer.1.len();
        //     // let start = std::time::Instant::now();

        //         // if read_ref.bases_count() >= (read.minimizer_pos as usize) + global_data.k / 2 {

        //         //     // *map_packet_ref
        //         //     //     .minimizer_collisions
        //         //     //     .entry(extra_hash)
        //         //     //     .or_insert(0) += 1;
        //         // }

        //         // let end = (read.minimizer_pos as usize) + 11;
        //         // if end >= global_data.k / 2 {
        //         //     let extra_hash = read_ref
        //         //         .sub_slice((end - global_data.k / 2)..end)
        //         //         .get_hash();
        //         //     // *map_packet_ref
        //         //     //     .minimizer_collisions
        //         //     //     .entry(extra_hash)
        //         //     //     .or_insert(0) += 1;
        //         //     std::hint::black_box(extra_hash);
        //         // }
        //     }
        //     continue;

        //     map_struct.extender.reset();

        //     println!("STARTED!");

        //     // for sk in map_struct.allocator.slice_vec(minimizer.1) {
        //     //     let read = sk.read.as_reference(&map_struct.superkmers_storage);
        //     //     map_struct.extender.add_sequence(
        //     //         &DeserializedRead {
        //     //             read,
        //     //             extra: sk.extra,
        //     //             multiplicity: sk.multiplicity,
        //     //             minimizer_pos: sk.minimizer_pos,
        //     //             flags: sk.flags,
        //     //             second_bucket: sk.second_bucket,
        //     //             is_window_duplicate: sk.is_window_duplicate,
        //     //         },
        //     //         &Default::default(),
        //     //     );
        //     // }

        //     let stats = map_struct.extender.get_stats();

        //     // if stats.duplicated_kmers > 100000 {
        //     //     for sk in map_struct.allocator.slice_vec(minimizer.1) {
        //     //         let read = sk.read.as_reference(&map_struct.superkmers_storage);
        //     //         println!(
        //     //             "READ: {} with minimizer: {}",
        //     //             read.to_string(),
        //     //             read.sub_slice(
        //     //                 (sk.minimizer_pos as usize)..((sk.minimizer_pos as usize) + 11)
        //     //             )
        //     //             .to_string()
        //     //         );
        //     //     }
        //     //     todo!();
        //     // }

        //     // if stats.unique_kmers == stats.total_kmers {
        //     //     fully_unique_count += stats.unique_kmers;
        //     //     fully_unique_sets += 1;
        //     // } else {
        //     //     kmers_in_duplicate_sets += stats.duplicated_kmers;
        //     // }
        //     // total_count += stats.total_kmers;
        //     // println!(
        //     //     "STATS: {} / {} perc unique: {:.2}%",
        //     //     stats.unique_kmers,
        //     //     stats.total_kmers,
        //     //     (stats.unique_kmers as f64 / stats.total_kmers as f64) * 100.0
        //     // );

        //     // let interm = start.elapsed();

        map_struct.extender.compute_unitigs::<COMPUTE_SIMPLITIGS>(
            &mut self.colors_data,
            |colors_data, out_seq, fw_hash, bw_hash| {
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

        //     // println!("Full")
        //     // println!(
        //     //     "Elapsed for {} sk: {:?} interm: {:?}",
        //     //     minimizer.1.len(),
        //     //     start.elapsed(),
        //     //     interm
        //     // );
        // }
        // println!(
        //     "Total unique sets: {}/{} unique kmers: {}/{} [kits: {}] perc: {:.2}",
        //     fully_unique_sets,
        //     map_struct.minimizer_superkmers.len(),
        //     fully_unique_count,
        //     total_count,
        //     kmers_in_duplicate_sets,
        //     (kmers_in_duplicate_sets as f64 / total_count as f64) * 100.0
        // );

        // let remaining = tot_counters - skipped_count;
        // println!(
        //     "Skipped: {}/{} out of {} remaining => {} ({}%)",
        //     skipped_count,
        //     map_struct.minimizer_superkmers.len(),
        //     tot_counters,
        //     remaining,
        //     (remaining as f64 / tot_counters as f64) * 100.0
        // );

        // if true || map_struct.extra_bucket_data == Some(DUPLICATES_BUCKET_EXTRA) {
        // } else {
        if false {
            // map_struct.minimizer_superkmers.len() > 0 {
            // let mut collisions: Vec<_> = map_struct.minimizer_superkmers.iter().copied().collect();
            // collisions.sort_unstable_by_key(|v| v.len());
            // let tot_collisions = collisions.iter().filter(|c| c.len() > 1).count();
            // let total_sum = collisions.iter().map(|v| v.len() as u64).sum::<u64>();
            // let average = total_sum as f64 / collisions.len() as f64;
            // let median = collisions[collisions.len() / 2].len() as f64;

            // let tot_collisions_sum = collisions
            //     .iter()
            //     .copied()
            //     .filter(|c| c.len() > 1)
            //     .map(|v| v.len() as u64)
            //     .sum::<u64>();

            // let weighted_median = {
            //     let mut weighted_values: Vec<_> = collisions
            //         .iter()
            //         .flat_map(|&value| std::iter::repeat(value).take(value.len() as usize))
            //         .collect();
            //     weighted_values.sort_unstable_by_key(|v| v.len());
            //     weighted_values[weighted_values.len() / 2].len()
            // };

            // println!(
            //     "Collisions: {}/{} [{}/{}] {:.2}% with average: {} and median: {} weighted median: {} max: {}",
            //     tot_collisions,
            //     collisions.len(),
            //     tot_collisions_sum,
            //     total_sum,
            //     (tot_collisions as f64 / collisions.len() as f64) * 100.0,
            //     average,
            //     median,
            //     weighted_median,
            //     collisions.last().unwrap().len()
            // );
        }

        //     todo!();
        // }

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
