use crate::query_pipeline::QueryPipeline;
use colors::colors_manager::ColorsManager;
use parallel_processor::memory_fs::RemoveFileMode;
use parallel_processor::phase_times_monitor::PHASES_TIMES_MONITOR;
use rayon::iter::{IntoParallelRefIterator, ParallelIterator};
use std::path::PathBuf;
use std::sync::atomic::Ordering;

impl QueryPipeline {
    pub fn colored_query_output<CX: ColorsManager>(
        query_input: PathBuf,
        colored_query_buckets: Vec<PathBuf>,
        output_file: PathBuf,
    ) {
        PHASES_TIMES_MONITOR
            .write()
            .start_phase("phase: colored query output".to_string());

        let buckets_count = colored_query_buckets.len();

        colored_query_buckets.par_iter().for_each(|input| {
            // let mut colormap_decoder = ColorsDeserializer::<DefaultColorsSerializer>::new(&colormap_file);
            // let mut temp_colors_buffer = Vec::new();
            //
            // let mut thread_buffer = thread_buffers.get();
            // let mut colored_buckets_writer =
            //     BucketsThreadDispatcher::new(&correct_color_buckets, thread_buffer.take());
            //
            // let mut counters_vec: Vec<(CounterEntry<ColorIndexType>, ColorIndexType)> = Vec::new();
            // LockFreeBinaryReader::new(
            //     input,
            //     RemoveFileMode::Remove {
            //         remove_fs: !KEEP_FILES.load(Ordering::Relaxed),
            //     },
            //     DEFAULT_PREFETCH_AMOUNT,
            // )
            // .decode_all_bucket_items::<CounterEntry<ColorIndexType>, _>(
            //     (),
            //     &mut (),
            //     |h, _| {
            //         counters_vec.push(h);
            //     },
            // );
            //
            // struct CountersCompare;
            // impl SortKey<(CounterEntry<ColorIndexType>, ColorIndexType)> for CountersCompare {
            //     type KeyType = u64;
            //     const KEY_BITS: usize = std::mem::size_of::<u64>() * 8;
            //
            //     fn compare(
            //         left: &(CounterEntry<ColorIndexType>, ColorIndexType),
            //         right: &(CounterEntry<ColorIndexType>, ColorIndexType),
            //     ) -> std::cmp::Ordering {
            //         left.1.cmp(&right.1)
            //     }
            //
            //     fn get_shifted(
            //         value: &(CounterEntry<ColorIndexType>, ColorIndexType),
            //         rhs: u8,
            //     ) -> u8 {
            //         (value.1 >> rhs) as u8
            //     }
            // }
            //
            // fast_smart_radix_sort::<_, CountersCompare, false>(&mut counters_vec[..]);
            //
            // for queries_by_color in counters_vec.group_by_mut(|a, b| a.1 == b.1) {
            //     let color = queries_by_color[0].1;
            //     temp_colors_buffer.clear();
            //     colormap_decoder.get_color_mappings(color, &mut temp_colors_buffer)
            //     // TODO: Write final colored query to disk!
            // }
            //
            // thread_buffer.put_back(colored_buckets_writer.finalize().0);
        });

        // correct_color_buckets.finalize()
    }
}
