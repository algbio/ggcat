use crate::colors::colors_manager::color_types::SingleKmerColorDataType;
use crate::colors::storage::ColorsSerializerTrait;
use crate::config::{
    BucketIndexType, SwapPriority, DEFAULT_LZ4_COMPRESSION_LEVEL, DEFAULT_PER_CPU_BUFFER_SIZE,
    DEFAULT_PREFETCH_AMOUNT, MINIMIZER_BUCKETS_CHECKPOINT_SIZE,
};
use crate::io::concurrent::temp_reads::extra_data::SequenceExtraData;
use crate::query_pipeline::counters_sorting::CounterEntry;
use crate::query_pipeline::QueryPipeline;
use crate::structs::query_colored_counters::QueryColoredCounters;
use crate::utils::get_memory_mode;
use crate::{ColorIndexType, ColorsDeserializer, KEEP_FILES};
use parallel_processor::buckets::concurrent::{BucketsThreadBuffer, BucketsThreadDispatcher};
use parallel_processor::buckets::readers::lock_free_binary_reader::LockFreeBinaryReader;
use parallel_processor::buckets::readers::BucketReader;
use parallel_processor::buckets::writers::compressed_binary_writer::CompressedBinaryWriter;
use parallel_processor::buckets::MultiThreadBuckets;
use parallel_processor::fast_smart_bucket_sort::{fast_smart_radix_sort, SortKey};
use parallel_processor::memory_fs::RemoveFileMode;
use parallel_processor::phase_times_monitor::PHASES_TIMES_MONITOR;
use parallel_processor::utils::scoped_thread_local::ScopedThreadLocal;
use rayon::prelude::*;
use std::marker::PhantomData;
use std::path::PathBuf;
use std::sync::atomic::Ordering;
use std::sync::Arc;

impl QueryPipeline {
    pub fn colormap_reading<CD: ColorsSerializerTrait>(
        colormap_file: PathBuf,
        colored_query_buckets: Vec<PathBuf>,
        temp_dir: PathBuf,
    ) -> Vec<PathBuf> {
        PHASES_TIMES_MONITOR
            .write()
            .start_phase("phase: colormap reading".to_string());

        let buckets_count = colored_query_buckets.len();
        let buckets_prefix_path = temp_dir.join("query_colors");

        let correct_color_buckets = Arc::new(MultiThreadBuckets::<CompressedBinaryWriter>::new(
            buckets_count,
            buckets_prefix_path,
            &(
                get_memory_mode(SwapPriority::MinimizerBuckets),
                MINIMIZER_BUCKETS_CHECKPOINT_SIZE,
                DEFAULT_LZ4_COMPRESSION_LEVEL,
            ),
        ));

        let thread_buffers = ScopedThreadLocal::new(move || {
            BucketsThreadBuffer::new(DEFAULT_PER_CPU_BUFFER_SIZE, buckets_count)
        });

        colored_query_buckets.par_iter().for_each(|input| {
            let mut colormap_decoder = ColorsDeserializer::<CD>::new(&colormap_file);
            let mut temp_colors_buffer = Vec::new();

            let mut thread_buffer = thread_buffers.get();
            let mut colored_buckets_writer =
                BucketsThreadDispatcher::new(&correct_color_buckets, thread_buffer.take());

            let mut counters_vec: Vec<(CounterEntry<ColorIndexType>, ColorIndexType)> = Vec::new();
            LockFreeBinaryReader::new(
                input,
                RemoveFileMode::Remove {
                    remove_fs: !KEEP_FILES.load(Ordering::Relaxed),
                },
                DEFAULT_PREFETCH_AMOUNT,
            )
            .decode_all_bucket_items::<CounterEntry<ColorIndexType>, _>(
                (),
                &mut (),
                |h, _| {
                    counters_vec.push(h);
                },
            );

            struct CountersCompare;
            impl SortKey<(CounterEntry<ColorIndexType>, ColorIndexType)> for CountersCompare {
                type KeyType = u64;
                const KEY_BITS: usize = std::mem::size_of::<u64>() * 8;

                fn compare(
                    left: &(CounterEntry<ColorIndexType>, ColorIndexType),
                    right: &(CounterEntry<ColorIndexType>, ColorIndexType),
                ) -> std::cmp::Ordering {
                    left.1.cmp(&right.1)
                }

                fn get_shifted(
                    value: &(CounterEntry<ColorIndexType>, ColorIndexType),
                    rhs: u8,
                ) -> u8 {
                    (value.1 >> rhs) as u8
                }
            }

            fast_smart_radix_sort::<_, CountersCompare, false>(&mut counters_vec[..]);

            for queries_by_color in counters_vec.group_by_mut(|a, b| a.1 == b.1) {
                let color = queries_by_color[0].1;
                temp_colors_buffer.clear();
                colormap_decoder.get_color_mappings(color, &mut temp_colors_buffer);

                // TODO: Encode colors here!
                // for entry in queries_by_color {
                //     colored_buckets_writer.add_element_extended(
                //         (entry.0.query_index / 10000) as BucketIndexType, // FIXME!
                //         &color,
                //         &temp_colors_buffer,
                //         &QueryColoredCounters {
                //             // query_index: entry.0.query_index,
                //             // counter: entry.0.counter,
                //             // _phantom: PhantomData,
                //         },
                //     );
                // }
            }

            thread_buffer.put_back(colored_buckets_writer.finalize().0);
        });

        correct_color_buckets.finalize()
    }
}
