use crate::pipeline::counters_sorting::{CounterEntry, CounterEntrySerializer};
use crate::structs::query_colored_counters::{
    ColorsRange, QueryColorDesc, QueryColoredCounters, QueryColoredCountersSerializer,
};
use colors::storage::deserializer::ColorsDeserializer;
use colors::storage::ColorsSerializerTrait;
use config::{
    get_compression_level_info, get_memory_mode, BucketIndexType, ColorIndexType, SwapPriority,
    DEFAULT_PER_CPU_BUFFER_SIZE, DEFAULT_PREFETCH_AMOUNT, KEEP_FILES,
    MINIMIZER_BUCKETS_CHECKPOINT_SIZE, QUERIES_COUNT_MIN_BATCH,
};
use nightly_quirks::prelude::*;
use nightly_quirks::slice_group_by::SliceGroupBy;
use parallel_processor::buckets::concurrent::{BucketsThreadBuffer, BucketsThreadDispatcher};
use parallel_processor::buckets::readers::compressed_binary_reader::CompressedBinaryReader;
use parallel_processor::buckets::readers::BucketReader;
use parallel_processor::buckets::writers::compressed_binary_writer::CompressedBinaryWriter;
use parallel_processor::buckets::{MultiThreadBuckets, SingleBucket};
use parallel_processor::fast_smart_bucket_sort::{fast_smart_radix_sort, SortKey};
use parallel_processor::memory_fs::RemoveFileMode;
use parallel_processor::phase_times_monitor::PHASES_TIMES_MONITOR;
use parallel_processor::utils::scoped_thread_local::ScopedThreadLocal;
use rayon::prelude::*;
use std::path::PathBuf;
use std::sync::atomic::Ordering;
use std::sync::Arc;

pub fn colormap_reading<CD: ColorsSerializerTrait>(
    colormap_file: PathBuf,
    colored_query_buckets: Vec<SingleBucket>,
    temp_dir: PathBuf,
    queries_count: u64,
) -> anyhow::Result<Vec<SingleBucket>> {
    PHASES_TIMES_MONITOR
        .write()
        .start_phase("phase: colormap reading".to_string());

    let buckets_count = colored_query_buckets.len();
    let buckets_prefix_path = temp_dir.join("query_colors");

    let correct_color_buckets = Arc::new(MultiThreadBuckets::<CompressedBinaryWriter>::new(
        buckets_count,
        buckets_prefix_path,
        None,
        &(
            get_memory_mode(SwapPriority::MinimizerBuckets),
            MINIMIZER_BUCKETS_CHECKPOINT_SIZE,
            get_compression_level_info(),
        ),
    ));

    let thread_buffers = ScopedThreadLocal::new(move || {
        BucketsThreadBuffer::new(DEFAULT_PER_CPU_BUFFER_SIZE, buckets_count)
    });

    // Try to build a color deserializer to check colormap correctness
    let _ = ColorsDeserializer::<CD>::new(&colormap_file, false)?;

    let tlocal_colormap_decoder = ScopedThreadLocal::new(move || {
        ColorsDeserializer::<CD>::new(&colormap_file, false).unwrap()
    });

    colored_query_buckets.par_iter().for_each(|input| {
        let mut colormap_decoder = tlocal_colormap_decoder.get();
        let mut temp_colors_buffer = Vec::new();
        let mut temp_queries_buffer = Vec::new();
        let mut temp_encoded_buffer = Vec::new();

        let mut thread_buffer = thread_buffers.get();
        let mut colored_buckets_writer =
            BucketsThreadDispatcher::<_, QueryColoredCountersSerializer>::new(
                &correct_color_buckets,
                thread_buffer.take(),
            );

        let mut counters_vec: Vec<(CounterEntry<ColorIndexType>, ColorIndexType)> = Vec::new();
        CompressedBinaryReader::new(
            &input.path,
            RemoveFileMode::Remove {
                remove_fs: !KEEP_FILES.load(Ordering::Relaxed),
            },
            DEFAULT_PREFETCH_AMOUNT,
        )
        .decode_all_bucket_items::<CounterEntrySerializer<ColorIndexType>, _>(
            (),
            &mut (),
            |h, _| {
                counters_vec.push(h);
            },
        );

        struct CountersCompare;
        impl SortKey<(CounterEntry<ColorIndexType>, ColorIndexType)> for CountersCompare {
            type KeyType = u32;
            const KEY_BITS: usize = std::mem::size_of::<u32>() * 8;

            fn compare(
                left: &(CounterEntry<ColorIndexType>, ColorIndexType),
                right: &(CounterEntry<ColorIndexType>, ColorIndexType),
            ) -> std::cmp::Ordering {
                left.1.cmp(&right.1)
            }

            fn get_shifted(value: &(CounterEntry<ColorIndexType>, ColorIndexType), rhs: u8) -> u8 {
                (value.1 >> rhs) as u8
            }
        }

        fast_smart_radix_sort::<_, CountersCompare, false>(&mut counters_vec[..]);

        for queries_by_color in counters_vec.nq_group_by_mut(|a, b| a.1 == b.1) {
            let color = queries_by_color[0].1;
            temp_colors_buffer.clear();
            colormap_decoder.get_color_mappings(color, &mut temp_colors_buffer);

            {
                temp_encoded_buffer.clear();
                let mut range_start = ColorIndexType::MAX;
                let mut range_end = ColorIndexType::MAX;

                for color in temp_colors_buffer.iter().copied() {
                    // Different range
                    if color != range_end {
                        if range_start != ColorIndexType::MAX {
                            ColorsRange::Range(range_start..range_end)
                                .write_to_vec(&mut temp_encoded_buffer);
                        }
                        range_start = color;
                    }
                    range_end = color + 1;
                }
                ColorsRange::Range(range_start..range_end).write_to_vec(&mut temp_encoded_buffer);
            }

            {
                temp_queries_buffer.clear();
                temp_queries_buffer.extend(queries_by_color.iter().map(|q| QueryColorDesc {
                    query_index: q.0.query_index,
                    count: q.0.counter,
                }));

                temp_queries_buffer.sort_unstable_by_key(|c| c.query_index);
            }

            // ggcat_logging::info!(
            //     " Queries: {:?} Colors: {:?} Compressed: {:?}",
            //     queries_by_color.iter().map(|q| &q.0).collect::<Vec<_>>(),
            //     temp_colors_buffer,
            //     temp_encoded_buffer
            // );

            let rounded_queries_count =
                (queries_count + 1).nq_div_ceil(QUERIES_COUNT_MIN_BATCH) * QUERIES_COUNT_MIN_BATCH;

            let get_query_bucket = |query_index: u64| {
                ((query_index - 1) * (buckets_count as u64) / rounded_queries_count)
                    as BucketIndexType
            };

            for entries in temp_queries_buffer.nq_group_by(|a, b| {
                get_query_bucket(a.query_index) == get_query_bucket(b.query_index)
            }) {
                let bucket = get_query_bucket(entries[0].query_index);
                colored_buckets_writer.add_element(
                    bucket,
                    &(),
                    &QueryColoredCounters {
                        queries: entries,
                        colors: &temp_encoded_buffer,
                    },
                );
            }
        }
        thread_buffer.put_back(colored_buckets_writer.finalize().0);
    });

    Ok(correct_color_buckets.finalize_single())
}
