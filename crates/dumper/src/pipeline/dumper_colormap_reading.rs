use crate::pipeline::dumper_minimizer_bucketing::DumperKmersReferenceData;
use colors::colors_manager::ColorsManager;
use colors::storage::deserializer::ColorsDeserializer;
use colors::storage::ColorsSerializerTrait;
use config::{
    get_compression_level_info, get_memory_mode, BucketIndexType, ColorIndexType, SwapPriority,
    DEFAULT_PER_CPU_BUFFER_SIZE, DEFAULT_PREFETCH_AMOUNT, KEEP_FILES,
    MINIMIZER_BUCKETS_CHECKPOINT_SIZE, QUERIES_COUNT_MIN_BATCH,
};
use io::concurrent::temp_reads::creads_utils::CompressedReadsBucketHelper;
use parallel_processor::buckets::concurrent::{BucketsThreadBuffer, BucketsThreadDispatcher};
use parallel_processor::buckets::readers::compressed_binary_reader::CompressedBinaryReader;
use parallel_processor::buckets::readers::BucketReader;
use parallel_processor::buckets::writers::compressed_binary_writer::CompressedBinaryWriter;
use parallel_processor::buckets::MultiThreadBuckets;
use parallel_processor::fast_smart_bucket_sort::{fast_smart_radix_sort, SortKey};
use parallel_processor::memory_fs::RemoveFileMode;
use parallel_processor::phase_times_monitor::PHASES_TIMES_MONITOR;
use parallel_processor::utils::scoped_thread_local::ScopedThreadLocal;
use rayon::prelude::*;
use std::path::PathBuf;
use std::sync::atomic::Ordering;
use std::sync::Arc;

pub fn colormap_reading<CX: ColorsManager, CD: ColorsSerializerTrait>(
    colormap_file: PathBuf,
    colored_unitigs_buckets: Vec<PathBuf>,
    temp_dir: PathBuf,
    queries_count: u64,
    output_function: impl Fn(&[u8]),
) -> Vec<PathBuf> {
    PHASES_TIMES_MONITOR
        .write()
        .start_phase("phase: colormap reading".to_string());

    let buckets_count = colored_unitigs_buckets.len();
    let buckets_prefix_path = temp_dir.join("query_colors");

    let correct_color_buckets = Arc::new(MultiThreadBuckets::<CompressedBinaryWriter>::new(
        buckets_count,
        buckets_prefix_path,
        &(
            get_memory_mode(SwapPriority::MinimizerBuckets),
            MINIMIZER_BUCKETS_CHECKPOINT_SIZE,
            get_compression_level_info(),
        ),
    ));

    let tlocal_colormap_decoder =
        ScopedThreadLocal::new(move || ColorsDeserializer::<CD>::new(&colormap_file, false));

    colored_unitigs_buckets.par_iter().for_each(|input| {
        let mut colormap_decoder = tlocal_colormap_decoder.get();

        let mut temp_bases = Vec::new();
        let mut temp_sequences = Vec::new();

        CompressedBinaryReader::new(
            input,
            RemoveFileMode::Remove {
                remove_fs: !KEEP_FILES.load(Ordering::Relaxed),
            },
            DEFAULT_PREFETCH_AMOUNT,
        )
        .decode_all_bucket_items::<CompressedReadsBucketHelper<
            DumperKmersReferenceData<SingleKmerColor<CX>>,
            typenum::consts::U0,
            false,
        >, _>((), &mut (), |(_, _, color_extra, read), _| {
            let new_read = read.copy_to_buffer(&mut temp_sequences);
            temp_sequences.push((new_read, color_extra));
        });

        struct ColoredUnitigsCompare;
        impl SortKey<(CompressedReadIndipendent, ColorIndexType)> for ColoredUnitigsCompare {
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

        fast_smart_radix_sort::<_, ColoredUnitigsCompare, false>(&mut counters_vec[..]);

        for queries_by_color in counters_vec.group_by_mut(|a, b| a.1 == b.1) {
            let color = queries_by_color[0].1;
            temp_colors_buffer.clear();
            colormap_decoder.get_color_mappings(color, &mut temp_colors_buffer);

            todo!("Call the callback!")
        }
        thread_buffer.put_back(colored_buckets_writer.finalize().0);
    });

    correct_color_buckets.finalize()
}
