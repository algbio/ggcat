use crate::pipeline::dumper_minimizer_bucketing::minimizer_bucketing;
use colors::DefaultColorsSerializer;
use colors::bundles::graph_querying::ColorBundleGraphQuerying;
use colors::colors_manager::{ColorMapReader, ColorsManager, ColorsMergeManager};
use config::{
    ColorIndexType, INTERMEDIATE_COMPRESSION_LEVEL_FAST, INTERMEDIATE_COMPRESSION_LEVEL_SLOW,
};
use io::compute_stats_from_input_blocks;
use io::sequences_stream::general::GeneralSequenceBlockData;
use parallel_processor::buckets::{BucketsCount, ExtraBuckets};
use parallel_processor::memory_fs::MemoryFs;
use parallel_processor::phase_times_monitor::PHASES_TIMES_MONITOR;
use pipeline::dumper_colormap_querying::colormap_query;
use pipeline::dumper_colormap_reading::colormap_reading;
use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering;

mod pipeline;

#[derive(Copy, Clone, Debug, PartialOrd, PartialEq)]
pub enum QuerierStartingStep {
    MinimizerBucketing = 0,
    KmersCounting = 1,
    CountersSorting = 2,
    ColorMapReading = 3,
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum ColoredQueryOutputFormat {
    JsonLinesWithNumbers,
    JsonLinesWithNames,
}

pub fn dump_unitigs(
    k: usize,
    m: usize,
    graph_input: impl AsRef<Path>,
    temp_dir: Option<PathBuf>,
    buckets_count_log: Option<usize>,
    threads_count: usize,
    single_thread_output_function: bool,
    default_compression_level: Option<u32>,
    output_function: impl Fn(&[u8], &[ColorIndexType], bool) + Send + Sync,
) -> anyhow::Result<()> {
    let temp_dir = temp_dir.unwrap_or(PathBuf::new());

    PHASES_TIMES_MONITOR.write().init();

    let color_map =
        <ColorBundleGraphQuerying as ColorsManager>::ColorsMergeManagerType::open_colors_table(
            graph_input.as_ref().with_extension("colors.dat"),
        )?;

    // TODO: Support GFA input
    let file_stats = compute_stats_from_input_blocks(&[GeneralSequenceBlockData::FASTA((
        graph_input.as_ref().to_path_buf(),
        None,
    ))])?;

    let buckets_count_log = buckets_count_log.unwrap_or_else(|| file_stats.best_buckets_count_log);

    if let Some(default_compression_level) = default_compression_level {
        INTERMEDIATE_COMPRESSION_LEVEL_SLOW.store(default_compression_level, Ordering::Relaxed);
        INTERMEDIATE_COMPRESSION_LEVEL_FAST.store(default_compression_level, Ordering::Relaxed);
    }

    let reorganized_unitigs = minimizer_bucketing::<ColorBundleGraphQuerying>(
        graph_input.as_ref().to_path_buf(),
        BucketsCount::new(buckets_count_log, ExtraBuckets::None),
        threads_count,
        temp_dir.as_path(),
        k,
        m,
        color_map.colors_subsets_count(),
    );

    MemoryFs::flush_all_to_disk();
    MemoryFs::free_memory();

    let colormap_file = graph_input.as_ref().with_extension("colors.dat");
    colormap_reading::<ColorBundleGraphQuerying, DefaultColorsSerializer>(
        k,
        colormap_file,
        reorganized_unitigs,
        single_thread_output_function,
        output_function,
    )
}

pub fn dump_colormap_query(
    colormap_file: PathBuf,
    color_subsets: Vec<ColorIndexType>,
    single_thread_output_function: bool,
    output_function: impl Fn(ColorIndexType, &[ColorIndexType]) + Send + Sync,
) -> anyhow::Result<()> {
    PHASES_TIMES_MONITOR.write().init();

    colormap_query::<ColorBundleGraphQuerying, DefaultColorsSerializer>(
        colormap_file,
        color_subsets,
        single_thread_output_function,
        output_function,
    )
}
