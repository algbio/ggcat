use crate::pipeline::dumper_minimizer_bucketing::minimizer_bucketing;
use colors::bundles::graph_querying::ColorBundleGraphQuerying;
use colors::colors_manager::{ColorMapReader, ColorsManager, ColorsMergeManager};
use colors::DefaultColorsSerializer;
use config::{
    ColorIndexType, INTERMEDIATE_COMPRESSION_LEVEL_FAST, INTERMEDIATE_COMPRESSION_LEVEL_SLOW,
};
use io::compute_stats_from_input_blocks;
use io::sequences_stream::general::GeneralSequenceBlockData;
use parallel_processor::memory_fs::MemoryFs;
use parallel_processor::phase_times_monitor::PHASES_TIMES_MONITOR;
use pipeline::dumper_colormap_reading::colormap_reading;
use std::fs::remove_file;
use std::path::PathBuf;
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
    graph_input: PathBuf,
    temp_dir: Option<PathBuf>,
    buckets_count_log: Option<usize>,
    threads_count: usize,
    single_thread_output_function: bool,
    default_compression_level: Option<u32>,
    output_function: impl Fn(&[u8], &[ColorIndexType], bool) + Send + Sync,
) {
    let temp_dir = temp_dir.unwrap_or(PathBuf::new());

    PHASES_TIMES_MONITOR.write().init();

    let color_map = <ColorBundleGraphQuerying as ColorsManager>::ColorsMergeManagerType::<
        hashes::cn_nthash::CanonicalNtHashIteratorFactory,
        hashes::cn_rkhash::u128::CanonicalRabinKarpHashFactory,
    >::open_colors_table(graph_input.with_extension("colors.dat"));

    // TODO: Support GFA input
    let file_stats = compute_stats_from_input_blocks(&[GeneralSequenceBlockData::FASTA((
        graph_input.clone(),
        None,
    ))]);

    let buckets_count_log = buckets_count_log.unwrap_or_else(|| file_stats.best_buckets_count_log);

    if let Some(default_compression_level) = default_compression_level {
        INTERMEDIATE_COMPRESSION_LEVEL_SLOW.store(default_compression_level, Ordering::Relaxed);
        INTERMEDIATE_COMPRESSION_LEVEL_FAST.store(default_compression_level, Ordering::Relaxed);
    }

    let buckets_count = 1 << buckets_count_log;

    let (reorganized_unitigs, buckets_stats) = minimizer_bucketing::<ColorBundleGraphQuerying>(
        graph_input.clone(),
        buckets_count,
        threads_count,
        temp_dir.as_path(),
        k,
        m,
        color_map.colors_subsets_count(),
    );
    let _ = remove_file(buckets_stats);

    MemoryFs::flush_all_to_disk();
    MemoryFs::free_memory();

    let colormap_file = graph_input.with_extension("colors.dat");
    colormap_reading::<ColorBundleGraphQuerying, DefaultColorsSerializer>(
        colormap_file,
        reorganized_unitigs,
        single_thread_output_function,
        output_function,
    );
}
