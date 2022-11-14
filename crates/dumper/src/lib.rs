#![feature(slice_group_by)]
#![feature(int_log)]
#![feature(int_roundings)]

use crate::pipeline::dumper_minimizer_bucketing::minimizer_bucketing;
use ::dynamic_dispatch::dynamic_dispatch;
use colors::colors_manager::{ColorMapReader, ColorsManager, ColorsMergeManager};
use colors::DefaultColorsSerializer;
use config::{INTERMEDIATE_COMPRESSION_LEVEL_FAST, INTERMEDIATE_COMPRESSION_LEVEL_SLOW};
use hashes::{HashFunctionFactory, MinimizerHashFunctionFactory};
use io::sequences_reader::SequencesReader;
use io::sequences_stream::general::GeneralSequenceBlockData;
use io::{compute_stats_from_input_blocks, generate_bucket_names};
use parallel_processor::phase_times_monitor::PHASES_TIMES_MONITOR;
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

#[dynamic_dispatch(BucketingHash = [
    hashes::cn_nthash::CanonicalNtHashIteratorFactory,
    #[cfg(not(feature = "devel-build"))]  hashes::fw_nthash::ForwardNtHashIteratorFactory
], MergingHash = [
    #[cfg(not(feature = "devel-build"))] hashes::fw_seqhash::u16::ForwardSeqHashFactory,
    #[cfg(not(feature = "devel-build"))] hashes::fw_seqhash::u32::ForwardSeqHashFactory,
    #[cfg(not(feature = "devel-build"))] hashes::fw_seqhash::u64::ForwardSeqHashFactory,
    #[cfg(not(feature = "devel-build"))] hashes::fw_seqhash::u128::ForwardSeqHashFactory,
    #[cfg(not(feature = "devel-build"))] hashes::fw_rkhash::u32::ForwardRabinKarpHashFactory,
    #[cfg(not(feature = "devel-build"))] hashes::fw_rkhash::u64::ForwardRabinKarpHashFactory,
    #[cfg(not(feature = "devel-build"))] hashes::fw_rkhash::u128::ForwardRabinKarpHashFactory,
    #[cfg(not(feature = "devel-build"))] hashes::cn_seqhash::u16::CanonicalSeqHashFactory,
    #[cfg(not(feature = "devel-build"))] hashes::cn_seqhash::u32::CanonicalSeqHashFactory,
    hashes::cn_seqhash::u64::CanonicalSeqHashFactory,
    #[cfg(not(feature = "devel-build"))] hashes::cn_seqhash::u128::CanonicalSeqHashFactory,
    #[cfg(not(feature = "devel-build"))] hashes::cn_rkhash::u32::CanonicalRabinKarpHashFactory,
    #[cfg(not(feature = "devel-build"))] hashes::cn_rkhash::u64::CanonicalRabinKarpHashFactory,
    #[cfg(not(feature = "devel-build"))] hashes::cn_rkhash::u128::CanonicalRabinKarpHashFactory,
], DumperColorsManager = [
    #[cfg(not(feature = "devel-build"))] colors::bundles::graph_querying::ColorBundleGraphQuerying,
    colors::non_colored::NonColoredManager,
])]
pub fn dump_unitigs<
    BucketingHash: MinimizerHashFunctionFactory,
    MergingHash: HashFunctionFactory,
    DumperColorsManager: ColorsManager,
>(
    k: usize,
    m: usize,
    graph_input: PathBuf,
    temp_dir: Option<PathBuf>,
    buckets_count_log: Option<usize>,
    threads_count: usize,
    default_compression_level: Option<u32>,
) {
    let temp_dir = temp_dir.unwrap_or(PathBuf::new());

    PHASES_TIMES_MONITOR.write().init();

    BucketingHash::initialize(k);
    MergingHash::initialize(k);

    let color_map = DumperColorsManager::ColorsMergeManagerType::<BucketingHash, MergingHash>::open_colors_table(
        graph_input.with_extension("colors.dat"),
    );

    // TODO: Support GFA input
    let file_stats =
        compute_stats_from_input_blocks(&[GeneralSequenceBlockData::FASTA(graph_input.clone())]);

    let buckets_count_log = buckets_count_log.unwrap_or_else(|| file_stats.best_buckets_count_log);

    if let Some(default_compression_level) = default_compression_level {
        INTERMEDIATE_COMPRESSION_LEVEL_SLOW.store(default_compression_level, Ordering::Relaxed);
        INTERMEDIATE_COMPRESSION_LEVEL_FAST.store(default_compression_level, Ordering::Relaxed);
    }

    let buckets_count = 1 << buckets_count_log;

    minimizer_bucketing::<DumperColorsManager>(
        graph_input.clone(),
        buckets_count,
        threads_count,
        temp_dir.as_path(),
        k,
        m,
        color_map.colors_subsets_count(),
    );

    // let counters_buckets = if step <= QuerierStartingStep::KmersCounting {
    //     parallel_kmers_counting::<BucketingHash, MergingHash, DumperColorsManager, _>(
    //         buckets,
    //         counters,
    //         buckets_count,
    //         temp_dir.as_path(),
    //         k,
    //         m,
    //         threads_count,
    //     )
    // } else {
    //     generate_bucket_names(temp_dir.join("counters"), buckets_count, None)
    // };
    //
    // let colored_buckets_prefix = temp_dir.join("color_counters");
    //
    // let query_kmers_count = {
    //     let mut sequences_lengths = vec![];
    //     SequencesReader::new().process_file_extended(
    //         &query_input,
    //         |seq| {
    //             sequences_lengths.push((seq.seq.len() - k + 1) as u64);
    //         },
    //         None,
    //         false,
    //         false,
    //     );
    //     sequences_lengths
    // };
    //
    // let colored_buckets = if step <= QuerierStartingStep::CountersSorting {
    //     counters_sorting::<DumperColorsManager>(
    //         k,
    //         counters_buckets,
    //         colored_buckets_prefix,
    //         color_map.colors_subsets_count(),
    //         output_file_prefix.clone(),
    //         &query_kmers_count,
    //     )
    // } else {
    //     generate_bucket_names(colored_buckets_prefix, buckets_count, None)
    // };
    //
    // if DumperColorsManager::COLORS_ENABLED {
    //     let colormap_file = graph_input.with_extension("colors.dat");
    //     let remapped_query_color_buckets = colormap_reading::<DefaultColorsSerializer>(
    //         colormap_file,
    //         colored_buckets,
    //         temp_dir.clone(),
    //         queries_count,
    //     );
    //
    //     colored_query_output::<BucketingHash, MergingHash, DumperColorsManager>(
    //         &color_map,
    //         remapped_query_color_buckets,
    //         output_file_prefix.clone(),
    //         temp_dir,
    //         &query_kmers_count,
    //         colored_query_output_format,
    //     );
    // }
}
