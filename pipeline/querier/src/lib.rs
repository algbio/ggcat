#![allow(warnings)] // FIXME: Remove
#![feature(generic_associated_types)]
#![feature(slice_group_by)]
#![feature(int_log)]
#![feature(int_roundings)]

use crate::pipeline::colored_query_output::colored_query_output;
use crate::pipeline::colormap_reading::colormap_reading;
use crate::pipeline::counters_sorting::counters_sorting;
use crate::pipeline::parallel_kmers_query::parallel_kmers_counting;
use crate::pipeline::querier_minimizer_bucketing::minimizer_bucketing;
use ::static_dispatch::static_dispatch;
use colors::colors_manager::{ColorMapReader, ColorsManager, ColorsMergeManager};
use colors::DefaultColorsSerializer;
use hashes::{HashFunctionFactory, MinimizerHashFunctionFactory};
use io::{compute_buckets_log_from_input_files, generate_bucket_names};
use parallel_processor::phase_times_monitor::PHASES_TIMES_MONITOR;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::PathBuf;

mod pipeline;
mod sparse_fenwick;
mod structs;

#[derive(Debug, PartialOrd, PartialEq)]
pub enum QuerierStartingStep {
    MinimizerBucketing = 0,
    KmersCounting = 1,
    CountersSorting = 2,
    ColorMapReading = 3,
}

#[static_dispatch(BucketingHash = [
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
], QuerierColorsManager = [
    #[cfg(not(feature = "devel-build"))] colors::bundles::graph_querying::ColorBundleGraphQuerying,
    colors::non_colored::NonColoredManager,
])]
pub fn run_query<
    BucketingHash: MinimizerHashFunctionFactory,
    MergingHash: HashFunctionFactory,
    QuerierColorsManager: ColorsManager,
>(
    k: usize,
    m: usize,
    step: QuerierStartingStep,
    graph_input: PathBuf,
    query_input: PathBuf,
    output_file: PathBuf,
    temp_dir: PathBuf,
    buckets_count_log: Option<usize>,
    threads_count: usize,
) {
    PHASES_TIMES_MONITOR.write().init();

    let color_map = QuerierColorsManager::ColorsMergeManagerType::<MergingHash>::open_colors_table(
        graph_input.with_extension("colors.dat"),
    );

    let buckets_count_log = buckets_count_log.unwrap_or_else(|| {
        compute_buckets_log_from_input_files(&[graph_input.clone(), query_input.clone()])
    });
    let buckets_count = 1 << buckets_count_log;

    let ((buckets, counters), queries_count) = if step <= QuerierStartingStep::MinimizerBucketing {
        minimizer_bucketing::<BucketingHash, QuerierColorsManager>(
            graph_input.clone(),
            query_input.clone(),
            temp_dir.as_path(),
            buckets_count,
            threads_count,
            k,
            m,
        )
    } else {
        (
            (
                generate_bucket_names(temp_dir.join("bucket"), buckets_count, None),
                temp_dir.join("buckets-counters.dat"),
            ),
            {
                let queries_count = BufReader::new(File::open(&query_input).unwrap())
                    .lines()
                    .count() as u64
                    / 2;
                queries_count
            },
        )
    };

    let counters_buckets = if step <= QuerierStartingStep::KmersCounting {
        parallel_kmers_counting::<BucketingHash, MergingHash, QuerierColorsManager, _>(
            buckets,
            counters,
            buckets_count,
            temp_dir.as_path(),
            k,
            m,
            threads_count,
        )
    } else {
        generate_bucket_names(temp_dir.join("counters"), buckets_count, None)
    };

    let colored_buckets_prefix = temp_dir.join("color_counters");

    let colored_buckets = if step <= QuerierStartingStep::CountersSorting {
        counters_sorting::<QuerierColorsManager>(
            k,
            query_input.clone(),
            counters_buckets,
            colored_buckets_prefix,
            color_map.colors_count(),
            output_file.clone(),
        )
    } else {
        generate_bucket_names(colored_buckets_prefix, buckets_count, None)
    };

    if QuerierColorsManager::COLORS_ENABLED {
        let colormap_file = graph_input.with_extension("colors.dat");
        let remapped_query_color_buckets = colormap_reading::<DefaultColorsSerializer>(
            colormap_file,
            colored_buckets,
            temp_dir,
            queries_count,
        );

        colored_query_output::<QuerierColorsManager>(
            query_input,
            remapped_query_color_buckets,
            output_file.clone(),
        );
    }

    PHASES_TIMES_MONITOR
        .write()
        .print_stats("Query completed.".to_string());

    println!("Final output saved to: {}", output_file.display());
}
