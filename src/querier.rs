use crate::colors::colors_manager::ColorsManager;
use crate::hashes::{HashFunctionFactory, MinimizerHashFunctionFactory};
use crate::query_pipeline::QueryPipeline;
use crate::utils::Utils;
use crate::QuerierStartingStep;
use parallel_processor::phase_times_monitor::PHASES_TIMES_MONITOR;
use std::path::PathBuf;

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

    // let global_colors_table = QuerierColorsManager::create_colors_table(
    //     output_file.with_extension("colors.dat"),
    //     color_names,
    // );
    let buckets_count_log = buckets_count_log.unwrap_or_else(|| {
        Utils::compute_buckets_log_from_input_files(&[graph_input.clone(), query_input.clone()])
    });
    let buckets_count = 1 << buckets_count_log;

    let (buckets, counters) = if step <= QuerierStartingStep::MinimizerBucketing {
        QueryPipeline::minimizer_bucketing::<BucketingHash, QuerierColorsManager>(
            graph_input,
            query_input.clone(),
            temp_dir.as_path(),
            buckets_count,
            threads_count,
            k,
            m,
        )
    } else {
        (
            Utils::generate_bucket_names(temp_dir.join("bucket"), buckets_count, None),
            temp_dir.join("buckets-counters.dat"),
        )
    };

    let counters_buckets = if step <= QuerierStartingStep::KmersCounting {
        QueryPipeline::parallel_kmers_counting::<BucketingHash, MergingHash, QuerierColorsManager, _>(
            buckets,
            counters,
            buckets_count,
            temp_dir.as_path(),
            k,
            m,
            threads_count,
        )
    } else {
        Utils::generate_bucket_names(temp_dir.join("counters"), buckets_count, Some("tmp"))
    };

    QueryPipeline::counters_sorting::<QuerierColorsManager>(
        k,
        query_input,
        counters_buckets,
        output_file.clone(),
    );

    PHASES_TIMES_MONITOR
        .write()
        .print_stats("Compacted De Brujin graph construction completed.".to_string());

    println!("Final output saved to: {}", output_file.display());
}
