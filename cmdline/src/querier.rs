use crate::query_pipeline::QueryPipeline;
use crate::QuerierStartingStep;
use colors::colors_manager::ColorsManager;
use colors::DefaultColorsSerializer;
use hashes::{HashFunctionFactory, MinimizerHashFunctionFactory};
use io::{compute_buckets_log_from_input_files, generate_bucket_names};
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

    let buckets_count_log = buckets_count_log.unwrap_or_else(|| {
        compute_buckets_log_from_input_files(&[graph_input.clone(), query_input.clone()])
    });
    let buckets_count = 1 << buckets_count_log;

    let (buckets, counters) = if step <= QuerierStartingStep::MinimizerBucketing {
        QueryPipeline::minimizer_bucketing::<BucketingHash, QuerierColorsManager>(
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
            generate_bucket_names(temp_dir.join("bucket"), buckets_count, None),
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
        generate_bucket_names(temp_dir.join("counters"), buckets_count, Some("tmp"))
    };

    let colored_buckets_prefix = temp_dir.join("color_counters");

    let colored_buckets = QueryPipeline::counters_sorting::<QuerierColorsManager>(
        k,
        query_input.clone(),
        counters_buckets,
        colored_buckets_prefix,
        output_file.clone(),
    );

    if QuerierColorsManager::COLORS_ENABLED {
        let colormap_file = graph_input.with_extension("colors.dat");
        let remapped_query_color_buckets = QueryPipeline::colormap_reading::<DefaultColorsSerializer>(
            colormap_file,
            colored_buckets,
            temp_dir,
        );

        QueryPipeline::colored_query_output::<QuerierColorsManager>(
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
