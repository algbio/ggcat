use crate::colors::colors_manager::ColorsManager;
use crate::hashes::HashFunctionFactory;
use crate::io::{DataReader, DataWriter};
use crate::query_pipeline::QueryPipeline;
use crate::utils::debug_utils::debug_print;
use crate::utils::Utils;
use crate::{QuerierStartingStep, KEEP_FILES, SAVE_MEMORY};
use itertools::Itertools;
use parallel_processor::phase_times_monitor::PHASES_TIMES_MONITOR;
use parking_lot::Mutex;
use std::fs::remove_file;
use std::path::PathBuf;
use std::sync::atomic::Ordering;
use std::thread;

pub fn run_query<
    BucketingHash: HashFunctionFactory,
    MergingHash: HashFunctionFactory,
    AssemblerColorsManager: ColorsManager,
    Reader: DataReader,
    Writer: DataWriter,
    const BUCKETS_COUNT: usize,
>(
    k: usize,
    m: usize,
    step: QuerierStartingStep,
    graph_input: PathBuf,
    query_input: PathBuf,
    output_file: PathBuf,
    temp_dir: PathBuf,
    threads_count: usize,
) {
    PHASES_TIMES_MONITOR.write().init();

    // let global_colors_table = AssemblerColorsManager::create_colors_table(
    //     output_file.with_extension("colors.dat"),
    //     color_names,
    // );

    let buckets = if step <= QuerierStartingStep::MinimizerBucketing {
        QueryPipeline::minimizer_bucketing::<BucketingHash, AssemblerColorsManager>(
            graph_input,
            query_input.clone(),
            temp_dir.as_path(),
            BUCKETS_COUNT,
            threads_count,
            k,
            m,
        )
    } else {
        Utils::generate_bucket_names(temp_dir.join("bucket"), BUCKETS_COUNT, Some("tmp"))
    };

    let counters_buckets = if step <= QuerierStartingStep::KmersCounting {
        QueryPipeline::parallel_kmers_counting::<
            BucketingHash,
            MergingHash,
            AssemblerColorsManager,
            _,
            Reader,
        >(
            buckets,
            BUCKETS_COUNT,
            temp_dir.as_path(),
            k,
            m,
            threads_count,
            SAVE_MEMORY.load(Ordering::Relaxed),
        )
    } else {
        Utils::generate_bucket_names(temp_dir.join("counters"), BUCKETS_COUNT, Some("tmp"))
    };

    QueryPipeline::counters_sorting(k, query_input, counters_buckets, output_file.clone());

    PHASES_TIMES_MONITOR
        .write()
        .print_stats("Compacted De Brujin graph construction completed.".to_string());

    println!("Final output saved to: {}", output_file.display());
}
