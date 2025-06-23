use crate::pipeline::colored_query_output::colored_query_output;
use crate::pipeline::colormap_reading::colormap_reading;
use crate::pipeline::counters_sorting::counters_sorting;
use crate::pipeline::parallel_kmers_query::parallel_kmers_counting;
use crate::pipeline::querier_minimizer_bucketing::minimizer_bucketing;
use ::dynamic_dispatch::dynamic_dispatch;
use colors::DefaultColorsSerializer;
use colors::colors_manager::{ColorMapReader, ColorsManager, ColorsMergeManager};
use config::{INTERMEDIATE_COMPRESSION_LEVEL_FAST, INTERMEDIATE_COMPRESSION_LEVEL_SLOW};
use hashes::HashFunctionFactory;
use hashes::default::MNHFactory;
use io::sequences_reader::SequencesReader;
use io::sequences_stream::general::GeneralSequenceBlockData;
use io::{compute_stats_from_input_blocks, generate_bucket_names};
use parallel_processor::buckets::{BucketsCount, ExtraBuckets};
use parallel_processor::phase_times_monitor::PHASES_TIMES_MONITOR;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::PathBuf;
use std::sync::atomic::Ordering;

mod pipeline;
mod structs;

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

#[dynamic_dispatch(MergingHash = [
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
pub fn run_query<MergingHash: HashFunctionFactory, QuerierColorsManager: ColorsManager>(
    k: usize,
    m: usize,
    step: QuerierStartingStep,
    graph_input: PathBuf,
    query_input: PathBuf,
    output_file_prefix: PathBuf,
    temp_dir: Option<PathBuf>,
    buckets_count_log: Option<usize>,
    threads_count: usize,
    default_compression_level: Option<u32>,
    colored_query_output_format: ColoredQueryOutputFormat,
) -> anyhow::Result<PathBuf> {
    let temp_dir = temp_dir.unwrap_or(PathBuf::new());

    PHASES_TIMES_MONITOR.write().init();

    MNHFactory::initialize(k);
    MergingHash::initialize(k);

    let color_map = QuerierColorsManager::ColorsMergeManagerType::open_colors_table(
        graph_input.with_extension("colors.dat"),
    )?;

    // TODO: Support GFA input
    let file_stats = compute_stats_from_input_blocks(&[
        GeneralSequenceBlockData::FASTA((graph_input.clone(), None)),
        GeneralSequenceBlockData::FASTA((query_input.clone(), None)),
    ])?;

    let buckets_count_log = buckets_count_log.unwrap_or_else(|| file_stats.best_buckets_count_log);
    let second_buckets_count_log = file_stats.best_second_buckets_count_log;

    if let Some(default_compression_level) = default_compression_level {
        INTERMEDIATE_COMPRESSION_LEVEL_SLOW.store(default_compression_level, Ordering::Relaxed);
        INTERMEDIATE_COMPRESSION_LEVEL_FAST.store(default_compression_level, Ordering::Relaxed);
    }

    let buckets_count = BucketsCount::new(buckets_count_log, ExtraBuckets::None);
    let second_buckets_count = BucketsCount::new(second_buckets_count_log, ExtraBuckets::None);

    let (buckets, queries_count) = if step <= QuerierStartingStep::MinimizerBucketing {
        minimizer_bucketing::<QuerierColorsManager>(
            graph_input.clone(),
            query_input.clone(),
            temp_dir.as_path(),
            buckets_count,
            second_buckets_count,
            threads_count,
            k,
            m,
        )
    } else {
        (
            generate_bucket_names(temp_dir.join("bucket"), buckets_count, None),
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
        parallel_kmers_counting::<MergingHash, QuerierColorsManager, _>(
            buckets,
            buckets_count,
            second_buckets_count,
            temp_dir.as_path(),
            k,
            m,
            threads_count,
            false,
        )
    } else {
        generate_bucket_names(temp_dir.join("counters"), buckets_count, None)
    };

    let colored_buckets_prefix = temp_dir.join("color_counters");

    let query_kmers_count = {
        let mut sequences_lengths = vec![];
        SequencesReader::new().process_file_extended(
            &query_input,
            |seq| {
                sequences_lengths.push((seq.seq.len().saturating_sub(k - 1)) as u64);
            },
            None,
            false,
            false,
        );
        sequences_lengths
    };

    let colored_buckets = if step <= QuerierStartingStep::CountersSorting {
        counters_sorting::<QuerierColorsManager>(
            k,
            counters_buckets,
            colored_buckets_prefix,
            color_map.colors_subsets_count(),
            output_file_prefix.clone(),
            &query_kmers_count,
        )
    } else {
        generate_bucket_names(colored_buckets_prefix, buckets_count, None)
    };

    if QuerierColorsManager::COLORS_ENABLED {
        let colormap_file = graph_input.with_extension("colors.dat");
        let remapped_query_color_buckets = colormap_reading::<DefaultColorsSerializer>(
            colormap_file,
            colored_buckets,
            temp_dir.clone(),
            queries_count,
        )?;

        colored_query_output::<MergingHash, QuerierColorsManager>(
            &color_map,
            remapped_query_color_buckets,
            output_file_prefix.clone(),
            temp_dir,
            &query_kmers_count,
            colored_query_output_format,
        )?;
    }

    PHASES_TIMES_MONITOR
        .write()
        .print_stats("Query completed.".to_string());

    let output_file_name = if output_file_prefix.extension().is_none() {
        if QuerierColorsManager::COLORS_ENABLED {
            output_file_prefix.with_extension("jsonl")
        } else {
            output_file_prefix.with_extension("csv")
        }
    } else {
        output_file_prefix
    };

    Ok(output_file_name)
}
