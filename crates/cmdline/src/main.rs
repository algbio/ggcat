extern crate alloc;
// #[cfg(test)]
// extern crate test;

// mod benchmarks;

use ::utils::compute_best_m;
use ahash::HashMap;
use clap::{ArgGroup, Parser, ValueEnum};
use colors::DefaultColorsSerializer;
use colors::colors_manager::ColorMapReader;
use colors::storage::deserializer::ColorsDeserializer;
use config::ColorIndexType;
use ggcat_api::{ExtraElaboration, GGCATConfig, GGCATInstance, GfaVersion};
use ggcat_logging::UnrecoverableErrorLogging;
use io::sequences_stream::general::GeneralSequenceBlockData;
use parallel_processor::memory_fs::MemoryFs;
use std::fs::File;
use std::io::BufRead;
use std::io::{BufReader, BufWriter, Write};
use std::panic;
use std::path::{Path, PathBuf};
use std::process::exit;
use std::sync::atomic::Ordering;
use std::time::Duration;

#[derive(Debug, Clone, PartialOrd, PartialEq, ValueEnum)]
pub enum AssemblerStartingStep {
    MinimizerBucketing = 0,
    KmersMerge = 1,
    UnitigsExtension = 2,
    MaximalUnitigsLinks = 3,
    FinalStep = 4,
}

#[derive(Debug, Clone, PartialOrd, PartialEq, ValueEnum)]
pub enum QuerierStartingStep {
    MinimizerBucketing = 0,
    KmersCounting = 1,
    CountersSorting = 2,
    ColorMapReading = 3,
}

#[derive(Copy, Clone, Debug, PartialOrd, PartialEq, ValueEnum)]
pub enum HashType {
    Auto = 0,
    SeqHash = 1,
    RabinKarp128 = 4,
}

#[derive(Parser, Debug)]
#[command(
    name = "ggcat",
    version,
    about = "Compacted and colored de Bruijn graph construction and querying"
)]
enum CliArgs {
    /// Build a new compacted graph
    Build(AssemblerArgs),
    /// Query a compacted graph
    Query(QueryArgs),
    /// Dump all color names from a colormap
    DumpColors(DumpColorsArgs),
    Matches(MatchesArgs),
    // Utils(CmdUtilsArgs),
}

#[derive(Parser, Debug)]
struct MatchesArgs {
    /// Input fasta file with associated colors file (in the same folder)
    input_file: PathBuf,

    /// Debug print matches of a color index (in hexadecimal)
    match_color: String,
}

#[derive(Parser, Debug)]
struct CommonArgs {
    /// The k-mers length
    #[arg(short, long = "kmer-length")]
    pub kmer_length: usize,

    /// Overrides the default m-mers (minimizers) length
    #[arg(long = "minimizer-length", help_heading = "Advanced Options")]
    pub minimizer_length: Option<usize>,

    /// Directory for temporary files
    #[arg(short = 't', long = "temp-dir", default_value = ".temp_files")]
    pub temp_dir: PathBuf,

    /// Keep intermediate temporary files for debugging purposes
    #[arg(long = "keep-temp-files", help_heading = "Advanced Options")]
    pub keep_temp_files: bool,

    #[arg(short = 'j', long, default_value = "16")]
    pub threads_count: usize,

    /// Hash type used to identify kmers
    #[arg(
        short = 'w',
        long,
        default_value = "auto",
        help_heading = "Advanced Options"
    )]
    pub hash_type: HashType,

    /// Treats reverse complementary kmers as different
    #[arg(short = 'f', long)]
    pub forward_only: bool,

    /// Maximum suggested memory usage (GB)
    /// The tool will try use only up to this GB of memory to store temporary files
    /// without writing to disk. This usage does not include the needed memory for the processing steps.
    /// GGCAT can allocate extra memory for files if the current memory is not enough to complete the current operation
    #[arg(short = 'm', long, default_value = "2")]
    pub memory: f64,

    /// Use all the given memory before writing to disk
    #[arg(short = 'p', long = "prefer-memory")]
    pub prefer_memory: bool,

    /// The log2 of the number of buckets
    #[arg(
        short = 'b',
        long = "buckets-count-log",
        help_heading = "Advanced Options"
    )]
    pub buckets_count_log: Option<usize>,

    /// The level of lz4 compression to be used for the intermediate files
    #[arg(
        long = "intermediate-compression-level",
        help_heading = "Advanced Options"
    )]
    pub intermediate_compression_level: Option<u32>,

    #[arg(hide = true, long = "only-bstats")]
    pub only_bstats: bool,
}

#[derive(Parser, Debug)]
#[command(group = ArgGroup::new("output-mode").required(false))]
struct AssemblerArgs {
    /// The input files
    pub input: Vec<PathBuf>,

    /// The lists of input files
    #[arg(short = 'l', long = "input-lists")]
    pub input_lists: Vec<PathBuf>,

    #[arg(short = 'o', long = "output-file", default_value = "output.fasta.lz4")]
    pub output_file: PathBuf,

    /// Enable colors
    #[arg(short, long)]
    pub colors: bool,

    /// The lists of input files with colors in format <COLOR_NAME><TAB><FILE_PATH>
    #[arg(short = 'd', long = "colored-input-lists")]
    pub colored_input_lists: Vec<PathBuf>,

    /// Minimum multiplicity required to keep a kmer
    #[arg(short = 's', long = "min-multiplicity", default_value = "2")]
    pub min_multiplicity: usize,

    /// Generate maximal unitigs connections references, in BCALM2 format L:<+/->:<other id>:<+/->
    #[arg(
        short = 'e',
        long = "generate-maximal-unitigs-links",
        group = "output-mode",
        help_heading = "Output mode"
    )]
    pub generate_maximal_unitigs_links: bool,

    /// Generate simplitigs instead of maximal unitigs
    #[arg(
        long = "simplitigs",
        alias = "fast-simplitigs",
        group = "output-mode",
        help_heading = "Output mode"
    )]
    pub simplitigs: bool,

    /// Generate eulertigs instead of maximal unitigs
    #[arg(
        long = "eulertigs",
        alias = "fast-eulertigs",
        group = "output-mode",
        help_heading = "Output mode"
    )]
    pub eulertigs: bool,

    /// Generate greedy matchtigs instead of maximal unitigs
    #[arg(
        long = "greedy-matchtigs",
        group = "output-mode",
        help_heading = "Output mode"
    )]
    pub greedy_matchtigs: bool,

    /// Generate eulertigs instead of maximal unitigs, old slower version
    #[arg(
        hide = true,
        long = "old-eulertigs",
        group = "output-mode",
        help_heading = "Output mode"
    )]
    pub old_eulertigs: bool,

    /// Generate old simplitigs instead of maximal unitigs, old slower version
    #[arg(
        hide = true,
        long = "old-simplitigs",
        group = "output-mode",
        help_heading = "Output mode"
    )]
    pub old_simplitigs: bool,

    #[command(flatten)]
    pub common_args: CommonArgs,

    /// Output the graph in GFA format v1
    #[arg(long = "gfa-v1", help_heading = "Output mode")]
    pub gfa_output_v1: bool,

    /// Output the graph in GFA format v2
    #[arg(long = "gfa-v2", help_heading = "Output mode")]
    pub gfa_output_v2: bool,

    #[arg(hide = true, long, default_value = "minimizer-bucketing")]
    pub step: AssemblerStartingStep,

    #[arg(hide = true, long = "last-step", default_value = "final-step")]
    pub last_step: AssemblerStartingStep,
}

#[derive(Parser, Debug)]
struct DumpColorsArgs {
    input_colormap: PathBuf,
    output_file: PathBuf,
}

/// Format of the queries output
#[derive(Copy, Clone, Debug, PartialEq, Eq, ValueEnum)]
pub enum ColoredQueryOutputFormat {
    JsonLinesWithNumbers,
    JsonLinesWithNames,
}

#[derive(Parser, Debug)]
struct QueryArgs {
    /// The input graph
    pub input_graph: PathBuf,

    /// The input query as a .fasta file
    pub input_query: PathBuf,

    /// Enable colors
    #[arg(short, long)]
    pub colors: bool,

    #[arg(short = 'o', long = "output-file-prefix", default_value = "output")]
    pub output_file_prefix: PathBuf,

    #[arg(long = "colored-query-output-format")]
    pub colored_query_output_format: Option<ColoredQueryOutputFormat>,

    #[arg(short = 'x', long, default_value = "MinimizerBucketing")]
    pub step: QuerierStartingStep,

    #[command(flatten)]
    pub common_args: CommonArgs,
}

// #[cfg(feature = "mem-analysis")]
// use parallel_processor::debug_allocator::{debug_print_allocations, DebugAllocator};
//
// #[cfg_attr(feature = "mem-analysis", global_allocator)]
// #[cfg(feature = "mem-analysis")]
// static DEBUG_ALLOCATOR: DebugAllocator = DebugAllocator::new();

fn initialize(args: &CommonArgs, out_file: &PathBuf) -> &'static GGCATInstance {
    let instance = GGCATInstance::create(GGCATConfig {
        temp_dir: Some(args.temp_dir.clone()),
        memory: args.memory,
        prefer_memory: args.prefer_memory,
        total_threads_count: args.threads_count,
        intermediate_compression_level: args.intermediate_compression_level,
        stats_file: Some(out_file.with_extension("stats.log")),
        messages_callback: None,
    });

    ggcat_api::debug::DEBUG_KEEP_FILES.store(args.keep_temp_files, Ordering::Relaxed);
    *ggcat_api::debug::BUCKETS_COUNT_LOG_FORCE.lock() = args.buckets_count_log;
    ggcat_api::debug::DEBUG_ONLY_BSTATS.store(args.only_bstats, Ordering::Relaxed);
    *ggcat_api::debug::DEBUG_HASH_TYPE.lock() = match args.hash_type {
        HashType::Auto => ggcat_api::HashType::Auto,
        HashType::SeqHash => ggcat_api::HashType::SeqHash,
        HashType::RabinKarp128 => ggcat_api::HashType::RabinKarp128,
    };

    println!(
        "Using m: {} with k: {}",
        args.minimizer_length
            .unwrap_or(compute_best_m(args.kmer_length)),
        args.kmer_length
    );

    // #[cfg(feature = "mem-analysis")]
    // debug_print_allocations("/tmp/allocations", Duration::from_secs(5));
    instance.unwrap()
}

fn convert_assembler_step(step: AssemblerStartingStep) -> utils::assembler_phases::AssemblerPhase {
    match step {
        AssemblerStartingStep::MinimizerBucketing => {
            utils::assembler_phases::AssemblerPhase::MinimizerBucketing
        }
        AssemblerStartingStep::KmersMerge => utils::assembler_phases::AssemblerPhase::KmersMerge,
        AssemblerStartingStep::UnitigsExtension => {
            utils::assembler_phases::AssemblerPhase::UnitigsExtension
        }
        AssemblerStartingStep::MaximalUnitigsLinks => {
            utils::assembler_phases::AssemblerPhase::MaximalUnitigsLinks
        }
        AssemblerStartingStep::FinalStep => utils::assembler_phases::AssemblerPhase::FinalStep,
    }
}

fn run_assembler_from_args(instance: &GGCATInstance, args: AssemblerArgs) {
    let mut inputs: Vec<_> = args.input.iter().cloned().map(|f| (f, None)).collect();

    if (args.input_lists.len() > 0 || args.input.len() > 0) && args.colored_input_lists.len() > 0 {
        println!("Cannot specify both colored input lists and other files/lists");
        exit(1);
    }

    if args.gfa_output_v1 && args.gfa_output_v2 {
        println!("Cannot specify both GFA v1 and GFA v2 output");
        exit(1);
    }

    for list in args.input_lists {
        for input in BufReader::new(
            File::open(&list)
                .log_unrecoverable_error_with_data(
                    "Error while opening input list file",
                    list.display(),
                )
                .unwrap(),
        )
        .lines()
        {
            if let Ok(input) = input {
                if input.trim().is_empty() {
                    continue;
                }

                let input = if <String as AsRef<Path>>::as_ref(&input).is_relative() {
                    list.parent().unwrap().join(input)
                } else {
                    PathBuf::from(input)
                };

                inputs.push((input, None));
            }
        }
    }

    let color_names: Vec<_> = if args.colored_input_lists.is_empty() {
        // Standard colors (input file names)
        inputs
            .iter()
            .map(|f| f.0.file_name().unwrap().to_string_lossy().to_string())
            .collect()
    } else {
        // Mapped colors
        let mut colors = HashMap::default();
        let mut next_index = 0;

        for list in args.colored_input_lists {
            for input in BufReader::new(
                File::open(&list)
                    .map_err(|e| {
                        panic!(
                            "Error while opening colored input list file {}: {}",
                            list.display(),
                            e
                        )
                    })
                    .unwrap(),
            )
            .lines()
            {
                if let Ok(input) = input {
                    if input.trim().is_empty() {
                        continue;
                    }

                    let parts = input.split("\t").collect::<Vec<_>>();
                    if parts.len() < 2 {
                        println!("Invalid line in colored input list: {}", input);
                        exit(1);
                    }

                    let color_name = parts[0..parts.len() - 1].join("\t");
                    let file_name = parts.last().unwrap().to_string();

                    let index = *colors.entry(color_name).or_insert_with(|| {
                        let index = next_index;
                        next_index += 1;
                        index
                    });

                    println!(
                        "Add index with color {} => {}",
                        parts[0..parts.len() - 1].join("\t"),
                        index
                    );

                    let file_name = if <String as AsRef<Path>>::as_ref(&file_name).is_relative() {
                        list.parent().unwrap().join(file_name)
                    } else {
                        PathBuf::from(file_name)
                    };

                    inputs.push((file_name, Some(index)));
                }
            }
        }
        let mut colors: Vec<_> = colors.into_iter().collect();
        colors.sort_by_key(|(_, i)| *i);

        colors.into_iter().map(|(c, _)| c).collect()
    };

    if inputs.is_empty() {
        println!("ERROR: No input files specified!");
        exit(1);
    }

    let inputs = inputs
        .into_iter()
        .map(|x| GeneralSequenceBlockData::FASTA(x))
        .collect();

    *ggcat_api::debug::DEBUG_ASSEMBLER_FIRST_STEP.lock() = convert_assembler_step(args.step);
    *ggcat_api::debug::DEBUG_ASSEMBLER_LAST_STEP.lock() = convert_assembler_step(args.last_step);

    let output_file = instance
        .build_graph(
            inputs,
            args.output_file.clone(),
            Some(&color_names),
            args.common_args.kmer_length,
            args.common_args.threads_count,
            args.common_args.forward_only,
            args.common_args.minimizer_length,
            args.colors,
            args.min_multiplicity,
            if args.generate_maximal_unitigs_links {
                ExtraElaboration::UnitigLinks
            } else if args.greedy_matchtigs {
                ExtraElaboration::GreedyMatchtigs
            } else if args.old_eulertigs {
                ExtraElaboration::Eulertigs
            } else if args.old_simplitigs {
                ExtraElaboration::Pathtigs
            } else if args.simplitigs {
                ExtraElaboration::FastSimplitigs
            } else if args.eulertigs {
                ExtraElaboration::FastEulertigs
            } else {
                ExtraElaboration::None
            },
            if args.gfa_output_v1 {
                Some(GfaVersion::V1)
            } else if args.gfa_output_v2 {
                Some(GfaVersion::V2)
            } else {
                None
            },
        )
        .unwrap();

    ggcat_logging::stats::write_stats(&args.output_file.with_extension("elab.stats.json"));

    println!("Final output saved to: {}", output_file.display());
}

fn convert_querier_step(step: QuerierStartingStep) -> querier::QuerierStartingStep {
    match step {
        QuerierStartingStep::MinimizerBucketing => querier::QuerierStartingStep::MinimizerBucketing,
        QuerierStartingStep::KmersCounting => querier::QuerierStartingStep::KmersCounting,
        QuerierStartingStep::CountersSorting => querier::QuerierStartingStep::CountersSorting,
        QuerierStartingStep::ColorMapReading => querier::QuerierStartingStep::ColorMapReading,
    }
}

fn run_querier_from_args(instance: &GGCATInstance, args: QueryArgs) -> PathBuf {
    *ggcat_api::debug::DEBUG_QUERIER_FIRST_STEP.lock() = convert_querier_step(args.step);

    instance
        .query_graph(
            args.input_graph,
            args.input_query,
            args.output_file_prefix,
            args.common_args.kmer_length,
            args.common_args.threads_count,
            args.common_args.forward_only,
            args.common_args.minimizer_length,
            args.colors,
            match args
                .colored_query_output_format
                .unwrap_or(ColoredQueryOutputFormat::JsonLinesWithNumbers)
            {
                ColoredQueryOutputFormat::JsonLinesWithNumbers => {
                    querier::ColoredQueryOutputFormat::JsonLinesWithNumbers
                }
                ColoredQueryOutputFormat::JsonLinesWithNames => {
                    querier::ColoredQueryOutputFormat::JsonLinesWithNames
                }
            },
        )
        .unwrap()
}

instrumenter::global_setup_instrumenter!();

fn main() {
    let args: CliArgs = CliArgs::parse();

    panic::set_hook(Box::new(move |panic_info| {
        let stdout = std::io::stdout();
        let _lock = stdout.lock();

        let stderr = std::io::stderr();
        let mut err_lock = stderr.lock();

        let backtrace = backtrace::Backtrace::new();
        write!(err_lock, "Panic: {}\nBacktrace:{:?}", panic_info, backtrace).unwrap();
        exit(1);
    }));

    match args {
        CliArgs::Build(args) => {
            let _guard = instrumenter::initialize_tracing(
                args.output_file.with_extension("tracing.json"),
                &["ix86arch::INSTRUCTION_RETIRED", "ix86arch::LLC_MISSES"],
            );

            let instance = initialize(&args.common_args, &args.output_file);

            run_assembler_from_args(&instance, args);
        }
        CliArgs::Matches(args) => {
            let colors_file = args.input_file.with_extension("colors.dat");
            let mut colors_deserializer =
                ColorsDeserializer::<DefaultColorsSerializer>::new(colors_file, true).unwrap();

            let mut colors = Vec::new();

            let color = ColorIndexType::from_str_radix(&args.match_color, 16)
                .expect("Invalid color, please use hex format");
            colors_deserializer.get_color_mappings(color, &mut colors);

            for color in colors {
                println!(
                    "MATCHES: {} => {}",
                    color,
                    colors_deserializer.get_color_name(color, false)
                );
            }
            return; // Skip final memory deallocation
        }
        CliArgs::Query(args) => {
            if !args.colors && args.colored_query_output_format.is_some() {
                println!(
                    "Warning: colored query output format is specified, but the graph is not colored"
                );
            }

            let _guard = instrumenter::initialize_tracing(
                args.output_file_prefix.with_extension("tracing.json"),
                &["ix86arch::INSTRUCTION_RETIRED", "ix86arch::LLC_MISSES"],
            );

            let instance = initialize(&args.common_args, &args.output_file_prefix);

            let output_file_name = run_querier_from_args(&instance, args);
            println!("Final output saved to: {}", output_file_name.display());
        }
        CliArgs::DumpColors(args) => {
            let output_file_name = args.output_file.with_extension("jsonl");

            let mut output_file = BufWriter::new(File::create(&output_file_name).unwrap());

            for (color_idx, color_name) in GGCATInstance::dump_colors(args.input_colormap)
                .unwrap()
                .enumerate()
            {
                writeln!(
                    output_file,
                    "{{\"color_index\":{}, \"color_name\":\"{}\" }}",
                    color_idx, color_name,
                )
                .unwrap();
            }

            drop(output_file);
            println!("Colors written to {}", output_file_name.display());

            return; // Skip final memory deallocation
        }
    }

    // Ensure termination
    std::thread::spawn(|| {
        std::thread::sleep(Duration::from_secs(5));
        exit(0);
    });
    MemoryFs::terminate();
}
