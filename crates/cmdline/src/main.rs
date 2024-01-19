#![cfg_attr(test, feature(test))]

extern crate alloc;
#[cfg(test)]
extern crate test;

mod benchmarks;

use ahash::HashMap;
use ggcat_api::{ExtraElaboration, GGCATConfig, GGCATInstance};
use std::fs::File;
use std::io::{BufReader, BufWriter, Write};
use std::panic;
use std::path::PathBuf;
use std::process::exit;
use std::sync::atomic::Ordering;
use std::time::Duration;
use structopt::StructOpt;

arg_enum! {
    #[derive(Debug, PartialOrd, PartialEq)]
    pub enum AssemblerStartingStep {
        MinimizerBucketing = 0,
        KmersMerge = 1,
        HashesSorting = 2,
        LinksCompaction = 3,
        ReorganizeReads = 4,
        BuildUnitigs = 5,
        MaximalUnitigsLinks = 6,
    }
}

arg_enum! {
    #[derive(Debug, PartialOrd, PartialEq)]
    pub enum QuerierStartingStep {
        MinimizerBucketing = 0,
        KmersCounting = 1,
        CountersSorting = 2,
        ColorMapReading = 3,
    }
}

arg_enum! {
    #[derive(Copy, Clone, Debug, PartialOrd, PartialEq)]
    pub enum HashType {
        Auto = 0,
        SeqHash = 1,
        RabinKarp32 = 2,
        RabinKarp64 = 3,
        RabinKarp128 = 4
    }
}

use ::utils::compute_best_m;
use colors::colors_manager::ColorMapReader;
use colors::storage::deserializer::ColorsDeserializer;
use colors::DefaultColorsSerializer;
use config::ColorIndexType;
use io::sequences_stream::general::GeneralSequenceBlockData;
use parallel_processor::memory_fs::MemoryFs;
use std::io::BufRead;
use structopt::clap::{arg_enum, ArgGroup};

#[derive(StructOpt, Debug)]
enum CliArgs {
    Build(AssemblerArgs),
    Query(QueryArgs),
    DumpColors(DumpColorsArgs),
    Matches(MatchesArgs),
    // Utils(CmdUtilsArgs),
}

#[derive(StructOpt, Debug)]
struct MatchesArgs {
    /// Input fasta file with associated colors file (in the same folder)
    input_file: PathBuf,

    /// Debug print matches of a color index (in hexadecimal)
    match_color: String,
}

#[derive(StructOpt, Debug)]
struct CommonArgs {
    /// Specifies the k-mers length
    #[structopt(short, long = "kmer-length")]
    pub kmer_length: usize,

    /// Overrides the default m-mers (minimizers) length
    #[structopt(long = "minimizer-length")]
    pub minimizer_length: Option<usize>,

    /// Directory for temporary files (default .temp_files)
    #[structopt(short = "t", long = "temp-dir", default_value = ".temp_files")]
    pub temp_dir: PathBuf,

    /// Keep intermediate temporary files for debugging purposes
    #[structopt(long = "keep-temp-files")]
    pub keep_temp_files: bool,

    #[structopt(short = "j", long, default_value = "16")]
    pub threads_count: usize,

    /// Hash type used to identify kmers
    #[structopt(short = "w", long, default_value = "Auto")]
    pub hash_type: HashType,

    /// Treats reverse complementary kmers as different
    #[structopt(short = "f", long)]
    pub forward_only: bool,

    /// Maximum suggested memory usage (GB)
    /// The tool will try use only up to this GB of memory to store temporary files
    /// without writing to disk. This usage does not include the needed memory for the processing steps.
    /// GGCAT can allocate extra memory for files if the current memory is not enough to complete the current operation
    #[structopt(short = "m", long, default_value = "2")]
    pub memory: f64,

    /// Use all the given memory before writing to disk
    #[structopt(short = "p", long = "prefer-memory")]
    pub prefer_memory: bool,

    /// The log2 of the number of buckets
    #[structopt(short = "b", long = "buckets-count-log")]
    pub buckets_count_log: Option<usize>,

    /// The level of lz4 compression to be used for the intermediate files
    #[structopt(long = "intermediate-compression-level")]
    pub intermediate_compression_level: Option<u32>,

    #[structopt(long = "only-bstats", hidden = true)]
    pub only_bstats: bool,
}

#[derive(StructOpt, Debug)]
#[structopt(group = ArgGroup::with_name("output-mode").required(false))]
struct AssemblerArgs {
    /// The input files
    pub input: Vec<PathBuf>,

    /// The lists of input files
    #[structopt(short = "l", long = "input-lists")]
    pub input_lists: Vec<PathBuf>,

    /// The lists of input files with colors in format <COLOR_NAME><TAB><FILE_PATH>
    #[structopt(short = "d", long = "colored-input-lists")]
    pub colored_input_lists: Vec<PathBuf>,

    /// Enable colors
    #[structopt(short, long)]
    pub colors: bool,

    /// Minimum multiplicity required to keep a kmer
    #[structopt(short = "s", long = "min-multiplicity", default_value = "2")]
    pub min_multiplicity: usize,

    // /// Minimum correctness probability for each kmer (using fastq quality checks)
    // #[structopt(short = "q", long = "quality-threshold")]
    // pub quality_threshold: Option<f64>,
    #[structopt(short = "n", long, default_value = "0", hidden = true)]
    pub number: usize,

    #[structopt(short = "o", long = "output-file", default_value = "output.fasta.lz4")]
    pub output_file: PathBuf,

    #[structopt(long, default_value = "MinimizerBucketing")]
    pub step: AssemblerStartingStep,

    #[structopt(long = "last-step", default_value = "BuildUnitigs")]
    pub last_step: AssemblerStartingStep,

    /// Generate maximal unitigs connections references, in BCALM2 format L:<+/->:<other id>:<+/->
    #[structopt(
        short = "e",
        long = "generate-maximal-unitigs-links",
        group = "output-mode"
    )]
    pub generate_maximal_unitigs_links: bool,

    /// Generate greedy matchtigs instead of maximal unitigs
    #[structopt(short = "g", long = "greedy-matchtigs", group = "output-mode")]
    pub greedy_matchtigs: bool,

    /// Generate eulertigs instead of maximal unitigs
    #[structopt(long = "eulertigs", group = "output-mode")]
    pub eulertigs: bool,

    /// Generate pathtigs instead of maximal unitigs
    #[structopt(long = "pathtigs", group = "output-mode")]
    pub pathtigs: bool,

    #[structopt(flatten)]
    pub common_args: CommonArgs,
}

#[derive(StructOpt, Debug)]
struct DumpColorsArgs {
    input_colormap: PathBuf,
    output_file: PathBuf,
}

arg_enum! {
    /// Format of the queries output
    #[derive(Copy, Clone, Debug, PartialEq, Eq)]
    pub enum ColoredQueryOutputFormat {
        JsonLinesWithNumbers,
        JsonLinesWithNames,
    }
}

#[derive(StructOpt, Debug)]
struct QueryArgs {
    /// The input graph
    pub input_graph: PathBuf,

    /// The input query as a .fasta file
    pub input_query: PathBuf,

    /// Enable colors
    #[structopt(short, long)]
    pub colors: bool,

    #[structopt(short = "o", long = "output-file-prefix", default_value = "output")]
    pub output_file_prefix: PathBuf,

    #[structopt(long = "colored-query-output-format")]
    pub colored_query_output_format: Option<ColoredQueryOutputFormat>,

    #[structopt(short = "x", long, default_value = "MinimizerBucketing")]
    pub step: QuerierStartingStep,

    #[structopt(flatten)]
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
    });

    ggcat_api::debug::DEBUG_KEEP_FILES.store(args.keep_temp_files, Ordering::Relaxed);
    *ggcat_api::debug::BUCKETS_COUNT_LOG_FORCE.lock() = args.buckets_count_log;
    ggcat_api::debug::DEBUG_ONLY_BSTATS.store(args.only_bstats, Ordering::Relaxed);
    *ggcat_api::debug::DEBUG_HASH_TYPE.lock() = match args.hash_type {
        HashType::Auto => ggcat_api::HashType::Auto,
        HashType::SeqHash => ggcat_api::HashType::SeqHash,
        HashType::RabinKarp32 => ggcat_api::HashType::RabinKarp32,
        HashType::RabinKarp64 => ggcat_api::HashType::RabinKarp64,
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
    instance
}

fn convert_assembler_step(step: AssemblerStartingStep) -> assembler::AssemblerStartingStep {
    match step {
        AssemblerStartingStep::MinimizerBucketing => {
            assembler::AssemblerStartingStep::MinimizerBucketing
        }
        AssemblerStartingStep::KmersMerge => assembler::AssemblerStartingStep::KmersMerge,
        AssemblerStartingStep::HashesSorting => assembler::AssemblerStartingStep::HashesSorting,
        AssemblerStartingStep::LinksCompaction => assembler::AssemblerStartingStep::LinksCompaction,
        AssemblerStartingStep::ReorganizeReads => assembler::AssemblerStartingStep::ReorganizeReads,
        AssemblerStartingStep::BuildUnitigs => assembler::AssemblerStartingStep::BuildUnitigs,
        AssemblerStartingStep::MaximalUnitigsLinks => {
            assembler::AssemblerStartingStep::MaximalUnitigsLinks
        }
    }
}

fn run_assembler_from_args(instance: &GGCATInstance, args: AssemblerArgs) {
    let mut inputs: Vec<_> = args.input.iter().cloned().map(|f| (f, None)).collect();

    if (args.input_lists.len() > 0 || args.input.len() > 0) && args.colored_input_lists.len() > 0 {
        println!("Cannot specify both colored input lists and other files/lists");
        exit(1);
    }

    for list in args.input_lists {
        for input in BufReader::new(File::open(list).unwrap()).lines() {
            if let Ok(input) = input {
                if input.trim().is_empty() {
                    continue;
                }
                inputs.push((PathBuf::from(input), None));
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
            for input in BufReader::new(File::open(list).unwrap()).lines() {
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

                    inputs.push((PathBuf::from(file_name), Some(index)));
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
    ggcat_api::debug::DEBUG_LINK_PHASE_ITERATION_START_STEP.store(args.number, Ordering::Relaxed);

    let output_file = instance.build_graph(
        inputs,
        args.output_file,
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
        } else if args.eulertigs {
            ExtraElaboration::Eulertigs
        } else if args.pathtigs {
            ExtraElaboration::Pathtigs
        } else {
            ExtraElaboration::None
        },
    );

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

    instance.query_graph(
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
}

instrumenter::global_setup_instrumenter!();

fn main() {
    let args: CliArgs = CliArgs::from_args();

    #[cfg(feature = "mem-analysis")]
    {
        parallel_processor::mem_tracker::init_memory_info();
        parallel_processor::mem_tracker::start_info_logging();
    }

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
                ColorsDeserializer::<DefaultColorsSerializer>::new(colors_file, true);

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
            initialize(&args.common_args, &args.output_file_prefix);

            if !args.colors && args.colored_query_output_format.is_some() {
                println!("Warning: colored query output format is specified, but the graph is not colored");
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

            for (color_idx, color_name) in
                GGCATInstance::dump_colors(args.input_colormap).enumerate()
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
