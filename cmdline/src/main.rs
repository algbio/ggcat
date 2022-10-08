#![feature(type_alias_impl_trait)]
#![feature(is_sorted, thread_local, panic_info_message)]
#![feature(slice_group_by)]
#![feature(trait_alias)]
#![feature(test)]
#![feature(slice_partition_dedup)]
#![feature(int_log)]
#![feature(new_uninit)]
// #![deny(warnings)]
#![allow(dead_code)]

extern crate alloc;
extern crate test;

mod benchmarks;

#[macro_use]
mod utils;
mod cmd_utils;

use backtrace::Backtrace;
use std::cmp::max;

use crate::cmd_utils::{process_cmdutils, CmdUtilsArgs};
use colors::bundles::multifile_building::ColorBundleMultifileBuilding;
use colors::colors_manager::ColorsManager;
use hashes::MinimizerHashFunctionFactory;
use parallel_processor::enable_counters_logging;
use parallel_processor::memory_data_size::MemoryDataSize;
use rayon::ThreadPoolBuilder;
use std::fs::{create_dir_all, File};
use std::io::{BufReader, Write};
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
        BuildUnitigs = 5
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

use ::utils::{compute_best_m, DEBUG_LEVEL};
use colors::bundles::graph_querying::ColorBundleGraphQuerying;
use colors::non_colored::NonColoredManager;
use colors::storage::deserializer::ColorsDeserializer;
use colors::DefaultColorsSerializer;
use config::{ColorIndexType, FLUSH_QUEUE_FACTOR, KEEP_FILES, PREFER_MEMORY};
use hashes::cn_nthash::CanonicalNtHashIteratorFactory;
use hashes::fw_nthash::ForwardNtHashIteratorFactory;
use parallel_processor::memory_fs::MemoryFs;
use parallel_processor::phase_times_monitor::PHASES_TIMES_MONITOR;
use static_dispatch::StaticDispatch;
use std::io::BufRead;
use structopt::clap::arg_enum;

#[derive(StructOpt, Debug)]
enum CliArgs {
    Build(AssemblerArgs),
    Matches(MatchesArgs),
    Query(QueryArgs),
    Utils(CmdUtilsArgs),
}

#[derive(StructOpt, Debug)]
struct MatchesArgs {
    /// Input fasta file with associated colors file (in the same folder)
    input_file: PathBuf,

    /// Debug print matches of a color index
    match_color: ColorIndexType,
}

#[derive(StructOpt, Debug)]
struct CommonArgs {
    /// Specifies the k-mers length
    #[structopt(short, default_value = "32")]
    pub klen: usize,

    /// Specifies the m-mers (minimizers) length, defaults to min(3, ceil((K + 2) / 3))
    #[structopt(long)]
    pub mlen: Option<usize>,

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

    /// Maximum memory usage (GB)
    #[structopt(short = "m", long, default_value = "2")]
    pub memory: f64,

    /// Use all the given memory before writing to disk
    #[structopt(short = "p", long = "prefer-memory")]
    pub prefer_memory: bool,

    /// The log2 of the number of buckets
    #[structopt(short = "b", long = "buckets-count-log")]
    pub buckets_count_log: Option<usize>,

    /// The level of debugging
    #[structopt(short = "d", long = "debug-level", default_value = "0")]
    pub debug_level: usize,

    #[structopt(long = "only-bstats", hidden = true)]
    pub only_bstats: bool,
}

#[derive(StructOpt, Debug)]
struct AssemblerArgs {
    /// The input files
    pub input: Vec<PathBuf>,

    /// The lists of input files
    #[structopt(short = "l", long = "input-lists")]
    pub input_lists: Vec<PathBuf>,

    /// Enable colors
    #[structopt(short, long)]
    pub colors: bool,

    /// Minimum multiplicity required to keep a kmer
    #[structopt(short = "s", long = "min-multiplicity", default_value = "2")]
    pub min_multiplicity: usize,

    // /// Minimum correctness probability for each kmer (using fastq quality checks)
    // #[structopt(short = "q", long = "quality-threshold")]
    // pub quality_threshold: Option<f64>,
    #[structopt(short = "n", long, default_value = "0")]
    pub number: usize,

    #[structopt(short = "o", long = "output-file", default_value = "output.fasta.lz4")]
    pub output_file: PathBuf,

    #[structopt(long, default_value = "MinimizerBucketing")]
    pub step: AssemblerStartingStep,

    #[structopt(long = "last-step", default_value = "BuildUnitigs")]
    pub last_step: AssemblerStartingStep,

    #[structopt(flatten)]
    pub common_args: CommonArgs,
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

    #[structopt(short = "o", long = "output-file", default_value = "output.csv")]
    pub output_file: PathBuf,

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

fn initialize(args: &CommonArgs, out_file: &PathBuf) {
    // Increase the maximum allowed number of open files
    fdlimit::raise_fd_limit();

    KEEP_FILES.store(args.keep_temp_files, Ordering::Relaxed);

    PREFER_MEMORY.store(args.prefer_memory, Ordering::Relaxed);

    DEBUG_LEVEL.store(args.debug_level, Ordering::Relaxed);

    ThreadPoolBuilder::new()
        .num_threads(args.threads_count)
        .thread_name(|i| format!("rayon-thread-{}", i))
        .build_global()
        .unwrap();

    create_dir_all(&args.temp_dir).unwrap();

    enable_counters_logging(
        out_file.with_extension("stats.log"),
        Duration::from_millis(1000),
        |val| {
            val["phase"] = PHASES_TIMES_MONITOR.read().get_phase_desc().into();
        },
    );

    MemoryFs::init(
        parallel_processor::memory_data_size::MemoryDataSize::from_bytes(
            (args.memory * (MemoryDataSize::OCTET_GIBIOCTET_FACTOR as f64)) as usize,
        ),
        FLUSH_QUEUE_FACTOR * args.threads_count,
        max(1, args.threads_count / 4),
        32768,
    );

    println!(
        "Using m: {} with k: {}",
        args.mlen.unwrap_or(compute_best_m(args.klen)),
        args.klen
    )

    // #[cfg(feature = "mem-analysis")]
    // debug_print_allocations("/tmp/allocations", Duration::from_secs(5));
}

fn get_hash_static_id(hash_type: HashType, k: usize, forward_only: bool) -> StaticDispatch<()> {
    let hash_type = match hash_type {
        HashType::Auto => {
            if k <= 64 {
                HashType::SeqHash
            } else {
                HashType::RabinKarp128
            }
        }
        x => x,
    };

    use hashes::*;

    match hash_type {
        HashType::SeqHash => {
            if k <= 8 {
                if forward_only {
                    fw_seqhash::u16::ForwardSeqHashFactory::STATIC_DISPATCH_ID
                } else {
                    cn_seqhash::u16::CanonicalSeqHashFactory::STATIC_DISPATCH_ID
                }
            } else if k <= 16 {
                if forward_only {
                    fw_seqhash::u32::ForwardSeqHashFactory::STATIC_DISPATCH_ID
                } else {
                    cn_seqhash::u32::CanonicalSeqHashFactory::STATIC_DISPATCH_ID
                }
            } else if k <= 32 {
                if forward_only {
                    fw_seqhash::u64::ForwardSeqHashFactory::STATIC_DISPATCH_ID
                } else {
                    cn_seqhash::u64::CanonicalSeqHashFactory::STATIC_DISPATCH_ID
                }
            } else if k <= 64 {
                if forward_only {
                    fw_seqhash::u128::ForwardSeqHashFactory::STATIC_DISPATCH_ID
                } else {
                    cn_seqhash::u128::CanonicalSeqHashFactory::STATIC_DISPATCH_ID
                }
            } else {
                panic!("Cannot use sequence hash for k > 64!");
            }
        }
        HashType::RabinKarp32 => {
            if forward_only {
                fw_rkhash::u32::ForwardRabinKarpHashFactory::STATIC_DISPATCH_ID
            } else {
                cn_rkhash::u32::CanonicalRabinKarpHashFactory::STATIC_DISPATCH_ID
            }
        }
        HashType::RabinKarp64 => {
            if forward_only {
                fw_rkhash::u64::ForwardRabinKarpHashFactory::STATIC_DISPATCH_ID
            } else {
                cn_rkhash::u64::CanonicalRabinKarpHashFactory::STATIC_DISPATCH_ID
            }
        }
        HashType::RabinKarp128 => {
            if forward_only {
                fw_rkhash::u128::ForwardRabinKarpHashFactory::STATIC_DISPATCH_ID
            } else {
                cn_rkhash::u128::CanonicalRabinKarpHashFactory::STATIC_DISPATCH_ID
            }
        }
        HashType::Auto => {
            unreachable!()
        }
    }
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
    }
}

fn run_assembler_from_args(
    generics: (StaticDispatch<()>, StaticDispatch<()>, StaticDispatch<()>),
    args: AssemblerArgs,
) {
    let mut inputs = args.input.clone();

    for list in args.input_lists {
        for input in BufReader::new(File::open(list).unwrap()).lines() {
            if let Ok(input) = input {
                inputs.push(PathBuf::from(input));
            }
        }
    }

    if inputs.is_empty() {
        println!("ERROR: No input files specified!");
        exit(1);
    }

    assembler::dynamic_dispatch::run_assembler(
        generics,
        args.common_args.klen,
        args.common_args
            .mlen
            .unwrap_or(compute_best_m(args.common_args.klen)),
        convert_assembler_step(args.step),
        convert_assembler_step(args.last_step),
        inputs,
        args.output_file,
        args.common_args.temp_dir,
        args.common_args.threads_count,
        args.min_multiplicity,
        args.common_args.buckets_count_log,
        Some(args.number),
        args.common_args.only_bstats,
    );
}

fn convert_querier_step(step: QuerierStartingStep) -> querier::QuerierStartingStep {
    match step {
        QuerierStartingStep::MinimizerBucketing => querier::QuerierStartingStep::MinimizerBucketing,
        QuerierStartingStep::KmersCounting => querier::QuerierStartingStep::KmersCounting,
        QuerierStartingStep::CountersSorting => querier::QuerierStartingStep::CountersSorting,
        QuerierStartingStep::ColorMapReading => querier::QuerierStartingStep::ColorMapReading,
    }
}

fn run_querier_from_args(
    generics: (StaticDispatch<()>, StaticDispatch<()>, StaticDispatch<()>),
    args: QueryArgs,
) {
    querier::dynamic_dispatch::run_query(
        generics,
        args.common_args.klen,
        args.common_args
            .mlen
            .unwrap_or(compute_best_m(args.common_args.klen)),
        convert_querier_step(args.step),
        args.input_graph,
        args.input_query,
        args.output_file,
        args.common_args.temp_dir,
        args.common_args.buckets_count_log,
        args.common_args.threads_count,
    );
}

instrumenter::global_setup_instrumenter!();

fn main() {
    let args: CliArgs = CliArgs::from_args();

    #[cfg(feature = "mem-analysis")]
    {
        parallel_processor::mem_tracker::init_memory_info();
        parallel_processor::mem_tracker::start_info_logging();
    }

    panic::set_hook(Box::new(move |info| {
        let stdout = std::io::stdout();
        let mut _lock = stdout.lock();

        let stderr = std::io::stderr();
        let mut err_lock = stderr.lock();

        if let Some(location) = info.location() {
            let _ = writeln!(err_lock, "Thread panicked at location: {}", location);
        }
        if let Some(message) = info.message() {
            let _ = writeln!(err_lock, "Error message: {}", message);
        }
        if let Some(s) = info.payload().downcast_ref::<&str>() {
            let _ = writeln!(err_lock, "Panic payload: {:?}", s);
        }

        println!("Backtrace: {:?}", Backtrace::new());

        exit(1);
    }));

    match args {
        CliArgs::Build(args) => {
            let _guard = instrumenter::initialize_tracing(
                args.output_file.with_extension("tracing.json"),
                &["ix86arch::INSTRUCTION_RETIRED", "ix86arch::LLC_MISSES"],
            );

            initialize(&args.common_args, &args.output_file);

            let bucketing_hash = if args.common_args.forward_only {
                <ForwardNtHashIteratorFactory as MinimizerHashFunctionFactory>::STATIC_DISPATCH_ID
            } else {
                <CanonicalNtHashIteratorFactory as MinimizerHashFunctionFactory>::STATIC_DISPATCH_ID
            };

            run_assembler_from_args(
                (
                    bucketing_hash,
                    get_hash_static_id(
                        args.common_args.hash_type,
                        args.common_args.klen,
                        args.common_args.forward_only,
                    ),
                    if args.colors {
                        ColorBundleMultifileBuilding::STATIC_DISPATCH_ID
                    } else {
                        NonColoredManager::STATIC_DISPATCH_ID
                    },
                ),
                args,
            )
        }
        CliArgs::Matches(args) => {
            let colors_file = args.input_file.with_extension("colors.dat");
            let mut colors_deserializer =
                ColorsDeserializer::<DefaultColorsSerializer>::new(colors_file);

            let mut colors = Vec::new();

            colors_deserializer.get_color_mappings(args.match_color, &mut colors);

            for color in colors {
                println!("MATCHES: {}", color);
            }
            return; // Skip final memory deallocation
        }
        CliArgs::Query(args) => {
            initialize(&args.common_args, &args.output_file);

            let bucketing_hash = if args.common_args.forward_only {
                <ForwardNtHashIteratorFactory as MinimizerHashFunctionFactory>::STATIC_DISPATCH_ID
            } else {
                <CanonicalNtHashIteratorFactory as MinimizerHashFunctionFactory>::STATIC_DISPATCH_ID
            };

            run_querier_from_args(
                (
                    bucketing_hash,
                    get_hash_static_id(
                        args.common_args.hash_type,
                        args.common_args.klen,
                        args.common_args.forward_only,
                    ),
                    if args.colors {
                        ColorBundleGraphQuerying::STATIC_DISPATCH_ID
                    } else {
                        NonColoredManager::STATIC_DISPATCH_ID
                    },
                ),
                args,
            )
        }
        CliArgs::Utils(args) => {
            process_cmdutils(args);
        }
    }

    MemoryFs::terminate();
}
