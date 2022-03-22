#![feature(type_alias_impl_trait)]
#![feature(is_sorted, thread_local, panic_info_message)]
#![feature(slice_group_by)]
#![feature(generic_associated_types)]
#![feature(trait_alias)]
#![feature(test)]
#![feature(slice_partition_dedup)]
// #![deny(warnings)]
#![allow(dead_code)]

extern crate alloc;
extern crate test;

#[macro_use]
extern crate static_assertions;
extern crate core;

mod assemble_pipeline;
mod benchmarks;
mod config;
mod hashes;
#[macro_use]
mod utils;
mod assembler;
mod assembler_generic_dispatcher;
mod cmd_utils;
mod colors;
mod io;
mod pipeline_common;
mod querier;
mod querier_generic_dispatcher;
mod query_pipeline;
mod rolling;

use backtrace::Backtrace;
use std::cmp::max;

use crate::assembler_generic_dispatcher::dispatch_assembler_hash_type;
use crate::cmd_utils::{process_cmdutils, CmdUtilsArgs};
use crate::colors::default_colors_manager::DefaultColorsManager;
use crate::colors::storage::run_length::RunLengthColorsSerializer;
use crate::colors::storage::serializer::ColorsSerializer;
use crate::colors::ColorIndexType;
use crate::io::sequences_reader::FastaSequence;
use crate::querier_generic_dispatcher::dispatch_querier_hash_type;
use crate::utils::compressed_read::CompressedRead;
use clap::arg_enum;
use parallel_processor::enable_counters_logging;
use parallel_processor::memory_data_size::MemoryDataSize;
use rayon::ThreadPoolBuilder;
use std::fs::create_dir_all;
use std::io::Write;
use std::panic;
use std::path::PathBuf;
use std::process::exit;
use std::sync::atomic::{AtomicBool, Ordering};
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

use crate::config::FLUSH_QUEUE_FACTOR;
use crate::utils::compute_best_m;
use parallel_processor::memory_fs::MemoryFs;
use parallel_processor::phase_times_monitor::PHASES_TIMES_MONITOR;

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

static KEEP_FILES: AtomicBool = AtomicBool::new(false);
static PREFER_MEMORY: AtomicBool = AtomicBool::new(false);

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

pub static SAVE_MEMORY: AtomicBool = AtomicBool::new(false);

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

        let _ = writeln!(
            err_lock,
            "Thread panicked at location: {:?}",
            info.location()
        );
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
            initialize(&args.common_args, &args.output_file);
            if args.colors {
                dispatch_assembler_hash_type::<DefaultColorsManager, { config::FIRST_BUCKETS_COUNT }>(
                    args,
                );
            } else {
                dispatch_assembler_hash_type::<(), { config::FIRST_BUCKETS_COUNT }>(args);
            }
        }
        CliArgs::Matches(args) => {
            let colors_file = args.input_file.with_extension("colors.dat");
            let colors = ColorsSerializer::<RunLengthColorsSerializer>::read_color(
                colors_file,
                args.match_color,
            );
            for color in colors {
                println!("MATCH: {}", color);
            }
        }
        CliArgs::Query(args) => {
            initialize(&args.common_args, &args.output_file);
            if args.colors {
                dispatch_querier_hash_type::<DefaultColorsManager, { config::FIRST_BUCKETS_COUNT }>(
                    args,
                );
            } else {
                dispatch_querier_hash_type::<(), { config::FIRST_BUCKETS_COUNT }>(args);
            }
        }
        CliArgs::Utils(args) => {
            process_cmdutils(args);
        }
    }

    MemoryFs::terminate();
}
