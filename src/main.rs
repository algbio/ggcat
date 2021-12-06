#![feature(new_uninit, core_intrinsics, type_alias_impl_trait)]
#![feature(is_sorted, thread_local, panic_info_message)]
#![feature(slice_group_by, raw_vec_internals)]
#![feature(llvm_asm)]
#![feature(option_result_unwrap_unchecked)]
#![feature(specialization)]
#![feature(generic_const_exprs)]
#![feature(generic_associated_types)]
#![feature(const_fn_floating_point_arithmetic)]
#![feature(trait_alias)]
#![allow(warnings)]
#![feature(test)]
#![feature(slice_partition_dedup)]

extern crate alloc;
extern crate test;

use crate::assemble_pipeline::AssemblePipeline;
use crate::utils::Utils;
use rayon::ThreadPoolBuilder;
use std::cmp::min;
use std::fs::{create_dir_all, remove_file, File};
use std::intrinsics::exact_div;
use std::path::PathBuf;
use std::process::exit;
use structopt::{clap::ArgGroup, StructOpt};

mod assemble_pipeline;
mod benchmarks;
mod hashes;
pub mod libdeflate;
mod types;
#[macro_use]
mod utils;
mod assembler;
mod assembler_generic_dispatcher;
mod colors;
mod io;
mod pipeline_common;
mod querier;
mod querier_generic_dispatcher;
mod query_pipeline;
mod rolling;

pub const DEFAULT_BUFFER_SIZE: usize = 1024 * 1024 * 24;

fn outputs_arg_group() -> ArgGroup<'static> {
    // As the attributes of the struct are executed before the struct
    // fields, we can't use .args(...), but we can use the group
    // attribute on the fields.
    ArgGroup::with_name("outputs").required(true)
}

use crate::assembler::run_assembler;
use crate::assembler_generic_dispatcher::dispatch_assembler_hash_type;
use crate::colors::colors_manager::ColorsManager;
use crate::colors::default_colors_manager::DefaultColorsManager;
use crate::colors::storage::run_length::RunLengthColorsSerializer;
use crate::colors::storage::serializer::ColorsSerializer;
use crate::colors::ColorIndexType;
use crate::hashes::cn_nthash::CanonicalNtHashIteratorFactory;
use crate::hashes::cn_seqhash;
use crate::hashes::cn_seqhash::u64::CanonicalSeqHashFactory;
use crate::hashes::fw_nthash::{ForwardNtHashIterator, ForwardNtHashIteratorFactory};
use crate::hashes::fw_seqhash::u64::ForwardSeqHashFactory;
use crate::hashes::HashFunctionFactory;
use crate::io::reads_reader::ReadsReader;
use crate::io::reads_writer::ReadsWriter;
use crate::io::sequences_reader::{FastaSequence, SequencesReader};
use crate::io::varint::decode_varint;
use crate::querier_generic_dispatcher::dispatch_querier_hash_type;
use crate::utils::compressed_read::CompressedRead;
use byteorder::ReadBytesExt;
use clap::arg_enum;
use parallel_processor::phase_times_monitor::PHASES_TIMES_MONITOR;
use parking_lot::Mutex;
use std::io::{BufRead, BufReader, Write};
use std::panic;
use std::sync::atomic::{AtomicBool, Ordering};

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
        KmersIntersection = 1,
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

#[derive(StructOpt, Debug)]
enum CliArgs {
    Build(AssemblerArgs),
    Matches(MatchesArgs),
    Query(QueryArgs),
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

    /// Specifies the m-mers (minimizers) length, defaults to min(12, ceil(K / 2))
    #[structopt(short)]
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

    /// Minimum correctness probability for each kmer (using fastq quality checks)
    #[structopt(short = "q", long = "quality-threshold")]
    pub quality_threshold: Option<f64>,

    #[structopt(short = "n", long, default_value = "0")]
    pub number: usize,

    #[structopt(short = "o", long = "output-file", default_value = "output.fasta.lz4")]
    pub output_file: PathBuf,

    #[structopt(short = "x", long, default_value = "MinimizerBucketing")]
    pub step: AssemblerStartingStep,

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

fn initialize(args: &CommonArgs) {
    // Increase the maximum allowed number of open files
    fdlimit::raise_fd_limit();

    KEEP_FILES.store(args.keep_temp_files, Ordering::Relaxed);

    ThreadPoolBuilder::new()
        .num_threads(args.threads_count)
        .build_global();

    create_dir_all(&args.temp_dir);
}

fn main() {
    let args: CliArgs = CliArgs::from_args();

    panic::set_hook(Box::new(move |info| {
        let stdout = std::io::stdout();
        let mut _lock = stdout.lock();

        let stderr = std::io::stderr();
        let mut err_lock = stderr.lock();

        writeln!(
            err_lock,
            "Thread panicked at location: {:?}",
            info.location()
        );
        if let Some(message) = info.message() {
            writeln!(err_lock, "Error message: {}", message);
        }
        if let Some(s) = info.payload().downcast_ref::<&str>() {
            writeln!(err_lock, "Panic payload: {:?}", s);
        }

        exit(1);
    }));

    const BUCKETS_COUNT: usize = 512;

    match args {
        CliArgs::Build(args) => {
            initialize(&args.common_args);
            if args.colors {
                dispatch_assembler_hash_type::<DefaultColorsManager, BUCKETS_COUNT>(args);
            } else {
                dispatch_assembler_hash_type::<(), BUCKETS_COUNT>(args);
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
            initialize(&args.common_args);
            if args.colors {
                dispatch_querier_hash_type::<DefaultColorsManager, BUCKETS_COUNT>(args);
            } else {
                dispatch_querier_hash_type::<(), BUCKETS_COUNT>(args);
            }
        }
    }
}
