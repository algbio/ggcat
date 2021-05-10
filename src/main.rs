#![feature(new_uninit, core_intrinsics)]
#![feature(is_sorted)]
#![feature(slice_group_by)]
#![feature(llvm_asm)]
#![feature(min_type_alias_impl_trait)]
#![feature(option_result_unwrap_unchecked)]
#![feature(specialization)]
#![allow(warnings)]
#![feature(test)]

extern crate test;

use crate::pipeline::kmers_merge::RetType;
use crate::pipeline::Pipeline;
use crate::reads_freezer::WriterChannels::Pipe;
use crate::rolling_minqueue::GenericsFunctional;
use crate::utils::Utils;
use rayon::ThreadPoolBuilder;
use std::cmp::min;
use std::fs::create_dir_all;
use std::intrinsics::exact_div;
use std::path::PathBuf;
use std::process::exit;
use structopt::{clap::ArgGroup, StructOpt};

mod benchmarks;
mod binary_writer;
mod compressed_read;
mod debug_functions;
mod fast_rand_bool;
pub mod hash_entry;
mod intermediate_storage;
pub mod libdeflate;
mod multi_thread_buckets;
mod pipeline;
mod progress;
mod reads_freezer;
mod rolling_kseq_iterator;
mod rolling_minqueue;
mod rolling_quality_check;
mod sequences_reader;
mod smart_bucket_sort;
mod unitig_link;
mod utils;
mod varint;
mod vec_slice;

fn outputs_arg_group() -> ArgGroup<'static> {
    // As the attributes of the struct are executed before the struct
    // fields, we can't use .args(...), but we can use the group
    // attribute on the fields.
    ArgGroup::with_name("outputs").required(true)
}

use clap::arg_enum;
arg_enum! {
    #[derive(Debug, PartialOrd, PartialEq)]
    enum StartingStep {
        MinimizerBucketing = 0,
        KmersMerge = 1,
        HashesSorting = 2,
        LinksCompaction = 3,
        ReorganizeReads = 4,
    }
}

#[derive(StructOpt, Debug)]
struct Cli {
    /// The input files
    #[structopt(required = true)]
    input: Vec<PathBuf>,

    /// Specifies the k-mers length
    #[structopt(short, default_value = "32")]
    klen: usize,

    /// Specifies the m-mers (minimizers) length, defaults to min(12, ceil(K / 2))
    #[structopt(short)]
    mlen: Option<usize>,

    /// Minimum multiplicity required to keep a kmer
    #[structopt(short = "s", long = "min-multiplicity", default_value = "2")]
    min_multiplicity: usize,

    /// Directory for temporary files (default .temp_files)
    #[structopt(short = "t", long = "temp-dir", default_value = ".temp_files")]
    temp_dir: PathBuf,

    #[structopt(short = "n", long, default_value = "0")]
    number: usize, // Tests built bloom filter against this file for coverage tests
    // #[structopt(short = "f", requires = "elabbloom")]
    // coverage: Option<String>,

    // Enables output compression
    // #[structopt(short, requires = "output")]
    // compress: bool
    #[structopt(short = "x", long, default_value = "MinimizerBucketing")]
    step: StartingStep,
}

struct NtHashMinTransform;
impl GenericsFunctional<u64, u32> for NtHashMinTransform {
    fn func(value: u64) -> u32 {
        (value >> 32) as u32
    }
}

fn main() {
    let args = Cli::from_args();

    const BUCKETS_COUNT: usize = 512;

    ThreadPoolBuilder::new().num_threads(16).build_global();

    create_dir_all(&args.temp_dir);

    let k: usize = args.klen;
    let m: usize = args.mlen.unwrap_or(min(12, (k + 2) / 3));

    let buckets = if args.step <= StartingStep::MinimizerBucketing {
        Pipeline::minimizer_bucketing(args.input, args.temp_dir.as_path(), BUCKETS_COUNT, k, m)
    } else {
        Utils::generate_bucket_names(args.temp_dir.join("bucket"), BUCKETS_COUNT, Some("lz4"))
    };

    let RetType { sequences, hashes } = if args.step <= StartingStep::KmersMerge {
        Pipeline::kmers_merge(
            buckets,
            BUCKETS_COUNT,
            args.min_multiplicity,
            args.temp_dir.as_path(),
            k,
            m,
        )
    } else {
        RetType {
            sequences: Utils::generate_bucket_names(
                args.temp_dir.join("result"),
                BUCKETS_COUNT,
                Some("fa.lz4"),
            ),
            hashes: Utils::generate_bucket_names(args.temp_dir.join("hashes"), BUCKETS_COUNT, None),
        }
    };

    let mut links = if args.step <= StartingStep::HashesSorting {
        Pipeline::hashes_sorting(hashes, args.temp_dir.as_path(), BUCKETS_COUNT)
    } else {
        Utils::generate_bucket_names(args.temp_dir.join("links"), BUCKETS_COUNT, None)
    };

    let mut loop_iteration = args.number;

    let (unitigs_map, reads_map) = if args.step <= StartingStep::LinksCompaction {
        loop {
            println!("Iteration: {}", loop_iteration);

            let (new_links, result) = Pipeline::links_compaction(
                links,
                args.temp_dir.as_path(),
                BUCKETS_COUNT,
                loop_iteration,
            );
            links = new_links;
            match result {
                None => {}
                Some(result) => {
                    println!("Completed compaction with {} iters", loop_iteration);
                    break result;
                }
            }
            // exit(0);
            loop_iteration += 1;
        }
    } else {
        (
            Utils::generate_bucket_names(args.temp_dir.join("unitigs_map"), BUCKETS_COUNT, None),
            Utils::generate_bucket_names(args.temp_dir.join("results_map"), BUCKETS_COUNT, None),
        )
    };

    if args.step <= StartingStep::ReorganizeReads {
        let reorganized_reads = Pipeline::reorganize_reads(
            sequences,
            reads_map,
            args.temp_dir.as_path(),
            BUCKETS_COUNT,
            k,
            m,
        );
    }
}
