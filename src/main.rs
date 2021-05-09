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
use std::path::PathBuf;
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

#[derive(StructOpt, Debug)]
struct Cli {
    /// The input files
    input: Vec<PathBuf>,

    /// Specifies the k-mers length
    #[structopt(short)]
    klen: usize,

    /// Specifies the m-mers (minimizers) length, defaults to min(12, ceil(K / 2))
    #[structopt(short)]
    mlen: Option<usize>,

    /// Minimum multiplicity required to keep a kmer
    #[structopt(short = "s", long = "min-multiplicity")]
    min_multiplicity: Option<usize>,

    /// Directory for temporary files (default .temp_files)
    #[structopt(short = "t", long = "temp-dir")]
    temp_dir: Option<PathBuf>,

    #[structopt(short = "n", long)]
    number: usize, // Tests built bloom filter against this file for coverage tests
                   // #[structopt(short = "f", requires = "elabbloom")]
                   // coverage: Option<String>,

                   // Enables output compression
                   // #[structopt(short, requires = "output")]
                   // compress: bool
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

    let temp_dir = args.temp_dir.unwrap_or(PathBuf::from(".temp_files"));

    create_dir_all(&temp_dir);

    let k: usize = args.klen;
    let m: usize = args.mlen.unwrap_or(min(12, (k + 2) / 3));
    let min_multiplicity: usize = args.min_multiplicity.unwrap_or(2);

    let buckets = Pipeline::minimizer_bucketing(args.input, temp_dir.as_path(), BUCKETS_COUNT, k, m);

    let RetType { sequences, hashes } = Pipeline::kmers_merge(
        buckets,
        BUCKETS_COUNT,
        min_multiplicity,
        temp_dir.as_path(),
        k,
        m,
    );

    // let hashes = args.input;

    let mut links = Pipeline::hashes_sorting(hashes, temp_dir.as_path(), BUCKETS_COUNT);

    let mut loop_iteration = args.number;

    let links_result;

    loop {
        let (new_links, result) =
            Pipeline::links_compaction(links, temp_dir.as_path(), BUCKETS_COUNT, loop_iteration);
        links = new_links;
        match result {
            None => {}
            Some(result) => {
                links_result = result;
                break;
            }
        }
        loop_iteration += 1;
    }
    println!("Completed compaction with {} iters", loop_iteration);

    let (unitig_map, reads_map) = links_result;

    // let sequences = args.input;
    // // let reads_map = Utils::generate_bucket_names(".temp_files/results_map", BUCKETS_COUNT);
    //
    // let reorganized_reads = Pipeline::reorganize_reads(
    //     sequences,
    //     reads_map,
    //     temp_dir.as_path(),
    //     BUCKETS_COUNT,
    //     k,
    //     m,
    // );
}
