#![feature(new_uninit, core_intrinsics)]
#![feature(is_sorted)]
#![feature(slice_group_by)]
#![feature(llvm_asm)]
#![feature(min_type_alias_impl_trait)]
#![feature(option_result_unwrap_unchecked)]
#![allow(warnings)]
#![feature(test)]

extern crate test;

use crate::pipeline::kmers_merge::RetType;
use crate::pipeline::Pipeline;
use crate::rolling_minqueue::GenericsFunctional;
use rayon::ThreadPoolBuilder;
use std::cmp::min;
use std::path::PathBuf;
use structopt::{clap::ArgGroup, StructOpt};
use std::fs::create_dir_all;

mod benchmarks;
mod binary_writer;
mod compressed_read;
mod debug_functions;
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
    input: Vec<String>,

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
    // Tests built bloom filter against this file for coverage tests
    // #[structopt(short = "f", requires = "elabbloom")]
    // coverage: Option<String>,

    // Enables output compression
    // #[structopt(short, requires = "output")]
    // compress: bool,
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

    let buckets =
        Pipeline::minimizer_bucketing(args.input, temp_dir.as_path(), BUCKETS_COUNT, k, m);

    let RetType { sequences, hashes } = Pipeline::kmers_merge(
        buckets,
        BUCKETS_COUNT,
        min_multiplicity,
        temp_dir.as_path(),
        k,
        m,
    );

    let links = Pipeline::hashes_sorting(hashes, temp_dir.as_path(), BUCKETS_COUNT);

    let _comp_links = Pipeline::links_compaction(links, temp_dir.as_path(), BUCKETS_COUNT, 0);
}
