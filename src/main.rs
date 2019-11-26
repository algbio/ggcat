#![allow(warnings)]


use crate::progress::Progress;
use crate::pipeline::Pipeline;
use crate::reads_freezer::ReadsFreezer;
use structopt::StructOpt;
use nix::unistd::PathconfVar::PIPE_BUF;
use crate::utils::{cast_static, Utils};
use std::time::Duration;

mod gzip_fasta_reader;
mod utils;
mod bloom_filter;
mod reads_freezer;
mod cache_bucket;
mod progress;
mod pipeline;

#[derive(StructOpt)]
enum Mode {
    Flat,
    Preprocess
}

#[derive(StructOpt)]
struct Cli {
    /// The input files
    inputs: Vec<String>,

    /// The output file
    #[structopt(short, long)]
    output: String,

    /// Enables processing from gzipped fasta
    #[structopt(short)]
    gzip_fasta: bool,

    /// Removes N and splits reads accordingly
    #[structopt(short)]
    nsplit: bool,

    /// Specifies the k-mers length, mandatory with -n
    #[structopt(short)]
    klen: Option<usize>,

    /// Enables buckets processing with the specified number of buckets
    #[structopt(short)]
    bucketing: Option<usize>,

    /// Enables output compression
    #[structopt(short)]
    compress: bool,
}

fn main() {
    let mut progress = Progress::new();

//    ctrlc::set_handler(move || {
//        println!("received Ctrl+C!");
//    });

    let args = Cli::from_args();

    let mut current: &ReadsFreezer;

    let reads;
    let cut_n;

    reads = if args.gzip_fasta {
        Pipeline::fasta_gzip_to_reads(args.inputs.as_slice())
    }
    else {
        Pipeline::file_freezers_to_reads(args.inputs.as_slice())
    };
    current = cast_static(&reads);

    if args.nsplit {

        if args.klen.is_none() {
            println!("-k is mandatory with -n");
            return;
        }

        cut_n = Pipeline::cut_n(current, args.klen.unwrap());
        current = cast_static(&cut_n);
    }

    if let Some(bnum) = args.bucketing {
        if args.klen.is_none() {
            println!("-k is mandatory with -b");
            return;
        }

        Pipeline::make_buckets(current, args.klen.unwrap(), bnum, "buckets/bucket-");
    }
    else {
        current.freeze(args.output, args.compress);
    }

    Utils::join_all();
    println!("Finished elab, elapsed {:.2} seconds", progress.elapsed());
}