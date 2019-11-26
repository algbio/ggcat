#![allow(warnings)]


use crate::progress::Progress;
use crate::pipeline::Pipeline;
use crate::reads_freezer::ReadsFreezer;
use structopt::StructOpt;

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

    /// Enables buckets processing
    #[structopt(short)]
    bucketing: bool,
}

fn main() {
    let mut progress = Progress::new();

    let args = Cli::from_args();

    let mut current: &ReadsFreezer;

    let reads;
    let cut_n;

    reads = if args.gzip_fasta {
        Pipeline::fasta_gzip_to_reads(args.inputs.as_slice())
    }
    else {
        Pipeline::fasta_gzip_to_reads(args.inputs.as_slice())
    };
    current = &reads;

    if args.nsplit {

        if args.klen.is_none() {
            println!("-k is mandatory with -n");
            return;
        }

        cut_n = Pipeline::cut_n(reads, args.klen.unwrap());
        current = &cut_n;
    }

    current.freeze(args.output, true);

    println!("Finished elab, elapsed {:.2} seconds", progress.elapsed());
}