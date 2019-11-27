#![allow(warnings)]


use crate::progress::Progress;
use crate::pipeline::Pipeline;
use crate::reads_freezer::ReadsFreezer;
use structopt::StructOpt;
use nix::unistd::PathconfVar::PIPE_BUF;
use crate::utils::{cast_static, Utils};
use std::time::Duration;
use structopt::clap::ArgGroup;


mod gzip_fasta_reader;
mod utils;
mod bloom_filter;
mod reads_freezer;
mod cache_bucket;
mod progress;
mod pipeline;
mod bloom_processing;

#[derive(StructOpt)]
enum Mode {
    Flat,
    Preprocess
}

fn outputs_arg_group() -> ArgGroup<'static> {
    // As the attributes of the struct are executed before the struct
    // fields, we can't use .args(...), but we can use the group
    // attribute on the fields.
    ArgGroup::with_name("outputs").required(true)
}

#[derive(StructOpt, Debug)]
#[structopt(group = outputs_arg_group())]
struct Cli {
    /// The input files
    inputs: Vec<String>,

    /// The output file
    #[structopt(short, long, group = "outputs")]
    output: Option<String>,

    /// Enables processing from gzipped fasta
    #[structopt(short)]
    gzip_fasta: bool,

    /// Removes N and splits reads accordingly
    #[structopt(short, requires = "klen")]
    nsplit: bool,

    /// Specifies the k-mers length, mandatory with -nbl
    #[structopt(short)]
    klen: Option<usize>,

    /// Bloom filter elaboration
    #[structopt(short, group = "outputs", requires = "klen")]
    elab_bloom: bool,

    /// Enables buckets processing with the specified number of buckets
    #[structopt(short, group = "outputs", requires = "klen")]
    bucketing: Option<usize>,

    /// Writes out all minimizer k-mers
    #[structopt(short, requires = "klen")]
    minimizers: bool,

    /// Enables output compression
    #[structopt(short, requires = "output")]
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
    let minim;

    reads = if args.gzip_fasta {
        Pipeline::fasta_gzip_to_reads(args.inputs.as_slice())
    }
    else {
        Pipeline::file_freezers_to_reads(args.inputs.as_slice())
    };
    current = cast_static(&reads);

    if args.nsplit {
        cut_n = Pipeline::cut_n(current, args.klen.unwrap());
        current = cast_static(&cut_n);
    }

    if args.minimizers {
        minim = Pipeline::save_minimals(current, args.klen.unwrap());
        current = cast_static(&minim);
    }

    if args.elab_bloom {
        Pipeline::bloom_filter(current, args.klen.unwrap());
    }
    else if let Some(bnum) = args.bucketing {
        Pipeline::make_buckets(current, args.klen.unwrap(), bnum, "buckets/bucket-");
    }
    else {
        current.freeze(args.output.unwrap(), args.compress);
    }

    Utils::join_all();
    println!("Finished elab {}, elapsed {:.2} seconds", args.klen.unwrap_or_else(|| 0), progress.elapsed());
}