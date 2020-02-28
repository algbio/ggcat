#![allow(warnings)]


use crate::progress::Progress;
use crate::pipeline::Pipeline;
use crate::reads_freezer::ReadsFreezer;
use structopt::StructOpt;
use nix::unistd::PathconfVar::PIPE_BUF;
use crate::utils::{cast_static, Utils};
use std::time::Duration;
use structopt::clap::ArgGroup;
use std::net::Shutdown::Read;
use std::fs::File;
use crate::gzip_fasta_reader::GzipFastaReader;
use crate::rolling_kmers_iterator::RollingKmersIterator;
use crate::nthash::RollingNtHashIterator;
use ::nthash::NtHashIterator;
use crate::rolling_quality_check::RollingQualityCheck;
use std::cmp::{max, min};
use rayon::iter::*;
use std::sync::atomic::Ordering;
use std::sync::atomic::*;

mod gzip_fasta_reader;
mod utils;
mod bloom_filter;
mod reads_freezer;
mod cache_bucket;
mod progress;
mod pipeline;
mod bloom_processing;
mod rolling_kmers_iterator;
mod nthash;
mod rolling_quality_check;

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
    elabbloom: bool,

    /// Enables buckets processing with the specified number of buckets
    #[structopt(short, group = "outputs", requires = "klen")]
    bucketing: Option<usize>,

    /// Tests built bloom filter against this file for coverage tests
    #[structopt(short = "f", requires = "elabbloom")]
    coverage: Option<String>,

    /// Decimate bloom filter
    #[structopt(short, requires = "elabbloom")]
    decimated: bool,

    /// Writes out all minimizer k-mers
    #[structopt(short, requires = "klen")]
    minimizers: bool,

    /// Enables output compression
    #[structopt(short, requires = "output")]
    compress: bool,

    /// Processes a bucket
    #[structopt(short, requires = "output", requires = "klen")]
    process_bucket: bool,
}


#[derive(StructOpt, Debug)]
struct Cli2 {
    /// The input files
    input: Vec<String>
}


fn main() {

    let args = Cli2::from_args();

    let mut total_at = AtomicU64::new(0);
    let mut correct_at = AtomicU64::new(0);

    args.input.par_iter().for_each(|input| {

        let mut total = 0;
        let mut correct = 0;

        let mut nthash = RollingNtHashIterator::new();
        let mut qcheck = RollingQualityCheck::new();
        GzipFastaReader::process_file_extended(input.to_string(), |x| {
            let qual = x.qual;
//
//        let mut prob_log = 0;


            let mut rolling_iter = RollingKmersIterator::new(x.seq, 32);
            let mut rolling_qiter = RollingKmersIterator::new(x.qual, x.qual.len());
//        let mut ntiter = NtHashIterator::new(x.seq, 32);

            let mut r = 0;
            for x in rolling_iter.iter(&mut nthash) {//.zip(ntiter.unwrap().iter()) {
                r ^= x;
            }

            let mut prob_log = rolling_qiter.iter(&mut qcheck).min().unwrap_or(std::u32::MAX);

//        for qb in qual {
////            println!("{}", -(0.1 * ((*qb as f32) - 33.0)));
//        }
            total += 1;

            let threshold: f64 = 0.95;
            let threshold_log = (-threshold.log10() * 1048576.0) as u32;

//        println!("{} < {}", prob_log, threshold_log);

            if prob_log < threshold_log {
                correct += 1;
                if correct % 100000 == 0 {
                    println!("Prob: {:.2}% correct => LEN: {} {}", (10.0 as f64).powf(-(prob_log as f64) / 1048576.0), x.seq.len(), r);
                    println!("{}", String::from_utf8_lossy(x.seq));
                    println!("{}", String::from_utf8_lossy(x.qual));
                    println!("Correct/Total = {}/{} ===> {:.2}%", correct, total, (correct as f64) / (total as f64) * 100.0);
                }
            }
        });
        total_at.fetch_add(total as u64, Ordering::Relaxed);
        correct_at.fetch_add(correct as u64, Ordering::Relaxed);
    });

    let correct = correct_at.load(Ordering::Relaxed);
    let total = total_at.load(Ordering::Relaxed);

    println!("Correct/Total = {}/{} ===> {:.2}%", correct, total, (correct as f64) / (total as f64) * 100.0);

    return;


    let mut progress = Progress::new();

//    ctrlc::set_handler(move || {
//        println!("received Ctrl+C!");
//    });

    let args = Cli::from_args();

    let mut current: &ReadsFreezer;

    let reads;
    let cut_n;
    let minim;
    let mut merge: Vec<Box<ReadsFreezer>> = Vec::new();

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

    if args.process_bucket {
        let kvalue = args.klen.unwrap();
//        for i in 1..1 {
            merge.push(Box::new(Pipeline::merge_bucket(current, kvalue, 256, 0)));
            current = cast_static(&merge.last().unwrap());
//        }
    }

    if args.minimizers {
        minim = Pipeline::save_minimals(current, args.klen.unwrap());
        current = cast_static(&minim);
    }

    if args.elabbloom {
        let mut filter = Pipeline::bloom_filter(current, args.klen.unwrap(), args.decimated);
        if let Some(ctest) = args.coverage {
//            Pipeline::bloom_check_coverage(ReadsFreezer::from_file(ctest.clone()), &mut filter);
            Pipeline::bloom_check_coverage(ReadsFreezer::from_file(ctest), &mut filter);
        }
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