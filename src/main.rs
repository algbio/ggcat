use crate::progress::Progress;
use std::env::args;
use crate::pipeline::Pipeline;
use crate::reads_freezer::ReadsFreezer;

mod gzip_fasta_reader;
mod utils;
mod bloom_filter;
mod reads_freezer;
mod cache_bucket;
mod progress;
mod pipeline;

fn preprocess(files: &Vec<String>, outfile: &str) {
    let reads: ReadsFreezer = Pipeline::fasta_gzip_to_reads(files.as_slice());
    let cut_n = Pipeline::cut_n(reads, 31);
    cut_n.freeze(outfile.to_string(), true);
}

fn main() {
    let mut progress = Progress::new();
    let files: Vec<_> = args().skip(1).collect();

    preprocess(&files, "without-n");

    println!("Finished elab, elapsed {:.2} seconds", progress.elapsed());
}