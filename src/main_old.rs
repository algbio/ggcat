#![allow(warnings)]
#[macro_use]
extern crate lazy_static;

use crate::binary_serializer::GzipFastaReader;
use crate::rolling_hash::RollingHash;
use std::env::args;
use crate::bloom_filter::BloomFilter;
use stopwatch::Stopwatch;
use std::collections::hash_map::DefaultHasher;
use std::hash::Hash;
use std::cmp::min;
use std::mem::MaybeUninit;
use std::alloc::Layout;
use crate::reads_freezer::ReadsFreezer;
use std::io::BufRead;
use std::net::Shutdown::Read;
use crate::pipeline::Pipeline;
use crate::progress::Progress;

mod binary_serializer;
mod single_dna_read;
mod single_dna_read_half;
mod utils;
mod bloom_filter;
mod rolling_hash;
mod reads_freezer;
mod cache_bucket;
mod progress;

mod pipeline;



//fn main() {
//    let mut progress = Progress::new();
//    let files: Vec<_> = args().skip(1).collect();
//
//    let reads = Pipeline::fasta_gzip_to_reads(files.as_slice());
//    let cut_n = Pipeline::cut_n(reads, 31);
//    cut_n.freeze("cut-n".to_string(), true);
//
//    println!("Finished elab, elapsed {:.2} seconds", progress.elapsed());
//
//        .for_each(|read| {
////    GzipFastaReader::process_file(files.as_slice()[0].clone(), |read| {
//
////       println!("{}", std::str::from_utf8(read).unwrap());
//        reads += read.len();
//        preads += read.len();
//
//        if preads > PRINT_INTERVAL {
//            println!("Reading {} speed: {:.1}M", reads, preads as f64 / partial_stopwatch.elapsed().as_secs_f64() / 1024.0 / 1024.0);
//            partial_stopwatch.reset();
//            partial_stopwatch.start();
//            preads = 0;
//        }

//    });
//}