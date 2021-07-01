#![feature(new_uninit, core_intrinsics)]
#![feature(is_sorted)]
#![feature(slice_group_by)]
#![feature(llvm_asm)]
#![feature(min_type_alias_impl_trait)]
#![feature(option_result_unwrap_unchecked)]
#![feature(specialization)]
#![feature(generic_associated_types)]
#![feature(const_fn_floating_point_arithmetic)]
#![feature(trait_alias)]
#![allow(warnings)]
#![feature(test)]

extern crate test;

use crate::pipeline::kmers_merge::RetType;
use crate::pipeline::Pipeline;
use crate::reads_freezer::WriterChannels::Pipe;
use crate::utils::Utils;
use rayon::ThreadPoolBuilder;
use std::cmp::min;
use std::fs::{create_dir_all, remove_file};
use std::intrinsics::exact_div;
use std::path::PathBuf;
use std::process::exit;
use structopt::{clap::ArgGroup, StructOpt};

mod benchmarks;
mod compressed_read;
mod debug_functions;
mod fast_rand_bool;
mod hash;
pub mod hash_entry;
mod hashes;
mod intermediate_storage;
pub mod libdeflate;
mod pipeline;
mod progress;
mod reads_freezer;
mod rolling_kseq_iterator;
mod rolling_minqueue;
mod rolling_quality_check;
mod sequences_reader;
mod types;
mod unitig_link;
#[macro_use]
mod utils;
mod varint;
mod vec_slice;

pub const DEFAULT_BUFFER_SIZE: usize = 1024 * 1024 * 24;

fn outputs_arg_group() -> ArgGroup<'static> {
    // As the attributes of the struct are executed before the struct
    // fields, we can't use .args(...), but we can use the group
    // attribute on the fields.
    ArgGroup::with_name("outputs").required(true)
}

use crate::compressed_read::CompressedRead;
use crate::hash::HashFunctionFactory;
use crate::hashes::cn_nthash::CanonicalNtHashIteratorFactory;
use crate::hashes::cn_seqhash::{CanonicalSeqHashFactory, CanonicalSeqHashIterator};
use crate::hashes::fw_nthash::{ForwardNtHashIterator, ForwardNtHashIteratorFactory};
use crate::hashes::fw_seqhash::ForwardSeqHashFactory;
use crate::reads_freezer::ReadsFreezer;
use crate::sequences_reader::{FastaSequence, SequencesReader};
use clap::arg_enum;
use parallel_processor::phase_times_monitor::PHASES_TIMES_MONITOR;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Mutex;
arg_enum! {
    #[derive(Debug, PartialOrd, PartialEq)]
    enum StartingStep {
        MinimizerBucketing = 0,
        KmersMerge = 1,
        HashesSorting = 2,
        LinksCompaction = 3,
        ReorganizeReads = 4,
        BuildUnitigs = 5
    }
}

#[derive(StructOpt, Debug)]
struct Cli {
    /// The input files
    #[structopt(required = true)]
    input: Vec<PathBuf>,

    #[structopt(short)]
    debug_reverse: bool,

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

    /// Minimum correctness probability for each kmer (using fastq quality checks)
    #[structopt(short = "q", long = "quality-threshold")]
    quality_threshold: Option<f64>,

    /// Keep intermediate temporary files for debugging purposes
    #[structopt(long = "keep-temp-files")]
    keep_temp_files: bool,

    #[structopt(short = "n", long, default_value = "0")]
    number: usize, // Tests built bloom filter against this file for coverage tests
    // #[structopt(short = "f", requires = "elabbloom")]
    // coverage: Option<String>,
    #[structopt(short = "j", long, default_value = "16")]
    threads_count: usize,

    #[structopt(short = "o", long = "output-file", default_value = "output.fasta.lz4")]
    output_file: PathBuf,

    // Enables output compression
    // #[structopt(short, requires = "output")]
    // compress: bool
    #[structopt(short = "x", long, default_value = "MinimizerBucketing")]
    step: StartingStep,
}

type BucketingHash = CanonicalNtHashIteratorFactory; // CanonicalNtHashIteratorFactory; // ForwardNtHashIteratorFactory;
type MergingHash = CanonicalSeqHashFactory; // CanonicalSeqHashFactory; // ForwardNtHashIteratorFactory; //SeqHashFactory; //

static KEEP_FILES: AtomicBool = AtomicBool::new(false);

fn main() {
    let args: Cli = Cli::from_args();

    // Increase the maximum allowed number of open files
    fdlimit::raise_fd_limit();

    const BUCKETS_COUNT: usize = 512;

    KEEP_FILES.store(args.keep_temp_files, Ordering::Relaxed);

    if args.debug_reverse {
        for file in args.input {
            let mut output = ReadsFreezer::optfile_splitted_compressed_lz4("complementary.lz4");
            let mut tmp_vec = Vec::new();
            SequencesReader::process_file_extended(
                &file,
                |x| {
                    const COMPL: [u8; 256] = {
                        let mut letters = [b'N'; 256];

                        letters[b'A' as usize] = b'T';
                        letters[b'C' as usize] = b'G';
                        letters[b'G' as usize] = b'C';
                        letters[b'T' as usize] = b'A';

                        letters
                    };
                    tmp_vec.clear();
                    tmp_vec.extend(x.seq.iter().map(|x| COMPL[*x as usize]).rev());

                    output.add_read(FastaSequence {
                        ident: x.ident,
                        seq: &tmp_vec[..],
                        qual: None,
                    })
                },
                false,
            );
            output.finalize();
        }
        return;
    }

    ThreadPoolBuilder::new()
        .num_threads(args.threads_count)
        .build_global();

    create_dir_all(&args.temp_dir);

    let k: usize = args.klen;
    let m: usize = args.mlen.unwrap_or(min(12, (k + 2) / 3));

    PHASES_TIMES_MONITOR.write().init();

    let buckets = if args.step <= StartingStep::MinimizerBucketing {
        Pipeline::minimizer_bucketing::<BucketingHash>(
            args.input,
            args.temp_dir.as_path(),
            BUCKETS_COUNT,
            args.threads_count,
            k,
            m,
            args.quality_threshold,
        )
    } else {
        Utils::generate_bucket_names(args.temp_dir.join("bucket"), BUCKETS_COUNT, Some("lz4"))
    };

    let RetType { sequences, hashes } = if args.step <= StartingStep::KmersMerge {
        Pipeline::kmers_merge::<BucketingHash, MergingHash, _>(
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
                Some("fasta.lz4"),
            ),
            hashes: Utils::generate_bucket_names(args.temp_dir.join("hashes"), BUCKETS_COUNT, None),
        }
    };

    let mut links = if args.step <= StartingStep::HashesSorting {
        Pipeline::hashes_sorting::<MergingHash, _>(hashes, args.temp_dir.as_path(), BUCKETS_COUNT)
    } else {
        Utils::generate_bucket_names(args.temp_dir.join("links"), BUCKETS_COUNT, None)
    };

    let mut loop_iteration = args.number;

    let unames =
        Utils::generate_bucket_names(args.temp_dir.join("unitigs_map"), BUCKETS_COUNT, None);
    let rnames =
        Utils::generate_bucket_names(args.temp_dir.join("results_map"), BUCKETS_COUNT, None);

    let (unitigs_map, reads_map) = if args.step <= StartingStep::LinksCompaction {
        for file in unames {
            remove_file(file);
        }

        for file in rnames {
            remove_file(file);
        }

        if loop_iteration != 0 {
            links = Utils::generate_bucket_names(
                args.temp_dir.join(format!("linksi{}", loop_iteration - 1)),
                BUCKETS_COUNT,
                None,
            );
        }

        PHASES_TIMES_MONITOR
            .write()
            .start_phase("phase: links compaction".to_string());

        let result = loop {
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
            loop_iteration += 1;
        };

        if !KEEP_FILES.load(Ordering::Relaxed) {
            for link_file in links {
                std::fs::remove_file(&link_file);
            }
        }
        result
    } else {
        (unames, rnames)
    };

    let mut final_unitigs_file = Mutex::new(match args.output_file.extension() {
        Some(ext) => match ext.to_string_lossy().to_string().as_str() {
            "lz4" => ReadsFreezer::optfile_splitted_compressed_lz4(&args.output_file),
            "gz" => ReadsFreezer::optfile_splitted_compressed(&args.output_file),
            _ => ReadsFreezer::optifile_splitted(&args.output_file),
        },
        None => ReadsFreezer::optifile_splitted(&args.output_file),
    });

    let reorganized_reads = if args.step <= StartingStep::ReorganizeReads {
        Pipeline::reorganize_reads(
            sequences,
            reads_map,
            args.temp_dir.as_path(),
            &final_unitigs_file,
            BUCKETS_COUNT,
            k,
            m,
        )
    } else {
        Utils::generate_bucket_names(
            args.temp_dir.join("reads_bucket"),
            BUCKETS_COUNT,
            Some("lz4"),
        )
    };

    if args.step <= StartingStep::BuildUnitigs {
        Pipeline::build_unitigs(
            reorganized_reads,
            unitigs_map,
            args.temp_dir.as_path(),
            &final_unitigs_file,
            BUCKETS_COUNT,
            k,
            m,
        );
    }

    std::fs::remove_dir(args.temp_dir.as_path());

    final_unitigs_file.into_inner().unwrap().finalize();

    PHASES_TIMES_MONITOR
        .write()
        .print_stats("Compacted De Brujin graph construction completed.".to_string());

    println!("Final output saved to: {}", args.output_file.display());
}
