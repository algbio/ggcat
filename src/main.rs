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
use crate::reads_storage::WriterChannels::Pipe;
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
mod reads_storage;
mod rolling_kseq_iterator;
mod rolling_minqueue;
mod rolling_quality_check;
mod sequences_reader;
mod types;
mod unitig_link;
#[macro_use]
mod utils;
mod assembler;
mod varint;
mod vec_slice;

pub const DEFAULT_BUFFER_SIZE: usize = 1024 * 1024 * 24;

fn outputs_arg_group() -> ArgGroup<'static> {
    // As the attributes of the struct are executed before the struct
    // fields, we can't use .args(...), but we can use the group
    // attribute on the fields.
    ArgGroup::with_name("outputs").required(true)
}

use crate::assembler::run_assembler;
use crate::compressed_read::CompressedRead;
use crate::hash::HashFunctionFactory;
use crate::hashes::cn_nthash::CanonicalNtHashIteratorFactory;
use crate::hashes::cn_seqhash;
use crate::hashes::cn_seqhash::u64::CanonicalSeqHashFactory;
use crate::hashes::fw_nthash::{ForwardNtHashIterator, ForwardNtHashIteratorFactory};
use crate::hashes::fw_seqhash::u64::ForwardSeqHashFactory;
use crate::reads_storage::ReadsStorage;
use crate::sequences_reader::{FastaSequence, SequencesReader};
use clap::arg_enum;
use parallel_processor::phase_times_monitor::PHASES_TIMES_MONITOR;
use parking_lot::Mutex;
use std::io::Write;
use std::panic;
use std::sync::atomic::{AtomicBool, Ordering};

arg_enum! {
    #[derive(Debug, PartialOrd, PartialEq)]
    pub enum StartingStep {
        MinimizerBucketing = 0,
        KmersMerge = 1,
        HashesSorting = 2,
        LinksCompaction = 3,
        ReorganizeReads = 4,
        BuildUnitigs = 5
    }
}

arg_enum! {
    #[derive(Copy, Clone, Debug, PartialOrd, PartialEq)]
    pub enum HashType {
        Auto = 0,
        SeqHash = 1,
        RabinKarp32 = 2,
        RabinKarp64 = 3,
        RabinKarp128 = 4
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
    number: usize,

    #[structopt(short = "j", long, default_value = "16")]
    threads_count: usize,

    #[structopt(short = "o", long = "output-file", default_value = "output.fasta.lz4")]
    output_file: PathBuf,

    /// Hash type used to identify kmers
    #[structopt(short = "w", long, default_value = "Auto")]
    hash_type: HashType,

    /// Treats reverse complementary kmers as different
    #[structopt(short = "f", long)]
    forward_only: bool,

    // Enables output compression
    // #[structopt(short, requires = "output")]
    // compress: bool
    #[structopt(short = "x", long, default_value = "MinimizerBucketing")]
    step: StartingStep,
}

static KEEP_FILES: AtomicBool = AtomicBool::new(false);

fn run_assembler_from_args<
    BucketingHash: HashFunctionFactory,
    MergingHash: HashFunctionFactory,
    const BUCKETS_COUNT: usize,
>(
    args: Cli,
) {
    run_assembler::<BucketingHash, MergingHash, BUCKETS_COUNT>(
        args.klen,
        args.mlen.unwrap_or(min(12, (args.klen + 2) / 3)),
        args.step,
        args.input,
        args.output_file,
        args.temp_dir,
        args.threads_count,
        args.min_multiplicity,
        args.quality_threshold,
        Some(args.number),
    );
}

fn main() {
    let args: Cli = Cli::from_args();

    panic::set_hook(Box::new(move |info| {
        let stdout = std::io::stdout();
        let mut _lock = stdout.lock();

        let stderr = std::io::stderr();
        let mut err_lock = stderr.lock();

        writeln!(
            err_lock,
            "Thread panicked at location: {:?}",
            info.location()
        );
        if let Some(s) = info.payload().downcast_ref::<&str>() {
            writeln!(err_lock, "Panic payload: {:?}", s);
        }

        exit(1);
    }));

    // Increase the maximum allowed number of open files
    fdlimit::raise_fd_limit();

    KEEP_FILES.store(args.keep_temp_files, Ordering::Relaxed);

    if args.debug_reverse {
        for file in args.input {
            let mut output = ReadsStorage::optfile_splitted_compressed_lz4("complementary.lz4");
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

    const BUCKETS_COUNT: usize = 512;

    let hash_type = match args.hash_type {
        HashType::Auto => {
            if args.klen <= 64 {
                HashType::SeqHash
            } else {
                HashType::RabinKarp128
            }
        }
        x => x,
    };

    use hashes::*;

    match hash_type {
        HashType::SeqHash => {
            let k = args.klen;

            if k <= 8 {
                if args.forward_only {
                    run_assembler_from_args::<
                        CanonicalNtHashIteratorFactory,
                        cn_seqhash::u16::CanonicalSeqHashFactory,
                        BUCKETS_COUNT,
                    >(args);
                } else {
                    run_assembler_from_args::<
                        CanonicalNtHashIteratorFactory,
                        cn_seqhash::u16::CanonicalSeqHashFactory,
                        BUCKETS_COUNT,
                    >(args)
                }
            } else if k <= 16 {
                if args.forward_only {
                    run_assembler_from_args::<
                        ForwardNtHashIteratorFactory,
                        fw_seqhash::u32::ForwardSeqHashFactory,
                        BUCKETS_COUNT,
                    >(args);
                } else {
                    run_assembler_from_args::<
                        CanonicalNtHashIteratorFactory,
                        cn_seqhash::u32::CanonicalSeqHashFactory,
                        BUCKETS_COUNT,
                    >(args)
                }
            } else if k <= 32 {
                if args.forward_only {
                    run_assembler_from_args::<
                        ForwardNtHashIteratorFactory,
                        fw_seqhash::u64::ForwardSeqHashFactory,
                        BUCKETS_COUNT,
                    >(args);
                } else {
                    run_assembler_from_args::<
                        CanonicalNtHashIteratorFactory,
                        cn_seqhash::u64::CanonicalSeqHashFactory,
                        BUCKETS_COUNT,
                    >(args)
                }
            } else if k <= 64 {
                if args.forward_only {
                    run_assembler_from_args::<
                        ForwardNtHashIteratorFactory,
                        fw_seqhash::u128::ForwardSeqHashFactory,
                        BUCKETS_COUNT,
                    >(args);
                } else {
                    run_assembler_from_args::<
                        CanonicalNtHashIteratorFactory,
                        cn_seqhash::u128::CanonicalSeqHashFactory,
                        BUCKETS_COUNT,
                    >(args)
                }
            } else {
                panic!("Cannot use sequence hash for k > 64!");
            }
        }
        HashType::RabinKarp32 => {
            if args.forward_only {
                run_assembler_from_args::<
                    ForwardNtHashIteratorFactory,
                    fw_rkhash::u32::ForwardRabinKarpHashFactory,
                    BUCKETS_COUNT,
                >(args);
            } else {
                run_assembler_from_args::<
                    CanonicalNtHashIteratorFactory,
                    cn_rkhash::u32::CanonicalRabinKarpHashFactory,
                    BUCKETS_COUNT,
                >(args)
            }
        }
        HashType::RabinKarp64 => {
            if args.forward_only {
                run_assembler_from_args::<
                    ForwardNtHashIteratorFactory,
                    fw_rkhash::u64::ForwardRabinKarpHashFactory,
                    BUCKETS_COUNT,
                >(args);
            } else {
                run_assembler_from_args::<
                    CanonicalNtHashIteratorFactory,
                    cn_rkhash::u64::CanonicalRabinKarpHashFactory,
                    BUCKETS_COUNT,
                >(args)
            }
        }
        HashType::RabinKarp128 => {
            if args.forward_only {
                run_assembler_from_args::<
                    ForwardNtHashIteratorFactory,
                    fw_rkhash::u128::ForwardRabinKarpHashFactory,
                    BUCKETS_COUNT,
                >(args);
            } else {
                run_assembler_from_args::<
                    CanonicalNtHashIteratorFactory,
                    cn_rkhash::u128::CanonicalRabinKarpHashFactory,
                    BUCKETS_COUNT,
                >(args)
            }
        }
        HashType::Auto => {
            unreachable!()
        }
    }
}
