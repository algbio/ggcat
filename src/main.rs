#![feature(new_uninit, core_intrinsics, type_alias_impl_trait)]
#![feature(is_sorted, thread_local, panic_info_message)]
#![feature(slice_group_by, raw_vec_internals)]
#![feature(llvm_asm)]
#![feature(option_result_unwrap_unchecked)]
#![feature(specialization)]
#![feature(generic_associated_types)]
#![feature(const_fn_floating_point_arithmetic)]
#![feature(trait_alias)]
#![allow(warnings)]
#![feature(test)]
#![feature(slice_partition_dedup)]

extern crate alloc;
extern crate test;

use crate::assemble_pipeline::AssemblePipeline;
use crate::utils::Utils;
use rayon::ThreadPoolBuilder;
use std::cmp::min;
use std::fs::{create_dir_all, remove_file, File};
use std::intrinsics::exact_div;
use std::path::PathBuf;
use std::process::exit;
use structopt::{clap::ArgGroup, StructOpt};

mod assemble_pipeline;
mod benchmarks;
mod hashes;
pub mod libdeflate;
mod types;
#[macro_use]
mod utils;
mod assembler;
mod colors;
mod io;
mod pipeline_common;
mod query_pipeline;
mod rolling;

pub const DEFAULT_BUFFER_SIZE: usize = 1024 * 1024 * 24;

fn outputs_arg_group() -> ArgGroup<'static> {
    // As the attributes of the struct are executed before the struct
    // fields, we can't use .args(...), but we can use the group
    // attribute on the fields.
    ArgGroup::with_name("outputs").required(true)
}

use crate::assembler::run_assembler;
use crate::colors::colors_manager::ColorsManager;
use crate::colors::default_colors_manager::DefaultColorsManager;
use crate::colors::storage::run_length::RunLengthColorsSerializer;
use crate::colors::storage::serializer::ColorsSerializer;
use crate::colors::ColorIndexType;
use crate::hashes::cn_nthash::CanonicalNtHashIteratorFactory;
use crate::hashes::cn_seqhash;
use crate::hashes::cn_seqhash::u64::CanonicalSeqHashFactory;
use crate::hashes::fw_nthash::{ForwardNtHashIterator, ForwardNtHashIteratorFactory};
use crate::hashes::fw_seqhash::u64::ForwardSeqHashFactory;
use crate::hashes::HashFunctionFactory;
use crate::io::reads_reader::ReadsReader;
use crate::io::reads_writer::ReadsWriter;
use crate::io::sequences_reader::{FastaSequence, SequencesReader};
use crate::io::varint::decode_varint;
use crate::utils::compressed_read::CompressedRead;
use byteorder::ReadBytesExt;
use clap::arg_enum;
use parallel_processor::phase_times_monitor::PHASES_TIMES_MONITOR;
use parking_lot::Mutex;
use std::io::{BufRead, BufReader, Write};
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
    #[structopt(long = "print-matches")]
    /// Debug print matches of a color index
    print_matches: Option<ColorIndexType>,

    /// The input files
    input: Vec<PathBuf>,

    /// The lists of input files
    #[structopt(short = "l", long = "input-lists")]
    input_lists: Vec<PathBuf>,

    #[structopt(short)]
    debug_reverse: bool,

    /// Enable colors
    #[structopt(short, long)]
    colors: bool,

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
    ColorsImpl: ColorsManager,
    const BUCKETS_COUNT: usize,
>(
    args: Cli,
) {
    let mut inputs = args.input.clone();

    for list in args.input_lists {
        for input in BufReader::new(File::open(list).unwrap()).lines() {
            if let Ok(input) = input {
                inputs.push(PathBuf::from(input));
            }
        }
    }

    if inputs.is_empty() {
        println!("ERROR: No input files specified!");
        exit(1);
    }

    run_assembler::<BucketingHash, MergingHash, ColorsImpl, BUCKETS_COUNT>(
        args.klen,
        args.mlen.unwrap_or(min(12, (args.klen + 2) / 3)),
        args.step,
        inputs,
        args.output_file,
        args.temp_dir,
        args.threads_count,
        args.min_multiplicity,
        args.quality_threshold,
        Some(args.number),
    );
}

fn dispatch_assembler_hash_type<ColorsImpl: ColorsManager, const BUCKETS_COUNT: usize>(args: Cli) {
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
                        ColorsImpl,
                        BUCKETS_COUNT,
                    >(args);
                } else {
                    run_assembler_from_args::<
                        CanonicalNtHashIteratorFactory,
                        cn_seqhash::u16::CanonicalSeqHashFactory,
                        ColorsImpl,
                        BUCKETS_COUNT,
                    >(args)
                }
            } else if k <= 16 {
                if args.forward_only {
                    run_assembler_from_args::<
                        ForwardNtHashIteratorFactory,
                        fw_seqhash::u32::ForwardSeqHashFactory,
                        ColorsImpl,
                        BUCKETS_COUNT,
                    >(args);
                } else {
                    run_assembler_from_args::<
                        CanonicalNtHashIteratorFactory,
                        cn_seqhash::u32::CanonicalSeqHashFactory,
                        ColorsImpl,
                        BUCKETS_COUNT,
                    >(args)
                }
            } else if k <= 32 {
                if args.forward_only {
                    run_assembler_from_args::<
                        ForwardNtHashIteratorFactory,
                        fw_seqhash::u64::ForwardSeqHashFactory,
                        ColorsImpl,
                        BUCKETS_COUNT,
                    >(args);
                } else {
                    run_assembler_from_args::<
                        CanonicalNtHashIteratorFactory,
                        cn_seqhash::u64::CanonicalSeqHashFactory,
                        ColorsImpl,
                        BUCKETS_COUNT,
                    >(args)
                }
            } else if k <= 64 {
                if args.forward_only {
                    run_assembler_from_args::<
                        ForwardNtHashIteratorFactory,
                        fw_seqhash::u128::ForwardSeqHashFactory,
                        ColorsImpl,
                        BUCKETS_COUNT,
                    >(args);
                } else {
                    run_assembler_from_args::<
                        CanonicalNtHashIteratorFactory,
                        cn_seqhash::u128::CanonicalSeqHashFactory,
                        ColorsImpl,
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
                    ColorsImpl,
                    BUCKETS_COUNT,
                >(args);
            } else {
                run_assembler_from_args::<
                    CanonicalNtHashIteratorFactory,
                    cn_rkhash::u32::CanonicalRabinKarpHashFactory,
                    ColorsImpl,
                    BUCKETS_COUNT,
                >(args)
            }
        }
        HashType::RabinKarp64 => {
            if args.forward_only {
                run_assembler_from_args::<
                    ForwardNtHashIteratorFactory,
                    fw_rkhash::u64::ForwardRabinKarpHashFactory,
                    ColorsImpl,
                    BUCKETS_COUNT,
                >(args);
            } else {
                run_assembler_from_args::<
                    CanonicalNtHashIteratorFactory,
                    cn_rkhash::u64::CanonicalRabinKarpHashFactory,
                    ColorsImpl,
                    BUCKETS_COUNT,
                >(args)
            }
        }
        HashType::RabinKarp128 => {
            if args.forward_only {
                run_assembler_from_args::<
                    ForwardNtHashIteratorFactory,
                    fw_rkhash::u128::ForwardRabinKarpHashFactory,
                    ColorsImpl,
                    BUCKETS_COUNT,
                >(args);
            } else {
                run_assembler_from_args::<
                    CanonicalNtHashIteratorFactory,
                    cn_rkhash::u128::CanonicalRabinKarpHashFactory,
                    ColorsImpl,
                    BUCKETS_COUNT,
                >(args)
            }
        }
        HashType::Auto => {
            unreachable!()
        }
    }
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
        if let Some(message) = info.message() {
            writeln!(err_lock, "Error message: {}", message);
        }
        if let Some(s) = info.payload().downcast_ref::<&str>() {
            writeln!(err_lock, "Panic payload: {:?}", s);
        }

        exit(1);
    }));

    if let Some(color_index) = args.print_matches {
        let colors_file = args.output_file.with_extension("colors.dat");
        let colors =
            ColorsSerializer::<RunLengthColorsSerializer>::read_color(colors_file, color_index);
        for color in colors {
            println!("MATCH: {}", color);
        }

        return;
    }

    // Increase the maximum allowed number of open files
    fdlimit::raise_fd_limit();

    KEEP_FILES.store(args.keep_temp_files, Ordering::Relaxed);

    if args.debug_reverse {
        for file in args.input {
            let mut output = ReadsWriter::new_compressed_lz4("complementary.lz4");
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

    if args.colors {
        dispatch_assembler_hash_type::<DefaultColorsManager, BUCKETS_COUNT>(args);
    } else {
        dispatch_assembler_hash_type::<(), BUCKETS_COUNT>(args);
    }
}
