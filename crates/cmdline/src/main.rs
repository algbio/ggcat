extern crate alloc;
// #[cfg(test)]
// extern crate test;

// mod benchmarks;

use ::utils::compute_best_m;
use ahash::HashMap;
use clap::{ArgGroup, Parser, ValueEnum};
use colors::DefaultColorsSerializer;
use colors::colors_manager::ColorMapReader;
use colors::storage::deserializer::ColorsDeserializer;
use config::ColorIndexType;
use config::OUTPUT_COMPRESSION_LEVEL;
use ggcat_api::{ExtraElaboration, GGCATConfig, GGCATInstance, GfaVersion};
use ggcat_logging::UnrecoverableErrorLogging;
use io::sequences_stream::GenericSequencesStream;
use io::sequences_stream::fasta::FastaFileSequencesStream;
use io::sequences_stream::general::GeneralSequenceBlockData;
use parallel_processor::memory_fs::MemoryFs;
use rayon::slice::ParallelSliceMut;
use std::cmp::Ordering as CmpOrdering;
use std::collections::BinaryHeap;
use std::fs::{self, File};
use std::io::BufRead;
use std::io::{BufReader, BufWriter, Read, Write};
use std::panic;
use std::path::{Path, PathBuf};
use std::process::exit;
use std::sync::atomic::Ordering;
use std::time::Duration;

#[derive(Debug, Clone, PartialOrd, PartialEq, ValueEnum)]
pub enum AssemblerStartingStep {
    MinimizerBucketing = 0,
    KmersMerge = 1,
    UnitigsExtension = 2,
    MaximalUnitigsLinks = 3,
    FinalStep = 4,
}

#[derive(Debug, Clone, PartialOrd, PartialEq, ValueEnum)]
pub enum QuerierStartingStep {
    MinimizerBucketing = 0,
    KmersCounting = 1,
    CountersSorting = 2,
    ColorMapReading = 3,
}

#[derive(Copy, Clone, Debug, PartialOrd, PartialEq, ValueEnum)]
pub enum HashType {
    Auto = 0,
    SeqHash = 1,
    RabinKarp128 = 4,
}

#[derive(Parser, Debug)]
#[command(
    name = "ggcat",
    version,
    about = "Compacted and colored de Bruijn graph construction and querying"
)]
enum CliArgs {
    /// Build a new compacted graph
    Build(AssemblerArgs),
    /// Build and directly export sorted colored FASTA with explicit bitsets
    BuildColoredFasta(AssemblerArgs),
    /// Query a compacted graph
    Query(QueryArgs),
    /// Dump graph sequences to FASTA with explicit color bitsets, sorted by colors
    DumpColoredFasta(DumpColoredFastaArgs),
    /// Dump all color names from a colormap
    DumpColors(DumpColorsArgs),
    Matches(MatchesArgs),
    // Utils(CmdUtilsArgs),
}

#[derive(Parser, Debug)]
struct MatchesArgs {
    /// Input fasta file with associated colors file (in the same folder)
    input_file: PathBuf,

    /// Debug print matches of a color index (in hexadecimal)
    match_color: String,
}

#[derive(Parser, Debug)]
struct CommonArgs {
    /// The k-mers length
    #[arg(short, long = "kmer-length")]
    pub kmer_length: usize,

    /// Overrides the default m-mers (minimizers) length
    #[arg(long = "minimizer-length", help_heading = "Advanced Options")]
    pub minimizer_length: Option<usize>,

    /// Directory for temporary files
    #[arg(short = 't', long = "temp-dir", default_value = ".temp_files")]
    pub temp_dir: PathBuf,

    /// Keep intermediate temporary files for debugging purposes
    #[arg(long = "keep-temp-files", help_heading = "Advanced Options")]
    pub keep_temp_files: bool,

    #[arg(short = 'j', long, default_value = "16")]
    pub threads_count: usize,

    /// Hash type used to identify kmers
    #[arg(
        short = 'w',
        long,
        default_value = "auto",
        help_heading = "Advanced Options"
    )]
    pub hash_type: HashType,

    /// Treats reverse complementary kmers as different
    #[arg(short = 'f', long)]
    pub forward_only: bool,

    /// Maximum suggested memory usage (GB)
    /// The tool will try use only up to this GB of memory to store temporary files
    /// without writing to disk. This usage does not include the needed memory for the processing steps.
    /// GGCAT can allocate extra memory for files if the current memory is not enough to complete the current operation
    #[arg(short = 'm', long, default_value = "2")]
    pub memory: f64,

    /// Use all the given memory before writing to disk
    #[arg(short = 'p', long = "prefer-memory")]
    pub prefer_memory: bool,

    /// The log2 of the number of buckets
    #[arg(
        short = 'b',
        long = "buckets-count-log",
        help_heading = "Advanced Options"
    )]
    pub buckets_count_log: Option<usize>,

    /// The level of lz4 compression to be used for the intermediate files
    #[arg(
        long = "intermediate-compression-level",
        help_heading = "Advanced Options"
    )]
    pub intermediate_compression_level: Option<u32>,

    /// Compression level for final output files (applies to .lz4/.gz/.zst/.zstd outputs)
    #[arg(long = "output-compression-level", default_value = "2")]
    pub output_compression_level: u32,

    #[arg(hide = true, long = "only-bstats")]
    pub only_bstats: bool,
}

#[derive(Parser, Debug)]
#[command(group = ArgGroup::new("output-mode").required(false))]
struct AssemblerArgs {
    /// The input files
    pub input: Vec<PathBuf>,

    /// The lists of input files
    #[arg(short = 'l', long = "input-lists")]
    pub input_lists: Vec<PathBuf>,

    #[arg(short = 'o', long = "output-file", default_value = "output.fasta.lz4")]
    pub output_file: PathBuf,

    /// Enable colors
    #[arg(short, long)]
    pub colors: bool,

    /// The lists of input files with colors in format <COLOR_NAME><TAB><FILE_PATH>
    #[arg(short = 'd', long = "colored-input-lists")]
    pub colored_input_lists: Vec<PathBuf>,

    /// Minimum multiplicity required to keep a kmer
    #[arg(short = 's', long = "min-multiplicity", default_value = "2")]
    pub min_multiplicity: usize,

    /// Generate maximal unitigs connections references, in BCALM2 format L:<+/->:<other id>:<+/->
    #[arg(
        short = 'e',
        long = "generate-maximal-unitigs-links",
        group = "output-mode",
        help_heading = "Output mode"
    )]
    pub generate_maximal_unitigs_links: bool,

    /// Generate simplitigs instead of maximal unitigs
    #[arg(
        long = "simplitigs",
        alias = "fast-simplitigs",
        group = "output-mode",
        help_heading = "Output mode"
    )]
    pub simplitigs: bool,

    /// Generate eulertigs instead of maximal unitigs
    #[arg(
        long = "eulertigs",
        alias = "fast-eulertigs",
        group = "output-mode",
        help_heading = "Output mode"
    )]
    pub eulertigs: bool,

    /// Generate greedy matchtigs instead of maximal unitigs
    #[arg(
        long = "greedy-matchtigs",
        group = "output-mode",
        help_heading = "Output mode"
    )]
    pub greedy_matchtigs: bool,

    /// Generate eulertigs instead of maximal unitigs, old slower version
    #[arg(
        hide = true,
        long = "old-eulertigs",
        group = "output-mode",
        help_heading = "Output mode"
    )]
    pub old_eulertigs: bool,

    /// Generate old simplitigs instead of maximal unitigs, old slower version
    #[arg(
        hide = true,
        long = "old-simplitigs",
        group = "output-mode",
        help_heading = "Output mode"
    )]
    pub old_simplitigs: bool,

    #[command(flatten)]
    pub common_args: CommonArgs,

    /// Output the graph in GFA format v1
    #[arg(long = "gfa-v1", help_heading = "Output mode")]
    pub gfa_output_v1: bool,

    /// Output the graph in GFA format v2
    #[arg(long = "gfa-v2", help_heading = "Output mode")]
    pub gfa_output_v2: bool,

    #[arg(hide = true, long, default_value = "minimizer-bucketing")]
    pub step: AssemblerStartingStep,

    #[arg(hide = true, long = "last-step", default_value = "final-step")]
    pub last_step: AssemblerStartingStep,
}

#[derive(Parser, Debug)]
struct DumpColorsArgs {
    input_colormap: PathBuf,
    output_file: PathBuf,
}

#[derive(Parser, Debug)]
struct DumpColoredFastaArgs {
    /// The input graph
    pub input_graph: PathBuf,

    /// The output FASTA file
    #[arg(
        short = 'o',
        long = "output-file",
        default_value = "output.colored.fasta"
    )]
    pub output_file: PathBuf,

    /// Directory for temporary files
    #[arg(short = 't', long = "temp-dir", default_value = ".temp_files")]
    pub temp_dir: PathBuf,

    /// Keep intermediate temporary files for debugging purposes
    #[arg(long = "keep-temp-files", help_heading = "Advanced Options")]
    pub keep_temp_files: bool,

    #[arg(short = 'j', long, default_value = "16")]
    pub threads_count: usize,

    /// Compression level for the output file when using .zst/.zstd extension
    #[arg(long = "output-compression-level", default_value = "2")]
    pub output_compression_level: u32,
}

/// Format of the queries output
#[derive(Copy, Clone, Debug, PartialEq, Eq, ValueEnum)]
pub enum ColoredQueryOutputFormat {
    JsonLinesWithNumbers,
    JsonLinesWithNames,
}

#[derive(Parser, Debug)]
struct QueryArgs {
    /// The input graph
    pub input_graph: PathBuf,

    /// The input query as a .fasta file
    pub input_query: PathBuf,

    /// Enable colors
    #[arg(short, long)]
    pub colors: bool,

    #[arg(short = 'o', long = "output-file-prefix", default_value = "output")]
    pub output_file_prefix: PathBuf,

    #[arg(long = "colored-query-output-format")]
    pub colored_query_output_format: Option<ColoredQueryOutputFormat>,

    #[arg(short = 'x', long, default_value = "MinimizerBucketing")]
    pub step: QuerierStartingStep,

    #[command(flatten)]
    pub common_args: CommonArgs,
}

// #[cfg(feature = "mem-analysis")]
// use parallel_processor::debug_allocator::{debug_print_allocations, DebugAllocator};
//
// #[cfg_attr(feature = "mem-analysis", global_allocator)]
// #[cfg(feature = "mem-analysis")]
// static DEBUG_ALLOCATOR: DebugAllocator = DebugAllocator::new();

fn initialize(args: &CommonArgs, out_file: &PathBuf) -> &'static GGCATInstance {
    let instance = GGCATInstance::create(GGCATConfig {
        temp_dir: Some(args.temp_dir.clone()),
        memory: args.memory,
        prefer_memory: args.prefer_memory,
        total_threads_count: args.threads_count,
        intermediate_compression_level: args.intermediate_compression_level,
        stats_file: Some(out_file.with_extension("stats.log")),
        messages_callback: None,
    });

    ggcat_api::debug::DEBUG_KEEP_FILES.store(args.keep_temp_files, Ordering::Relaxed);
    *ggcat_api::debug::BUCKETS_COUNT_LOG_FORCE.lock() = args.buckets_count_log;
    ggcat_api::debug::DEBUG_ONLY_BSTATS.store(args.only_bstats, Ordering::Relaxed);
    OUTPUT_COMPRESSION_LEVEL.store(args.output_compression_level, Ordering::Relaxed);
    *ggcat_api::debug::DEBUG_HASH_TYPE.lock() = match args.hash_type {
        HashType::Auto => ggcat_api::HashType::Auto,
        HashType::SeqHash => ggcat_api::HashType::SeqHash,
        HashType::RabinKarp128 => ggcat_api::HashType::RabinKarp128,
    };

    println!(
        "Using m: {} with k: {}",
        args.minimizer_length
            .unwrap_or(compute_best_m(args.kmer_length)),
        args.kmer_length
    );

    // #[cfg(feature = "mem-analysis")]
    // debug_print_allocations("/tmp/allocations", Duration::from_secs(5));
    instance.unwrap()
}

fn convert_assembler_step(step: AssemblerStartingStep) -> utils::assembler_phases::AssemblerPhase {
    match step {
        AssemblerStartingStep::MinimizerBucketing => {
            utils::assembler_phases::AssemblerPhase::MinimizerBucketing
        }
        AssemblerStartingStep::KmersMerge => utils::assembler_phases::AssemblerPhase::KmersMerge,
        AssemblerStartingStep::UnitigsExtension => {
            utils::assembler_phases::AssemblerPhase::UnitigsExtension
        }
        AssemblerStartingStep::MaximalUnitigsLinks => {
            utils::assembler_phases::AssemblerPhase::MaximalUnitigsLinks
        }
        AssemblerStartingStep::FinalStep => utils::assembler_phases::AssemblerPhase::FinalStep,
    }
}

fn get_extra_elab(args: &AssemblerArgs) -> ExtraElaboration {
    if args.generate_maximal_unitigs_links {
        ExtraElaboration::UnitigLinks
    } else if args.greedy_matchtigs {
        ExtraElaboration::GreedyMatchtigs
    } else if args.old_eulertigs {
        ExtraElaboration::Eulertigs
    } else if args.old_simplitigs {
        ExtraElaboration::Pathtigs
    } else if args.simplitigs {
        ExtraElaboration::FastSimplitigs
    } else if args.eulertigs {
        ExtraElaboration::FastEulertigs
    } else {
        ExtraElaboration::None
    }
}

fn run_assembler_from_args(instance: &GGCATInstance, args: AssemblerArgs) {
    let extra_elab = get_extra_elab(&args);
    let mut inputs: Vec<_> = args.input.iter().cloned().map(|f| (f, None)).collect();

    if (args.input_lists.len() > 0 || args.input.len() > 0) && args.colored_input_lists.len() > 0 {
        println!("Cannot specify both colored input lists and other files/lists");
        exit(1);
    }

    if args.gfa_output_v1 && args.gfa_output_v2 {
        println!("Cannot specify both GFA v1 and GFA v2 output");
        exit(1);
    }

    for list in args.input_lists {
        for input in BufReader::new(
            File::open(&list)
                .log_unrecoverable_error_with_data(
                    "Error while opening input list file",
                    list.display(),
                )
                .unwrap(),
        )
        .lines()
        {
            if let Ok(input) = input {
                if input.trim().is_empty() {
                    continue;
                }

                let input = if <String as AsRef<Path>>::as_ref(&input).is_relative() {
                    list.parent().unwrap().join(input)
                } else {
                    PathBuf::from(input)
                };

                inputs.push((input, None));
            }
        }
    }

    let color_names: Vec<_> = if args.colored_input_lists.is_empty() {
        // Standard colors (input file names)
        inputs
            .iter()
            .map(|f| f.0.file_name().unwrap().to_string_lossy().to_string())
            .collect()
    } else {
        // Mapped colors
        let mut colors = HashMap::default();
        let mut next_index = 0;

        for list in args.colored_input_lists {
            for input in BufReader::new(
                File::open(&list)
                    .map_err(|e| {
                        panic!(
                            "Error while opening colored input list file {}: {}",
                            list.display(),
                            e
                        )
                    })
                    .unwrap(),
            )
            .lines()
            {
                if let Ok(input) = input {
                    if input.trim().is_empty() {
                        continue;
                    }

                    let parts = input.split("\t").collect::<Vec<_>>();
                    if parts.len() < 2 {
                        println!("Invalid line in colored input list: {}", input);
                        exit(1);
                    }

                    let color_name = parts[0..parts.len() - 1].join("\t");
                    let file_name = parts.last().unwrap().to_string();

                    let index = *colors.entry(color_name).or_insert_with(|| {
                        let index = next_index;
                        next_index += 1;
                        index
                    });

                    println!(
                        "Add index with color {} => {}",
                        parts[0..parts.len() - 1].join("\t"),
                        index
                    );

                    let file_name = if <String as AsRef<Path>>::as_ref(&file_name).is_relative() {
                        list.parent().unwrap().join(file_name)
                    } else {
                        PathBuf::from(file_name)
                    };

                    inputs.push((file_name, Some(index)));
                }
            }
        }
        let mut colors: Vec<_> = colors.into_iter().collect();
        colors.sort_by_key(|(_, i)| *i);

        colors.into_iter().map(|(c, _)| c).collect()
    };

    if inputs.is_empty() {
        println!("ERROR: No input files specified!");
        exit(1);
    }

    let inputs = inputs
        .into_iter()
        .map(|x| GeneralSequenceBlockData::FASTA(x))
        .collect();

    *ggcat_api::debug::DEBUG_ASSEMBLER_FIRST_STEP.lock() =
        convert_assembler_step(args.step.clone());
    *ggcat_api::debug::DEBUG_ASSEMBLER_LAST_STEP.lock() =
        convert_assembler_step(args.last_step.clone());

    let output_file = instance
        .build_graph(
            inputs,
            args.output_file.clone(),
            Some(&color_names),
            args.common_args.kmer_length,
            args.common_args.threads_count,
            args.common_args.forward_only,
            args.common_args.minimizer_length,
            args.colors,
            args.min_multiplicity,
            extra_elab,
            if args.gfa_output_v1 {
                Some(GfaVersion::V1)
            } else if args.gfa_output_v2 {
                Some(GfaVersion::V2)
            } else {
                None
            },
        )
        .unwrap();

    ggcat_logging::stats::write_stats(&args.output_file.with_extension("elab.stats.json"));

    println!("Final output saved to: {}", output_file.display());
}

fn run_build_colored_fasta_from_args(instance: &GGCATInstance, args: AssemblerArgs) {
    let mut inputs: Vec<_> = args.input.iter().cloned().map(|f| (f, None)).collect();

    if (args.input_lists.len() > 0 || args.input.len() > 0) && args.colored_input_lists.len() > 0 {
        println!("Cannot specify both colored input lists and other files/lists");
        exit(1);
    }

    if args.gfa_output_v1 || args.gfa_output_v2 {
        println!("GFA output is not compatible with build-colored-fasta");
        exit(1);
    }

    for list in args.input_lists.clone() {
        for input in BufReader::new(
            File::open(&list)
                .log_unrecoverable_error_with_data(
                    "Error while opening input list file",
                    list.display(),
                )
                .unwrap(),
        )
        .lines()
        {
            if let Ok(input) = input {
                if input.trim().is_empty() {
                    continue;
                }

                let input = if <String as AsRef<Path>>::as_ref(&input).is_relative() {
                    list.parent().unwrap().join(input)
                } else {
                    PathBuf::from(input)
                };

                inputs.push((input, None));
            }
        }
    }

    let color_names: Vec<_> = if args.colored_input_lists.is_empty() {
        inputs
            .iter()
            .map(|f| f.0.file_name().unwrap().to_string_lossy().to_string())
            .collect()
    } else {
        let mut colors = HashMap::default();
        let mut next_index = 0;

        for list in args.colored_input_lists.clone() {
            for input in BufReader::new(
                File::open(&list)
                    .map_err(|e| {
                        panic!(
                            "Error while opening colored input list file {}: {}",
                            list.display(),
                            e
                        )
                    })
                    .unwrap(),
            )
            .lines()
            {
                if let Ok(input) = input {
                    if input.trim().is_empty() {
                        continue;
                    }

                    let parts = input.split("\t").collect::<Vec<_>>();
                    if parts.len() < 2 {
                        println!("Invalid line in colored input list: {}", input);
                        exit(1);
                    }

                    let color_name = parts[0..parts.len() - 1].join("\t");
                    let file_name = parts.last().unwrap().to_string();

                    let index = *colors.entry(color_name).or_insert_with(|| {
                        let index = next_index;
                        next_index += 1;
                        index
                    });

                    println!(
                        "Add index with color {} => {}",
                        parts[0..parts.len() - 1].join("\t"),
                        index
                    );

                    let file_name = if <String as AsRef<Path>>::as_ref(&file_name).is_relative() {
                        list.parent().unwrap().join(file_name)
                    } else {
                        PathBuf::from(file_name)
                    };

                    inputs.push((file_name, Some(index)));
                }
            }
        }
        let mut colors: Vec<_> = colors.into_iter().collect();
        colors.sort_by_key(|(_, i)| *i);
        colors.into_iter().map(|(c, _)| c).collect()
    };

    if inputs.is_empty() {
        println!("ERROR: No input files specified!");
        exit(1);
    }

    let inputs: Vec<_> = inputs
        .into_iter()
        .map(GeneralSequenceBlockData::FASTA)
        .collect();

    let extra_elab = get_extra_elab(&args);
    *ggcat_api::debug::DEBUG_ASSEMBLER_FIRST_STEP.lock() =
        convert_assembler_step(args.step.clone());
    *ggcat_api::debug::DEBUG_ASSEMBLER_LAST_STEP.lock() =
        convert_assembler_step(args.last_step.clone());

    let raw_records_file = args.output_file.with_extension("records.tmp");
    let output_file = instance
        .build_graph_color_records(
            inputs,
            raw_records_file.clone(),
            Some(&color_names),
            args.common_args.kmer_length,
            args.common_args.threads_count,
            args.common_args.forward_only,
            args.common_args.minimizer_length,
            args.min_multiplicity,
            extra_elab,
        )
        .unwrap();

    let colormap_file = GGCATInstance::get_colormap_file(&output_file);
    convert_color_records_to_bitset_fasta(
        &output_file,
        &colormap_file,
        &args.output_file,
        &args.common_args.temp_dir,
        args.common_args.keep_temp_files,
        args.common_args.threads_count,
        args.common_args.output_compression_level,
    );

    if !args.common_args.keep_temp_files {
        let _ = fs::remove_file(&output_file);
        let _ = fs::remove_file(&colormap_file);
    }

    ggcat_logging::stats::write_stats(&args.output_file.with_extension("elab.stats.json"));
    println!("Final output saved to: {}", args.output_file.display());
}

fn convert_querier_step(step: QuerierStartingStep) -> querier::QuerierStartingStep {
    match step {
        QuerierStartingStep::MinimizerBucketing => querier::QuerierStartingStep::MinimizerBucketing,
        QuerierStartingStep::KmersCounting => querier::QuerierStartingStep::KmersCounting,
        QuerierStartingStep::CountersSorting => querier::QuerierStartingStep::CountersSorting,
        QuerierStartingStep::ColorMapReading => querier::QuerierStartingStep::ColorMapReading,
    }
}

fn run_querier_from_args(instance: &GGCATInstance, args: QueryArgs) -> PathBuf {
    *ggcat_api::debug::DEBUG_QUERIER_FIRST_STEP.lock() = convert_querier_step(args.step);

    instance
        .query_graph(
            args.input_graph,
            args.input_query,
            args.output_file_prefix,
            args.common_args.kmer_length,
            args.common_args.threads_count,
            args.common_args.forward_only,
            args.common_args.minimizer_length,
            args.colors,
            match args
                .colored_query_output_format
                .unwrap_or(ColoredQueryOutputFormat::JsonLinesWithNumbers)
            {
                ColoredQueryOutputFormat::JsonLinesWithNumbers => {
                    querier::ColoredQueryOutputFormat::JsonLinesWithNumbers
                }
                ColoredQueryOutputFormat::JsonLinesWithNames => {
                    querier::ColoredQueryOutputFormat::JsonLinesWithNames
                }
            },
        )
        .unwrap()
}

#[derive(Eq, PartialEq)]
struct SortedRecord {
    key: Vec<u8>,
    seq: Vec<u8>,
}

#[derive(Eq, PartialEq)]
struct MergeHeapItem {
    record: SortedRecord,
    chunk_index: usize,
}

impl Ord for MergeHeapItem {
    fn cmp(&self, other: &Self) -> CmpOrdering {
        other
            .record
            .key
            .cmp(&self.record.key)
            .then_with(|| other.record.seq.cmp(&self.record.seq))
    }
}

impl PartialOrd for MergeHeapItem {
    fn partial_cmp(&self, other: &Self) -> Option<CmpOrdering> {
        Some(self.cmp(other))
    }
}

fn parse_color_subsets_from_ident(ident: &[u8], out: &mut Vec<ColorIndexType>) {
    out.clear();
    for token in ident.split(|b| *b == b' ') {
        if token.len() < 4 || token[0] != b'C' || token[1] != b':' {
            continue;
        }

        let Some(color_sep) = token[2..].iter().position(|b| *b == b':') else {
            continue;
        };
        let color_hex = &token[2..(2 + color_sep)];
        let color_hex = std::str::from_utf8(color_hex).unwrap();
        let color_subset = ColorIndexType::from_str_radix(color_hex, 16).unwrap();
        out.push(color_subset);
    }

    out.sort_unstable();
    out.dedup();
}

fn write_record(writer: &mut BufWriter<File>, record: &SortedRecord) {
    writer
        .write_all(&(record.key.len() as u32).to_le_bytes())
        .unwrap();
    writer
        .write_all(&(record.seq.len() as u32).to_le_bytes())
        .unwrap();
    writer.write_all(&record.key).unwrap();
    writer.write_all(&record.seq).unwrap();
}

fn read_record(reader: &mut BufReader<File>) -> Option<SortedRecord> {
    let mut len_buffer = [0u8; 4];
    match reader.read_exact(&mut len_buffer) {
        Ok(_) => {}
        Err(err) if err.kind() == std::io::ErrorKind::UnexpectedEof => return None,
        Err(err) => panic!("Cannot read chunk key length: {err}"),
    }
    let key_len = u32::from_le_bytes(len_buffer) as usize;

    reader.read_exact(&mut len_buffer).unwrap();
    let seq_len = u32::from_le_bytes(len_buffer) as usize;

    let mut key = vec![0u8; key_len];
    reader.read_exact(&mut key).unwrap();

    let mut seq = vec![0u8; seq_len];
    reader.read_exact(&mut seq).unwrap();

    Some(SortedRecord { key, seq })
}

fn read_color_record(reader: &mut BufReader<File>) -> Option<(Vec<ColorIndexType>, Vec<u8>)> {
    let mut len_buffer = [0u8; 4];
    match reader.read_exact(&mut len_buffer) {
        Ok(_) => {}
        Err(err) if err.kind() == std::io::ErrorKind::UnexpectedEof => return None,
        Err(err) => panic!("Cannot read color-record subset length: {err}"),
    }
    let subset_count = u32::from_le_bytes(len_buffer) as usize;

    reader.read_exact(&mut len_buffer).unwrap();
    let seq_len = u32::from_le_bytes(len_buffer) as usize;

    let mut subsets = vec![0u32; subset_count];
    for subset in &mut subsets {
        reader.read_exact(&mut len_buffer).unwrap();
        *subset = u32::from_le_bytes(len_buffer);
    }

    let mut seq = vec![0u8; seq_len];
    reader.read_exact(&mut seq).unwrap();

    Some((subsets, seq))
}

fn make_output_writer(
    output_file: impl AsRef<Path>,
    output_compression_level: u32,
) -> BufWriter<Box<dyn Write + Send>> {
    let output_file = output_file.as_ref();
    let file = File::create(output_file).unwrap();

    match output_file.extension().and_then(|e| e.to_str()) {
        Some("zst") | Some("zstd") => {
            let level = output_compression_level.min(22) as i32;
            let encoder = zstd::stream::write::Encoder::new(file, level)
                .unwrap()
                .auto_finish();
            BufWriter::with_capacity(8 * 1024 * 1024, Box::new(encoder))
        }
        _ => BufWriter::with_capacity(8 * 1024 * 1024, Box::new(file)),
    }
}

fn flush_sorted_chunk(
    records: &mut Vec<SortedRecord>,
    chunk_files: &mut Vec<PathBuf>,
    chunk_dir: &Path,
) {
    if records.is_empty() {
        return;
    }

    records.par_sort_unstable_by(|left, right| {
        left.key
            .cmp(&right.key)
            .then_with(|| left.seq.cmp(&right.seq))
    });

    let chunk_path = chunk_dir.join(format!("chunk_{:08}.bin", chunk_files.len()));
    let mut chunk_writer =
        BufWriter::with_capacity(8 * 1024 * 1024, File::create(&chunk_path).unwrap());
    for record in records.iter() {
        write_record(&mut chunk_writer, record);
    }
    chunk_writer.flush().unwrap();
    chunk_files.push(chunk_path);
    records.clear();
}

fn convert_color_records_to_bitset_fasta(
    records_file: impl AsRef<Path>,
    colormap_file: impl AsRef<Path>,
    output_file: impl AsRef<Path>,
    temp_dir: impl AsRef<Path>,
    keep_temp_files: bool,
    threads_count: usize,
    output_compression_level: u32,
) {
    const CHUNK_TARGET_BYTES: usize = 128 * 1024 * 1024;

    let _ = rayon::ThreadPoolBuilder::new()
        .num_threads(threads_count)
        .build_global();

    let mut colors_deserializer =
        ColorsDeserializer::<DefaultColorsSerializer>::new(colormap_file, true).unwrap();
    let colors_count = colors_deserializer.colors_count();
    if colors_count == 0 {
        panic!("Input graph has no colors");
    }

    let temp_chunks_dir = temp_dir.as_ref().join(format!(
        "dump_colored_fasta_{}_{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis()
    ));
    fs::create_dir_all(&temp_chunks_dir).unwrap();

    let mut subset_cache: HashMap<ColorIndexType, Vec<ColorIndexType>> = HashMap::default();
    let mut bitset = vec![b'0'; colors_count];
    let mut records = Vec::new();
    let mut chunk_files = Vec::new();
    let mut current_chunk_bytes = 0usize;
    let mut total_sequences = 0u64;
    let mut skipped_sequences = 0u64;

    let mut reader =
        BufReader::with_capacity(8 * 1024 * 1024, File::open(records_file.as_ref()).unwrap());
    while let Some((subsets, seq)) = read_color_record(&mut reader) {
        total_sequences += 1;
        bitset.fill(b'0');

        if subsets.is_empty() {
            if colors_count == 1 {
                bitset[0] = b'1';
            } else {
                skipped_sequences += 1;
                continue;
            }
        } else {
            for subset in subsets {
                let subset_colors = subset_cache.entry(subset).or_insert_with(|| {
                    let mut colors = Vec::new();
                    colors_deserializer.get_color_mappings(subset, &mut colors);
                    colors
                });
                for color in subset_colors.iter().copied() {
                    bitset[color as usize] = b'1';
                }
            }
        }

        let record = SortedRecord {
            key: bitset.clone(),
            seq,
        };
        current_chunk_bytes += record.key.len() + record.seq.len();
        records.push(record);

        if current_chunk_bytes >= CHUNK_TARGET_BYTES {
            flush_sorted_chunk(&mut records, &mut chunk_files, &temp_chunks_dir);
            current_chunk_bytes = 0;
        }
    }

    flush_sorted_chunk(&mut records, &mut chunk_files, &temp_chunks_dir);

    let mut output_writer = make_output_writer(output_file.as_ref(), output_compression_level);

    let mut chunk_readers: Vec<_> = chunk_files
        .iter()
        .map(|path| BufReader::with_capacity(8 * 1024 * 1024, File::open(path).unwrap()))
        .collect();

    let mut heap = BinaryHeap::new();
    for (chunk_index, reader) in chunk_readers.iter_mut().enumerate() {
        if let Some(record) = read_record(reader) {
            heap.push(MergeHeapItem {
                record,
                chunk_index,
            });
        }
    }

    let mut output_index = 0u64;
    while let Some(item) = heap.pop() {
        write!(output_writer, ">{} BS:Z:", output_index).unwrap();
        output_writer.write_all(&item.record.key).unwrap();
        output_writer.write_all(b"\n").unwrap();
        output_writer.write_all(&item.record.seq).unwrap();
        output_writer.write_all(b"\n").unwrap();
        output_index += 1;

        if let Some(record) = read_record(&mut chunk_readers[item.chunk_index]) {
            heap.push(MergeHeapItem {
                record,
                chunk_index: item.chunk_index,
            });
        }
    }
    output_writer.flush().unwrap();

    if total_sequences == 0 {
        panic!("Input graph contains no sequences");
    }
    if output_index == 0 {
        panic!("No colored records were produced");
    }
    if skipped_sequences > 0 {
        println!(
            "Warning: skipped {} sequence(s) without color annotations",
            skipped_sequences
        );
    }

    if !keep_temp_files {
        for chunk in chunk_files {
            fs::remove_file(chunk).unwrap();
        }
        fs::remove_dir(temp_chunks_dir).unwrap();
    } else {
        println!("Temporary chunks kept at: {}", temp_chunks_dir.display());
    }
}

fn run_dump_colored_fasta_from_args(args: DumpColoredFastaArgs) -> PathBuf {
    const CHUNK_TARGET_BYTES: usize = 128 * 1024 * 1024;
    let _ = rayon::ThreadPoolBuilder::new()
        .num_threads(args.threads_count)
        .build_global();

    let colors_file = args.input_graph.with_extension("colors.dat");
    let mut colors_deserializer =
        ColorsDeserializer::<DefaultColorsSerializer>::new(colors_file, true).unwrap();
    let colors_count = colors_deserializer.colors_count();
    if colors_count == 0 {
        panic!("Input graph has no colors");
    }

    let temp_chunks_dir = args.temp_dir.join(format!(
        "dump_colored_fasta_{}_{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis()
    ));
    fs::create_dir_all(&temp_chunks_dir).unwrap();

    let mut subset_cache: HashMap<ColorIndexType, Vec<ColorIndexType>> = HashMap::default();
    let mut subset_ids = Vec::new();
    let mut bitset = vec![b'0'; colors_count];

    let mut records = Vec::new();
    let mut chunk_files = Vec::new();
    let mut current_chunk_bytes = 0usize;
    let mut total_sequences = 0u64;
    let mut skipped_sequences = 0u64;

    FastaFileSequencesStream::new().read_block(
        &(args.input_graph.clone(), None),
        true,
        None,
        |seq, _| {
            total_sequences += 1;
            parse_color_subsets_from_ident(seq.ident_data, &mut subset_ids);
            if subset_ids.is_empty() {
                skipped_sequences += 1;
                return;
            }

            bitset.fill(b'0');
            for subset in subset_ids.iter().copied() {
                let subset_colors = subset_cache.entry(subset).or_insert_with(|| {
                    let mut colors = Vec::new();
                    colors_deserializer.get_color_mappings(subset, &mut colors);
                    colors
                });
                for color in subset_colors.iter().copied() {
                    bitset[color as usize] = b'1';
                }
            }

            let record = SortedRecord {
                key: bitset.clone(),
                seq: seq.seq.to_vec(),
            };
            current_chunk_bytes += record.key.len() + record.seq.len();
            records.push(record);

            if current_chunk_bytes >= CHUNK_TARGET_BYTES {
                flush_sorted_chunk(&mut records, &mut chunk_files, &temp_chunks_dir);
                current_chunk_bytes = 0;
            }
        },
    );

    flush_sorted_chunk(&mut records, &mut chunk_files, &temp_chunks_dir);

    let mut output_writer = make_output_writer(&args.output_file, args.output_compression_level);

    let mut chunk_readers: Vec<_> = chunk_files
        .iter()
        .map(|path| BufReader::with_capacity(8 * 1024 * 1024, File::open(path).unwrap()))
        .collect();

    let mut heap = BinaryHeap::new();
    for (chunk_index, reader) in chunk_readers.iter_mut().enumerate() {
        if let Some(record) = read_record(reader) {
            heap.push(MergeHeapItem {
                record,
                chunk_index,
            });
        }
    }

    let mut output_index = 0u64;
    while let Some(item) = heap.pop() {
        write!(output_writer, ">{} BS:Z:", output_index).unwrap();
        output_writer.write_all(&item.record.key).unwrap();
        output_writer.write_all(b"\n").unwrap();
        output_writer.write_all(&item.record.seq).unwrap();
        output_writer.write_all(b"\n").unwrap();
        output_index += 1;

        if let Some(record) = read_record(&mut chunk_readers[item.chunk_index]) {
            heap.push(MergeHeapItem {
                record,
                chunk_index: item.chunk_index,
            });
        }
    }
    output_writer.flush().unwrap();

    if total_sequences == 0 {
        panic!("Input graph contains no sequences");
    }
    if output_index == 0 {
        panic!(
            "Input graph does not contain color annotations in FASTA headers (no C:<subset>:<count> tags found)"
        );
    }
    if skipped_sequences > 0 {
        println!(
            "Warning: skipped {} sequence(s) without color annotations",
            skipped_sequences
        );
    }

    if !args.keep_temp_files {
        for chunk in chunk_files {
            fs::remove_file(chunk).unwrap();
        }
        fs::remove_dir(temp_chunks_dir).unwrap();
    } else {
        println!("Temporary chunks kept at: {}", temp_chunks_dir.display());
    }

    args.output_file
}

instrumenter::global_setup_instrumenter!();

fn main() {
    let args: CliArgs = CliArgs::parse();

    panic::set_hook(Box::new(move |panic_info| {
        let stdout = std::io::stdout();
        let _lock = stdout.lock();

        let stderr = std::io::stderr();
        let mut err_lock = stderr.lock();

        let backtrace = backtrace::Backtrace::new();
        write!(err_lock, "Panic: {}\nBacktrace:{:?}", panic_info, backtrace).unwrap();
        exit(1);
    }));

    match args {
        CliArgs::Build(args) => {
            let _guard = instrumenter::initialize_tracing(
                args.output_file.with_extension("tracing.json"),
                &["ix86arch::INSTRUCTION_RETIRED", "ix86arch::LLC_MISSES"],
            );

            let instance = initialize(&args.common_args, &args.output_file);

            run_assembler_from_args(&instance, args);
        }
        CliArgs::BuildColoredFasta(args) => {
            let _guard = instrumenter::initialize_tracing(
                args.output_file.with_extension("tracing.json"),
                &["ix86arch::INSTRUCTION_RETIRED", "ix86arch::LLC_MISSES"],
            );

            let instance = initialize(&args.common_args, &args.output_file);

            run_build_colored_fasta_from_args(&instance, args);
        }
        CliArgs::Matches(args) => {
            let colors_file = args.input_file.with_extension("colors.dat");
            let mut colors_deserializer =
                ColorsDeserializer::<DefaultColorsSerializer>::new(colors_file, true).unwrap();

            let mut colors = Vec::new();

            let color = ColorIndexType::from_str_radix(&args.match_color, 16)
                .expect("Invalid color, please use hex format");
            colors_deserializer.get_color_mappings(color, &mut colors);

            for color in colors {
                println!(
                    "MATCHES: {} => {}",
                    color,
                    colors_deserializer.get_color_name(color, false)
                );
            }
            return; // Skip final memory deallocation
        }
        CliArgs::Query(args) => {
            if !args.colors && args.colored_query_output_format.is_some() {
                println!(
                    "Warning: colored query output format is specified, but the graph is not colored"
                );
            }

            let _guard = instrumenter::initialize_tracing(
                args.output_file_prefix.with_extension("tracing.json"),
                &["ix86arch::INSTRUCTION_RETIRED", "ix86arch::LLC_MISSES"],
            );

            let instance = initialize(&args.common_args, &args.output_file_prefix);

            let output_file_name = run_querier_from_args(&instance, args);
            println!("Final output saved to: {}", output_file_name.display());
        }
        CliArgs::DumpColoredFasta(args) => {
            let output_file_name = run_dump_colored_fasta_from_args(args);
            println!("Final output saved to: {}", output_file_name.display());
            return; // Skip final memory deallocation
        }
        CliArgs::DumpColors(args) => {
            let output_file_name = args.output_file.with_extension("jsonl");

            let mut output_file = BufWriter::new(File::create(&output_file_name).unwrap());

            for (color_idx, color_name) in GGCATInstance::dump_colors(args.input_colormap)
                .unwrap()
                .enumerate()
            {
                writeln!(
                    output_file,
                    "{{\"color_index\":{}, \"color_name\":\"{}\" }}",
                    color_idx, color_name,
                )
                .unwrap();
            }

            drop(output_file);
            println!("Colors written to {}", output_file_name.display());

            return; // Skip final memory deallocation
        }
    }

    // Ensure termination
    std::thread::spawn(|| {
        std::thread::sleep(Duration::from_secs(5));
        exit(0);
    });
    MemoryFs::terminate();
}
