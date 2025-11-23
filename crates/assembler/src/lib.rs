#![cfg_attr(not(feature = "full"), allow(unused_imports))]

use ::dynamic_dispatch::dynamic_dispatch;
use assembler_kmers_merge::structs::RetType;
use assembler_pipeline::OutputFileMode;
use assembler_pipeline::build_final_unitigs;
use assembler_pipeline::compute_matchtigs::MatchtigHelperTrait;
use assembler_pipeline::compute_matchtigs::MatchtigsStorageBackend;
use assembler_pipeline::compute_matchtigs::compute_matchtigs_thread;
use assembler_pipeline::eulertigs::build_eulertigs;
use assembler_pipeline::extend_unitigs::extend_unitigs;
use assembler_pipeline::get_final_output_writer;
use assembler_pipeline::maximal_unitig_links::build_maximal_unitigs_links;
use colors::colors_manager::ColorsManager;
use colors::colors_manager::ColorsMergeManager;
use colors::colors_manager::color_types::PartialUnitigsColorStructure;
use config::get_compression_level_info;
use config::{
    DEFAULT_PER_CPU_BUFFER_SIZE, INTERMEDIATE_COMPRESSION_LEVEL_FAST,
    INTERMEDIATE_COMPRESSION_LEVEL_SLOW, KEEP_FILES, MAX_COLORMAP_WRITING_THREADS,
    MINIMUM_LOG_DELTA_TIME, SwapPriority, get_memory_mode,
};
use ggcat_logging::stats;
use hashes::HashFunctionFactory;
use io::concurrent::structured_sequences::IdentSequenceWriter;
use io::concurrent::structured_sequences::StructuredSequenceBackend;
use io::concurrent::structured_sequences::StructuredSequenceBackendInit;
use io::concurrent::structured_sequences::StructuredSequenceBackendWrapper;
use io::concurrent::structured_sequences::StructuredSequenceWriter;
use io::concurrent::structured_sequences::binary::StructSeqBinaryWriter;
use io::concurrent::structured_sequences::binary::StructSeqBinaryWriterWrapper;
use io::concurrent::structured_sequences::fasta::FastaWriterWrapper;
use io::concurrent::structured_sequences::gfa::GFAWriterWrapperV1;
use io::concurrent::structured_sequences::gfa::GFAWriterWrapperV2;
use io::concurrent::temp_reads::extra_data::SequenceExtraData;
use io::concurrent::temp_reads::extra_data::SequenceExtraDataConsecutiveCompression;
use io::debug_load_buckets;
use io::debug_load_single_buckets;
use io::debug_save_buckets;
use io::debug_save_single_buckets;
use io::sequences_stream::general::GeneralSequenceBlockData;
use io::{DUPLICATES_BUCKET_EXTRA, compute_stats_from_input_blocks};
use parallel_processor::buckets::ExtraBucketData;
use parallel_processor::buckets::concurrent::BucketsThreadBuffer;
use parallel_processor::buckets::writers::compressed_binary_writer::CompressedCheckpointSize;
use parallel_processor::buckets::writers::lock_free_binary_writer::LockFreeBinaryWriter;
use parallel_processor::buckets::{BucketsCount, ExtraBuckets, MultiThreadBuckets};
use parallel_processor::memory_data_size::MemoryDataSize;
use parallel_processor::memory_fs::{MemoryFs, RemoveFileMode};
use parallel_processor::phase_times_monitor::PHASES_TIMES_MONITOR;
use parallel_processor::utils::scoped_thread_local::ScopedThreadLocal;
use std::any::Any;
use std::fs::remove_file;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Instant;
use utils::assembler_phases::AssemblerPhase;

pub use assembler_pipeline::compute_matchtigs::MatchtigMode;

#[dynamic_dispatch(MergingHash = [
    #[cfg(all(feature = "hash-forward", feature = "hash-16bit"))] hashes::fw_seqhash::u16::ForwardSeqHashFactory,
    #[cfg(all(feature = "hash-forward", feature = "hash-32bit"))] hashes::fw_seqhash::u32::ForwardSeqHashFactory,
    #[cfg(all(feature = "hash-forward", feature = "hash-64bit"))] hashes::fw_seqhash::u64::ForwardSeqHashFactory,
    #[cfg(all(feature = "hash-forward", feature = "hash-128bit"))] hashes::fw_seqhash::u128::ForwardSeqHashFactory,
    #[cfg(feature = "hash-16bit")] hashes::cn_seqhash::u16::CanonicalSeqHashFactory,
    #[cfg(feature = "hash-32bit")] hashes::cn_seqhash::u32::CanonicalSeqHashFactory,
    #[cfg(feature = "hash-64bit")] hashes::cn_seqhash::u64::CanonicalSeqHashFactory,
    #[cfg(feature = "hash-128bit")] hashes::cn_seqhash::u128::CanonicalSeqHashFactory,
    #[cfg(feature = "hash-rkarp")] hashes::cn_rkhash::u128::CanonicalRabinKarpHashFactory,
    #[cfg(all(feature = "hash-forward", feature = "hash-rkarp"))] hashes::fw_rkhash::u128::ForwardRabinKarpHashFactory,
], AssemblerColorsManager = [
    #[cfg(feature = "enable-colors")] colors::bundles::multifile_building::ColorBundleMultifileBuilding,
    colors::non_colored::NonColoredManager,
], OutputMode = [
    FastaWriterWrapper,
    #[cfg(feature = "enable-gfa")] GFAWriterWrapperV1,
    #[cfg(feature = "enable-gfa")] GFAWriterWrapperV2
])]
pub fn run_assembler<
    MergingHash: HashFunctionFactory,
    AssemblerColorsManager: ColorsManager,
    OutputMode: StructuredSequenceBackendWrapper,
>(
    k: usize,
    m: usize,
    step: AssemblerPhase,
    last_step: AssemblerPhase,
    input_blocks: Vec<GeneralSequenceBlockData>,
    color_names: &[String],
    output_file: PathBuf,
    temp_dir: Option<PathBuf>,
    threads_count: usize,
    min_multiplicity: usize,
    buckets_count_log: Option<usize>,
    default_compression_level: Option<u32>,
    generate_maximal_unitigs_links: bool,
    compute_tigs_mode: Option<MatchtigMode>,
    only_bstats: bool,
    forward_only: bool,
) -> anyhow::Result<PathBuf> {
    let temp_dir = temp_dir.unwrap_or(PathBuf::new());

    PHASES_TIMES_MONITOR.write().init();

    stats!(let stats.start_time = Instant::now());

    let file_stats = compute_stats_from_input_blocks(&input_blocks)?;

    let buckets_count_log = buckets_count_log.unwrap_or_else(|| file_stats.best_buckets_count_log);
    let second_buckets_count_log = file_stats.best_second_buckets_count_log;

    // Avoid spawning too many threads
    let threads_count = threads_count.min(1 << buckets_count_log);

    ggcat_logging::info!(
        "Buckets count: {}/{} with uncompacted chunk size: {}",
        1 << buckets_count_log,
        1 << second_buckets_count_log,
        MemoryDataSize::from_bytes(file_stats.bucket_size_compaction_threshold as usize)
    );

    if let Some(default_compression_level) = default_compression_level {
        INTERMEDIATE_COMPRESSION_LEVEL_SLOW.store(default_compression_level, Ordering::Relaxed);
        INTERMEDIATE_COMPRESSION_LEVEL_FAST.store(default_compression_level, Ordering::Relaxed);
    }

    let colormap_threads_count = (threads_count / 2).max(1).min(MAX_COLORMAP_WRITING_THREADS);

    let global_colors_table = Arc::new(
        AssemblerColorsManager::ColorsMergeManagerType::create_colors_table(
            output_file.with_extension("colors.dat"),
            color_names,
            colormap_threads_count,
            true,
        )?,
    );

    stats!(
        stats.assembler.preprocess_time = ggcat_logging::get_stat_opt!(stats.start_time)
            .elapsed()
            .into()
    );

    let first_phase_buckets_count = BucketsCount::new(
        buckets_count_log,
        ExtraBuckets::Extra {
            count: 1, /* for the k-mers having multiple minimizers */
            data: DUPLICATES_BUCKET_EXTRA,
        },
    );

    let second_buckets_count = BucketsCount::new(second_buckets_count_log, ExtraBuckets::None);

    let buckets = if step <= AssemblerPhase::MinimizerBucketing {
        assembler_minimizer_bucketing::dynamic_dispatch::minimizer_bucketing(
            (AssemblerColorsManager::dynamic_dispatch_id(),),
            input_blocks,
            temp_dir.as_path(),
            first_phase_buckets_count,
            second_buckets_count,
            threads_count,
            k,
            m,
            Some(file_stats.bucket_size_compaction_threshold),
            file_stats.target_chunk_size,
            forward_only,
        )
    } else {
        debug_load_buckets(&temp_dir, "minimizer-bucketing.debug").unwrap()
    };

    let fs_stats = MemoryFs::get_stats();

    ggcat_logging::info!(
        "Temp buckets files size: {:.2} [MAX SIZE: {:.2} / DISK SIZE: {:.2}] total buckets: {} total chunks: {}",
        MemoryDataSize::from_bytes(fs_extra::dir::get_size(&temp_dir).unwrap_or(0) as usize),
        MemoryDataSize::from_bytes(fs_stats.max_files_usage as usize),
        MemoryDataSize::from_bytes(fs_stats.max_files_usage as usize),
        buckets.len(),
        buckets.iter().map(|x| x.chunks.len()).sum::<usize>()
    );

    if KEEP_FILES.load(Ordering::Relaxed) || last_step < AssemblerPhase::FinalStep {
        debug_save_buckets(&temp_dir, "minimizer-bucketing.debug", &buckets);
    }

    if last_step <= AssemblerPhase::MinimizerBucketing {
        PHASES_TIMES_MONITOR
            .write()
            .print_stats("Completed minimizer bucketing.".to_string());
        return Ok(PathBuf::new());
    } else {
        MemoryFs::flush_to_disk(KEEP_FILES.load(Ordering::Relaxed));
        MemoryFs::free_memory();
    }

    // write partial stats to allow early debug interruption
    ggcat_logging::stats::write_stats(&output_file.with_extension("elab.stats.json"));

    if only_bstats {
        use rayon::prelude::*;
        buckets.par_iter().enumerate().for_each(|(index, bucket)| {
            ggcat_logging::info!("Stats for bucket index: {}", index);
            for chunk in &bucket.chunks {
                kmers_transform::debug_bucket_stats::compute_stats_for_bucket::<MergingHash>(
                    chunk.clone(),
                    index,
                    buckets.len(),
                    second_buckets_count_log,
                    k,
                    m,
                );
            }
        });
        return Ok(PathBuf::new());
    }

    let buckets_count = BucketsCount::new(buckets_count_log, ExtraBuckets::None);

    let output_file_mode: OutputFileMode<OutputMode, _, ()> = if generate_maximal_unitigs_links
        || compute_tigs_mode.needs_matchtigs_library()
        || compute_tigs_mode == Some(MatchtigMode::FastEulerTigs)
    {
        OutputFileMode::Intermediate {
            // Temporary file to store maximal unitigs data without links info, if further processing is requested
            flat_unitigs:
                Arc::new(
                    StructuredSequenceWriter::new(
                        StructSeqBinaryWriter::<
                            PartialUnitigsColorStructure<AssemblerColorsManager>,
                            (),
                        >::new(
                            temp_dir.join("maximal_unitigs.tmp"),
                            &(
                                get_memory_mode(SwapPriority::FinalMaps as usize),
                                CompressedCheckpointSize::new_from_size(
                                    MemoryDataSize::from_mebioctets(4),
                                ),
                                get_compression_level_info(),
                            ),
                            &(),
                        ),
                        k,
                    ),
                ),
            circular_unitigs: if let Some(MatchtigMode::FastEulerTigs) = compute_tigs_mode {
                Some(Arc::new(StructuredSequenceWriter::new(
                    StructSeqBinaryWriter::new(
                        temp_dir.join("circular_unitigs.tmp"),
                        &(
                            get_memory_mode(SwapPriority::FinalMaps as usize),
                            CompressedCheckpointSize::new_from_size(
                                MemoryDataSize::from_mebioctets(1),
                            ),
                            get_compression_level_info(),
                        ),
                        &(),
                    ),
                    k,
                )))
            } else {
                None
            },
        }
    } else {
        OutputFileMode::Final {
            output_file: Arc::new(StructuredSequenceWriter::new(
                get_final_output_writer::<_, _, OutputMode::Backend<_, _>>(&output_file),
                k,
            )),
        }
    };

    let RetType { sequences } = if step <= AssemblerPhase::KmersMerge {
        match &output_file_mode {
            OutputFileMode::Final { output_file } => {
                assembler_kmers_merge::dynamic_dispatch::kmers_merge(
                    (
                        MergingHash::dynamic_dispatch_id(),
                        AssemblerColorsManager::dynamic_dispatch_id(),
                        OutputMode::dynamic_dispatch_id(),
                    ),
                    buckets,
                    output_file.clone(),
                    global_colors_table.clone(),
                    buckets_count,
                    second_buckets_count,
                    min_multiplicity,
                    temp_dir.as_path(),
                    k,
                    m,
                    compute_tigs_mode.needs_simplitigs(),
                    threads_count,
                )
            }
            OutputFileMode::Intermediate {
                flat_unitigs,
                circular_unitigs: _,
            } => assembler_kmers_merge::dynamic_dispatch::kmers_merge(
                (
                    MergingHash::dynamic_dispatch_id(),
                    AssemblerColorsManager::dynamic_dispatch_id(),
                    StructSeqBinaryWriterWrapper::dynamic_dispatch_id(),
                ),
                buckets,
                flat_unitigs.clone(),
                global_colors_table.clone(),
                buckets_count,
                second_buckets_count,
                min_multiplicity,
                temp_dir.as_path(),
                k,
                m,
                compute_tigs_mode.needs_simplitigs(),
                threads_count,
            ),
        }
    } else {
        RetType {
            sequences: debug_load_single_buckets(&temp_dir, "sequences-buckets.debug").unwrap(),
        }
    };

    if KEEP_FILES.load(Ordering::Relaxed) || last_step < AssemblerPhase::FinalStep {
        debug_save_single_buckets(&temp_dir, "sequences-buckets.debug", &sequences);
    }

    if last_step <= AssemblerPhase::KmersMerge {
        PHASES_TIMES_MONITOR
            .write()
            .print_stats("Completed kmers merge.".to_string());
        return Ok(PathBuf::new());
    } else {
        MemoryFs::flush_to_disk(KEEP_FILES.load(Ordering::Relaxed));
        MemoryFs::free_memory();
    }

    // write partial stats to allow early debug interruption
    ggcat_logging::stats::write_stats(&output_file.with_extension("elab.stats.json"));
    drop(global_colors_table);

    build_final_unitigs::<MergingHash, AssemblerColorsManager, OutputMode>(
        k,
        sequences,
        step,
        last_step,
        &output_file,
        &temp_dir,
        threads_count,
        generate_maximal_unitigs_links,
        compute_tigs_mode,
        Box::new(output_file_mode),
    );

    let _ = std::fs::remove_dir(temp_dir.as_path());

    PHASES_TIMES_MONITOR
        .write()
        .print_stats("Compacted De Bruijn graph construction completed.".to_string());

    ggcat_logging::stats::write_stats(&output_file);

    Ok(output_file)
}
