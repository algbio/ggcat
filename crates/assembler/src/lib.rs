#![cfg_attr(not(feature = "full"), allow(unused_imports))]

use ::dynamic_dispatch::dynamic_dispatch;
use assembler_kmers_merge::structs::RetType;
use assembler_pipeline::compute_matchtigs::MatchtigHelperTrait;
use assembler_pipeline::hashes_sorting;
use assembler_pipeline::links_compaction::links_compaction;
use colors::colors_manager::ColorsManager;
use colors::colors_manager::ColorsMergeManager;
use config::{
    DEFAULT_PER_CPU_BUFFER_SIZE, INTERMEDIATE_COMPRESSION_LEVEL_FAST,
    INTERMEDIATE_COMPRESSION_LEVEL_SLOW, KEEP_FILES, MAX_COLORMAP_WRITING_THREADS,
    MINIMUM_LOG_DELTA_TIME, SwapPriority, get_memory_mode,
};
use ggcat_logging::stats;
use hashes::HashFunctionFactory;
use io::concurrent::structured_sequences::StructuredSequenceBackendWrapper;
use io::concurrent::structured_sequences::fasta::FastaWriterWrapper;
use io::concurrent::structured_sequences::gfa::GFAWriterWrapperV1;
use io::concurrent::structured_sequences::gfa::GFAWriterWrapperV2;
use io::debug_load_buckets;
use io::debug_load_single_buckets;
use io::debug_save_buckets;
use io::debug_save_single_buckets;
use io::sequences_stream::general::GeneralSequenceBlockData;
use io::{DUPLICATES_BUCKET_EXTRA, compute_stats_from_input_blocks};
use parallel_processor::buckets::concurrent::BucketsThreadBuffer;
use parallel_processor::buckets::writers::lock_free_binary_writer::LockFreeBinaryWriter;
use parallel_processor::buckets::{BucketsCount, ExtraBuckets, MultiThreadBuckets};
use parallel_processor::memory_data_size::MemoryDataSize;
use parallel_processor::memory_fs::{MemoryFs, RemoveFileMode};
use parallel_processor::phase_times_monitor::PHASES_TIMES_MONITOR;
use parallel_processor::utils::scoped_thread_local::ScopedThreadLocal;
use std::fs::remove_file;
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

    if last_step <= AssemblerPhase::MinimizerBucketing {
        debug_save_buckets(&temp_dir, "minimizer-bucketing.debug", &buckets);
        PHASES_TIMES_MONITOR
            .write()
            .print_stats("Completed minimizer bucketing.".to_string());
        return Ok(PathBuf::new());
    } else {
        MemoryFs::flush_to_disk(true);
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

    let RetType { sequences, hashes } = if step <= AssemblerPhase::KmersMerge {
        assembler_kmers_merge::dynamic_dispatch::kmers_merge(
            (
                MergingHash::dynamic_dispatch_id(),
                AssemblerColorsManager::dynamic_dispatch_id(),
            ),
            buckets,
            global_colors_table.clone(),
            buckets_count,
            second_buckets_count,
            min_multiplicity,
            temp_dir.as_path(),
            k,
            m,
            compute_tigs_mode.needs_simplitigs(),
            threads_count,
            forward_only,
        )
    } else {
        RetType {
            sequences: debug_load_single_buckets(&temp_dir, "sequences-buckets.debug").unwrap(),
            hashes: debug_load_single_buckets(&temp_dir, "hashes-buckets.debug").unwrap(),
        }
    };
    if last_step <= AssemblerPhase::KmersMerge {
        debug_save_single_buckets(&temp_dir, "sequences-buckets.debug", &sequences);
        debug_save_single_buckets(&temp_dir, "hashes-buckets.debug", &hashes);
        PHASES_TIMES_MONITOR
            .write()
            .print_stats("Completed kmers merge.".to_string());
        return Ok(PathBuf::new());
    } else {
        MemoryFs::flush_to_disk(true);
        MemoryFs::free_memory();
    }

    // write partial stats to allow early debug interruption
    ggcat_logging::stats::write_stats(&output_file.with_extension("elab.stats.json"));
    drop(global_colors_table);

    let mut links = if step <= AssemblerPhase::HashesSorting {
        hashes_sorting::dynamic_dispatch::hashes_sorting(
            (MergingHash::dynamic_dispatch_id(),),
            hashes,
            temp_dir.as_path(),
            buckets_count,
        )
    } else {
        debug_load_single_buckets(&temp_dir, "links-buckets.debug").unwrap()
    };
    if last_step <= AssemblerPhase::HashesSorting {
        debug_save_single_buckets(&temp_dir, "links-buckets.debug", &links);
        PHASES_TIMES_MONITOR
            .write()
            .print_stats("Hashes sorting.".to_string());
        return Ok(PathBuf::new());
    } else {
        MemoryFs::flush_to_disk(true);
        MemoryFs::free_memory();
    }

    let mut loop_iteration = 0;

    let (unitigs_map, reads_map) = if step <= AssemblerPhase::LinksCompaction {
        let result_map_buckets = Arc::new(MultiThreadBuckets::<LockFreeBinaryWriter>::new(
            buckets_count,
            temp_dir.join("results_map"),
            None,
            &(
                get_memory_mode(SwapPriority::FinalMaps),
                LockFreeBinaryWriter::CHECKPOINT_SIZE_UNLIMITED,
            ),
            &(),
        ));

        let final_buckets = Arc::new(MultiThreadBuckets::<LockFreeBinaryWriter>::new(
            buckets_count,
            temp_dir.join("unitigs_map"),
            None,
            &(
                get_memory_mode(SwapPriority::FinalMaps),
                LockFreeBinaryWriter::CHECKPOINT_SIZE_UNLIMITED,
            ),
            &(),
        ));

        PHASES_TIMES_MONITOR
            .write()
            .start_phase("phase: links compaction".to_string());

        let mut log_timer = Instant::now();

        let links_scoped_buffer = ScopedThreadLocal::new(move || {
            BucketsThreadBuffer::new(DEFAULT_PER_CPU_BUFFER_SIZE, &buckets_count)
        });
        let results_map_scoped_buffer = ScopedThreadLocal::new(move || {
            BucketsThreadBuffer::new(DEFAULT_PER_CPU_BUFFER_SIZE, &buckets_count)
        });

        let result = loop {
            let do_logging = if log_timer.elapsed() > MINIMUM_LOG_DELTA_TIME {
                log_timer = Instant::now();
                true
            } else {
                false
            };

            if do_logging {
                ggcat_logging::info!("Iteration: {}", loop_iteration);
            }

            let (new_links, remaining) = links_compaction(
                links,
                temp_dir.as_path(),
                buckets_count,
                loop_iteration,
                &result_map_buckets,
                &final_buckets,
                // &links_manager,
                &links_scoped_buffer,
                &results_map_scoped_buffer,
            );

            if do_logging {
                ggcat_logging::info!(
                    "Remaining: {} {}",
                    remaining,
                    PHASES_TIMES_MONITOR
                        .read()
                        .get_formatted_counter_without_memory()
                );
            }

            links = new_links;
            if remaining == 0 {
                ggcat_logging::info!("Completed compaction with {} iters", loop_iteration);
                break (
                    final_buckets.finalize_single(),
                    result_map_buckets.finalize_single(),
                );
            }
            loop_iteration += 1;
        };

        for link_bucket in links {
            MemoryFs::remove_file(
                &link_bucket.path,
                RemoveFileMode::Remove {
                    remove_fs: !KEEP_FILES.load(Ordering::Relaxed),
                },
            )
            .unwrap();
        }
        result
    } else {
        (
            debug_load_single_buckets(&temp_dir, "unitigs-map.debug").unwrap(),
            debug_load_single_buckets(&temp_dir, "reads-map.debug").unwrap(),
        )
    };

    if last_step <= AssemblerPhase::LinksCompaction {
        debug_save_single_buckets(&temp_dir, "unitigs-map.debug", &unitigs_map);
        debug_save_single_buckets(&temp_dir, "reads-map.debug", &reads_map);

        PHASES_TIMES_MONITOR
            .write()
            .print_stats("Links Compaction.".to_string());
        return Ok(PathBuf::new());
    } else {
        MemoryFs::flush_to_disk(true);
        MemoryFs::free_memory();
    }

    assembler_pipeline::dynamic_dispatch::build_final_unitigs(
        (
            MergingHash::dynamic_dispatch_id(),
            AssemblerColorsManager::dynamic_dispatch_id(),
            OutputMode::dynamic_dispatch_id(),
        ),
        k,
        sequences,
        reads_map,
        unitigs_map,
        buckets_count,
        step,
        last_step,
        &output_file,
        &temp_dir,
        threads_count,
        generate_maximal_unitigs_links,
        compute_tigs_mode,
    );

    let _ = std::fs::remove_dir(temp_dir.as_path());

    PHASES_TIMES_MONITOR
        .write()
        .print_stats("Compacted De Bruijn graph construction completed.".to_string());

    ggcat_logging::stats::write_stats(&output_file);

    Ok(output_file)
}
