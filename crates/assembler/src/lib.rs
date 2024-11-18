use crate::pipeline::build_unitigs::build_unitigs;
use crate::pipeline::compute_matchtigs::MatchtigHelperTrait;
use crate::pipeline::compute_matchtigs::{compute_matchtigs_thread, MatchtigsStorageBackend};
use crate::pipeline::hashes_sorting::hashes_sorting;
use crate::pipeline::links_compaction::links_compaction;
use crate::pipeline::maximal_unitig_links::build_maximal_unitigs_links;
use crate::pipeline::reorganize_reads::reorganize_reads;
use ::dynamic_dispatch::dynamic_dispatch;
use assembler_kmers_merge::structs::RetType;
use colors::colors_manager::ColorsManager;
use colors::colors_manager::ColorsMergeManager;
use config::{
    get_compression_level_info, get_memory_mode, SwapPriority, DEFAULT_PER_CPU_BUFFER_SIZE,
    INTERMEDIATE_COMPRESSION_LEVEL_FAST, INTERMEDIATE_COMPRESSION_LEVEL_SLOW, KEEP_FILES,
    MAXIMUM_SECOND_BUCKETS_LOG, MINIMUM_LOG_DELTA_TIME,
};
use hashes::{HashFunctionFactory, MinimizerHashFunctionFactory};
use io::concurrent::structured_sequences::binary::StructSeqBinaryWriter;
use io::concurrent::structured_sequences::fasta::FastaWriterWrapper;
use io::concurrent::structured_sequences::gfa::GFAWriterWrapper;
use io::concurrent::structured_sequences::{
    IdentSequenceWriter, StructuredSequenceBackend, StructuredSequenceBackendInit,
    StructuredSequenceBackendWrapper, StructuredSequenceWriter,
};
use io::sequences_stream::general::GeneralSequenceBlockData;
use io::{compute_stats_from_input_blocks, generate_bucket_names};
use parallel_processor::buckets::concurrent::BucketsThreadBuffer;
use parallel_processor::buckets::writers::compressed_binary_writer::CompressedCheckpointSize;
use parallel_processor::buckets::writers::lock_free_binary_writer::LockFreeBinaryWriter;
use parallel_processor::buckets::MultiThreadBuckets;
use parallel_processor::memory_data_size::MemoryDataSize;
use parallel_processor::memory_fs::{MemoryFs, RemoveFileMode};
use parallel_processor::phase_times_monitor::PHASES_TIMES_MONITOR;
use parallel_processor::utils::scoped_thread_local::ScopedThreadLocal;
use std::fs::remove_file;
use std::path::PathBuf;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Instant;

mod pipeline;
mod structs;

pub use pipeline::compute_matchtigs::MatchtigMode;

#[derive(Clone, PartialEq, PartialOrd)]
pub enum AssemblerStartingStep {
    MinimizerBucketing = 0,
    KmersMerge = 1,
    HashesSorting = 2,
    LinksCompaction = 3,
    ReorganizeReads = 4,
    BuildUnitigs = 5,
    MaximalUnitigsLinks = 6,
}

fn get_writer<
    C: IdentSequenceWriter,
    L: IdentSequenceWriter,
    W: StructuredSequenceBackend<C, L> + StructuredSequenceBackendInit,
>(
    output_file: &PathBuf,
) -> W {
    match output_file.extension() {
        Some(ext) => match ext.to_string_lossy().to_string().as_str() {
            "lz4" => W::new_compressed_lz4(&output_file, 2),
            "gz" => W::new_compressed_gzip(&output_file, 2),
            _ => W::new_plain(&output_file),
        },
        None => W::new_plain(&output_file),
    }
}

#[dynamic_dispatch(BucketingHash = [
    hashes::cn_nthash::CanonicalNtHashIteratorFactory,
    #[cfg(not(feature = "devel-build"))] hashes::fw_nthash::ForwardNtHashIteratorFactory
], MergingHash = [
    #[cfg(not(feature = "devel-build"))] hashes::fw_seqhash::u16::ForwardSeqHashFactory,
    #[cfg(not(feature = "devel-build"))] hashes::fw_seqhash::u32::ForwardSeqHashFactory,
    #[cfg(not(feature = "devel-build"))] hashes::fw_seqhash::u64::ForwardSeqHashFactory,
    #[cfg(not(feature = "devel-build"))] hashes::fw_seqhash::u128::ForwardSeqHashFactory,
    #[cfg(not(feature = "devel-build"))] hashes::fw_rkhash::u32::ForwardRabinKarpHashFactory,
    #[cfg(not(feature = "devel-build"))] hashes::fw_rkhash::u64::ForwardRabinKarpHashFactory,
    #[cfg(not(feature = "devel-build"))] hashes::fw_rkhash::u128::ForwardRabinKarpHashFactory,
    #[cfg(not(feature = "devel-build"))] hashes::cn_seqhash::u16::CanonicalSeqHashFactory,
    #[cfg(not(feature = "devel-build"))] hashes::cn_seqhash::u32::CanonicalSeqHashFactory,
    #[cfg(not(feature = "devel-build"))] hashes::cn_seqhash::u64::CanonicalSeqHashFactory,
    hashes::cn_seqhash::u128::CanonicalSeqHashFactory,
    #[cfg(not(feature = "devel-build"))] hashes::cn_rkhash::u32::CanonicalRabinKarpHashFactory,
    #[cfg(not(feature = "devel-build"))] hashes::cn_rkhash::u64::CanonicalRabinKarpHashFactory,
    #[cfg(not(feature = "devel-build"))] hashes::cn_rkhash::u128::CanonicalRabinKarpHashFactory,
], AssemblerColorsManager = [
    #[cfg(not(feature = "devel-build"))] colors::bundles::multifile_building::ColorBundleMultifileBuilding,
    colors::non_colored::NonColoredManager,
], OutputMode = [
    FastaWriterWrapper,
    #[cfg(not(feature = "devel-build"))] GFAWriterWrapper
])]
pub fn run_assembler<
    BucketingHash: MinimizerHashFunctionFactory,
    MergingHash: HashFunctionFactory,
    AssemblerColorsManager: ColorsManager,
    OutputMode: StructuredSequenceBackendWrapper,
>(
    k: usize,
    m: usize,
    step: AssemblerStartingStep,
    last_step: AssemblerStartingStep,
    input_blocks: Vec<GeneralSequenceBlockData>,
    color_names: &[String],
    output_file: PathBuf,
    temp_dir: Option<PathBuf>,
    threads_count: usize,
    min_multiplicity: usize,
    buckets_count_log: Option<usize>,
    loopit_number: Option<usize>,
    default_compression_level: Option<u32>,
    generate_maximal_unitigs_links: bool,
    compute_tigs_mode: Option<MatchtigMode>,
    only_bstats: bool,
) -> anyhow::Result<PathBuf> {
    let temp_dir = temp_dir.unwrap_or(PathBuf::new());

    PHASES_TIMES_MONITOR.write().init();

    let file_stats = compute_stats_from_input_blocks(&input_blocks)?;

    let buckets_count_log = buckets_count_log.unwrap_or_else(|| file_stats.best_buckets_count_log);

    if let Some(default_compression_level) = default_compression_level {
        INTERMEDIATE_COMPRESSION_LEVEL_SLOW.store(default_compression_level, Ordering::Relaxed);
        INTERMEDIATE_COMPRESSION_LEVEL_FAST.store(default_compression_level, Ordering::Relaxed);
    }

    let buckets_count = 1 << buckets_count_log;

    let global_colors_table = Arc::new(
        AssemblerColorsManager::ColorsMergeManagerType::create_colors_table(
            output_file.with_extension("colors.dat"),
            color_names,
        )?,
    );

    let (buckets, counters) = if step <= AssemblerStartingStep::MinimizerBucketing {
        assembler_minimizer_bucketing::static_dispatch::minimizer_bucketing::<
            BucketingHash,
            AssemblerColorsManager,
        >(
            input_blocks,
            temp_dir.as_path(),
            buckets_count,
            threads_count,
            k,
            m,
        )
    } else {
        (
            generate_bucket_names(temp_dir.join("bucket"), buckets_count, None),
            temp_dir.join("buckets-counters.dat"),
        )
    };

    ggcat_logging::info!(
        "Temp buckets files size: {:.2}",
        MemoryDataSize::from_bytes(fs_extra::dir::get_size(&temp_dir).unwrap_or(0) as usize)
    );

    if last_step <= AssemblerStartingStep::MinimizerBucketing {
        PHASES_TIMES_MONITOR
            .write()
            .print_stats("Completed minimizer bucketing.".to_string());
        return Ok(PathBuf::new());
    } else {
        MemoryFs::flush_all_to_disk();
        MemoryFs::free_memory();
    }

    if only_bstats {
        use rayon::prelude::*;
        buckets.par_iter().enumerate().for_each(|(index, bucket)| {
            kmers_transform::debug_bucket_stats::compute_stats_for_bucket::<
                BucketingHash,
                MergingHash,
            >(
                bucket.clone(),
                index,
                buckets.len(),
                MAXIMUM_SECOND_BUCKETS_LOG,
                k,
                m,
            );
        });
        return Ok(PathBuf::new());
    }

    let RetType { sequences, hashes } = if step <= AssemblerStartingStep::KmersMerge {
        assembler_kmers_merge::kmers_merge::<BucketingHash, MergingHash, AssemblerColorsManager, _>(
            buckets,
            counters,
            global_colors_table.clone(),
            buckets_count,
            min_multiplicity,
            temp_dir.as_path(),
            k,
            m,
            compute_tigs_mode.is_simplitigs(),
            threads_count,
        )
    } else {
        RetType {
            sequences: generate_bucket_names(temp_dir.join("result"), buckets_count, None),
            hashes: generate_bucket_names(temp_dir.join("hashes"), buckets_count, None),
        }
    };
    if last_step <= AssemblerStartingStep::KmersMerge {
        PHASES_TIMES_MONITOR
            .write()
            .print_stats("Completed kmers merge.".to_string());
        return Ok(PathBuf::new());
    } else {
        MemoryFs::flush_all_to_disk();
        MemoryFs::free_memory();
    }

    AssemblerColorsManager::ColorsMergeManagerType::print_color_stats(&global_colors_table);

    drop(global_colors_table);

    let mut links = if step <= AssemblerStartingStep::HashesSorting {
        hashes_sorting::<MergingHash, _>(hashes, temp_dir.as_path(), buckets_count)
    } else {
        generate_bucket_names(temp_dir.join("links"), buckets_count, None)
    };
    if last_step <= AssemblerStartingStep::HashesSorting {
        PHASES_TIMES_MONITOR
            .write()
            .print_stats("Hashes sorting.".to_string());
        return Ok(PathBuf::new());
    } else {
        MemoryFs::flush_all_to_disk();
        MemoryFs::free_memory();
    }

    let mut loop_iteration = loopit_number.unwrap_or(0);

    let unames = generate_bucket_names(temp_dir.join("unitigs_map"), buckets_count, None);
    let rnames = generate_bucket_names(temp_dir.join("results_map"), buckets_count, None);

    // let mut links_manager = UnitigLinksManager::new(buckets_count);

    let (unitigs_map, reads_map) = if step <= AssemblerStartingStep::LinksCompaction {
        for file in unames {
            let _ = remove_file(file);
        }

        for file in rnames {
            let _ = remove_file(file);
        }

        let result_map_buckets = Arc::new(MultiThreadBuckets::<LockFreeBinaryWriter>::new(
            buckets_count,
            temp_dir.join("results_map"),
            &(
                get_memory_mode(SwapPriority::FinalMaps),
                LockFreeBinaryWriter::CHECKPOINT_SIZE_UNLIMITED,
            ),
        ));

        let final_buckets = Arc::new(MultiThreadBuckets::<LockFreeBinaryWriter>::new(
            buckets_count,
            temp_dir.join("unitigs_map"),
            &(
                get_memory_mode(SwapPriority::FinalMaps),
                LockFreeBinaryWriter::CHECKPOINT_SIZE_UNLIMITED,
            ),
        ));

        if loop_iteration != 0 {
            links = generate_bucket_names(
                temp_dir.join(format!("linksi{}", loop_iteration - 1)),
                buckets_count,
                None,
            );
        }

        PHASES_TIMES_MONITOR
            .write()
            .start_phase("phase: links compaction".to_string());

        let mut log_timer = Instant::now();

        let links_scoped_buffer = ScopedThreadLocal::new(move || {
            BucketsThreadBuffer::new(DEFAULT_PER_CPU_BUFFER_SIZE, buckets_count)
        });
        let results_map_scoped_buffer = ScopedThreadLocal::new(move || {
            BucketsThreadBuffer::new(DEFAULT_PER_CPU_BUFFER_SIZE, buckets_count)
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
                break (final_buckets.finalize(), result_map_buckets.finalize());
            }
            loop_iteration += 1;
        };

        for link_file in links {
            MemoryFs::remove_file(
                &link_file,
                RemoveFileMode::Remove {
                    remove_fs: !KEEP_FILES.load(Ordering::Relaxed),
                },
            )
            .unwrap();
        }
        result
    } else {
        (unames, rnames)
    };

    if last_step <= AssemblerStartingStep::LinksCompaction {
        PHASES_TIMES_MONITOR
            .write()
            .print_stats("Links Compaction.".to_string());
        return Ok(PathBuf::new());
    } else {
        MemoryFs::flush_all_to_disk();
        MemoryFs::free_memory();
    }

    let final_unitigs_file = StructuredSequenceWriter::new(
        get_writer::<_, _, OutputMode::Backend<_, _>>(&output_file),
        k,
    );

    // Temporary file to store maximal unitigs data without links info, if further processing is requested
    let compressed_temp_unitigs_file =
        if generate_maximal_unitigs_links || compute_tigs_mode.needs_matchtigs_library() {
            Some(StructuredSequenceWriter::new(
                StructSeqBinaryWriter::new(
                    temp_dir.join("maximal_unitigs.tmp"),
                    &(
                        get_memory_mode(SwapPriority::FinalMaps as usize),
                        CompressedCheckpointSize::new_from_size(MemoryDataSize::from_mebioctets(4)),
                        get_compression_level_info(),
                    ),
                ),
                k,
            ))
        } else {
            None
        };

    let (reorganized_reads, _final_unitigs_bucket) =
        if step <= AssemblerStartingStep::ReorganizeReads {
            if generate_maximal_unitigs_links || compute_tigs_mode.needs_matchtigs_library() {
                reorganize_reads::<
                    BucketingHash,
                    MergingHash,
                    AssemblerColorsManager,
                    StructSeqBinaryWriter<_, _>,
                >(
                    sequences,
                    reads_map,
                    temp_dir.as_path(),
                    compressed_temp_unitigs_file.as_ref().unwrap(),
                    buckets_count,
                )
            } else {
                reorganize_reads::<
                    BucketingHash,
                    MergingHash,
                    AssemblerColorsManager,
                    OutputMode::Backend<_, _>,
                >(
                    sequences,
                    reads_map,
                    temp_dir.as_path(),
                    &final_unitigs_file,
                    buckets_count,
                )
            }
        } else {
            (
                generate_bucket_names(temp_dir.join("reads_bucket"), buckets_count, Some("tmp")),
                (generate_bucket_names(temp_dir.join("reads_bucket_lonely"), 1, Some("tmp"))
                    .into_iter()
                    .next()
                    .unwrap()),
            )
        };

    if last_step <= AssemblerStartingStep::ReorganizeReads {
        PHASES_TIMES_MONITOR
            .write()
            .print_stats("Reorganize reads.".to_string());
        return Ok(PathBuf::new());
    } else {
        MemoryFs::flush_all_to_disk();
        MemoryFs::free_memory();
    }

    // links_manager.compute_id_offsets();

    if step <= AssemblerStartingStep::BuildUnitigs {
        if generate_maximal_unitigs_links || compute_tigs_mode.needs_matchtigs_library() {
            build_unitigs::<
                BucketingHash,
                MergingHash,
                AssemblerColorsManager,
                StructSeqBinaryWriter<_, _>,
            >(
                reorganized_reads,
                unitigs_map,
                temp_dir.as_path(),
                compressed_temp_unitigs_file.as_ref().unwrap(),
                k,
            );
        } else {
            build_unitigs::<
                BucketingHash,
                MergingHash,
                AssemblerColorsManager,
                OutputMode::Backend<_, _>,
            >(
                reorganized_reads,
                unitigs_map,
                temp_dir.as_path(),
                &final_unitigs_file,
                k,
            );
        }
    }

    if step <= AssemblerStartingStep::MaximalUnitigsLinks {
        if generate_maximal_unitigs_links || compute_tigs_mode.needs_matchtigs_library() {
            let compressed_temp_unitigs_file = compressed_temp_unitigs_file.unwrap();
            let temp_path = compressed_temp_unitigs_file.get_path();
            compressed_temp_unitigs_file.finalize();

            if let Some(compute_tigs_mode) = compute_tigs_mode.get_matchtigs_mode() {
                let matchtigs_backend = MatchtigsStorageBackend::new();

                let matchtigs_receiver = matchtigs_backend.get_receiver();

                let handle = std::thread::Builder::new()
                    .name("greedy_matchtigs".to_string())
                    .spawn(move || {
                        compute_matchtigs_thread::<
                            BucketingHash,
                            MergingHash,
                            AssemblerColorsManager,
                            _,
                        >(
                            k,
                            threads_count,
                            matchtigs_receiver,
                            &final_unitigs_file,
                            compute_tigs_mode,
                        );
                    })
                    .unwrap();

                build_maximal_unitigs_links::<
                    BucketingHash,
                    MergingHash,
                    AssemblerColorsManager,
                    MatchtigsStorageBackend<_>,
                >(
                    temp_path,
                    temp_dir.as_path(),
                    &StructuredSequenceWriter::new(matchtigs_backend, k),
                    k,
                );

                handle.join().unwrap();
            } else if generate_maximal_unitigs_links {
                final_unitigs_file.finalize();

                let final_unitigs_file = StructuredSequenceWriter::new(
                    get_writer::<_, _, OutputMode::Backend<_, _>>(&output_file),
                    k,
                );

                build_maximal_unitigs_links::<
                    BucketingHash,
                    MergingHash,
                    AssemblerColorsManager,
                    OutputMode::Backend<_, _>,
                >(temp_path, temp_dir.as_path(), &final_unitigs_file, k);
                final_unitigs_file.finalize();
            }
        } else {
            final_unitigs_file.finalize();
        }
    } else {
        final_unitigs_file.finalize();
    }

    let _ = std::fs::remove_dir(temp_dir.as_path());

    PHASES_TIMES_MONITOR
        .write()
        .print_stats("Compacted De Bruijn graph construction completed.".to_string());

    Ok(output_file)
}
