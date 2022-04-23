use crate::assemble_pipeline::parallel_kmers_merge::structs::RetType;
use crate::assemble_pipeline::unitig_links_manager::UnitigLinksManager;
use crate::assemble_pipeline::AssemblePipeline;
use crate::colors::colors_manager::ColorsManager;
use crate::config::{SwapPriority, DEFAULT_PER_CPU_BUFFER_SIZE, MINIMUM_LOG_DELTA_TIME};
use crate::hashes::{HashableHashFunctionFactory, MinimizerHashFunctionFactory};
use crate::io::reads_writer::ReadsWriter;
use crate::utils::{get_memory_mode, Utils};
use crate::{AssemblerStartingStep, KEEP_FILES};
use parallel_processor::buckets::concurrent::BucketsThreadBuffer;
use parallel_processor::buckets::writers::lock_free_binary_writer::LockFreeBinaryWriter;
use parallel_processor::buckets::MultiThreadBuckets;
use parallel_processor::memory_data_size::MemoryDataSize;
use parallel_processor::memory_fs::{MemoryFs, RemoveFileMode};
use parallel_processor::phase_times_monitor::PHASES_TIMES_MONITOR;
use parallel_processor::utils::scoped_thread_local::ScopedThreadLocal;
use parking_lot::Mutex;
use std::fs::remove_file;
use std::path::PathBuf;
use std::sync::atomic::Ordering;
use std::time::Instant;

pub fn run_assembler<
    BucketingHash: MinimizerHashFunctionFactory,
    MergingHash: HashableHashFunctionFactory,
    AssemblerColorsManager: ColorsManager,
>(
    k: usize,
    m: usize,
    step: AssemblerStartingStep,
    last_step: AssemblerStartingStep,
    input: Vec<PathBuf>,
    output_file: PathBuf,
    temp_dir: PathBuf,
    threads_count: usize,
    min_multiplicity: usize,
    buckets_count_log: Option<usize>,
    loopit_number: Option<usize>,
) {
    PHASES_TIMES_MONITOR.write().init();

    let buckets_count_log =
        buckets_count_log.unwrap_or_else(|| Utils::compute_buckets_log_from_input_files(&input));
    let buckets_count = 1 << buckets_count_log;

    let color_names: Vec<_> = input
        .iter()
        .map(|f| f.file_name().unwrap().to_string_lossy().to_string())
        .collect();

    let global_colors_table = AssemblerColorsManager::create_colors_table(
        output_file.with_extension("colors.dat"),
        color_names,
    );

    let buckets = if step <= AssemblerStartingStep::MinimizerBucketing {
        AssemblePipeline::minimizer_bucketing::<BucketingHash, AssemblerColorsManager>(
            input,
            temp_dir.as_path(),
            buckets_count,
            threads_count,
            k,
            m,
        )
    } else {
        Utils::generate_bucket_names(temp_dir.join("bucket"), buckets_count, None)
    };

    println!(
        "Temp buckets files size: {:.2}",
        MemoryDataSize::from_bytes(fs_extra::dir::get_size(&temp_dir).unwrap_or(0) as usize)
    );

    if last_step <= AssemblerStartingStep::MinimizerBucketing {
        PHASES_TIMES_MONITOR
            .write()
            .print_stats("Completed minimizer bucketing.".to_string());
        return;
    }

    let RetType { sequences, hashes } = if step <= AssemblerStartingStep::KmersMerge {
        AssemblePipeline::parallel_kmers_merge::<MergingHash, AssemblerColorsManager, _>(
            buckets,
            &global_colors_table,
            buckets_count,
            min_multiplicity,
            temp_dir.as_path(),
            k,
            m,
            threads_count,
        )
    } else {
        RetType {
            sequences: Utils::generate_bucket_names(
                temp_dir.join("result"),
                buckets_count,
                Some("tmp"),
            ),
            hashes: Utils::generate_bucket_names(temp_dir.join("hashes"), buckets_count, None),
        }
    };
    if last_step <= AssemblerStartingStep::KmersMerge {
        PHASES_TIMES_MONITOR
            .write()
            .print_stats("Completed kmers merge.".to_string());
        return;
    }

    AssemblerColorsManager::print_color_stats(&global_colors_table);

    drop(global_colors_table);

    let mut links = if step <= AssemblerStartingStep::HashesSorting {
        AssemblePipeline::hashes_sorting::<MergingHash, _>(
            hashes,
            temp_dir.as_path(),
            buckets_count,
        )
    } else {
        Utils::generate_bucket_names(temp_dir.join("links"), buckets_count, None)
    };
    if last_step <= AssemblerStartingStep::HashesSorting {
        PHASES_TIMES_MONITOR
            .write()
            .print_stats("Hashes sorting.".to_string());
        return;
    }

    let mut loop_iteration = loopit_number.unwrap_or(0);

    let unames = Utils::generate_bucket_names(temp_dir.join("unitigs_map"), buckets_count, None);
    let rnames = Utils::generate_bucket_names(temp_dir.join("results_map"), buckets_count, None);

    let mut links_manager = UnitigLinksManager::new(buckets_count);

    let (unitigs_map, reads_map) = if step <= AssemblerStartingStep::LinksCompaction {
        for file in unames {
            let _ = remove_file(file);
        }

        for file in rnames {
            let _ = remove_file(file);
        }

        let mut result_map_buckets = MultiThreadBuckets::<LockFreeBinaryWriter>::new(
            buckets_count,
            temp_dir.join("results_map"),
            &(
                get_memory_mode(SwapPriority::FinalMaps as usize),
                LockFreeBinaryWriter::CHECKPOINT_SIZE_UNLIMITED,
            ),
        );

        let mut final_buckets = MultiThreadBuckets::<LockFreeBinaryWriter>::new(
            buckets_count,
            temp_dir.join("unitigs_map"),
            &(
                get_memory_mode(SwapPriority::FinalMaps as usize),
                LockFreeBinaryWriter::CHECKPOINT_SIZE_UNLIMITED,
            ),
        );

        if loop_iteration != 0 {
            links = Utils::generate_bucket_names(
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
                println!("Iteration: {}", loop_iteration);
            }

            let (new_links, remaining) = AssemblePipeline::links_compaction(
                links,
                temp_dir.as_path(),
                buckets_count,
                loop_iteration,
                &mut result_map_buckets,
                &mut final_buckets,
                &links_manager,
                &links_scoped_buffer,
                &results_map_scoped_buffer,
            );

            if do_logging {
                println!(
                    "Remaining: {} {}",
                    remaining,
                    PHASES_TIMES_MONITOR
                        .read()
                        .get_formatted_counter_without_memory()
                );
            }

            links = new_links;
            if remaining == 0 {
                println!("Completed compaction with {} iters", loop_iteration);
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
        return;
    }

    let final_unitigs_file = Mutex::new(match output_file.extension() {
        Some(ext) => match ext.to_string_lossy().to_string().as_str() {
            "lz4" => ReadsWriter::new_compressed_lz4(&output_file, 2),
            "gz" => ReadsWriter::new_compressed_gzip(&output_file, 2),
            _ => ReadsWriter::new_plain(&output_file),
        },
        None => ReadsWriter::new_plain(&output_file),
    });

    let (reorganized_reads, _final_unitigs_bucket) = if step
        <= AssemblerStartingStep::ReorganizeReads
    {
        AssemblePipeline::reorganize_reads::<MergingHash, AssemblerColorsManager>(
            sequences,
            reads_map,
            temp_dir.as_path(),
            &final_unitigs_file,
            buckets_count,
        )
    } else {
        (
            Utils::generate_bucket_names(temp_dir.join("reads_bucket"), buckets_count, Some("tmp")),
            (Utils::generate_bucket_names(temp_dir.join("reads_bucket_lonely"), 1, Some("tmp"))
                .into_iter()
                .next()
                .unwrap()),
        )
    };

    if last_step <= AssemblerStartingStep::ReorganizeReads {
        PHASES_TIMES_MONITOR
            .write()
            .print_stats("Reorganize reads.".to_string());
        return;
    }

    links_manager.compute_id_offsets();

    if step <= AssemblerStartingStep::BuildUnitigs {
        AssemblePipeline::build_unitigs::<MergingHash, AssemblerColorsManager>(
            reorganized_reads,
            unitigs_map,
            temp_dir.as_path(),
            &final_unitigs_file,
            k,
            &links_manager,
        );
    }

    let _ = std::fs::remove_dir(temp_dir.as_path());

    final_unitigs_file.into_inner().finalize();

    PHASES_TIMES_MONITOR
        .write()
        .print_stats("Compacted De Brujin graph construction completed.".to_string());

    println!("Final output saved to: {}", output_file.display());
}
