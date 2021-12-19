use crate::assemble_pipeline::parallel_kmers_merge::structs::RetType;
use crate::assemble_pipeline::AssemblePipeline;
use crate::colors::colors_manager::ColorsManager;
use crate::colors::default_colors_manager::DefaultColorsManager;
use crate::hashes::HashFunctionFactory;
use crate::io::reads_reader::ReadsReader;
use crate::io::reads_writer::ReadsWriter;
use crate::utils::debug_utils::debug_print;
use crate::utils::Utils;
use crate::{AssemblerStartingStep, KEEP_FILES};
use itertools::Itertools;
use parallel_processor::phase_times_monitor::PHASES_TIMES_MONITOR;
use parking_lot::Mutex;
use std::fs::remove_file;
use std::path::PathBuf;
use std::sync::atomic::Ordering;

pub fn run_assembler<
    BucketingHash: HashFunctionFactory,
    MergingHash: HashFunctionFactory,
    AssemblerColorsManager: ColorsManager,
    const BUCKETS_COUNT: usize,
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
    quality_threshold: Option<f64>,
    loopit_number: Option<usize>,
) {
    PHASES_TIMES_MONITOR.write().init();

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
            BUCKETS_COUNT,
            threads_count,
            k,
            m,
            quality_threshold,
        )
    } else {
        Utils::generate_bucket_names(temp_dir.join("bucket"), BUCKETS_COUNT, Some("tmp"))
    };
    if last_step <= AssemblerStartingStep::MinimizerBucketing {
        return;
    }

    let RetType { sequences, hashes } = if step <= AssemblerStartingStep::KmersMerge {
        AssemblePipeline::parallel_kmers_merge::<
            BucketingHash,
            MergingHash,
            AssemblerColorsManager,
            _,
        >(
            buckets,
            &global_colors_table,
            BUCKETS_COUNT,
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
                BUCKETS_COUNT,
                Some("lz4"),
            ),
            hashes: Utils::generate_bucket_names(temp_dir.join("hashes"), BUCKETS_COUNT, None),
        }
    };
    if last_step <= AssemblerStartingStep::KmersMerge {
        return;
    }

    AssemblerColorsManager::print_color_stats(&global_colors_table);

    drop(global_colors_table);

    let mut links = if step <= AssemblerStartingStep::HashesSorting {
        AssemblePipeline::hashes_sorting::<MergingHash, _>(
            hashes,
            temp_dir.as_path(),
            BUCKETS_COUNT,
        )
    } else {
        Utils::generate_bucket_names(temp_dir.join("links"), BUCKETS_COUNT, None)
    };
    if last_step <= AssemblerStartingStep::HashesSorting {
        return;
    }

    let mut loop_iteration = loopit_number.unwrap_or(0);

    let unames = Utils::generate_bucket_names(temp_dir.join("unitigs_map"), BUCKETS_COUNT, None);
    let rnames = Utils::generate_bucket_names(temp_dir.join("results_map"), BUCKETS_COUNT, None);

    let (unitigs_map, reads_map) = if step <= AssemblerStartingStep::LinksCompaction {
        for file in unames {
            remove_file(file);
        }

        for file in rnames {
            remove_file(file);
        }

        if loop_iteration != 0 {
            links = Utils::generate_bucket_names(
                temp_dir.join(format!("linksi{}", loop_iteration - 1)),
                BUCKETS_COUNT,
                None,
            );
        }

        PHASES_TIMES_MONITOR
            .write()
            .start_phase("phase: links compaction".to_string());

        let result = loop {
            println!("Iteration: {}", loop_iteration);

            let (new_links, result) = AssemblePipeline::links_compaction(
                links,
                temp_dir.as_path(),
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

    if last_step <= AssemblerStartingStep::LinksCompaction {
        return;
    }

    let mut final_unitigs_file = Mutex::new(match output_file.extension() {
        Some(ext) => match ext.to_string_lossy().to_string().as_str() {
            "lz4" => ReadsWriter::new_compressed_lz4(&output_file),
            "gz" => ReadsWriter::new_compressed_gzip(&output_file),
            _ => ReadsWriter::new_plain(&output_file),
        },
        None => ReadsWriter::new_plain(&output_file),
    });

    let reorganized_reads = if step <= AssemblerStartingStep::ReorganizeReads {
        AssemblePipeline::reorganize_reads::<MergingHash, AssemblerColorsManager>(
            sequences,
            reads_map,
            temp_dir.as_path(),
            &final_unitigs_file,
            BUCKETS_COUNT,
            k,
            m,
        )
    } else {
        Utils::generate_bucket_names(temp_dir.join("reads_bucket"), BUCKETS_COUNT, Some("lz4"))
    };

    if last_step <= AssemblerStartingStep::ReorganizeReads {
        return;
    }

    if step <= AssemblerStartingStep::BuildUnitigs {
        AssemblePipeline::build_unitigs::<MergingHash, AssemblerColorsManager>(
            reorganized_reads,
            unitigs_map,
            temp_dir.as_path(),
            &final_unitigs_file,
            BUCKETS_COUNT,
            k,
            m,
        );
    }

    std::fs::remove_dir(temp_dir.as_path());

    final_unitigs_file.into_inner().finalize();

    PHASES_TIMES_MONITOR
        .write()
        .print_stats("Compacted De Brujin graph construction completed.".to_string());

    println!("Final output saved to: {}", output_file.display());
}
