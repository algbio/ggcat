use crate::colors::colors_manager::ColorsManager;
use crate::hashes::HashFunctionFactory;
use crate::io::{DataReader, DataWriter};
use crate::query_pipeline::QueryPipeline;
use crate::utils::debug_utils::debug_print;
use crate::utils::Utils;
use crate::{QuerierStartingStep, KEEP_FILES};
use itertools::Itertools;
use parallel_processor::phase_times_monitor::PHASES_TIMES_MONITOR;
use parking_lot::Mutex;
use std::fs::remove_file;
use std::path::PathBuf;
use std::sync::atomic::Ordering;
use std::thread;

pub fn run_query<
    BucketingHash: HashFunctionFactory,
    MergingHash: HashFunctionFactory,
    AssemblerColorsManager: ColorsManager,
    Reader: DataReader,
    Writer: DataWriter,
    const BUCKETS_COUNT: usize,
>(
    k: usize,
    m: usize,
    step: QuerierStartingStep,
    graph_input: PathBuf,
    query_input: PathBuf,
    output_file: PathBuf,
    temp_dir: PathBuf,
    threads_count: usize,
) {
    PHASES_TIMES_MONITOR.write().init();

    // let global_colors_table = AssemblerColorsManager::create_colors_table(
    //     output_file.with_extension("colors.dat"),
    //     color_names,
    // );

    let buckets = if step <= QuerierStartingStep::MinimizerBucketing {
        QueryPipeline::minimizer_bucketing::<BucketingHash, AssemblerColorsManager, Writer>(
            graph_input,
            query_input.clone(),
            temp_dir.as_path(),
            BUCKETS_COUNT,
            threads_count,
            k,
            m,
        )
    } else {
        Utils::generate_bucket_names(temp_dir.join("bucket"), BUCKETS_COUNT, Some("tmp"))
    };

    let counters_buckets = if step <= QuerierStartingStep::KmersCounting {
        QueryPipeline::parallel_kmers_counting::<
            BucketingHash,
            MergingHash,
            AssemblerColorsManager,
            _,
            Reader,
        >(
            buckets,
            BUCKETS_COUNT,
            temp_dir.as_path(),
            k,
            m,
            threads_count,
        )
    } else {
        Utils::generate_bucket_names(temp_dir.join("counters"), BUCKETS_COUNT, Some("tmp"))
    };

    QueryPipeline::counters_sorting(k, query_input, counters_buckets, output_file.clone());

    //
    //     #[cfg(not(feature = "kpar"))]
    //     {
    //         Pipeline::kmers_merge::<BucketingHash, MergingHash, AssemblerColorsManager, _>(
    //             buckets,
    //             &global_colors_table,
    //             BUCKETS_COUNT,
    //             min_multiplicity,
    //             temp_dir.as_path(),
    //             k,
    //             m,
    //         )
    //     }
    // } else {
    //     RetType {
    //         sequences: Utils::generate_bucket_names(
    //             temp_dir.join("result"),
    //             BUCKETS_COUNT,
    //             Some("lz4"),
    //         ),
    //         hashes: Utils::generate_bucket_names(temp_dir.join("hashes"), BUCKETS_COUNT, None),
    //     }
    // };
    //
    // debug_print();
    //
    // drop(global_colors_table);
    //
    // let mut links = if step <= QuerierStartingStep::HashesSorting {
    //     Pipeline::hashes_sorting::<MergingHash, _>(hashes, temp_dir.as_path(), BUCKETS_COUNT)
    // } else {
    //     Utils::generate_bucket_names(temp_dir.join("links"), BUCKETS_COUNT, None)
    // };
    //
    // let mut loop_iteration = loopit_number.unwrap_or(0);
    //
    // let unames = Utils::generate_bucket_names(temp_dir.join("unitigs_map"), BUCKETS_COUNT, None);
    // let rnames = Utils::generate_bucket_names(temp_dir.join("results_map"), BUCKETS_COUNT, None);
    //
    // let (unitigs_map, reads_map) = if step <= QuerierStartingStep::LinksCompaction {
    //     for file in unames {
    //         remove_file(file);
    //     }
    //
    //     for file in rnames {
    //         remove_file(file);
    //     }
    //
    //     if loop_iteration != 0 {
    //         links = Utils::generate_bucket_names(
    //             temp_dir.join(format!("linksi{}", loop_iteration - 1)),
    //             BUCKETS_COUNT,
    //             None,
    //         );
    //     }
    //
    //     PHASES_TIMES_MONITOR
    //         .write()
    //         .start_phase("phase: links compaction".to_string());
    //
    //     let result = loop {
    //         println!("Iteration: {}", loop_iteration);
    //
    //         let (new_links, result) = Pipeline::links_compaction(
    //             links,
    //             temp_dir.as_path(),
    //             BUCKETS_COUNT,
    //             loop_iteration,
    //         );
    //         links = new_links;
    //         match result {
    //             None => {}
    //             Some(result) => {
    //                 println!("Completed compaction with {} iters", loop_iteration);
    //                 break result;
    //             }
    //         }
    //         loop_iteration += 1;
    //     };
    //
    //     if !KEEP_FILES.load(Ordering::Relaxed) {
    //         for link_file in links {
    //             std::fs::remove_file(&link_file);
    //         }
    //     }
    //     result
    // } else {
    //     (unames, rnames)
    // };
    //
    // let mut final_unitigs_file = Mutex::new(match output_file.extension() {
    //     Some(ext) => match ext.to_string_lossy().to_string().as_str() {
    //         "lz4" => ReadsStorage::writer_compressed_lz4(&output_file),
    //         "gz" => ReadsStorage::writer_compressed(&output_file),
    //         _ => ReadsStorage::writer_plain(&output_file),
    //     },
    //     None => ReadsStorage::writer_plain(&output_file),
    // });
    //
    // let reorganized_reads = if step <= QuerierStartingStep::ReorganizeReads {
    //     Pipeline::reorganize_reads::<MergingHash, AssemblerColorsManager>(
    //         sequences,
    //         reads_map,
    //         temp_dir.as_path(),
    //         &final_unitigs_file,
    //         BUCKETS_COUNT,
    //         k,
    //         m,
    //     )
    // } else {
    //     Utils::generate_bucket_names(temp_dir.join("reads_bucket"), BUCKETS_COUNT, Some("lz4"))
    // };
    //
    // if step <= QuerierStartingStep::BuildUnitigs {
    //     Pipeline::build_unitigs::<MergingHash, AssemblerColorsManager>(
    //         reorganized_reads,
    //         unitigs_map,
    //         temp_dir.as_path(),
    //         &final_unitigs_file,
    //         BUCKETS_COUNT,
    //         k,
    //         m,
    //     );
    // }
    //
    // std::fs::remove_dir(temp_dir.as_path());
    //
    // final_unitigs_file.into_inner().finalize();

    PHASES_TIMES_MONITOR
        .write()
        .print_stats("Compacted De Brujin graph construction completed.".to_string());

    println!("Final output saved to: {}", output_file.display());
}
