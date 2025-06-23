#![cfg_attr(not(feature = "full"), allow(unused_imports))]

use std::path::{Path, PathBuf};

use ::dynamic_dispatch::dynamic_dispatch;
use colors::colors_manager::ColorsManager;
use config::{SwapPriority, get_compression_level_info, get_memory_mode};
use hashes::HashFunctionFactory;
use io::{
    concurrent::structured_sequences::{
        IdentSequenceWriter, StructuredSequenceBackend, StructuredSequenceBackendInit,
        StructuredSequenceBackendWrapper, StructuredSequenceWriter,
        binary::StructSeqBinaryWriter,
        fasta::FastaWriterWrapper,
        gfa::{GFAWriterWrapperV1, GFAWriterWrapperV2},
    },
    generate_bucket_names,
    sequences_stream::general::GeneralSequenceBlockData,
};
use parallel_processor::{
    buckets::{
        BucketsCount, SingleBucket, writers::compressed_binary_writer::CompressedCheckpointSize,
    },
    memory_data_size::MemoryDataSize,
    memory_fs::MemoryFs,
    phase_times_monitor::PHASES_TIMES_MONITOR,
};
use utils::assembler_phases::AssemblerStartingStep;

use crate::maximal_unitig_links::build_maximal_unitigs_links;
use crate::reorganize_reads::reorganize_reads;
use crate::{
    build_unitigs::build_unitigs,
    compute_matchtigs::{MatchtigMode, MatchtigsStorageBackend, compute_matchtigs_thread},
};
use crate::{compute_matchtigs::MatchtigHelperTrait, eulertigs::build_eulertigs};

pub mod build_unitigs;
pub mod compute_matchtigs;
pub mod eulertigs;
pub mod hashes_sorting;
pub mod links_compaction;
pub mod maximal_unitig_links;
pub mod reorganize_reads;
pub mod structs;

fn get_writer<
    C: IdentSequenceWriter,
    L: IdentSequenceWriter,
    W: StructuredSequenceBackend<C, L> + StructuredSequenceBackendInit,
>(
    output_file: &Path,
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
pub fn build_final_unitigs<
    MergingHash: HashFunctionFactory,
    AssemblerColorsManager: ColorsManager,
    OutputMode: StructuredSequenceBackendWrapper,
>(
    k: usize,
    sequences: Vec<SingleBucket>,
    reads_map: Vec<SingleBucket>,
    unitigs_map: Vec<SingleBucket>,
    buckets_count: BucketsCount,
    step: AssemblerStartingStep,
    last_step: AssemblerStartingStep,
    output_file: &Path,
    temp_dir: &Path,
    threads_count: usize,
    generate_maximal_unitigs_links: bool,
    compute_tigs_mode: Option<MatchtigMode>,
) {
    let final_unitigs_file = StructuredSequenceWriter::new(
        get_writer::<_, _, OutputMode::Backend<_, _>>(output_file),
        k,
    );

    // Temporary file to store maximal unitigs data without links info, if further processing is requested
    let compressed_temp_unitigs_file = if generate_maximal_unitigs_links
        || compute_tigs_mode.needs_matchtigs_library()
        || compute_tigs_mode == Some(MatchtigMode::FastEulerTigs)
    {
        Some(StructuredSequenceWriter::new(
            StructSeqBinaryWriter::new(
                temp_dir.join("maximal_unitigs.tmp"),
                &(
                    get_memory_mode(SwapPriority::FinalMaps as usize),
                    CompressedCheckpointSize::new_from_size(MemoryDataSize::from_mebioctets(4)),
                    get_compression_level_info(),
                ),
                &(),
            ),
            k,
        ))
    } else {
        None
    };

    let circular_temp_unitigs_file = if let Some(MatchtigMode::FastEulerTigs) = compute_tigs_mode {
        Some(StructuredSequenceWriter::new(
            StructSeqBinaryWriter::new(
                temp_dir.join("circular_unitigs.tmp"),
                &(
                    get_memory_mode(SwapPriority::FinalMaps as usize),
                    CompressedCheckpointSize::new_from_size(MemoryDataSize::from_mebioctets(1)),
                    get_compression_level_info(),
                ),
                &(),
            ),
            k,
        ))
    } else {
        None
    };

    let (reorganized_reads, _final_unitigs_bucket) = if step
        <= AssemblerStartingStep::ReorganizeReads
    {
        if generate_maximal_unitigs_links || compute_tigs_mode.needs_temporary_tigs() {
            reorganize_reads::<MergingHash, AssemblerColorsManager, StructSeqBinaryWriter<_, _>>(
                k,
                sequences,
                reads_map,
                temp_dir,
                compressed_temp_unitigs_file.as_ref().unwrap(),
                circular_temp_unitigs_file.as_ref(),
                buckets_count,
            )
        } else {
            reorganize_reads::<MergingHash, AssemblerColorsManager, OutputMode::Backend<_, _>>(
                k,
                sequences,
                reads_map,
                temp_dir,
                &final_unitigs_file,
                None,
                buckets_count,
            )
        }
    } else {
        (
            generate_bucket_names(temp_dir.join("reads_bucket"), buckets_count, Some("tmp")),
            (generate_bucket_names(
                temp_dir.join("reads_bucket_lonely"),
                BucketsCount::ONE,
                Some("tmp"),
            )
            .into_iter()
            .next()
            .unwrap()
            .path),
        )
    };

    if last_step <= AssemblerStartingStep::ReorganizeReads {
        PHASES_TIMES_MONITOR
            .write()
            .print_stats("Reorganize reads.".to_string());
        return;
    } else {
        MemoryFs::flush_all_to_disk();
        MemoryFs::free_memory();
    }

    // links_manager.compute_id_offsets();
    if step <= AssemblerStartingStep::BuildUnitigs {
        if generate_maximal_unitigs_links
            || compute_tigs_mode.needs_matchtigs_library()
            || compute_tigs_mode == Some(MatchtigMode::FastEulerTigs)
        {
            build_unitigs::<MergingHash, AssemblerColorsManager, StructSeqBinaryWriter<_, _>>(
                reorganized_reads,
                unitigs_map,
                temp_dir,
                compressed_temp_unitigs_file.as_ref().unwrap(),
                if compute_tigs_mode == Some(MatchtigMode::FastEulerTigs) {
                    circular_temp_unitigs_file.as_ref()
                } else {
                    None
                },
                k,
            );
        } else {
            build_unitigs::<MergingHash, AssemblerColorsManager, OutputMode::Backend<_, _>>(
                reorganized_reads,
                unitigs_map,
                temp_dir,
                &final_unitigs_file,
                None,
                k,
            );
        }
    }

    if step <= AssemblerStartingStep::MaximalUnitigsLinks {
        if compute_tigs_mode == Some(MatchtigMode::FastEulerTigs) {
            let circular_temp_unitigs_file = circular_temp_unitigs_file.unwrap();
            let circular_temp_path = circular_temp_unitigs_file.get_path();
            circular_temp_unitigs_file.finalize();

            let compressed_temp_unitigs_file = compressed_temp_unitigs_file.unwrap();
            let temp_path = compressed_temp_unitigs_file.get_path();
            compressed_temp_unitigs_file.finalize();

            build_eulertigs::<MergingHash, AssemblerColorsManager, _, _>(
                circular_temp_path,
                temp_path,
                temp_dir,
                &final_unitigs_file,
                k,
            );
        } else if generate_maximal_unitigs_links || compute_tigs_mode.needs_matchtigs_library() {
            let compressed_temp_unitigs_file = compressed_temp_unitigs_file.unwrap();
            let temp_path = compressed_temp_unitigs_file.get_path();
            compressed_temp_unitigs_file.finalize();

            if let Some(compute_tigs_mode) = compute_tigs_mode.get_matchtigs_mode() {
                let matchtigs_backend = MatchtigsStorageBackend::new();

                let matchtigs_receiver = matchtigs_backend.get_receiver();

                let handle = std::thread::Builder::new()
                    .name("greedy_matchtigs".to_string())
                    .spawn(move || {
                        compute_matchtigs_thread::<AssemblerColorsManager, _>(
                            k,
                            threads_count,
                            matchtigs_receiver,
                            &final_unitigs_file,
                            compute_tigs_mode,
                        );
                    })
                    .unwrap();

                build_maximal_unitigs_links::<
                    MergingHash,
                    AssemblerColorsManager,
                    MatchtigsStorageBackend<_>,
                >(
                    temp_path,
                    temp_dir,
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
                    MergingHash,
                    AssemblerColorsManager,
                    OutputMode::Backend<_, _>,
                >(temp_path, temp_dir, &final_unitigs_file, k);
                final_unitigs_file.finalize();
            }
        } else {
            final_unitigs_file.finalize();
        }
    } else {
        final_unitigs_file.finalize();
    }
}
