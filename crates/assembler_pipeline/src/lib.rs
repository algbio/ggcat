#![cfg_attr(not(feature = "full"), allow(unused_imports))]

use std::{
    any::Any,
    path::{Path, PathBuf},
    sync::Arc,
};

use ::dynamic_dispatch::dynamic_dispatch;
use colors::colors_manager::{ColorsManager, color_types::PartialUnitigsColorStructure};
use config::{SwapPriority, get_compression_level_info, get_memory_mode};
use hashes::HashFunctionFactory;
use io::{
    concurrent::{
        structured_sequences::{
            IdentSequenceWriter, StructuredSequenceBackend, StructuredSequenceBackendInit,
            StructuredSequenceBackendWrapper, StructuredSequenceWriter,
            binary::StructSeqBinaryWriter,
            fasta::FastaWriterWrapper,
            gfa::{GFAWriterWrapperV1, GFAWriterWrapperV2},
        },
        temp_reads::extra_data::{SequenceExtraData, SequenceExtraDataConsecutiveCompression},
    },
    debug_load_single_buckets, debug_save_single_buckets,
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
use utils::assembler_phases::AssemblerPhase;

use crate::compute_matchtigs::{MatchtigMode, MatchtigsStorageBackend, compute_matchtigs_thread};
use crate::{compute_matchtigs::MatchtigHelperTrait, eulertigs::build_eulertigs};
use crate::{extend_unitigs::extend_unitigs, maximal_unitig_links::build_maximal_unitigs_links};

pub mod compute_matchtigs;
pub mod eulertigs;
pub mod extend_unitigs;
pub mod maximal_unitig_links;

pub enum OutputFileMode<
    OutputMode: StructuredSequenceBackendWrapper,
    ColorInfo: IdentSequenceWriter + SequenceExtraDataConsecutiveCompression,
    LinksInfo: IdentSequenceWriter + SequenceExtraData,
> {
    Final {
        output_file: Arc<
            StructuredSequenceWriter<
                ColorInfo,
                LinksInfo,
                OutputMode::Backend<ColorInfo, LinksInfo>,
            >,
        >,
    },
    Intermediate {
        flat_unitigs:
            Arc<StructuredSequenceWriter<ColorInfo, (), StructSeqBinaryWriter<ColorInfo, ()>>>,
        circular_unitigs: Option<
            Arc<StructuredSequenceWriter<ColorInfo, (), StructSeqBinaryWriter<ColorInfo, ()>>>,
        >,
    },
}

pub fn get_final_output_writer<
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
    step: AssemblerPhase,
    last_step: AssemblerPhase,
    output_file: &Path,
    temp_dir: &Path,
    threads_count: usize,
    generate_maximal_unitigs_links: bool,
    compute_tigs_mode: Option<MatchtigMode>,
    output_file_mode: Box<dyn Any>,
) {
    let output_file_mode = *output_file_mode.downcast::<OutputFileMode<
        OutputMode,
        PartialUnitigsColorStructure<AssemblerColorsManager>,
        (),
    >>().unwrap();

    if step <= AssemblerPhase::UnitigsExtension {
        match &output_file_mode {
            OutputFileMode::Final { output_file } => {
                extend_unitigs::<MergingHash, AssemblerColorsManager, OutputMode::Backend<_, _>>(
                    sequences,
                    &temp_dir,
                    output_file,
                    None,
                    k,
                );
            }
            OutputFileMode::Intermediate {
                flat_unitigs,
                circular_unitigs,
            } => {
                extend_unitigs::<MergingHash, AssemblerColorsManager, StructSeqBinaryWriter<_, _>>(
                    sequences,
                    &temp_dir,
                    flat_unitigs,
                    circular_unitigs.as_deref(),
                    k,
                );
            }
        }
    }

    if last_step <= AssemblerPhase::UnitigsExtension {
        return;
    }

    if step <= AssemblerPhase::MaximalUnitigsLinks {
        match output_file_mode {
            OutputFileMode::Final { output_file } => {
                Arc::try_unwrap(output_file)
                    .map_err(|_| ())
                    .unwrap()
                    .finalize();
            }
            OutputFileMode::Intermediate {
                flat_unitigs,
                circular_unitigs,
            } => {
                let final_unitigs_file = StructuredSequenceWriter::new(
                    get_final_output_writer::<_, _, OutputMode::Backend<_, _>>(&output_file),
                    k,
                );

                if compute_tigs_mode == Some(MatchtigMode::FastEulerTigs) {
                    let circular_temp_unitigs_file = Arc::try_unwrap(circular_unitigs.unwrap())
                        .map_err(|_| ())
                        .unwrap();
                    let circular_temp_path = circular_temp_unitigs_file.get_path();
                    circular_temp_unitigs_file.finalize();

                    let compressed_temp_unitigs_file =
                        Arc::try_unwrap(flat_unitigs).map_err(|_| ()).unwrap();
                    let temp_path = compressed_temp_unitigs_file.get_path();
                    compressed_temp_unitigs_file.finalize();

                    build_eulertigs::<MergingHash, AssemblerColorsManager, _, _>(
                        circular_temp_path,
                        temp_path,
                        &temp_dir,
                        &final_unitigs_file,
                        k,
                    );
                } else if generate_maximal_unitigs_links
                    || compute_tigs_mode.needs_matchtigs_library()
                {
                    let compressed_temp_unitigs_file =
                        Arc::try_unwrap(flat_unitigs).map_err(|_| ()).unwrap();
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
                            &temp_dir,
                            &StructuredSequenceWriter::new(matchtigs_backend, k),
                            k,
                        );

                        handle.join().unwrap();
                    } else if generate_maximal_unitigs_links {
                        final_unitigs_file.finalize();

                        let final_unitigs_file = StructuredSequenceWriter::new(
                            get_final_output_writer::<_, _, OutputMode::Backend<_, _>>(
                                &output_file,
                            ),
                            k,
                        );

                        build_maximal_unitigs_links::<
                            MergingHash,
                            AssemblerColorsManager,
                            OutputMode::Backend<_, _>,
                        >(temp_path, &temp_dir, &final_unitigs_file, k);
                        final_unitigs_file.finalize();
                    }
                }
            }
        }
    }
}
