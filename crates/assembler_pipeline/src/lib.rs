#![cfg_attr(not(feature = "full"), allow(unused_imports))]

use std::{
    any::Any,
    path::{Path, PathBuf},
    sync::Arc,
    sync::atomic::Ordering,
};

use ::dynamic_dispatch::dynamic_dispatch;
use colors::colors_manager::{
    ColorsManager, ColorsMergeManager,
    color_types::{ColorsMergeManagerType, GlobalColorsTableWriter, PartialUnitigsColorStructure},
};
use config::ColorIndexType;
use config::{OUTPUT_COMPRESSION_LEVEL, SwapPriority, get_compression_level_info, get_memory_mode};
use hashes::HashFunctionFactory;
use io::{
    concurrent::temp_reads::extra_data::SequenceExtraDataTempBufferManagement,
    concurrent::temp_reads::extra_data::{
        SequenceExtraData, SequenceExtraDataConsecutiveCompression,
    },
    concurrent_filewriter::ConcurrentFileWriter,
    debug_load_single_buckets, debug_save_single_buckets,
    ident_writer::IdentSequenceWriter,
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
use sequence_output::structured_sequences::{
    StructuredSequenceBackend, StructuredSequenceBackendInit, StructuredSequenceBackendWrapper,
    StructuredSequenceWriter,
    binary::StructSeqBinaryWriter,
    fasta::FastaWriterWrapper,
    gfa::{GFAWriterWrapperV1, GFAWriterWrapperV2},
};
use utils::assembler_phases::AssemblerPhase;

use crate::{compute_matchtigs::MatchtigHelperTrait, eulertigs::build_eulertigs};
use crate::{
    compute_matchtigs::{MatchtigMode, MatchtigsStorageBackend, compute_matchtigs_thread},
    extend_unitigs::INDIRECT_UNITIGS_FILE,
};
use crate::{extend_unitigs::extend_unitigs, maximal_unitig_links::build_maximal_unitigs_links};

pub mod compute_matchtigs;
pub mod eulertigs;
pub mod extend_unitigs;
pub mod maximal_unitig_links;

pub struct ShortContig {
    pub bases: Vec<u8>,
    pub color: ColorIndexType,
}

fn write_short_contigs<CX, L, BK>(
    writer: &StructuredSequenceWriter<CX, L, BK>,
    short_contigs: &[ShortContig],
    colors_table: &GlobalColorsTableWriter<CX>,
    k: usize,
    links: L,
) where
    CX: ColorsManager,
    L: IdentSequenceWriter + SequenceExtraData + Clone,
    BK: StructuredSequenceBackend<CX, L>,
{
    if short_contigs.is_empty() {
        return;
    }
    let mut output_buffer = BK::alloc_temp_buffer(k);
    let mut colors_buffer = PartialUnitigsColorStructure::<CX>::new_temp_buffer();
    for contig in short_contigs {
        PartialUnitigsColorStructure::<CX>::clear_temp_buffer(&mut colors_buffer);
        let colors = ColorsMergeManagerType::<CX>::short_contig_color(
            colors_table,
            contig.color,
            contig.bases.len(),
            &mut colors_buffer,
        );
        writer.write_short_sequence(
            &mut output_buffer,
            &contig.bases,
            colors,
            &colors_buffer,
            links.clone(),
            &L::new_temp_buffer(),
        );
    }
}

pub enum OutputFileMode<
    OutputMode: StructuredSequenceBackendWrapper,
    CX: ColorsManager,
    LinksInfo: IdentSequenceWriter + SequenceExtraData,
> {
    Final {
        output_file:
            Arc<StructuredSequenceWriter<CX, LinksInfo, OutputMode::Backend<CX, LinksInfo>>>,
    },
    Intermediate {
        flat_unitigs: Arc<StructuredSequenceWriter<CX, (), StructSeqBinaryWriter<CX, ()>>>,
        circular_unitigs:
            Option<Arc<StructuredSequenceWriter<CX, (), StructSeqBinaryWriter<CX, ()>>>>,
    },
}

pub fn get_final_output_writer<
    CX: ColorsManager,
    L: IdentSequenceWriter,
    W: StructuredSequenceBackend<CX, L> + StructuredSequenceBackendInit,
>(
    output_file: &Path,
) -> W {
    let output_compression_level = OUTPUT_COMPRESSION_LEVEL.load(Ordering::Relaxed);

    match output_file.extension() {
        Some(ext) => match ext.to_string_lossy().to_string().as_str() {
            "lz4" => W::new_compressed_lz4(&output_file, output_compression_level.min(16)),
            "gz" => W::new_compressed_gzip(&output_file, output_compression_level.min(9)),
            "zst" | "zstd" => {
                W::new_compressed_zstd(&output_file, output_compression_level.min(22))
            }
            "bz2" => W::new_compressed_bz2(&output_file, output_compression_level.min(9)),
            "xz" => W::new_compressed_xz(&output_file, output_compression_level.min(9)),
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
    short_contigs: &[ShortContig],
    colors_table: &dyn Any,
) {
    let colors_table = colors_table
        .downcast_ref::<Arc<GlobalColorsTableWriter<AssemblerColorsManager>>>()
        .unwrap();
    let output_file_mode = *output_file_mode
        .downcast::<OutputFileMode<OutputMode, AssemblerColorsManager, ()>>()
        .unwrap();

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
        let indirect_unitigs_file =
            ConcurrentFileWriter::append_existing(temp_dir.join(INDIRECT_UNITIGS_FILE)).unwrap();

        match output_file_mode {
            OutputFileMode::Final { output_file } => {
                write_short_contigs(&output_file, short_contigs, colors_table.as_ref(), k, ());
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
                    write_short_contigs(
                        &final_unitigs_file,
                        short_contigs,
                        colors_table.as_ref(),
                        k,
                        (),
                    );
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
                        &indirect_unitigs_file,
                    );
                } else if generate_maximal_unitigs_links
                    || compute_tigs_mode.needs_matchtigs_library()
                {
                    let compressed_temp_unitigs_file =
                        Arc::try_unwrap(flat_unitigs).map_err(|_| ()).unwrap();
                    let temp_path = compressed_temp_unitigs_file.get_path();
                    compressed_temp_unitigs_file.finalize();

                    if let Some(compute_tigs_mode) = compute_tigs_mode.get_matchtigs_mode() {
                        write_short_contigs(
                            &final_unitigs_file,
                            short_contigs,
                            colors_table.as_ref(),
                            k,
                            (),
                        );
                        let matchtigs_backend = MatchtigsStorageBackend::new();

                        let matchtigs_receiver = matchtigs_backend.get_receiver();

                        let indirect_unitigs_file_thread = indirect_unitigs_file.clone();
                        let handle = std::thread::Builder::new()
                            .name("greedy_matchtigs".to_string())
                            .spawn(move || {
                                compute_matchtigs_thread::<AssemblerColorsManager, _>(
                                    k,
                                    threads_count,
                                    matchtigs_receiver,
                                    &final_unitigs_file,
                                    compute_tigs_mode,
                                    &indirect_unitigs_file_thread,
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
                            &indirect_unitigs_file,
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
                        write_short_contigs(
                            &final_unitigs_file,
                            short_contigs,
                            colors_table.as_ref(),
                            k,
                            crate::maximal_unitig_links::maximal_unitig_index::DoubleMaximalUnitigLinks::EMPTY,
                        );

                        build_maximal_unitigs_links::<
                            MergingHash,
                            AssemblerColorsManager,
                            OutputMode::Backend<_, _>,
                        >(
                            temp_path,
                            &temp_dir,
                            &final_unitigs_file,
                            k,
                            &indirect_unitigs_file,
                        );
                        final_unitigs_file.finalize();
                    }
                }
            }
        }
    }
}
