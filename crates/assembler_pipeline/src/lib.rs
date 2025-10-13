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
pub mod extend_unitigs;
pub mod hashes_sorting;
pub mod links_compaction;
pub mod maximal_unitig_links;
pub mod reorganize_reads;
pub mod structs;
