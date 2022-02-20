pub struct AssemblePipeline;

pub mod assembler_minimizer_bucketing;
pub mod build_unitigs;
mod formatter_fasta;
mod hashes_sorting;
mod links_compaction;
pub mod parallel_kmers_merge;
mod reorganize_reads;
pub mod unitig_links_manager;
