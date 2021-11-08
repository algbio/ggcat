pub struct AssemblePipeline;

pub mod assembler_minimizer_bucketing;
mod build_unitigs;
mod formatter_fasta;
mod hashes_sorting;
#[cfg(not(feature = "kpar"))]
pub mod kmers_merge;
mod links_compaction;
#[cfg(feature = "kpar")]
pub mod parallel_kmers_merge;
mod reorganize_reads;

#[cfg(not(feature = "kpar"))]
pub use kmers_merge as current_kmers_merge;

#[cfg(feature = "kpar")]
pub use parallel_kmers_merge as current_kmers_merge;
