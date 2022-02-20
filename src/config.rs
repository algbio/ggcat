use parallel_processor::memory_data_size::MemoryDataSize;
use std::mem::size_of;
use std::sync::atomic::AtomicUsize;

pub type BucketIndexType = u16;
pub type MinimizerType = u32;
pub type SortingHashType = u16;

pub const FIRST_BUCKET_BITS: usize = 10;
pub const SECOND_BUCKET_BITS: usize = 8;

pub const SORTING_HASH_SHIFT: usize =
    (size_of::<MinimizerType>() - size_of::<SortingHashType>()) * 8;

pub const READ_INTERMEDIATE_CHUNKS_SIZE: usize = 1024 * 1024 * 1;
pub static READ_INTERMEDIATE_QUEUE_MULTIPLIER: AtomicUsize = AtomicUsize::new(3);

pub const DEFAULT_MINIMIZER_MASK: MinimizerType = MinimizerType::MAX;

/// For the resplit mask, exclude the second bucket bits, as it will not be used while splitting
pub const RESPLIT_MINIMIZER_MASK: MinimizerType =
    (MinimizerType::MAX << SORTING_HASH_SHIFT) | (FIRST_BUCKETS_COUNT as u32 - 1);

pub const RESPLITTING_MAX_K_M_DIFFERENCE: usize = 5;

const_assert!(FIRST_BUCKET_BITS + SECOND_BUCKET_BITS >= 16);

pub const FIRST_BUCKETS_COUNT: usize = 1 << FIRST_BUCKET_BITS;
pub const SECOND_BUCKETS_COUNT: usize = 1 << SECOND_BUCKET_BITS;

pub const MERGE_RESULTS_BUCKETS_COUNT: usize = 256;

pub const DEFAULT_OUTPUT_BUFFER_SIZE: usize = 1024 * 1024 * 4;
pub const DEFAULT_PER_CPU_BUFFER_SIZE: MemoryDataSize = MemoryDataSize::from_kibioctets(8);

pub struct SwapPriority {}
#[allow(non_upper_case_globals)]
impl SwapPriority {
    pub const MinimizerBuckets: usize = 0;
    pub const FinalMaps: usize = 1;
    pub const ResultBuckets: usize = 1;
    pub const HashBuckets: usize = 2;
    pub const ReorganizeReads: usize = 3;
    pub const LinksBuckets: usize = 3;
    pub const LinkPairs: usize = 4;
    pub const KmersMergeBuckets: usize = 6;
}
