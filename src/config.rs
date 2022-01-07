use parallel_processor::memory_data_size::MemoryDataSize;
use std::sync::atomic::AtomicUsize;

pub type BucketIndexType = u16;
pub type MinimizerType = u32;
pub type SortingHashType = u16;

pub const FIRST_BUCKET_BITS: usize = 9;
pub const SECOND_BUCKET_BITS: usize = 8;

pub const READ_INTERMEDIATE_CHUNKS_SIZE: usize = 1024 * 1024 * 8;
pub static READ_INTERMEDIATE_QUEUE_SIZE: AtomicUsize = AtomicUsize::new(32);

const_assert!(FIRST_BUCKET_BITS + SECOND_BUCKET_BITS >= 16);

pub const FIRST_BUCKETS_COUNT: usize = 1 << FIRST_BUCKET_BITS;
pub const SECOND_BUCKETS_COUNT: usize = 1 << SECOND_BUCKET_BITS;

pub const DEFAULT_OUTPUT_BUFFER_SIZE: usize = 1024 * 1024 * 4;
pub const DEFAULT_PER_CPU_BUFFER_SIZE: MemoryDataSize = MemoryDataSize::from_kibioctets(8.0);

pub struct SwapPriority {}
#[allow(non_upper_case_globals)]
impl SwapPriority {
    pub const MinimizerBuckets: usize = 0;
    pub const FinalMaps: usize = 1;
    pub const Default: usize = 1;
    pub const HashBuckets: usize = 2;
    pub const LinksBuckets: usize = 3;
    pub const KmersMergeBuckets: usize = 4;
}
