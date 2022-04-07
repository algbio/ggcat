use parallel_processor::buckets::writers::compressed_binary_writer::CompressedCheckpointSize;
use parallel_processor::buckets::writers::lock_free_binary_writer::LockFreeCheckpointSize;
use parallel_processor::memory_data_size::MemoryDataSize;
use std::mem::size_of;
use std::sync::atomic::AtomicUsize;
use std::time::Duration;

pub type BucketIndexType = u16;
pub type MinimizerType = u32;
pub type SortingHashType = u16;

pub const FIRST_BUCKET_BITS: usize = 10;
pub const SECOND_BUCKET_BITS: usize = 8;

pub const SORTING_HASH_SHIFT: usize =
    (size_of::<MinimizerType>() - size_of::<SortingHashType>()) * 8;

pub const READ_INTERMEDIATE_CHUNKS_SIZE: usize = 1024 * 512 * 1;
pub static READ_INTERMEDIATE_QUEUE_MULTIPLIER: AtomicUsize = AtomicUsize::new(2);

pub const DEFAULT_MINIMIZER_MASK: MinimizerType = MinimizerType::MAX;

pub const FLUSH_QUEUE_FACTOR: usize = 1;

/// For the resplit mask, exclude the second bucket bits, as it will not be used while splitting
pub const RESPLIT_MINIMIZER_MASK: MinimizerType =
    (MinimizerType::MAX << SORTING_HASH_SHIFT) | (FIRST_BUCKETS_COUNT as u32 - 1);

pub const RESPLITTING_MAX_K_M_DIFFERENCE: usize = 5;

const_assert!(FIRST_BUCKET_BITS + SECOND_BUCKET_BITS >= 16);

pub const FIRST_BUCKETS_COUNT: usize = 1 << FIRST_BUCKET_BITS;
pub const SECOND_BUCKETS_COUNT: usize = 1 << SECOND_BUCKET_BITS;

pub const PARTIAL_VECS_CHECKPOINT_SIZE: LockFreeCheckpointSize =
    LockFreeCheckpointSize::new_from_size(MemoryDataSize::from_mebioctets(2));

pub const MINIMIZER_BUCKETS_CHECKPOINT_SIZE: CompressedCheckpointSize =
    CompressedCheckpointSize::new_from_size(MemoryDataSize::from_mebioctets(8));

pub const MERGE_RESULTS_BUCKETS_COUNT: usize = 256;

pub const DEFAULT_OUTPUT_BUFFER_SIZE: usize = 1024 * 1024 * 4;
pub const DEFAULT_PER_CPU_BUFFER_SIZE: MemoryDataSize = MemoryDataSize::from_kibioctets(4);

pub const MINIMUM_LOG_DELTA_TIME: Duration = Duration::from_secs(15);

pub const MINIMUM_RESPLIT_SIZE: usize = 1024 * 1024 * 32;

pub const DEFAULT_LZ4_COMPRESSION_LEVEL: u32 = 1;

pub const EXTRA_BUFFERS_COUNT: usize = SECOND_BUCKETS_COUNT * 20 / 3;

pub const OUTLIER_MIN_DIFFERENCE: f64 = 0.3;
pub const OUTLIER_MAX_SIZE_RATIO: f64 = 0.1;
pub const SUBBUCKET_OUTLIER_DIVISOR: usize = 16;
pub const OUTLIER_MAX_NUMBER_RATIO: usize = 64;

pub struct SwapPriority {}
#[allow(non_upper_case_globals)]
impl SwapPriority {
    pub const MinimizerBuckets: usize = 0;
    pub const FinalMaps: usize = 1;
    pub const ResultBuckets: usize = 1;
    pub const HashBuckets: usize = 2;
    pub const QueryCounters: usize = 2;
    pub const ReorganizeReads: usize = 3;
    pub const LinksBuckets: usize = 3;
    pub const LinkPairs: usize = 4;
    pub const KmersMergeBuckets: usize = 6;
}
