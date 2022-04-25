use parallel_processor::buckets::writers::compressed_binary_writer::CompressedCheckpointSize;
use parallel_processor::buckets::writers::lock_free_binary_writer::LockFreeCheckpointSize;
use parallel_processor::memory_data_size::MemoryDataSize;
use std::sync::atomic::AtomicUsize;
use std::time::Duration;

pub type BucketIndexType = u16;
pub type MinimizerType = u32;

pub const READ_INTERMEDIATE_CHUNKS_SIZE: usize = 1024 * 512 * 1;
pub static READ_INTERMEDIATE_QUEUE_MULTIPLIER: AtomicUsize = AtomicUsize::new(2);

/// 2MB read file prefetch
pub const DEFAULT_PREFETCH_AMOUNT: Option<usize> = Some(1024 * 1024 * 2);

pub const FLUSH_QUEUE_FACTOR: usize = 1;

pub const PARTIAL_VECS_CHECKPOINT_SIZE: LockFreeCheckpointSize =
    LockFreeCheckpointSize::new_from_size(MemoryDataSize::from_mebioctets(2));

pub const MINIMIZER_BUCKETS_CHECKPOINT_SIZE: CompressedCheckpointSize =
    CompressedCheckpointSize::new_from_size(MemoryDataSize::from_mebioctets(8));

pub const DEFAULT_OUTPUT_BUFFER_SIZE: usize = 1024 * 1024 * 4;
pub const DEFAULT_PER_CPU_BUFFER_SIZE: MemoryDataSize = MemoryDataSize::from_kibioctets(4);

pub const MINIMUM_LOG_DELTA_TIME: Duration = Duration::from_secs(15);

pub const DEFAULT_LZ4_COMPRESSION_LEVEL: u32 = 0;

// 192MB of reads for each bucket
pub const MAX_BUCKET_SIZE: u64 = 192 * 1024 * 1024;
pub const MIN_BUCKETS_COUNT_LOG: usize = 9;
pub const MAX_BUCKETS_COUNT_LOG: usize = 13;

pub const USE_SECOND_BUCKET: bool = false;

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
