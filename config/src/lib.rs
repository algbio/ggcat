// use crate::RunLengthColorsSerializer;
use parallel_processor::buckets::writers::compressed_binary_writer::CompressedCheckpointSize;
use parallel_processor::memory_data_size::MemoryDataSize;
use parallel_processor::memory_fs::file::internal::MemoryFileMode;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

pub type BucketIndexType = u16;
pub type MinimizerType = u32;
pub type ColorIndexType = u32;

pub const PACKETS_PRIORITY_DEFAULT: usize = 0;
pub const PACKETS_PRIORITY_REWRITTEN: usize = 0;
pub const PACKETS_PRIORITY_DONE_RESPLIT: usize = 0;
pub const PACKETS_PRIORITY_FILES: usize = 1;
// pub const PACKETS_PRIORITY_PENDING_RESPLIT: usize = 3;

// pub type DefaultColorsSerializer = RunLengthColorsSerializer;

pub const READ_INTERMEDIATE_CHUNKS_SIZE: usize = 1024 * 512 * 1;
pub static READ_INTERMEDIATE_QUEUE_MULTIPLIER: AtomicUsize = AtomicUsize::new(2);

pub const KMERS_TRANSFORM_READS_CHUNKS_SIZE: usize = 1024 * 24;

/// 2MB read file prefetch
pub const DEFAULT_PREFETCH_AMOUNT: Option<usize> = Some(1024 * 1024 * 2);

pub const FLUSH_QUEUE_FACTOR: usize = 1;

pub const PARTIAL_VECS_CHECKPOINT_SIZE: CompressedCheckpointSize =
    CompressedCheckpointSize::new_from_size(MemoryDataSize::from_mebioctets(2));

pub const MINIMIZER_BUCKETS_CHECKPOINT_SIZE: CompressedCheckpointSize =
    CompressedCheckpointSize::new_from_size(MemoryDataSize::from_mebioctets(8));

pub const DEFAULT_OUTPUT_BUFFER_SIZE: usize = 1024 * 1024 * 4;
pub const DEFAULT_PER_CPU_BUFFER_SIZE: MemoryDataSize = MemoryDataSize::from_kibioctets(4);

pub const MINIMUM_LOG_DELTA_TIME: Duration = Duration::from_secs(10);

pub const DEFAULT_LZ4_COMPRESSION_LEVEL: u32 = 0;

// 192MB of reads for each bucket
pub const MAX_BUCKET_SIZE: u64 = 192 * 1024 * 1024;
pub const MIN_BUCKETS_COUNT_LOG: usize = 10;
pub const MAX_BUCKETS_COUNT_LOG: usize = 13;
pub const MAX_RESPLIT_BUCKETS_COUNT_LOG: usize = 9;

pub const MIN_BUCKET_CHUNKS_FOR_READING_THREAD: usize = 2;

pub const USE_SECOND_BUCKET: bool = false;

pub const RESPLITTING_MAX_K_M_DIFFERENCE: usize = 10;

pub const MINIMUM_SUBBUCKET_KMERS_COUNT: usize = 1024 * 512;
pub const MAXIMUM_SECOND_BUCKETS_LOG: usize = 8;
pub const MAXIMUM_SECOND_BUCKETS_COUNT: usize = 1 << MAXIMUM_SECOND_BUCKETS_LOG;
pub const MAXIMUM_JIT_PROCESSED_BUCKETS: usize = 16;

pub const MIN_OUTLIER_SIZE: u64 = 200000;
pub const MAX_NON_OUTLIER_SIZE: u64 = 500000;

// Assembler include flags
pub const READ_FLAG_INCL_BEGIN: u8 = 1 << 0;
pub const READ_FLAG_INCL_END: u8 = 1 << 1;

pub const COLORS_SINGLE_BATCH_SIZE: u64 = 20000;

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
    pub const KmersMergeTempColors: usize = 4;
    pub const KmersMergeBuckets: usize = 6;
}

// Functions depending on global config parameters set at runtime
pub static KEEP_FILES: AtomicBool = AtomicBool::new(false);
pub static PREFER_MEMORY: AtomicBool = AtomicBool::new(false);

pub fn get_memory_mode(swap_priority: usize) -> MemoryFileMode {
    if PREFER_MEMORY.load(Ordering::Relaxed) {
        MemoryFileMode::PreferMemory { swap_priority }
    } else {
        MemoryFileMode::DiskOnly
    }
}
