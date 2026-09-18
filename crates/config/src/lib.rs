// use crate::RunLengthColorsSerializer;
use parallel_processor::buckets::writers::compressed_binary_writer::{
    CompressedCheckpointSize, CompressionLevelInfo,
};
use parallel_processor::buckets::writers::lock_free_binary_writer::LockFreeCheckpointSize;
use parallel_processor::memory_data_size::MemoryDataSize;
use parallel_processor::memory_fs::file::internal::MemoryFileMode;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::time::Duration;

pub type BucketIndexType = u16;
pub type MinimizerType = u32;
pub type ColorIndexType = u32;
pub type ColorCounterType = usize;
pub type MultiplicityCounterType = u32;

pub const PACKETS_PRIORITY_DEFAULT: usize = 0;
pub const PACKETS_PRIORITY_REWRITTEN: usize = 0;
pub const PACKETS_PRIORITY_COMPACT: usize = 1;
pub const PACKETS_PRIORITY_DONE_RESPLIT: usize = 0;
pub const PACKETS_PRIORITY_FILES: usize = 1;

// pub type DefaultColorsSerializer = RunLengthColorsSerializer;

/// Bases held by each of the eight lanes of a parsed sequences batch.
///
/// A batch is therefore 512K bases, 128 KiB of packed words, which leaves room
/// for the per-lane copies and window masks the bucketing threads build from it
/// while trying to stay inside a core's private cache.
pub const SIMD_LANE_BASES: usize = 64 * 1024;

/// Largest identifier arena a single batch may accumulate before it is sent.
pub const SIMD_BATCH_MAX_HEADER_BYTES: usize = 1024 * 256;

pub const KMERS_TRANSFORM_READS_CHUNKS_SIZE: usize = 1024 * 24;

/// The maximum number of threads that can be spawned for each bucket
pub const MAX_KMERS_TRANSFORM_READERS_PER_BUCKET: usize = 4;

pub const FLUSH_QUEUE_FACTOR: usize = 16;

pub const PARTIAL_VECS_CHECKPOINT_SIZE: CompressedCheckpointSize =
    CompressedCheckpointSize::new_from_size(MemoryDataSize::from_mebioctets(2));

pub const MINIMIZER_BUCKETS_CHECKPOINT_SIZE: LockFreeCheckpointSize =
    LockFreeCheckpointSize::new_from_size(MemoryDataSize::from_mebioctets(8));

pub const MINIMIZER_BUCKETS_COMPACTED_CHECKPOINT_SIZE: CompressedCheckpointSize =
    CompressedCheckpointSize::new_from_size(MemoryDataSize::from_mebioctets(8));

pub const PARTIAL_UNITIGS_COMPACTED_CHECKPOINT_SIZE: CompressedCheckpointSize =
    CompressedCheckpointSize::new_from_size(MemoryDataSize::from_mebioctets(8));

pub const MAX_SUBPARTITION_SIZE: u64 = 1024 * 1024 * 512;
pub const MAX_SUBSUBPARTITION_SIZE: usize = 1024 * 128;
pub const MAX_SUBPARTITIONS_COUNT: u64 = 1 << 11;
pub const MIN_SUBPARTITIONS_COUNT: u64 = 8;
pub const MAX_EXTREMITIES_HASHMAP_SIZE: usize = 524288;

pub const DEFAULT_OUTPUT_BUFFER_SIZE: usize = 1024 * 1024 * 4;
pub const DEFAULT_PER_CPU_BUFFER_SIZE: MemoryDataSize = MemoryDataSize::from_kibioctets(4);

pub const MINIMUM_GLOBAL_MEMORY: MemoryDataSize = MemoryDataSize::from_gibioctets(2);
pub const MEMORY_THRESHOLD_CLEAR_START_OFFSET: MemoryDataSize =
    MemoryDataSize::from_mebioctets(512);

pub const MINIMUM_LOG_DELTA_TIME: Duration = Duration::from_secs(10);

// The maximum size multiplier of a subbucket when compared to the sizes averages
pub const MAX_RESPLIT_SUBBUCKET_AVERAGE_MULTIPLIER: u64 = 2;
pub const MAX_SUBBUCKET_AVERAGE_MULTIPLIER: u64 = 8;
pub const MIN_AVERAGE_CAP: u64 = 5000;
pub const MIN_RESPLIT_BUCKETS_COUNT: u64 = 4;
pub const MAX_RESPLIT_BUCKETS_COUNT: u64 = 1024;

// 1GB of reads max for each bucket
pub const MIN_BUCKET_SIZE: u64 = 512 * 1024;
pub const MAX_BUCKET_SIZE: u64 = 1024 * 1024 * 1024;
pub const MIN_BUCKETS_COUNT_LOG: usize = 2;
pub const DEFAULT_BUCKETS_COUNT_LOG: usize = 10;
pub const MAX_BUCKETS_COUNT_LOG: usize = 13;

pub const MIN_SECOND_BUCKET_SIZE: u64 = 2 * 1024;
pub const MAX_SECOND_BUCKET_SIZE: u64 = 4 * 1024 * 1024;
pub const MIN_SECOND_BUCKETS_COUNT_LOG: usize = 1;
pub const DEFAULT_SECOND_BUCKETS_COUNT_LOG: usize = 6;
pub const MAX_SECOND_BUCKETS_COUNT_LOG: usize = 8;

pub const MAX_RESPLIT_BUCKETS_COUNT_LOG: usize = 9;

pub const MIN_BUCKET_CHUNKS_FOR_READING_THREAD: usize = 2;

pub const RESPLITTING_MAX_K_M_DIFFERENCE: usize = 10;

pub const MIN_SUBSPLIT_COUNT: usize = 16;

pub const MAX_INTERMEDIATE_MAP_SIZE: u64 = 1024 * 1024 * 32;

// Assembler include flags
pub const READ_FLAG_INCL_BEGIN: u8 = 1 << 0;
pub const READ_FLAG_INCL_END: u8 = 1 << 1;

pub const COLORS_SINGLE_INDEX_DEFAULT_COLORS: usize = 1024;
pub const COLORS_SINGLE_BATCH_SIZE: u64 = 20000;
pub const QUERIES_COUNT_MIN_BATCH: u64 = 1000;
pub const MAX_COLORMAP_WRITING_THREADS: usize = 24;
pub const COLORS_BUFFER_DEFAULT_SIZE: usize = 1024 * 16;

/// Total memory the per-bucket super-kmer deduplicators may hold between them.
/// Split across the buckets, so the per-bucket window shrinks as the bucket
/// count grows rather than the total growing with it.
pub const MINIMIZER_DEDUPLICATION_MEMORY: usize = 2048 * 1024 * 1024;

/// Collapse ratio, in percent of records removed over one drain window, below
/// which a bucket's deduplicator is not paying for itself: it stops folding
/// records and writes them straight out in the narrow form the bucketing
/// threads already produced.
pub const MINIMIZER_DEDUP_BYPASS_COLLAPSE_PERCENT: u64 = 10;

/// Smallest window allowed to decide. A drain triggered after a handful of
/// records says nothing about the input.
pub const MINIMIZER_DEDUP_BYPASS_MIN_RECORDS: u64 = 4096;

/// How many storage-fulls of input one bypass covers before the bucket is
/// measured again.
///
/// A measured window folds a whole storage-full at several times the cost of
/// forwarding it, so this is what bounds the price of staying adaptive: one
/// window in sixty-four is under two percent. It is applied in full from the
/// first bypass rather than ramped up to, because that decision already rests
/// on a whole window's evidence and ramping would spend most of a run paying
/// for the small budgets on the way.
pub const MINIMIZER_DEDUP_BYPASS_SKIP_FACTOR: u32 = 64;

pub const DEFAULT_COMPACTION_MAP_SUBBUCKET_ELEMENTS: usize = 8192;
pub const DEFAULT_COMPACTION_MAP_SUBBUCKET_VEC_ELEMENTS: usize = 8192;
pub const DEFAULT_COMPACTION_STORAGE_PER_BUCKET_SIZE: usize = 8192;
pub const DEFAULT_UNCOMPACTED_TEMP_STORAGE: usize = 2 * DEFAULT_BUCKETS_CHUNK_SIZE as usize;

pub const PRIORITY_SCHEDULING_HIGH: usize = 0;
pub const PRIORITY_SCHEDULING_BASE: usize = 1;
pub const PRIORITY_SCHEDULING_LOW: usize = 2;

// Each chunk in a bucket must be between 256KB and 8MB
pub const MIN_BUCKETS_CHUNK_SIZE: u64 = 1024 * 256;
pub const DEFAULT_BUCKETS_CHUNK_SIZE: u64 = 1024 * 1024 * 2;
/// Largest an uncompacted bucket chunk may grow before it is compacted.
///
/// Held down to 2 MiB so that compaction runs often and on little data at a
/// time, which is what makes a light compaction -- one that never re-reads an
/// already compacted chunk -- cheap enough to be the common case.
pub const MAX_BUCKETS_CHUNK_SIZE: u64 = 1024 * 1024 * 2;

// The soft limit to compaction iterations per bucket, sizes will be chosen to avoid exceeding this threshold
pub const MAX_COMPACTION_ITERATIONS: u64 = 8;

pub const TARGET_CHUNKS_PER_BUCKET: u64 = 16;
pub const MIN_TARGET_CHUNK_SIZE: u64 = MIN_BUCKETS_CHUNK_SIZE;
pub const DEFAULT_TARGET_CHUNK_SIZE_MULTIPLIER: u64 = 8;
pub const MAX_TARGET_CHUNK_SIZE: u64 = 1024 * 1024 * 64;

pub const HASH_MAX_OVERREAD: usize = 16;

pub const MAX_INLINE_UNITIG_SIZE: usize = 4096;

// Higher priority means faster swap to disk
pub struct SwapPriority {}
#[allow(non_upper_case_globals)]
impl SwapPriority {
    pub const MinimizerUncompressedTempBuckets: usize = 0;
    pub const ResplitBuckets: usize = 0;
    pub const MinimizerBuckets: usize = 1;
    pub const FinalMaps: usize = 1;
    pub const ResultBuckets: usize = 1;
    pub const HashBuckets: usize = 2;
    pub const QueryCounters: usize = 2;
    pub const ReorganizeReads: usize = 3;
    pub const LinksBuckets: usize = 3;
    pub const LinkPairs: usize = 4;
    pub const KmersMergeTempColors: usize = 4;
    pub const ColoredQueryBuckets: usize = 5;
    pub const KmersMergeBuckets: usize = 6;
    pub const OversizeUnitigs: usize = 7;
}

// Functions depending on global config parameters set at runtime
pub static KEEP_FILES: AtomicBool = AtomicBool::new(false);
pub static INTERMEDIATE_COMPRESSION_LEVEL_SLOW: AtomicU32 = AtomicU32::new(3);
pub static INTERMEDIATE_COMPRESSION_LEVEL_FAST: AtomicU32 = AtomicU32::new(0);
pub static OUTPUT_COMPRESSION_LEVEL: AtomicU32 = AtomicU32::new(2);
pub static PREFER_MEMORY: AtomicBool = AtomicBool::new(false);

pub fn get_memory_mode(swap_priority: usize) -> MemoryFileMode {
    if PREFER_MEMORY.load(Ordering::Relaxed) {
        MemoryFileMode::PreferMemory { swap_priority }
    } else {
        MemoryFileMode::DiskOnly
    }
}

pub fn get_compression_level_info() -> CompressionLevelInfo {
    CompressionLevelInfo {
        fast_disk: INTERMEDIATE_COMPRESSION_LEVEL_FAST.load(Ordering::Relaxed),
        slow_disk: INTERMEDIATE_COMPRESSION_LEVEL_SLOW.load(Ordering::Relaxed),
    }
}
