use crate::sequences_stream::general::GeneralSequenceBlockData;
use config::{
    DEFAULT_BUCKETS_CHUNK_SIZE, DEFAULT_BUCKETS_COUNT_LOG, DEFAULT_PER_CPU_BUFFER_SIZE,
    DEFAULT_SECOND_BUCKETS_COUNT_LOG, DEFAULT_TARGET_CHUNK_SIZE_MULTIPLIER, MAX_BUCKET_SIZE,
    MAX_BUCKETS_CHUNK_SIZE, MAX_BUCKETS_COUNT_LOG, MAX_COMPACTION_ITERATIONS,
    MAX_SECOND_BUCKET_SIZE, MAX_SECOND_BUCKETS_COUNT_LOG, MAX_TARGET_CHUNK_SIZE, MIN_BUCKET_SIZE,
    MIN_BUCKETS_CHUNK_SIZE, MIN_BUCKETS_COUNT_LOG, MIN_SECOND_BUCKET_SIZE,
    MIN_SECOND_BUCKETS_COUNT_LOG, MIN_TARGET_CHUNK_SIZE,
};
use parallel_processor::buckets::{BucketsCount, ExtraBucketData, ExtraBuckets, SingleBucket};
use std::path::Path;

pub mod compressed_read;
pub mod concurrent;
pub mod lines_reader;
pub mod sequences_reader;
pub mod sequences_stream;
pub mod structs;
pub mod varint;

pub const DUPLICATES_BUCKET_EXTRA: ExtraBucketData = ExtraBucketData(0xdddd);

pub fn generate_bucket_names(
    root: impl AsRef<Path>,
    count: BucketsCount,
    suffix: Option<&str>,
) -> Vec<SingleBucket> {
    (0..count.total_buckets_count)
        .map(|i| SingleBucket {
            index: i,
            path: root.as_ref().with_extension(format!(
                "{}{}",
                i,
                match suffix {
                    None => String::from(""),
                    Some(s) => format!(".{}", s),
                }
            )),
            extra_bucket_data: if i >= count.normal_buckets_count {
                match count.extra_buckets_count {
                    ExtraBuckets::None => None,
                    ExtraBuckets::Extra { data, .. } => Some(data),
                }
            } else {
                None
            },
        })
        .collect()
}

pub struct FilesStatsInfo {
    pub best_buckets_count_log: usize,
    pub best_second_buckets_count_log: usize,
    pub bucket_size_compaction_threshold: u64,
    pub target_chunk_size: u64,
}

pub fn compute_stats_from_input_blocks(
    blocks: &[GeneralSequenceBlockData],
) -> anyhow::Result<FilesStatsInfo> {
    let mut bases_count = 0;
    for block in blocks {
        bases_count += block.estimated_bases_count()?;
    }

    let buckets_count = {
        let min_buckets_count = bases_count / MAX_BUCKET_SIZE;
        let max_buckets_count = bases_count / MIN_BUCKET_SIZE;
        let default_buckets_count = 1 << DEFAULT_BUCKETS_COUNT_LOG;

        default_buckets_count
            // Avoid producing a lot of small buckets
            .min(max_buckets_count)
            // Avoid producing too large buckets
            .max(min_buckets_count)
            .next_power_of_two()
            // Global thresholds
            .min(1 << MAX_BUCKETS_COUNT_LOG)
            .max(1 << MIN_BUCKETS_COUNT_LOG)
    };

    let bucket_size_compaction_threshold = {
        let bases_per_bucket = bases_count / buckets_count;
        let min_threshold_limiting_compactions = bases_per_bucket / MAX_COMPACTION_ITERATIONS;

        // Compute the threshold
        DEFAULT_BUCKETS_CHUNK_SIZE
            // Avoid producing more than MAX_COMPACTION_ITERATIONS chunks
            .max(min_threshold_limiting_compactions.next_power_of_two())
            // Cap the maximum size to the actual size of a bucket
            .min(bases_per_bucket)
            // Global thresholds
            .max(MIN_BUCKETS_CHUNK_SIZE)
            .min(MAX_BUCKETS_CHUNK_SIZE)
            // Subtract the cpu buffer size to avoid wasting a full block for the new data added before chunking
            - (DEFAULT_PER_CPU_BUFFER_SIZE.as_bytes() as u64)
    };

    let target_chunk_size = (bucket_size_compaction_threshold
        * DEFAULT_TARGET_CHUNK_SIZE_MULTIPLIER)
        .min(MAX_TARGET_CHUNK_SIZE)
        .max(MIN_TARGET_CHUNK_SIZE);

    let second_buckets_count = {
        let bases_per_bucket = bases_count / buckets_count;
        let min_second_buckets_count = bases_per_bucket / MAX_SECOND_BUCKET_SIZE;
        let max_second_buckets_count = bases_per_bucket / MIN_SECOND_BUCKET_SIZE;
        let default_buckets_count = 1 << DEFAULT_SECOND_BUCKETS_COUNT_LOG;

        default_buckets_count
            // Avoid producing a lot of small buckets
            .min(max_second_buckets_count)
            // Avoid producing too large buckets
            .max(min_second_buckets_count)
            .next_power_of_two()
            // Global thresholds
            .min(1 << MAX_SECOND_BUCKETS_COUNT_LOG)
            .max(1 << MIN_SECOND_BUCKETS_COUNT_LOG)
    };

    // let second_buckets_count = BucketsCount::new(
    //     ExtraBuckets::None,
    // );

    Ok(FilesStatsInfo {
        best_buckets_count_log: buckets_count.ilog2() as usize,
        best_second_buckets_count_log: second_buckets_count.ilog2() as usize,
        bucket_size_compaction_threshold,
        target_chunk_size,
    })
}
