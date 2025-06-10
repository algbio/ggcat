use crate::sequences_stream::general::GeneralSequenceBlockData;
use config::{MAX_BUCKET_SIZE, MAX_BUCKETS_COUNT_LOG, MIN_BUCKETS_COUNT_LOG};
use parallel_processor::buckets::{BucketsCount, ExtraBucketData, ExtraBuckets, SingleBucket};
use std::cmp::{max, min};
use std::path::Path;

pub mod chunks_writer;
pub mod compressed_read;
pub mod concurrent;
pub mod lines_reader;
// pub mod reads_writer;
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
    // pub best_lz4_compression_level: u32,
}

pub fn compute_stats_from_input_blocks(
    blocks: &[GeneralSequenceBlockData],
) -> anyhow::Result<FilesStatsInfo> {
    let mut bases_count = 0;
    for block in blocks {
        bases_count += block.estimated_bases_count()?;
    }

    let buckets_count = bases_count / MAX_BUCKET_SIZE;

    let buckets_log = (max(1, buckets_count) - 1).next_power_of_two().ilog2() as usize;

    Ok(FilesStatsInfo {
        best_buckets_count_log: min(
            MAX_BUCKETS_COUNT_LOG,
            max(MIN_BUCKETS_COUNT_LOG, buckets_log),
        ),
        // best_lz4_compression_level: 0,
    })
}
