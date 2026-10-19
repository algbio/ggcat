use std::path::Path;

use crate::sequences_stream::general::GeneralSequenceBlockData;
use config::{
    DEFAULT_BUCKETS_CHUNK_SIZE, DEFAULT_BUCKETS_COUNT_LOG, DEFAULT_PER_CPU_BUFFER_SIZE,
    DEFAULT_SECOND_BUCKETS_COUNT_LOG, DEFAULT_TARGET_CHUNK_SIZE_MULTIPLIER, MAX_BUCKET_SIZE,
    MAX_BUCKETS_CHUNK_SIZE, MAX_BUCKETS_COUNT_LOG, MAX_COMPACTION_ITERATIONS,
    MAX_SECOND_BUCKET_SIZE, MAX_SECOND_BUCKETS_COUNT_LOG, MAX_TARGET_CHUNK_SIZE, MIN_BUCKET_SIZE,
    MIN_BUCKETS_CHUNK_SIZE, MIN_BUCKETS_COUNT_LOG, MIN_SECOND_BUCKET_SIZE,
    MIN_SECOND_BUCKETS_COUNT_LOG, MIN_TARGET_CHUNK_SIZE,
};
use parallel_processor::{
    DEFAULT_BINCODE_CONFIG,
    buckets::{ExtraBucketData, MultiChunkBucket, SingleBucket},
};

pub mod binary_reader;
pub mod compressed_read;
pub mod concurrent;
pub mod concurrent_filewriter;
pub mod gzip_wrapper;
pub mod ident_writer;
pub mod input_size;
pub mod lines_reader;
pub mod memstorage;
pub mod partial_unitigs_extra_data;
pub mod sequences_reader;
pub mod sequences_splitter;
pub mod sequences_stream;
pub mod simd;
pub mod structs;
pub mod varint;

pub const DUPLICATES_BUCKET_EXTRA: ExtraBucketData = ExtraBucketData(0xdddd);

pub fn debug_save_single_buckets(temp_dir: &Path, file: &str, buckets: &[SingleBucket]) {
    std::fs::write(
        temp_dir.join(file),
        bincode::encode_to_vec(buckets, DEFAULT_BINCODE_CONFIG).unwrap(),
    )
    .unwrap();
}

pub fn debug_load_single_buckets(temp_dir: &Path, file: &str) -> anyhow::Result<Vec<SingleBucket>> {
    let data = std::fs::read(temp_dir.join(file))?;
    let buckets: Vec<SingleBucket> = bincode::decode_from_slice(&data, DEFAULT_BINCODE_CONFIG)?.0;
    Ok(buckets)
}

pub fn debug_save_buckets(temp_dir: &Path, file: &str, buckets: &[MultiChunkBucket]) {
    std::fs::write(
        temp_dir.join(file),
        bincode::encode_to_vec(buckets, DEFAULT_BINCODE_CONFIG).unwrap(),
    )
    .unwrap();
}

pub fn debug_load_buckets(temp_dir: &Path, file: &str) -> anyhow::Result<Vec<MultiChunkBucket>> {
    let data = std::fs::read(temp_dir.join(file))?;
    let buckets: Vec<MultiChunkBucket> =
        bincode::decode_from_slice(&data, DEFAULT_BINCODE_CONFIG)?.0;
    Ok(buckets)
}

/// Version of the inputs checkpoint, the first field of the file.
const INPUTS_CHECKPOINT_VERSION: u32 = 2;

/// Saves what the minimizer bucketing was planned with and gave: the colors, as the caller
/// encodes them, and the statistics that chose the buckets. A run resuming past it reads them
/// back with [`debug_load_inputs`], so the buckets are read with the counts and the colors they
/// were written with, without reading the inputs again.
pub fn debug_save_inputs(
    temp_dir: &Path,
    file: &str,
    colors: &impl bincode::Encode,
    stats: &FilesStatsInfo,
) -> anyhow::Result<()> {
    std::fs::write(
        temp_dir.join(file),
        bincode::encode_to_vec(
            (INPUTS_CHECKPOINT_VERSION, colors, stats),
            DEFAULT_BINCODE_CONFIG,
        )?,
    )?;
    Ok(())
}

/// The colors and the statistics [`debug_save_inputs`] saved, the colors decoded as `C`.
pub fn debug_load_inputs<C: bincode::Decode<()>>(
    temp_dir: &Path,
    file: &str,
) -> anyhow::Result<(C, FilesStatsInfo)> {
    let data = std::fs::read(temp_dir.join(file))?;
    let (version, _): (u32, _) = bincode::decode_from_slice(&data, DEFAULT_BINCODE_CONFIG)?;
    anyhow::ensure!(
        version == INPUTS_CHECKPOINT_VERSION,
        "Unsupported input checkpoint version {version}"
    );
    let ((_, colors, stats), _): ((u32, C, FilesStatsInfo), _) =
        bincode::decode_from_slice(&data, DEFAULT_BINCODE_CONFIG)?;
    Ok((colors, stats))
}

#[derive(bincode::Encode, bincode::Decode)]
pub struct FilesStatsInfo {
    pub best_buckets_count_log: usize,
    pub best_second_buckets_count_log: usize,
    pub bucket_size_compaction_threshold: u64,
    pub target_chunk_size: u64,
}

pub fn compute_stats_from_input_blocks(
    blocks: &[GeneralSequenceBlockData],
) -> anyhow::Result<FilesStatsInfo> {
    Ok(compute_stats_from_input_sizes(
        &input_size::estimate_blocks(blocks)?,
    ))
}

/// Reuse the same estimates for sizing and scheduling, without reopening inputs.
pub fn compute_stats_from_input_sizes(sizes: &[u64]) -> FilesStatsInfo {
    let bases_count = sizes
        .iter()
        .fold(0u64, |total, size| total.saturating_add(*size));

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

    FilesStatsInfo {
        best_buckets_count_log: buckets_count.ilog2() as usize,
        best_second_buckets_count_log: second_buckets_count.ilog2() as usize,
        bucket_size_compaction_threshold,
        target_chunk_size,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn inputs_checkpoint_round_trips_and_checks_its_version() {
        let directory =
            std::env::temp_dir().join(format!("ggcat-inputs-checkpoint-{}", std::process::id()));
        std::fs::create_dir_all(&directory).unwrap();
        let names = vec!["a.fa".to_string(), "b.tar.xz".to_string()];
        let stats = FilesStatsInfo {
            best_buckets_count_log: 10,
            best_second_buckets_count_log: 6,
            bucket_size_compaction_threshold: 2 << 20,
            target_chunk_size: 64 << 20,
        };
        debug_save_inputs(&directory, "inputs.debug", &names, &stats).unwrap();
        let (loaded_names, loaded) =
            debug_load_inputs::<Vec<String>>(&directory, "inputs.debug").unwrap();
        assert_eq!(loaded_names, names);
        assert_eq!(
            (
                loaded.best_buckets_count_log,
                loaded.best_second_buckets_count_log,
                loaded.bucket_size_compaction_threshold,
                loaded.target_chunk_size
            ),
            (10, 6, 2 << 20, 64 << 20)
        );

        let other_version = bincode::encode_to_vec(
            (INPUTS_CHECKPOINT_VERSION + 1, &names, &stats),
            DEFAULT_BINCODE_CONFIG,
        )
        .unwrap();
        std::fs::write(directory.join("inputs.debug"), other_version).unwrap();
        assert!(debug_load_inputs::<Vec<String>>(&directory, "inputs.debug").is_err());
        std::fs::remove_dir_all(directory).unwrap();
    }
}
