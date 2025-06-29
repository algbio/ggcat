use std::{iter::repeat_with, path::Path};

use config::BucketIndexType;
use io::concurrent::temp_reads::creads_utils::ReadsCheckpointData;
use parallel_processor::{
    buckets::readers::binary_reader::{BinaryReaderChunk, ChunkedBinaryReaderIndex},
    memory_fs::RemoveFileMode,
};

use crate::MinimizerBucketMode;

pub struct SplittedBucket {
    pub single_chunks: Vec<BinaryReaderChunk>,
    pub multi_chunks: Vec<BinaryReaderChunk>,
    pub sequences_count: u64,
    pub total_size: u64,
    pub sub_bucket: BucketIndexType,
}

impl SplittedBucket {
    pub fn from_multi_chunks(
        paths: impl Iterator<Item = impl AsRef<Path>>,
        remove_files: RemoveFileMode,
        prefetch_amount: Option<usize>,
        sequences_count: u64,
    ) -> Self {
        let mut total_size = 0;

        let multi_chunks = paths
            .map(|path| {
                let file_index =
                    ChunkedBinaryReaderIndex::from_file(path, remove_files, prefetch_amount);
                total_size += file_index.get_file_size();
                file_index.into_chunks()
            })
            .flatten()
            .collect();

        Self {
            single_chunks: vec![],
            multi_chunks,
            sequences_count,
            total_size,
            sub_bucket: 0,
        }
    }

    pub fn generate(
        paths: impl Iterator<Item = impl AsRef<Path>>,
        remove_files: RemoveFileMode,
        prefetch_amount: Option<usize>,
        sub_buckets_count: usize,
    ) -> Vec<Option<Self>> {
        let mut bucket_chunks = vec![];

        for file in paths {
            let file_index =
                ChunkedBinaryReaderIndex::from_file(file, remove_files, prefetch_amount);

            let data_format = file_index.get_data_format_info::<MinimizerBucketMode>();

            bucket_chunks.extend(
                file_index
                    .into_chunks()
                    .into_iter()
                    // The first element is empty and has no extra data, skip it
                    .filter_map(|c| {
                        Some((
                            (
                                c.get_extra_data::<ReadsCheckpointData>()?,
                                data_format,
                                c.get_unique_file_id(),
                            ),
                            c,
                        ))
                    }),
            );
        }

        bucket_chunks.sort_by_key(|c| c.0.0.target_subbucket);

        let mut sub_buckets: Vec<(
            Vec<(ReadsCheckpointData, MinimizerBucketMode, usize)>,
            Vec<BinaryReaderChunk>,
        )> = vec![];
        for chunk in bucket_chunks {
            if let Some(last) = sub_buckets.last_mut() {
                if last.0[last.0.len() - 1].0.target_subbucket == chunk.0.0.target_subbucket {
                    last.0.push(chunk.0);
                    last.1.push(chunk.1);
                } else {
                    sub_buckets.push((vec![chunk.0], vec![chunk.1]));
                }
            } else {
                sub_buckets.push((vec![chunk.0], vec![chunk.1]));
            }
        }

        let mut split_buckets: Vec<_> = repeat_with(|| None).take(sub_buckets_count).collect();

        for (mut info, chunks) in sub_buckets {
            let mut single_chunks = vec![];
            let mut multi_chunks = vec![];

            for (chunk, info) in chunks.into_iter().zip(info.iter()) {
                match info.1 {
                    MinimizerBucketMode::Single => unreachable!(),
                    MinimizerBucketMode::SingleGrouped => single_chunks.push(chunk),
                    MinimizerBucketMode::Compacted => multi_chunks.push(chunk),
                }
            }

            // Dedup headers that refer to the same bucket/sub-bucket
            info.dedup_by(|a, b| a == b);

            let sequences_count = info.iter().map(|i| i.0.sequences_count as u64).sum::<u64>();

            let total_size = multi_chunks.iter().map(|c| c.get_length()).sum::<u64>()
                + single_chunks.iter().map(|c| c.get_length()).sum::<u64>();

            let sub_bucket = info[0].0.target_subbucket;

            assert!(split_buckets[sub_bucket as usize].is_none());
            split_buckets[sub_bucket as usize] = Some(SplittedBucket {
                single_chunks,
                multi_chunks,
                sequences_count,
                total_size,
                sub_bucket,
            });
        }

        split_buckets
    }
}
