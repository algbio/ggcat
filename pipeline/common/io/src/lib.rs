#![feature(int_log, let_chains)]

use config::{BucketIndexType, MAX_BUCKETS_COUNT_LOG, MAX_BUCKET_SIZE, MIN_BUCKETS_COUNT_LOG};
use std::cmp::{max, min};
use std::path::{Path, PathBuf};

pub mod chunks_writer;
pub mod compressed_read;
pub mod concurrent;
pub mod lines_reader;
pub mod reads_writer;
pub mod sequences_reader;
pub mod structs;
pub mod varint;

pub fn get_bucket_index(bucket_file: impl AsRef<Path>) -> BucketIndexType {
    let mut file_path = bucket_file.as_ref().to_path_buf();

    while let Some(extension) = file_path.extension() {
        if extension != "lz4" {
            if let Some(extension) = extension.to_str() {
                match extension.parse() {
                    Ok(bucket_index) => return bucket_index,
                    Err(_) => {}
                };
            }
        }
        file_path = file_path.with_extension("");
    }
    panic!(
        "Cannot find bucket index for file {:?}",
        bucket_file.as_ref()
    );
}

pub fn generate_bucket_names(
    root: impl AsRef<Path>,
    count: usize,
    suffix: Option<&str>,
) -> Vec<PathBuf> {
    (0..count)
        .map(|i| {
            root.as_ref().with_extension(format!(
                "{}{}",
                i,
                match suffix {
                    None => String::from(""),
                    Some(s) => format!(".{}", s),
                }
            ))
        })
        .collect()
}

pub fn compute_buckets_log_from_input_files(files: &[PathBuf]) -> usize {
    // TODO: Improve this ratio estimation
    const COMPRESSED_READS_RATIO: f64 = 0.5;

    let mut bases_count = 0;

    for file in files {
        let length = std::fs::metadata(file)
            .expect(&format!("Error while opening file {}", file.display()))
            .len();

        let file_bases_count = if file
            .extension()
            .map(|x| x == "gz" || x == "lz4")
            .unwrap_or(false)
        {
            (length as f64 * COMPRESSED_READS_RATIO) as u64
        } else {
            length
        };
        bases_count += file_bases_count;
    }

    let buckets_count = bases_count / MAX_BUCKET_SIZE;

    let buckets_log = (max(1, buckets_count) - 1).next_power_of_two().ilog2() as usize;

    min(
        MAX_BUCKETS_COUNT_LOG,
        max(MIN_BUCKETS_COUNT_LOG, buckets_log),
    )
}
