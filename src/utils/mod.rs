pub mod async_slice_queue;
pub mod compressed_read;
pub mod debug_functions;
pub mod debug_utils;
pub mod fast_rand_bool;
pub mod owned_drop;
pub mod resource_counter;
pub mod vec_slice;

use crate::config::{
    BucketIndexType, MAX_BUCKETS_COUNT_LOG, MAX_BUCKET_SIZE, MIN_BUCKETS_COUNT_LOG,
};
use crate::PREFER_MEMORY;
use parallel_processor::memory_fs::file::internal::MemoryFileMode;
use std::cmp::{max, min};
use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering;

pub struct Utils;

const C_INV_LETTERS: [u8; 4] = [b'A', b'C', b'T', b'G'];

#[macro_export]
macro_rules! panic_debug {
    ($($arg:tt)*) => {
        #[cfg(feature = "debug")]
        panic!($($arg)*);
        #[cfg(not(feature = "debug"))]
        unsafe { std::hint::unreachable_unchecked() }
    };
}

pub fn get_memory_mode(swap_priority: usize) -> MemoryFileMode {
    if PREFER_MEMORY.load(Ordering::Relaxed) {
        MemoryFileMode::PreferMemory { swap_priority }
    } else {
        MemoryFileMode::DiskOnly
    }
}

pub fn compute_best_m(k: usize) -> usize {
    min(k - 1, max(2, ((k as f64).log(2.0) * 2.0).ceil() as usize))
}

impl Utils {
    #[inline(always)]
    pub fn compress_base(base: u8) -> u8 {
        (base >> 1) & 0x3
    }

    #[inline(always)]
    pub fn decompress_base(cbase: u8) -> u8 {
        C_INV_LETTERS[cbase as usize]
    }

    #[inline(always)]
    pub fn conditional_rc_base(cbase: u8, do_rc: bool) -> u8 {
        cbase ^ if do_rc { 2 } else { 0 }
    }

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

        let buckets_log = (max(1, buckets_count) - 1).next_power_of_two().log2() as usize;

        min(
            MAX_BUCKETS_COUNT_LOG,
            max(MIN_BUCKETS_COUNT_LOG, buckets_log),
        )
    }
}

#[macro_export]
macro_rules! make_comparer {
    ($Name:ident, $type_name:ty, $key:ident: $key_type:ty) => {
        struct $Name;
        impl SortKey<$type_name> for $Name {
            type KeyType = $key_type;
            const KEY_BITS: usize = std::mem::size_of::<$key_type>() * 8;

            fn compare(left: &$type_name, right: &$type_name) -> std::cmp::Ordering {
                left.$key.cmp(&right.$key)
            }

            fn get_shifted(value: &$type_name, rhs: u8) -> u8 {
                (value.$key >> rhs) as u8
            }
        }
    };
}
