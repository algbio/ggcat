pub mod async_slice_queue;
pub mod compressed_read;
pub mod debug_functions;
pub mod debug_utils;
pub mod fast_rand_bool;
pub mod vec_slice;

use crate::config::BucketIndexType;
use crate::PREFER_MEMORY;
use parallel_processor::memory_fs::file::internal::MemoryFileMode;
use std::cmp::max;
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
    if k < 27 {
        max(2, (k + 3) / 2)
    } else {
        ((k + 2) as f64 / 3.0).round() as usize
    }
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
}
