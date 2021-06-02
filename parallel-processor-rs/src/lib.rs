#![feature(trait_alias)]
#![feature(is_sorted, specialization)]

use core::cmp;

#[macro_use]
pub mod stats_logger;
pub mod binary_writer;
pub mod lock_free_binary_writer;
pub mod masked_smart_bucket_sort;
pub mod memory_fs;
pub mod multi_thread_buckets;
pub mod phase_times_monitor;
pub mod semaphore;
pub mod smart_bucket_sort;
pub mod threadpools_chain;
pub mod types;

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}

pub struct Utils {}

impl Utils {
    pub fn multiply_by(val: usize, mult: f64) -> usize {
        ((val as f64) * mult) as usize
    }

    // Taken from https://github.com/banyan/rust-pretty-bytes/blob/master/src/converter.rs
    pub fn convert_human(num: f64) -> String {
        let negative = if num.is_sign_positive() { "" } else { "-" };
        let num = num.abs();
        let units = ["B", "kB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB"];
        if num < 1_f64 {
            return format!("{}{} {}", negative, num, "B");
        }
        let delimiter = 1000_f64;
        let exponent = cmp::min(
            (num.ln() / delimiter.ln()).floor() as i32,
            (units.len() - 1) as i32,
        );
        let pretty_bytes = format!("{:.2}", num / delimiter.powi(exponent))
            .parse::<f64>()
            .unwrap()
            * 1_f64;
        let unit = units[exponent as usize];
        format!("{}{} {}", negative, pretty_bytes, unit)
    }
}
