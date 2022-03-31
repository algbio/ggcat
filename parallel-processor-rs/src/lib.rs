#![feature(is_sorted, generic_associated_types, int_log)]
// #![deny(warnings)]

#[macro_use]
pub extern crate counter_stats;

pub use counter_stats::logging::enable_counters_logging;

#[macro_use]
pub mod memory_fs;
pub mod buckets;
pub mod debug_allocator;
pub mod fast_smart_bucket_sort;
pub mod mem_tracker;
pub mod memory_data_size;
pub mod phase_times_monitor;
pub mod threadpools_chain;
pub mod utils;

pub struct Utils {}

impl Utils {
    pub fn multiply_by(val: usize, mult: f64) -> usize {
        ((val as f64) * mult) as usize
    }
}
