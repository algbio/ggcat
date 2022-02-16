// #![deny(warnings)]

#[macro_use]
pub mod stats_logger;
#[macro_use]
pub mod memory_fs;

pub mod binary_writer;
pub mod debug_allocator;
pub mod fast_smart_bucket_sort;
pub mod lock_free_binary_writer;
pub mod mem_tracker;
pub mod memory_data_size;
pub mod phase_times_monitor;
pub mod threadpools_chain;
pub mod buckets;

pub struct Utils {}

impl Utils {
    pub fn multiply_by(val: usize, mult: f64) -> usize {
        ((val as f64) * mult) as usize
    }
}
