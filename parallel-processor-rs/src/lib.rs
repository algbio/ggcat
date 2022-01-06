#![feature(trait_alias)]
#![feature(is_sorted, specialization)]
#![feature(const_fn_trait_bound)]
#![feature(associated_type_defaults)]
#![feature(new_uninit)]
#![feature(drain_filter)]
#![feature(const_fn_floating_point_arithmetic)]

#[macro_use]
pub mod stats_logger;
#[macro_use]
pub mod memory_fs;

pub mod binary_writer;
pub mod fast_smart_bucket_sort;
pub mod lock_free_binary_writer;
pub mod mem_tracker;
pub mod memory_data_size;
pub mod multi_thread_buckets;
pub mod phase_times_monitor;
pub mod threadpools_chain;

pub struct Utils {}

impl Utils {
    pub fn multiply_by(val: usize, mult: f64) -> usize {
        ((val as f64) * mult) as usize
    }
}
