#![feature(generic_associated_types)]
#![feature(thread_local)]
#![feature(new_uninit)]
#![feature(slice_partition_dedup)]

use crate::storage::run_length::RunLengthColorsSerializer;

pub mod bundles;
pub mod colors_manager;
pub mod colors_memmap;
pub mod managers;
pub mod non_colored;
pub mod parsers;
pub mod storage;

pub(crate) mod async_slice_queue;

pub type DefaultColorsSerializer = RunLengthColorsSerializer;
