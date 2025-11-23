use crate::storage::run_length::RunLengthColorsSerializer;

pub mod bundles;
pub mod colors_manager;
pub mod colors_memmap_writer;
pub mod managers;
pub mod non_colored;
pub mod parsers;
pub mod storage;

pub type DefaultColorsSerializer = RunLengthColorsSerializer;
