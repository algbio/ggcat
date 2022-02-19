pub mod extra_data;
pub mod reads_reader;
pub mod reads_writer;
pub mod thread_writer;

const INTERMEDIATE_READS_MAGIC: [u8; 16] = *b"BILOKI_INTEREADS";

use desse::{Desse, DesseSized};
use serde::{Deserialize, Serialize};

#[derive(Debug, Desse, DesseSized, Default)]
struct IntermediateReadsHeader {
    magic: [u8; 16],
    index_offset: u64,
}

#[derive(Serialize, Deserialize)]
struct IntermediateReadsIndex {
    index: Vec<u64>,
}
