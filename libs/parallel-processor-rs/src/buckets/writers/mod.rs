use crate::memory_fs::file::writer::FileWriter;
use desse::{Desse, DesseSized};
use mt_debug_counters::counter::{AtomicCounter, SumMode};
use serde::{Deserialize, Serialize};
use std::io::Write;

pub mod compressed_binary_writer;
pub mod lock_free_binary_writer;

pub(crate) static THREADS_BUSY_WRITING: AtomicCounter<SumMode> =
    declare_counter_i64!("threads_busy_writing", SumMode, false);

#[derive(Debug, Desse, DesseSized, Default)]
pub(crate) struct BucketHeader {
    pub magic: [u8; 16],
    pub index_offset: u64,
}

#[derive(Serialize, Deserialize)]
pub(crate) struct BucketCheckpoints {
    pub index: Vec<u64>,
}

pub(crate) fn initialize_bucket_file(file: &mut FileWriter) -> u64 {
    // Write empty header
    file.write_all(&BucketHeader::default().serialize()[..])
        .unwrap();

    file.len() as u64
}

pub(crate) fn finalize_bucket_file(mut file: FileWriter, magic: &[u8; 16], checkpoints: Vec<u64>) {
    file.flush().unwrap();
    let index_position = file.len() as u64;
    bincode::serialize_into(&mut file, &BucketCheckpoints { index: checkpoints }).unwrap();

    file.write_at_start(
        &BucketHeader {
            magic: *magic,
            index_offset: index_position,
        }
        .serialize()[..],
    )
    .unwrap();
    file.flush_async();
}
