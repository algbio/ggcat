use crate::assemble_pipeline::current_kmers_merge::{KmersFlags, MERGE_BUCKETS_COUNT};
use crate::colors::colors_manager::ColorsManager;
use crate::io::concurrent::intermediate_storage::{IntermediateReadsReader, SequenceExtraData};
use crate::io::concurrent::intermediate_storage_single::IntermediateSequencesStorageSingleBucket;
use crate::types::BucketIndexType;
use crate::utils::async_vec::AsyncVec;
use crate::utils::chunked_vector::ChunkedVector;
use crate::utils::flexible_pool::FlexiblePool;
use crate::KEEP_FILES;
use crossbeam::queue::SegQueue;
use parallel_processor::multi_thread_buckets::MultiThreadBuckets;
use parking_lot::Mutex;
use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering;
use std::sync::Arc;

pub struct MapEntry<CHI> {
    pub count: usize,
    pub ignored: bool,
    pub color_index: CHI,
}

pub struct ResultsBucket<'a, X: SequenceExtraData> {
    pub read_index: u64,
    pub bucket_ref: IntermediateSequencesStorageSingleBucket<'a, X>,
}

impl<'a, X: SequenceExtraData> ResultsBucket<'a, X> {
    pub fn add_read(&mut self, el: X, read: &[u8]) -> u64 {
        self.bucket_ref.add_read(el, read);
        let read_index = self.read_index;
        self.read_index += 1;
        read_index
    }

    pub fn get_bucket_index(&self) -> BucketIndexType {
        self.bucket_ref.get_bucket_index()
    }
}

pub struct RetType {
    pub sequences: Vec<PathBuf>,
    pub hashes: Vec<PathBuf>,
}
