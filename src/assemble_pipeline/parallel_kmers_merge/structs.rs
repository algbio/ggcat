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

#[derive(Copy, Clone, Ord, PartialOrd, Eq, PartialEq, Debug)]
pub struct ReadRef {
    pub read_start: *const u8,
    pub hash: u64,
}

unsafe impl Sync for ReadRef {}
unsafe impl Send for ReadRef {}

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

pub struct BucketProcessData<CX: ColorsManager> {
    pub reader: IntermediateReadsReader<KmersFlags<CX::MinimizerBucketingSeqColorDataType>>,
    pub buckets: MultiThreadBuckets<AsyncVec<ReadRef>>,
    pub vec_refs: Arc<Mutex<Vec<Vec<ChunkedVector<u8>>>>>,
}

impl<CX: ColorsManager> BucketProcessData<CX> {
    pub fn new(
        path: impl AsRef<Path>,
        pool: FlexiblePool<AsyncVec<ReadRef>>,
        vecs_queue: Arc<SegQueue<(AsyncVec<ReadRef>, Vec<ChunkedVector<u8>>)>>,
    ) -> Self {
        let vec_refs = {
            let mut vec = Vec::new();
            vec.resize_with(MERGE_BUCKETS_COUNT, || Vec::new());
            Arc::new(Mutex::new(vec))
        };

        let vec_refs_clone = vec_refs.clone();
        let on_drop: Box<dyn Fn(AsyncVec<ReadRef>)> = Box::new(move |x: AsyncVec<ReadRef>| {
            let index = x.get_index();
            vecs_queue.push((x, std::mem::take(&mut vec_refs_clone.lock()[index])));
        });

        Self {
            reader:
                IntermediateReadsReader::<KmersFlags<CX::MinimizerBucketingSeqColorDataType>>::new(
                    path,
                    !KEEP_FILES.load(Ordering::Relaxed),
                ),
            buckets: MultiThreadBuckets::new(MERGE_BUCKETS_COUNT, &(pool, Arc::new(on_drop)), None),
            vec_refs,
        }
    }

    pub fn add_chunks_refs(&self, chunks: &mut Vec<ChunkedVector<u8>>) {
        let mut vec_refs = self.vec_refs.lock();
        for (idx, chunk) in chunks.drain(..).enumerate() {
            vec_refs[idx].push(chunk);
        }
    }
}

pub struct RetType {
    pub sequences: Vec<PathBuf>,
    pub hashes: Vec<PathBuf>,
}
