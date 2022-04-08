use crate::config::{SwapPriority, PARTIAL_VECS_CHECKPOINT_SIZE, SECOND_BUCKETS_COUNT};
use crate::utils::get_memory_mode;
use crate::utils::resource_counter::ResourceCounter;
use crate::KEEP_FILES;
use crossbeam::queue::SegQueue;
use parallel_processor::buckets::readers::generic_binary_reader::{
    ChunkDecoder, GenericChunkedBinaryReader,
};
use parallel_processor::buckets::writers::lock_free_binary_writer::LockFreeBinaryWriter;
use parallel_processor::buckets::MultiThreadBuckets;
use parallel_processor::memory_fs::RemoveFileMode;
use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering;
use std::sync::Arc;

pub struct ProcessQueueItem {
    pub path: PathBuf,
    pub can_resplit: bool,
    pub potential_outlier: bool,
    pub main_bucket_size: usize,
    pub buffers_counter: Arc<ResourceCounter>,
}

impl Drop for ProcessQueueItem {
    fn drop(&mut self) {
        self.buffers_counter
            .deallocate(1, SECOND_BUCKETS_COUNT as u64);
    }
}

pub struct BucketProcessData<FileType: ChunkDecoder> {
    pub reader: GenericChunkedBinaryReader<FileType>,
    pub buckets: MultiThreadBuckets<LockFreeBinaryWriter>,
    process_queue: Arc<SegQueue<ProcessQueueItem>>,
    buffers_counter: Arc<ResourceCounter>,
    can_resplit: bool,
    is_outlier: bool,
}

impl<FileType: ChunkDecoder> BucketProcessData<FileType> {
    pub fn new_blocking(
        path: impl AsRef<Path>,
        split_name: &str,
        process_queue: Arc<SegQueue<ProcessQueueItem>>,
        buffers_counter: Arc<ResourceCounter>,
        resplit_phase: bool,
        is_outlier: bool,
    ) -> Self {
        if resplit_phase {
            buffers_counter.allocate_overflow(SECOND_BUCKETS_COUNT as u64);
        } else {
            buffers_counter.allocate_blocking(SECOND_BUCKETS_COUNT as u64);
        }
        let tmp_dir = path.as_ref().parent().unwrap_or(Path::new("."));
        Self {
            reader: GenericChunkedBinaryReader::<FileType>::new(
                &path,
                RemoveFileMode::Remove {
                    remove_fs: !KEEP_FILES.load(Ordering::Relaxed),
                },
            ),
            buckets: MultiThreadBuckets::new(
                SECOND_BUCKETS_COUNT,
                PathBuf::from(tmp_dir).join(split_name),
                &(
                    get_memory_mode(SwapPriority::KmersMergeBuckets),
                    PARTIAL_VECS_CHECKPOINT_SIZE,
                ),
            ),
            process_queue,
            buffers_counter,
            can_resplit: !resplit_phase,
            is_outlier,
        }
    }
}

impl<FileType: ChunkDecoder> Drop for BucketProcessData<FileType> {
    fn drop(&mut self) {
        let bucket_file_size = self.reader.get_length();
        for path in self.buckets.finalize() {
            self.process_queue.push(ProcessQueueItem {
                path,
                can_resplit: self.can_resplit,
                potential_outlier: self.is_outlier,
                main_bucket_size: bucket_file_size,
                buffers_counter: self.buffers_counter.clone(),
            });
        }
    }
}
