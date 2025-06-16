use crate::pipeline::maximal_unitig_links::maximal_unitig_index::{
    DoubleMaximalUnitigLinks, MaximalUnitigIndex, MaximalUnitigLink,
};
use config::{DEFAULT_PREFETCH_AMOUNT, KEEP_FILES};
use parallel_processor::buckets::SingleBucket;
use parallel_processor::buckets::readers::binary_reader::ChunkedBinaryReaderIndex;
use parallel_processor::buckets::readers::typed_binary_reader::TypedStreamReader;
use parallel_processor::memory_fs::RemoveFileMode;
use parking_lot::{Mutex, RwLock};
use std::cmp::min;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use utils::vec_slice::VecSlice;

use super::maximal_unitig_index::MaximalUnitigLinkSerializer;

pub struct MaximalUnitigLinksMapping {
    start_index: u64,
    mappings: Vec<DoubleMaximalUnitigLinks>,
    mappings_data: Vec<MaximalUnitigIndex>,
}

impl MaximalUnitigLinksMapping {
    pub fn empty() -> Self {
        Self {
            start_index: 0,
            mappings: Vec::new(),
            mappings_data: Vec::new(),
        }
    }

    fn load_from_bucket(bucket: &Path, start_index: u64, unitigs_per_bucket: usize) -> Self {
        let mut self_ = Self {
            start_index,
            mappings: vec![
                DoubleMaximalUnitigLinks {
                    links: [
                        MaximalUnitigLink::new(0, VecSlice::EMPTY),
                        MaximalUnitigLink::new(0, VecSlice::EMPTY)
                    ],
                    is_self_complemental: false
                };
                unitigs_per_bucket
            ],
            mappings_data: vec![],
        };

        let file_index = ChunkedBinaryReaderIndex::from_file(
            bucket,
            RemoveFileMode::Remove {
                remove_fs: !KEEP_FILES.load(Ordering::Relaxed),
            },
            DEFAULT_PREFETCH_AMOUNT,
        );

        let mappings_data = TypedStreamReader::get_items::<MaximalUnitigLinkSerializer>(
            None,
            (),
            file_index.into_chunks(),
            |(mappings_data, item), _| {
                let index = item.index() - self_.start_index;

                let current_slice = item.entries.get_slice(&mappings_data);

                let forward_index = if current_slice[0].flags.flip_current() {
                    1
                } else {
                    0
                };

                self_.mappings[index as usize].links[forward_index] = item;
            },
        )
        .buffer;

        self_.mappings_data = mappings_data;

        self_
    }

    pub(crate) fn has_mapping(&self, index: u64) -> bool {
        (index - self.start_index) < self.mappings.len() as u64
    }

    pub(crate) fn get_mapping(
        &self,
        index: u64,
    ) -> (DoubleMaximalUnitigLinks, &Vec<MaximalUnitigIndex>) {
        (
            self.mappings[(index - self.start_index) as usize].clone(),
            &self.mappings_data,
        )
    }
}

pub struct MaximalUnitigLinksMappingsLoader {
    buckets: Vec<SingleBucket>,
    unitigs_per_bucket: usize,

    minimum_buckets: Vec<AtomicUsize>,

    next_disposed_bucket_index: RwLock<usize>,
    loaded_buckets: Vec<Mutex<Option<Arc<MaximalUnitigLinksMapping>>>>,
}

impl MaximalUnitigLinksMappingsLoader {
    pub fn new(
        buckets: Vec<SingleBucket>,
        unitigs_per_bucket: usize,
        threads_count: usize,
    ) -> Self {
        let buckets_count = buckets.len();

        Self {
            buckets,
            unitigs_per_bucket,
            minimum_buckets: (0..threads_count).map(|_| AtomicUsize::new(0)).collect(),
            next_disposed_bucket_index: RwLock::new(0),
            loaded_buckets: (0..buckets_count).map(|_| Mutex::new(None)).collect(),
        }
    }

    fn dispose_buckets(&self) {
        let minimum_bucket = self
            .minimum_buckets
            .iter()
            .map(|b| b.load(Ordering::Relaxed))
            .min()
            .unwrap();

        if *self.next_disposed_bucket_index.read() < minimum_bucket {
            let mut next_disposed_bucket_index = self.next_disposed_bucket_index.write();
            while *next_disposed_bucket_index < min(minimum_bucket, self.loaded_buckets.len()) {
                self.loaded_buckets[*next_disposed_bucket_index]
                    .lock()
                    .take();
                // ggcat_logging::info!("Disposing bucket {}", *next_disposed_bucket_index);
                *next_disposed_bucket_index += 1;
            }
        }
    }

    pub fn get_mapping_for(
        &self,
        index: u64,
        thread_index: usize,
    ) -> Arc<MaximalUnitigLinksMapping> {
        let bucket_index = (index / self.unitigs_per_bucket as u64) as usize;

        self.minimum_buckets[thread_index].store(bucket_index, Ordering::Relaxed);

        self.dispose_buckets();

        let mut bucket_guard = self.loaded_buckets[bucket_index].lock();

        if let Some(bucket) = bucket_guard.as_ref() {
            bucket.clone()
        } else {
            let bucket = Arc::new(MaximalUnitigLinksMapping::load_from_bucket(
                &self.buckets[bucket_index].path,
                bucket_index as u64 * self.unitigs_per_bucket as u64,
                self.unitigs_per_bucket,
            ));
            *bucket_guard = Some(bucket.clone());
            bucket
        }
    }

    pub fn notify_thread_ending(&self, thread_index: usize) {
        self.minimum_buckets[thread_index].store(usize::MAX, Ordering::Relaxed);
        self.dispose_buckets();
    }
}
