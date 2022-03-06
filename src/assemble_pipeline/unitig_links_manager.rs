use crate::config::BucketIndexType;
use std::sync::atomic::{AtomicUsize, Ordering};

struct Counters {
    index: AtomicUsize,
}

pub struct UnitigLinksManager {
    links_data: Vec<Counters>,
    prefix_indexes: Vec<usize>,
    final_unitig_indexes_offset: usize,
}

impl UnitigLinksManager {
    pub fn new(buckets_count: usize) -> Self {
        UnitigLinksManager {
            links_data: (0..(buckets_count + 1))
                .map(|_| Counters {
                    index: AtomicUsize::new(0),
                })
                .collect(),
            prefix_indexes: vec![0; buckets_count + 2],
            final_unitig_indexes_offset: 0,
        }
    }

    pub fn compute_id_offsets(&mut self) {
        // Build the final ids offsets
        for i in 0..self.links_data.len() {
            self.prefix_indexes[i + 1] =
                self.prefix_indexes[i] + self.links_data[i].index.load(Ordering::Relaxed);
        }

        // Set the final unitigs offset
        self.final_unitig_indexes_offset = self.prefix_indexes[self.links_data.len() - 1];
    }

    #[inline(always)]
    #[track_caller]
    pub fn get_unitig_index(&self, bucket: BucketIndexType, index: usize) -> usize {
        self.prefix_indexes[bucket as usize] + index
    }

    #[inline(always)]
    #[track_caller]
    pub fn get_final_unitig_index(&self, index: usize) -> usize {
        self.final_unitig_indexes_offset + index
    }
}

pub struct ThreadUnitigsLinkManager<'a> {
    links_manager: &'a UnitigLinksManager,
    bucket_index: BucketIndexType,
    counter: usize,
}

impl<'a> ThreadUnitigsLinkManager<'a> {
    pub fn new(links_manager: &'a UnitigLinksManager, bucket: BucketIndexType) -> Self {
        Self {
            links_manager,
            bucket_index: bucket,
            counter: links_manager.links_data[bucket as usize]
                .index
                .load(Ordering::Relaxed),
        }
    }

    #[inline(always)]
    pub fn notify_add_read(&mut self) {
        self.counter += 1;
    }
}

impl<'a> Drop for ThreadUnitigsLinkManager<'a> {
    fn drop(&mut self) {
        self.links_manager.links_data[self.bucket_index as usize]
            .index
            .store(self.counter, Ordering::Relaxed);
    }
}
