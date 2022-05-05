use std::path::{Path, PathBuf};
use std::sync::Arc;

pub mod bucket_writer;
pub mod concurrent;
pub mod readers;
pub mod single;
pub mod writers;

pub trait LockFreeBucket {
    type InitData;

    fn new(path: &Path, data: &Self::InitData, index: usize) -> Self;
    fn write_data(&self, bytes: &[u8]);
    fn get_path(&self) -> PathBuf;
    fn finalize(self);
}

pub struct MultiThreadBuckets<B: LockFreeBucket> {
    buckets: Vec<B>,
}

impl<B: LockFreeBucket> MultiThreadBuckets<B> {
    pub fn new(size: usize, path: PathBuf, init_data: &B::InitData) -> MultiThreadBuckets<B> {
        let mut buckets = Vec::with_capacity(size);

        for i in 0..size {
            buckets.push(B::new(&path, init_data, i));
        }
        MultiThreadBuckets { buckets }
    }

    pub fn into_buckets(mut self) -> impl Iterator<Item = B> {
        let buckets = std::mem::take(&mut self.buckets);
        buckets.into_iter()
    }

    pub fn get_path(&self, bucket: u16) -> PathBuf {
        self.buckets[bucket as usize].get_path()
    }

    pub fn add_data(&self, index: u16, data: &[u8]) {
        self.buckets[index as usize].write_data(data);
    }

    pub fn count(&self) -> usize {
        self.buckets.len()
    }

    pub fn finalize(self: Arc<Self>) -> Vec<PathBuf> {
        while Arc::strong_count(&self) > 1 {
            println!("Arc busy {}, waiting", Arc::strong_count(&self));
            std::thread::sleep_ms(1000);
        }

        let mut self_ = Arc::try_unwrap(self)
            .unwrap_or_else(|_| panic!("Cannot take full ownership of multi thread buckets!"));

        self_
            .buckets
            .drain(..)
            .map(|bucket| {
                let path = bucket.get_path();
                bucket.finalize();
                path
            })
            .collect()
    }
}

impl<B: LockFreeBucket> Drop for MultiThreadBuckets<B> {
    fn drop(&mut self) {
        self.buckets.drain(..).for_each(|bucket| {
            bucket.finalize();
        });
    }
}

unsafe impl<B: LockFreeBucket> Send for MultiThreadBuckets<B> {}

unsafe impl<B: LockFreeBucket> Sync for MultiThreadBuckets<B> {}
