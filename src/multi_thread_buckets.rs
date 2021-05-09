use crate::binary_writer::BinaryWriter;
use serde::Serialize;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

pub trait BucketType {
    type InitType: ?Sized;

    fn new(init_data: &Self::InitType, index: usize) -> Self;
    fn get_path(&self) -> PathBuf;
    fn finalize(self);
}

pub struct MultiThreadBuckets<B: BucketType> {
    buckets: Vec<Mutex<B>>,
}

impl<B: BucketType> MultiThreadBuckets<B> {
    pub fn new(size: usize, init_data: &B::InitType) -> MultiThreadBuckets<B> {
        let mut buckets = Vec::new();
        buckets.reserve(size);

        for i in 0..size {
            buckets.push(Mutex::new(B::new(init_data, i)));
        }
        MultiThreadBuckets { buckets }
    }

    pub fn flush(&self, index: usize, flush_fn: impl FnOnce(&mut B)) {
        let mut bucket = self.buckets[index].lock().unwrap();
        flush_fn(&mut bucket);
    }

    pub fn finalize(self) -> Vec<PathBuf> {
        self.buckets
            .iter()
            .map(|b| b.lock().unwrap().get_path())
            .collect()
    }
}

impl<B: BucketType> Drop for MultiThreadBuckets<B> {
    fn drop(&mut self) {
        while let Some(bucket) = self.buckets.pop() {
            bucket.into_inner().unwrap().finalize();
        }
    }
}

unsafe impl<B: BucketType> Send for MultiThreadBuckets<B> {}
unsafe impl<B: BucketType> Sync for MultiThreadBuckets<B> {}

pub trait BucketWriter {
    type BucketType;
    type ExtraData;
    fn write_to(&self, bucket: &mut Self::BucketType, extra_data: &Self::ExtraData);
}

pub struct BucketsThreadDispatcher<'a, B: BucketType, T: BucketWriter<BucketType = B> + Clone> {
    mtb: &'a MultiThreadBuckets<B>,
    thread_data: Vec<Vec<T>>,
    max_bucket_size: usize,
}

impl<'a, B: BucketType, T: BucketWriter<BucketType = B> + Clone> BucketsThreadDispatcher<'a, B, T> {
    pub fn new(
        max_bucket_size: usize,
        mtb: &'a MultiThreadBuckets<B>,
    ) -> BucketsThreadDispatcher<'a, B, T> {
        let mut thread_data = Vec::new();
        thread_data.resize(mtb.buckets.len(), Vec::with_capacity(max_bucket_size));

        Self {
            mtb,
            thread_data,
            max_bucket_size,
        }
    }
    #[inline]
    pub fn add_element(&mut self, bucket: usize, extra_data: &T::ExtraData, element: T) {
        self.thread_data[bucket].push(element);
        if self.thread_data[bucket].len() >= self.max_bucket_size {
            self.mtb.flush(bucket, |mtb_bucket| {
                for el in self.thread_data[bucket].iter() {
                    el.write_to(mtb_bucket, extra_data);
                }
                self.thread_data[bucket].clear();
            })
        }
    }

    pub fn finalize(mut self, extra_data: &T::ExtraData) {
        for (index, vec) in self.thread_data.iter_mut().enumerate() {
            if vec.len() == 0 {
                continue;
            }
            self.mtb.flush(index, |mtb_bucket| {
                for el in vec.iter() {
                    el.write_to(mtb_bucket, extra_data);
                }
            })
        }
        self.thread_data.clear();
    }
}

impl<'a, B: BucketType, T: BucketWriter<BucketType = B> + Clone> Drop
    for BucketsThreadDispatcher<'a, B, T>
{
    fn drop(&mut self) {
        assert_eq!(self.thread_data.len(), 0);
    }
}
