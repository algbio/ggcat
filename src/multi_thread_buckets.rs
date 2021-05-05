use std::sync::Mutex;

pub trait BucketType {
    type InitType: ?Sized;

    fn new(init_data: &Self::InitType, index: usize) -> Self;
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

    pub fn finalize(self) {}
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
