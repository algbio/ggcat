use crate::types::BucketIndexType;
use parking_lot::RwLock;
use rand::{thread_rng, RngCore};
use rayon::iter::ParallelIterator;

use crate::memory_data_size::*;
use crate::Utils;
use std::cmp::{max, min};
use std::io::Write;
use std::marker::PhantomData;
use std::mem::swap;
use std::path::PathBuf;

pub trait BucketType: Send {
    type InitType: ?Sized;

    const SUPPORTS_LOCK_FREE: bool;

    fn new(init_data: &Self::InitType, index: usize) -> Self;
    fn write_bytes(&mut self, bytes: &[u8]);
    fn write_bytes_lock_free(&self, _bytes: &[u8]) {}
    fn get_path(&self) -> PathBuf;
    fn finalize(self);
}

pub struct MultiThreadBuckets<B: BucketType> {
    buckets: Vec<RwLock<B>>,
}

#[derive(Clone, Debug)]
pub struct DecimationFactor {
    pub numerator: usize,
    pub denominator: usize,
}

impl DecimationFactor {
    pub fn from_ratio(mut ratio: f64) -> DecimationFactor {
        if ratio > 1.0 {
            ratio = 1.0;
        }

        DecimationFactor {
            numerator: (ratio * 32.0) as usize,
            denominator: 32,
        }
    }
}

impl<B: BucketType> MultiThreadBuckets<B> {
    pub fn new(
        size: usize,
        init_data: &B::InitType,
        alternative_data: Option<(&B::InitType, DecimationFactor)>,
    ) -> MultiThreadBuckets<B> {
        let mut buckets = Vec::with_capacity(size);

        for i in 0..size {
            let init_data = match &alternative_data {
                None => init_data,
                Some((alt_data, decimation)) => {
                    if i % decimation.denominator < decimation.numerator {
                        *alt_data
                    } else {
                        init_data
                    }
                }
            };

            buckets.push(RwLock::new(B::new(init_data, i)));
        }
        MultiThreadBuckets { buckets }
    }

    pub fn add_data(&self, index: BucketIndexType, data: &[u8]) {
        if B::SUPPORTS_LOCK_FREE {
            let bucket = self.buckets[index as usize].read();
            bucket.write_bytes_lock_free(data);
        } else {
            let mut bucket = self.buckets[index as usize].write();
            bucket.write_bytes(data);
        }
    }

    pub fn finalize(self) -> Vec<PathBuf> {
        self.buckets.iter().map(|b| b.read().get_path()).collect()
    }
}

impl<B: BucketType> Drop for MultiThreadBuckets<B> {
    fn drop(&mut self) {
        let mut buckets = Vec::new();
        swap(&mut self.buckets, &mut buckets);

        buckets.into_iter().for_each(|bucket| {
            bucket.into_inner().finalize();
        });
    }
}

unsafe impl<B: BucketType> Send for MultiThreadBuckets<B> {}
unsafe impl<B: BucketType> Sync for MultiThreadBuckets<B> {}

pub trait BucketWriter {
    type ExtraData;
    fn write_to(&self, bucket: impl Write, extra_data: &Self::ExtraData);
    fn get_size(&self) -> usize;
}

impl<const SIZE: usize> BucketWriter for [u8; SIZE] {
    type ExtraData = ();
    #[inline(always)]
    fn write_to(&self, mut bucket: impl Write, _extra_data: &Self::ExtraData) {
        bucket.write(self);
    }

    #[inline(always)]
    fn get_size(&self) -> usize {
        self.len()
    }
}

pub struct BucketsThreadDispatcher<'a, B: BucketType, T: BucketWriter + Clone> {
    mtb: &'a MultiThreadBuckets<B>,
    thread_data: Vec<Vec<u8>>,
    max_bucket_size: usize,
    _phantom: PhantomData<T>,
}

impl<'a, B: BucketType, T: BucketWriter + Clone> BucketsThreadDispatcher<'a, B, T> {
    pub fn new(
        max_buffersize: MemoryDataSize,
        mtb: &'a MultiThreadBuckets<B>,
    ) -> BucketsThreadDispatcher<'a, B, T> {
        let mut thread_data = Vec::new();
        thread_data.reserve(mtb.buckets.len());
        let mut randomval = thread_rng();

        let mut rand_max_buffer_size = max_buffersize;

        for _i in 0..mtb.buckets.len() {
            let fraction = (randomval.next_u32() as f64 / (u32::MAX as f64)) * 0.40 - 0.20;
            let capacity = max_buffersize * (1.0 + fraction);
            thread_data.push(Vec::with_capacity(capacity.as_bytes()));
            rand_max_buffer_size = rand_max_buffer_size.max(capacity);
        }

        Self {
            mtb,
            thread_data,
            max_bucket_size: rand_max_buffer_size.as_bytes(),
            _phantom: PhantomData,
        }
    }

    #[inline]
    pub fn add_element(&mut self, bucket: BucketIndexType, extra_data: &T::ExtraData, element: T) {
        let bucket_buf = &mut self.thread_data[bucket as usize];
        if element.get_size() + bucket_buf.len() > min(bucket_buf.capacity(), self.max_bucket_size)
        {
            self.mtb.add_data(bucket, bucket_buf.as_slice());
            bucket_buf.clear();
        }
        element.write_to(bucket_buf, extra_data);
    }

    pub fn finalize(self) {}
}

impl<'a, B: BucketType, T: BucketWriter + Clone> Drop for BucketsThreadDispatcher<'a, B, T> {
    fn drop(&mut self) {
        for (index, vec) in self.thread_data.iter_mut().enumerate() {
            if vec.len() == 0 {
                continue;
            }
            self.mtb.add_data(index as BucketIndexType, vec.as_slice());
        }
        self.thread_data.clear();
    }
}
