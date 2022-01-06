use parking_lot::RwLock;
use rand::{thread_rng, RngCore};
use rayon::iter::ParallelIterator;

use crate::memory_data_size::*;
use std::cmp::{max, min};
use std::io::Write;
use std::marker::PhantomData;
use std::mem::{size_of, swap};
use std::path::PathBuf;

pub trait BucketType: Send {
    type InitType: ?Sized;
    type DataType = u8;

    const SUPPORTS_LOCK_FREE: bool;

    fn new(init_data: &Self::InitType, index: usize) -> Self;
    fn write_data(&mut self, data: &[Self::DataType]);
    fn write_data_lock_free(&self, _data: &[Self::DataType]) {}
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

    pub fn get_path(&self, bucket: u16) -> PathBuf {
        self.buckets[bucket as usize].read().get_path()
    }

    pub fn add_data(&self, index: u16, data: &[B::DataType]) {
        if B::SUPPORTS_LOCK_FREE {
            let bucket = self.buckets[index as usize].read();
            bucket.write_data_lock_free(data);
        } else {
            let mut bucket = self.buckets[index as usize].write();
            bucket.write_data(data);
        }
    }

    pub fn finalize(&mut self) -> Vec<PathBuf> {
        let paths = self.buckets.iter().map(|b| b.read().get_path()).collect();
        self.buckets.drain(..).for_each(|bucket| {
            bucket.into_inner().finalize();
        });
        paths
    }
}

impl<B: BucketType> Drop for MultiThreadBuckets<B> {
    fn drop(&mut self) {
        self.buckets.drain(..).for_each(|bucket| {
            bucket.into_inner().finalize();
        });
    }
}

unsafe impl<B: BucketType> Send for MultiThreadBuckets<B> {}
unsafe impl<B: BucketType> Sync for MultiThreadBuckets<B> {}

pub trait BucketWriter<DataType = u8> {
    type ExtraData;
    fn write_to(&self, bucket: &mut Vec<DataType>, extra_data: &Self::ExtraData);
    fn get_size(&self) -> usize;
}

impl<T: Copy> BucketWriter<T> for T {
    type ExtraData = ();

    #[inline(always)]
    fn write_to(&self, bucket: &mut Vec<T>, _extra_data: &Self::ExtraData) {
        bucket.push(*self);
    }

    #[inline(always)]
    fn get_size(&self) -> usize {
        1
    }
}

impl<const SIZE: usize> BucketWriter for [u8; SIZE] {
    type ExtraData = ();
    #[inline(always)]
    fn write_to(&self, mut bucket: &mut Vec<u8>, _extra_data: &Self::ExtraData) {
        bucket.write(self).unwrap();
    }

    #[inline(always)]
    fn get_size(&self) -> usize {
        self.len()
    }
}

impl BucketWriter for [u8] {
    type ExtraData = ();
    #[inline(always)]
    fn write_to(&self, mut bucket: &mut Vec<u8>, _extra_data: &Self::ExtraData) {
        bucket.write(self).unwrap();
    }

    #[inline(always)]
    fn get_size(&self) -> usize {
        self.len()
    }
}

pub struct BucketsThreadDispatcher<'a, B: BucketType, T: BucketWriter<B::DataType> + ?Sized> {
    mtb: &'a MultiThreadBuckets<B>,
    thread_data: Vec<Vec<B::DataType>>,
    max_bucket_size: usize,
    _phantom: PhantomData<T>,
}

impl<'a, B: BucketType, T: BucketWriter<B::DataType> + ?Sized> BucketsThreadDispatcher<'a, B, T> {
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
            thread_data.push(Vec::with_capacity(
                capacity.as_bytes() / size_of::<B::DataType>(),
            ));
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
    pub fn add_element(&mut self, bucket: u16, extra_data: &T::ExtraData, element: &T) {
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

impl<'a, B: BucketType, T: BucketWriter<B::DataType> + ?Sized> Drop
    for BucketsThreadDispatcher<'a, B, T>
{
    fn drop(&mut self) {
        for (index, vec) in self.thread_data.iter_mut().enumerate() {
            if vec.len() == 0 {
                continue;
            }
            self.mtb.add_data(index as u16, vec.as_slice());
        }
        self.thread_data.clear();
    }
}
