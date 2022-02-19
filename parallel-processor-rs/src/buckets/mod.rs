use crate::buckets::bucket_type::BucketType;
use parking_lot::RwLock;
use std::path::PathBuf;

pub mod bucket_type;
pub mod bucket_writer;
pub mod concurrent;
pub mod single;

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
            bucket.write_batch_data_lock_free(data);
        } else {
            let mut bucket = self.buckets[index as usize].write();
            bucket.write_batch_data(data);
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
