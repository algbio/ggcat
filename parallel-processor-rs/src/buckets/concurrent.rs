use crate::buckets::bucket_writer::BucketItem;
use crate::buckets::{LockFreeBucket, MultiThreadBuckets};
use crate::memory_data_size::MemoryDataSize;
use rand::{thread_rng, RngCore};
use std::marker::PhantomData;

pub struct BucketsThreadDispatcher<'a, B: LockFreeBucket> {
    mtb: &'a MultiThreadBuckets<B>,
    thread_data: Vec<Vec<u8>>,
}

impl<'a, B: LockFreeBucket> BucketsThreadDispatcher<'a, B> {
    pub fn new(
        max_buffersize: MemoryDataSize,
        mtb: &'a MultiThreadBuckets<B>,
    ) -> BucketsThreadDispatcher<'a, B> {
        let mut thread_data = Vec::with_capacity(mtb.buckets.len());
        let mut random_gen = thread_rng();

        for _ in 0..mtb.buckets.len() {
            let fraction = (random_gen.next_u32() as f64 / (u32::MAX as f64)) * 0.40 - 0.20;
            let capacity = max_buffersize * (1.0 + fraction);
            thread_data.push(Vec::with_capacity(capacity.as_bytes()));
        }

        Self { mtb, thread_data }
    }

    #[inline]
    pub fn add_element<'b, T: BucketItem + ?Sized + 'b>(
        &mut self,
        bucket: u16,
        extra_data: &T::ExtraData,
        element: &T,
    ) {
        let bucket_buf = &mut self.thread_data[bucket as usize];
        if element.get_size(extra_data) + bucket_buf.len() > bucket_buf.capacity()
            && bucket_buf.len() > 0
        {
            self.mtb.add_data(bucket, bucket_buf.as_slice());
            bucket_buf.clear();
        }
        element.write_to(bucket_buf, extra_data);
    }

    pub fn finalize(self) {}
}

impl<'a, B: LockFreeBucket> Drop for BucketsThreadDispatcher<'a, B> {
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
