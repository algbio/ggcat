use crate::buckets::bucket_type::BucketType;
use crate::buckets::bucket_writer::BucketWriter;
use crate::buckets::MultiThreadBuckets;
use crate::memory_data_size::MemoryDataSize;
use rand::{thread_rng, RngCore};
use std::cmp::min;
use std::marker::PhantomData;
use std::mem::size_of;

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
