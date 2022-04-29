use crate::buckets::bucket_writer::BucketItem;
use crate::buckets::{LockFreeBucket, MultiThreadBuckets};
use crate::memory_data_size::MemoryDataSize;
use std::ops::DerefMut;

pub struct BucketsThreadBuffer {
    buffers: Vec<Vec<u8>>,
}

impl BucketsThreadBuffer {
    pub fn new(max_buffer_size: MemoryDataSize, buckets_count: usize) -> Self {
        let mut buffers = Vec::with_capacity(buckets_count);
        let capacity = max_buffer_size.as_bytes();
        for _ in 0..buckets_count {
            buffers.push(Vec::with_capacity(capacity));
        }

        Self { buffers }
    }
}

pub struct BucketsThreadDispatcher<'a, B: LockFreeBucket> {
    mtb: &'a MultiThreadBuckets<B>,
    thread_data: &'a mut BucketsThreadBuffer,
    owned_buffer: Option<Box<BucketsThreadBuffer>>,
}

impl<'a, B: LockFreeBucket> BucketsThreadDispatcher<'a, B> {
    pub fn new(
        mtb: &'a MultiThreadBuckets<B>,
        thread_data: &'a mut BucketsThreadBuffer,
    ) -> BucketsThreadDispatcher<'a, B> {
        assert_eq!(mtb.buckets.len(), thread_data.buffers.len());
        Self {
            mtb,
            thread_data,
            owned_buffer: None,
        }
    }

    pub fn new_owned(
        mtb: &'a MultiThreadBuckets<B>,
        thread_data: BucketsThreadBuffer,
    ) -> BucketsThreadDispatcher<'a, B> {
        assert_eq!(mtb.buckets.len(), thread_data.buffers.len());
        let mut owned_buffer = Box::new(thread_data);

        Self {
            mtb,
            thread_data: unsafe { &mut *(owned_buffer.deref_mut() as *mut BucketsThreadBuffer) },
            owned_buffer: Some(owned_buffer),
        }
    }

    #[inline]
    pub fn add_element<T: BucketItem + ?Sized>(
        &mut self,
        bucket: u16,
        extra_data: &T::ExtraData,
        element: &T,
    ) {
        let bucket_buf = &mut self.thread_data.buffers[bucket as usize];
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
        for (index, vec) in self.thread_data.buffers.iter_mut().enumerate() {
            if vec.len() == 0 {
                continue;
            }
            self.mtb.add_data(index as u16, vec.as_slice());
            vec.clear();
        }
    }
}
