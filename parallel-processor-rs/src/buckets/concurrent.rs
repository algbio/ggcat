use crate::buckets::bucket_writer::BucketItem;
use crate::buckets::{LockFreeBucket, MultiThreadBuckets};
use crate::memory_data_size::MemoryDataSize;
use crate::utils::panic_on_drop::PanicOnDrop;
use std::ops::DerefMut;
use std::sync::Arc;

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

pub struct BucketsThreadDispatcher<B: LockFreeBucket> {
    mtb: Arc<MultiThreadBuckets<B>>,
    thread_data: BucketsThreadBuffer,
    drop_panic: PanicOnDrop,
}

impl<B: LockFreeBucket> BucketsThreadDispatcher<B> {
    pub fn new(
        mtb: &Arc<MultiThreadBuckets<B>>,
        thread_data: BucketsThreadBuffer,
    ) -> BucketsThreadDispatcher<B> {
        assert_eq!(mtb.buckets.len(), thread_data.buffers.len());
        Self {
            mtb: mtb.clone(),
            thread_data,
            drop_panic: PanicOnDrop::new("buckets thread dispatcher not finalized"),
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

    pub fn finalize(mut self) -> BucketsThreadBuffer {
        for (index, vec) in self.thread_data.buffers.iter_mut().enumerate() {
            if vec.len() == 0 {
                continue;
            }
            self.mtb.add_data(index as u16, vec.as_slice());
            vec.clear();
        }
        self.drop_panic.manually_drop();
        self.thread_data
    }
}
