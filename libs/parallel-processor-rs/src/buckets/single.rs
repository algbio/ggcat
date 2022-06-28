use crate::buckets::bucket_writer::BucketItem;
use crate::buckets::{LockFreeBucket, MultiThreadBuckets};
use crate::memory_data_size::MemoryDataSize;
use std::path::PathBuf;

pub struct SingleBucketThreadDispatcher<'a, B: LockFreeBucket> {
    buckets: &'a MultiThreadBuckets<B>,
    bucket_index: u16,
    buffer: Vec<u8>,
}

impl<'a, B: LockFreeBucket> SingleBucketThreadDispatcher<'a, B> {
    pub fn new(
        buffer_size: MemoryDataSize,
        bucket_index: u16,
        buckets: &'a MultiThreadBuckets<B>,
    ) -> Self {
        let buffer = Vec::with_capacity(buffer_size.as_bytes());

        Self {
            buckets,
            bucket_index,
            buffer,
        }
    }

    pub fn get_bucket_index(&self) -> u16 {
        self.bucket_index
    }

    pub fn get_path(&self) -> PathBuf {
        self.buckets.get_path(self.bucket_index)
    }

    fn flush_buffer(&mut self) {
        if self.buffer.len() == 0 {
            return;
        }

        self.buckets.add_data(self.bucket_index, &self.buffer);
        self.buffer.clear();
    }

    pub fn add_element_extended<T: BucketItem + ?Sized>(
        &mut self,
        extra_data: &T::ExtraData,
        extra_buffer: &T::ExtraDataBuffer,
        element: &T,
    ) {
        if element.get_size(extra_data) + self.buffer.len() > self.buffer.capacity() {
            self.flush_buffer();
        }
        element.write_to(&mut self.buffer, extra_data, extra_buffer);
    }

    pub fn add_element<T: BucketItem<ExtraDataBuffer = ()> + ?Sized>(
        &mut self,
        extra_data: &T::ExtraData,
        element: &T,
    ) {
        self.add_element_extended(extra_data, &(), element);
    }

    pub fn finalize(self) {}
}

impl<'a, B: LockFreeBucket> Drop for SingleBucketThreadDispatcher<'a, B> {
    fn drop(&mut self) {
        self.flush_buffer();
    }
}
