use crate::buckets::bucket_type::BucketType;
use crate::buckets::bucket_writer::BucketWriter;
use crate::buckets::MultiThreadBuckets;
use crate::memory_data_size::MemoryDataSize;
use std::cmp::min;
use std::path::PathBuf;

pub struct SingleBucketThreadDispatcher<'a, B: BucketType> {
    buckets: &'a MultiThreadBuckets<B>,
    bucket_index: u16,
    buffer: Vec<B::DataType>,
    max_bucket_size: usize,
}

impl<'a, B: BucketType> SingleBucketThreadDispatcher<'a, B> {
    const ALLOWED_LEN: usize = 65536;

    pub fn new(
        max_buffersize: MemoryDataSize,
        bucket_index: u16,
        buckets: &'a MultiThreadBuckets<B>,
    ) -> Self {
        let buffer = Vec::with_capacity(crate::Utils::multiply_by(Self::ALLOWED_LEN, 1.05));

        Self {
            buckets,
            bucket_index,
            buffer,
            max_bucket_size: max_buffersize.as_bytes(),
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

    pub fn add_element<T: BucketWriter<B::DataType> + ?Sized>(
        &mut self,
        extra_data: &T::ExtraData,
        element: &T,
    ) {
        if element.get_size() + self.buffer.len()
            > min(self.buffer.capacity(), self.max_bucket_size)
        {
            self.flush_buffer();
        }
        element.write_to(&mut self.buffer, extra_data);
    }

    pub fn finalize(self) {}
}

impl<'a, B: BucketType> Drop for SingleBucketThreadDispatcher<'a, B> {
    fn drop(&mut self) {
        self.flush_buffer();
    }
}
