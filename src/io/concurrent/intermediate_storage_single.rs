use crate::config::BucketIndexType;
use crate::io::concurrent::temp_reads::extra_data::SequenceExtraData;
use crate::io::concurrent::temp_reads::reads_writer::IntermediateReadsWriter;
use crate::CompressedRead;
use parallel_processor::buckets::MultiThreadBuckets;
use std::path::PathBuf;

pub struct IntermediateSequencesStorageSingleBucket<'a, T: SequenceExtraData> {
    buckets: &'a MultiThreadBuckets<IntermediateReadsWriter<T>>,
    bucket_index: BucketIndexType,
    buffer: Vec<u8>,
}
impl<'a, T: SequenceExtraData> IntermediateSequencesStorageSingleBucket<'a, T> {
    const ALLOWED_LEN: usize = 65536;

    pub fn new(
        bucket_index: BucketIndexType,
        buckets: &'a MultiThreadBuckets<IntermediateReadsWriter<T>>,
    ) -> Self {
        let buffer = Vec::with_capacity(parallel_processor::Utils::multiply_by(
            Self::ALLOWED_LEN,
            1.05,
        ));

        Self {
            buckets,
            bucket_index,
            buffer,
        }
    }

    pub fn get_bucket_index(&self) -> BucketIndexType {
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

    #[allow(non_camel_case_types)]
    pub fn add_read<FLAGS_COUNT: typenum::Unsigned>(&mut self, el: T, seq: &[u8], flags: u8) {
        if self.buffer.len() > 0 && self.buffer.len() + seq.len() > Self::ALLOWED_LEN {
            self.flush_buffer();
        }

        el.encode(&mut self.buffer);
        CompressedRead::from_plain_write_directly_to_buffer_with_flags::<FLAGS_COUNT>(
            seq,
            &mut self.buffer,
            flags,
        );
    }
}

impl<'a, T: SequenceExtraData> Drop for IntermediateSequencesStorageSingleBucket<'a, T> {
    fn drop(&mut self) {
        self.flush_buffer();
    }
}
