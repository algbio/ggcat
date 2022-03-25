use crate::config::BucketIndexType;
use crate::io::concurrent::temp_reads::extra_data::SequenceExtraData;
use crate::io::concurrent::temp_reads::reads_writer::IntermediateReadsWriter;
use crate::utils::compressed_read::CompressedRead;
use parallel_processor::buckets::MultiThreadBuckets;

pub struct IntermediateReadsThreadWriter<'a, T: SequenceExtraData> {
    buckets: &'a MultiThreadBuckets<IntermediateReadsWriter<T>>,
    buffers: Vec<Vec<u8>>,
}

impl<'a, T: SequenceExtraData> IntermediateReadsThreadWriter<'a, T> {
    const ALLOWED_LEN: usize = 65536;

    pub fn new(
        buckets_count: usize,
        buckets: &'a MultiThreadBuckets<IntermediateReadsWriter<T>>,
    ) -> Self {
        let mut buffers = Vec::with_capacity(buckets_count);
        for _ in 0..buckets_count {
            buffers.push(Vec::with_capacity(parallel_processor::Utils::multiply_by(
                Self::ALLOWED_LEN,
                1.05,
            )));
        }

        Self { buckets, buffers }
    }

    fn flush_buffers(&mut self, bucket: BucketIndexType) {
        // self.buckets
        //     .add_data(bucket, &self.buffers[bucket as usize]);
        self.buffers[bucket as usize].clear();
    }

    #[allow(non_camel_case_types)]
    pub fn add_read<FLAGS_COUNT: typenum::Unsigned>(
        &mut self,
        el: T,
        seq: &[u8],
        bucket: BucketIndexType,
        flags: u8,
    ) {
        if self.buffers[bucket as usize].len() > 0
            && self.buffers[bucket as usize].len() + seq.len() > Self::ALLOWED_LEN
        {
            self.flush_buffers(bucket);
        }

        el.encode(&mut self.buffers[bucket as usize]);
        CompressedRead::from_plain_write_directly_to_buffer_with_flags::<FLAGS_COUNT>(
            seq,
            &mut self.buffers[bucket as usize],
            flags,
        );
    }

    pub fn finalize(self) {}
}

impl<'a, T: SequenceExtraData> Drop for IntermediateReadsThreadWriter<'a, T> {
    fn drop(&mut self) {
        for bucket in 0..self.buffers.len() {
            if self.buffers[bucket].len() > 0 {
                self.flush_buffers(bucket as BucketIndexType);
            }
        }
    }
}
