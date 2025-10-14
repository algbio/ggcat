use config::BucketIndexType;
use io::compressed_read::CompressedRead;
use io::concurrent::temp_reads::creads_utils::{
    CompressedReadsBucketData, CompressedReadsBucketDataSerializer, NoMinimizerPosition,
    NoMultiplicity, NoSecondBucket, ToReadData,
};
use io::concurrent::temp_reads::extra_data::SequenceExtraDataConsecutiveCompression;
use parallel_processor::buckets::bucket_writer::BucketItemSerializer;
use parallel_processor::buckets::writers::compressed_binary_writer::CompressedBinaryWriter;
use parallel_processor::buckets::{LockFreeBucket, SingleBucket};
use std::marker::PhantomData;
use structs::partial_unitigs_extra_data::PartialUnitigExtraData;
use utils::owned_drop::OwnedDrop;

pub struct ResultsBucket<X: SequenceExtraDataConsecutiveCompression> {
    pub read_index: u64,
    pub reads_writer: OwnedDrop<CompressedBinaryWriter>,
    pub temp_buffer: Vec<u8>,
    pub bucket_index: BucketIndexType,
    pub serializer: CompressedReadsBucketDataSerializer<
        PartialUnitigExtraData<X>,
        NoSecondBucket,
        NoMultiplicity,
        NoMinimizerPosition,
        typenum::U0,
    >,
    pub _phantom: PhantomData<X>,
}

impl<X: SequenceExtraDataConsecutiveCompression> ResultsBucket<X> {
    pub fn add_read<'a, R: ToReadData<'a>>(
        &mut self,
        el: PartialUnitigExtraData<X>,
        read: R,
        extra_buffer: &X::TempBuffer,
    ) -> u64 {
        self.temp_buffer.clear();
        self.serializer.write_to(
            &CompressedReadsBucketData::new(read, 0, 0, 0, false),
            &mut self.temp_buffer,
            &el,
            extra_buffer,
        );
        self.reads_writer.write_data(self.temp_buffer.as_slice());

        let read_index = self.read_index;
        self.read_index += 1;
        read_index
    }

    pub fn add_compressed_read(
        &mut self,
        el: PartialUnitigExtraData<X>,
        read: CompressedRead,
        extra_buffer: &X::TempBuffer,
    ) -> u64 {
        self.temp_buffer.clear();
        self.serializer.write_to(
            &CompressedReadsBucketData::new_packed(read, 0, 0, 0, false),
            &mut self.temp_buffer,
            &el,
            extra_buffer,
        );
        self.reads_writer.write_data(self.temp_buffer.as_slice());

        let read_index = self.read_index;
        self.read_index += 1;
        read_index
    }

    pub fn get_bucket_index(&self) -> BucketIndexType {
        self.bucket_index
    }
}

impl<X: SequenceExtraDataConsecutiveCompression> Drop for ResultsBucket<X> {
    fn drop(&mut self) {
        unsafe { self.reads_writer.take().finalize() }
    }
}

pub struct RetType {
    pub sequences: Vec<SingleBucket>,
}
