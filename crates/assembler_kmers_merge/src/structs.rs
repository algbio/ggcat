use config::BucketIndexType;
use io::concurrent::temp_reads::creads_utils::{
    CompressedReadsBucketData, CompressedReadsBucketDataSerializer,
};
use io::concurrent::temp_reads::extra_data::{
    SequenceExtraDataConsecutiveCompression, SequenceExtraDataTempBufferManagement,
};
use parallel_processor::buckets::bucket_writer::BucketItemSerializer;
use parallel_processor::buckets::writers::compressed_binary_writer::CompressedBinaryWriter;
use parallel_processor::buckets::LockFreeBucket;
use std::marker::PhantomData;
use std::path::PathBuf;
use structs::unitigs_counters::UnitigsCounters;
use utils::owned_drop::OwnedDrop;

pub struct ResultsBucket<X: SequenceExtraDataConsecutiveCompression> {
    pub read_index: u64,
    pub reads_writer: OwnedDrop<CompressedBinaryWriter>,
    pub temp_buffer: Vec<u8>,
    pub bucket_index: BucketIndexType,
    pub serializer:
        CompressedReadsBucketDataSerializer<PartialUnitigExtraData<X>, typenum::U0, false>,
    pub _phantom: PhantomData<X>,
}

#[derive(Clone, Debug)]
pub struct PartialUnitigExtraData<X: SequenceExtraDataConsecutiveCompression> {
    pub counters: UnitigsCounters,
    pub colors: X,
}

impl<X: SequenceExtraDataConsecutiveCompression> SequenceExtraDataTempBufferManagement
    for PartialUnitigExtraData<X>
{
    type TempBuffer = X::TempBuffer;

    fn new_temp_buffer() -> Self::TempBuffer {
        X::new_temp_buffer()
    }

    fn clear_temp_buffer(buffer: &mut Self::TempBuffer) {
        X::clear_temp_buffer(buffer)
    }

    fn copy_temp_buffer(dest: &mut Self::TempBuffer, src: &Self::TempBuffer) {
        X::copy_temp_buffer(dest, src)
    }

    fn copy_extra_from(extra: Self, src: &Self::TempBuffer, dst: &mut Self::TempBuffer) -> Self {
        Self {
            colors: X::copy_extra_from(extra.colors, src, dst),
            counters: extra.counters,
        }
    }
}

impl<X: SequenceExtraDataConsecutiveCompression> SequenceExtraDataConsecutiveCompression
    for PartialUnitigExtraData<X>
{
    type LastData = X::LastData;

    fn decode_extended(
        buffer: &mut Self::TempBuffer,
        reader: &mut impl std::io::Read,
        last_data: Self::LastData,
    ) -> Option<Self> {
        let color = X::decode_extended(buffer, reader, last_data)?;
        let counter = UnitigsCounters::decode_extended(&mut (), reader, ())?;
        Some(Self {
            colors: color,
            counters: counter,
        })
    }

    fn encode_extended(
        &self,
        buffer: &Self::TempBuffer,
        writer: &mut impl std::io::Write,
        last_data: Self::LastData,
    ) {
        self.colors.encode_extended(buffer, writer, last_data);
        self.counters.encode_extended(&(), writer, ());
    }

    fn obtain_last_data(&self, last_data: Self::LastData) -> Self::LastData {
        self.colors.obtain_last_data(last_data)
    }

    fn max_size(&self) -> usize {
        self.colors.max_size() + self.counters.max_size()
    }
}

impl<X: SequenceExtraDataConsecutiveCompression> ResultsBucket<X> {
    pub fn add_read(
        &mut self,
        el: PartialUnitigExtraData<X>,
        read: &[u8],
        extra_buffer: &X::TempBuffer,
    ) -> u64 {
        self.temp_buffer.clear();
        self.serializer.write_to(
            &CompressedReadsBucketData::new(read, 0, 0),
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
    pub sequences: Vec<PathBuf>,
    pub hashes: Vec<PathBuf>,
}
