use crate::hashes::HashableSequence;
use crate::io::concurrent::temp_reads::extra_data::SequenceExtraData;
use crate::io::varint::decode_varint_flags;
use crate::CompressedRead;
use byteorder::ReadBytesExt;
use parallel_processor::buckets::bucket_writer::BucketItem;
use std::io::Read;
use std::marker::PhantomData;

pub struct CompressedReadsBucketHelper<'a, E: SequenceExtraData, FlagsCount: typenum::Unsigned> {
    read: &'a [u8],
    flags: u8,
    _phantom: PhantomData<(E, FlagsCount)>,
}

impl<'a, E: SequenceExtraData, FlagsCount: typenum::Unsigned>
    CompressedReadsBucketHelper<'a, E, FlagsCount>
{
    #[inline(always)]
    pub fn new(read: &'a [u8], flags: u8) -> Self {
        Self {
            read,
            flags,
            _phantom: PhantomData,
        }
    }
}

impl<'a, E: SequenceExtraData, FlagsCount: typenum::Unsigned> BucketItem
    for CompressedReadsBucketHelper<'a, E, FlagsCount>
{
    type ExtraData = E;
    type ReadBuffer = Vec<u8>;
    type ReadType<'b> = (u8, E, CompressedRead<'b>);

    #[inline(always)]
    fn write_to(&self, bucket: &mut Vec<u8>, extra_data: &Self::ExtraData) {
        extra_data.encode(bucket);
        CompressedRead::from_plain_write_directly_to_buffer_with_flags::<FlagsCount>(
            self.read, bucket, self.flags,
        );
    }

    #[inline]
    fn read_from<'b, S: Read>(
        mut stream: S,
        read_buffer: &'b mut Self::ReadBuffer,
    ) -> Option<Self::ReadType<'b>> {
        let extra = E::decode(&mut stream)?;
        let (size, flags) = decode_varint_flags::<_, FlagsCount>(|| stream.read_u8().ok())?;

        if size == 0 {
            return None;
        }

        read_buffer.clear();
        let bytes = ((size + 3) / 4) as usize;
        read_buffer.reserve(bytes);
        unsafe {
            read_buffer.set_len(bytes);
        }

        stream.read_exact(read_buffer.as_mut_slice()).unwrap();

        Some((
            flags,
            extra,
            CompressedRead::new_from_compressed(read_buffer.as_slice(), size as usize),
        ))
    }

    #[inline(always)]
    fn get_size(&self, extra: &Self::ExtraData) -> usize {
        ((self.read.bases_count() + 3) / 4) + extra.max_size() + 10
    }
}
