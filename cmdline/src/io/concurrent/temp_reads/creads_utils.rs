use crate::hashes::HashableSequence;
use crate::io::concurrent::temp_reads::extra_data::SequenceExtraData;
use crate::io::varint::{decode_varint_flags, encode_varint_flags, VARINT_FLAGS_MAX_SIZE};
use crate::CompressedRead;
use byteorder::ReadBytesExt;
use parallel_processor::buckets::bucket_writer::BucketItem;
use std::io::Read;
use std::marker::PhantomData;

enum ReadData<'a> {
    Plain(&'a [u8]),
    Packed(CompressedRead<'a>),
}

pub struct CompressedReadsBucketHelper<
    'a,
    E: SequenceExtraData,
    FlagsCount: typenum::Unsigned,
    const WITH_SECOND_BUCKET: bool,
> {
    read: ReadData<'a>,
    extra_bucket: u8,
    flags: u8,
    _phantom: PhantomData<(E, FlagsCount)>,
}

impl<'a, E: SequenceExtraData, FlagsCount: typenum::Unsigned, const WITH_SECOND_BUCKET: bool>
    CompressedReadsBucketHelper<'a, E, FlagsCount, WITH_SECOND_BUCKET>
{
    #[inline(always)]
    pub fn new(read: &'a [u8], flags: u8, extra_bucket: u8) -> Self {
        Self {
            read: ReadData::Plain(read),
            extra_bucket,
            flags,
            _phantom: PhantomData,
        }
    }

    #[inline(always)]
    pub fn new_packed(read: CompressedRead<'a>, flags: u8, extra_bucket: u8) -> Self {
        Self {
            read: ReadData::Packed(read),
            flags,
            extra_bucket,
            _phantom: PhantomData,
        }
    }
}

impl<'a, E: SequenceExtraData, FlagsCount: typenum::Unsigned, const WITH_SECOND_BUCKET: bool>
    BucketItem for CompressedReadsBucketHelper<'a, E, FlagsCount, WITH_SECOND_BUCKET>
{
    type ExtraData = E;
    type ReadBuffer = Vec<u8>;
    type ExtraDataBuffer = E::TempBuffer;
    type ReadType<'b> = (u8, u8, E, CompressedRead<'b>);

    #[inline(always)]
    fn write_to(
        &self,
        bucket: &mut Vec<u8>,
        extra_data: &Self::ExtraData,
        extra_data_buffer: &Self::ExtraDataBuffer,
    ) {
        if WITH_SECOND_BUCKET {
            bucket.push(self.extra_bucket);
        }

        extra_data.encode_extended(extra_data_buffer, bucket);
        match self.read {
            ReadData::Plain(read) => {
                CompressedRead::from_plain_write_directly_to_buffer_with_flags::<FlagsCount>(
                    read, bucket, self.flags,
                );
            }
            ReadData::Packed(read) => {
                encode_varint_flags::<_, _, FlagsCount>(
                    |b| bucket.extend_from_slice(b),
                    read.bases_count() as u64,
                    self.flags,
                );
                read.copy_to_buffer(bucket);
            }
        }
    }

    #[inline]
    fn read_from<'b, S: Read>(
        mut stream: S,
        read_buffer: &'b mut Self::ReadBuffer,
        extra_read_buffer: &mut Self::ExtraDataBuffer,
    ) -> Option<Self::ReadType<'b>> {
        let second_bucket = if WITH_SECOND_BUCKET {
            stream.read_u8().ok()?
        } else {
            0
        };

        let extra = E::decode_extended(extra_read_buffer, &mut stream)?;
        let (size, flags) = decode_varint_flags::<_, FlagsCount>(|| stream.read_u8().ok())?;

        if size == 0 {
            return None;
        }

        read_buffer.clear();

        let bytes = ((size + 3) / 4) as usize;
        read_buffer.reserve(bytes);
        let buffer_start = read_buffer.len();
        unsafe {
            read_buffer.set_len(buffer_start + bytes);
        }

        stream.read_exact(&mut read_buffer[buffer_start..]).unwrap();

        Some((
            flags,
            second_bucket,
            extra,
            CompressedRead::new_from_compressed(&read_buffer[buffer_start..], size as usize),
        ))
    }

    #[inline(always)]
    fn get_size(&self, extra: &Self::ExtraData) -> usize {
        let bases_count = match self.read {
            ReadData::Plain(read) => read.bases_count(),
            ReadData::Packed(read) => read.bases_count(),
        };

        ((bases_count + 3) / 4)
            + extra.max_size()
            + VARINT_FLAGS_MAX_SIZE
            + if WITH_SECOND_BUCKET { 1 } else { 0 }
    }
}
