use crate::compressed_read::CompressedRead;
use crate::varint::{decode_varint_flags, encode_varint_flags, VARINT_FLAGS_MAX_SIZE};
use byteorder::ReadBytesExt;
use parallel_processor::buckets::bucket_writer::BucketItemSerializer;
use std::io::Read;
use std::marker::PhantomData;

use super::extra_data::SequenceExtraDataConsecutiveCompression;

enum ReadData<'a> {
    Plain(&'a [u8]),
    Packed(CompressedRead<'a>),
}

pub struct CompressedReadsBucketData<'a> {
    read: ReadData<'a>,
    extra_bucket: u8,
    flags: u8,
}

impl<'a> CompressedReadsBucketData<'a> {
    #[inline(always)]
    pub fn new(read: &'a [u8], flags: u8, extra_bucket: u8) -> Self {
        Self {
            read: ReadData::Plain(read),
            extra_bucket,
            flags,
        }
    }

    #[inline(always)]
    pub fn new_packed(read: CompressedRead<'a>, flags: u8, extra_bucket: u8) -> Self {
        Self {
            read: ReadData::Packed(read),
            flags,
            extra_bucket,
        }
    }
}

pub struct CompressedReadsBucketDataSerializer<
    E: SequenceExtraDataConsecutiveCompression,
    FlagsCount: typenum::Unsigned,
    const WITH_SECOND_BUCKET: bool,
> {
    last_data: E::LastData,
    _phantom: PhantomData<FlagsCount>,
}

impl<
        'a,
        E: SequenceExtraDataConsecutiveCompression,
        FlagsCount: typenum::Unsigned,
        const WITH_SECOND_BUCKET: bool,
    > BucketItemSerializer
    for CompressedReadsBucketDataSerializer<E, FlagsCount, WITH_SECOND_BUCKET>
{
    type InputElementType<'b> = CompressedReadsBucketData<'b>;
    type ExtraData = E;
    type ReadBuffer = Vec<u8>;
    type ExtraDataBuffer = E::TempBuffer;
    type ReadType<'b> = (u8, u8, E, CompressedRead<'b>);

    #[inline(always)]
    fn new() -> Self {
        Self {
            last_data: Default::default(),
            _phantom: PhantomData,
        }
    }

    #[inline(always)]
    fn reset(&mut self) {
        self.last_data = Default::default();
    }

    #[inline(always)]
    fn write_to(
        &mut self,
        element: &Self::InputElementType<'_>,
        bucket: &mut Vec<u8>,
        extra_data: &Self::ExtraData,
        extra_data_buffer: &Self::ExtraDataBuffer,
    ) {
        if WITH_SECOND_BUCKET {
            bucket.push(element.extra_bucket);
        }

        extra_data.encode_extended(extra_data_buffer, bucket, self.last_data);
        self.last_data = extra_data.obtain_last_data(self.last_data);

        match element.read {
            ReadData::Plain(read) => {
                CompressedRead::from_plain_write_directly_to_buffer_with_flags::<FlagsCount>(
                    read,
                    bucket,
                    element.flags,
                );
            }
            ReadData::Packed(read) => {
                encode_varint_flags::<_, _, FlagsCount>(
                    |b| bucket.extend_from_slice(b),
                    read.size as u64,
                    element.flags,
                );
                read.copy_to_buffer(bucket);
            }
        }
    }

    #[inline]
    fn read_from<'b, S: Read>(
        &mut self,
        mut stream: S,
        read_buffer: &'b mut Self::ReadBuffer,
        extra_read_buffer: &mut Self::ExtraDataBuffer,
    ) -> Option<Self::ReadType<'b>> {
        let second_bucket = if WITH_SECOND_BUCKET {
            stream.read_u8().ok()?
        } else {
            0
        };

        let extra = E::decode_extended(extra_read_buffer, &mut stream, self.last_data)?;
        self.last_data = extra.obtain_last_data(self.last_data);

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
    fn get_size(&self, element: &Self::InputElementType<'_>, extra: &Self::ExtraData) -> usize {
        let bases_count = match element.read {
            ReadData::Plain(read) => read.len(),
            ReadData::Packed(read) => read.size,
        };

        ((bases_count + 3) / 4)
            + extra.max_size()
            + VARINT_FLAGS_MAX_SIZE
            + if WITH_SECOND_BUCKET { 1 } else { 0 }
    }
}
