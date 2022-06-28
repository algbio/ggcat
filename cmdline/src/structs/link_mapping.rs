use crate::config::BucketIndexType;
use crate::io::varint::{decode_varint, encode_varint, VARINT_MAX_SIZE};
use byteorder::ReadBytesExt;
use parallel_processor::buckets::bucket_writer::BucketItem;
use std::io::{Read, Write};

#[derive(Clone, Debug)]
pub struct LinkMapping {
    pub bucket: BucketIndexType,
    pub entry: u64,
}

impl BucketItem for LinkMapping {
    type ExtraData = ();
    type ReadBuffer = ();
    type ExtraDataBuffer = ();
    type ReadType<'a> = Self;

    #[inline(always)]
    fn write_to(
        &self,
        bucket: &mut Vec<u8>,
        _extra_data: &Self::ExtraData,
        _: &Self::ExtraDataBuffer,
    ) {
        encode_varint(|b| bucket.write_all(b), self.bucket as u64).unwrap();
        encode_varint(|b| bucket.write_all(b), self.entry).unwrap();
    }

    fn read_from<'a, S: Read>(
        mut stream: S,
        _read_buffer: &'a mut Self::ReadBuffer,
        _: &mut Self::ExtraDataBuffer,
    ) -> Option<Self::ReadType<'a>> {
        let bucket = decode_varint(|| stream.read_u8().ok())?;
        let entry = decode_varint(|| stream.read_u8().ok())?;
        Some(Self {
            bucket: bucket as BucketIndexType,
            entry,
        })
    }

    #[inline(always)]
    fn get_size(&self, _: &()) -> usize {
        VARINT_MAX_SIZE * 2
    }
}
