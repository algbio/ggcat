use byteorder::ReadBytesExt;
use config::BucketIndexType;
use io::varint::{decode_varint, encode_varint, VARINT_MAX_SIZE};
use parallel_processor::buckets::bucket_writer::BucketItemSerializer;
use std::io::{Read, Write};

#[derive(Clone, Debug)]
pub struct LinkMapping {
    pub bucket: BucketIndexType,
    pub entry: u64,
}

pub struct LinkMappingSerializer;

impl BucketItemSerializer for LinkMappingSerializer {
    type InputElementType<'a> = LinkMapping;
    type ExtraData = ();
    type ReadBuffer = ();
    type ExtraDataBuffer = ();
    type ReadType<'a> = LinkMapping;

    #[inline(always)]
    fn new() -> Self {
        Self
    }

    #[inline(always)]
    fn reset(&mut self) {}

    #[inline(always)]
    fn write_to(
        &mut self,
        element: &Self::InputElementType<'_>,
        bucket: &mut Vec<u8>,
        _extra_data: &Self::ExtraData,
        _: &Self::ExtraDataBuffer,
    ) {
        encode_varint(|b| bucket.write_all(b), element.bucket as u64).unwrap();
        encode_varint(|b| bucket.write_all(b), element.entry).unwrap();
    }

    fn read_from<'a, S: Read>(
        &mut self,
        mut stream: S,
        _read_buffer: &'a mut Self::ReadBuffer,
        _: &mut Self::ExtraDataBuffer,
    ) -> Option<Self::ReadType<'a>> {
        let bucket = decode_varint(|| stream.read_u8().ok())?;
        let entry = decode_varint(|| stream.read_u8().ok())?;
        Some(LinkMapping {
            bucket: bucket as BucketIndexType,
            entry,
        })
    }

    #[inline(always)]
    fn get_size(&self, _element: &Self::InputElementType<'_>, _: &()) -> usize {
        VARINT_MAX_SIZE * 2
    }
}
