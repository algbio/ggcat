use byteorder::ReadBytesExt;
use colors::storage::run_length::ColorIndexSerializer;
use config::ColorIndexType;
use io::varint::{decode_varint, encode_varint, VARINT_MAX_SIZE};
use parallel_processor::buckets::bucket_writer::BucketItemSerializer;
use std::io::Read;
use std::ops::Range;

#[derive(Debug, Clone)]
pub enum ColorsRange {
    Range(Range<ColorIndexType>),
}

impl ColorsRange {
    pub fn write_to_vec(self, vec: &mut Vec<ColorIndexType>) {
        let ColorsRange::Range(range) = self;
        vec.push(range.start);
        vec.push(range.end);
    }

    pub fn from_slice(slice: &[ColorIndexType]) -> Self {
        ColorsRange::Range(slice[0]..slice[1])
    }
}

#[derive(Debug, Clone)]
pub struct QueryColorDesc {
    pub query_index: u64,
    pub count: u64,
}

pub struct QueryColoredCounters<'a> {
    pub queries: &'a [QueryColorDesc],
    pub colors: &'a [ColorIndexType],
}

pub struct QueryColoredCountersSerializer;

impl BucketItemSerializer for QueryColoredCountersSerializer {
    type InputElementType<'a> = QueryColoredCounters<'a>;
    type ExtraData = ();
    type ReadBuffer = (Vec<QueryColorDesc>, Vec<ColorIndexType>);
    type ExtraDataBuffer = ();
    type ReadType<'b> = QueryColoredCounters<'b>;

    type CheckpointData = ();

    fn new() -> Self {
        Self
    }

    fn reset(&mut self) {}

    fn write_to(
        &mut self,
        element: &QueryColoredCounters<'_>,
        bucket: &mut Vec<u8>,
        _extra_data: &Self::ExtraData,
        _extra_read_buffer: &Self::ExtraDataBuffer,
    ) {
        encode_varint(
            |b| bucket.extend_from_slice(b),
            element.queries.len() as u64,
        );
        for query in element.queries.iter() {
            encode_varint(|b| bucket.extend_from_slice(b), query.query_index);
            encode_varint(|b| bucket.extend_from_slice(b), query.count);
        }

        assert_eq!(element.colors.len() % 2, 0);
        ColorIndexSerializer::serialize_colors(bucket, &element.colors);
    }

    fn read_from<'b, S: Read>(
        &mut self,
        mut stream: S,
        read_buffer: &'b mut Self::ReadBuffer,
        _extra_read_buffer: &mut Self::ExtraDataBuffer,
    ) -> Option<Self::ReadType<'b>> {
        read_buffer.0.clear();
        read_buffer.1.clear();

        let queries_count = decode_varint(|| stream.read_u8().ok())?;
        for _ in 0..queries_count {
            let query_index = decode_varint(|| stream.read_u8().ok())?;
            let count = decode_varint(|| stream.read_u8().ok())?;
            read_buffer.0.push(QueryColorDesc { query_index, count });
        }

        ColorIndexSerializer::deserialize_colors(stream, &mut read_buffer.1)?;
        Some(QueryColoredCounters {
            queries: &read_buffer.0,
            colors: &read_buffer.1,
        })
    }

    fn get_size(&self, element: &Self::InputElementType<'_>, _extra: &Self::ExtraData) -> usize {
        (element.colors.len() + element.queries.len() + 1) * VARINT_MAX_SIZE * 4
    }
}
