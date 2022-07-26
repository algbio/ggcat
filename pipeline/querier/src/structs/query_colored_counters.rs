use byteorder::ReadBytesExt;
use colors::storage::run_length::ColorIndexSerializer;
use config::ColorIndexType;
use io::varint::{decode_varint, encode_varint, VARINT_MAX_SIZE};
use parallel_processor::buckets::bucket_writer::BucketItem;
use std::borrow::Cow;
use std::io::{Cursor, Read};
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

impl<'a> BucketItem for QueryColoredCounters<'a> {
    type ExtraData = ();
    type ReadBuffer = (Vec<QueryColorDesc>, Vec<ColorIndexType>);
    type ExtraDataBuffer = ();
    type ReadType<'b> = QueryColoredCounters<'b>;

    fn write_to(
        &self,
        bucket: &mut Vec<u8>,
        _extra_data: &Self::ExtraData,
        _extra_read_buffer: &Self::ExtraDataBuffer,
    ) {
        encode_varint(|b| bucket.extend_from_slice(b), self.queries.len() as u64);
        for query in self.queries.iter() {
            encode_varint(|b| bucket.extend_from_slice(b), query.query_index);
            encode_varint(|b| bucket.extend_from_slice(b), query.count);
        }

        assert_eq!(self.colors.len() % 2, 0);
        ColorIndexSerializer::serialize_colors(bucket, &self.colors);
    }

    fn read_from<'b, S: Read>(
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

    fn get_size(&self, _extra: &Self::ExtraData) -> usize {
        (self.colors.len() + self.queries.len() + 1) * VARINT_MAX_SIZE * 4
    }
}
