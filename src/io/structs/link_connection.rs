use crate::io::concurrent::temp_reads::extra_data::SequenceExtraData;
use crate::io::structs::unitig_link::UnitigIndex;
use parallel_processor::buckets::bucket_writer::BucketWriter;
use std::io::{Read, Write};

#[derive(Debug)]
pub struct LinkConnection {
    pub source: UnitigIndex,
    pub dest: UnitigIndex,
}

impl SequenceExtraData for LinkConnection {
    #[inline(always)]
    fn decode<'a>(reader: &'a mut impl Read) -> Option<Self> {
        let source = UnitigIndex::decode(reader)?;
        let dest = UnitigIndex::decode(reader)?;
        Some(Self { source, dest })
    }

    #[inline(always)]
    fn encode<'a>(&self, writer: &'a mut impl Write) {
        self.source.encode(writer);
        self.dest.encode(writer);
    }

    #[inline(always)]
    fn max_size(&self) -> usize {
        self.source.max_size() + self.dest.max_size()
    }
}

impl BucketWriter for LinkConnection {
    type ExtraData = ();

    fn write_to(&self, bucket: &mut Vec<u8>, _extra_data: &Self::ExtraData) {
        self.encode(bucket);
    }

    fn get_size(&self) -> usize {
        30
    }
}
