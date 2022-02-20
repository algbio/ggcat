use crate::config::BucketIndexType;
use parallel_processor::buckets::bucket_writer::BucketWriter;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
pub struct LinkRemap {
    pub index: u64,
    pub new_bucket: BucketIndexType,
    pub new_index: u64,
    pub at_beginning: bool,
}

impl BucketWriter for LinkRemap {
    type ExtraData = ();

    fn write_to(&self, bucket: &mut Vec<u8>, _extra_data: &Self::ExtraData) {
        bincode::serialize_into(bucket, self).unwrap();
    }

    fn get_size(&self) -> usize {
        30
    }
}
