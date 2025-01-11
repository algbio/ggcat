use config::{BucketIndexType, MultiplicityCounterType};
use io::compressed_read::CompressedRead;

pub trait RewriteBucketCompute: Sized + 'static + Send {
    fn get_rewrite_bucket<C>(
        k: usize,
        m: usize,
        seq_data: &(u8, u8, C, CompressedRead, MultiplicityCounterType),
        used_hash_bits: usize,
        bucket_bits_count: usize,
    ) -> BucketIndexType;
}
