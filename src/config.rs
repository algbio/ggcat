pub type BucketIndexType = u16;
pub type MinimizerType = u32;
pub type SortingHashType = u16;

pub const FIRST_BUCKET_BITS: usize = 9;
pub const SECOND_BUCKET_BITS: usize = 8;

const_assert!(FIRST_BUCKET_BITS + SECOND_BUCKET_BITS >= 16);

pub const FIRST_BUCKETS_COUNT: usize = 1 << FIRST_BUCKET_BITS;
pub const SECOND_BUCKETS_COUNT: usize = 1 << SECOND_BUCKET_BITS;