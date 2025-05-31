use config::BucketIndexType;
use config::MultiplicityCounterType;
use config::READ_FLAG_INCL_END;
use hashes::ExtendableHashTraitType;
use hashes::HashFunction;
use hashes::default::MNHFactory;
use hashes::{HashFunctionFactory, HashableSequence};
use io::compressed_read::CompressedRead;
use minimizer_bucketing::resplit_bucket::RewriteBucketCompute;

#[inline(always)]
pub fn get_superkmer_minimizer(
    k: usize,
    m: usize,
    flags: u8,
    read: &CompressedRead,
) -> (usize, u64) {
    let decr_val = ((read.bases_count() == k) && (flags & READ_FLAG_INCL_END) == 0) as usize;

    let hashes = MNHFactory::new(read.sub_slice((1 - decr_val)..(k - decr_val)), m);

    let minimizer = hashes
        .iter()
        .enumerate()
        .min_by_key(|(_, k)| k.to_unextendable())
        .unwrap();

    (minimizer.0 + 1 - decr_val, minimizer.1.to_unextendable())
}

pub struct RewriteBucketComputeAssembler;

impl RewriteBucketCompute for RewriteBucketComputeAssembler {
    #[inline(always)]
    fn get_rewrite_bucket<C>(
        k: usize,
        m: usize,
        seq_data: &(u8, u8, C, CompressedRead, MultiplicityCounterType),
        used_hash_bits: usize,
        bucket_bits_count: usize,
    ) -> BucketIndexType {
        let minimizer = get_superkmer_minimizer(k, m, seq_data.0, &seq_data.3).1;
        MNHFactory::get_bucket(used_hash_bits, bucket_bits_count, minimizer)
    }
}
