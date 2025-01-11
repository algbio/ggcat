use config::BucketIndexType;
use config::MultiplicityCounterType;
use config::READ_FLAG_INCL_END;
use hashes::default::MNHFactory;
use hashes::ExtendableHashTraitType;
use hashes::HashFunction;
use hashes::{HashFunctionFactory, HashableSequence, MinimizerHashFunctionFactory};
use io::compressed_read::CompressedRead;
use minimizer_bucketing::resplit_bucket::RewriteBucketCompute;

pub struct RewriteBucketComputeAssembler;

impl RewriteBucketCompute for RewriteBucketComputeAssembler {
    fn get_rewrite_bucket<C>(
        k: usize,
        m: usize,
        seq_data: &(u8, u8, C, CompressedRead, MultiplicityCounterType),
        used_hash_bits: usize,
        bucket_bits_count: usize,
    ) -> BucketIndexType {
        let read = &seq_data.3;
        let flags = seq_data.0;
        let decr_val = ((read.bases_count() == k) && (flags & READ_FLAG_INCL_END) == 0) as usize;

        let hashes = MNHFactory::new(read.sub_slice((1 - decr_val)..(k - decr_val)), m);

        let minimizer = hashes
            .iter()
            .min_by_key(|k| MNHFactory::get_full_minimizer(k.to_unextendable()))
            .unwrap();

        MNHFactory::get_bucket(
            used_hash_bits,
            bucket_bits_count,
            minimizer.to_unextendable(),
        )
    }
}
