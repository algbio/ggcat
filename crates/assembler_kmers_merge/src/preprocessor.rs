use crate::ParallelKmersMergeFactory;
use colors::colors_manager::ColorsManager;
use config::BucketIndexType;
use config::READ_FLAG_INCL_END;
use hashes::ExtendableHashTraitType;
use hashes::HashFunction;
use hashes::{HashFunctionFactory, HashableSequence, MinimizerHashFunctionFactory};
use io::compressed_read::CompressedRead;
use kmers_transform::{KmersTransformExecutorFactory, KmersTransformPreprocessor};
use std::marker::PhantomData;

pub struct ParallelKmersMergePreprocessor<
    H: MinimizerHashFunctionFactory,
    MH: HashFunctionFactory,
    CX: ColorsManager,
    const COMPUTE_SIMPLITIGS: bool,
> {
    _phantom: PhantomData<(H, MH, CX)>,
}

impl<
        H: MinimizerHashFunctionFactory,
        MH: HashFunctionFactory,
        CX: ColorsManager,
        const COMPUTE_SIMPLITIGS: bool,
    > ParallelKmersMergePreprocessor<H, MH, CX, COMPUTE_SIMPLITIGS>
{
    pub fn new() -> Self {
        Self {
            _phantom: PhantomData,
        }
    }
}

impl<
        H: MinimizerHashFunctionFactory,
        MH: HashFunctionFactory,
        CX: ColorsManager,
        const COMPUTE_SIMPLITIGS: bool,
    > KmersTransformPreprocessor<ParallelKmersMergeFactory<H, MH, CX, COMPUTE_SIMPLITIGS>>
    for ParallelKmersMergePreprocessor<H, MH, CX, COMPUTE_SIMPLITIGS>
{
    fn get_sequence_bucket<C>(
        &self,
        global_data: &<ParallelKmersMergeFactory<H, MH, CX, COMPUTE_SIMPLITIGS> as KmersTransformExecutorFactory>::GlobalExtraData,
        seq_data: &(u8, u8, C, CompressedRead),
        used_hash_bits: usize,
        bucket_bits_count: usize,
    ) -> BucketIndexType {
        let read = &seq_data.3;
        let flags = seq_data.0;
        let decr_val =
            ((read.bases_count() == global_data.k) && (flags & READ_FLAG_INCL_END) == 0) as usize;

        let hashes = H::new(
            read.sub_slice((1 - decr_val)..(global_data.k - decr_val)),
            global_data.m,
        );

        let minimizer = hashes
            .iter()
            .min_by_key(|k| H::get_full_minimizer(k.to_unextendable()))
            .unwrap();

        H::get_bucket(
            used_hash_bits,
            bucket_bits_count,
            minimizer.to_unextendable(),
        )
    }
}
