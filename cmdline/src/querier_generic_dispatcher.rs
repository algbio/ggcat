use crate::querier::run_query;
use crate::{HashType, QueryArgs};
use colors::colors_manager::ColorsManager;
use hashes::cn_nthash::CanonicalNtHashIteratorFactory;
use hashes::fw_nthash::ForwardNtHashIteratorFactory;
use hashes::{HashFunctionFactory, MinimizerHashFunctionFactory};
use utils::compute_best_m;

pub(crate) fn dispatch_querier_hash_type<ColorsImpl: ColorsManager>(args: QueryArgs) {
    let hash_type = match args.common_args.hash_type {
        HashType::Auto => {
            if args.common_args.klen <= 64 {
                HashType::SeqHash
            } else {
                HashType::RabinKarp128
            }
        }
        x => x,
    };

    use hashes::*;

    match hash_type {
        HashType::SeqHash => {
            let k = args.common_args.klen;

            if k <= 8 {
                if args.common_args.forward_only {
                    run_querier_from_args::<
                        CanonicalNtHashIteratorFactory,
                        cn_seqhash::u16::CanonicalSeqHashFactory,
                        ColorsImpl,
                    >(args);
                } else {
                    run_querier_from_args::<
                        CanonicalNtHashIteratorFactory,
                        cn_seqhash::u16::CanonicalSeqHashFactory,
                        ColorsImpl,
                    >(args)
                }
            } else if k <= 16 {
                if args.common_args.forward_only {
                    run_querier_from_args::<
                        ForwardNtHashIteratorFactory,
                        fw_seqhash::u32::ForwardSeqHashFactory,
                        ColorsImpl,
                    >(args);
                } else {
                    run_querier_from_args::<
                        CanonicalNtHashIteratorFactory,
                        cn_seqhash::u32::CanonicalSeqHashFactory,
                        ColorsImpl,
                    >(args)
                }
            } else if k <= 32 {
                if args.common_args.forward_only {
                    run_querier_from_args::<
                        ForwardNtHashIteratorFactory,
                        fw_seqhash::u64::ForwardSeqHashFactory,
                        ColorsImpl,
                    >(args);
                } else {
                    run_querier_from_args::<
                        CanonicalNtHashIteratorFactory,
                        cn_seqhash::u64::CanonicalSeqHashFactory,
                        ColorsImpl,
                    >(args)
                }
            } else if k <= 64 {
                if args.common_args.forward_only {
                    run_querier_from_args::<
                        ForwardNtHashIteratorFactory,
                        fw_seqhash::u128::ForwardSeqHashFactory,
                        ColorsImpl,
                    >(args);
                } else {
                    run_querier_from_args::<
                        CanonicalNtHashIteratorFactory,
                        cn_seqhash::u128::CanonicalSeqHashFactory,
                        ColorsImpl,
                    >(args)
                }
            } else {
                panic!("Cannot use sequence hash for k > 64!");
            }
        }
        HashType::RabinKarp32 => {
            if args.common_args.forward_only {
                run_querier_from_args::<
                    ForwardNtHashIteratorFactory,
                    fw_rkhash::u32::ForwardRabinKarpHashFactory,
                    ColorsImpl,
                >(args);
            } else {
                run_querier_from_args::<
                    CanonicalNtHashIteratorFactory,
                    cn_rkhash::u32::CanonicalRabinKarpHashFactory,
                    ColorsImpl,
                >(args)
            }
        }
        HashType::RabinKarp64 => {
            if args.common_args.forward_only {
                run_querier_from_args::<
                    ForwardNtHashIteratorFactory,
                    fw_rkhash::u64::ForwardRabinKarpHashFactory,
                    ColorsImpl,
                >(args);
            } else {
                run_querier_from_args::<
                    CanonicalNtHashIteratorFactory,
                    cn_rkhash::u64::CanonicalRabinKarpHashFactory,
                    ColorsImpl,
                >(args)
            }
        }
        HashType::RabinKarp128 => {
            if args.common_args.forward_only {
                run_querier_from_args::<
                    ForwardNtHashIteratorFactory,
                    fw_rkhash::u128::ForwardRabinKarpHashFactory,
                    ColorsImpl,
                >(args);
            } else {
                run_querier_from_args::<
                    CanonicalNtHashIteratorFactory,
                    cn_rkhash::u128::CanonicalRabinKarpHashFactory,
                    ColorsImpl,
                >(args)
            }
        }
        HashType::Auto => {
            unreachable!()
        }
    }
}
