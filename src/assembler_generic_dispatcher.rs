use crate::assembler::run_assembler;
use crate::colors::colors_manager::ColorsManager;
use crate::hashes::cn_nthash::CanonicalNtHashIteratorFactory;
use crate::hashes::fw_nthash::ForwardNtHashIteratorFactory;
use crate::hashes::{HashableHashFunctionFactory, MinimizerHashFunctionFactory};
use crate::utils::compute_best_m;
use crate::{AssemblerArgs, HashType};
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::PathBuf;
use std::process::exit;

fn run_assembler_from_args<
    BucketingHash: MinimizerHashFunctionFactory,
    MergingHash: HashableHashFunctionFactory,
    ColorsImpl: ColorsManager,
>(
    args: AssemblerArgs,
) {
    let mut inputs = args.input.clone();

    for list in args.input_lists {
        for input in BufReader::new(File::open(list).unwrap()).lines() {
            if let Ok(input) = input {
                inputs.push(PathBuf::from(input));
            }
        }
    }

    if inputs.is_empty() {
        println!("ERROR: No input files specified!");
        exit(1);
    }

    run_assembler::<BucketingHash, MergingHash, ColorsImpl>(
        args.common_args.klen,
        args.common_args
            .mlen
            .unwrap_or(compute_best_m(args.common_args.klen)),
        args.step,
        args.last_step,
        inputs,
        args.output_file,
        args.common_args.temp_dir,
        args.common_args.threads_count,
        args.min_multiplicity,
        args.common_args.buckets_count_log,
        Some(args.number),
    );
}

pub(crate) fn dispatch_assembler_hash_type<ColorsImpl: ColorsManager>(args: AssemblerArgs) {
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

    use crate::hashes::*;

    match hash_type {
        HashType::SeqHash => {
            let k = args.common_args.klen;

            if k <= 8 {
                if args.common_args.forward_only {
                    run_assembler_from_args::<
                        CanonicalNtHashIteratorFactory,
                        cn_seqhash::u16::CanonicalSeqHashFactory,
                        ColorsImpl,
                    >(args);
                } else {
                    run_assembler_from_args::<
                        CanonicalNtHashIteratorFactory,
                        cn_seqhash::u16::CanonicalSeqHashFactory,
                        ColorsImpl,
                    >(args)
                }
            } else if k <= 16 {
                if args.common_args.forward_only {
                    run_assembler_from_args::<
                        ForwardNtHashIteratorFactory,
                        fw_seqhash::u32::ForwardSeqHashFactory,
                        ColorsImpl,
                    >(args);
                } else {
                    run_assembler_from_args::<
                        CanonicalNtHashIteratorFactory,
                        cn_seqhash::u32::CanonicalSeqHashFactory,
                        ColorsImpl,
                    >(args)
                }
            } else if k <= 32 {
                if args.common_args.forward_only {
                    run_assembler_from_args::<
                        ForwardNtHashIteratorFactory,
                        fw_seqhash::u64::ForwardSeqHashFactory,
                        ColorsImpl,
                    >(args);
                } else {
                    run_assembler_from_args::<
                        CanonicalNtHashIteratorFactory,
                        cn_seqhash::u64::CanonicalSeqHashFactory,
                        ColorsImpl,
                    >(args)
                }
            } else if k <= 64 {
                if args.common_args.forward_only {
                    run_assembler_from_args::<
                        ForwardNtHashIteratorFactory,
                        fw_seqhash::u128::ForwardSeqHashFactory,
                        ColorsImpl,
                    >(args);
                } else {
                    run_assembler_from_args::<
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
                run_assembler_from_args::<
                    ForwardNtHashIteratorFactory,
                    fw_rkhash::u32::ForwardRabinKarpHashFactory,
                    ColorsImpl,
                >(args);
            } else {
                run_assembler_from_args::<
                    CanonicalNtHashIteratorFactory,
                    cn_rkhash::u32::CanonicalRabinKarpHashFactory,
                    ColorsImpl,
                >(args)
            }
        }
        HashType::RabinKarp64 => {
            if args.common_args.forward_only {
                run_assembler_from_args::<
                    ForwardNtHashIteratorFactory,
                    fw_rkhash::u64::ForwardRabinKarpHashFactory,
                    ColorsImpl,
                >(args);
            } else {
                run_assembler_from_args::<
                    CanonicalNtHashIteratorFactory,
                    cn_rkhash::u64::CanonicalRabinKarpHashFactory,
                    ColorsImpl,
                >(args)
            }
        }
        HashType::RabinKarp128 => {
            if args.common_args.forward_only {
                run_assembler_from_args::<
                    ForwardNtHashIteratorFactory,
                    fw_rkhash::u128::ForwardRabinKarpHashFactory,
                    ColorsImpl,
                >(args);
            } else {
                run_assembler_from_args::<
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
