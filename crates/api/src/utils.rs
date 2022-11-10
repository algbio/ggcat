use dynamic_dispatch::DynamicDispatch;

#[derive(Copy, Clone)]
pub enum HashType {
    Auto = 0,
    SeqHash = 1,
    RabinKarp32 = 2,
    RabinKarp64 = 3,
    RabinKarp128 = 4,
}

pub(crate) fn get_hash_static_id(
    hash_type: HashType,
    k: usize,
    forward_only: bool,
) -> DynamicDispatch<()> {
    use hashes::*;

    let hash_type = match hash_type {
        HashType::Auto => {
            if k <= 64 {
                HashType::SeqHash
            } else {
                HashType::RabinKarp128
            }
        }
        x => x,
    };

    match hash_type {
        HashType::SeqHash => {
            if k <= 8 {
                if forward_only {
                    fw_seqhash::u16::ForwardSeqHashFactory::DYNAMIC_DISPATCH_ID
                } else {
                    cn_seqhash::u16::CanonicalSeqHashFactory::DYNAMIC_DISPATCH_ID
                }
            } else if k <= 16 {
                if forward_only {
                    fw_seqhash::u32::ForwardSeqHashFactory::DYNAMIC_DISPATCH_ID
                } else {
                    cn_seqhash::u32::CanonicalSeqHashFactory::DYNAMIC_DISPATCH_ID
                }
            } else if k <= 32 {
                if forward_only {
                    fw_seqhash::u64::ForwardSeqHashFactory::DYNAMIC_DISPATCH_ID
                } else {
                    cn_seqhash::u64::CanonicalSeqHashFactory::DYNAMIC_DISPATCH_ID
                }
            } else if k <= 64 {
                if forward_only {
                    fw_seqhash::u128::ForwardSeqHashFactory::DYNAMIC_DISPATCH_ID
                } else {
                    cn_seqhash::u128::CanonicalSeqHashFactory::DYNAMIC_DISPATCH_ID
                }
            } else {
                panic!("Cannot use sequence hash for k > 64!");
            }
        }
        HashType::RabinKarp32 => {
            if forward_only {
                fw_rkhash::u32::ForwardRabinKarpHashFactory::DYNAMIC_DISPATCH_ID
            } else {
                cn_rkhash::u32::CanonicalRabinKarpHashFactory::DYNAMIC_DISPATCH_ID
            }
        }
        HashType::RabinKarp64 => {
            if forward_only {
                fw_rkhash::u64::ForwardRabinKarpHashFactory::DYNAMIC_DISPATCH_ID
            } else {
                cn_rkhash::u64::CanonicalRabinKarpHashFactory::DYNAMIC_DISPATCH_ID
            }
        }
        HashType::RabinKarp128 => {
            if forward_only {
                fw_rkhash::u128::ForwardRabinKarpHashFactory::DYNAMIC_DISPATCH_ID
            } else {
                cn_rkhash::u128::CanonicalRabinKarpHashFactory::DYNAMIC_DISPATCH_ID
            }
        }
        HashType::Auto => {
            unreachable!()
        }
    }
}
