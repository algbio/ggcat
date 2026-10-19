use dynamic_dispatch::DynamicDispatch;

#[derive(Copy, Clone)]
pub enum HashType {
    Auto = 0,
    SeqHash = 1,
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
            // The 16 and 32 bit hashes are opt-in features: without them, small k use the 64 bit hash
            if k <= 8 && cfg!(feature = "hash-16bit") {
                if forward_only {
                    fw_seqhash::u16::ForwardSeqHashFactory::dynamic_dispatch_id()
                } else {
                    cn_seqhash::u16::CanonicalSeqHashFactory::dynamic_dispatch_id()
                }
            } else if k <= 16 && cfg!(feature = "hash-32bit") {
                if forward_only {
                    fw_seqhash::u32::ForwardSeqHashFactory::dynamic_dispatch_id()
                } else {
                    cn_seqhash::u32::CanonicalSeqHashFactory::dynamic_dispatch_id()
                }
            } else if k <= 32 {
                if forward_only {
                    fw_seqhash::u64::ForwardSeqHashFactory::dynamic_dispatch_id()
                } else {
                    cn_seqhash::u64::CanonicalSeqHashFactory::dynamic_dispatch_id()
                }
            } else if k <= 64 {
                if forward_only {
                    fw_seqhash::u128::ForwardSeqHashFactory::dynamic_dispatch_id()
                } else {
                    cn_seqhash::u128::CanonicalSeqHashFactory::dynamic_dispatch_id()
                }
            } else {
                panic!("Cannot use sequence hash for k > 64!");
            }
        }
        HashType::RabinKarp128 => {
            if forward_only {
                fw_rkhash::u128::ForwardRabinKarpHashFactory::dynamic_dispatch_id()
            } else {
                cn_rkhash::u128::CanonicalRabinKarpHashFactory::dynamic_dispatch_id()
            }
        }
        HashType::Auto => {
            unreachable!()
        }
    }
}
