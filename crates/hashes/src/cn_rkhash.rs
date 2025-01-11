pub mod u32 {
    use crate::dummy_hasher::DummyHasherBuilder;

    type HashIntegerType = u32;
    const MULTIPLIER: HashIntegerType = 0xdc7d07b1;
    const MULT_INV: HashIntegerType = 0xfd0ee151;

    pub const MULT_A: HashIntegerType = 0x58107bed;
    pub const MULT_C: HashIntegerType = 0x6da984cf;
    pub const MULT_G: HashIntegerType = 0x7d6c2d5d;
    pub const MULT_T: HashIntegerType = 0x3ea1c319;

    include!("base/cn_rkhash_base.rs");
}

pub mod u64 {
    use crate::dummy_hasher::DummyHasherBuilder;

    type HashIntegerType = u64;
    const MULTIPLIER: HashIntegerType = 0x660b123642ca9149;
    const MULT_INV: HashIntegerType = 0x397f178c6ae330f9;

    pub const MULT_A: HashIntegerType = 0x34889973de695e1b;
    pub const MULT_C: HashIntegerType = 0x72dacb3a60672825;
    pub const MULT_G: HashIntegerType = 0x61bf33e452d231a5;
    pub const MULT_T: HashIntegerType = 0x759db32ccd931bb5;

    include!("base/cn_rkhash_base.rs");

    #[cfg(test)]
    mod tests_reverse {
        use crate::{cn_seqhash::u64::CanonicalSeqHashFactory, HashFunctionFactory};

        #[test]
        fn cn_seqhash_reverse() {
            let hash = 1531907577009573; // 11581873256642304;
            let inverted = CanonicalSeqHashFactory::invert(hash);
            const C_INV_LETTERS: [u8; 4] = [b'A', b'C', b'T', b'G'];

            println!(
                "{:?}",
                String::from_utf8(
                    inverted
                        .iter()
                        .map(|b| {
                            let b = *b as usize;
                            [
                                C_INV_LETTERS[b & 0b11],
                                C_INV_LETTERS[(b >> 2) & 0b11],
                                C_INV_LETTERS[(b >> 4) & 0b11],
                                C_INV_LETTERS[(b >> 6) & 0b11],
                            ]
                            .into_iter()
                        })
                        .flatten()
                        .take(27)
                        .collect::<Vec<u8>>()
                )
                .unwrap()
            );
        }
    }
}

pub mod u128 {
    use crate::dummy_hasher::DummyHasherBuilder;

    type HashIntegerType = u128;
    const MULTIPLIER: HashIntegerType = 0x3eb9402f3e733993add64d3ca00e1b6b;
    const MULT_INV: HashIntegerType = 0x9cb6ff6f1b1a6d733e0952e899c3943;

    pub const MULT_A: HashIntegerType = 0x4751137d01d863c5b8c36de2b7d399df;
    pub const MULT_C: HashIntegerType = 0x37ea3a13226503fb783f5cb69f4552bd;
    pub const MULT_G: HashIntegerType = 0x50796b285343f09a0c53113ae736572b;
    pub const MULT_T: HashIntegerType = 0x1e62d96a5e1f5ade2d4e68d8f88110b7;

    include!("base/cn_rkhash_base.rs");
}
