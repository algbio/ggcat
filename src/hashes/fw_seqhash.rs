use crate::hash::{
    ExtendableHashTraitType, HashFunction, HashFunctionFactory, HashableSequence,
    UnextendableHashTraitType,
};
use crate::types::MinimizerType;
use std::mem::size_of;

pub mod u16 {
    type HashIntegerType = u16;
    include!("base/fw_seqhash_base.rs");
}

pub mod u32 {
    type HashIntegerType = u32;
    include!("base/fw_seqhash_base.rs");
}

pub mod u64 {
    type HashIntegerType = u64;
    include!("base/fw_seqhash_base.rs");
}

pub mod u128 {
    type HashIntegerType = u128;
    include!("base/fw_seqhash_base.rs");
}
