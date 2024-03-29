pub mod u16 {
    type HashIntegerType = u16;
    include!("base/cn_seqhash_base.rs");
}

pub mod u32 {
    type HashIntegerType = u32;
    include!("base/cn_seqhash_base.rs");
}

pub mod u64 {
    type HashIntegerType = u64;
    include!("base/cn_seqhash_base.rs");
}

pub mod u128 {
    type HashIntegerType = u128;
    include!("base/cn_seqhash_base.rs");
}
