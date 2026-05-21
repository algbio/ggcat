pub mod u16 {
    type HashIntegerType = u16;
    const BUCKET_MULTIPLIER: u16 = 0x0193;
    const BUCKET_BASIS: u16 = 0x9dc5;
    include!("base/fw_seqhash_base.rs");
}

pub mod u32 {
    type HashIntegerType = u32;
    const BUCKET_MULTIPLIER: u32 = 0x01000193;
    const BUCKET_BASIS: u32 = 0x811c9dc5;
    include!("base/fw_seqhash_base.rs");
}

pub mod u64 {
    type HashIntegerType = u64;
    const BUCKET_MULTIPLIER: u64 = 0x00000100000001b3;
    const BUCKET_BASIS: u64 = 0xcbf29ce484222325;
    include!("base/fw_seqhash_base.rs");
}

pub mod u128 {
    type HashIntegerType = u128;
    const BUCKET_MULTIPLIER: u128 = 0x0000000001000000000000000000013b;
    const BUCKET_BASIS: u128 = 0x6c62272e07bb014262b821756295c58d;
    include!("base/fw_seqhash_base.rs");
}
