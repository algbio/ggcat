pub const HASH_A: u64 = 0x3c8b_fbb3_95c6_0474;
pub const HASH_C: u64 = 0x3193_c185_62a0_2b4c;
pub const HASH_G: u64 = 0x2032_3ed0_8257_2324;
pub const HASH_T: u64 = 0x2955_49f5_4be2_4456;

#[inline(always)]
pub fn h(c: u8) -> u64 {
    unsafe { *H_LOOKUP.get_unchecked(c as usize) }
}

#[inline(always)]
pub fn rc(c: u8) -> u64 {
    unsafe { *RC_LOOKUP.get_unchecked(c as usize) }
}

const H_LOOKUP: [u64; 256] = {
    let mut lookup = [1; 256];

    // Support compressed reads transparently
    lookup[0 /*b'A'*/] = HASH_A;
    lookup[1 /*b'C'*/] = HASH_C;
    lookup[2 /*b'T'*/] = HASH_T;
    lookup[3 /*b'G'*/] = HASH_G;
    lookup[4 /*b'N'*/] = 0;

    lookup[b'A' as usize] = HASH_A;
    lookup[b'C' as usize] = HASH_C;
    lookup[b'G' as usize] = HASH_G;
    lookup[b'T' as usize] = HASH_T;
    lookup[b'N' as usize] = 0;
    lookup
};

const RC_LOOKUP: [u64; 256] = {
    let mut lookup = [1; 256];

    // Support compressed reads transparently
    lookup[0 /*b'A'*/] = HASH_T;
    lookup[1 /*b'C'*/] = HASH_G;
    lookup[2 /*b'T'*/] = HASH_A;
    lookup[3 /*b'G'*/] = HASH_C;
    lookup[4 /*b'N'*/] = 0;

    lookup[b'A' as usize] = HASH_T;
    lookup[b'C' as usize] = HASH_G;
    lookup[b'G' as usize] = HASH_C;
    lookup[b'T' as usize] = HASH_A;
    lookup[b'N' as usize] = 0;
    lookup
};
