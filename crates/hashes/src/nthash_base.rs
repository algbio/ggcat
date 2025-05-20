// pub const HASH_A: u64 = 0x3c8b_fbb3_95c6_0474;
// pub const HASH_C: u64 = 0x3193_c185_62a0_2b4c;
// pub const HASH_G: u64 = 0x2032_3ed0_8257_2324;
// pub const HASH_T: u64 = 0x2955_49f5_4be2_4456;

// #[inline(always)]
// pub fn h(c: u8, compressed: bool) -> u64 {
//     unsafe {
//         if compressed {
//             *H_LOOKUP_COMPRESSED.get_unchecked(c as usize)
//         } else {
//             *H_LOOKUP_COMPRESSED.get_unchecked(((c >> 1) & 0x3) as usize)
//         }
//     }
// }

// #[inline(always)]
// pub fn rc(c: u8, compressed: bool) -> u64 {
//     unsafe {
//         if compressed {
//             *RC_LOOKUP_COMPRESSED.get_unchecked(c as usize)
//         } else {
//             *RC_LOOKUP_COMPRESSED.get_unchecked(((c >> 1) & 0x3) as usize)
//         }
//     }
// }

// const H_LOOKUP_COMPRESSED: [u64; 4] = {
//     let mut lookup = [1; 4];
//     // Support compressed reads
//     lookup[0 /*b'A'*/] = HASH_A;
//     lookup[1 /*b'C'*/] = HASH_C;
//     lookup[2 /*b'T'*/] = HASH_T;
//     lookup[3 /*b'G'*/] = HASH_G;
//     lookup
// };

// const RC_LOOKUP_COMPRESSED: [u64; 4] = {
//     let mut lookup = [1; 4];

//     // Support compressed reads
//     lookup[0 /*b'A'*/] = HASH_T;
//     lookup[1 /*b'C'*/] = HASH_G;
//     lookup[2 /*b'T'*/] = HASH_A;
//     lookup[3 /*b'G'*/] = HASH_C;
//     lookup
// };

const MULTIPLIER: u64 = 0x397f178c6ae330f9;

#[inline(always)]
pub fn h(c: u8, compressed: bool) -> u64 {
    let mult = if compressed { c << 1 } else { c & 0x6 } + 1;
    (mult as u64).wrapping_mul(MULTIPLIER)
}

#[inline(always)]
pub fn rc(c: u8, compressed: bool) -> u64 {
    let mult = (if compressed { c << 1 } else { c & 0x6 } ^ 4) + 1;
    (mult as u64).wrapping_mul(MULTIPLIER)
}
