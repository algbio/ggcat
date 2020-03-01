use crate::rolling_kseq_iterator::RollingKseqImpl;
use std::hint::unreachable_unchecked;

const H_LOOKUP: [u64; 256] = {
    let mut lookup = [1; 256];
    lookup[b'A' as usize] = 0x3c8b_fbb3_95c6_0474;
    lookup[b'C' as usize] = 0x3193_c185_62a0_2b4c;
    lookup[b'G' as usize] = 0x2032_3ed0_8257_2324;
    lookup[b'T' as usize] = 0x2955_49f5_4be2_4456;
    lookup[b'N' as usize] = 0;
    lookup
};

#[inline(always)]
fn h(c: u8) -> u64 {
    let val = unsafe { *H_LOOKUP.get_unchecked(c as usize) };
    if val == 1 {
        unsafe { unreachable_unchecked(); }
//        panic!("Non-ACGTN nucleotide encountered!")
    }
    val
}

pub struct RollingNtHashIterator {
    fh: u64,
    k_minus1: usize
}

impl RollingNtHashIterator {
    pub fn new() -> RollingNtHashIterator {
        RollingNtHashIterator {
            fh: 0,
            k_minus1: 0
        }
    }
}

impl RollingKseqImpl<u8, u64> for RollingNtHashIterator {

    #[inline(always)]
    fn clear(&mut self, ksize: usize) {
        self.fh = 0;
        self.k_minus1 = ksize - 1;
    }

    #[inline(always)]
    fn init(&mut self, index: usize, base: u8) {
        self.fh ^= h(base).rotate_left((self.k_minus1 - index - 1) as u32);
    }

    #[inline(always)]
    fn iter(&mut self, index: usize, out_base: u8, in_base: u8) -> u64 {
        let res = self.fh.rotate_left(1) ^ h(in_base);
        self.fh =  res ^ h(out_base).rotate_left((self.k_minus1) as u32);
        res
    }
}