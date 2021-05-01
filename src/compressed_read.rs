use nthash::NtSequence;
use std::iter::FromIterator;
use std::marker::PhantomData;
use std::ops::{Index, Range};
use std::slice::from_raw_parts;

#[derive(Copy, Clone)]
pub struct CompressedRead<'a> {
    size: usize,
    start: u8,
    data: *const u8,
    _phantom: PhantomData<&'a ()>,
}

pub const H_LOOKUP: [u64; 4] = [
    /*b'A'*/ 0x3c8b_fbb3_95c6_0474,
    /*b'C'*/ 0x3193_c185_62a0_2b4c,
    /*b'T'*/ 0x2955_49f5_4be2_4456,
    /*b'G'*/ 0x2032_3ed0_8257_2324,
];
pub const H_INV_LETTERS: [u8; 4] = [b'A', b'C', b'T', b'G'];

impl<'a> CompressedRead<'a> {
    #[inline]
    pub fn new(data: &'a [u8], bases_count: usize) -> Self {
        if (data.len() * 4) < bases_count {
            panic!("Compressed read overflow!");
        }

        CompressedRead {
            size: bases_count,
            start: 0,
            data: data.as_ptr(),
            _phantom: PhantomData,
        }
    }

    #[inline]
    pub fn new_offset(data: &'a [u8], offset: usize, bases_count: usize) -> Self {
        if (data.len() * 4) < bases_count {
            panic!("Compressed read overflow!");
        }

        CompressedRead {
            size: bases_count,
            start: offset as u8,
            data: data.as_ptr(),
            _phantom: PhantomData,
        }
    }

    #[inline]
    pub fn from_compressed_reads(reads: &'a [u8], reads_offset: usize, bases_count: usize) -> Self {
        let byte_start = reads_offset / 4;
        let byte_offset = reads_offset % 4;
        Self::new_offset(
            &reads[byte_start..byte_start + ((byte_offset + bases_count + 3) / 4)],
            byte_offset,
            bases_count,
        )
    }

    pub fn cmp_slice(&self) -> &[u8] {
        unsafe { from_raw_parts(self.data, (self.size + self.start as usize + 3) / 4) }
    }

    pub fn sub_slice(&self, range: Range<usize>) -> CompressedRead<'a> {
        assert!(range.start <= range.end);

        let start = (range.start % 4) as u8;
        let sbyte = range.start / 4;

        CompressedRead {
            size: range.end - range.start,
            start,
            data: unsafe { self.data.add(sbyte) },
            _phantom: Default::default(),
        }
    }

    #[inline(always)]
    pub unsafe fn get_base_unchecked(&self, index: usize) -> u8 {
        let index = index + self.start as usize;
        ((*self.data.add(index / 4) >> ((index % 4) * 2)) & 0x3)
    }

    pub fn write_to_slice(&self, slice: &mut [u8]) {
        for (val, letter) in slice.iter_mut().zip(
            (0..self.size).map(|i| unsafe { H_INV_LETTERS[self.get_base_unchecked(i) as usize] }),
        ) {
            *val = letter;
        }
    }

    pub fn to_string(&self) -> String {
        String::from_iter(
            (0..self.size)
                .map(|i| unsafe { H_INV_LETTERS[self.get_base_unchecked(i) as usize] as char }),
        )
    }
}

impl<'a> NtSequence for CompressedRead<'a> {
    #[inline(always)]
    unsafe fn get_h_unchecked(&self, index: usize) -> u64 {
        H_LOOKUP[self.get_base_unchecked(index) as usize]
    }

    #[inline(always)]
    fn bases_count(&self) -> usize {
        self.size
    }
}
