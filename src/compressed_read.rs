use crate::hash::HashableSequence;
use crate::utils::Utils;
use crate::varint::encode_varint;
use std::iter::FromIterator;
use std::marker::PhantomData;
use std::ops::{Index, Range};
use std::slice::from_raw_parts;

#[derive(Copy, Clone)]
pub struct CompressedRead<'a> {
    size: usize,
    pub start: u8,
    data: *const u8,
    _phantom: PhantomData<&'a ()>,
}

#[derive(Copy, Clone)]
pub struct CompressedReadIndipendent {
    start: usize,
    size: usize,
}

impl CompressedReadIndipendent {
    pub fn from_read(read: &CompressedRead, storage: &mut Vec<u8>) -> CompressedReadIndipendent {
        let start = storage.len() * 4;
        storage.extend_from_slice(read.get_compr_slice());
        CompressedReadIndipendent {
            start,
            size: read.bases_count(),
        }
    }

    pub fn as_reference(&self, storage: &Vec<u8>) -> CompressedRead {
        CompressedRead {
            size: self.size,
            start: (self.start % 4) as u8,
            data: unsafe { storage.as_ptr().add(self.start / 4) },
            _phantom: Default::default(),
        }
    }
}

impl<'a> CompressedRead<'a> {
    pub fn new_from_plain(seq: &'a [u8], storage: &mut Vec<u8>) -> CompressedReadIndipendent {
        let start = storage.len() * 4;
        for chunk in seq.chunks(16) {
            let mut value = 0;
            for aa in chunk.iter().rev() {
                value = (value << 2) | Utils::compress_base(*aa) as u32;
            }
            storage.extend_from_slice(&value.to_le_bytes()[..(chunk.len() + 3) / 4]);
        }

        CompressedReadIndipendent {
            start,
            size: seq.len(),
        }
    }

    #[inline]
    pub fn new_from_compressed(data: &'a [u8], bases_count: usize) -> Self {
        Self::new_offset(data, 0, bases_count)
    }

    #[inline]
    fn new_offset(data: &'a [u8], offset: usize, bases_count: usize) -> Self {
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

    pub fn get_compr_slice(&self) -> &[u8] {
        unsafe { from_raw_parts(self.data, (self.size + self.start as usize + 3) / 4) }
    }

    pub fn sub_slice(&self, range: Range<usize>) -> CompressedRead<'a> {
        assert!(range.start <= range.end);

        let start = ((self.start + range.start as u8) % 4) as u8;
        let sbyte = (self.start as usize + range.start) / 4;

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
        for (val, letter) in slice.iter_mut().zip(self.as_bases_iter()) {
            *val = letter;
        }
    }

    pub fn as_bases_iter(&'a self) -> impl Iterator<Item = u8> + 'a {
        (0..self.size).map(move |i| unsafe { Utils::decompress_base(self.get_base_unchecked(i)) })
    }

    pub fn to_string(&self) -> String {
        String::from_iter(
            (0..self.size)
                .map(|i| unsafe { Utils::decompress_base(self.get_base_unchecked(i)) as char }),
        )
    }
}

impl<'a> HashableSequence for CompressedRead<'a> {
    #[inline(always)]
    unsafe fn get_unchecked_cbase(&self, index: usize) -> u8 {
        self.get_base_unchecked(index)
    }

    #[inline(always)]
    fn bases_count(&self) -> usize {
        self.size
    }
}
