use crate::varint::encode_varint_flags;
use core::fmt::{Debug, Formatter};
use hashes::HashableSequence;
use std::io::Write;
use std::iter::FromIterator;
use std::marker::PhantomData;
use std::ops::Range;
use std::slice::from_raw_parts;
use utils::Utils;

#[derive(Copy, Clone)]
pub struct CompressedRead<'a> {
    pub(crate) size: usize,
    pub start: u8,
    data: *const u8,
    _phantom: PhantomData<&'a ()>,
}

unsafe impl<'a> Sync for CompressedRead<'a> {}
unsafe impl<'a> Send for CompressedRead<'a> {}

impl<'a> Debug for CompressedRead<'a> {
    fn fmt(&self, f: &mut Formatter<'_>) -> core::fmt::Result {
        f.write_str(&self.to_string())
    }
}

#[derive(Copy, Clone)]
pub struct CompressedReadIndipendent {
    start: usize,
    size: usize,
}

impl CompressedReadIndipendent {
    pub fn from_plain(plain: &[u8], storage: &mut Vec<u8>) -> CompressedReadIndipendent {
        let start = storage.len() * 4;

        CompressedRead::compress_from_plain(plain, |b| {
            storage.extend_from_slice(b);
        });

        CompressedReadIndipendent {
            start,
            size: plain.len(),
        }
    }

    pub fn from_read(read: &CompressedRead, storage: &mut Vec<u8>) -> CompressedReadIndipendent {
        let start = storage.len() * 4;
        storage.extend_from_slice(read.get_packed_slice());
        CompressedReadIndipendent {
            start,
            size: read.size,
        }
    }

    pub fn as_reference<'a>(&self, storage: &'a Vec<u8>) -> CompressedRead<'a> {
        CompressedRead {
            size: self.size,
            start: (self.start % 4) as u8,
            data: unsafe { storage.as_ptr().add(self.start / 4) },
            _phantom: Default::default(),
        }
    }

    pub fn bases_count(&self) -> usize {
        self.size
    }
}

impl<'a> CompressedRead<'a> {
    #[inline(always)]
    #[allow(non_camel_case_types)]
    pub fn from_plain_write_directly_to_buffer_with_flags<FLAGS_COUNT: typenum::Unsigned>(
        seq: &'a [u8],
        buffer: &mut Vec<u8>,
        flags: u8,
    ) {
        encode_varint_flags::<_, _, FLAGS_COUNT>(
            |b| buffer.extend_from_slice(b),
            seq.len() as u64,
            flags,
        );
        Self::compress_from_plain(seq, |b| {
            buffer.extend_from_slice(b);
        });
    }

    #[inline(always)]
    #[allow(non_camel_case_types)]
    pub fn from_plain_write_directly_to_stream_with_flags<
        W: Write,
        FLAGS_COUNT: typenum::Unsigned,
    >(
        seq: &'a [u8],
        stream: &mut W,
        flags: u8,
    ) {
        encode_varint_flags::<_, _, FLAGS_COUNT>(
            |b| stream.write(b).unwrap(),
            seq.len() as u64,
            flags,
        );
        Self::compress_from_plain(seq, |b| {
            stream.write(b).unwrap();
        });
    }

    #[inline(always)]
    fn compress_from_plain(seq: &'a [u8], mut writer: impl FnMut(&[u8])) {
        for chunk in seq.chunks(16) {
            let mut value = 0;
            for aa in chunk.iter().rev() {
                value = (value << 2) | Utils::compress_base(*aa) as u32;
            }
            writer(&value.to_le_bytes()[..(chunk.len() + 3) / 4])
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

    pub fn get_packed_slice(&self) -> &[u8] {
        unsafe { from_raw_parts(self.data, (self.size + self.start as usize + 3) / 4) }
    }

    pub fn copy_to_buffer(&self, buffer: &mut Vec<u8>) {
        if self.start == 0 {
            buffer.extend_from_slice(self.get_packed_slice());
        } else {
            let bytes_count = (self.size + 3) / 4;

            buffer.reserve(bytes_count);
            unsafe {
                let mut dest_ptr = buffer.as_mut_ptr().add(buffer.len());
                buffer.set_len(buffer.len() + bytes_count);

                let right_offset = self.start * 2;
                let left_offset = 8 - right_offset;

                for b in 0..bytes_count {
                    let current = *self.data.add(b);
                    let next = *self.data.add(b + 1);

                    *dest_ptr = (current >> right_offset) | (next << left_offset);
                    dest_ptr = dest_ptr.add(1);
                }
            }
        }
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
        (*self.data.add(index / 4) >> ((index % 4) * 2)) & 0x3
    }

    pub fn write_unpacked_to_vec(&self, vec: &mut Vec<u8>) {
        vec.reserve(self.size);
        let start = vec.len();
        unsafe {
            vec.set_len(vec.len() + self.size);
        }

        for (val, letter) in vec[start..start + self.size]
            .iter_mut()
            .zip(self.as_bases_iter())
        {
            *val = letter;
        }
    }

    pub fn write_unpacked_to_slice(&self, slice: &mut [u8]) {
        for (val, letter) in slice.iter_mut().zip(self.as_bases_iter()) {
            *val = letter;
        }
    }

    pub fn as_bases_iter(&'a self) -> impl Iterator<Item = u8> + 'a {
        (0..self.size).map(move |i| unsafe { Utils::decompress_base(self.get_base_unchecked(i)) })
    }

    pub fn as_reverse_complement_bases_iter(&'a self) -> impl Iterator<Item = u8> + 'a {
        (0..self.size)
            .rev()
            .map(move |i| unsafe { Utils::decompress_base(self.get_base_unchecked(i) ^ 2) })
    }

    pub fn to_string(&self) -> String {
        String::from_iter(
            (0..self.size)
                .map(|i| unsafe { Utils::decompress_base(self.get_base_unchecked(i)) as char }),
        )
    }

    pub fn get_length(&self) -> usize {
        self.bases_count()
    }
}
//
// impl<'a> FastaCompatibleRead for CompressedRead<'a> {
//     type IntermediateData = Range<usize>;
//
//     fn write_unpacked_to_buffer(&self, buffer: &mut Vec<u8>) -> Self::IntermediateData {
//         let start = buffer.len();
//         buffer.extend(
//             (0..self.size).map(|i| unsafe { Utils::decompress_base(self.get_base_unchecked(i)) }),
//         );
//         start..buffer.len()
//     }
//
//     fn as_slice_from_buffer<'b>(
//         &'b self,
//         buffer: &'b Vec<u8>,
//         data: Self::IntermediateData,
//     ) -> &'b [u8] {
//         &buffer[data]
//     }
//

// }
//
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
