use crate::concurrent::temp_reads::creads_utils::MinimizerModeOption;
use crate::varint::{decode_varint_flags, encode_varint_flags};
use byteorder::ReadBytesExt;
use core::fmt::{Debug, Formatter};
use hashes::HashableSequence;
use std::hash::Hash;
use std::io::Read;
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

impl<'a> Hash for CompressedRead<'a> {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.get_packed_slice().hash(state);
    }
}

#[repr(transparent)]
pub struct BorrowableCompressedRead {
    // Hack to be able to store both a pointer and a size, to store the actual size of the read instead of the number of used bytes
    // Usable only when start == 0
    data: [()],
}

impl BorrowableCompressedRead {
    pub fn get_compressed_read(&self) -> CompressedRead {
        let data = self.data.as_ptr() as *const u8;
        let size = self.data.len();
        CompressedRead {
            size,
            start: 0,
            data,
            _phantom: Default::default(),
        }
    }
}

impl Hash for BorrowableCompressedRead {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        let compressed_read = self.get_compressed_read();
        compressed_read.size.hash(state);
        compressed_read.get_packed_slice().hash(state);
    }
}

impl PartialEq for BorrowableCompressedRead {
    fn eq(&self, other: &Self) -> bool {
        let compressed_read = self.get_compressed_read();
        let other_compressed_read = other.get_compressed_read();

        compressed_read.size == other_compressed_read.size
            && compressed_read.get_packed_slice() == other_compressed_read.get_packed_slice()
    }
}

impl Eq for BorrowableCompressedRead {}

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

    pub unsafe fn from_start_buffer_position(
        start: usize,
        size: usize,
    ) -> CompressedReadIndipendent {
        Self {
            start: start * 4,
            size,
        }
    }

    pub fn from_read_inplace(read: &CompressedRead, storage: &[u8]) -> CompressedReadIndipendent {
        CompressedReadIndipendent {
            start: read.start as usize + (read.data as usize - storage.as_ptr() as usize) * 4,
            size: read.size,
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

    #[inline(always)]
    pub fn get_packed_slice_aligned(&self, buffer: &[u8]) -> &[u8] {
        debug_assert_eq!(self.start % 4, 0);
        unsafe {
            from_raw_parts(
                buffer.as_ptr().add(self.buffer_start_index()),
                (self.size + 3) / 4,
            )
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

    pub fn buffer_start_index(&self) -> usize {
        self.start / 4
    }

    pub fn bases_count(&self) -> usize {
        self.size
    }
}

impl<'a> CompressedRead<'a> {
    const WINDOW_DUPLICATE_SENTINEL: u64 = u64::MAX >> 16;

    #[inline(always)]
    pub fn encode_length<MinimizerMode: MinimizerModeOption, FlagsCount: typenum::Unsigned>(
        buffer: &mut Vec<u8>,
        length: usize,
        min_size: usize,
        minimizer_position: u16,
        min_size_log: u8,
        flags: u8,
        is_window_duplicate: bool,
    ) {
        let encoded_length = if MinimizerMode::ENABLED {
            #[cold]
            fn cold() {}

            if is_window_duplicate {
                cold();
                encode_varint_flags::<_, _, FlagsCount>(
                    |b| buffer.extend_from_slice(b),
                    Self::WINDOW_DUPLICATE_SENTINEL,
                    0,
                )
            }

            debug_assert!(
                (minimizer_position as usize) < min_size,
                "Minimizer: {} >= {}",
                minimizer_position,
                min_size
            );
            debug_assert!(
                minimizer_position < (1 << min_size_log),
                "Minimizer: {} >= {}",
                minimizer_position,
                (1 << min_size_log)
            );

            (((length - min_size) as u64) << min_size_log) | (minimizer_position as u64)
        } else {
            (length - min_size) as u64
        };

        encode_varint_flags::<_, _, FlagsCount>(
            |b| buffer.extend_from_slice(b),
            encoded_length,
            flags,
        );
    }

    #[inline(always)]
    pub fn from_plain_write_directly_to_buffer_with_flags<
        MinimizerMode: MinimizerModeOption,
        FlagsCount: typenum::Unsigned,
    >(
        seq: &'a [u8],
        buffer: &mut Vec<u8>,
        min_size: usize,
        minimizer_position: u16,
        min_size_log: u8,
        flags: u8,
        rc: bool,
        is_window_duplicate: bool,
    ) {
        Self::encode_length::<MinimizerMode, FlagsCount>(
            buffer,
            seq.len(),
            min_size,
            minimizer_position,
            min_size_log,
            flags,
            is_window_duplicate,
        );
        if rc {
            Self::compress_from_plain_rc(seq, |b| {
                buffer.extend_from_slice(b);
            });
        } else {
            Self::compress_from_plain(seq, |b| {
                buffer.extend_from_slice(b);
            });
        }
    }

    pub fn get_borrowable(&self) -> &'a BorrowableCompressedRead {
        assert_eq!(self.start, 0);
        unsafe {
            std::mem::transmute(&*std::ptr::slice_from_raw_parts(
                self.data as *const (),
                self.size,
            ))
        }
    }

    #[inline(always)]
    pub fn read_from_stream<
        S: Read,
        MinimizerMode: MinimizerModeOption,
        FlagsCount: typenum::Unsigned,
    >(
        temp_buffer: &'a mut Vec<u8>,
        stream: &mut S,
        min_size: usize,
        min_size_log: u8,
    ) -> Option<(Self, u16, u8, bool)> {
        let (mut encoded_size, mut flags) =
            decode_varint_flags::<_, FlagsCount>(|| stream.read_u8().ok())?;

        let (size, minimizer_pos, is_window_duplicate) = if MinimizerMode::ENABLED {
            #[cold]
            fn cold() {}

            let is_window_duplicate = encoded_size == Self::WINDOW_DUPLICATE_SENTINEL;
            if is_window_duplicate {
                cold();
                (encoded_size, flags) =
                    decode_varint_flags::<_, FlagsCount>(|| stream.read_u8().ok())?;
            }

            let size = encoded_size >> min_size_log;
            let minimizer_pos = size & ((1 << min_size_log) - 1);
            (
                size as usize + min_size,
                minimizer_pos as u16,
                is_window_duplicate,
            )
        } else {
            (encoded_size as usize + min_size, 0, false)
        };

        let bytes = (size + 3) / 4;
        temp_buffer.reserve(bytes);
        let buffer_start = temp_buffer.len();
        unsafe {
            temp_buffer.set_len(buffer_start + bytes);
        }

        stream.read_exact(&mut temp_buffer[buffer_start..]).ok()?;

        Some((
            CompressedRead::new_from_compressed(&temp_buffer[buffer_start..], size),
            minimizer_pos,
            flags,
            is_window_duplicate,
        ))
    }

    #[inline(always)]
    pub fn compress_from_plain(seq: &'a [u8], mut writer: impl FnMut(&[u8])) {
        for chunk in seq.chunks(16) {
            let mut value = 0;
            for aa in chunk.iter().rev() {
                value = (value << 2) | Utils::compress_base(*aa) as u32;
            }
            writer(&value.to_le_bytes()[..(chunk.len() + 3) / 4])
        }
    }

    #[inline(always)]
    pub fn compress_from_plain_rc(seq: &'a [u8], mut writer: impl FnMut(&[u8])) {
        let mut current = seq.len();
        while current > 0 {
            let amount = current.min(16);
            let chunk = unsafe { from_raw_parts(seq.as_ptr().add(current - amount), amount) };
            let mut value = 0;
            for aa in chunk.iter() {
                value = (value << 2) | Utils::compress_base_complement(*aa) as u32;
            }
            writer(&value.to_le_bytes()[..(chunk.len() + 3) / 4]);
            current -= amount;
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

    #[inline(always)]
    pub fn get_packed_slice(&self) -> &'a [u8] {
        unsafe { from_raw_parts(self.data, (self.size + self.start as usize + 3) / 4) }
    }

    pub fn copy_to_buffer(&self, buffer: &mut Vec<u8>) {
        /*
        0 => 11111111
        1 => 00000011
        2 => 00001111
        3 => 00111111

        0 => 11111111
        3 => 00000011
        2 => 00001111
        1 => 00111111
        */
        let final_offset = (self.size ^ (self.size << 1)) % 4;
        let mask = u8::MAX >> (final_offset * 2);

        if self.start == 0 || self.size == 0 {
            buffer.extend_from_slice(self.get_packed_slice());
        } else {
            let bytes_count = (self.size + 3) / 4;

            buffer.reserve(bytes_count);
            unsafe {
                let mut dest_ptr = buffer.as_mut_ptr().add(buffer.len());
                buffer.set_len(buffer.len() + bytes_count);

                let right_offset = self.start * 2;
                let left_offset = 8 - right_offset;

                for b in 1..self.get_packed_slice().len() {
                    let current = *self.data.add(b - 1);
                    let next = *self.data.add(b);

                    *dest_ptr = (current >> right_offset) | (next << left_offset);
                    dest_ptr = dest_ptr.add(1);
                }
                // Handle the last one separately
                *dest_ptr = *self.data.add(bytes_count - 1) >> right_offset;
            }
        }
        // Mask the last byte to ensure byte equality between partial compressions
        buffer.last_mut().map(|l| *l &= mask);
    }

    pub fn copy_to_buffer_rc(&self, buffer: &mut Vec<u8>) {
        let final_offset = (self.size ^ (self.size << 1)) % 4;
        let mask = u8::MAX >> (final_offset * 2);

        if (self.size + self.start as usize) % 4 == 0 {
            buffer.extend(self.get_packed_slice().iter().rev().map(|byte| {
                let mut b = *byte ^ 0xAA;
                // Reverse the bases
                b = (b & 0xF0) >> 4 | (b & 0x0F) << 4;
                b = (b & 0xCC) >> 2 | (b & 0x33) << 2;
                b
            }));
        } else {
            let bytes_count = (self.size + 3) / 4;

            buffer.reserve(bytes_count);
            unsafe {
                let mut dest_ptr = buffer.as_mut_ptr().add(buffer.len());
                buffer.set_len(buffer.len() + bytes_count);

                let rc_start = (self.size + self.start as usize) % 4;

                let right_offset = rc_start * 2;
                let left_offset = 8 - right_offset;

                for b in (1..self.get_packed_slice().len()).rev() {
                    let current = *self.data.add(b);
                    let prev = *self.data.add(b - 1);

                    let mut b = (current << left_offset) | (prev >> right_offset);
                    b = b ^ 0xAA;
                    b = (b & 0xF0) >> 4 | (b & 0x0F) << 4;
                    b = (b & 0xCC) >> 2 | (b & 0x33) << 2;
                    *dest_ptr = b;
                    dest_ptr = dest_ptr.add(1);
                }
                let mut b = *self.data << left_offset;
                b = b ^ 0xAA;
                b = (b & 0xF0) >> 4 | (b & 0x0F) << 4;
                b = (b & 0xCC) >> 2 | (b & 0x33) << 2;
                *dest_ptr = b;
            }
        }
        // Mask the last byte to ensure byte equality between partial compressions
        buffer.last_mut().map(|l| *l &= mask);
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
        unsafe { (*self.data.add(index / 4) >> ((index % 4) * 2)) & 0x3 }
    }

    pub fn write_unpacked_to_vec(&self, vec: &mut Vec<u8>, rc: bool) {
        vec.reserve(self.size);
        let start = vec.len();
        unsafe {
            vec.set_len(vec.len() + self.size);
        }

        if rc {
            for (val, letter) in vec[start..start + self.size]
                .iter_mut()
                .zip(self.as_reverse_complement_bases_iter())
            {
                *val = letter;
            }
        } else {
            for (val, letter) in vec[start..start + self.size]
                .iter_mut()
                .zip(self.as_bases_iter())
            {
                *val = letter;
            }
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
    const IS_COMPRESSED: bool = true;
    #[inline(always)]
    unsafe fn get_unchecked_cbase(&self, index: usize) -> u8 {
        unsafe { self.get_base_unchecked(index) }
    }

    #[inline(always)]
    fn bases_count(&self) -> usize {
        self.size
    }
}

#[test]
fn test_compression() {
    let mut buffer = vec![];
    let bases = b"ACGTACGCGGTAGCTAAGCATCGATGCCGATCGTGTTTAACCATG";
    CompressedRead::compress_from_plain(bases, |b| buffer.extend_from_slice(b));
    let read = CompressedRead::from_compressed_reads(&buffer, 0, bases.len());
    assert_eq!(read.to_string().as_bytes(), bases);
}

#[test]
fn test_rc_compression() {
    let mut buffer = vec![];
    let bases = b"ACGTACGCGGTAGCTAAGCATCGATGCCGATCGTGTTTAACCATG";
    let expected_rc = b"CATGGTTAAACACGATCGGCATCGATGCTTAGCTACCGCGTACGT";

    // Compress the reverse complement
    CompressedRead::compress_from_plain_rc(bases, |b| buffer.extend_from_slice(b));
    let read = CompressedRead::from_compressed_reads(&buffer, 0, bases.len());

    // Verify the reverse complement
    assert_eq!(read.to_string(), std::str::from_utf8(expected_rc).unwrap());
}

#[test]
fn test_rc_compression_and_unpacking() {
    let mut buffer = vec![];
    let bases = b"ACGTACGCGGTAGCTAAGCATCGATGCCGATCGTGTTTAACCATG";
    let expected_rc = b"CATGGTTAAACACGATCGGCATCGATGCTTAGCTACCGCGTACGT";

    // Compress the reverse complement
    CompressedRead::compress_from_plain_rc(bases, |b| buffer.extend_from_slice(b));
    let read = CompressedRead::from_compressed_reads(&buffer, 0, bases.len());

    // Unpack the reverse complement and verify
    let mut unpacked = vec![];
    read.write_unpacked_to_vec(&mut unpacked, false);
    assert_eq!(
        std::str::from_utf8(&unpacked).unwrap(),
        std::str::from_utf8(expected_rc).unwrap()
    );
}

#[test]
fn test_read_copy_to_buffer() {
    fn rc_string(input: &str) -> String {
        input
            .chars()
            .rev()
            .map(|c| match c {
                'A' => 'T',
                'T' => 'A',
                'C' => 'G',
                'G' => 'C',
                _ => c,
            })
            .collect()
    }

    for rc in [false, true] {
        for s in 0..17 {
            let mut bases1 = b"GCGTACGCGGTAGCTAAGCATCGATGCCGATCGTGTTTAACCATG".to_vec();
            let mut bases2 = b"GCGTACGCGGTAGCTAAGCATCGATGCCGATCGTGTTTAACCATC".to_vec();

            for i in 0..17 {
                println!("Iteration {} with start offset: {} rc: {}", i, s, rc);
                let mut buffer = vec![];
                let mut buffer2 = vec![];
                CompressedRead::compress_from_plain(&bases1, |b| buffer.extend_from_slice(b));
                CompressedRead::compress_from_plain(&bases2, |b| buffer2.extend_from_slice(b));
                let read = CompressedRead::from_compressed_reads(&buffer, 0, bases1.len());
                let read2 = CompressedRead::from_compressed_reads(&buffer2, 0, bases2.len());

                println!(
                    "Bases: {} Bases2: {}",
                    std::str::from_utf8(&bases1).unwrap(),
                    std::str::from_utf8(&bases2).unwrap()
                );

                if rc {
                    println!(
                        "BaseR: {} BaseR2: {}",
                        rc_string(std::str::from_utf8(&bases1).unwrap()),
                        rc_string(std::str::from_utf8(&bases2).unwrap())
                    );
                }

                let mut test_buf1 = vec![];
                if rc {
                    read.sub_slice(s..(bases1.len() - 1))
                        .copy_to_buffer_rc(&mut test_buf1);
                } else {
                    read.sub_slice(s..(bases1.len() - 1))
                        .copy_to_buffer(&mut test_buf1);
                }

                let mut test_buf2 = vec![];
                if rc {
                    read2
                        .sub_slice(s..(bases1.len() - 1))
                        .copy_to_buffer_rc(&mut test_buf2);
                } else {
                    read2
                        .sub_slice(s..(bases1.len() - 1))
                        .copy_to_buffer(&mut test_buf2);
                }

                assert_eq!(
                    read.sub_slice(s..(bases1.len() - 1)).to_string(),
                    read2.sub_slice(s..(bases2.len() - 1)).to_string()
                );

                let read_c =
                    CompressedRead::from_compressed_reads(&test_buf1, 0, bases1.len() - 1 - s);
                let read_c2 =
                    CompressedRead::from_compressed_reads(&test_buf2, 0, bases2.len() - 1 - s);

                if rc {
                    assert_eq!(
                        read_c.to_string(),
                        rc_string(&read.sub_slice(s..(bases1.len() - 1)).to_string()),
                        "First byte: {:08b}",
                        read.get_packed_slice()[0]
                    );
                } else {
                    assert_eq!(
                        read_c.to_string(),
                        read.sub_slice(s..(bases1.len() - 1)).to_string()
                    );
                }
                if rc {
                    assert_eq!(
                        read_c.to_string(),
                        rc_string(&read2.sub_slice(s..(bases1.len() - 1)).to_string())
                    );
                } else {
                    assert_eq!(
                        read_c.to_string(),
                        read2.sub_slice(s..(bases1.len() - 1)).to_string()
                    );
                }
                assert_eq!(read_c.to_string(), read_c2.to_string());

                println!(" Left: {}", read_c.to_string());
                println!("Right: {}", read_c2.to_string());

                assert_eq!(
                    test_buf1,
                    test_buf2,
                    "Last binary repr: {:08b} vs {:08b}",
                    test_buf1.last().unwrap(),
                    test_buf2.last().unwrap()
                );

                bases1.insert(bases1.len() - 1, b'T');
                bases2.insert(bases2.len() - 1, b'T');
            }
        }
    }
}
