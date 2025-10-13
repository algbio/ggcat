use crate::concurrent::temp_reads::creads_utils::{AlignModeOption, MinimizerModeOption};
use crate::varint::{decode_varint, decode_varint_flags, encode_varint, encode_varint_flags};
use byteorder::ReadBytesExt;
use core::fmt::{Debug, Formatter};
use hashes::HashableSequence;
use rustc_hash::FxBuildHasher;
use std::cmp::Ordering;
use std::hash::{Hash, Hasher};
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

// impl<'a> Hash for CompressedRead<'a> {
//     fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
//         self.get_hash_repr().hash(state);
//     }
// }

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

#[derive(Copy, Clone, Default)]
pub struct CompressedReadIndipendent {
    pub start: usize,
    pub size: usize,
}

unsafe fn get_hash_repr_aligned_overflow16(
    slice: &[u8],
    start_base_offset: usize,
    end_base: usize,
) -> u64 {
    let mut buffer = [0; 16];
    let mut position = 0;
    let mut start_mask = u128::MAX << (start_base_offset * 2);
    while position * 4 < end_base {
        let value =
            u128::from_le_bytes(unsafe { *(slice.as_ptr().add(position) as *const [u8; 16]) });

        let end_mask_bits = ((end_base - position * 4) * 2).min(128);
        let end_mask = 1u128
            .wrapping_shl(end_mask_bits as u32)
            .wrapping_sub(1 + (end_mask_bits == 128) as u128);

        buffer = (u128::from_le_bytes(buffer) ^ value & end_mask & start_mask).to_ne_bytes();
        position += 16;
        start_mask = u128::MAX;
    }
    let low = u64::from_ne_bytes(unsafe { *(buffer.as_ptr() as *const [u8; 8]) });
    let high = u64::from_ne_bytes(unsafe { *(buffer.as_ptr().add(8) as *const [u8; 8]) });
    low ^ high
}

impl CompressedReadIndipendent {
    #[inline(always)]
    pub unsafe fn get_u64_unchecked_aligned(&self, data: &[u8], base_offset: usize) -> u64 {
        let index = base_offset + self.start;
        unsafe { (data.as_ptr().add(index / 4) as *const u64).read_unaligned() }
    }

    pub fn get_suffix_difference_slow(
        &self,
        other: &Self,
        storage: &[u8],
        offset_self: usize,
        offset_other: usize,
    ) -> (usize, Ordering) {
        let self_slice = self
            .as_reference(storage)
            .sub_slice(offset_self..self.bases_count())
            .to_string();

        let self_slice = self_slice.replace("A", "0");
        let self_slice = self_slice.replace("C", "1");
        let self_slice = self_slice.replace("G", "3");
        let self_slice = self_slice.replace("T", "2");

        let other_slice = other
            .as_reference(storage)
            .sub_slice(offset_other..other.bases_count())
            .to_string();

        let other_slice = other_slice.replace("A", "0");
        let other_slice = other_slice.replace("C", "1");
        let other_slice = other_slice.replace("G", "3");
        let other_slice = other_slice.replace("T", "2");

        if self_slice.starts_with(&other_slice) || other_slice.starts_with(&self_slice) {
            return (usize::MAX, self_slice.len().cmp(&other_slice.len()));
        }

        let min_length = self_slice.len().min(other_slice.len());
        for i in 0..min_length {
            if self_slice.as_bytes()[i] != other_slice.as_bytes()[i] {
                return (i, self_slice.as_bytes()[i].cmp(&other_slice.as_bytes()[i]));
            }
        }
        (min_length, self_slice.len().cmp(&other_slice.len()))
    }

    #[inline(always)]
    pub unsafe fn get_centered_suffix_difference(
        &self,
        other: &Self,
        storage: &[u8],
        offset_self: usize,
        offset_other: usize,
    ) -> (usize, Ordering) {
        unsafe {
            let mut self_ptr = storage.as_ptr().add((self.start + offset_self) / 4);
            let mut other_ptr = storage.as_ptr().add((other.start + offset_other) / 4);

            let self_size = self.size - offset_self;
            let other_size = other.size - offset_other;

            let bases_count = self_size.min(other_size);
            let mut offset = 0;

            while offset < bases_count {
                let first = (self_ptr as *const u64).read_unaligned();
                let second = (other_ptr as *const u64).read_unaligned();
                let remaining_bases = bases_count - offset;
                let remaining_bases_mask: u64 = if remaining_bases >= 32 {
                    u64::MAX
                } else {
                    (1u64 << ((remaining_bases * 2) as u32)) - 1
                };

                let differences = (first ^ second) & remaining_bases_mask;
                if differences != 0 {
                    // Find the first base mismatch
                    let first_diff = differences.trailing_zeros() / 2;
                    let diff_offset = offset + first_diff as usize;

                    let first_diff_mask = differences & (!differences + 1);

                    return (
                        diff_offset.min(bases_count),
                        (first & first_diff_mask)
                            .cmp(&(second & first_diff_mask))
                            .then_with(|| self_size.cmp(&other_size)),
                    );
                }

                offset += 32;
                self_ptr = self_ptr.add(8);
                other_ptr = other_ptr.add(8);
            }

            // In case of equality, prefer smaller sizes first
            (self_size.min(other_size), self_size.cmp(&other_size))
        }
    }

    #[inline(always)]
    pub unsafe fn get_centered_prefix_difference(
        &self,
        other: &Self,
        storage: &[u8],
        offset_self: usize,
        offset_other: usize,
    ) -> (usize, Ordering) {
        unsafe {
            let mut self_ptr = storage.as_ptr().add((self.start + offset_self) / 4).sub(8);
            let mut other_ptr = storage
                .as_ptr()
                .add((other.start + offset_other) / 4)
                .sub(8);

            let bases_count = offset_self.min(offset_other);
            let mut offset = 0;
            while offset < bases_count {
                let first = (self_ptr as *const u64).read_unaligned();
                let second = (other_ptr as *const u64).read_unaligned();
                let remaining_bases = bases_count - offset;
                let remaining_bases_mask: u64 = if remaining_bases >= 32 {
                    u64::MAX
                } else {
                    u64::MAX << ((64 - remaining_bases * 2) as u32)
                };

                let differences = (first ^ second) & remaining_bases_mask;

                if differences != 0 {
                    // Find the first base mismatch
                    let leading_zeros = differences.leading_zeros();
                    let first_diff = leading_zeros / 2;
                    let diff_offset = offset + first_diff as usize;

                    let first_diff_mask = 1 << (63 - leading_zeros);
                    return (
                        diff_offset.min(bases_count),
                        (first & first_diff_mask)
                            .cmp(&(second & first_diff_mask))
                            .then_with(|| offset_self.cmp(&offset_other)),
                    );
                }

                offset += 32;
                self_ptr = self_ptr.sub(8);
                other_ptr = other_ptr.sub(8);
            }

            // In case of equality, prefer smaller sizes first
            (
                offset_self.min(offset_other),
                offset_self.cmp(&offset_other),
            )
        }
    }

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

    pub fn from_read<const ALIGNED: bool>(
        read: &CompressedRead,
        storage: &mut Vec<u8>,
    ) -> CompressedReadIndipendent {
        let start = storage.len() * 4 + if ALIGNED { 0 } else { read.start as usize };
        storage.extend_from_slice(read.get_packed_slice());
        CompressedReadIndipendent {
            start,
            size: read.size,
        }
    }

    #[inline(always)]
    pub unsafe fn compute_hash_aligned_overflow16(
        &self,
        storage: &[u8],
        start_offset: usize,
        last_base: usize,
    ) -> u64 {
        use std::hash::BuildHasher;

        let slice = self.get_packed_slice_aligned(storage);

        let mut hasher = FxBuildHasher.build_hasher();
        hasher
            .write_u64(unsafe { get_hash_repr_aligned_overflow16(slice, start_offset, last_base) });
        hasher.finish()
    }

    #[inline(always)]
    pub fn get_packed_slice_aligned(&self, buffer: &[u8]) -> &[u8] {
        // debug_assert_eq!(self.start % 4, 0);
        unsafe {
            from_raw_parts(
                buffer.as_ptr().add(self.buffer_start_index()),
                (self.size + self.start % 4 + 3) / 4,
            )
        }
    }

    #[inline]
    pub fn sub_slice(&self, range: Range<usize>) -> Self {
        assert!(range.start <= range.end);

        CompressedReadIndipendent {
            size: range.end - range.start,
            start: self.start + range.start,
        }
    }

    #[inline]
    pub fn as_reference<'a>(&self, storage: &'a [u8]) -> CompressedRead<'a> {
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

type AlignmentWordType = u64;
const ALIGNMENT_WORD_SIZE: usize = size_of::<AlignmentWordType>();
const ALIGNMENT_WORD_BITS: usize = ALIGNMENT_WORD_SIZE * 8;

impl<'a> CompressedRead<'a> {
    const WINDOW_DUPLICATE_SENTINEL: u64 = u64::MAX >> 16;

    #[inline(always)]
    pub unsafe fn compute_hash_aligned_overflow16(&self) -> u64 {
        use std::hash::BuildHasher;

        let slice = self.get_packed_slice();

        let mut hasher = FxBuildHasher.build_hasher();
        hasher.write_u64(unsafe {
            get_hash_repr_aligned_overflow16(
                slice,
                self.start as usize,
                self.bases_count() + self.start as usize,
            )
        });
        hasher.finish()
    }

    pub fn equality_compare_start_zero(&self, other: &Self) -> bool {
        debug_assert_eq!(self.start, 0);
        debug_assert_eq!(other.start, 0);
        debug_assert!(self.size != 0);
        let complete_bytes_count = self.size / 4;
        let last_byte_index = (self.size + 3) / 4 - 1;
        let last_byte_mask = 0xFFu8.wrapping_shr((self.size as u32 % 4) * 2);
        unsafe {
            from_raw_parts(self.data, complete_bytes_count)
                == from_raw_parts(other.data, complete_bytes_count)
                && (*self.data.add(last_byte_index) & last_byte_mask
                    == *other.data.add(last_byte_index) & last_byte_mask)
        }
    }

    #[inline(always)]
    pub fn encode_length<MinimizerMode: MinimizerModeOption, FlagsCount: typenum::Unsigned>(
        buffer: &mut Vec<u8>,
        length: usize,
        min_size: usize,
        mut minimizer_position: u16,
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
                );
                // The minimizer position is encoded separately as it could be bigger than the maximum allowed in normal reads
                encode_varint::<_>(|b| buffer.extend_from_slice(b), minimizer_position as u64);
                minimizer_position = 0;
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
    fn offset_read(required_offset: usize, slice: &mut [u8], size: usize) {
        let bit_offset = required_offset * 2;
        let last_start =
            ((required_offset + size + 3) / 4 / ALIGNMENT_WORD_SIZE) * ALIGNMENT_WORD_SIZE;

        unsafe {
            let start_pointer = slice.as_mut_ptr();

            let mut last_align_word = start_pointer.add(last_start) as *mut AlignmentWordType;
            let mut last_align_word_val = std::ptr::read_unaligned(last_align_word) << bit_offset;
            while last_align_word as usize != start_pointer as usize {
                let prev = last_align_word.sub(1);
                let prev_val = std::ptr::read_unaligned(prev);
                let current = last_align_word;
                std::ptr::write_unaligned(
                    current,
                    last_align_word_val | prev_val >> (ALIGNMENT_WORD_BITS - bit_offset),
                );

                last_align_word_val = prev_val << bit_offset;
                last_align_word = prev;
            }
            std::ptr::write_unaligned(start_pointer as *mut AlignmentWordType, last_align_word_val);
        }
    }

    #[inline(always)]
    pub fn read_from_stream<
        S: Read,
        MinimizerMode: MinimizerModeOption,
        FlagsCount: typenum::Unsigned,
        AlignMode: AlignModeOption,
    >(
        temp_buffer: &'a mut Vec<u8>,
        stream: &mut S,
        min_size: usize,
        min_size_log: u8,
    ) -> Option<(Self, u16, u8, bool)> {
        let (mut encoded_size, mut flags) =
            decode_varint_flags::<_, FlagsCount>(|| stream.read_u8().ok())?;

        let (size, minimizer_pos, is_window_duplicate, required_offset) = if MinimizerMode::ENABLED
        {
            #[cold]
            fn cold() {}

            let is_window_duplicate = encoded_size == Self::WINDOW_DUPLICATE_SENTINEL;
            let minimizer_pos = if is_window_duplicate {
                let extra_minimizer_pos = decode_varint(|| stream.read_u8().ok())?;
                cold();
                (encoded_size, flags) =
                    decode_varint_flags::<_, FlagsCount>(|| stream.read_u8().ok())?;
                extra_minimizer_pos
            } else {
                encoded_size & ((1 << min_size_log) - 1)
            };

            let size = encoded_size >> min_size_log;
            (
                size as usize + min_size,
                minimizer_pos as u16,
                is_window_duplicate,
                /*
                0 => 0   |****| => |****|
                1 => 3    |x***|* => |xxxx|****|
                2 => 2
                3 => 1
                xor the second bit with the first one's value, so that 3 becomes 1 and 1 becomes 3
                 */
                ((minimizer_pos ^ (minimizer_pos << 1)) % 4) as usize, // Offset required to align the minimizer start to byte boundaries
            )
        } else {
            (encoded_size as usize + min_size, 0, false, 0)
        };

        let bytes = (size + 3) / 4;
        let total_bytes = (size + required_offset + 3) / 4;

        let overwrite_offset = if AlignMode::ENABLED {
            ALIGNMENT_WORD_SIZE
        } else {
            0
        }
        .max(AlignMode::OVERREAD_MIN);
        temp_buffer.reserve(total_bytes + overwrite_offset);
        let buffer_start = temp_buffer.len();
        unsafe {
            temp_buffer.set_len(buffer_start + total_bytes);
        }

        stream
            .read_exact(&mut temp_buffer[buffer_start..(buffer_start + bytes)])
            .unwrap();

        if AlignMode::ENABLED {
            if required_offset > 0 {
                Self::offset_read(required_offset, &mut temp_buffer[buffer_start..], size);
            }
        }

        Some((
            if AlignMode::ENABLED {
                CompressedRead::new_offset(&temp_buffer[buffer_start..], required_offset, size)
            } else {
                CompressedRead::new_from_compressed(&temp_buffer[buffer_start..], size)
            },
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

    #[inline(always)]
    pub fn get_packed_slice(&self) -> &'a [u8] {
        unsafe { from_raw_parts(self.data, (self.size + self.start as usize + 3) / 4) }
    }

    // fn get_hash_repr(&self) -> u64 {
    //     let mut buffer = [0; 16];

    //     let final_offset = (self.size ^ (self.size << 1)) % 4;
    //     let mask = u8::MAX >> (final_offset * 2);
    //     let mut index = 0;

    //     if self.start == 0 || self.size == 0 {
    //         let slice = self.get_packed_slice();
    //         for chunk in slice.chunks(16) {
    //             let value = u128::from_ne_bytes(unsafe { *(chunk.as_ptr() as *const [u8; 16]) });
    //             buffer = (u128::from_ne_bytes(buffer) ^ value).to_ne_bytes();
    //         }
    //         let reminder = slice.len() % 16;
    //         let remaining = &slice[slice.len() - reminder..];
    //         buffer[..reminder]
    //             .iter_mut()
    //             .zip(remaining)
    //             .for_each(|(b, v)| *b ^= v);
    //         index = (slice.len() + 15) % 16;
    //     } else {
    //         let bytes_count = (self.size + 3) / 4;
    //         unsafe {
    //             let right_offset = self.start * 2;
    //             let left_offset = 8 - right_offset;

    //             for b in 1..self.get_packed_slice().len() {
    //                 let current = *self.data.add(b - 1);
    //                 let next = *self.data.add(b);

    //                 let value = (current >> right_offset) | (next << left_offset);
    //                 buffer[index] ^= value;
    //                 index = (index + 1) % 16;
    //             }
    //             // Handle the last one separately
    //             buffer[index] = *self.data.add(bytes_count - 1) >> right_offset;
    //         }
    //     }
    //     // Mask the last byte to ensure byte equality between partial compressions
    //     buffer[index] &= mask;

    //     let low = u64::from_ne_bytes(unsafe { *(buffer.as_ptr() as *const [u8; 8]) });
    //     let high = u64::from_ne_bytes(unsafe { *(buffer.as_ptr().add(8) as *const [u8; 8]) });
    //     low ^ high
    // }

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

    // pub fn get_hash(&self) -> u64 {
    //     use std::hash::{BuildHasher, Hasher};
    //     let mut hasher = FxBuildHasher::default().build_hasher();
    //     self.hash(&mut hasher);
    //     let hash = hasher.finish();
    //     hash
    // }

    // pub fn get_hash_aligned(&self) -> u64 {
    //     debug_assert_eq!(self.start, 0);
    //     use std::hash::{BuildHasher, Hasher};
    //     let mut hasher = FxBuildHasher::default().build_hasher();
    //     self.hash(&mut hasher);
    //     let hash = hasher.finish();
    //     hash
    // }

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

    pub fn as_bases_iter(&'a self) -> impl ExactSizeIterator<Item = u8> + 'a {
        (0..self.size).map(move |i| unsafe { Utils::decompress_base(self.get_base_unchecked(i)) })
    }

    pub fn as_reverse_complement_bases_iter(&'a self) -> impl ExactSizeIterator<Item = u8> + 'a {
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
    fn subslice(&self, start: usize, end: usize) -> Self {
        self.sub_slice(start..end)
    }

    #[inline(always)]
    unsafe fn get_unchecked_cbase(&self, index: usize) -> u8 {
        unsafe { self.get_base_unchecked(index) }
    }

    #[inline(always)]
    fn bases_count(&self) -> usize {
        self.size
    }
}

#[cfg(test)]
mod tests {
    use std::{cmp::Ordering, io::Cursor};

    use crate::{
        compressed_read::{
            ALIGNMENT_WORD_SIZE, CompressedRead, CompressedReadIndipendent,
            get_hash_repr_aligned_overflow16,
        },
        concurrent::temp_reads::creads_utils::{
            AssemblerMinimizerPosition, CompressedReadsBucketData, NoAlignment, ReadData,
        },
    };

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
    fn test_offset_read() {
        let mut buffer = vec![];
        let bases = b"ACGTACGCGGTAGCTAAGCATCGATGCCGATCGTGTTTAACCATG";

        CompressedRead::compress_from_plain_rc(bases, |b| buffer.extend_from_slice(b));
        let before = CompressedRead::from_compressed_reads(&buffer, 0, bases.len()).to_string();

        for offset in 1..3 {
            let mut buffer = buffer.clone();
            buffer.reserve(ALIGNMENT_WORD_SIZE);
            CompressedRead::offset_read(offset, &mut buffer, bases.len());

            let final_val = CompressedRead::new_offset(&buffer, offset, bases.len()).to_string();

            if before != final_val {
                panic!(
                    "Alignment mismatch: before = {}, after = {} required offset: {}",
                    before, final_val, offset
                );
            }
        }
    }

    #[test]
    fn test_suffix_difference_check() {
        let mut buffer = vec![];
        let bases1 = b"GAGCAAGTCCTTGCTCTCACTC";
        let bases2 = b"GAGCAAGTCCTG";

        let first = CompressedReadIndipendent::from_plain(bases1, &mut buffer);
        let second = CompressedReadIndipendent::from_plain(bases2, &mut buffer);
        buffer.reserve(ALIGNMENT_WORD_SIZE);

        let result = unsafe { first.get_centered_suffix_difference(&second, &buffer, 0, 0) };
        assert_eq!(result, (11, Ordering::Less))
    }

    #[test]
    fn test_prefix_difference_check() {
        let mut buffer = vec![];
        let bases1 = b"ACGTACGCAGTAGCTAATCATCGATGCCGATCACGTACGCGGTAGCTAATCATCGATGCCGATC";
        let bases2 = b"AAAAACGTACGCAGTAGCTAATCATCGATGCCGATCACGTACGCGGTAGCTAATCATCGATGCCGATC";

        let first = CompressedReadIndipendent::from_plain(bases1, &mut buffer);
        let second = CompressedReadIndipendent::from_plain(bases2, &mut buffer);
        buffer.reserve(ALIGNMENT_WORD_SIZE);

        let result = unsafe { first.get_centered_prefix_difference(&second, &buffer, 4, 8) };
        assert_eq!(result, (usize::MAX, Ordering::Less))
    }

    #[test]
    fn test_unsafe_hash() {
        let mut buffer = Vec::with_capacity(64 + 16);

        let mut hashes1 = vec![];
        for i in 0..64u8 {
            buffer.push((i.wrapping_add(7)).wrapping_mul(117));
            let hash = unsafe { get_hash_repr_aligned_overflow16(&buffer, buffer.len() * 4) };
            println!("Hash{}: {}", i, hash);
            hashes1.push(hash);
        }
        buffer.fill(123);
        buffer.clear();
        for i in 0..64u8 {
            buffer.push((i.wrapping_add(7)).wrapping_mul(117));
            let hash = unsafe { get_hash_repr_aligned_overflow16(&buffer, buffer.len() * 4) };
            println!("Hash{}: {}", i, hash);
            assert_eq!(hashes1[i as usize], hash);
        }
    }

    #[test]
    fn test_unsafe_hash2() {
        let read1 = "CGATCATATCACCGGCAATGAGAATCCCATCCGGTCTGTACCG";
        let read2 = "ATTTGAGGAAACGTAAAATGAGAATCCATAAGGCAAAGGAAAA";
        let offset = 16;

        let mut buffer = Vec::with_capacity(64 + 16);

        CompressedRead::compress_from_plain(read1.as_bytes(), |b| buffer.extend_from_slice(b));

        let start2 = buffer.len();
        CompressedRead::compress_from_plain(read2.as_bytes(), |b| buffer.extend_from_slice(b));
        let compressed_read1 = CompressedRead::from_compressed_reads(&buffer, 0, read1.len());
        let compressed_read2 =
            CompressedRead::from_compressed_reads(&buffer, start2 * 4, read2.len());

        println!("CR1: {}", compressed_read1.sub_slice(16..27).to_string());
        println!("CR2: {}", compressed_read2.sub_slice(16..27).to_string());

        let (hash1, hash2) = unsafe {
            (
                compressed_read1
                    .sub_slice(16..27)
                    .compute_hash_aligned_overflow16(),
                compressed_read2
                    .sub_slice(16..27)
                    .compute_hash_aligned_overflow16(),
            )
        };

        println!("CR1H: {}", hash1);
        println!("CR2H: {}", hash2);

        assert_eq!(hash1, hash2);
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

    #[test]
    #[ignore]
    fn check_read_deserialization() {
        // Error while encoding 100066 27/5! MM: ggcat_io::concurrent::temp_reads::creads_utils::AssemblerMinimizerPosition
        // FC: typenum::uint::UInt<typenum::uint::UInt<typenum::uint::UTerm, typenum::bit::B1>, typenum::bit::B0>
        // read_size: 99 read:  flags: 3 minimi_pos: 80 iwd: false mult: 1

        let plain = b"CAGACAAACAGACAAACAGACAAACAGACAAACAGACAAACAGACAAACAGACAAACAGACAAACAGACAAACAGACAAACAGACAAACAGACAAACAG";
        let mut tmp_buf = vec![];
        CompressedRead::compress_from_plain(plain, |b| tmp_buf.extend_from_slice(b));
        let read = unsafe { CompressedReadIndipendent::from_start_buffer_position(0, plain.len()) };
        let read = read.as_reference(&tmp_buf);

        assert_eq!(read.to_string().as_bytes(), plain);

        let element = CompressedReadsBucketData {
            read: ReadData::Packed(read),
            multiplicity: 1,
            minimizer_pos: 80,
            extra_bucket: 0,
            flags: 3,
            is_window_duplicate: false,
        };
        let mut tbuffer = vec![];

        {
            let is_rc = matches!(element.read, ReadData::PackedRc(_));
            CompressedRead::encode_length::<AssemblerMinimizerPosition, typenum::U2>(
                &mut tbuffer,
                read.size,
                27,
                element.minimizer_pos,
                5,
                element.flags,
                element.is_window_duplicate,
            );

            if is_rc {
                read.copy_to_buffer_rc(&mut tbuffer);
            } else {
                read.copy_to_buffer(&mut tbuffer);
            }
        }

        let input = std::fs::read("/tmp/error100066.dat").unwrap();
        assert_eq!(tbuffer, input);
        let mut tmp_buffer = vec![];
        println!("Input len: {}", input.len());
        let decompressed = CompressedRead::read_from_stream::<
            _,
            crate::concurrent::temp_reads::creads_utils::AssemblerMinimizerPosition,
            typenum::U2,
            NoAlignment,
        >(&mut tmp_buffer, &mut Cursor::new(&input), 27, 5);
        assert!(decompressed.is_some());
    }
}
