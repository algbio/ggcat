//! Varint primitives shared by every bucket format.
//!
//! Both directions are written around the case that dominates every caller: a
//! value below 128, which is a single byte. Past that, an encoder with eight
//! bytes of room and a decoder that can see eight bytes ahead work a machine
//! word at a time rather than a byte at a time, which is what [`scatter`] and
//! [`decode_varint_word`] are for. The workspace builds release at opt-level 1,
//! so the byte loops these replace really are loops there.

use std::mem::MaybeUninit;

pub const VARINT_FLAGS_MAX_SIZE: usize = 10;
pub const VARINT_MAX_SIZE: usize = 9;

/// The continuation bit of every byte of a word.
const CONTINUATION: u64 = 0x8080_8080_8080_8080;

/// The largest value that still fits in the eight byte fast paths.
const WORD_MAX: u64 = 1 << 56;

/// Number of bytes the varint encoding of `value` occupies.
#[inline(always)]
const fn varint_len(value: u64) -> usize {
    // One seven-bit group per byte, and zero still takes a byte.
    let bits = 64 - value.leading_zeros() as usize;
    if bits == 0 { 1 } else { bits.div_ceil(7) }
}

/// Spreads the seven-bit groups of `value` into one byte each, the inverse of
/// the fold in [`decode_varint_word`]. Only defined below [`WORD_MAX`], where
/// the groups still fit in eight bytes.
#[inline(always)]
const fn scatter(value: u64) -> u64 {
    let mut x = value;
    x = (x & 0x0000_0000_0FFF_FFFF) | ((x & 0x00FF_FFFF_F000_0000) << 4);
    x = (x & 0x0000_3FFF_0000_3FFF) | ((x & 0x0FFF_C000_0FFF_C000) << 2);
    x = (x & 0x007F_007F_007F_007F) | ((x & 0x3F80_3F80_3F80_3F80) << 1);
    x
}

/// The continuation bit of every byte but the last of a `size` byte varint.
#[inline(always)]
const fn continuation_mask(size: usize) -> u64 {
    CONTINUATION & !(u64::MAX << ((size - 1) * 8))
}

/// The general encoder, for the values too wide for a single word.
#[inline(always)]
#[allow(clippy::uninit_assumed_init)]
fn encode_varint_wide<T>(write_bytes: impl FnOnce(&[u8]) -> T, mut value: u64) -> T {
    #[allow(invalid_value)]
    let mut bytes: [u8; VARINT_MAX_SIZE] = unsafe { MaybeUninit::uninit().assume_init() };
    let mut index = 0;
    while index < bytes.len() {
        let rem = ((value > 127) as u8) << 7;
        bytes[index] = ((value as u8) & 0b1111111) | rem;
        value >>= 7;
        index += 1;
        if value == 0 {
            break;
        }
    }
    write_bytes(&bytes[..index])
}

#[inline(always)]
pub fn encode_varint<T>(write_bytes: impl FnOnce(&[u8]) -> T, value: u64) -> T {
    if value < 0x80 {
        // The common case everywhere: lengths, indices and color deltas are
        // nearly always one byte.
        return write_bytes(&[value as u8]);
    }
    if value < WORD_MAX {
        // Lay the whole varint out in one word and store it at once, instead of
        // a compare and a store per byte.
        let size = varint_len(value);
        let word = scatter(value) | continuation_mask(size);
        return write_bytes(&word.to_le_bytes()[..size]);
    }
    encode_varint_wide(write_bytes, value)
}

/// Appends a varint to a buffer, which is what every bucket serializer writes
/// into. The one byte case is a `push`: no slice, no length, no `Result`.
#[inline(always)]
pub fn encode_varint_to_vec(out: &mut Vec<u8>, value: u64) {
    if value < 0x80 {
        out.push(value as u8);
    } else {
        encode_varint(|bytes| out.extend_from_slice(bytes), value);
    }
}

#[inline(always)]
#[allow(clippy::uninit_assumed_init)]
pub fn encode_varint_flags<T, F: FnOnce(&[u8]) -> T, FlagsCount: typenum::Unsigned>(
    write_bytes: F,
    mut value: u64,
    flags: u8,
) -> T {
    let useful_first_bits: usize = 8 - FlagsCount::to_usize();
    let first_byte_max_value: u8 = ((1u16 << (useful_first_bits - 1)) - 1) as u8;
    let flags_bits = ((flags as u16) << useful_first_bits) as u8;

    if value <= first_byte_max_value as u64 {
        // The whole value rides in the flags byte, which is the usual case for
        // a read length that fits.
        return write_bytes(&[flags_bits | (value as u8)]);
    }

    #[allow(invalid_value)]
    let mut bytes: [u8; VARINT_FLAGS_MAX_SIZE] = unsafe { MaybeUninit::uninit().assume_init() };

    let fr_rem = 1u8 << (useful_first_bits - 1);
    bytes[0] = flags_bits | (value as u8 & first_byte_max_value) | fr_rem;

    value >>= useful_first_bits - 1;
    let mut index = 1;

    while index < bytes.len() {
        if value == 0 {
            break;
        }
        let rem = ((value > 127) as u8) << 7;
        bytes[index] = ((value as u8) & 0b1111111) | rem;
        value >>= 7;
        index += 1;
    }
    write_bytes(&bytes[..index])
}

#[inline(always)]
pub fn decode_varint_flags<F: FnMut() -> Option<u8>, FlagsCount: typenum::Unsigned>(
    mut read_byte: F,
) -> Option<(u64, u8)> {
    let first_byte = read_byte()?;

    let useful_first_bits: usize = 8 - FlagsCount::to_usize();
    let first_byte_max_value: u8 = ((1u16 << (useful_first_bits - 1)) - 1) as u8;

    let flags = ((first_byte as u16) >> useful_first_bits) as u8;
    let mut result = (first_byte & first_byte_max_value) as u64;
    if first_byte & (1 << (useful_first_bits - 1)) == 0 {
        return Some((result, flags));
    }

    let mut offset = useful_first_bits - 1;
    loop {
        let value = read_byte()?;
        result |= ((value & 0b1111111) as u64) << offset;
        if value & 0b10000000 == 0 {
            break;
        }
        offset += 7;
    }
    Some((result, flags))
}

#[inline(always)]
pub fn decode_varint(mut read_byte: impl FnMut() -> Option<u8>) -> Option<u64> {
    let first = read_byte()?;
    if first < 0x80 {
        return Some(first as u64);
    }
    let mut result = (first & 0b1111111) as u64;
    let mut offset = 7u32;
    loop {
        let value = read_byte()?;
        result |= ((value & 0b1111111) as u64) << offset;
        if value & 0b10000000 == 0 {
            break;
        }
        offset += 7;
    }
    Some(result)
}

/// Decodes the varint that starts at the low end of `word`, as a mask and a
/// fold instead of one unpredictable branch per byte. Returns the value and the
/// number of bytes it occupied, or `None` when the word holds no terminator, so
/// that the caller falls back to the byte-at-a-time decoder.
#[inline(always)]
pub fn decode_varint_word(word: u64) -> Option<(u64, usize)> {
    let terminators = !word & CONTINUATION;
    if terminators == 0 {
        // Eight continuation bytes in a row: a varint this wide is rare enough
        // that the general decoder can have it.
        return None;
    }
    let size = (terminators.trailing_zeros() >> 3) as usize + 1;
    // Drop the bytes past the terminator and every continuation bit, then fold
    // the surviving seven-bit groups together, halving the gaps each round.
    let mut value = word & (u64::MAX >> (64 - (size << 3))) & !CONTINUATION;
    value = (value & 0x007F_007F_007F_007F) | ((value & 0x7F00_7F00_7F00_7F00) >> 1);
    value = (value & 0x0000_3FFF_0000_3FFF) | ((value & 0x3FFF_0000_3FFF_0000) >> 2);
    value = (value & 0x0000_0000_0FFF_FFFF) | ((value & 0x0FFF_FFFF_0000_0000) >> 4);
    Some((value, size))
}

/// Source of the bytes of one encoded record.
///
/// [`SliceVarintSource`] is the one worth reaching for: owning the bytes is
/// what lets it read a word at a time. The other two exist for the entry points
/// that can only hand over one byte, and fall back to the byte loops.
pub trait VarintSource {
    fn next_byte(&mut self) -> Option<u8>;

    #[inline(always)]
    fn next_varint(&mut self) -> Option<u64> {
        decode_varint(|| self.next_byte())
    }

    #[inline(always)]
    fn next_varint_flags<FlagsCount: typenum::Unsigned>(&mut self) -> Option<(u64, u8)> {
        decode_varint_flags::<_, FlagsCount>(|| self.next_byte())
    }
}

/// Reads from a buffer that already holds the whole record.
pub struct SliceVarintSource<'a> {
    bytes: &'a [u8],
    position: usize,
}

impl<'a> SliceVarintSource<'a> {
    #[inline(always)]
    pub fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, position: 0 }
    }

    /// How many bytes have been consumed, so a caller can walk to the next
    /// record.
    #[inline(always)]
    pub fn position(&self) -> usize {
        self.position
    }

    #[inline(always)]
    pub fn remaining(&self) -> &'a [u8] {
        &self.bytes[self.position..]
    }
}

impl VarintSource for SliceVarintSource<'_> {
    #[inline(always)]
    fn next_byte(&mut self) -> Option<u8> {
        let byte = *self.bytes.get(self.position)?;
        self.position += 1;
        Some(byte)
    }

    #[inline(always)]
    fn next_varint(&mut self) -> Option<u64> {
        let first = *self.bytes.get(self.position)?;
        if first < 0x80 {
            self.position += 1;
            return Some(first as u64);
        }
        if self.bytes.len() - self.position >= 8 {
            // SAFETY: eight bytes remain from `position`; the load is unaligned.
            let word = u64::from_le_bytes(unsafe {
                self.bytes
                    .as_ptr()
                    .add(self.position)
                    .cast::<[u8; 8]>()
                    .read_unaligned()
            });
            if let Some((value, size)) = decode_varint_word(word) {
                self.position += size;
                return Some(value);
            }
        }
        decode_varint(|| self.next_byte())
    }
}

/// Reads from a buffer whose end is not known.
pub struct PointerVarintSource {
    pointer: *const u8,
}

impl PointerVarintSource {
    /// # Safety
    /// A whole encoded record must start at `pointer`.
    #[inline(always)]
    pub unsafe fn new(pointer: *const u8) -> Self {
        Self { pointer }
    }

    #[inline(always)]
    pub fn pointer(&self) -> *const u8 {
        self.pointer
    }
}

impl VarintSource for PointerVarintSource {
    #[inline(always)]
    fn next_byte(&mut self) -> Option<u8> {
        // SAFETY: guaranteed by the caller of `new`.
        unsafe {
            let byte = *self.pointer;
            self.pointer = self.pointer.add(1);
            Some(byte)
        }
    }
}

/// Reads from a buffered stream, taking the look-ahead straight out of the
/// buffer the reader already keeps. This is the way to get the word-at-a-time
/// decode on a stream: a bare `Read` cannot be looked ahead of without
/// consuming, but `fill_buf` hands over the bytes without committing to them.
pub struct BufVarintSource<'a, R: std::io::BufRead> {
    reader: &'a mut R,
}

impl<'a, R: std::io::BufRead> BufVarintSource<'a, R> {
    #[inline(always)]
    pub fn new(reader: &'a mut R) -> Self {
        Self { reader }
    }
}

impl<R: std::io::BufRead> VarintSource for BufVarintSource<'_, R> {
    #[inline(always)]
    fn next_byte(&mut self) -> Option<u8> {
        let byte = *self.reader.fill_buf().ok()?.first()?;
        self.reader.consume(1);
        Some(byte)
    }

    #[inline(always)]
    fn next_varint(&mut self) -> Option<u64> {
        let buffer = self.reader.fill_buf().ok()?;
        let first = *buffer.first()?;
        if first < 0x80 {
            self.reader.consume(1);
            return Some(first as u64);
        }
        if buffer.len() >= 8 {
            // SAFETY: eight bytes are buffered; the load is unaligned.
            let word =
                u64::from_le_bytes(unsafe { buffer.as_ptr().cast::<[u8; 8]>().read_unaligned() });
            if let Some((value, size)) = decode_varint_word(word) {
                self.reader.consume(size);
                return Some(value);
            }
        }
        // A varint straddling the end of the buffer falls back to bytes, which
        // refill as they go. It happens once per buffer, not once per value.
        decode_varint(|| self.next_byte())
    }
}

/// Reads from a stream, one call per byte. Nothing here can look ahead, because
/// a record is only delimited by its own contents.
pub struct ReadVarintSource<'a, R: std::io::Read> {
    reader: &'a mut R,
}

impl<'a, R: std::io::Read> ReadVarintSource<'a, R> {
    #[inline(always)]
    pub fn new(reader: &'a mut R) -> Self {
        Self { reader }
    }
}

impl<R: std::io::Read> VarintSource for ReadVarintSource<'_, R> {
    #[inline(always)]
    fn next_byte(&mut self) -> Option<u8> {
        let mut byte = 0u8;
        self.reader
            .read_exact(std::slice::from_mut(&mut byte))
            .ok()?;
        Some(byte)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use byteorder::ReadBytesExt;
    use std::io::{Cursor, Write};

    /// The encoder before the word fast paths, kept as the definition of the
    /// on-disk format: the new one has to agree with it byte for byte, or every
    /// bucket already on disk becomes unreadable.
    fn reference_encode(mut value: u64) -> Vec<u8> {
        let mut bytes = [0u8; VARINT_MAX_SIZE];
        let mut index = 0;
        while index < bytes.len() {
            let rem = ((value > 127) as u8) << 7;
            bytes[index] = ((value as u8) & 0b1111111) | rem;
            value >>= 7;
            index += 1;
            if value == 0 {
                break;
            }
        }
        bytes[..index].to_vec()
    }

    fn reference_decode(bytes: &[u8]) -> Option<(u64, usize)> {
        let mut result = 0;
        let mut offset = 0u32;
        let mut index = 0;
        loop {
            let value = *bytes.get(index)?;
            index += 1;
            let next = (value & 0b10000000) != 0;
            result |= ((value & 0b1111111) as u64) << offset;
            if !next {
                break;
            }
            offset += 7;
        }
        Some((result, index))
    }

    /// Values chosen to land on every varint length and on both sides of each
    /// boundary, plus a spread of arbitrary ones.
    fn corpus() -> Vec<u64> {
        let mut values = vec![0, 1, 127, 128, u64::MAX];
        for shift in 0..64 {
            let base = 1u64 << shift;
            values.extend([base - 1, base, base + 1]);
        }
        let mut state = 0x243F_6A88_85A3_08D3u64;
        for _ in 0..20000 {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            values.push(state);
            values.push(state >> (state % 64));
        }
        values
    }

    /// The buffer targeted encoder is a second implementation of the same
    /// format, so it has to agree with the reference byte for byte too.
    #[test]
    fn vec_encoder_matches_the_reference() {
        for value in corpus() {
            let mut encoded = Vec::new();
            encode_varint_to_vec(&mut encoded, value);
            assert_eq!(encoded, reference_encode(value), "value {value:#x}");
        }
        // Appending must not disturb what is already in the buffer.
        let mut appended = vec![0xAA, 0xBB];
        encode_varint_to_vec(&mut appended, 300);
        assert_eq!(&appended[..2], &[0xAA, 0xBB]);
        assert_eq!(&appended[2..], &reference_encode(300)[..]);
    }

    #[test]
    fn encoding_matches_the_on_disk_format_exactly() {
        for value in corpus() {
            let mut encoded = Vec::new();
            encode_varint(|b| encoded.extend_from_slice(b), value);
            assert_eq!(encoded, reference_encode(value), "value {value:#x}");
            assert!(encoded.len() <= VARINT_MAX_SIZE);
        }
    }

    #[test]
    fn every_decoder_agrees_with_the_reference() {
        for value in corpus() {
            let encoded = reference_encode(value);
            let Some((expected, length)) = reference_decode(&encoded) else {
                continue;
            };

            let mut cursor = Cursor::new(&encoded);
            assert_eq!(
                decode_varint(|| cursor.read_u8().ok()),
                Some(expected),
                "value {value:#x}"
            );

            // Once with the eight byte look-ahead available, once without, so
            // both the word path and the fallback are covered.
            for padding in [16, 0] {
                let mut padded = encoded.clone();
                padded.resize(encoded.len() + padding, 0xAA);
                let mut source = SliceVarintSource::new(&padded);
                assert_eq!(source.next_varint(), Some(expected), "value {value:#x}");
                assert_eq!(source.position(), length, "value {value:#x}");
            }

            let mut source = unsafe { PointerVarintSource::new(encoded.as_ptr()) };
            assert_eq!(source.next_varint(), Some(expected), "value {value:#x}");

            let mut stream = encoded.as_slice();
            let mut source = ReadVarintSource::new(&mut stream);
            assert_eq!(source.next_varint(), Some(expected), "value {value:#x}");

            // Buffered, both with the whole value in the buffer and with the
            // buffer cut short so the byte fallback has to refill.
            for capacity in [16, 1] {
                let mut buffered = std::io::BufReader::with_capacity(capacity, encoded.as_slice());
                let mut source = BufVarintSource::new(&mut buffered);
                assert_eq!(source.next_varint(), Some(expected), "value {value:#x}");
                assert_eq!(
                    std::io::BufRead::fill_buf(&mut buffered).unwrap().len(),
                    0,
                    "value {value:#x} left bytes unconsumed at capacity {capacity}"
                );
            }
        }
    }

    #[test]
    fn truncated_input_is_rejected_rather_than_read_past() {
        // A lone continuation byte has no terminator in either decoder.
        assert_eq!(decode_varint(|| None), None);
        let mut source = SliceVarintSource::new(&[0x80]);
        assert_eq!(source.next_varint(), None);
        let mut source = SliceVarintSource::new(&[0x80; 7]);
        assert_eq!(source.next_varint(), None);
        let mut source = SliceVarintSource::new(&[0x80; 9]);
        assert_eq!(source.next_varint(), None);
        let mut empty = &[][..];
        let mut source = ReadVarintSource::new(&mut empty);
        assert_eq!(source.next_varint(), None);
        let mut buffered = std::io::BufReader::new(&[0x80u8; 3][..]);
        let mut source = BufVarintSource::new(&mut buffered);
        assert_eq!(source.next_varint(), None);
    }

    #[test]
    fn varints() {
        let mut result: Vec<u8> = vec![];

        for i in 0..100000 {
            result.clear();
            encode_varint(|b| result.write_all(b), i).unwrap();
            let mut cursor = Cursor::new(&result);
            assert_eq!(
                i,
                decode_varint(|| Some(cursor.read_u8().unwrap())).unwrap()
            );
        }
    }

    #[test]
    fn varints_flags() {
        let mut result: Vec<u8> = vec![];

        for i in 0..100000 {
            result.clear();
            encode_varint_flags::<_, _, typenum::U2>(|b| result.write_all(b), i, (i % 4) as u8)
                .unwrap();
            let mut cursor = Cursor::new(&result);
            assert_eq!(
                (i, (i % 4) as u8),
                decode_varint_flags::<_, typenum::U2>(|| Some(cursor.read_u8().unwrap())).unwrap()
            );
        }
    }

    /// The flags encoder has the same obligation to the on-disk format as the
    /// plain one, for every flag width the codebase uses.
    #[test]
    fn flag_encoding_round_trips_at_every_width() {
        fn check<FlagsCount: typenum::Unsigned>(value: u64, flags: u8) {
            let flags = flags & ((1u16 << FlagsCount::to_usize()) - 1) as u8;
            let mut encoded = Vec::new();
            encode_varint_flags::<_, _, FlagsCount>(|b| encoded.extend_from_slice(b), value, flags);
            assert!(encoded.len() <= VARINT_FLAGS_MAX_SIZE);
            let mut cursor = Cursor::new(&encoded);
            assert_eq!(
                decode_varint_flags::<_, FlagsCount>(|| cursor.read_u8().ok()),
                Some((value, flags)),
                "value {value:#x} flags {flags}"
            );
        }
        for value in corpus().into_iter().filter(|v| *v < 1 << 56) {
            check::<typenum::U0>(value, 0);
            check::<typenum::U1>(value, 1);
            check::<typenum::U2>(value, 3);
            check::<typenum::U3>(value, 5);
        }
    }
}
