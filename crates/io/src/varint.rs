use std::mem::MaybeUninit;

pub const VARINT_FLAGS_MAX_SIZE: usize = 10;
pub const VARINT_MAX_SIZE: usize = 9;

#[inline(always)]
#[allow(clippy::uninit_assumed_init)]
pub fn encode_varint<T>(write_bytes: impl FnOnce(&[u8]) -> T, mut value: u64) -> T {
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
#[allow(clippy::uninit_assumed_init)]
pub fn encode_varint_flags<T, F: FnOnce(&[u8]) -> T, FlagsCount: typenum::Unsigned>(
    write_bytes: F,
    mut value: u64,
    flags: u8,
) -> T {
    #[allow(invalid_value)]
    let mut bytes: [u8; VARINT_FLAGS_MAX_SIZE] = unsafe { MaybeUninit::uninit().assume_init() };

    let useful_first_bits: usize = 8 - FlagsCount::to_usize();
    let first_byte_max_value: u8 = ((1u16 << (useful_first_bits - 1)) - 1) as u8;

    let fr_rem = ((value > first_byte_max_value as u64) as u8) << (useful_first_bits - 1);

    bytes[0] = (((flags as u16) << useful_first_bits) as u8)
        | (value as u8 & first_byte_max_value)
        | fr_rem;

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
    let mut offset = useful_first_bits - 1;
    let mut next = first_byte & (1 << (useful_first_bits - 1)) != 0;

    loop {
        if !next {
            break;
        }
        let value = read_byte()?;
        next = (value & 0b10000000) != 0;
        result |= ((value & 0b1111111) as u64) << offset;
        offset += 7;
    }
    Some((result, flags))
}

#[inline(always)]
pub fn decode_varint(mut read_byte: impl FnMut() -> Option<u8>) -> Option<u64> {
    let mut result = 0;
    let mut offset = 0u32;
    loop {
        let value = read_byte()?;
        let next = (value & 0b10000000) != 0;
        result |= ((value & 0b1111111) as u64) << offset;
        if !next {
            break;
        }
        offset += 7;
    }
    Some(result)
}

#[cfg(test)]
mod tests {
    use crate::varint::{decode_varint, decode_varint_flags, encode_varint, encode_varint_flags};
    use byteorder::ReadBytesExt;
    use std::io::{Cursor, Write};

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
}
