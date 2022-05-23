use std::mem::MaybeUninit;

pub const VARINT_FLAGS_MAX_SIZE: usize = 10;
pub const VARINT_MAX_SIZE: usize = 9;

#[inline(always)]
#[allow(clippy::uninit_assumed_init)]
pub fn encode_varint<T>(write_bytes: impl FnOnce(&[u8]) -> T, mut value: u64) -> T {
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
#[allow(non_camel_case_types)]
#[allow(clippy::uninit_assumed_init)]
pub fn encode_varint_flags<T, F: FnOnce(&[u8]) -> T, FLAGS_COUNT: typenum::Unsigned>(
    write_bytes: F,
    mut value: u64,
    flags: u8,
) -> T {
    let mut bytes: [u8; VARINT_FLAGS_MAX_SIZE] = unsafe { MaybeUninit::uninit().assume_init() };

    let useful_first_bits: usize = 8 - FLAGS_COUNT::to_usize();
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
#[allow(non_camel_case_types)]
pub fn decode_varint_flags<F: FnMut() -> Option<u8>, FLAGS_COUNT: typenum::Unsigned>(
    mut read_byte: F,
) -> Option<(u64, u8)> {
    let first_byte = read_byte()?;

    let useful_first_bits: usize = 8 - FLAGS_COUNT::to_usize();
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
    use crate::utils::compressed_read::{CompressedRead, CompressedReadIndipendent};
    use byteorder::WriteBytesExt;
    use parallel_processor::multi_thread_buckets::BucketType;
    use rand::RngCore;
    use std::io::{Cursor, Write};
    use std::iter::FromIterator;
    use std::panic::resume_unwind;

    #[test]
    fn varints() {
        let mut result: Vec<u8> = vec![];

        for i in 0..100000 {
            result.clear();
            encode_varint(|b| result.write_all(b), i);
            let mut cursor = Cursor::new(&result);
            let mut vecreader = VecReader::new(4096, &mut cursor);
            assert_eq!(i, decode_varint(|| Some(vecreader.read_byte())).unwrap());
        }
    }

    #[test]
    fn varints_flags() {
        let mut result: Vec<u8> = vec![];

        for i in 0..100000 {
            result.clear();
            encode_varint_flags::<_, _, typenum::U2>(|b| result.write_all(b), i, (i % 4) as u8);
            let mut cursor = Cursor::new(&result);
            let mut vecreader = VecReader::new(4096, &mut cursor);
            assert_eq!(
                (i, (i % 4) as u8),
                decode_varint_flags::<_, typenum::U2>(|| Some(vecreader.read_byte())).unwrap()
            );
        }
    }

    #[test]
    fn encoding() {
        let mut sequences: Vec<String> = Vec::new();
        let mut rng = rand::thread_rng();

        const LETTERS: [u8; 4] = [b'A', b'C', b'T', b'G'];

        let k = 1;

        let mut buffer = [0; 1024];
        for i in 0..100000 {
            let size = (rng.next_u32() % 120) as usize + k;
            rng.fill_bytes(&mut buffer[..size]);
            sequences.push(String::from_iter(
                buffer[..size]
                    .iter()
                    .map(|x| LETTERS[*x as usize % 4] as char),
            ));
        }

        let mut tmp = Vec::new();

        let mut writer = IntermediateReadsWriter::<()>::new("/tmp/test-encoding".as_ref(), 0);
        for read in sequences.iter() {
            tmp.clear();
            CompressedRead::from_plain_write_directly_to_buffer_with_flags::<typenum::U0>(
                read.as_bytes(),
                &mut tmp,
                0,
            );
            writer.write_data(tmp.as_slice());
        }
        writer.finalize();

        let mut reader =
            IntermediateReadsReader::<()>::new("/tmp/test-encoding.0.lz4".to_string(), false);

        let mut index = 0;
        reader.for_each::<_, typenum::U0>(|_, _, x| {
            let val = x.to_string();
            // println!("SQ: {} / {}", val, sequences[index]);
            if val != sequences[index].as_str() {
                println!("R: {}", val);
                println!("E: {}", sequences[index]);
                panic!("AA {}", index);
            }
            index += 1;
        });
    }
}
