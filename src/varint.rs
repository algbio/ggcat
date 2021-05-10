use byteorder::ReadBytesExt;
use serde::{Deserializer, Serializer};
use std::io::{Read, Write};
use std::mem::MaybeUninit;

#[inline(always)]
pub fn encode_varint<T>(mut write_bytes: impl FnOnce(&[u8]) -> T, mut value: u64) -> T {
    let mut bytes: [u8; 9] = unsafe { MaybeUninit::uninit().assume_init() };
    let mut index = 0;
    while index < bytes.len() {
        let rem = ((value > 127) as u8) << 7;
        bytes[index] = (((value as u8) & 0b1111111) | rem);
        value >>= 7;
        index += 1;
        if value == 0 {
            break;
        }
    }
    write_bytes(&bytes[..index])
}

pub fn decode_varint(mut read_byte: impl FnMut() -> Option<u8>) -> Option<u64> {
    let mut result = 0;
    let mut offset = 0u32;
    loop {
        let mut value = read_byte()?;
        let next = (value & 0b10000000) != 0;
        result |= ((value & 0b1111111) as u64) << offset;
        if !next {
            break;
        }
        offset += 7;
    }
    Some(result)
}

#[inline(always)]
pub fn serialize<S>(t: &u64, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    encode_varint(move |b| serializer.serialize_bytes(b), *t)
}

pub fn deserialize<'de, D>(d: D) -> Result<u64, D::Error>
where
    D: Deserializer<'de>,
{
    todo!()
}

mod tests {
    use crate::compressed_read::{CompressedRead, CompressedReadIndipendent};
    use crate::intermediate_storage::{
        IntermediateReadsReader, IntermediateReadsWriter, VecReader,
    };
    use crate::multi_thread_buckets::BucketType;
    use crate::varint::{decode_varint, encode_varint};
    use byteorder::WriteBytesExt;
    use rand::RngCore;
    use std::io::{Cursor, Write};
    use std::iter::FromIterator;
    use std::panic::resume_unwind;

    #[test]
    fn varints() {
        let mut result: Vec<u8> = vec![];

        for i in 0..100000 {
            result.clear();
            encode_varint(|b| result.write(b), i);
            let mut cursor = Cursor::new(&result);
            let mut vecreader = VecReader::new(4096, &mut cursor);
            assert_eq!(i, decode_varint(|| Some(vecreader.read_byte())).unwrap());
        }
    }

    #[test]
    fn encoding() {
        let mut sequences: Vec<String> = Vec::new();
        let mut rng = rand::thread_rng();

        const LETTERS: [u8; 4] = [b'A', b'C', b'T', b'G'];

        let mut buffer = [0; 1024];
        for i in 0..100000 {
            let size = (rng.next_u32() % 512) as usize + 1;
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
            let read = CompressedRead::new_from_plain(read.as_bytes(), &mut tmp);

            writer.add_acgt_read(&(), read.as_reference(&tmp));
        }
        writer.finalize();

        let mut reader = IntermediateReadsReader::<()>::new("/tmp/test-encoding.0.lz4".to_string());

        let mut index = 0;
        reader.for_each(|(_, x)| {
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
