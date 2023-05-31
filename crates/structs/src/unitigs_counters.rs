use byteorder::ReadBytesExt;
use io::{
    concurrent::temp_reads::extra_data::{HasEmptyExtraBuffer, SequenceExtraData},
    varint::{decode_varint, encode_varint, VARINT_MAX_SIZE},
};
use serde::{Deserialize, Serialize};
use std::io::{Read, Write};

#[derive(Copy, Clone, Serialize, Deserialize, Debug)]
pub struct UnitigsCounters {
    pub first: u64,
    pub sum: u64,
    pub last: u64,
}

impl UnitigsCounters {
    #[inline]
    pub fn new() -> Self {
        Self {
            first: 0,
            sum: 0,
            last: 0,
        }
    }

    #[inline]
    pub fn add_counter(&mut self, counter: u64) {
        if self.first == 0 {
            self.first = counter;
        }
        self.sum += counter;
        self.last = counter;
    }
}

impl HasEmptyExtraBuffer for UnitigsCounters {}

impl SequenceExtraData for UnitigsCounters {
    fn decode_extended(_: &mut Self::TempBuffer, reader: &mut impl Read) -> Option<Self> {
        let first = decode_varint(|| reader.read_u8().ok())?;
        let sum = decode_varint(|| reader.read_u8().ok())?;
        let last = decode_varint(|| reader.read_u8().ok())?;
        Some(Self { first, sum, last })
    }

    fn encode_extended(&self, _: &Self::TempBuffer, writer: &mut impl Write) {
        encode_varint(|b| writer.write(b).ok(), self.first).unwrap();
        encode_varint(|b| writer.write(b).ok(), self.sum).unwrap();
        encode_varint(|b| writer.write(b).ok(), self.last).unwrap();
    }

    #[inline(always)]
    fn max_size(&self) -> usize {
        3 * VARINT_MAX_SIZE
    }
}
