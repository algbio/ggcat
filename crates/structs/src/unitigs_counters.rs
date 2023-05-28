use io::concurrent::temp_reads::extra_data::{HasEmptyExtraBuffer, SequenceExtraData};
use serde::{Deserialize, Serialize};
use std::io::{Read, Write};

#[derive(Copy, Clone, Serialize, Deserialize, Debug)]
pub struct UnitigsCounters {
    first: u64,
    sum: u64,
    last: u64,
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

#[derive(Clone, Debug)]
pub struct UnitigsCountersSerializer;

impl HasEmptyExtraBuffer for UnitigsCountersSerializer {}

impl SequenceExtraData for UnitigsCountersSerializer {
    fn decode_extended(buffer: &mut Self::TempBuffer, reader: &mut impl Read) -> Option<Self> {
        // let start = buffer.colors.len();

        // let colors_count = decode_varint(|| reader.read_u8().ok())?;

        // for _ in 0..colors_count {
        //     buffer.colors.push((
        //         decode_varint(|| reader.read_u8().ok())? as ColorIndexType,
        //         decode_varint(|| reader.read_u8().ok())?,
        //     ));
        // }
        // Some(Self {
        //     slice: start..buffer.colors.len(),
        // })
        todo!()
    }

    fn encode_extended(&self, buffer: &Self::TempBuffer, writer: &mut impl Write) {
        // let colors_count = self.slice.end - self.slice.start;
        // encode_varint(|b| writer.write_all(b), colors_count as u64).unwrap();

        // for i in self.slice.clone() {
        //     let el = buffer.colors[i];
        //     encode_varint(|b| writer.write_all(b), el.0 as u64).unwrap();
        //     encode_varint(|b| writer.write_all(b), el.1).unwrap();
        // }
    }

    #[inline(always)]
    fn max_size(&self) -> usize {
        todo!()
        // (2 * (self.slice.end - self.slice.start) + 1) * VARINT_MAX_SIZE
    }
}
