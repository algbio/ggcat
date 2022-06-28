use crate::io::concurrent::temp_reads::extra_data::SequenceExtraData;
use crate::io::varint::{decode_varint, encode_varint};
use byteorder::ReadBytesExt;
use std::io::{Read, Write};

pub mod bundles;
pub mod colors_manager;
pub mod colors_memmap;
pub mod managers;
pub mod non_colored;
pub mod parsers;
pub mod storage;

pub type ColorIndexType = u32;

impl SequenceExtraData for ColorIndexType {
    type TempBuffer = ();

    fn decode_extended(_: &mut Self::TempBuffer, reader: &mut impl Read) -> Option<Self> {
        decode_varint(|| reader.read_u8().ok()).map(|x| x as ColorIndexType)
    }

    fn encode_extended(&self, _: &Self::TempBuffer, writer: &mut impl Write) {
        encode_varint(|b| writer.write_all(b).unwrap(), *self as u64);
    }

    fn max_size(&self) -> usize {
        5
    }
}
