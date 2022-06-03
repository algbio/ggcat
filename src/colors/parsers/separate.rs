use crate::colors::colors_manager::{ColorsParser, MinimizerBucketingSeqColorData};
use crate::colors::parsers::SingleSequenceInfo;
use crate::io::concurrent::temp_reads::extra_data::SequenceExtraData;
use crate::io::varint::{decode_varint, encode_varint, VARINT_MAX_SIZE};
use crate::ColorIndexType;
use byteorder::ReadBytesExt;
use std::io::{Read, Write};
use std::ops::Range;

#[derive(Copy, Clone, Default, Debug, Eq, PartialEq)]
pub struct MinBkSingleColor(ColorIndexType);

#[inline(always)]
fn decode_minbk_single_color(get_byte_fn: impl FnMut() -> Option<u8>) -> Option<MinBkSingleColor> {
    Some(MinBkSingleColor(
        decode_varint(get_byte_fn)? as ColorIndexType
    ))
}

impl SequenceExtraData for MinBkSingleColor {
    type TempBuffer = ();

    fn decode_from_slice_extended(_: &mut (), slice: &[u8]) -> Option<Self> {
        let mut index = 0;
        decode_minbk_single_color(|| {
            let data = slice[index];
            index += 1;
            Some(data)
        })
    }

    unsafe fn decode_from_pointer_extended(_: &mut (), mut ptr: *const u8) -> Option<Self> {
        decode_minbk_single_color(|| {
            let data = *ptr;
            ptr = ptr.add(1);
            Some(data)
        })
    }

    fn decode_extended(_: &mut (), reader: &mut impl Read) -> Option<Self> {
        decode_minbk_single_color(|| reader.read_u8().ok())
    }

    fn encode_extended(&self, _: &(), writer: &mut impl Write) {
        encode_varint(|b| writer.write_all(b), self.0 as u64).unwrap();
    }

    #[inline(always)]
    fn max_size(&self) -> usize {
        VARINT_MAX_SIZE
    }
}

impl MinimizerBucketingSeqColorData for MinBkSingleColor {
    type KmerColor = ColorIndexType;
    type KmerColorIterator<'a> = std::iter::Repeat<ColorIndexType>;

    fn create(sequence_info: SingleSequenceInfo, _: &mut ()) -> Self {
        Self(sequence_info.file_index as ColorIndexType)
    }

    fn get_iterator<'a>(&'a self, _: &'a ()) -> Self::KmerColorIterator<'a> {
        std::iter::repeat(self.0)
    }

    fn get_subslice(&self, _range: Range<usize>) -> Self {
        *self
    }
}

pub struct SeparateColorsParser;

impl ColorsParser for SeparateColorsParser {
    type SingleKmerColorDataType = ColorIndexType;
    type MinimizerBucketingSeqColorDataType = MinBkSingleColor;
}
