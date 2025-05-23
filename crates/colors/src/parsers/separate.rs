use crate::colors_manager::{ColorsParser, MinimizerBucketingSeqColorData};
use crate::parsers::SingleSequenceInfo;
use byteorder::ReadBytesExt;
use config::ColorIndexType;
use io::concurrent::temp_reads::extra_data::{
    HasEmptyExtraBuffer, SequenceExtraDataConsecutiveCompression,
};
use io::varint::{VARINT_MAX_SIZE, decode_varint, encode_varint};
use std::io::{Read, Write};
use std::ops::Range;

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct MinBkSingleColor(ColorIndexType);

impl Default for MinBkSingleColor {
    fn default() -> Self {
        Self(ColorIndexType::MAX)
    }
}

#[inline(always)]
fn decode_minbk_single_color(
    get_byte_fn: impl FnMut() -> Option<u8>,
    last_data: MinBkSingleColor,
) -> Option<MinBkSingleColor> {
    let color_value = decode_varint(get_byte_fn)? as ColorIndexType;

    Some(if color_value == 0 {
        last_data
    } else {
        MinBkSingleColor(color_value - 1)
    })
}

impl HasEmptyExtraBuffer for MinBkSingleColor {}
impl SequenceExtraDataConsecutiveCompression for MinBkSingleColor {
    type LastData = Self;

    fn decode_from_slice_extended(
        _: &mut (),
        slice: &[u8],
        last_data: Self::LastData,
    ) -> Option<Self> {
        let mut index = 0;
        decode_minbk_single_color(
            || {
                let data = slice[index];
                index += 1;
                Some(data)
            },
            last_data,
        )
    }

    unsafe fn decode_from_pointer_extended(
        _: &mut (),
        mut ptr: *const u8,
        last_data: Self::LastData,
    ) -> Option<Self> {
        decode_minbk_single_color(
            || unsafe {
                let data = *ptr;
                ptr = ptr.add(1);
                Some(data)
            },
            last_data,
        )
    }

    fn decode_extended(
        _: &mut (),
        reader: &mut impl Read,
        last_data: Self::LastData,
    ) -> Option<Self> {
        decode_minbk_single_color(|| reader.read_u8().ok(), last_data)
    }

    fn encode_extended(&self, _: &(), writer: &mut impl Write, last_data: Self::LastData) {
        encode_varint(
            |b| writer.write_all(b),
            if last_data == *self {
                0
            } else {
                self.0 as u64 + 1
            },
        )
        .unwrap();
    }

    #[inline(always)]
    fn max_size(&self) -> usize {
        VARINT_MAX_SIZE
    }

    #[inline(always)]
    fn obtain_last_data(&self, _last_data: Self::LastData) -> Self::LastData {
        *self
    }
}

impl MinimizerBucketingSeqColorData for MinBkSingleColor {
    type KmerColor = ColorIndexType;
    type KmerColorIterator<'a> = std::iter::Repeat<ColorIndexType>;

    fn create(sequence_info: SingleSequenceInfo, _: &mut ()) -> Self {
        Self(sequence_info.static_color as ColorIndexType)
    }

    fn get_iterator<'a>(&'a self, _: &'a ()) -> Self::KmerColorIterator<'a> {
        std::iter::repeat(self.0)
    }

    fn get_subslice(&self, _range: Range<usize>, _reverse: bool) -> Self {
        *self
    }
}

pub struct SeparateColorsParser;

impl ColorsParser for SeparateColorsParser {
    type SingleKmerColorDataType = ColorIndexType;
    type MinimizerBucketingSeqColorDataType = MinBkSingleColor;
}
