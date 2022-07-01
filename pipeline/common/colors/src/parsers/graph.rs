use crate::colors_manager::{ColorsParser, MinimizerBucketingSeqColorData};
use crate::parsers::SingleSequenceInfo;
use atoi::{FromRadix10, FromRadix16};
use bstr::ByteSlice;
use byteorder::ReadBytesExt;
use config::ColorIndexType;
use io::concurrent::temp_reads::extra_data::{
    SequenceExtraData, SequenceExtraDataTempBufferManagement,
};
use io::varint::{decode_varint, encode_varint, VARINT_MAX_SIZE};
use std::io::{Read, Write};
use std::ops::Range;

#[derive(Clone, Default, Debug, Eq, PartialEq)]
pub struct MinBkMultipleColors {
    colors_slice: Range<usize>,
}

pub struct MinBkSingleColorIterator<'a> {
    colors_slice: &'a [(usize, ColorIndexType)],
    vec_pos: usize,
    iter_idx: usize,
}

impl<'a> Iterator for MinBkSingleColorIterator<'a> {
    type Item = ColorIndexType;

    #[inline(always)]
    fn next(&mut self) -> Option<Self::Item> {
        while (self.vec_pos + 1) < self.colors_slice.len()
            && self.colors_slice[self.vec_pos + 1].0 <= self.iter_idx
        {
            self.vec_pos += 1;
        }
        self.iter_idx += 1;

        Some(self.colors_slice[self.vec_pos].1)
    }
}

#[inline(always)]
fn decode_minbk_color(
    buffer: &mut Vec<(usize, ColorIndexType)>,
    mut get_byte_fn: impl FnMut() -> Option<u8>,
) -> Option<MinBkMultipleColors> {
    let colors_count = decode_varint(&mut get_byte_fn)?;
    buffer.reserve(colors_count as usize);
    let buffer_start = buffer.len();
    for _ in 0..colors_count {
        let position = decode_varint(&mut get_byte_fn)? as usize;
        let color = decode_varint(&mut get_byte_fn)? as ColorIndexType;
        buffer.push((position, color));
    }
    Some(MinBkMultipleColors {
        colors_slice: buffer_start..buffer.len(),
    })
}

impl SequenceExtraDataTempBufferManagement<Vec<(usize, ColorIndexType)>> for MinBkMultipleColors {
    #[inline(always)]
    fn new_temp_buffer() -> Vec<(usize, ColorIndexType)> {
        Vec::new()
    }

    #[inline(always)]
    fn clear_temp_buffer(buffer: &mut Vec<(usize, ColorIndexType)>) {
        buffer.clear();
    }

    fn copy_extra_from(
        extra: Self,
        src: &Vec<(usize, ColorIndexType)>,
        dst: &mut Vec<(usize, ColorIndexType)>,
    ) -> Self {
        let start = dst.len();
        dst.extend_from_slice(&src[extra.colors_slice]);
        Self {
            colors_slice: start..dst.len(),
        }
    }
}

impl SequenceExtraData for MinBkMultipleColors {
    type TempBuffer = Vec<(usize, ColorIndexType)>;

    fn decode_from_slice_extended(buffer: &mut Self::TempBuffer, slice: &[u8]) -> Option<Self> {
        let mut index = 0;
        decode_minbk_color(buffer, || {
            let data = slice[index];
            index += 1;
            Some(data)
        })
    }

    unsafe fn decode_from_pointer_extended(
        buffer: &mut Self::TempBuffer,
        mut ptr: *const u8,
    ) -> Option<Self> {
        decode_minbk_color(buffer, || {
            let data = *ptr;
            ptr = ptr.add(1);
            Some(data)
        })
    }

    fn decode_extended(buffer: &mut Self::TempBuffer, reader: &mut impl Read) -> Option<Self> {
        decode_minbk_color(buffer, || reader.read_u8().ok())
    }

    fn encode_extended(&self, buffer: &Self::TempBuffer, writer: &mut impl Write) {
        encode_varint(|b| writer.write_all(b), buffer.len() as u64).unwrap();

        for (pos, color) in buffer[self.colors_slice.clone()].iter() {
            encode_varint(|b| writer.write_all(b), *pos as u64).unwrap();
            encode_varint(|b| writer.write_all(b), *color as u64).unwrap();
        }
    }

    #[inline(always)]
    fn max_size(&self) -> usize {
        self.colors_slice.len() * (VARINT_MAX_SIZE * 2) + VARINT_MAX_SIZE
    }
}

fn parse_colors(ident: &[u8], colors_buffer: &mut Vec<(usize, ColorIndexType)>) -> Range<usize> {
    let start_pos = colors_buffer.len();
    for col_pos in ident.find_iter(b"C:") {
        let (color_index, next_pos) = ColorIndexType::from_radix_16(&ident[col_pos..]);
        let position_index = usize::from_radix_10(&ident[(col_pos + next_pos)..]).0;
        colors_buffer.push((position_index, color_index));
    }

    start_pos..colors_buffer.len()
}

impl MinimizerBucketingSeqColorData for MinBkMultipleColors {
    type KmerColor = ColorIndexType;
    type KmerColorIterator<'a> = MinBkSingleColorIterator<'a>;

    fn create(sequence_info: SingleSequenceInfo, buffer: &mut Self::TempBuffer) -> Self {
        Self {
            colors_slice: parse_colors(sequence_info.sequence_ident, buffer),
        }
    }

    fn get_iterator<'a>(&'a self, buffer: &'a Self::TempBuffer) -> Self::KmerColorIterator<'a> {
        MinBkSingleColorIterator {
            colors_slice: &buffer[self.colors_slice.clone()],
            vec_pos: 0,
            iter_idx: 0,
        }
    }

    fn get_subslice(&self, range: Range<usize>) -> Self {
        assert!(self.colors_slice.len() >= range.end);
        let start = self.colors_slice.start + range.start;
        let end = self.colors_slice.start + range.end;
        Self {
            colors_slice: start..end,
        }
    }
}

pub struct GraphColorsParser;

impl ColorsParser for GraphColorsParser {
    type SingleKmerColorDataType = ColorIndexType;
    type MinimizerBucketingSeqColorDataType = MinBkMultipleColors;
}
