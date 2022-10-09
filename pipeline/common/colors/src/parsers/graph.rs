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
use std::cmp::min;
use std::io::{Read, Write};
use std::ops::Range;

#[derive(Clone, Default, Debug, Eq, PartialEq)]
pub struct MinBkMultipleColors {
    buffer_slice: Range<usize>,
    colors_subslice: Range<usize>,
}

impl MinBkMultipleColors {
    fn optimize_buffer_start(&mut self, buffer: &[(usize, ColorIndexType)]) {
        while buffer.len() > 0 && self.colors_subslice.start >= buffer[self.buffer_slice.start].0 {
            self.colors_subslice.start -= buffer[self.buffer_slice.start].0;
            self.colors_subslice.end -= buffer[self.buffer_slice.start].0;
            self.buffer_slice.start += 1;
        }
    }
}

pub struct MinBkColorsIterator<'a> {
    colors_slice: &'a [(usize, ColorIndexType)],
    slice_idx: usize,
    colors_left: usize,
    remaining_colors: usize,
}

impl<'a> Iterator for MinBkColorsIterator<'a> {
    type Item = ColorIndexType;

    #[inline(always)]
    fn next(&mut self) -> Option<Self::Item> {
        if self.remaining_colors == 0 {
            None
        } else if self.colors_left == 0 {
            self.slice_idx += 1;
            let (colors_left, color) = self.colors_slice[self.slice_idx];
            self.colors_left = colors_left - 1;
            self.remaining_colors -= 1;

            Some(color)
        } else {
            self.colors_left -= 1;
            self.remaining_colors -= 1;
            Some(self.colors_slice[self.slice_idx].1)
        }
    }
}

#[inline(always)]
fn decode_minbk_color(
    buffer: &mut Vec<(usize, ColorIndexType)>,
    mut get_byte_fn: impl FnMut() -> Option<u8>,
) -> Option<MinBkMultipleColors> {
    let color_groups_count = decode_varint(&mut get_byte_fn)?;
    let mut colors_count = 0;

    buffer.reserve(color_groups_count as usize);
    let buffer_start = buffer.len();
    for _ in 0..color_groups_count {
        let count = decode_varint(&mut get_byte_fn)? as usize;
        let color = decode_varint(&mut get_byte_fn)? as ColorIndexType;
        buffer.push((count, color));
        colors_count += count;
    }
    Some(MinBkMultipleColors {
        buffer_slice: buffer_start..buffer.len(),
        colors_subslice: 0..colors_count,
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

    fn copy_temp_buffer(
        dest: &mut Vec<(usize, ColorIndexType)>,
        src: &Vec<(usize, ColorIndexType)>,
    ) {
        dest.clear();
        dest.extend_from_slice(&src);
    }

    fn copy_extra_from(
        mut extra: Self,
        src: &Vec<(usize, ColorIndexType)>,
        dst: &mut Vec<(usize, ColorIndexType)>,
    ) -> Self {
        extra.optimize_buffer_start(src);

        let buffer_start = dst.len();

        let mut remaining = extra.colors_subslice.len();
        let mut src_slice_pos = extra.buffer_slice.start;

        let mut count = min(
            remaining,
            src[src_slice_pos].0 - extra.colors_subslice.start,
        );

        while count > 0 {
            dst.push((count, src[src_slice_pos].1));
            remaining -= count;

            if remaining == 0 {
                break;
            }

            src_slice_pos += 1;
            count = min(remaining, src[src_slice_pos].0);
        }

        Self {
            buffer_slice: buffer_start..dst.len(),
            colors_subslice: 0..extra.colors_subslice.len(),
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
        let mut self_ = self.clone();
        self_.optimize_buffer_start(buffer);

        let mut write_to_buffer = |write_len: Option<usize>| {
            if let Some(write_len) = write_len {
                encode_varint(|b| writer.write_all(b), write_len as u64).unwrap();
            }

            let mut items_count = 0;
            let mut remaining = self_.colors_subslice.len();
            let mut src_slice_pos = self_.buffer_slice.start;

            let mut count = min(
                remaining,
                buffer[src_slice_pos].0 - self_.colors_subslice.start,
            );

            while count > 0 {
                if write_len.is_some() {
                    encode_varint(|b| writer.write_all(b), count as u64).unwrap();
                    encode_varint(|b| writer.write_all(b), buffer[src_slice_pos].1 as u64).unwrap();
                }
                items_count += 1;
                remaining -= count;

                if remaining == 0 {
                    break;
                }

                src_slice_pos += 1;
                count = min(remaining, buffer[src_slice_pos].0);
            }

            items_count
        };

        let elements_count = write_to_buffer(None);
        write_to_buffer(Some(elements_count));
    }

    #[inline(always)]
    fn max_size(&self) -> usize {
        self.buffer_slice.len() * (VARINT_MAX_SIZE * 2) + VARINT_MAX_SIZE
    }
}

fn parse_colors(ident: &[u8], colors_buffer: &mut Vec<(usize, ColorIndexType)>) -> Range<usize> {
    let mut colors_count = 0;
    for col_pos in ident.find_iter(b"C:") {
        let (color_index, next_pos) = ColorIndexType::from_radix_16(&ident[(col_pos + 2)..]);

        let kmers_count = usize::from_radix_10(&ident[(col_pos + next_pos + 3)..]).0;
        colors_buffer.push((kmers_count, color_index));
        colors_count += kmers_count
    }
    if colors_count == 0 {
        println!("Warn: 0 colors for {:?}", std::str::from_utf8(ident));
    }
    0..colors_count
}

impl MinimizerBucketingSeqColorData for MinBkMultipleColors {
    type KmerColor = ColorIndexType;
    type KmerColorIterator<'a> = MinBkColorsIterator<'a>;

    fn create(sequence_info: SingleSequenceInfo, buffer: &mut Self::TempBuffer) -> Self {
        let buffer_start = buffer.len();
        let colors_subslice = parse_colors(sequence_info.sequence_ident, buffer);

        Self {
            buffer_slice: buffer_start..buffer.len(),
            colors_subslice,
        }
    }

    fn get_iterator<'a>(&'a self, buffer: &'a Self::TempBuffer) -> Self::KmerColorIterator<'a> {
        // self.colors_slice

        let mut self_ = self.clone();
        self_.optimize_buffer_start(buffer);

        MinBkColorsIterator {
            colors_slice: &buffer[self_.buffer_slice.clone()],
            slice_idx: 0,
            colors_left: buffer[self_.buffer_slice.start].0 - self_.colors_subslice.start,
            remaining_colors: self_.colors_subslice.len(),
        }
    }

    fn get_subslice(&self, range: Range<usize>) -> Self {
        assert!(
            self.colors_subslice.len() >= range.end,
            "{} >= {}",
            self.colors_subslice.len(),
            range.end
        );
        let start = self.colors_subslice.start + range.start;
        let end = self.colors_subslice.start + range.end;
        Self {
            buffer_slice: self.buffer_slice.clone(),
            colors_subslice: start..end,
        }
    }

    fn debug_count(&self) -> usize {
        self.colors_subslice.len()
    }
}

pub struct GraphColorsParser;

impl ColorsParser for GraphColorsParser {
    type SingleKmerColorDataType = ColorIndexType;
    type MinimizerBucketingSeqColorDataType = MinBkMultipleColors;
}

#[cfg(test)]
mod tests {
    use crate::colors_manager::MinimizerBucketingSeqColorData;
    use crate::parsers::graph::MinBkMultipleColors;
    use crate::parsers::SingleSequenceInfo;
    use io::concurrent::temp_reads::extra_data::SequenceExtraData;
    use std::io::Cursor;

    #[test]
    fn graph_multiple_colors_structure() {
        let input_colors = "C:1:12 C:2:1 C:3:3 C:4:23 C:5:7 C:6:24";

        let mut extra_buffer = Vec::new();

        let colors = MinBkMultipleColors::create(
            SingleSequenceInfo {
                file_index: 0,
                sequence_ident: input_colors.as_bytes(),
            },
            &mut extra_buffer,
        );

        let colors_count = colors.debug_count();
        assert_eq!(colors_count, 70);

        for start in 12..=70 {
            for end in (start + 1)..=70 {
                let subset = colors.get_subslice(start..end);
                assert_eq!(subset.debug_count(), end - start);

                let mut encoded_buffer = Vec::new();
                let mut cursor = Cursor::new(&mut encoded_buffer);

                subset.encode_extended(&extra_buffer, &mut cursor);

                let mut decoded_extra_buffer = vec![];

                let decoded = MinBkMultipleColors::decode_extended(
                    &mut decoded_extra_buffer,
                    &mut Cursor::new(encoded_buffer),
                )
                .unwrap();

                assert_eq!(
                    decoded.debug_count(),
                    end - start,
                    "start: {}, end: {}",
                    start,
                    end
                );

                let original = subset.get_iterator(&extra_buffer).collect::<Vec<_>>();
                let decoded = decoded
                    .get_iterator(&decoded_extra_buffer)
                    .collect::<Vec<_>>();

                assert_eq!(original, decoded);
            }
        }
    }
}
