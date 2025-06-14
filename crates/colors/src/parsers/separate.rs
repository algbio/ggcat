use crate::colors_manager::{ColorsParser, MinimizerBucketingSeqColorData};
use crate::parsers::SingleSequenceInfo;
use byteorder::ReadBytesExt;
use config::{ColorIndexType, DEFAULT_PER_CPU_BUFFER_SIZE};
use io::concurrent::temp_reads::extra_data::{
    HasEmptyExtraBuffer, SequenceExtraDataCombiner, SequenceExtraDataConsecutiveCompression,
    SequenceExtraDataTempBufferManagement,
};
use io::varint::{VARINT_MAX_SIZE, decode_varint, encode_varint};
use std::io::{Read, Write};
use std::ops::Range;
use utils::inline_vec::{AllocatorU32, InlineVec};

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct MinBkSingleColor(ColorIndexType);

#[derive(Copy, Clone, Debug)]
pub struct MinBkMultipleColors(InlineVec<ColorIndexType, 2>);

impl Default for MinBkSingleColor {
    fn default() -> Self {
        Self(ColorIndexType::MAX)
    }
}

impl Default for MinBkMultipleColors {
    fn default() -> Self {
        Self(InlineVec::default())
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
    type KmerColor<'a> = ColorIndexType;
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

impl SequenceExtraDataTempBufferManagement for MinBkMultipleColors {
    type TempBuffer = AllocatorU32;

    fn new_temp_buffer() -> Self::TempBuffer {
        AllocatorU32::new(DEFAULT_PER_CPU_BUFFER_SIZE.as_bytes())
    }

    fn clear_temp_buffer(buffer: &mut Self::TempBuffer) {
        buffer.reset();
    }

    fn copy_temp_buffer(dest: &mut Self::TempBuffer, src: &Self::TempBuffer) {
        dest.copy_from(src);
    }

    fn copy_extra_from(extra: Self, src: &Self::TempBuffer, dst: &mut Self::TempBuffer) -> Self {
        let src_slice = src.slice_vec(&extra.0);
        let mut new_vec = dst.new_vec(src_slice.len());
        let dst_slice = dst.slice_vec_mut(&mut new_vec);
        dst_slice.copy_from_slice(src_slice);
        Self(new_vec)
    }
}

impl SequenceExtraDataConsecutiveCompression for MinBkMultipleColors {
    type LastData = ();

    fn decode_extended(
        buffer: &mut Self::TempBuffer,
        reader: &mut impl Read,
        _last_data: Self::LastData,
    ) -> Option<Self> {
        let mut vector = buffer.new_vec(1);
        buffer.push_vec(&mut vector, 123);
        Some(Self(vector))

        // let len = decode_varint(|| reader.read_u8().ok())? as usize + 1;
        // let mut vec = buffer.new_vec(len);
        // let slice = buffer.slice_vec_mut(&mut vec);

        // slice[0] = decode_varint(|| reader.read_u8().ok())? as ColorIndexType;
        // for i in 1..len {
        //     let delta = decode_varint(|| reader.read_u8().ok())? as ColorIndexType;
        //     slice[i] = slice[i - 1] + delta;
        // }

        // Some(Self(vec))
    }

    fn encode_extended(
        &self,
        buffer: &Self::TempBuffer,
        writer: &mut impl Write,
        _last_data: Self::LastData,
    ) {
        // let slice = buffer.slice_vec(&self.0);
        // debug_assert!(slice.is_sorted());
        // debug_assert_ne!(slice.len(), 0);

        // encode_varint(|b| writer.write_all(b), (slice.len() - 1) as u64).unwrap();
        // encode_varint(|b| writer.write_all(b), slice[0] as u64).unwrap();

        // let mut last = slice[0];

        // for i in 1..slice.len() {
        //     let delta = slice[i] - last;
        //     encode_varint(|b| writer.write_all(b), delta as u64).unwrap();
        //     last = slice[i];
        // }
    }

    fn obtain_last_data(&self, last_data: Self::LastData) -> Self::LastData {
        last_data
    }

    fn max_size(&self) -> usize {
        self.0.len() * VARINT_MAX_SIZE
    }
}

impl MinimizerBucketingSeqColorData for MinBkMultipleColors {
    type KmerColor<'a> = &'a [ColorIndexType];
    type KmerColorIterator<'a> = std::iter::Repeat<&'a [ColorIndexType]>;

    fn create(sequence_info: SingleSequenceInfo, extra_buffer: &mut AllocatorU32) -> Self {
        let mut vector = extra_buffer.new_vec(1);
        extra_buffer.push_vec(&mut vector, sequence_info.static_color);
        Self(vector)
    }

    fn get_iterator<'a>(&'a self, extra_buffer: &'a AllocatorU32) -> Self::KmerColorIterator<'a> {
        std::iter::repeat(extra_buffer.slice_vec(&self.0))
    }

    fn get_subslice(&self, _range: Range<usize>, _reverse: bool) -> Self {
        *self
    }
}

impl SequenceExtraDataCombiner for MinBkMultipleColors {
    type SingleDataType = MinBkSingleColor;

    fn combine_entries(
        &mut self,
        out_buffer: &mut Self::TempBuffer,
        color: Self,
        in_buffer: &Self::TempBuffer,
    ) {
        out_buffer.extend_vec(&mut self.0, in_buffer.slice_vec(&color.0));
    }

    fn to_single(
        &self,
        in_buffer: &Self::TempBuffer,
        _out_buffer: &mut <Self::SingleDataType as SequenceExtraDataTempBufferManagement>::TempBuffer,
    ) -> Self::SingleDataType {
        MinBkSingleColor(in_buffer.slice_vec(&self.0)[0])
    }

    fn prepare_for_serialization(&mut self, buffer: &mut Self::TempBuffer) {
        let slice = buffer.slice_vec_mut(&mut self.0);
        slice.sort_unstable();
        let mut size = 1;
        while size < slice.len() && slice[size - 1] != slice[size] {
            size += 1;
        }

        for i in size..slice.len() {
            if slice[size - 1] != slice[i] {
                slice[size] = slice[i];
                size += 1;
            }
        }

        unsafe {
            self.0.set_len(size);
        }
    }

    #[inline(always)]
    fn from_single_entry<'a>(
        out_buffer: &'a mut Self::TempBuffer,
        single: Self::SingleDataType,
        _in_buffer: &'a mut <Self::SingleDataType as SequenceExtraDataTempBufferManagement>::TempBuffer,
    ) -> (Self, &'a mut Self::TempBuffer) {
        let mut self_ = Self(InlineVec::new());
        out_buffer.push_vec(&mut self_.0, single.0);
        (self_, out_buffer)
    }
}

pub struct SeparateColorsParser;

impl ColorsParser for SeparateColorsParser {
    type SingleKmerColorDataType = ColorIndexType;
    type MinimizerBucketingSeqColorDataType = MinBkSingleColor;
    type MinimizerBucketingMultipleSeqColorDataType = MinBkMultipleColors;
}
