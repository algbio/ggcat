use crate::bucket_colors::{ColorArena, ColorHandle, ColorRun};
use crate::colors_manager::{
    ColorsParser, MinimizerBucketingSeqColorData, MinimizerBucketingSeqColorDataIterable,
};
use crate::parsers::SingleSequenceInfo;
use config::{ColorIndexType, DEFAULT_PER_CPU_BUFFER_SIZE};
use io::concurrent::temp_reads::extra_data::{
    HasEmptyExtraBuffer, SequenceExtraDataCombiner, SequenceExtraDataConsecutiveCompression,
    SequenceExtraDataTempBufferManagement, TempBuffer,
};
use io::varint::{
    BufVarintSource, PointerVarintSource, SliceVarintSource, VARINT_MAX_SIZE, VarintSource,
    encode_varint, encode_varint_to_vec,
};
use std::io::{BufRead, Write};
use std::ops::Range;

/*
 * This file contains the color parsing and management for super-kmers where each kmer shares the exact same set of colors
*/

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct MinBkSingleColor(ColorIndexType);

/// A canonical color set serialized in temporary compactor buckets, held and
/// written as maximal runs rather than as individual colors.
///
/// Wire format: run count minus one, then one record per run. A record is the
/// gap from the previous run's last color -- the first run's gap is its absolute
/// first color -- shifted up one bit, the freed low bit saying whether a length
/// varint follows, and that length biased by two. Framing each record on its own
/// is what lets the decoder rebuild runs without expanding them; spending a bit
/// rather than a whole varint is what keeps a one-color set at two bytes.
/// Temporary buckets never outlive a process, so this format has no
/// compatibility obligation; the graph annotations and the color-map
/// second-order delta format are unchanged.
#[derive(Copy, Clone, Debug)]
pub struct MinBkMultipleColors(ColorHandle);

impl Default for MinBkSingleColor {
    fn default() -> Self {
        Self(ColorIndexType::MAX)
    }
}

impl Default for MinBkMultipleColors {
    fn default() -> Self {
        Self(ColorHandle::default())
    }
}

#[inline(always)]
fn decode_minbk_single_color(
    source: &mut impl VarintSource,
    last_data: MinBkSingleColor,
) -> Option<MinBkSingleColor> {
    let color_value = source.next_varint()? as ColorIndexType;

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
        _read_flags: u8,
    ) -> Option<Self> {
        decode_minbk_single_color(&mut SliceVarintSource::new(slice), last_data)
    }

    unsafe fn decode_from_pointer_extended(
        _: &mut (),
        ptr: *const u8,
        last_data: Self::LastData,
        _read_flags: u8,
    ) -> Option<Self> {
        decode_minbk_single_color(&mut unsafe { PointerVarintSource::new(ptr) }, last_data)
    }

    fn decode_extended(
        _: &mut (),
        reader: &mut impl BufRead,
        last_data: Self::LastData,
        _read_flags: u8,
    ) -> Option<Self> {
        decode_minbk_single_color(&mut BufVarintSource::new(reader), last_data)
    }

    fn encode_extended(
        &self,
        _: &(),
        writer: &mut impl Write,
        last_data: Self::LastData,
        _sequence_length: usize,
        _reverse_complement: bool,
        _read_flags: u8,
    ) {
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

    /// The bucketing phase writes one of these per superkmer, so it goes
    /// straight into the bucket buffer rather than through `Write`.
    #[inline(always)]
    fn encode_to_vec_extended(
        &self,
        _: &(),
        out: &mut Vec<u8>,
        last_data: Self::LastData,
        _sequence_length: usize,
        _reverse_complement: bool,
        _read_flags: u8,
    ) {
        encode_varint_to_vec(
            out,
            if last_data == *self {
                0
            } else {
                self.0 as u64 + 1
            },
        );
    }

    #[inline(always)]
    fn max_size(&self) -> usize {
        VARINT_MAX_SIZE
    }

    #[inline(always)]
    fn obtain_last_data(
        &self,
        _last_data: Self::LastData,
        _reverse_complement: bool,
    ) -> Self::LastData {
        *self
    }
}

impl MinimizerBucketingSeqColorData for MinBkSingleColor {
    fn create(sequence_info: SingleSequenceInfo, _: &mut ()) -> Self {
        Self(sequence_info.static_color as ColorIndexType)
    }

    fn get_subslice(&self, _range: Range<usize>, _reverse: bool) -> Self {
        *self
    }
}

impl<'a> MinimizerBucketingSeqColorDataIterable<'a, ColorIndexType> for MinBkSingleColor {
    type KmerColorIterator = std::iter::Repeat<ColorIndexType>;

    fn get_iterator(&'a self, _: &'a ()) -> Self::KmerColorIterator {
        std::iter::repeat(self.0)
    }

    fn get_unique_color(&'a self, _buffer: &'a Self::TempBuffer) -> ColorIndexType {
        self.0
    }
}

impl SequenceExtraDataTempBufferManagement for MinBkMultipleColors {
    type TempBuffer = ColorArena;
    fn new_temp_buffer() -> Self::TempBuffer {
        ColorArena::new(DEFAULT_PER_CPU_BUFFER_SIZE.as_bytes())
    }
    fn clear_temp_buffer(buffer: &mut Self::TempBuffer) {
        buffer.reset();
    }
    fn copy_temp_buffer(dest: &mut Self::TempBuffer, src: &Self::TempBuffer) {
        dest.copy_from(src);
    }
    fn copy_extra_from(extra: Self, src: &Self::TempBuffer, dst: &mut Self::TempBuffer) -> Self {
        Self(dst.copy_entry(src, &extra.0))
    }
}

impl SequenceExtraDataConsecutiveCompression for MinBkMultipleColors {
    type LastData = ();
    /// Both overrides skip the `Read` indirection: the decoder can look a whole
    /// word ahead when it owns the bytes, which it cannot do through a stream.
    fn decode_from_slice_extended(
        buffer: &mut Self::TempBuffer,
        slice: &[u8],
        _: (),
        _: u8,
    ) -> Option<Self> {
        buffer
            .decode_from_slice(slice)
            .map(|(handle, _)| Self(handle))
    }

    unsafe fn decode_from_pointer_extended(
        buffer: &mut Self::TempBuffer,
        ptr: *const u8,
        _: (),
        _: u8,
    ) -> Option<Self> {
        unsafe { buffer.decode_from_pointer(ptr) }.map(Self)
    }

    fn decode_extended(
        buffer: &mut Self::TempBuffer,
        reader: &mut impl BufRead,
        _: (),
        _: u8,
    ) -> Option<Self> {
        buffer.decode(reader).map(Self)
    }
    fn encode_extended(
        &self,
        buffer: &Self::TempBuffer,
        writer: &mut impl Write,
        _: (),
        _: usize,
        _: bool,
        _: u8,
    ) {
        buffer.write_to(&self.0, writer);
    }
    fn encode_to_vec_extended(
        &self,
        buffer: &Self::TempBuffer,
        out: &mut Vec<u8>,
        _: (),
        _: usize,
        _: bool,
        _: u8,
    ) {
        buffer.write_to_vec(&self.0, out);
    }
    fn obtain_last_data(&self, _: (), _: bool) {}
    fn max_size(&self) -> usize {
        self.0.encoded_len()
    }
}

impl MinimizerBucketingSeqColorData for MinBkMultipleColors {
    fn create(sequence_info: SingleSequenceInfo, buffer: &mut ColorArena) -> Self {
        Self(buffer.singleton(sequence_info.static_color))
    }
    fn get_subslice(&self, _: Range<usize>, _: bool) -> Self {
        *self
    }
}

/// Every k-mer of a superkmer shares the superkmer's colors, handed over as the
/// runs the arena already holds: nothing on this path expands them.
impl<'a> MinimizerBucketingSeqColorDataIterable<'a, &'a [ColorRun]> for MinBkMultipleColors {
    type KmerColorIterator = std::iter::Repeat<&'a [ColorRun]>;
    fn get_iterator(&'a self, buffer: &'a ColorArena) -> Self::KmerColorIterator {
        std::iter::repeat(buffer.runs(&self.0))
    }
    fn get_unique_color(&'a self, buffer: &'a ColorArena) -> &'a [ColorRun] {
        buffer.runs(&self.0)
    }
}

impl SequenceExtraDataCombiner for MinBkMultipleColors {
    type SingleDataType = MinBkSingleColor;
    const ALLOW_COMBINE: bool = true;
    fn combine_entries(
        &mut self,
        out_buffer: &mut Self::TempBuffer,
        color: Self,
        in_buffer: &Self::TempBuffer,
    ) {
        out_buffer.append_from(&mut self.0, in_buffer, &color.0);
    }
    fn to_single(
        &self,
        buffer: &Self::TempBuffer,
        _: &mut TempBuffer<Self::SingleDataType>,
    ) -> Self::SingleDataType {
        MinBkSingleColor(buffer.unique_color(&self.0))
    }
    fn prepare_for_serialization(&mut self, buffer: &mut Self::TempBuffer) {
        buffer.prepare(&mut self.0);
    }
    fn from_single_entry<'a>(
        buffer: &'a mut Self::TempBuffer,
        single: Self::SingleDataType,
        _: &'a mut TempBuffer<Self::SingleDataType>,
    ) -> (Self, &'a mut Self::TempBuffer) {
        (Self(buffer.singleton(single.0)), buffer)
    }
}

pub struct SeparateColorsParser;

impl ColorsParser for SeparateColorsParser {
    type SingleKmerColorDataType = ColorIndexType;
    type MinimizerBucketingSeqColorDataType = MinBkSingleColor;
    type MinimizerBucketingMultipleSeqColorDataType = MinBkMultipleColors;
}

#[cfg(test)]
mod tests;
