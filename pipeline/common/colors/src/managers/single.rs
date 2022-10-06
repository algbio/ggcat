use crate::colors_manager::ColorsMergeManager;
use crate::storage::deserializer::ColorsDeserializer;
use crate::DefaultColorsSerializer;
use byteorder::ReadBytesExt;
use config::ColorIndexType;
use hashbrown::HashMap;
use hashes::{HashFunctionFactory, MinimizerHashFunctionFactory};
use io::compressed_read::CompressedRead;
use io::concurrent::temp_reads::extra_data::{
    SequenceExtraData, SequenceExtraDataTempBufferManagement,
};
use io::varint::{decode_varint, encode_varint, VARINT_MAX_SIZE};
use std::collections::VecDeque;
use std::io::{Read, Write};
use std::marker::PhantomData;
use std::ops::Range;
use std::path::Path;
use structs::map_entry::MapEntry;

pub struct SingleColorManager<H: MinimizerHashFunctionFactory, MH: HashFunctionFactory>(
    PhantomData<(H, MH)>,
);

impl<H: MinimizerHashFunctionFactory, MH: HashFunctionFactory> ColorsMergeManager<H, MH>
    for SingleColorManager<H, MH>
{
    type SingleKmerColorDataType = ColorIndexType;
    type GlobalColorsTableWriter = ();
    type GlobalColorsTableReader = ColorsDeserializer<DefaultColorsSerializer>;

    fn create_colors_table(
        _path: impl AsRef<Path>,
        _color_names: Vec<String>,
    ) -> Self::GlobalColorsTableWriter {
        ()
    }

    fn open_colors_table(path: impl AsRef<Path>) -> Self::GlobalColorsTableReader {
        ColorsDeserializer::new(path)
    }

    fn print_color_stats(_global_colors_table: &Self::GlobalColorsTableWriter) {}

    type ColorsBufferTempStructure = ();

    fn allocate_temp_buffer_structure() -> Self::ColorsBufferTempStructure {
        ()
    }

    fn reinit_temp_buffer_structure(_data: &mut Self::ColorsBufferTempStructure) {}

    fn add_temp_buffer_structure_el(
        _data: &mut Self::ColorsBufferTempStructure,
        kmer_color: &ColorIndexType,
        _el: (usize, MH::HashTypeUnextendable),
        entry: &mut MapEntry<Self::HashMapTempColorIndex>,
    ) {
        assert!(
            entry.color_index.color_index == ColorIndexType::MAX
                || entry.color_index.color_index == *kmer_color
        );
        entry.color_index.color_index = *kmer_color;
    }

    #[inline(always)]
    fn add_temp_buffer_sequence(
        _data: &mut Self::ColorsBufferTempStructure,
        _sequence: CompressedRead,
        _k: usize,
        _m: usize,
        _flags: u8,
    ) {
    }

    type HashMapTempColorIndex = SingleHashMapTempColorIndex;

    fn new_color_index() -> Self::HashMapTempColorIndex {
        SingleHashMapTempColorIndex {
            color_index: ColorIndexType::MAX,
        }
    }

    fn process_colors(
        _global_colors_table: &Self::GlobalColorsTableWriter,
        _data: &mut Self::ColorsBufferTempStructure,
        _map: &mut HashMap<MH::HashTypeUnextendable, MapEntry<Self::HashMapTempColorIndex>>,
        _k: usize,
        _min_multiplicity: usize,
    ) {
    }

    type PartialUnitigsColorStructure = UnitigColorDataSerializer;
    type TempUnitigColorStructure = DefaultUnitigsTempColorData;

    fn alloc_unitig_color_structure() -> Self::TempUnitigColorStructure {
        DefaultUnitigsTempColorData {
            colors: VecDeque::new(),
        }
    }

    fn reset_unitig_color_structure(ts: &mut Self::TempUnitigColorStructure) {
        ts.colors.clear();
    }

    fn extend_forward(
        _ts: &mut Self::TempUnitigColorStructure,
        _entry: &MapEntry<Self::HashMapTempColorIndex>,
    ) {
        panic!("Unsupported!");
    }

    fn extend_backward(
        _ts: &mut Self::TempUnitigColorStructure,
        _entry: &MapEntry<Self::HashMapTempColorIndex>,
    ) {
        panic!("Unsupported!");
    }

    fn join_structures<const REVERSE: bool>(
        _dest: &mut Self::TempUnitigColorStructure,
        _src: &Self::PartialUnitigsColorStructure,
        _src_buffer: &<Self::PartialUnitigsColorStructure as SequenceExtraData>::TempBuffer,
        _skip: u64,
    ) {
        panic!("Unsupported!");
    }

    fn pop_base(_target: &mut Self::TempUnitigColorStructure) {
        panic!("Unsupported!");
    }

    fn encode_part_unitigs_colors(
        _ts: &mut Self::TempUnitigColorStructure,
        _colors_buffer: &mut <Self::PartialUnitigsColorStructure as SequenceExtraData>::TempBuffer,
    ) -> Self::PartialUnitigsColorStructure {
        panic!("Unsupported!");
    }

    fn print_color_data(
        _colors: &Self::PartialUnitigsColorStructure,
        _colors_buffer: &<Self::PartialUnitigsColorStructure as SequenceExtraData>::TempBuffer,
        _buffer: &mut impl Write,
    ) {
        panic!("Unsupported!");
    }

    fn debug_tucs(_str: &Self::TempUnitigColorStructure, _seq: &[u8]) {
        unimplemented!();
    }
}

pub struct SingleHashMapTempColorIndex {
    color_index: ColorIndexType,
}

#[derive(Debug)]
pub struct DefaultUnitigsTempColorData {
    colors: VecDeque<(ColorIndexType, u64)>,
}

#[derive(Debug)]
pub struct UnitigsSerializerTempBuffer {
    colors: Vec<(ColorIndexType, u64)>,
}

#[derive(Clone, Debug)]
pub struct UnitigColorDataSerializer {
    slice: Range<usize>,
}

impl SequenceExtraDataTempBufferManagement<UnitigsSerializerTempBuffer>
    for UnitigColorDataSerializer
{
    fn new_temp_buffer() -> UnitigsSerializerTempBuffer {
        UnitigsSerializerTempBuffer { colors: Vec::new() }
    }

    fn clear_temp_buffer(buffer: &mut UnitigsSerializerTempBuffer) {
        buffer.colors.clear();
    }

    fn copy_temp_buffer(dest: &mut UnitigsSerializerTempBuffer, src: &UnitigsSerializerTempBuffer) {
        dest.colors.clear();
        dest.colors.extend_from_slice(&src.colors);
    }

    fn copy_extra_from(
        extra: Self,
        src: &UnitigsSerializerTempBuffer,
        dst: &mut UnitigsSerializerTempBuffer,
    ) -> Self {
        let start = dst.colors.len();
        dst.colors.extend(&src.colors[extra.slice]);
        Self {
            slice: start..dst.colors.len(),
        }
    }
}

impl SequenceExtraData for UnitigColorDataSerializer {
    type TempBuffer = UnitigsSerializerTempBuffer;

    fn decode_extended(buffer: &mut Self::TempBuffer, reader: &mut impl Read) -> Option<Self> {
        let start = buffer.colors.len();

        let colors_count = decode_varint(|| reader.read_u8().ok())?;

        for _ in 0..colors_count {
            buffer.colors.push((
                decode_varint(|| reader.read_u8().ok())? as ColorIndexType,
                decode_varint(|| reader.read_u8().ok())?,
            ));
        }
        Some(Self {
            slice: start..buffer.colors.len(),
        })
    }

    fn encode_extended(&self, buffer: &Self::TempBuffer, writer: &mut impl Write) {
        let colors_count = self.slice.end - self.slice.start;
        encode_varint(|b| writer.write_all(b), colors_count as u64).unwrap();

        for i in self.slice.clone() {
            let el = buffer.colors[i];
            encode_varint(|b| writer.write_all(b), el.0 as u64).unwrap();
            encode_varint(|b| writer.write_all(b), el.1).unwrap();
        }
    }

    #[inline(always)]
    fn max_size(&self) -> usize {
        (2 * (self.slice.end - self.slice.start) + 1) * VARINT_MAX_SIZE
    }
}
