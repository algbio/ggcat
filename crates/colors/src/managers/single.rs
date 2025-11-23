use crate::DefaultColorsSerializer;
use crate::colors_manager::ColorsMergeManager;
use crate::storage::deserializer::ColorsDeserializer;
use byteorder::ReadBytesExt;
use config::{ColorCounterType, ColorIndexType};
use hashbrown::HashMap;
use hashes::HashFunctionFactory;
use io::concurrent::structured_sequences::IdentSequenceWriter;
use io::concurrent::temp_reads::extra_data::{
    SequenceExtraData, SequenceExtraDataTempBufferManagement,
};
use io::varint::{VARINT_MAX_SIZE, decode_varint, encode_varint};
use std::collections::VecDeque;
use std::io::{Read, Write};
use std::path::Path;
use structs::map_entry::MapEntry;

pub struct SingleColorManager;

impl ColorsMergeManager for SingleColorManager {
    type SingleKmerColorDataType = ColorIndexType;
    type TableColorEntry = ();
    type GlobalColorsTableWriter = ();
    type GlobalColorsTableReader = ColorsDeserializer<DefaultColorsSerializer>;

    fn create_colors_table(
        _path: impl AsRef<Path>,
        _color_names: &[String],
        _threads_count: usize,
        _print_stats: bool,
    ) -> anyhow::Result<Self::GlobalColorsTableWriter> {
        Ok(())
    }

    fn open_colors_table(path: impl AsRef<Path>) -> anyhow::Result<Self::GlobalColorsTableReader> {
        ColorsDeserializer::new(path, true)
    }

    type ColorsBufferTempStructure = ();

    fn allocate_temp_buffer_structure() -> Self::ColorsBufferTempStructure {
        ()
    }

    fn reinit_temp_buffer_structure(_data: &mut Self::ColorsBufferTempStructure) {}

    fn add_temp_buffer_structure_el<MH: HashFunctionFactory>(
        _data: &mut Self::ColorsBufferTempStructure,
        _kmer_color: &[ColorIndexType],
        _color_entry: &mut Self::HashMapTempColorIndex,
        _same_color: bool,
        _reached_threshold: bool,
    ) {
        unimplemented!()
        // assert!(
        //     entry.color_index.color_index == ColorIndexType::MAX
        //         || entry.color_index.color_index == *kmer_color
        // );
        // entry.color_index.color_index = *kmer_color;
    }

    type HashMapTempColorIndex = SingleHashMapTempColorIndex;

    fn new_color_index() -> Self::HashMapTempColorIndex {
        SingleHashMapTempColorIndex
    }

    fn process_colors<MH: HashFunctionFactory>(
        _global_colors_table: &Self::GlobalColorsTableWriter,
        _data: &mut Self::ColorsBufferTempStructure,
    ) {
    }

    fn assign_color(
        _global_colors_table: &Self::GlobalColorsTableWriter,
        _data: &mut [Self::SingleKmerColorDataType],
    ) -> Self::TableColorEntry {
        ()
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
        _data: &Self::ColorsBufferTempStructure,
        _ts: &mut Self::TempUnitigColorStructure,
        _entry: Self::HashMapTempColorIndex,
    ) {
        panic!("Unsupported!");
    }

    fn extend_backward(
        _data: &Self::ColorsBufferTempStructure,
        _ts: &mut Self::TempUnitigColorStructure,
        _entry: Self::HashMapTempColorIndex,
    ) {
        panic!("Unsupported!");
    }

    fn extend_forward_with_color(
        _ts: &mut Self::TempUnitigColorStructure,
        _entry_color: Self::TableColorEntry,
        _count: usize,
    ) {
        panic!("Unsupported!");
    }

    fn join_structures<const REVERSE: bool>(
        _dest: &mut Self::TempUnitigColorStructure,
        _src: &Self::PartialUnitigsColorStructure,
        _src_buffer: &<Self::PartialUnitigsColorStructure as SequenceExtraDataTempBufferManagement>::TempBuffer,
        _skip: ColorCounterType,
        _count: Option<usize>,
    ) {
        panic!("Unsupported!");
    }

    fn pop_base(_target: &mut Self::TempUnitigColorStructure) {
        panic!("Unsupported!");
    }

    fn encode_part_unitigs_colors(
        _ts: &mut Self::TempUnitigColorStructure,
        _colors_buffer: &mut <Self::PartialUnitigsColorStructure as SequenceExtraDataTempBufferManagement>::TempBuffer,
    ) -> Self::PartialUnitigsColorStructure {
        panic!("Unsupported!");
    }

    fn debug_tucs(_str: &Self::TempUnitigColorStructure, _seq: &[u8]) {
        unimplemented!();
    }

    fn debug_colors<MH: HashFunctionFactory>(
        _data: &Self::ColorsBufferTempStructure,
        _color: &Self::PartialUnitigsColorStructure,
        _colors_buffer: &<Self::PartialUnitigsColorStructure as SequenceExtraDataTempBufferManagement>::TempBuffer,
        _seq: &[u8],
        _hmap: &HashMap<MH::HashTypeUnextendable, MapEntry<Self::HashMapTempColorIndex>>,
    ) {
        unimplemented!()
    }
}

#[derive(Copy, Clone)]
pub struct SingleHashMapTempColorIndex;

#[derive(Debug)]
pub struct DefaultUnitigsTempColorData {
    colors: VecDeque<(ColorIndexType, u64)>,
}

#[derive(Debug, Default)]
pub struct UnitigsSerializerTempBuffer {
    colors: Vec<(ColorIndexType, u64)>,
}

#[derive(Clone, Copy, Debug)]
pub struct UnitigColorDataSerializer {
    slice_start: usize,
    slice_end: usize,
}

impl Default for UnitigColorDataSerializer {
    fn default() -> Self {
        Self {
            slice_start: 0,
            slice_end: 0,
        }
    }
}

impl SequenceExtraDataTempBufferManagement for UnitigColorDataSerializer {
    type TempBuffer = UnitigsSerializerTempBuffer;

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
        dst.colors
            .extend(&src.colors[extra.slice_start..extra.slice_end]);
        Self {
            slice_start: start,
            slice_end: dst.colors.len(),
        }
    }
}

impl SequenceExtraData for UnitigColorDataSerializer {
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
            slice_start: start,
            slice_end: buffer.colors.len(),
        })
    }

    fn encode_extended(&self, buffer: &Self::TempBuffer, writer: &mut impl Write) {
        let colors_count = self.slice_end - self.slice_start;
        encode_varint(|b| writer.write_all(b), colors_count as u64).unwrap();

        for i in self.slice_start..self.slice_end {
            let el = buffer.colors[i];
            encode_varint(|b| writer.write_all(b), el.0 as u64).unwrap();
            encode_varint(|b| writer.write_all(b), el.1).unwrap();
        }
    }

    #[inline(always)]
    fn max_size(&self) -> usize {
        (2 * (self.slice_end - self.slice_start) + 1) * VARINT_MAX_SIZE
    }
}

impl IdentSequenceWriter for UnitigColorDataSerializer {
    fn write_as_ident(&self, _stream: &mut impl Write, _extra_buffer: &Self::TempBuffer) {}

    fn write_as_gfa<const VERSION: u32>(
        &self,
        _k: u64,
        _index: u64,
        _length: u64,
        _stream: &mut impl Write,
        _extra_buffer: &Self::TempBuffer,
    ) {
    }

    fn parse_as_ident<'a>(_ident: &[u8], _extra_buffer: &mut Self::TempBuffer) -> Option<Self> {
        todo!()
    }

    fn parse_as_gfa<'a>(_ident: &[u8], _extra_buffer: &mut Self::TempBuffer) -> Option<Self> {
        todo!()
    }
}
