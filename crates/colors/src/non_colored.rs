use crate::colors_manager::{
    ColorsManager, ColorsMergeManager, ColorsParser, MinimizerBucketingSeqColorData,
    MinimizerBucketingSeqColorDataIterable,
};
use crate::parsers::SingleSequenceInfo;
use config::{BucketIndexType, ColorCounterType};
use dynamic_dispatch::dynamic_dispatch;
use hashbrown::HashMap;
use hashes::HashFunctionFactory;
use io::concurrent::structured_sequences::IdentSequenceWriter;
use io::concurrent::temp_reads::extra_data::{
    HasEmptyExtraBuffer, SequenceExtraData, SequenceExtraDataCombiner,
    SequenceExtraDataTempBufferManagement,
};
use parallel_processor::fast_smart_bucket_sort::FastSortable;
use std::fmt::Debug;
use std::io::{Read, Write};
use std::ops::Range;
use std::path::Path;
use structs::map_entry::MapEntry;

#[derive(Debug, Hash, Eq, PartialEq, Ord, PartialOrd, Copy, Clone, Default)]
pub struct NonColoredManager;

/// Dummy colors manager
#[dynamic_dispatch]
impl ColorsManager for NonColoredManager {
    const COLORS_ENABLED: bool = false;
    type SingleKmerColorDataType = NonColoredManager;

    fn get_bucket_from_color(
        _color: &Self::SingleKmerColorDataType,
        _colors_count: u64,
        _buckets_count_log: usize,
    ) -> BucketIndexType {
        panic!("Cannot get color bucket for non colored manager!");
    }

    type ColorsParserType = NonColoredManager;
    type ColorsMergeManagerType = NonColoredManager;
}

impl HasEmptyExtraBuffer for NonColoredManager {}

impl SequenceExtraData for NonColoredManager {
    #[inline(always)]
    fn decode_extended(_buffer: &mut Self::TempBuffer, _reader: &mut impl Read) -> Option<Self> {
        Some(NonColoredManager)
    }

    #[inline(always)]
    fn encode_extended(
        &self,
        _buffer: &Self::TempBuffer,
        _writer: &mut impl Write,
        _reverse_complement: bool,
    ) {
    }

    #[inline(always)]
    fn max_size(&self) -> usize {
        0
    }
}

impl Iterator for NonColoredManager {
    type Item = Self;

    #[inline(always)]
    fn next(&mut self) -> Option<Self::Item> {
        Some(Self)
    }
}

impl<'a> MinimizerBucketingSeqColorData for NonColoredManager {
    #[inline(always)]
    fn create(_file_index: SingleSequenceInfo, _: &mut ()) -> Self {
        NonColoredManager
    }

    fn get_subslice(&self, _range: Range<usize>, _reverse: bool) -> Self {
        Self
    }
}

impl<'a> MinimizerBucketingSeqColorDataIterable<'a, NonColoredManager> for NonColoredManager {
    type KmerColorIterator = std::iter::Repeat<NonColoredManager>;

    #[inline(always)]
    fn get_iterator(&'a self, _: &'a ()) -> Self::KmerColorIterator {
        std::iter::repeat(NonColoredManager)
    }

    fn get_unique_color(&'a self, _buffer: &'a Self::TempBuffer) -> NonColoredManager {
        NonColoredManager
    }
}

impl<'a, KmerColor> MinimizerBucketingSeqColorDataIterable<'a, &'a [KmerColor]>
    for NonColoredManager
{
    type KmerColorIterator = std::iter::Repeat<&'a [KmerColor]>;

    fn get_iterator(&'a self, _buffer: &'a Self::TempBuffer) -> Self::KmerColorIterator {
        std::iter::repeat(&[])
    }

    fn get_unique_color(&'a self, _buffer: &'a Self::TempBuffer) -> &'a [KmerColor] {
        &[]
    }
}

impl SequenceExtraDataCombiner for NonColoredManager {
    type SingleDataType = NonColoredManager;
    const ALLOW_COMBINE: bool = true;

    fn combine_entries(
        &mut self,
        _out_buffer: &mut Self::TempBuffer,
        _color: Self,
        _in_buffer: &Self::TempBuffer,
    ) {
    }

    fn to_single(
        &self,
        _in_buffer: &Self::TempBuffer,
        _out_buffer: &mut <Self::SingleDataType as SequenceExtraDataTempBufferManagement>::TempBuffer,
    ) -> Self::SingleDataType {
        NonColoredManager
    }

    fn prepare_for_serialization(&mut self, _buffer: &mut Self::TempBuffer) {}

    #[inline(always)]
    fn from_single_entry<'a>(
        out_buffer: &'a mut Self::TempBuffer,
        _color: Self::SingleDataType,
        _in_buffer: &'a mut <Self::SingleDataType as SequenceExtraDataTempBufferManagement>::TempBuffer,
    ) -> (Self, &'a mut Self::TempBuffer) {
        (Self, out_buffer)
    }
}

impl FastSortable for NonColoredManager {
    fn get_shifted(&self, _rhs: u8) -> u8 {
        0
    }
}

impl ColorsParser for NonColoredManager {
    type SingleKmerColorDataType = NonColoredManager;
    type MinimizerBucketingSeqColorDataType = NonColoredManager;
    type MinimizerBucketingMultipleSeqColorDataType = NonColoredManager;
}

impl IdentSequenceWriter for NonColoredManager {
    #[inline(always)]
    fn write_as_ident(&self, _stream: &mut impl Write, _extra_buffer: &Self::TempBuffer) {}
    #[inline(always)]
    fn write_as_gfa<const VERSION: u32>(
        &self,
        _k: u64,
        _index: u64,
        _length: u64,
        _stream: &mut impl Write,
        _extra_buffer: &Self::TempBuffer,
    ) {
    }

    #[inline(always)]
    fn parse_as_ident<'a>(_ident: &[u8], _extra_buffer: &mut Self::TempBuffer) -> Option<Self> {
        Some(NonColoredManager)
    }
    #[inline(always)]
    fn parse_as_gfa<'a>(_ident: &[u8], _extra_buffer: &mut Self::TempBuffer) -> Option<Self> {
        Some(NonColoredManager)
    }
}

impl ColorsMergeManager for NonColoredManager {
    type SingleKmerColorDataType = NonColoredManager;
    type TableColorEntry = NonColoredManager;
    type GlobalColorsTableWriter = ();
    type GlobalColorsTableReader = ();

    fn create_colors_table(
        _path: impl AsRef<Path>,
        _color_names: &[String],
        _threads_count: usize,
        _print_stats: bool,
    ) -> anyhow::Result<Self::GlobalColorsTableWriter> {
        Ok(())
    }

    fn open_colors_table(_path: impl AsRef<Path>) -> anyhow::Result<Self::GlobalColorsTableReader> {
        Ok(())
    }

    type ColorsBufferTempStructure = NonColoredManager;

    #[inline(always)]
    fn allocate_temp_buffer_structure() -> Self::ColorsBufferTempStructure {
        NonColoredManager
    }

    #[inline(always)]
    fn reinit_temp_buffer_structure(_data: &mut Self::ColorsBufferTempStructure) {}

    #[inline(always)]
    fn add_temp_buffer_structure_el<MH: HashFunctionFactory>(
        _data: &mut Self::ColorsBufferTempStructure,
        _kmer_colors: &[Self::SingleKmerColorDataType],
        _color_entry: &mut Self::HashMapTempColorIndex,
        _same_color: bool,
        _reached_threshold: bool,
    ) {
    }

    type HashMapTempColorIndex = NonColoredManager;

    #[inline(always)]
    fn new_color_index() -> Self::HashMapTempColorIndex {
        NonColoredManager
    }

    #[inline(always)]
    fn process_colors<MH: HashFunctionFactory>(
        _global_colors_table: &Self::GlobalColorsTableWriter,
        _data: &mut Self::ColorsBufferTempStructure,
    ) {
        unreachable!()
    }

    fn assign_color(
        _global_colors_table: &Self::GlobalColorsTableWriter,
        _data: &mut [Self::SingleKmerColorDataType],
    ) -> Self::HashMapTempColorIndex {
        Self
    }

    type PartialUnitigsColorStructure = NonColoredManager;
    type TempUnitigColorStructure = NonColoredManager;

    #[inline(always)]
    fn alloc_unitig_color_structure() -> Self::TempUnitigColorStructure {
        NonColoredManager
    }

    #[inline(always)]
    fn reset_unitig_color_structure(_ts: &mut Self::TempUnitigColorStructure) {}

    #[inline(always)]
    fn extend_forward(
        _data: &Self::ColorsBufferTempStructure,
        _ts: &mut Self::TempUnitigColorStructure,
        _entry: Self::HashMapTempColorIndex,
    ) {
    }

    #[inline(always)]
    fn extend_backward(
        _data: &Self::ColorsBufferTempStructure,
        _ts: &mut Self::TempUnitigColorStructure,
        _entry: Self::HashMapTempColorIndex,
    ) {
    }

    fn extend_forward_with_color(
        _ts: &mut Self::TempUnitigColorStructure,
        _entry_color: Self::TableColorEntry,
        _count: usize,
    ) {
    }

    fn extend_backward_with_color(
        _ts: &mut Self::TempUnitigColorStructure,
        _entry_color: Self::TableColorEntry,
        _count: usize,
    ) {
    }

    #[inline(always)]
    fn join_structures<const REVERSE: bool>(
        _dest: &mut Self::TempUnitigColorStructure,
        _src: &Self::PartialUnitigsColorStructure,
        _src_buffer: &<Self::PartialUnitigsColorStructure as SequenceExtraDataTempBufferManagement>::TempBuffer,
        _skip: ColorCounterType,
        _count: Option<usize>,
    ) {
    }

    #[inline(always)]
    fn pop_base(_target: &mut Self::TempUnitigColorStructure) {}

    #[inline(always)]
    fn encode_part_unitigs_colors(
        _ts: &mut Self::TempUnitigColorStructure,
        _colors_buffer: &mut <Self::PartialUnitigsColorStructure as SequenceExtraDataTempBufferManagement>::TempBuffer,
    ) -> Self::PartialUnitigsColorStructure {
        NonColoredManager
    }

    fn debug_tucs(_str: &Self::TempUnitigColorStructure, _seq: &[u8]) {}
    fn debug_colors<MH: HashFunctionFactory>(
        _data: &Self::ColorsBufferTempStructure,
        _color: &Self::PartialUnitigsColorStructure,
        _colors_buffer: &<Self::PartialUnitigsColorStructure as SequenceExtraDataTempBufferManagement>::TempBuffer,
        _seq: &[u8],
        _hmap: &HashMap<MH::HashTypeUnextendable, MapEntry<Self::HashMapTempColorIndex>>,
    ) {
    }
}
