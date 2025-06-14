use crate::colors_manager::{
    ColorsManager, ColorsMergeManager, ColorsParser, MinimizerBucketingSeqColorData,
};
use crate::parsers::SingleSequenceInfo;
use config::{BucketIndexType, ColorCounterType};
use dynamic_dispatch::dynamic_dispatch;
use hashbrown::HashMap;
use hashes::HashFunctionFactory;
use io::compressed_read::CompressedRead;
use io::concurrent::structured_sequences::IdentSequenceWriter;
use io::concurrent::temp_reads::extra_data::{
    HasEmptyExtraBuffer, SequenceExtraData, SequenceExtraDataCombiner,
    SequenceExtraDataConsecutiveCompression, SequenceExtraDataTempBufferManagement,
};
use parallel_processor::fast_smart_bucket_sort::FastSortable;
use rustc_hash::FxHashMap;
use std::fmt::Debug;
use std::io::{Read, Write};
use std::marker::PhantomData;
use std::ops::Range;
use std::path::Path;
use structs::map_entry::MapEntry;

#[derive(Debug, Hash, Eq, PartialEq, Ord, PartialOrd, Copy, Clone, Default)]
pub struct NonColoredManager;

#[derive(Debug, Hash, Eq, PartialEq, Ord, PartialOrd, Copy, Clone, Default)]
pub struct NonColoredMultipleColors<T>(PhantomData<T>);

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
impl<T: Sync + Send + Debug + Clone> HasEmptyExtraBuffer for NonColoredMultipleColors<T> {}

impl SequenceExtraData for NonColoredManager {
    #[inline(always)]
    fn decode_extended(_buffer: &mut Self::TempBuffer, _reader: &mut impl Read) -> Option<Self> {
        Some(NonColoredManager)
    }

    #[inline(always)]
    fn encode_extended(&self, _buffer: &Self::TempBuffer, _writer: &mut impl Write) {}

    #[inline(always)]
    fn max_size(&self) -> usize {
        0
    }
}

impl<T: Sync + Send + Debug + Clone> SequenceExtraData for NonColoredMultipleColors<T> {
    #[inline(always)]
    fn decode_extended(_buffer: &mut Self::TempBuffer, _reader: &mut impl Read) -> Option<Self> {
        Some(Self(PhantomData))
    }

    #[inline(always)]
    fn encode_extended(&self, _buffer: &Self::TempBuffer, _writer: &mut impl Write) {}

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

impl MinimizerBucketingSeqColorData for NonColoredManager {
    type KmerColor<'a> = NonColoredManager;
    type KmerColorIterator<'a> = std::iter::Repeat<NonColoredManager>;

    #[inline(always)]
    fn create(_file_index: SingleSequenceInfo, _: &mut ()) -> Self {
        NonColoredManager
    }

    #[inline(always)]
    fn get_iterator<'a>(&'a self, _: &'a ()) -> Self::KmerColorIterator<'a> {
        std::iter::repeat(NonColoredManager)
    }

    fn get_subslice(&self, _range: Range<usize>, _reverse: bool) -> Self {
        Self
    }
}

impl<T: Sync + Send + Debug + Copy + Default + MinimizerBucketingSeqColorData + 'static>
    MinimizerBucketingSeqColorData for NonColoredMultipleColors<T>
{
    type KmerColor<'a> = &'a [T::KmerColor<'a>];

    type KmerColorIterator<'a> = std::iter::Repeat<&'a [T::KmerColor<'a>]>;

    fn create(_stream_info: SingleSequenceInfo, _buffer: &mut Self::TempBuffer) -> Self {
        Self(PhantomData)
    }

    fn get_iterator<'a>(&'a self, _buffer: &'a Self::TempBuffer) -> Self::KmerColorIterator<'a> {
        std::iter::repeat(&[])
    }

    fn get_subslice(&self, _range: Range<usize>, _reverse: bool) -> Self {
        Self(PhantomData)
    }
}

impl<T: Sync + Send + Debug + Clone + SequenceExtraDataConsecutiveCompression + Default>
    SequenceExtraDataCombiner for NonColoredMultipleColors<T>
{
    type SingleDataType = T;

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
        T::default()
    }

    fn prepare_for_serialization(&mut self, _buffer: &mut Self::TempBuffer) {}

    #[inline(always)]
    fn from_single_entry<'a>(
        out_buffer: &'a mut Self::TempBuffer,
        _color: Self::SingleDataType,
        _in_buffer: &'a mut <Self::SingleDataType as SequenceExtraDataTempBufferManagement>::TempBuffer,
    ) -> (Self, &'a mut Self::TempBuffer) {
        (Self(PhantomData), out_buffer)
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
    type MinimizerBucketingMultipleSeqColorDataType = NonColoredMultipleColors<NonColoredManager>;
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
    type GlobalColorsTableWriter = ();
    type GlobalColorsTableReader = ();

    fn create_colors_table(
        _path: impl AsRef<Path>,
        _color_names: &[String],
    ) -> anyhow::Result<Self::GlobalColorsTableWriter> {
        Ok(())
    }

    fn open_colors_table(_path: impl AsRef<Path>) -> anyhow::Result<Self::GlobalColorsTableReader> {
        Ok(())
    }

    fn print_color_stats(_global_colors_table: &Self::GlobalColorsTableWriter) {}

    type ColorsBufferTempStructure = NonColoredManager;

    #[inline(always)]
    fn allocate_temp_buffer_structure(_init_data: &Path) -> Self::ColorsBufferTempStructure {
        NonColoredManager
    }

    #[inline(always)]
    fn reinit_temp_buffer_structure(_data: &mut Self::ColorsBufferTempStructure) {}

    #[inline(always)]
    fn add_temp_buffer_structure_el<MH: HashFunctionFactory>(
        _data: &mut Self::ColorsBufferTempStructure,
        _kmer_colors: &[Self::SingleKmerColorDataType],
        _el: (usize, <MH as HashFunctionFactory>::HashTypeUnextendable),
        _entry: &mut MapEntry<Self::HashMapTempColorIndex>,
    ) {
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

    type HashMapTempColorIndex = NonColoredManager;

    #[inline(always)]
    fn new_color_index() -> Self::HashMapTempColorIndex {
        NonColoredManager
    }

    #[inline(always)]
    fn process_colors<MH: HashFunctionFactory>(
        _global_colors_table: &Self::GlobalColorsTableWriter,
        _data: &mut Self::ColorsBufferTempStructure,
        _map: &mut FxHashMap<
            <MH as HashFunctionFactory>::HashTypeUnextendable,
            MapEntry<Self::HashMapTempColorIndex>,
        >,
        _k: usize,
        _min_multiplicity: usize,
    ) {
        unreachable!()
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
        _ts: &mut Self::TempUnitigColorStructure,
        _entry: &MapEntry<Self::HashMapTempColorIndex>,
    ) {
    }

    #[inline(always)]
    fn extend_backward(
        _ts: &mut Self::TempUnitigColorStructure,
        _entry: &MapEntry<Self::HashMapTempColorIndex>,
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
        _color: &Self::PartialUnitigsColorStructure,
        _colors_buffer: &<Self::PartialUnitigsColorStructure as SequenceExtraDataTempBufferManagement>::TempBuffer,
        _seq: &[u8],
        _hmap: &HashMap<MH::HashTypeUnextendable, MapEntry<Self::HashMapTempColorIndex>>,
    ) {
    }
}
