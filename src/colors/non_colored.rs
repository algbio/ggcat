use crate::assemble_pipeline::parallel_kmers_merge::structs::MapEntry;
use crate::colors::colors_manager::{
    ColorsManager, ColorsMergeManager, ColorsParser, MinimizerBucketingSeqColorData,
};
use crate::colors::default_colors_manager::SingleSequenceInfo;
use crate::hashes::HashFunctionFactory;
use crate::io::concurrent::temp_reads::extra_data::SequenceExtraData;
use hashbrown::HashMap;
use std::io::{Read, Write};
use std::ops::Range;
use std::path::Path;

#[derive(Debug, Clone, Default)]
pub struct NonColoredManager;

/// Dummy colors manager
impl ColorsManager for NonColoredManager {
    const COLORS_ENABLED: bool = false;
    type SingleKmerColorDataType = NonColoredManager;

    type ColorsParserType = NonColoredManager;
    type ColorsMergeManagerType<H: HashFunctionFactory> = NonColoredManager;
}

impl SequenceExtraData for NonColoredManager {
    #[inline(always)]
    fn decode<'a>(_reader: &'a mut impl Read) -> Option<Self> {
        Some(NonColoredManager)
    }

    #[inline(always)]
    fn encode<'a>(&self, _writer: &'a mut impl Write) {}

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
    type KmerColor = NonColoredManager;
    type KmerColorIterator<'a> = std::iter::Repeat<NonColoredManager>;

    #[inline(always)]
    fn create(_file_index: SingleSequenceInfo) -> Self {
        NonColoredManager
    }

    #[inline(always)]
    fn get_iterator<'a>(&'a self) -> Self::KmerColorIterator<'a> {
        std::iter::repeat(NonColoredManager)
    }

    fn get_subslice(&self, _range: Range<usize>) -> Self {
        Self
    }
}

impl ColorsParser for NonColoredManager {
    type SingleKmerColorDataType = NonColoredManager;
    type MinimizerBucketingSeqColorDataType = NonColoredManager;
}

impl<H: HashFunctionFactory> ColorsMergeManager<H> for NonColoredManager {
    type SingleKmerColorDataType = NonColoredManager;
    type GlobalColorsTable = NonColoredManager;

    fn create_colors_table(
        _path: impl AsRef<Path>,
        _color_names: Vec<String>,
    ) -> Self::GlobalColorsTable {
        NonColoredManager
    }

    fn print_color_stats(_global_colors_table: &Self::GlobalColorsTable) {}

    type ColorsBufferTempStructure = NonColoredManager;

    #[inline(always)]
    fn allocate_temp_buffer_structure() -> Self::ColorsBufferTempStructure {
        NonColoredManager
    }

    #[inline(always)]
    fn reinit_temp_buffer_structure(_data: &mut Self::ColorsBufferTempStructure) {}

    #[inline(always)]
    fn add_temp_buffer_structure_el(
        _data: &mut Self::ColorsBufferTempStructure,
        _kmer_color: &Self::SingleKmerColorDataType,
        _el: (usize, <H as HashFunctionFactory>::HashTypeUnextendable),
    ) {
    }

    type HashMapTempColorIndex = NonColoredManager;

    #[inline(always)]
    fn new_color_index() -> Self::HashMapTempColorIndex {
        NonColoredManager
    }

    #[inline(always)]
    fn process_colors(
        _global_colors_table: &Self::GlobalColorsTable,
        _data: &mut Self::ColorsBufferTempStructure,
        _map: &mut HashMap<
            <H as HashFunctionFactory>::HashTypeUnextendable,
            MapEntry<Self::HashMapTempColorIndex>,
        >,
        _min_multiplicity: usize,
    ) {
        todo!()
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
        _skip: u64,
    ) {
    }

    #[inline(always)]
    fn pop_base(_target: &mut Self::TempUnitigColorStructure) {}

    #[inline(always)]
    fn clear_deserialized_unitigs_colors() {}

    #[inline(always)]
    fn encode_part_unitigs_colors(
        _ts: &mut Self::TempUnitigColorStructure,
    ) -> Self::PartialUnitigsColorStructure {
        NonColoredManager
    }

    fn print_color_data(_data: &Self::PartialUnitigsColorStructure, _buffer: &mut impl Write) {}

    fn debug_tucs(_str: &Self::TempUnitigColorStructure, _seq: &[u8]) {}
}
