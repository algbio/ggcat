use crate::assemble_pipeline::parallel_kmers_merge::structs::MapEntry;
use crate::hashes::HashFunctionFactory;
use crate::io::concurrent::intermediate_storage::SequenceExtraData;
use hashbrown::HashMap;
use std::io::Write;
use std::path::Path;

/// Encoded color(s) of a minimizer bucketing step sequence
pub trait MinimizerBucketingSeqColorData:
    Default + Copy + Clone + SequenceExtraData + Send + Sync
{
    fn create(file_index: u64) -> Self;
}

/// Helper trait to manage colors labeling on KmersMerge step
pub trait ColorsMergeManager<H: HashFunctionFactory, C: ColorsManager>: Sized {
    /// Temporary buffer that holds color values for each kmer while merging them
    type ColorsBufferTempStructure;
    fn allocate_temp_buffer_structure() -> Self::ColorsBufferTempStructure;
    fn reinit_temp_buffer_structure(data: &mut Self::ColorsBufferTempStructure);
    fn add_temp_buffer_structure_el(
        data: &mut Self::ColorsBufferTempStructure,
        flags: &C::MinimizerBucketingSeqColorDataType,
        el: (usize, H::HashTypeUnextendable),
    );

    /// Temporary storage for colors associated with a single kmer in the hashmap (holds the color subset index)
    type HashMapTempColorIndex;
    fn new_color_index() -> Self::HashMapTempColorIndex;

    /// This step finds the color subset indexes for each map entry
    fn process_colors(
        global_colors_table: &C::GlobalColorsTable,
        data: &mut Self::ColorsBufferTempStructure,
        map: &mut HashMap<H::HashTypeUnextendable, MapEntry<Self::HashMapTempColorIndex>>,
        min_multiplicity: usize,
    );

    type PartialUnitigsColorStructure: SequenceExtraData + Clone + 'static;
    type TempUnitigColorStructure;

    fn alloc_unitig_color_structure() -> Self::TempUnitigColorStructure;
    fn reset_unitig_color_structure(ts: &mut Self::TempUnitigColorStructure);
    fn extend_forward(
        ts: &mut Self::TempUnitigColorStructure,
        entry: &MapEntry<Self::HashMapTempColorIndex>,
    );
    fn extend_backward(
        ts: &mut Self::TempUnitigColorStructure,
        entry: &MapEntry<Self::HashMapTempColorIndex>,
    );

    fn join_structures<const REVERSE: bool>(
        dest: &mut Self::TempUnitigColorStructure,
        src: &Self::PartialUnitigsColorStructure,
        skip: u64,
    );

    fn pop_base(target: &mut Self::TempUnitigColorStructure);

    fn clear_deserialized_unitigs_colors();

    /// Encodes partial unitig colors into the extra data structure
    fn encode_part_unitigs_colors(
        ts: &mut Self::TempUnitigColorStructure,
    ) -> Self::PartialUnitigsColorStructure;

    fn print_color_data(data: &Self::PartialUnitigsColorStructure, buffer: &mut impl Write);

    fn debug_tucs(str: &Self::TempUnitigColorStructure, seq: &[u8]);
}

pub trait ColorsManager: 'static + Sized {
    const COLORS_ENABLED: bool;

    type GlobalColorsTable: Sync + 'static;
    fn create_colors_table(
        path: impl AsRef<Path>,
        color_names: Vec<String>,
    ) -> Self::GlobalColorsTable;

    fn print_color_stats(global_colors_table: &Self::GlobalColorsTable);

    type MinimizerBucketingSeqColorDataType: MinimizerBucketingSeqColorData;

    type ColorsMergeManagerType<H: HashFunctionFactory>: ColorsMergeManager<H, Self>;

    // fn join_unitigs();
}

/// Dummy colors manager
impl ColorsManager for () {
    const COLORS_ENABLED: bool = false;
    type GlobalColorsTable = ();

    fn create_colors_table(
        _path: impl AsRef<Path>,
        _color_names: Vec<String>,
    ) -> Self::GlobalColorsTable {
        ()
    }

    fn print_color_stats(_global_colors_table: &Self::GlobalColorsTable) {}

    type MinimizerBucketingSeqColorDataType = ();
    type ColorsMergeManagerType<H: HashFunctionFactory> = ();
}

impl MinimizerBucketingSeqColorData for () {
    #[inline(always)]
    fn create(_file_index: u64) -> Self {
        ()
    }
}

impl<H: HashFunctionFactory, C: ColorsManager> ColorsMergeManager<H, C> for () {
    type ColorsBufferTempStructure = ();

    #[inline(always)]
    fn allocate_temp_buffer_structure() -> Self::ColorsBufferTempStructure {
        ()
    }

    #[inline(always)]
    fn reinit_temp_buffer_structure(_data: &mut Self::ColorsBufferTempStructure) {}

    #[inline(always)]
    fn add_temp_buffer_structure_el(
        _data: &mut Self::ColorsBufferTempStructure,
        _flags: &C::MinimizerBucketingSeqColorDataType,
        _el: (usize, <H as HashFunctionFactory>::HashTypeUnextendable),
    ) {
    }

    type HashMapTempColorIndex = ();

    #[inline(always)]
    fn new_color_index() -> Self::HashMapTempColorIndex {
        ()
    }

    #[inline(always)]
    fn process_colors(
        _global_colors_table: &C::GlobalColorsTable,
        _data: &mut Self::ColorsBufferTempStructure,
        _map: &mut HashMap<
            <H as HashFunctionFactory>::HashTypeUnextendable,
            MapEntry<Self::HashMapTempColorIndex>,
        >,
        _min_multiplicity: usize,
    ) {
        todo!()
    }

    type PartialUnitigsColorStructure = ();
    type TempUnitigColorStructure = ();

    #[inline(always)]
    fn alloc_unitig_color_structure() -> Self::TempUnitigColorStructure {
        ()
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
        ()
    }

    #[inline(always)]
    fn pop_base(_target: &mut Self::TempUnitigColorStructure) {}

    #[inline(always)]
    fn clear_deserialized_unitigs_colors() {}

    #[inline(always)]
    fn encode_part_unitigs_colors(
        _ts: &mut Self::TempUnitigColorStructure,
    ) -> Self::PartialUnitigsColorStructure {
        ()
    }

    fn print_color_data(_data: &Self::PartialUnitigsColorStructure, _buffer: &mut impl Write) {}

    fn debug_tucs(_str: &Self::TempUnitigColorStructure, _seq: &[u8]) {}
}
