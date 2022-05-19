use crate::assemble_pipeline::parallel_kmers_merge::structs::MapEntry;
use crate::colors::default_colors_manager::SingleSequenceInfo;
use crate::hashes::HashFunctionFactory;
use crate::io::concurrent::temp_reads::extra_data::SequenceExtraData;
use hashbrown::HashMap;
use std::io::Write;
use std::ops::Range;
use std::path::Path;

pub mod color_types {
    #![allow(dead_code)]

    use crate::colors::colors_manager::{ColorsManager, ColorsMergeManager, ColorsParser};

    macro_rules! color_manager_type_alias {
        ($tyn:ident) => {
            pub type $tyn<H, C> =
                <<C as ColorsManager>::ColorsMergeManagerType<H> as ColorsMergeManager<H>>::$tyn;
        };
    }

    macro_rules! color_parser_type_alias {
        ($tyn:ident) => {
            pub type $tyn<C> = <<C as ColorsManager>::ColorsParserType as ColorsParser>::$tyn;
        };
    }

    color_manager_type_alias!(GlobalColorsTable);
    color_manager_type_alias!(ColorsBufferTempStructure);
    color_manager_type_alias!(HashMapTempColorIndex);
    color_manager_type_alias!(PartialUnitigsColorStructure);
    color_manager_type_alias!(TempUnitigColorStructure);

    color_parser_type_alias!(SingleKmerColorDataType);
    color_parser_type_alias!(MinimizerBucketingSeqColorDataType);

    pub type ColorsParserType<C> = <C as ColorsManager>::ColorsParserType;
    pub type ColorsMergeManagerType<H, C> = <C as ColorsManager>::ColorsMergeManagerType<H>;
}

/// Encoded color(s) of a minimizer bucketing step sequence
pub trait MinimizerBucketingSeqColorData:
    Default + Clone + SequenceExtraData + Send + Sync
{
    type KmerColor;
    type KmerColorIterator<'a>: Iterator<Item = Self::KmerColor>
    where
        Self: 'a;

    fn create<'a>(file_info: SingleSequenceInfo<'a>) -> Self;
    fn get_iterator<'a>(&'a self) -> Self::KmerColorIterator<'a>;
    fn get_subslice(&self, range: Range<usize>) -> Self;
}

/// Helper trait to manage colors parsing from different sources (actually 2, color from file or color from annotated dbg graph)
pub trait ColorsParser: Sized {
    type SingleKmerColorDataType;
    type MinimizerBucketingSeqColorDataType: MinimizerBucketingSeqColorData<
        KmerColor = Self::SingleKmerColorDataType,
    >;
}

/// Helper trait to manage colors labeling on KmersMerge step
pub trait ColorsMergeManager<H: HashFunctionFactory>: Sized {
    type SingleKmerColorDataType;
    type GlobalColorsTable: Sync + Send + 'static;

    /// Creates a new colors table at the given path
    fn create_colors_table(
        path: impl AsRef<Path>,
        color_names: Vec<String>,
    ) -> Self::GlobalColorsTable;

    /// Prints to stdout the final stats for the colors table
    fn print_color_stats(global_colors_table: &Self::GlobalColorsTable);

    /// Temporary buffer that holds color values for each kmer while merging them
    type ColorsBufferTempStructure: 'static + Send + Sync;
    fn allocate_temp_buffer_structure() -> Self::ColorsBufferTempStructure;
    fn reinit_temp_buffer_structure(data: &mut Self::ColorsBufferTempStructure);
    fn add_temp_buffer_structure_el(
        data: &mut Self::ColorsBufferTempStructure,
        kmer_color: &Self::SingleKmerColorDataType,
        el: (usize, H::HashTypeUnextendable),
    );

    /// Temporary storage for colors associated with a single kmer in the hashmap (holds the color subset index)
    type HashMapTempColorIndex: 'static + Send + Sync;
    fn new_color_index() -> Self::HashMapTempColorIndex;

    /// This step finds the color subset indexes for each map entry
    fn process_colors(
        global_colors_table: &Self::GlobalColorsTable,
        data: &mut Self::ColorsBufferTempStructure,
        map: &mut HashMap<H::HashTypeUnextendable, MapEntry<Self::HashMapTempColorIndex>>,
        min_multiplicity: usize,
    );

    /// Struct used to hold color information about unitigs
    type PartialUnitigsColorStructure: SequenceExtraData + Clone + 'static;
    /// Struct holding the result of joining multiple partial unitigs to build a final unitig
    type TempUnitigColorStructure: 'static + Send + Sync;

    /// These functions are used to keep track of the colors while producing the partial unitigs
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

    /// Encodes the color data as ident sequence
    fn print_color_data(data: &Self::PartialUnitigsColorStructure, buffer: &mut impl Write);

    fn debug_tucs(str: &Self::TempUnitigColorStructure, seq: &[u8]);
}

pub trait ColorsManager: 'static + Sync + Send + Sized {
    const COLORS_ENABLED: bool;

    type SingleKmerColorDataType;

    type ColorsParserType: ColorsParser<SingleKmerColorDataType = Self::SingleKmerColorDataType>;
    type ColorsMergeManagerType<H: HashFunctionFactory>: ColorsMergeManager<
        H,
        SingleKmerColorDataType = Self::SingleKmerColorDataType,
    >;
}
