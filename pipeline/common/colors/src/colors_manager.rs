use crate::parsers::SingleSequenceInfo;
use config::BucketIndexType;
use hashbrown::HashMap;
use hashes::HashFunctionFactory;
use io::concurrent::temp_reads::extra_data::SequenceExtraData;
use static_dispatch::static_dispatch;
use std::cmp::min;
use std::hash::Hash;
use std::io::Write;
use std::ops::Range;
use std::path::Path;
use structs::map_entry::MapEntry;

pub mod color_types {
    #![allow(dead_code)]

    use crate::colors_manager::{ColorsManager, ColorsMergeManager, ColorsParser};

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

    color_manager_type_alias!(GlobalColorsTableWriter);
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
    Default + Clone + SequenceExtraData + Send + Sync + 'static
{
    type KmerColor;
    type KmerColorIterator<'a>: Iterator<Item = Self::KmerColor>
    where
        Self: 'a;

    fn create(file_info: SingleSequenceInfo, buffer: &mut Self::TempBuffer) -> Self;
    fn get_iterator<'a>(&'a self, buffer: &'a Self::TempBuffer) -> Self::KmerColorIterator<'a>;
    fn get_subslice(&self, range: Range<usize>) -> Self;

    fn debug_count(&self) -> usize {
        0
    }
}

pub trait ColorMapReader {
    fn colors_count(&self) -> u64;
}

impl ColorMapReader for () {
    fn colors_count(&self) -> u64 {
        0
    }
}

/// Helper trait to manage colors parsing from different sources (actually 2, color from file or color from annotated dbg graph)
pub trait ColorsParser: Sized {
    type SingleKmerColorDataType: Clone
        + Eq
        + PartialEq
        + Ord
        + PartialOrd
        + SequenceExtraData<TempBuffer = ()>
        + Hash
        + Eq
        + Sync
        + Send
        + 'static;
    type MinimizerBucketingSeqColorDataType: MinimizerBucketingSeqColorData<
        KmerColor = Self::SingleKmerColorDataType,
    >;
}

/// Helper trait to manage colors labeling on KmersMerge step
pub trait ColorsMergeManager<H: HashFunctionFactory>: Sized {
    type SingleKmerColorDataType: Clone
        + Eq
        + PartialEq
        + Ord
        + PartialOrd
        + SequenceExtraData<TempBuffer = ()>
        + Hash
        + Eq
        + Sync
        + Send
        + 'static;
    type GlobalColorsTableWriter: Sync + Send + 'static;
    type GlobalColorsTableReader: ColorMapReader + Sync + Send + 'static;

    /// Creates a new colors table at the given path
    fn create_colors_table(
        path: impl AsRef<Path>,
        color_names: Vec<String>,
    ) -> Self::GlobalColorsTableWriter;

    /// Creates a new colors table at the given path
    fn open_colors_table(path: impl AsRef<Path>) -> Self::GlobalColorsTableReader;

    /// Prints to stdout the final stats for the colors table
    fn print_color_stats(global_colors_table: &Self::GlobalColorsTableWriter);

    /// Temporary buffer that holds color values for each kmer while merging them
    type ColorsBufferTempStructure: 'static + Send + Sync;
    fn allocate_temp_buffer_structure() -> Self::ColorsBufferTempStructure;
    fn reinit_temp_buffer_structure(data: &mut Self::ColorsBufferTempStructure);
    fn add_temp_buffer_structure_el(
        data: &mut Self::ColorsBufferTempStructure,
        kmer_color: &Self::SingleKmerColorDataType,
        el: (usize, H::HashTypeUnextendable),
        entry: &mut MapEntry<Self::HashMapTempColorIndex>,
    );

    /// Temporary storage for colors associated with a single kmer in the hashmap (holds the color subset index)
    type HashMapTempColorIndex: 'static + Send + Sync;
    fn new_color_index() -> Self::HashMapTempColorIndex;

    /// This step finds the color subset indexes for each map entry
    fn process_colors(
        global_colors_table: &Self::GlobalColorsTableWriter,
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
        src_buffer: &<Self::PartialUnitigsColorStructure as SequenceExtraData>::TempBuffer,
        skip: u64,
    );

    fn pop_base(target: &mut Self::TempUnitigColorStructure);

    /// Encodes partial unitig colors into the extra data structure
    fn encode_part_unitigs_colors(
        ts: &mut Self::TempUnitigColorStructure,
        colors_buffer: &mut <Self::PartialUnitigsColorStructure as SequenceExtraData>::TempBuffer,
    ) -> Self::PartialUnitigsColorStructure;

    /// Encodes the color data as ident sequence
    fn print_color_data(
        color: &Self::PartialUnitigsColorStructure,
        color_buffer: &<Self::PartialUnitigsColorStructure as SequenceExtraData>::TempBuffer,
        buffer: &mut impl Write,
    );

    fn debug_tucs(str: &Self::TempUnitigColorStructure, seq: &[u8]);
}

#[static_dispatch]
pub trait ColorsManager: 'static + Sync + Send + Sized {
    const COLORS_ENABLED: bool;

    type SingleKmerColorDataType: Clone
        + Eq
        + PartialEq
        + Ord
        + PartialOrd
        + SequenceExtraData<TempBuffer = ()>
        + Hash
        + Eq
        + Sync
        + Send
        + 'static;

    #[inline(always)]
    fn get_bucket_from_u64_color(
        color: u64,
        colors_count: u64,
        buckets_count_log: u32,
        stride: u64,
    ) -> BucketIndexType {
        let colors_count = colors_count.div_ceil(stride) * stride;

        min(
            (1 << buckets_count_log) - 1,
            color * (1 << buckets_count_log) / colors_count,
        ) as BucketIndexType
    }

    fn get_bucket_from_color(
        color: &Self::SingleKmerColorDataType,
        colors_count: u64,
        buckets_count_log: u32,
    ) -> BucketIndexType;

    type ColorsParserType: ColorsParser<SingleKmerColorDataType = Self::SingleKmerColorDataType>;
    type ColorsMergeManagerType<H: HashFunctionFactory>: ColorsMergeManager<
        H,
        SingleKmerColorDataType = Self::SingleKmerColorDataType,
    >;
}
