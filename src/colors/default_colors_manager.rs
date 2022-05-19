use crate::colors::colors_manager::ColorsManager;
use crate::colors::managers::multiple::MultipleColorsManager;
use crate::colors::parsers::separate::SeparateColorsParser;
use crate::colors::ColorIndexType;
use crate::hashes::HashFunctionFactory;

pub struct SingleSequenceInfo<'a> {
    pub file_index: usize,
    pub sequence_ident: &'a [u8],
}

#[derive(Copy, Clone)]
pub struct DefaultColorsManager;

impl ColorsManager for DefaultColorsManager {
    const COLORS_ENABLED: bool = true;
    type SingleKmerColorDataType = ColorIndexType;

    type ColorsParserType = SeparateColorsParser;
    type ColorsMergeManagerType<H: HashFunctionFactory> = MultipleColorsManager<H>;
}
