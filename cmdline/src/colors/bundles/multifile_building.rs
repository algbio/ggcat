use crate::colors::colors_manager::ColorsManager;
use crate::colors::managers::multiple::MultipleColorsManager;
use crate::colors::parsers::separate::SeparateColorsParser;
use crate::colors::ColorIndexType;
use crate::hashes::HashFunctionFactory;

#[derive(Copy, Clone)]
pub struct ColorBundleMultifileBuilding;

impl ColorsManager for ColorBundleMultifileBuilding {
    const COLORS_ENABLED: bool = true;
    type SingleKmerColorDataType = ColorIndexType;

    type ColorsParserType = SeparateColorsParser;
    type ColorsMergeManagerType<H: HashFunctionFactory> = MultipleColorsManager<H>;
}
