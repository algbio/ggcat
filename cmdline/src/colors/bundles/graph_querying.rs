use crate::colors::colors_manager::ColorsManager;
use crate::colors::managers::single::SingleColorManager;
use crate::colors::parsers::graph::GraphColorsParser;
use crate::colors::ColorIndexType;
use crate::hashes::HashFunctionFactory;

#[derive(Copy, Clone)]
pub struct ColorBundleGraphQuerying;

impl ColorsManager for ColorBundleGraphQuerying {
    const COLORS_ENABLED: bool = true;
    type SingleKmerColorDataType = ColorIndexType;

    type ColorsParserType = GraphColorsParser;
    type ColorsMergeManagerType<H: HashFunctionFactory> = SingleColorManager<H>;
}
