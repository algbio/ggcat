use crate::colors_manager::ColorsManager;
use crate::managers::single::SingleColorManager;
use crate::parsers::graph::GraphColorsParser;
use config::ColorIndexType;
use hashes::HashFunctionFactory;

#[derive(Copy, Clone)]
pub struct ColorBundleGraphQuerying;

impl ColorsManager for ColorBundleGraphQuerying {
    const COLORS_ENABLED: bool = true;
    type SingleKmerColorDataType = ColorIndexType;

    type ColorsParserType = GraphColorsParser;
    type ColorsMergeManagerType<H: HashFunctionFactory> = SingleColorManager<H>;
}
