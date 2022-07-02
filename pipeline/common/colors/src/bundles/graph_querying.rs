use crate::colors_manager::ColorsManager;
use crate::managers::single::SingleColorManager;
use crate::parsers::graph::GraphColorsParser;
use config::ColorIndexType;
use hashes::HashFunctionFactory;
use static_dispatch::static_dispatch;

#[derive(Copy, Clone)]
pub struct ColorBundleGraphQuerying;

#[static_dispatch]
impl ColorsManager for ColorBundleGraphQuerying {
    const COLORS_ENABLED: bool = true;
    type SingleKmerColorDataType = ColorIndexType;

    type ColorsParserType = GraphColorsParser;
    type ColorsMergeManagerType<H: HashFunctionFactory> = SingleColorManager<H>;
}
