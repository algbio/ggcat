use crate::colors_manager::ColorsManager;
use crate::managers::multiple::MultipleColorsManager;
use crate::parsers::separate::SeparateColorsParser;
use config::ColorIndexType;
use hashes::HashFunctionFactory;
use static_dispatch::static_dispatch;

#[derive(Copy, Clone)]
pub struct ColorBundleMultifileBuilding;

#[static_dispatch]
impl ColorsManager for ColorBundleMultifileBuilding {
    const COLORS_ENABLED: bool = true;
    type SingleKmerColorDataType = ColorIndexType;

    type ColorsParserType = SeparateColorsParser;
    type ColorsMergeManagerType<H: HashFunctionFactory> = MultipleColorsManager<H>;
}
