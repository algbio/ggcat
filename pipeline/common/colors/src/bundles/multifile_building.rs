use crate::colors_manager::ColorsManager;
use crate::managers::multiple::MultipleColorsManager;
use crate::parsers::separate::SeparateColorsParser;
use config::{BucketIndexType, ColorIndexType, COLORS_SINGLE_BATCH_SIZE};
use dynamic_dispatch::dynamic_dispatch;
use hashes::{HashFunctionFactory, MinimizerHashFunctionFactory};

#[derive(Copy, Clone)]
pub struct ColorBundleMultifileBuilding;

#[dynamic_dispatch]
impl ColorsManager for ColorBundleMultifileBuilding {
    const COLORS_ENABLED: bool = true;
    type SingleKmerColorDataType = ColorIndexType;

    #[inline(always)]
    fn get_bucket_from_color(
        color: &Self::SingleKmerColorDataType,
        colors_count: u64,
        buckets_count_log: u32,
    ) -> BucketIndexType {
        Self::get_bucket_from_u64_color(
            *color as u64,
            colors_count,
            buckets_count_log,
            COLORS_SINGLE_BATCH_SIZE,
        )
    }

    type ColorsParserType = SeparateColorsParser;
    type ColorsMergeManagerType<H: MinimizerHashFunctionFactory, MH: HashFunctionFactory> =
        MultipleColorsManager<H, MH>;
}
