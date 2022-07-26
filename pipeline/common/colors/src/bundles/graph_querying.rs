use crate::colors_manager::ColorsManager;
use crate::managers::single::SingleColorManager;
use crate::parsers::graph::GraphColorsParser;
use config::{BucketIndexType, ColorIndexType, COLORS_SINGLE_BATCH_SIZE};
use hashes::HashFunctionFactory;
use static_dispatch::static_dispatch;

#[derive(Copy, Clone)]
pub struct ColorBundleGraphQuerying;

#[static_dispatch]
impl ColorsManager for ColorBundleGraphQuerying {
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

    type ColorsParserType = GraphColorsParser;
    type ColorsMergeManagerType<H: HashFunctionFactory> = SingleColorManager<H>;
}
