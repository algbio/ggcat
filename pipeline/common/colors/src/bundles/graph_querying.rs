use crate::colors_manager::ColorsManager;
use crate::managers::single::SingleColorManager;
use crate::parsers::graph::GraphColorsParser;
use config::{BucketIndexType, ColorIndexType};
use hashes::HashFunctionFactory;
use static_dispatch::static_dispatch;
use std::cmp::min;

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
        min(
            (1 << buckets_count_log) - 1,
            *color as u64 * (1 << buckets_count_log) / colors_count,
        ) as BucketIndexType
    }

    type ColorsParserType = GraphColorsParser;
    type ColorsMergeManagerType<H: HashFunctionFactory> = SingleColorManager<H>;
}
