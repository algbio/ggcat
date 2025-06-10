use crate::colors_manager::ColorsManager;
use crate::managers::single::SingleColorManager;
use crate::parsers::graph::GraphColorsParser;
use config::{BucketIndexType, COLORS_SINGLE_BATCH_SIZE, ColorIndexType};
use dynamic_dispatch::dynamic_dispatch;

#[derive(Copy, Clone, Debug)]
pub struct ColorBundleGraphQuerying;

#[dynamic_dispatch]
impl ColorsManager for ColorBundleGraphQuerying {
    const COLORS_ENABLED: bool = true;
    type SingleKmerColorDataType = ColorIndexType;

    #[inline(always)]
    fn get_bucket_from_color(
        color: &Self::SingleKmerColorDataType,
        colors_count: u64,
        buckets_count_log: usize,
    ) -> BucketIndexType {
        Self::get_bucket_from_u64_color(
            *color as u64,
            colors_count,
            buckets_count_log,
            COLORS_SINGLE_BATCH_SIZE,
        )
    }

    type ColorsParserType = GraphColorsParser;
    type ColorsMergeManagerType = SingleColorManager;
}
