use crate::colors::storage::serializer::{ColorsFlushProcessing, ColorsIndexEntry};
use crate::colors::ColorIndexType;
use crate::io::chunks_writer::ChunksWriter;
use std::io::Read;
use std::path::Path;

pub mod roaring;
pub mod run_length;
pub mod serializer;

pub trait ColorsSerializerImpl {
    fn decode_color(
        reader: impl Read,
        entry_info: ColorsIndexEntry,
        color: ColorIndexType,
    ) -> Vec<ColorIndexType>;
    // fn decode_colors(reader: impl Read) -> ;

    fn new(writer: ColorsFlushProcessing, checkpoint_distance: usize, colors_count: u64) -> Self;
    fn serialize_colors(&self, colors: &[ColorIndexType]) -> ColorIndexType;
    fn get_subsets_count(&self) -> u64;
    fn print_stats(&self);
    fn finalize(self) -> ColorsFlushProcessing;
}
