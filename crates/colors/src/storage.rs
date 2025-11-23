use crate::storage::serializer::ColorsFlushProcessing;
use config::ColorIndexType;
use parallel_processor::execution_manager::objects_pool::PoolObjectTrait;
use std::io::Read;

pub mod deserializer;
pub mod roaring;
pub mod run_length;
pub mod serializer;

pub trait ColorsSerializerTrait: Sized + Sync + Send + 'static {
    const MAGIC: [u8; 16];

    type PreSerializer: PoolObjectTrait<InitData = ()>;

    type CheckpointTracker: Sized + Send;
    type CheckpointBuffer: Default;
    type CompressedCheckpointBuffer: Default;

    type CheckpointWriter<'a>;

    fn decode_color(reader: impl Read, out_vec: Option<&mut Vec<ColorIndexType>>);
    // fn decode_colors(reader: impl Read) -> ;

    fn new(
        writer: ColorsFlushProcessing,
        checkpoint_distance: usize,
        colors_count: u64,
    ) -> (Self, Self::CheckpointTracker);

    // Preserialize the colors into a temporary buffer to be sent to another processing thread
    fn preserialize_colors(pre_serializer: &mut Self::PreSerializer, colors: &[ColorIndexType]);

    // Write a new color subset to a temporary checkpoint buffer (single-threaded)
    fn write_color_subset<'a>(
        tracker: &mut Self::CheckpointTracker,
        buffer: &'a mut Self::CheckpointBuffer,
        pre_serializer: &Self::PreSerializer,
    ) -> Option<Self::CheckpointWriter<'a>>;

    fn flush_checkpoint(
        &self,
        checkpoint: Self::CheckpointWriter<'_>,
        compressed_buffer: &mut Self::CompressedCheckpointBuffer,
    );
    fn final_flush_buffer(
        &self,
        tracker: &mut Self::CheckpointTracker,
        buffer: Self::CheckpointBuffer,
        compressed_buffer: Self::CompressedCheckpointBuffer,
    );

    fn get_subsets_count(tracker: &mut Self::CheckpointTracker) -> u64;
    fn print_stats(tracker: &Self::CheckpointTracker);
    fn finalize(self, tracker: Self::CheckpointTracker);
}
