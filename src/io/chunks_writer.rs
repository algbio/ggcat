use crate::colors::ColorIndexType;
use std::io::Write;

pub trait ChunksWriter {
    type ProcessingData;
    type TargetData;
    type StreamType<'a>
    where
        Self: 'a;

    fn start_processing(&self) -> Self::ProcessingData;
    fn flush_data(&self, tmp_data: &mut Self::ProcessingData, data: &[Self::TargetData]);
    fn get_stream<'a>(&'a self, tmp_data: &'a mut Self::ProcessingData) -> Self::StreamType<'a>;

    fn end_processing(
        &self,
        tmp_data: Self::ProcessingData,
        start_index: ColorIndexType,
        stride: ColorIndexType,
    );
}
