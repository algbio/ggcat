use std::sync::Arc;

use colors::colors_manager::{
    ColorsManager,
    color_types::{
        GlobalColorsTableWriter, MinimizerBucketingSeqColorDataType, PartialUnitigsColorStructure,
        TempUnitigColorStructure,
    },
};
use hashes::HashFunctionFactory;
use io::concurrent::temp_reads::extra_data::SequenceExtraDataTempBufferManagement;
use kmers_transform::{GroupProcessStats, reads_buffer::DeserializedReadIndependent};

pub mod hashmap;

#[derive(Clone, Copy, Debug)]
pub struct GlobalExtenderParams {
    pub k: usize,
    pub m: usize,
    pub min_multiplicity: usize,
}

pub struct UnitigExtensionColorsData<CX: ColorsManager> {
    pub colors_global_table: Arc<GlobalColorsTableWriter<CX>>,
    pub unitigs_temp_colors: TempUnitigColorStructure<CX>,
    pub temp_color_buffer:
        <PartialUnitigsColorStructure<CX> as SequenceExtraDataTempBufferManagement>::TempBuffer,
}

pub trait UnitigsExtenderTrait<MH: HashFunctionFactory, CX: ColorsManager> {
    fn new(params: &GlobalExtenderParams) -> Self;
    fn reset(&mut self);
    fn get_memory_usage(&self) -> usize;
    fn add_sequence(
        &mut self,
        sequences_data: &[u8],
        extra_buffer: &<MinimizerBucketingSeqColorDataType<CX> as SequenceExtraDataTempBufferManagement>::TempBuffer,
        sequence: DeserializedReadIndependent<MinimizerBucketingSeqColorDataType<CX>>,
    );
    fn get_stats(&self) -> GroupProcessStats;
    fn compute_unitigs<const COMPUTE_SIMPLITIGS: bool>(
        &mut self,
        colors_manager: &mut UnitigExtensionColorsData<CX>,
        output_unitig: impl FnMut(
            &mut UnitigExtensionColorsData<CX>,
            &[u8],
            Option<MH::HashTypeUnextendable>,
            Option<MH::HashTypeUnextendable>,
        ),
    );
    fn set_suggested_sizes(&mut self, hashmap_size: u64, sequences_size: u64);
}
