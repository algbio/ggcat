use colors::colors_manager::{ColorsManager, color_types::PartialUnitigsColorStructure};
use io::{
    compressed_read::CompressedRead,
    concurrent::temp_reads::extra_data::SequenceExtraDataTempBufferManagement,
    partial_unitigs_extra_data::{IndirectReadInfo, PartialUnitigExtraData, PartialUnitigMode},
};

pub struct JoinedSequence<'a, CX: ColorsManager> {
    pub sequence: CompressedRead<'a>,
    pub extra: PartialUnitigExtraData<PartialUnitigsColorStructure<CX>>,
    pub extra_buffer: &'a <PartialUnitigExtraData<PartialUnitigsColorStructure<CX>> as SequenceExtraDataTempBufferManagement>::TempBuffer,
}

pub struct IndirectSequencesJoiner<CX: ColorsManager> {
    bases_buffer: Vec<u8>,
    bases_count: usize,
    extra: PartialUnitigExtraData<PartialUnitigsColorStructure<CX>>,
    extra_buffer: <PartialUnitigExtraData<PartialUnitigsColorStructure<CX>> as SequenceExtraDataTempBufferManagement>::TempBuffer,
}

impl<CX: ColorsManager> IndirectSequencesJoiner<CX> {
    pub fn new() -> Self {
        todo!()
    }

    pub fn reset(&mut self) {
        self.bases_buffer.clear();
        self.bases_count = 0;
        self.extra = PartialUnitigExtraData {
            colors: Default::default(),
            mode: PartialUnitigMode::Inline,
        };
        PartialUnitigExtraData::<PartialUnitigsColorStructure<CX>>::clear_temp_buffer(
            &mut self.extra_buffer,
        );
    }

    pub fn append_sequence(
        &mut self,
        sequence: CompressedRead,
        extra: &PartialUnitigExtraData<PartialUnitigsColorStructure<CX>>,
        colors_range: (usize, Option<usize>),
        is_rc: bool,
        extra_buffer: &<PartialUnitigExtraData<PartialUnitigsColorStructure<CX>> as SequenceExtraDataTempBufferManagement>::TempBuffer,
    ) {
    }

    pub fn append_indirect(&mut self, indirect: IndirectReadInfo) {}

    pub fn get_sequence<'a>(&'a self) -> JoinedSequence<'a, CX> {
        JoinedSequence {
            sequence: CompressedRead::new_from_compressed(&self.bases_buffer, self.bases_count),
            extra: self.extra.clone(),
            extra_buffer: &self.extra_buffer,
        }
    }
}
