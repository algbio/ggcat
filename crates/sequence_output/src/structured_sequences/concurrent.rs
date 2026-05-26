use colors::colors_manager::{ColorsManager, color_types::PartialUnitigsColorStructure};
use hashes::HashableSequence;

use io::{
    compressed_read::{CompressedRead, CompressedReadIndipendent},
    concurrent::temp_reads::extra_data::SequenceExtraDataTempBufferManagement,
    concurrent_filewriter::ConcurrentFileWriter,
    ident_writer::IdentSequenceWriter,
    partial_unitigs_extra_data::PartialUnitigExtraData,
};

use crate::structured_sequences::{StructuredSequenceBackend, StructuredSequenceWriter};

use super::SequenceAbundanceType;

pub struct FastaWriterConcurrentBuffer<
    'a,
    CX: ColorsManager,
    LinksInfo: IdentSequenceWriter,
    Backend: StructuredSequenceBackend<CX, LinksInfo>,
> {
    target: &'a StructuredSequenceWriter<CX, LinksInfo, Backend>,
    sequences: Vec<(
        CompressedReadIndipendent,
        PartialUnitigExtraData<PartialUnitigsColorStructure<CX>>,
        LinksInfo,
        SequenceAbundanceType,
    )>,
    seq_buf: Vec<u8>,
    extra_buffers: (
        <PartialUnitigExtraData<PartialUnitigsColorStructure<CX>> as SequenceExtraDataTempBufferManagement>::TempBuffer,
        LinksInfo::TempBuffer,
    ),
    temp_buffer: Backend::SequenceTempBuffer,
    current_index: Option<u64>,
    auto_flush: bool,
}

impl<
    'a,
    CX: ColorsManager,
    LinksInfo: IdentSequenceWriter,
    Backend: StructuredSequenceBackend<CX, LinksInfo>,
> FastaWriterConcurrentBuffer<'a, CX, LinksInfo, Backend>
{
    pub fn new(
        target: &'a StructuredSequenceWriter<CX, LinksInfo, Backend>,
        max_size: usize,
        auto_flush: bool,
        k: usize,
    ) -> Self {
        Self {
            target,
            sequences: Vec::with_capacity(max_size / 128),
            seq_buf: Vec::with_capacity(max_size),
            extra_buffers: (
                PartialUnitigExtraData::<PartialUnitigsColorStructure<CX>>::new_temp_buffer(),
                LinksInfo::new_temp_buffer(),
            ),
            temp_buffer: Backend::alloc_temp_buffer(k),
            current_index: None,
            auto_flush,
        }
    }

    pub fn flush(&mut self, indirect_file: Option<&ConcurrentFileWriter>) -> u64 {
        if self.sequences.len() == 0 {
            return 0;
        }

        let first_read_index = self.target.write_sequences(
            &mut self.temp_buffer,
            self.current_index.map(|c| c - self.sequences.len() as u64),
            self.sequences
                .drain(..)
                .map(|(slice, extra, link, abundance)| {
                    (slice.as_reference(&self.seq_buf), extra, link, abundance)
                }),
            &self.extra_buffers,
            indirect_file,
        );

        PartialUnitigExtraData::<PartialUnitigsColorStructure<CX>>::clear_temp_buffer(
            &mut self.extra_buffers.0,
        );
        LinksInfo::clear_temp_buffer(&mut self.extra_buffers.1);
        self.seq_buf.clear();

        first_read_index
    }

    #[inline(always)]
    fn will_overflow(vec: &Vec<u8>, len: usize) -> bool {
        vec.len() > 0 && (vec.len() + len > vec.capacity())
    }

    pub fn add_read(
        &mut self,
        sequence: CompressedRead,
        sequence_index: Option<u64>,
        extra_data: PartialUnitigExtraData<PartialUnitigsColorStructure<CX>>,
        extra_buffer: &<PartialUnitigExtraData<PartialUnitigsColorStructure<CX>> as SequenceExtraDataTempBufferManagement>::TempBuffer,
        links: LinksInfo,
        links_extra_buffer: &LinksInfo::TempBuffer,
        indirect_file: Option<&ConcurrentFileWriter>,
    ) -> Option<u64> {
        let mut result = None;
        let mut different_index = false;

        if let Some(sequence_index) = sequence_index {
            if Some(sequence_index) != self.current_index {
                result = Some(self.flush(indirect_file));
                self.current_index = Some(sequence_index);
                different_index = true;
            }
        }

        if !different_index
            && self.auto_flush
            && Self::will_overflow(&self.seq_buf, sequence.bases_count().div_ceil(4))
        {
            result = Some(self.flush(indirect_file));
        }

        let extra_data =
            PartialUnitigExtraData::<PartialUnitigsColorStructure<CX>>::copy_extra_from(
                extra_data,
                &extra_buffer,
                &mut self.extra_buffers.0,
            );
        let links =
            LinksInfo::copy_extra_from(links, &links_extra_buffer, &mut self.extra_buffers.1);

        let sequence = CompressedReadIndipendent::from_read::<false>(&sequence, &mut self.seq_buf);
        self.sequences.push((
            sequence,
            extra_data,
            links,
            match () {
                #[cfg(feature = "support_kmer_counters")]
                () => extra_data.counters,
                #[cfg(not(feature = "support_kmer_counters"))]
                () => (),
            },
        ));

        if let Some(current_index) = &mut self.current_index {
            *current_index += 1;
        }

        result
    }

    pub fn finalize(mut self, indirect_file: Option<&ConcurrentFileWriter>) -> u64 {
        self.flush(indirect_file)
    }
}

impl<
    'a,
    CX: ColorsManager,
    LinksInfo: IdentSequenceWriter,
    Backend: StructuredSequenceBackend<CX, LinksInfo>,
> Drop for FastaWriterConcurrentBuffer<'a, CX, LinksInfo, Backend>
{
    fn drop(&mut self) {
        assert_eq!(self.sequences.len(), 0);
    }
}
