use crate::sequences_stream::file::FileSequencesStream;
use crate::sequences_stream::{GenericSequencesStream, SequenceInfo};
use crate::simd::{ParserSettings, SequenceBuffer};
use simd_accel::masks::MaskKernel;
use std::sync::Arc;
use utils::dna_sequence::{DnaSequence, DnaSequencesFileType};

pub trait DynamicSequencesStream: Sync + Send + 'static {
    fn read_block(
        &self,
        block: usize,
        copy_ident_data: bool,
        partial_read_copyback: Option<usize>,
        callback: &mut dyn FnMut(DnaSequence<'_, &[u8]>, SequenceInfo),
    );

    fn estimated_base_count(&self, block: usize) -> u64;
}

pub enum GeneralSequenceBlockData {
    FILE(<FileSequencesStream as GenericSequencesStream>::SequenceBlockData),
    Dynamic((Arc<dyn for<'a> DynamicSequencesStream>, usize)),
}

impl GeneralSequenceBlockData {
    pub fn estimated_bases_count(&self) -> anyhow::Result<u64> {
        match self {
            GeneralSequenceBlockData::FILE(block) => {
                FileSequencesStream::estimated_bases_count(&block.0)
            }
            GeneralSequenceBlockData::Dynamic((reader, block)) => {
                Ok(reader.estimated_base_count(*block))
            }
        }
    }
}

pub struct GeneralSequencesStream {
    file_reader: Option<FileSequencesStream>,
    /// The kernel the parsers of dynamic streams run, chosen once for this CPU.
    kernel: MaskKernel,
}

impl GenericSequencesStream for GeneralSequencesStream {
    type SequenceBlockData = GeneralSequenceBlockData;

    fn new() -> Self {
        Self {
            file_reader: None,
            kernel: MaskKernel::detect(),
        }
    }

    fn read_block(
        &mut self,
        block: &Self::SequenceBlockData,
        settings: &ParserSettings,
        file_info: impl FnMut(&str, SequenceInfo) -> SequenceInfo,
        mut callback: impl FnMut(SequenceBuffer, SequenceInfo) -> SequenceBuffer,
    ) -> u64 {
        match block {
            GeneralSequenceBlockData::FILE(block) => self
                .file_reader
                .get_or_insert_with(FileSequencesStream::new)
                .read_block(block, settings, file_info, callback),
            GeneralSequenceBlockData::Dynamic((reader, index)) => {
                // The stream hands over a record at a time, each with its own format and
                // information, and a buffer holds sequences of only one of each: a record that
                // changes either hands the buffer over first.
                let mut parser = settings.parser(self.kernel);
                let mut current: Option<(DnaSequencesFileType, SequenceInfo)> = None;
                reader.read_block(
                    *index,
                    settings.keep_ident,
                    settings.copyback_size,
                    &mut |sequence, info| {
                        if current != Some((sequence.format, info)) {
                            if let Some((_, current_info)) = current {
                                parser.finish(|buffer| callback(buffer, current_info));
                            }
                            parser.reset_for_records(sequence.format);
                            current = Some((sequence.format, info));
                        }
                        parser.push_record(sequence.ident_data, sequence.seq, |buffer| {
                            callback(buffer, info)
                        });
                    },
                );
                if let Some((_, info)) = current {
                    parser.finish(|buffer| callback(buffer, info));
                }
                parser.records()
            }
        }
    }
}
