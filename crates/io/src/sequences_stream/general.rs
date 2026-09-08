use crate::sequences_reader::DnaSequence;
use crate::sequences_stream::fasta::FastaFileSequencesStream;
use crate::sequences_stream::{GenericSequencesStream, SequenceInfo};
use std::sync::Arc;

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
    FASTA(<FastaFileSequencesStream as GenericSequencesStream>::SequenceBlockData),
    TAR(super::tar::TarSequenceBlock),
    GFA(),
    Dynamic((Arc<dyn for<'a> DynamicSequencesStream>, usize)),
}

impl GeneralSequenceBlockData {
    pub fn estimated_bases_count(&self) -> anyhow::Result<u64> {
        match self {
            GeneralSequenceBlockData::FASTA(block) => {
                FastaFileSequencesStream::get_estimated_bases_count(&block.0)
            }
            GeneralSequenceBlockData::TAR(block) => {
                FastaFileSequencesStream::get_estimated_bases_count(&block.path)
            }
            GeneralSequenceBlockData::GFA() => {
                todo!()
            }
            GeneralSequenceBlockData::Dynamic((reader, block)) => {
                Ok(reader.estimated_base_count(*block))
            }
        }
    }
}

pub struct GeneralSequencesStream {
    fasta_file_reader: Option<FastaFileSequencesStream>,
}

impl GenericSequencesStream for GeneralSequencesStream {
    type SequenceBlockData = GeneralSequenceBlockData;

    fn new() -> Self {
        Self {
            fasta_file_reader: None,
        }
    }

    fn read_block(
        &mut self,
        block: &Self::SequenceBlockData,
        copy_ident_data: bool,
        partial_read_copyback: Option<usize>,
        mut callback: impl FnMut(DnaSequence<&[u8]>, SequenceInfo),
    ) {
        match block {
            GeneralSequenceBlockData::FASTA(block) => {
                if self.fasta_file_reader.is_none() {
                    self.fasta_file_reader = Some(FastaFileSequencesStream::new());
                }
                self.fasta_file_reader.as_mut().unwrap().read_block(
                    block,
                    copy_ident_data,
                    partial_read_copyback,
                    callback,
                );
            }
            GeneralSequenceBlockData::TAR(block) => {
                let mut reader = crate::sequences_reader::SequencesReader::new();
                if let Err(error) = block.read(
                    &mut reader,
                    copy_ident_data,
                    partial_read_copyback,
                    callback,
                ) {
                    block
                        .registry
                        .lock()
                        .errors
                        .push(format!("Archive {}: {error:#}", block.path.display()));
                }
            }
            GeneralSequenceBlockData::GFA() => {
                unimplemented!();
            }
            GeneralSequenceBlockData::Dynamic((reader, index)) => {
                reader.read_block(
                    *index,
                    copy_ident_data,
                    partial_read_copyback,
                    &mut callback,
                );
            }
        }
    }
}
