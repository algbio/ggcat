use ggcat_logging::UnrecoverableErrorLogging;

use crate::sequences_reader::{DnaSequence, SequencesReader};
use crate::sequences_stream::{GenericSequencesStream, SequenceInfo};
use std::path::PathBuf;

pub struct FileSequencesStream {
    sequences_reader: SequencesReader,
}

impl FileSequencesStream {
    /// The decoded size of `file`, from its compression metadata (see [`crate::input_size`]).
    pub fn estimated_bases_count(file: &PathBuf) -> anyhow::Result<u64> {
        crate::input_size::estimate(file)
            .log_unrecoverable_error_with_data("Error while estimating input size", file.display())
    }
}

impl GenericSequencesStream for FileSequencesStream {
    type SequenceBlockData = (PathBuf, Option<u32>);

    fn new() -> Self {
        Self {
            sequences_reader: SequencesReader::new(),
        }
    }

    fn read_block(
        &mut self,
        block: &Self::SequenceBlockData,
        copy_ident_data: bool,
        partial_read_copyback: Option<usize>,
        mut callback: impl FnMut(DnaSequence<&[u8]>, SequenceInfo),
    ) {
        self.sequences_reader.process_file_extended(
            &block.0,
            |x| callback(x, SequenceInfo { color: block.1 }),
            partial_read_copyback,
            copy_ident_data,
            false,
        );
    }
}
