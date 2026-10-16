use ggcat_logging::UnrecoverableErrorLogging;

use crate::sequences_stream::{GenericSequencesStream, SequenceInfo};
use crate::simd::{ParserSettings, SequenceBuffer, SimdSequencesReader};
use std::path::PathBuf;

pub struct FileSequencesStream {
    sequences_reader: SimdSequencesReader,
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
            sequences_reader: SimdSequencesReader::new(),
        }
    }

    fn read_block(
        &mut self,
        block: &Self::SequenceBlockData,
        settings: &ParserSettings,
        mut callback: impl FnMut(SequenceBuffer, SequenceInfo) -> SequenceBuffer,
    ) -> u64 {
        let info = SequenceInfo { color: block.1 };
        self.sequences_reader.process_file_extended(
            &block.0,
            settings,
            |buffer| callback(buffer, info),
            false,
        )
    }
}
