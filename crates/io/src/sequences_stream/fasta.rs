use anyhow::Context;
use ggcat_logging::UnrecoverableErrorLogging;

use crate::raw_reader::RawBytesReader;
use crate::sequences_reader::{DnaSequence, SequencesReader};
use crate::sequences_sink::SequencesSink;
use crate::sequences_stream::{GenericSequencesStream, SequenceInfo};
use std::path::PathBuf;

pub struct FastaFileSequencesStream {
    sequences_reader: SequencesReader,
    raw_reader: RawBytesReader,
}

impl FastaFileSequencesStream {
    pub fn get_estimated_bases_count(file: &PathBuf) -> anyhow::Result<u64> {
        let archive_path = super::tar::split_archive_member(file).map(|(path, _)| path);
        let file = archive_path.as_ref().unwrap_or(file);
        crate::input_size::estimate(file)
            .log_unrecoverable_error_with_data("Error while estimating input size", file.display())
    }
}

impl GenericSequencesStream for FastaFileSequencesStream {
    type SequenceBlockData = (PathBuf, Option<u32>);

    fn new() -> Self {
        Self {
            sequences_reader: SequencesReader::new(),
            raw_reader: RawBytesReader::new(),
        }
    }

    fn read_block(
        &mut self,
        block: &Self::SequenceBlockData,
        copy_ident_data: bool,
        partial_read_copyback: Option<usize>,
        mut callback: impl FnMut(DnaSequence<&[u8]>, SequenceInfo),
    ) {
        if let Some((path, _)) = super::tar::split_archive_member(&block.0) {
            super::tar::TarSequenceBlock::new(path, block.1)
                .read(
                    &mut self.sequences_reader,
                    copy_ident_data,
                    partial_read_copyback,
                    callback,
                )
                .unwrap_or_else(|error| {
                    panic!("Error reading archive {}: {error:#}", block.0.display())
                });
            return;
        }
        self.sequences_reader.process_file_extended(
            &block.0,
            |x| callback(x, SequenceInfo { color: block.1 }),
            partial_read_copyback,
            copy_ident_data,
            false,
        );
    }

    fn read_block_into(
        &mut self,
        block: &Self::SequenceBlockData,
        sink: &mut impl SequencesSink,
    ) -> anyhow::Result<()> {
        if let Some((path, member)) = super::tar::split_archive_member(&block.0) {
            let mut archive = super::tar::TarSequenceBlock::new(
                path,
                if member.is_none() { block.1 } else { None },
            );
            if let (Some(member), Some(color)) = (member, block.1) {
                archive.member_colors.insert(member, color);
            }
            return archive.read_into(&mut self.raw_reader, sink);
        }

        let format = SequencesReader::file_type(&block.0)
            .with_context(|| format!("Cannot recognize file type of '{}'", block.0.display()))?;
        sink.begin_stream(format, SequenceInfo { color: block.1 });
        let result = self
            .raw_reader
            .read_file(&block.0, |bytes| sink.push_bytes(bytes));
        sink.end_stream();
        result
    }
}
