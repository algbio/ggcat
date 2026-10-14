//! Sequences read with the SIMD parser of `simd-accel`, from files and archives as
//! [`SequencesReader`](crate::sequences_reader::SequencesReader) reads them.

use crate::binary_reader::{BinaryEvent, GenericBinaryReader, is_archive};
use crate::compressed_read::CompressedRead;
use simd_accel::compressed_read::SimdCompressedRead;
use simd_accel::masks::MaskKernel;
use simd_accel::sequence_parser::{BLOCK_SIZE, SequenceParser};
use std::path::Path;
use utils::dna_sequence::{DnaSequence, DnaSequencesFileType};

/// How much the parser gets at once: a whole number of its blocks, so that the kernel it runs
/// costs a branch per chunk rather than per block.
const CHUNK_SIZE: usize = 64 * 1024;
const _: () = assert!(CHUNK_SIZE.is_multiple_of(BLOCK_SIZE));

pub struct SimdSequencesReader {
    /// Hands the input out in chunks of exactly [`CHUNK_SIZE`] bytes, only the last one of each
    /// file shorter.
    reader: GenericBinaryReader,
    /// The kernel every file's parser runs, chosen once for this CPU.
    kernel: MaskKernel,
}

impl SimdSequencesReader {
    pub fn new() -> Self {
        Self {
            reader: GenericBinaryReader::new(Some(CHUNK_SIZE)),
            kernel: MaskKernel::detect(),
        }
    }

    /// The kernel the parsers run.
    pub fn kernel(&self) -> MaskKernel {
        self.kernel
    }

    /// As [`SequencesReader::process_file_extended`], with the bases packed: every file, or
    /// every member of an archive, is fed to a parser of its own a chunk at a time.
    ///
    /// [`SequencesReader::process_file_extended`]: crate::sequences_reader::SequencesReader::process_file_extended
    pub fn process_file_extended<F: for<'a> FnMut(DnaSequence<'a, CompressedRead<'a>>)>(
        &mut self,
        source: impl AsRef<Path>,
        mut func: F,
        line_split_copyback: Option<usize>,
        copy_ident: bool,
        remove_file: bool,
    ) {
        // TODO: the parser takes no options yet.
        let _ = (line_split_copyback, copy_ident);
        let source = source.as_ref();
        let kernel = self.kernel;

        let mut emit = |sequence: DnaSequence<'_, SimdCompressedRead<'_>>| {
            func(DnaSequence {
                ident_data: sequence.ident_data,
                seq: sequence.seq.into(),
                format: sequence.format,
            })
        };
        // The parser of the file being read, `None` for a file skipped.
        let mut parser: Option<SequenceParser> = None;

        let result = self.reader.read_file(source, |event| match event {
            BinaryEvent::FilePath(name) => {
                // The previous file is finished before the next one starts.
                if let Some(parser) = &mut parser {
                    parser.finish(&mut emit);
                }
                let name = name.unwrap_or_default();
                parser = match DnaSequencesFileType::from_name(name) {
                    Some(DnaSequencesFileType::FASTA) => Some(SequenceParser::with_kernel(kernel)),
                    Some(DnaSequencesFileType::FASTQ) => {
                        todo!("the SIMD parser reads FASTA only")
                    }
                    Some(DnaSequencesFileType::GFA) => todo!(),
                    // Archives can hold other files too, such as their documentation.
                    None if is_archive(source) => {
                        ggcat_logging::info!("Skipping {name}: unsupported sequence format");
                        None
                    }
                    None => panic!("Cannot recognize file type of '{}'", source.display()),
                };
            }
            BinaryEvent::Chunk(chunk) => {
                if let Some(parser) = &mut parser {
                    parser.parse_data(chunk, &mut emit);
                }
            }
            BinaryEvent::Extra { .. } => {}
        });
        if let Some(parser) = &mut parser {
            parser.finish(&mut emit);
        }
        if let Err(error) = result {
            ggcat_logging::error!(
                "WARNING: Error while reading file {}: {error:#}",
                source.display()
            );
        }

        if remove_file {
            std::fs::remove_file(source).unwrap();
        }
    }
}
