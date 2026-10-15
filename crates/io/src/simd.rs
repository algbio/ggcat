//! Sequences read with the SIMD parser of `simd-accel`, from files and archives as
//! [`SequencesReader`](crate::sequences_reader::SequencesReader) reads them.

use crate::binary_reader::{BinaryEvent, GenericBinaryReader, is_archive};
use simd_accel::masks::MaskKernel;
use simd_accel::sequence_parser::{
    BLOCK_SIZE, DEFAULT_BASES_CAPACITY, DEFAULT_IDENT_CAPACITY, SequenceParser,
};
use std::path::Path;
use utils::dna_sequence::DnaSequencesFileType;

/// What the reader hands out, from `simd-accel`: its sequences turn into io's `CompressedRead`
/// with `.into()`.
pub use simd_accel::sequence_buffer::{SequenceBuffer, SequenceDescription};

/// How much the parser gets at once: a whole number of its blocks, as it requires of every
/// slice but an input's last, and many of them, so that the kernel it runs costs a branch per
/// chunk rather than per block.
const CHUNK_SIZE: usize = 64 * 1024;
const _: () = assert!(CHUNK_SIZE.is_multiple_of(BLOCK_SIZE));

pub struct SimdSequencesReader {
    /// Hands the input out in chunks of exactly [`CHUNK_SIZE`] bytes, only the last one of each
    /// file shorter.
    reader: GenericBinaryReader,
    /// The kernel every parser runs, chosen once for this CPU.
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

    /// As [`SequencesReader::process_file_extended`], with the bases packed and handed to
    /// `func` a [`SequenceBuffer`] at a time: the source gets a parser, which every file, or
    /// every member of an archive, is fed to a chunk at a time, after a reset to its format.
    /// Each file's last buffer is handed over at its end; `line_split_copyback` splits the
    /// sequences too long for a buffer, as the line reader's does. The sequences hold only ACGT
    /// bases, split at the other bytes, and those shorter than `min_seqlen` are left out.
    ///
    /// [`SequencesReader::process_file_extended`]: crate::sequences_reader::SequencesReader::process_file_extended
    pub fn process_file_extended<F: FnMut(&SequenceBuffer)>(
        &mut self,
        source: impl AsRef<Path>,
        mut func: F,
        line_split_copyback: Option<usize>,
        copy_ident: bool,
        min_seqlen: usize,
        remove_file: bool,
    ) {
        let source = source.as_ref();
        let mut parser = SequenceParser::with_kernel(
            self.kernel,
            copy_ident,
            line_split_copyback,
            min_seqlen,
            DEFAULT_BASES_CAPACITY,
            DEFAULT_IDENT_CAPACITY,
        );
        // Whether the file being read is a sequence file the parser reads, not one skipped.
        let mut parsing = false;

        let result = self.reader.read_file(source, |event| match event {
            BinaryEvent::FilePath(name) => {
                // The previous file is finished before the next one starts.
                if parsing {
                    parser.finish(&mut func);
                }
                let name = name.unwrap_or_default();
                parsing = match DnaSequencesFileType::from_name(name) {
                    Some(format @ (DnaSequencesFileType::FASTA | DnaSequencesFileType::FASTQ)) => {
                        parser.reset(format);
                        true
                    }
                    Some(DnaSequencesFileType::GFA) => todo!(),
                    // Archives can hold other files too, such as their documentation.
                    None if is_archive(source) => {
                        ggcat_logging::info!("Skipping {name}: unsupported sequence format");
                        false
                    }
                    None => panic!("Cannot recognize file type of '{}'", source.display()),
                };
            }
            BinaryEvent::Chunk(chunk) => {
                if parsing {
                    parser.parse_data(chunk, &mut func);
                }
            }
            BinaryEvent::Extra { .. } => {}
        });
        if parsing {
            parser.finish(&mut func);
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
