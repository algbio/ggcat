use ggcat_logging::UnrecoverableErrorLogging;

use crate::sequences_stream::{GenericSequencesStream, SequenceInfo};
use crate::simd::{ParserSettings, SequenceBuffer, SimdSequencesReader};
use std::cell::Cell;
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
        mut file_info: impl FnMut(&str, SequenceInfo) -> SequenceInfo,
        mut callback: impl FnMut(SequenceBuffer, SequenceInfo) -> SequenceBuffer,
    ) -> u64 {
        let block_info = SequenceInfo { color: block.1 };
        // That of the file being read: each file's buffers are handed over before the next one
        // starts.
        let info = Cell::new(block_info);
        self.sequences_reader.process_file_with_names(
            &block.0,
            settings,
            |name| info.set(file_info(name, block_info)),
            |buffer| callback(buffer, info.get()),
            false,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::simd::{DEFAULT_BASES_CAPACITY, DEFAULT_IDENT_CAPACITY};

    /// A tar archive of `files`, each a name and its content.
    fn archive(files: &[(&str, &str)]) -> Vec<u8> {
        let mut builder = tar::Builder::new(Vec::new());
        for (name, data) in files {
            let mut header = tar::Header::new_gnu();
            header.set_size(data.len() as u64);
            header.set_mode(0o644);
            header.set_cksum();
            builder.append_data(&mut header, name, data.as_bytes()).unwrap();
        }
        builder.into_inner().unwrap()
    }

    /// The names `file_info` gets, with the information it is given, when it gives the n-th
    /// file the colour `10 * n`; and each buffer's sequences, with their colour.
    fn read(block: &(PathBuf, Option<u32>)) -> (Vec<(String, SequenceInfo)>, Vec<(usize, u32)>) {
        let settings = ParserSettings {
            keep_ident: false,
            copyback_size: None,
            min_seqlen: 1,
            record_indices: false,
            bases_capacity: DEFAULT_BASES_CAPACITY,
            ident_capacity: DEFAULT_IDENT_CAPACITY,
        };
        let (mut files, mut buffers) = (Vec::new(), Vec::new());
        FileSequencesStream::new().read_block(
            block,
            &settings,
            |name, info| {
                files.push((name.to_owned(), info));
                SequenceInfo {
                    color: Some(10 * files.len() as u32),
                }
            },
            |buffer, info| {
                buffers.push((buffer.descriptions().len(), info.color.unwrap()));
                SequenceBuffer::new()
            },
        );
        (files, buffers)
    }

    #[test]
    fn each_file_read_gets_the_information_given_for_its_name() {
        let directory =
            std::env::temp_dir().join(format!("ggcat-file-stream-{}", std::process::id()));
        std::fs::create_dir_all(&directory).unwrap();
        let block_info = SequenceInfo { color: Some(7) };

        let path = directory.join("input.tar");
        let members = [
            ("a.fa", ">a\nACGTACGTAC\n>b\nGGGGCCCCAA\n"),
            ("notes.txt", "not sequences\n"),
            ("dir/b.fq", "@c\nTTTTAAAACC\n+\nIIIIIIIIII\n"),
        ];
        std::fs::write(&path, archive(&members)).unwrap();
        let (files, buffers) = read(&(path.clone(), Some(7)));
        // The members that are not sequences are not named.
        let name = |member| format!("{}:{member}", path.display());
        assert_eq!(
            files,
            [(name("a.fa"), block_info), (name("dir/b.fq"), block_info)]
        );
        assert_eq!(buffers, [(2, 10), (1, 20)]);

        let path = directory.join("input.fa");
        std::fs::write(&path, ">a\nACGTACGTAC\n").unwrap();
        let (files, buffers) = read(&(path.clone(), Some(7)));
        assert_eq!(files, [(path.display().to_string(), block_info)]);
        assert_eq!(buffers, [(1, 10)]);
        std::fs::remove_dir_all(directory).unwrap();
    }
}
