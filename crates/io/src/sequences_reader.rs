use crate::lines_reader::{LinesReader, LinesSource};
use config::DEFAULT_OUTPUT_BUFFER_SIZE;
use hashes::HashableSequence;
use nightly_quirks::branch_pred::unlikely;
use std::cmp::max;
use std::path::Path;

const IDENT_STATE: usize = 0;
const SEQ_STATE: usize = 1;
const QUAL_STATE: usize = 2;

#[derive(Copy, Clone)]
pub enum DnaSequencesFileType {
    FASTA,
    FASTQ,
    GFA,
    BINARY,
}

#[derive(Copy, Clone)]
pub struct DnaSequence<'a, S: HashableSequence + 'a> {
    pub ident_data: &'a [u8],
    pub seq: S,
    pub format: DnaSequencesFileType,
}

const SEQ_LETTERS_MAPPING: [u8; 256] = {
    let mut lookup = [b'N'; 256];
    lookup[b'A' as usize] = b'A';
    lookup[b'C' as usize] = b'C';
    lookup[b'G' as usize] = b'G';
    lookup[b'T' as usize] = b'T';
    lookup[b'a' as usize] = b'A';
    lookup[b'c' as usize] = b'C';
    lookup[b'g' as usize] = b'G';
    lookup[b't' as usize] = b'T';
    lookup
};

pub struct SequencesReader {
    lines_reader: LinesReader,
}

impl SequencesReader {
    pub fn new() -> Self {
        Self {
            lines_reader: LinesReader::new(),
        }
    }

    fn normalize_sequence(seq: &mut [u8]) {
        for el in seq.iter_mut() {
            *el = SEQ_LETTERS_MAPPING[*el as usize];
        }
    }

    pub fn process_file_extended<F: for<'a> FnMut(DnaSequence<'a, &'a [u8]>)>(
        &mut self,
        source: impl AsRef<Path>,
        func: F,
        line_split_copyback: Option<usize>,
        copy_ident: bool,
        remove_file: bool,
    ) {
        self.process_source_extended(
            LinesSource::File(source.as_ref()),
            func,
            line_split_copyback,
            copy_ident,
            remove_file,
        );
    }

    /// Parse a borrowed, already decompressed stream without extracting it to disk.
    pub fn process_reader_extended(
        &mut self,
        reader: &mut dyn std::io::Read,
        name: &Path,
        func: impl for<'a> FnMut(DnaSequence<'a, &'a [u8]>),
        line_split_copyback: Option<usize>,
        copy_ident: bool,
    ) {
        self.process_source_extended(
            LinesSource::Stream(reader, name),
            func,
            line_split_copyback,
            copy_ident,
            false,
        );
    }

    pub fn archive_file_type(path: &Path) -> Option<DnaSequencesFileType> {
        let name = path.file_name()?.to_str()?;
        let name = crate::sequences_stream::tar::strip_compression_suffix(name);
        match Path::new(name).extension()?.to_str()? {
            "fq" | "fastq" => Some(DnaSequencesFileType::FASTQ),
            "fa" | "fasta" | "fna" | "ffn" => Some(DnaSequencesFileType::FASTA),
            _ => None,
        }
    }

    pub fn file_type(path: &Path) -> Option<DnaSequencesFileType> {
        let mut name = path.file_name()?.to_str()?;
        while let Some((stem, extension)) = name.rsplit_once('.') {
            match extension {
                "fq" | "fastq" => return Some(DnaSequencesFileType::FASTQ),
                "fa" | "fasta" | "fna" | "ffn" => return Some(DnaSequencesFileType::FASTA),
                _ => name = stem,
            }
        }
        None
    }

    fn process_source_extended(
        &mut self,
        source: LinesSource<'_>,
        func: impl for<'a> FnMut(DnaSequence<'a, &'a [u8]>),
        line_split_copyback: Option<usize>,
        copy_ident: bool,
        remove_file: bool,
    ) {
        let path = match &source {
            LinesSource::File(path) | LinesSource::Stream(_, path) => *path,
        };
        let file_type = Self::file_type(path);
        match file_type {
            None => panic!("Cannot recognize file type of '{}'", path.display()),
            Some(ftype) => match ftype {
                DnaSequencesFileType::FASTA => {
                    self.process_fasta(source, func, line_split_copyback, copy_ident, remove_file);
                }
                DnaSequencesFileType::FASTQ => {
                    self.process_fastq(source, func, remove_file);
                }
                DnaSequencesFileType::GFA => {
                    todo!()
                }
                DnaSequencesFileType::BINARY => {
                    todo!()
                }
            },
        }
    }

    fn process_fasta(
        &mut self,
        source: LinesSource<'_>,
        mut func: impl for<'a> FnMut(DnaSequence<'a, &'a [u8]>),
        line_split_copyback: Option<usize>,
        copy_ident: bool,
        remove_file: bool,
    ) {
        let mut intermediate = [Vec::new(), Vec::new()];
        let mut on_comment = false;
        let mut state = SEQ_STATE;
        let mut new_line = true;

        let flush_size = max(
            DEFAULT_OUTPUT_BUFFER_SIZE,
            line_split_copyback.unwrap_or(0) * 2,
        );

        self.lines_reader.process_source(
            source,
            |line: &[u8], partial, finished| {
                if on_comment {
                    on_comment = !partial;
                }
                // If a new ident line is found (or it's the last line)
                else if finished || (new_line && line.len() > 0 && line[0] == b'>') {
                    if intermediate[SEQ_STATE].len() > 0 {
                        Self::normalize_sequence(&mut intermediate[SEQ_STATE]);
                        func(DnaSequence {
                            ident_data: &intermediate[IDENT_STATE],
                            seq: &intermediate[SEQ_STATE],
                            format: DnaSequencesFileType::FASTA,
                        });
                    }

                    if copy_ident {
                        intermediate[IDENT_STATE].clear();
                        intermediate[IDENT_STATE].extend_from_slice(line);
                    }
                    intermediate[SEQ_STATE].clear();

                    state = if partial { IDENT_STATE } else { SEQ_STATE };
                } else if new_line && line.len() > 0 && line[0] == b';' {
                    on_comment = true;
                } else if state == IDENT_STATE {
                    if copy_ident {
                        intermediate[IDENT_STATE].extend_from_slice(line);
                    }

                    if !partial {
                        state = SEQ_STATE;
                    }
                } else {
                    intermediate[SEQ_STATE].extend_from_slice(line);
                }

                if let Some(copyback) = line_split_copyback {
                    if intermediate[SEQ_STATE].len() >= flush_size {
                        Self::normalize_sequence(&mut intermediate[SEQ_STATE]);
                        func(DnaSequence {
                            ident_data: &intermediate[IDENT_STATE],
                            seq: &intermediate[SEQ_STATE],
                            format: DnaSequencesFileType::FASTQ,
                        });
                        let copy_start = intermediate[SEQ_STATE].len() - copyback;
                        intermediate[SEQ_STATE].copy_within(copy_start.., 0);
                        intermediate[SEQ_STATE].truncate(copyback);
                    }
                }
                new_line = !partial;
            },
            remove_file,
        );
    }

    fn process_fastq(
        &mut self,
        source: LinesSource<'_>,
        mut func: impl for<'a> FnMut(DnaSequence<'a, &'a [u8]>),
        // get_quality: bool,
        remove_file: bool,
    ) {
        let mut state = IDENT_STATE;
        let mut skipped_plus = false;

        let mut intermediate = [Vec::new(), Vec::new(), Vec::new()];

        self.lines_reader.process_source(
            source,
            |line: &[u8], partial, finished| {
                if unlikely(finished) {
                    if intermediate[SEQ_STATE].len() > 0 {
                        Self::normalize_sequence(&mut intermediate[SEQ_STATE]);
                        func(DnaSequence {
                            ident_data: &intermediate[IDENT_STATE],
                            seq: &intermediate[SEQ_STATE],
                            format: DnaSequencesFileType::FASTQ,
                        });
                    }

                    return;
                }

                if state == QUAL_STATE {
                    if !skipped_plus {
                        if !partial {
                            skipped_plus = true;
                        }
                        return;
                    }

                    if !partial {
                        Self::normalize_sequence(&mut intermediate[SEQ_STATE]);
                        func(DnaSequence {
                            ident_data: &intermediate[IDENT_STATE],
                            seq: &intermediate[SEQ_STATE],
                            format: DnaSequencesFileType::FASTQ,
                        });

                        intermediate[IDENT_STATE].clear();
                        intermediate[SEQ_STATE].clear();
                        intermediate[QUAL_STATE].clear();

                        skipped_plus = false;
                    }
                } else {
                    intermediate[state].extend_from_slice(line);
                }

                if !partial {
                    state = (state + 1) % 3;
                }
            },
            remove_file,
        );
    }
}
