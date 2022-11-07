use crate::lines_reader::LinesReader;
use config::DEFAULT_OUTPUT_BUFFER_SIZE;
use nightly_quirks::branch_pred::unlikely;
use std::cmp::max;
use std::path::Path;

const IDENT_STATE: usize = 0;
const SEQ_STATE: usize = 1;
const QUAL_STATE: usize = 2;

enum FileType {
    Fasta,
    Fastq,
}

// TODO: Support both fasta and gfa sequences
#[derive(Copy, Clone)]
pub struct FastaSequence<'a> {
    pub ident: &'a [u8],
    pub seq: &'a [u8],
    pub qual: Option<&'a [u8]>,
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

    pub fn process_file_extended<F: FnMut(FastaSequence)>(
        &mut self,
        source: impl AsRef<Path>,
        func: F,
        line_split_copyback: Option<usize>,
        copy_ident: bool,
        remove_file: bool,
    ) {
        const FASTQ_EXTS: &[&str] = &["fq", "fastq"];
        const FASTA_EXTS: &[&str] = &["fa", "fasta", "fna", "ffn"];

        let mut file_type = None;
        let mut tmp = source.as_ref().file_name().unwrap().to_str().unwrap();
        let mut path: &Path = tmp.as_ref();

        while let Some(ext) = path.extension() {
            if FASTQ_EXTS.contains(&ext.to_str().unwrap()) {
                file_type = Some(FileType::Fastq);
                break;
            }
            if FASTA_EXTS.contains(&ext.to_str().unwrap()) {
                file_type = Some(FileType::Fasta);
                break;
            }
            tmp = &tmp[0..tmp.len() - ext.len() - 1];
            path = tmp.as_ref()
        }

        match file_type {
            None => panic!(
                "Cannot recognize file type of '{}'",
                source.as_ref().display()
            ),
            Some(ftype) => match ftype {
                FileType::Fasta => {
                    self.process_fasta(source, func, line_split_copyback, copy_ident, remove_file);
                }
                FileType::Fastq => {
                    self.process_fastq(source, func, false, remove_file);
                }
            },
        }
    }

    fn process_fasta(
        &mut self,
        source: impl AsRef<Path>,
        mut func: impl FnMut(FastaSequence),
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

        self.lines_reader.process_lines(
            source,
            |line: &[u8], partial, finished| {
                if on_comment {
                    on_comment = !partial;
                }
                // If a new ident line is found (or it's the last line)
                else if finished || (new_line && line.len() > 0 && line[0] == b'>') {
                    if intermediate[SEQ_STATE].len() > 0 {
                        Self::normalize_sequence(&mut intermediate[SEQ_STATE]);
                        func(FastaSequence {
                            ident: &intermediate[IDENT_STATE],
                            seq: &intermediate[SEQ_STATE],
                            qual: None,
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

                if let Some(copyback) = line_split_copyback &&
                    (intermediate[SEQ_STATE].len() >= flush_size) {
                    Self::normalize_sequence(&mut intermediate[SEQ_STATE]);
                    func(FastaSequence {
                        ident: &intermediate[IDENT_STATE],
                        seq: &intermediate[SEQ_STATE],
                        qual: None,
                    });
                    let copy_start = intermediate[SEQ_STATE].len() - copyback;
                    intermediate[SEQ_STATE].copy_within(copy_start.., 0);
                    intermediate[SEQ_STATE].truncate(copyback);
                }

                new_line = !partial;
            },
            remove_file,
        );
    }

    fn process_fastq(
        &mut self,
        source: impl AsRef<Path>,
        mut func: impl FnMut(FastaSequence),
        get_quality: bool,
        remove_file: bool,
    ) {
        let mut state = IDENT_STATE;
        let mut skipped_plus = false;

        let mut intermediate = [Vec::new(), Vec::new(), Vec::new()];

        self.lines_reader.process_lines(
            source,
            |line: &[u8], partial, finished| {
                if unlikely(finished) {
                    return;
                }

                if state == QUAL_STATE {
                    if !skipped_plus {
                        if !partial {
                            skipped_plus = true;
                        }
                        return;
                    }

                    if get_quality {
                        intermediate[state].extend_from_slice(line);
                    }

                    if !partial {
                        Self::normalize_sequence(&mut intermediate[SEQ_STATE]);
                        func(FastaSequence {
                            ident: &intermediate[IDENT_STATE],
                            seq: &intermediate[SEQ_STATE],
                            qual: if get_quality {
                                Some(&intermediate[QUAL_STATE])
                            } else {
                                None
                            },
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
