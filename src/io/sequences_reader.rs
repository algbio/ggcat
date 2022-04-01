use crate::io::lines_reader::LinesReader;
use nightly_quirks::branch_pred::unlikely;
use std::path::Path;

const IDENT_STATE: usize = 0;
const SEQ_STATE: usize = 1;
const QUAL_STATE: usize = 2;

enum FileType {
    Fasta,
    Fastq,
}

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

pub struct SequencesReader;
impl SequencesReader {
    fn normalize_sequence(seq: &mut [u8]) {
        for el in seq.iter_mut() {
            *el = SEQ_LETTERS_MAPPING[*el as usize];
        }
    }

    pub fn process_file_extended<F: FnMut(FastaSequence)>(
        source: impl AsRef<Path>,
        func: F,
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
                    Self::process_fasta(source, func, remove_file);
                }
                FileType::Fastq => {
                    Self::process_fastq(source, func, remove_file);
                }
            },
        }
    }

    fn process_fasta(
        source: impl AsRef<Path>,
        mut func: impl FnMut(FastaSequence),
        remove_file: bool,
    ) {
        // let mut intermediate = [Vec::new(), Vec::new()];

        LinesReader::process_lines(
            source,
            |line: &[u8], finished| {
                // if finished || (line.len() > 0 && line[0] == b'>') {
                //     if intermediate[SEQ_STATE].len() > 0 {
                //         Self::normalize_sequence(&mut intermediate[SEQ_STATE]);
                //         func(FastaSequence {
                //             ident: &intermediate[IDENT_STATE],
                //             seq: &intermediate[SEQ_STATE],
                //             qual: None,
                //         });
                //         intermediate[SEQ_STATE].clear();
                //     }
                //     intermediate[IDENT_STATE].clear();
                //     intermediate[IDENT_STATE].extend_from_slice(line);
                // }
                // // Comment line, ignore it
                // else if line.len() > 0 && line[0] == b';' {
                //     return;
                // } else {
                //     // Sequence line
                //     intermediate[SEQ_STATE].extend_from_slice(line);
                // }
            },
            remove_file,
        );
    }

    fn process_fastq(
        source: impl AsRef<Path>,
        mut func: impl FnMut(FastaSequence),
        remove_file: bool,
    ) {
        let mut state = IDENT_STATE;
        let mut skipped_plus = false;

        let mut intermediate = [Vec::new(), Vec::new()];

        LinesReader::process_lines(
            source,
            |line: &[u8], finished| {
                if unlikely(finished) {
                    return;
                }
                match state {
                    QUAL_STATE => {
                        if !skipped_plus {
                            skipped_plus = true;
                            return;
                        }

                        Self::normalize_sequence(&mut intermediate[SEQ_STATE]);
                        func(FastaSequence {
                            ident: &intermediate[IDENT_STATE],
                            seq: &intermediate[SEQ_STATE],
                            qual: Some(line),
                        });
                        skipped_plus = false;
                    }
                    state => {
                        intermediate[state].clear();
                        intermediate[state].extend_from_slice(line);
                    }
                }
                state = (state + 1) % 3;
            },
            remove_file,
        );
    }
}
