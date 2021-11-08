use crate::libdeflate::decompress_file;
use bio::io::fastq;
use bio::io::fastq::Record;
use bstr::ByteSlice;
use nix::sys::signal::{self, Signal};
use nix::unistd::Pid;
use rand::Rng;
use serde::Serialize;
use std::fs::File;
use std::hint::unreachable_unchecked;
use std::intrinsics::unlikely;
use std::io::{BufRead, BufReader, Read};
use std::num::Wrapping;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::process::Stdio;
use std::ptr::null_mut;
use std::slice::from_raw_parts;

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

    #[inline]
    fn process_line<'a, 'b>(buffer: &'b mut &'a [u8]) -> (bool, &'a [u8]) {
        match buffer.find(&[b'\n']) {
            None => {
                // No newline

                let buflen = if buffer.len() > 0 && buffer[buffer.len() - 1] == b'\r' {
                    buffer.len() - 1
                } else {
                    buffer.len()
                };

                let obuffer = &buffer[..buflen];

                *buffer = &[];
                (false, obuffer)
            }
            Some(pos) => {
                let mut bpos = pos;
                if bpos != 0 && buffer[bpos - 1] == b'\r' {
                    bpos -= 1;
                }
                let obuffer = &buffer[..bpos];

                *buffer = &buffer[pos + 1..];
                (true, obuffer)
            }
        }
    }

    pub fn process_file_extended<F: FnMut(FastaSequence)>(
        source: impl AsRef<Path>,
        mut func: F,
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

    pub fn read_binary_file(file: impl AsRef<Path>, mut callback: impl FnMut(&[u8]), remove: bool) {
        if file.as_ref().extension().filter(|x| *x == "gz").is_some() {
            decompress_file(
                &file,
                |data| {
                    callback(data);
                },
                1024 * 512,
            );
        } else if file.as_ref().extension().filter(|x| *x == "lz4").is_some() {
            let mut file = lz4::Decoder::new(
                File::open(&file).expect(&format!("Cannot open file {}", file.as_ref().display())),
            )
            .unwrap();
            let mut buffer = [0; 1024 * 512];
            while let Ok(count) = file.read(&mut buffer) {
                if count == 0 {
                    break;
                }
                callback(&buffer[0..count]);
            }
        } else {
            let mut file =
                File::open(&file).expect(&format!("Cannot open file {}", file.as_ref().display()));
            let mut buffer = [0; 1024 * 512];
            while let Ok(count) = file.read(&mut buffer) {
                if count == 0 {
                    break;
                }
                callback(&buffer[0..count]);
            }
        }

        if remove {
            std::fs::remove_file(file);
        }
    }

    fn process_fasta(
        source: impl AsRef<Path>,
        mut func: impl FnMut(FastaSequence),
        remove_file: bool,
    ) {
        // Ident, seq
        let mut intermediate = [Vec::new(), Vec::new()];
        let mut state = IDENT_STATE;

        let mut process_function = |mut buffer: &[u8]| {
            let mut sequence_info = [&[][..], &[][..]];

            if state != IDENT_STATE {
                let (complete, line) = Self::process_line(&mut buffer);

                intermediate[state].extend_from_slice(line);
                if !complete {
                    return;
                }
                if state == SEQ_STATE {
                    Self::normalize_sequence(&mut intermediate[SEQ_STATE][..]);
                    func(FastaSequence {
                        ident: &intermediate[IDENT_STATE][..],
                        seq: &intermediate[SEQ_STATE][..],
                        qual: None,
                    });
                    intermediate.iter_mut().for_each(|vec| vec.clear());
                    state = IDENT_STATE;
                } else {
                    state = (state + 1) % 2;
                }
            }

            while buffer.len() > 0 {
                let (complete, line) = Self::process_line(&mut buffer);

                sequence_info[state] = line;
                if !complete {
                    break;
                }

                if state == SEQ_STATE {
                    let [int_ident, int_seq] = &mut intermediate;

                    func(FastaSequence {
                        ident: if int_ident.len() > 0 {
                            int_ident.extend_from_slice(&sequence_info[IDENT_STATE]);
                            &int_ident[..]
                        } else {
                            &sequence_info[IDENT_STATE]
                        },
                        seq: {
                            int_seq.extend_from_slice(&sequence_info[SEQ_STATE]);
                            Self::normalize_sequence(&mut int_seq[..]);
                            &int_seq[..]
                        },
                        qual: None,
                    });
                    intermediate.iter_mut().for_each(|vec| vec.clear());
                    sequence_info.iter_mut().for_each(|slice| *slice = &[]);
                }
                state = (state + 1) % 2;
            }
            sequence_info
                .iter()
                .zip(intermediate.iter_mut())
                .for_each(|(slice, vec)| vec.extend_from_slice(slice));
        };

        Self::read_binary_file(&source, process_function, remove_file);
    }

    fn process_fastq(
        source: impl AsRef<Path>,
        mut func: impl FnMut(FastaSequence),
        remove_file: bool,
    ) {
        // Ident, seq, qual
        let mut intermediate = [Vec::new(), Vec::new(), Vec::new()];
        let mut state = IDENT_STATE;
        let mut skipped_plus = false;

        let mut process_function = |mut buffer: &[u8]| {
            let mut sequence_info = [&[][..], &[][..], &[][..]];

            if state != IDENT_STATE {
                let (complete, line) = Self::process_line(&mut buffer);
                intermediate[state].extend_from_slice(line);
                if !complete {
                    return;
                }
                if state == QUAL_STATE {
                    if !skipped_plus {
                        intermediate[QUAL_STATE].clear();
                        skipped_plus = true;
                    } else {
                        Self::normalize_sequence(&mut intermediate[SEQ_STATE][..]);
                        func(FastaSequence {
                            ident: &intermediate[IDENT_STATE][..],
                            seq: &intermediate[SEQ_STATE][..],
                            qual: Some(&intermediate[QUAL_STATE][..]),
                        });
                        intermediate.iter_mut().for_each(|vec| vec.clear());
                        state = IDENT_STATE;
                        skipped_plus = false;
                    }
                } else {
                    state = (state + 1) % 3;
                }
            }

            while buffer.len() > 0 {
                let (complete, line) = Self::process_line(&mut buffer);

                sequence_info[state] = line;
                if !complete {
                    break;
                }

                if state == QUAL_STATE {
                    if !skipped_plus {
                        intermediate[QUAL_STATE].clear();
                        sequence_info[QUAL_STATE] = &[];
                        skipped_plus = true;
                        continue;
                    }

                    let [int_ident, int_seq, int_qual] = &mut intermediate;

                    func(FastaSequence {
                        ident: if int_ident.len() > 0 {
                            int_ident.extend_from_slice(&sequence_info[IDENT_STATE]);
                            &int_ident[..]
                        } else {
                            &sequence_info[IDENT_STATE]
                        },
                        seq: {
                            int_seq.extend_from_slice(&sequence_info[SEQ_STATE]);
                            Self::normalize_sequence(&mut int_seq[..]);
                            &int_seq[..]
                        },
                        qual: Some(if int_qual.len() > 0 {
                            int_qual.extend_from_slice(&sequence_info[QUAL_STATE]);
                            &int_qual[..]
                        } else {
                            &sequence_info[QUAL_STATE]
                        }),
                    });
                    intermediate.iter_mut().for_each(|vec| vec.clear());
                    sequence_info.iter_mut().for_each(|slice| *slice = &[]);
                    skipped_plus = false;
                }
                state = (state + 1) % 3;
            }
            sequence_info
                .iter()
                .zip(intermediate.iter_mut())
                .for_each(|(slice, vec)| vec.extend_from_slice(slice));
        };

        Self::read_binary_file(&source, process_function, remove_file);
    }
}
