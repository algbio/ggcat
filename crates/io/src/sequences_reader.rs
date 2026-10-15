use crate::binary_reader::is_archive;
use crate::lines_reader::LinesReader;
use config::DEFAULT_OUTPUT_BUFFER_SIZE;
use nightly_quirks::branch_pred::unlikely;
use std::cmp::max;
use std::path::Path;
use utils::dna_sequence::{DnaSequence, DnaSequencesFileType};

const IDENT_STATE: usize = 0;
const SEQ_STATE: usize = 1;
const QUAL_STATE: usize = 2;

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

fn normalize_sequence(seq: &mut [u8]) {
    for el in seq.iter_mut() {
        *el = SEQ_LETTERS_MAPPING[*el as usize];
    }
}

/// Parses a FASTA file, one line at a time.
struct FastaParser {
    intermediate: [Vec<u8>; 2],
    on_comment: bool,
    state: usize,
    new_line: bool,
    line_split_copyback: Option<usize>,
    flush_size: usize,
    copy_ident: bool,
}

impl FastaParser {
    fn new(line_split_copyback: Option<usize>, copy_ident: bool) -> Self {
        Self {
            intermediate: [Vec::new(), Vec::new()],
            on_comment: false,
            state: SEQ_STATE,
            new_line: true,
            line_split_copyback,
            flush_size: max(
                DEFAULT_OUTPUT_BUFFER_SIZE,
                line_split_copyback.unwrap_or(0) * 2,
            ),
            copy_ident,
        }
    }

    #[inline(always)]
    fn process_line(
        &mut self,
        line: &[u8],
        partial: bool,
        finished: bool,
        func: &mut impl for<'a> FnMut(DnaSequence<'a, &'a [u8]>),
    ) {
        let intermediate = &mut self.intermediate;

        // A comment line goes on for as long as its parts are partial; the end of the input ends
        // it too.
        if self.on_comment && !finished {
            self.on_comment = partial;
        }
        // If a new ident line is found (or it's the last line)
        else if finished || (self.new_line && line.len() > 0 && line[0] == b'>') {
            if intermediate[SEQ_STATE].len() > 0 {
                normalize_sequence(&mut intermediate[SEQ_STATE]);
                func(DnaSequence {
                    ident_data: &intermediate[IDENT_STATE],
                    seq: &intermediate[SEQ_STATE],
                    format: DnaSequencesFileType::FASTA,
                });
            }

            if self.copy_ident {
                intermediate[IDENT_STATE].clear();
                intermediate[IDENT_STATE].extend_from_slice(line);
            }
            intermediate[SEQ_STATE].clear();

            self.state = if partial { IDENT_STATE } else { SEQ_STATE };
        } else if self.new_line && line.len() > 0 && line[0] == b';' {
            // A comment line, skipped, also in the parts that follow if it does not end here.
            self.on_comment = partial;
        } else if self.state == IDENT_STATE {
            if self.copy_ident {
                intermediate[IDENT_STATE].extend_from_slice(line);
            }

            if !partial {
                self.state = SEQ_STATE;
            }
        } else {
            intermediate[SEQ_STATE].extend_from_slice(line);
        }

        if let Some(copyback) = self.line_split_copyback {
            if intermediate[SEQ_STATE].len() >= self.flush_size {
                normalize_sequence(&mut intermediate[SEQ_STATE]);
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
        self.new_line = !partial;
    }
}

/// Parses a FASTQ file, one line at a time.
struct FastqParser {
    intermediate: [Vec<u8>; 3],
    state: usize,
    skipped_plus: bool,
}

impl FastqParser {
    fn new() -> Self {
        Self {
            intermediate: [Vec::new(), Vec::new(), Vec::new()],
            state: IDENT_STATE,
            skipped_plus: false,
        }
    }

    #[inline(always)]
    fn process_line(
        &mut self,
        line: &[u8],
        partial: bool,
        finished: bool,
        func: &mut impl for<'a> FnMut(DnaSequence<'a, &'a [u8]>),
    ) {
        let intermediate = &mut self.intermediate;

        if unlikely(finished) {
            if intermediate[SEQ_STATE].len() > 0 {
                normalize_sequence(&mut intermediate[SEQ_STATE]);
                func(DnaSequence {
                    ident_data: &intermediate[IDENT_STATE],
                    seq: &intermediate[SEQ_STATE],
                    format: DnaSequencesFileType::FASTQ,
                });
            }

            return;
        }

        if self.state == QUAL_STATE {
            if !self.skipped_plus {
                if !partial {
                    self.skipped_plus = true;
                }
                return;
            }

            if !partial {
                normalize_sequence(&mut intermediate[SEQ_STATE]);
                func(DnaSequence {
                    ident_data: &intermediate[IDENT_STATE],
                    seq: &intermediate[SEQ_STATE],
                    format: DnaSequencesFileType::FASTQ,
                });

                intermediate[IDENT_STATE].clear();
                intermediate[SEQ_STATE].clear();
                intermediate[QUAL_STATE].clear();

                self.skipped_plus = false;
            }
        } else {
            intermediate[self.state].extend_from_slice(line);
        }

        if !partial {
            self.state = (self.state + 1) % 3;
        }
    }
}

/// The parser of the file being read, picked by its name.
enum SequencesParser {
    Fasta(FastaParser),
    Fastq(FastqParser),
    /// A file inside an archive that holds no sequences.
    Skipped,
}

pub struct SequencesReader {
    lines_reader: LinesReader,
}

impl SequencesReader {
    pub fn new() -> Self {
        Self {
            lines_reader: LinesReader::new(),
        }
    }

    pub fn process_file_extended<F: for<'a> FnMut(DnaSequence<'a, &'a [u8]>)>(
        &mut self,
        source: impl AsRef<Path>,
        mut func: F,
        line_split_copyback: Option<usize>,
        copy_ident: bool,
        remove_file: bool,
    ) {
        let source = source.as_ref();
        self.lines_reader.process_lines(
            source,
            |name| {
                let name = name.unwrap_or_default();
                match DnaSequencesFileType::from_name(name) {
                    Some(DnaSequencesFileType::FASTA) => {
                        SequencesParser::Fasta(FastaParser::new(line_split_copyback, copy_ident))
                    }
                    Some(DnaSequencesFileType::FASTQ) => SequencesParser::Fastq(FastqParser::new()),
                    Some(DnaSequencesFileType::GFA) => todo!(),
                    // Archives can hold other files too, such as their documentation.
                    None if is_archive(source) => {
                        ggcat_logging::info!("Skipping {name}: unsupported sequence format");
                        SequencesParser::Skipped
                    }
                    None => panic!("Cannot recognize file type of '{}'", source.display()),
                }
            },
            // The parser is picked once per chunk, each running its own loop over the lines.
            |parser, lines| match parser {
                SequencesParser::Fasta(parser) => lines.for_each(|line, partial, finished| {
                    parser.process_line(line, partial, finished, &mut func)
                }),
                SequencesParser::Fastq(parser) => lines.for_each(|line, partial, finished| {
                    parser.process_line(line, partial, finished, &mut func)
                }),
                SequencesParser::Skipped => {}
            },
            remove_file,
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    const FASTA: &[u8] = b">r1 desc\r\nACGTacgt\r\nNNXY\r\n>r2\nGGGG\nCC";
    const FASTQ: &[u8] = b"@q1\nACGTN\n+\nIIIII\n@q2 x\nacgt\n+q2 x\nIIII\n";

    fn records(pairs: &[(&str, &str)]) -> Vec<(String, String)> {
        pairs
            .iter()
            .map(|(ident, seq)| (ident.to_string(), seq.to_string()))
            .collect()
    }

    /// The records parsed from `path`, as (ident, sequence).
    fn parse(path: &Path) -> Vec<(String, String)> {
        let mut parsed = Vec::new();
        SequencesReader::new().process_file_extended(
            path,
            |sequence| {
                parsed.push((
                    String::from_utf8_lossy(sequence.ident_data).into_owned(),
                    String::from_utf8_lossy(sequence.seq).into_owned(),
                ))
            },
            None,
            true,
            false,
        );
        parsed
    }

    fn fasta_records() -> Vec<(String, String)> {
        records(&[(">r1 desc", "ACGTACGTNNNN"), (">r2", "GGGGCC")])
    }

    fn fastq_records() -> Vec<(String, String)> {
        records(&[("@q1", "ACGTN"), ("@q2 x", "ACGT")])
    }

    #[test]
    fn files_and_archive_members_get_their_own_parser() {
        let directory =
            std::env::temp_dir().join(format!("ggcat-sequences-reader-{}", std::process::id()));
        std::fs::create_dir_all(&directory).unwrap();

        let fasta = directory.join("a.fa");
        std::fs::write(&fasta, FASTA).unwrap();
        assert_eq!(parse(&fasta), fasta_records());
        let fastq = directory.join("b.fq");
        std::fs::write(&fastq, FASTQ).unwrap();
        assert_eq!(parse(&fastq), fastq_records());

        // A line longer than a chunk, so it reaches the parser in parts.
        let long = vec![b'A'; 5 << 20];
        let long_fasta = [&b">long\n"[..], &long, b"\n"].concat();
        let mut gzip = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::fast());
        gzip.write_all(FASTQ).unwrap();
        let mut builder = tar::Builder::new(Vec::new());
        for (name, data) in [
            ("a.fa", FASTA.to_vec()),
            ("dir/b.fq.gz", gzip.finish().unwrap()),
            ("readme.txt", b"not sequences\n".to_vec()),
            ("long.fa", long_fasta),
            ("c.fq", FASTQ.to_vec()),
        ] {
            let mut header = tar::Header::new_gnu();
            header.set_size(data.len() as u64);
            header.set_mode(0o644);
            builder.append_data(&mut header, name, &data[..]).unwrap();
        }
        let archive = directory.join("input.tar");
        std::fs::write(&archive, builder.into_inner().unwrap()).unwrap();

        let mut expected = fasta_records();
        expected.extend(fastq_records());
        expected.push((">long".into(), String::from_utf8(long).unwrap()));
        expected.extend(fastq_records());
        assert!(parse(&archive) == expected);

        std::fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn fasta_comment_lines_are_skipped_wherever_a_line_starts() {
        let directory = std::env::temp_dir().join(format!(
            "ggcat-sequences-reader-comments-{}",
            std::process::id()
        ));
        std::fs::create_dir_all(&directory).unwrap();

        // Before the first header, after a header, between sequence lines, between records, and
        // at the end without a line feed.
        let fasta = directory.join("comments.fa");
        std::fs::write(
            &fasta,
            b";first\n>r1 a\n;after the header\nACGT\n;inside\nGGCC\n>r2\nAAAA\n;between\n>r3\nCC\n;last",
        )
        .unwrap();
        let expected = records(&[(">r1 a", "ACGTGGCC"), (">r2", "AAAA"), (">r3", "CC")]);
        assert_eq!(parse(&fasta), expected);

        // A comment longer than a chunk, so it reaches the parser in parts.
        let comment = [&b";"[..], &vec![b'x'; 5 << 20]].concat();
        let long = directory.join("long.fa");
        std::fs::write(
            &long,
            [&b">r1\nACGT\n"[..], &comment, b"\nGG\n>r2\nTT\n"].concat(),
        )
        .unwrap();
        assert_eq!(parse(&long), records(&[(">r1", "ACGTGG"), (">r2", "TT")]));

        std::fs::remove_dir_all(directory).unwrap();
    }
}
