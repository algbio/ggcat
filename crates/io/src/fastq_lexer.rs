//! FASTQ line lexer.
//!
//! FASTQ records are found by counting lines, exactly as the record reader
//! does, because a quality line may start with any character. Only the
//! sequence line is handed on; the quality line is discarded.

use memchr::memchr;

const IDENT: u8 = 0;
const SEQUENCE: u8 = 1;
const PLUS: u8 = 2;
const QUALITY: u8 = 3;

pub struct FastqLexer {
    copy_ident: bool,
    state: u8,
    ident: Vec<u8>,
    sequence: Vec<u8>,
}

impl FastqLexer {
    pub fn new(copy_ident: bool) -> Self {
        Self {
            copy_ident,
            state: IDENT,
            ident: Vec::new(),
            sequence: Vec::new(),
        }
    }

    pub fn begin_stream(&mut self) {
        self.state = IDENT;
        self.ident.clear();
        self.sequence.clear();
    }

    pub fn push(&mut self, mut bytes: &[u8], mut on_record: impl FnMut(&[u8], &[u8])) {
        while !bytes.is_empty() {
            match memchr(b'\n', bytes) {
                Some(position) => {
                    self.feed_line(
                        strip_carriage_return(&bytes[..position]),
                        false,
                        &mut on_record,
                    );
                    bytes = &bytes[position + 1..];
                }
                None => {
                    self.feed_line(strip_carriage_return(bytes), true, &mut on_record);
                    break;
                }
            }
        }
    }

    /// Flushes a record whose quality line the file does not have.
    pub fn end_stream(&mut self, mut on_record: impl FnMut(&[u8], &[u8])) {
        if self.state == SEQUENCE && !self.sequence.is_empty() {
            on_record(&self.ident, &self.sequence);
            self.ident.clear();
            self.sequence.clear();
        }
        self.state = IDENT;
    }

    fn feed_line(&mut self, line: &[u8], partial: bool, on_record: &mut impl FnMut(&[u8], &[u8])) {
        match self.state {
            IDENT if self.copy_ident => self.ident.extend_from_slice(line),
            SEQUENCE => self.sequence.extend_from_slice(line),
            _ => {}
        }
        if partial {
            return;
        }
        if self.state == SEQUENCE {
            // The quality line carries nothing this pipeline uses, so the
            // record is complete as soon as its bases are.
            on_record(&self.ident, &self.sequence);
            self.ident.clear();
            self.sequence.clear();
        }
        self.state = match self.state {
            IDENT => SEQUENCE,
            SEQUENCE => PLUS,
            PLUS => QUALITY,
            _ => IDENT,
        };
    }
}

#[inline(always)]
fn strip_carriage_return(line: &[u8]) -> &[u8] {
    match line.last() {
        Some(&b'\r') => &line[..line.len() - 1],
        _ => line,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn collect(input: &[u8], chunk: usize, copy_ident: bool) -> Vec<(String, String)> {
        let mut lexer = FastqLexer::new(copy_ident);
        lexer.begin_stream();
        let mut records = Vec::new();
        let mut push = |ident: &[u8], sequence: &[u8]| {
            records.push((
                String::from_utf8(ident.to_vec()).unwrap(),
                String::from_utf8(sequence.to_vec()).unwrap(),
            ))
        };
        for piece in input.chunks(chunk) {
            lexer.push(piece, &mut push);
        }
        lexer.end_stream(&mut push);
        records
    }

    #[test]
    fn reads_records_independently_of_the_chunking() {
        let input = b"@a\r\nACGT\r\n+\r\n!!!!\r\n@b desc\nTTGG\n+b\n####\n";
        for chunk in [1usize, 3, 7, 64, 4096] {
            assert_eq!(
                collect(input, chunk, true),
                vec![
                    ("@a".to_string(), "ACGT".to_string()),
                    ("@b desc".to_string(), "TTGG".to_string()),
                ],
                "chunk {chunk}"
            );
            assert_eq!(
                collect(input, chunk, false)
                    .into_iter()
                    .map(|(ident, _)| ident)
                    .collect::<Vec<_>>(),
                vec![String::new(), String::new()]
            );
        }
    }

    #[test]
    fn keeps_a_record_whose_quality_line_is_missing() {
        for chunk in [1usize, 5, 4096] {
            assert_eq!(
                collect(b"@a\nACGT\n+\n!!!!\n@b\nTT", chunk, true),
                vec![
                    ("@a".to_string(), "ACGT".to_string()),
                    ("@b".to_string(), "TT".to_string()),
                ]
            );
        }
    }

    #[test]
    fn keeps_empty_sequences() {
        assert_eq!(
            collect(b"@a\n\n+\n\n@b\nAC\n+\n!!\n", 4096, true),
            vec![
                ("@a".to_string(), String::new()),
                ("@b".to_string(), "AC".to_string()),
            ]
        );
    }
}
