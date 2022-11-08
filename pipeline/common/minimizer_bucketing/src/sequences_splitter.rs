use io::sequences_reader::DnaSequence;
use std::ops::Range;

pub struct SequencesSplitter {
    k: usize,
    pub valid_bases: u64,
}

impl SequencesSplitter {
    pub fn new(k: usize) -> Self {
        Self { k, valid_bases: 0 }
    }

    #[inline]
    pub fn process_sequences(
        &mut self,
        fasta_seq: &DnaSequence,
        mut process_fn: impl FnMut(&[u8], Range<usize>),
    ) {
        let mut start;
        let mut end = 0;

        while end < fasta_seq.seq.len() {
            start = end;
            // Skip all not recognized characters
            while start < fasta_seq.seq.len() && fasta_seq.seq[start] == b'N' {
                start += 1;
            }
            end = start;
            // Find the last valid character in this sequence
            while end < fasta_seq.seq.len() && fasta_seq.seq[end] != b'N' {
                end += 1;
            }
            // If the length of the read is long enough, return it
            if end - start >= self.k {
                self.valid_bases += (end - start) as u64;
                process_fn(&fasta_seq.seq[start..end], start..end);
            }
        }
    }
}
