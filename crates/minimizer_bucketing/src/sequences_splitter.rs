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
            // Skip all the unknown characters, skipping both 'N' and 'n'
            while start < fasta_seq.seq.len() && ((fasta_seq.seq[start] ^ b'N') & 0x7 == 0) {
                start += 1;
            }
            end = start;
            // Find the last valid character in this sequence, stopping when a 'N' or 'n' is found
            while end < fasta_seq.seq.len() && ((fasta_seq.seq[end] ^ b'N') & 0x7 != 0) {
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
