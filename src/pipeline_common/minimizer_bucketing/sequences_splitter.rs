use crate::io::sequences_reader::FastaSequence;
use crate::rolling::kseq_iterator::RollingKseqIterator;
use crate::rolling::quality_check::RollingQualityCheck;
use itertools::Itertools;
use std::ops::Range;

pub struct SequencesSplitter {
    quality_check: RollingQualityCheck,
    k: usize,
    pub valid_bases: u64,
}

impl SequencesSplitter {
    pub fn new(k: usize) -> Self {
        Self {
            quality_check: RollingQualityCheck::new(),
            k,
            valid_bases: 0,
        }
    }

    #[inline]
    pub fn process_sequences(
        &mut self,
        fasta_seq: &FastaSequence,
        quality_threshold: Option<f64>,
        mut process_fn: impl FnMut(&[u8], Range<usize>),
    ) {
        let mut start = 0;
        let mut end = 0;

        let quality_log_threshold: u64 =
            RollingQualityCheck::get_log_for_correct_probability(quality_threshold.unwrap_or(0.0));

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
                if quality_threshold.is_some() && fasta_seq.qual.is_some() {
                    let q = fasta_seq.qual.unwrap();

                    for (valid, mut el) in &RollingKseqIterator::iter_seq(
                        &q[start..end],
                        self.k,
                        &mut self.quality_check,
                    )
                    .enumerate()
                    .group_by(|(_, x)| *x < quality_log_threshold)
                    {
                        if !valid {
                            continue;
                        }
                        let first_el = el.next().unwrap();
                        let last_el = el.last().unwrap_or(first_el);

                        let start_index = start + first_el.0;
                        let end_index = start + last_el.0 + self.k;

                        self.valid_bases += (end_index - start_index) as u64;
                        process_fn(
                            &fasta_seq.seq[start_index..end_index],
                            start_index..end_index,
                        );
                    }
                } else {
                    self.valid_bases += (end - start) as u64;
                    process_fn(&fasta_seq.seq[start..end], start..end);
                }
            }
        }
    }
}
