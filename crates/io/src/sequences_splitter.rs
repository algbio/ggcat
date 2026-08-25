use std::range::Range;

use zstd::zstd_safe::WriteBuf;

use crate::{compressed_read::CompressedRead, sequences_reader::DnaSequence};

pub fn split_and_compress_sequences(
    compress_temp_buffer: &mut Vec<u8>,
    min_length: usize,
    fasta_seq: &DnaSequence<'_, &[u8]>,
    mut process_fn: impl FnMut(CompressedRead, Range<usize>),
) -> u64 {
    let mut valid_bases = 0;
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

        let seq_len = end - start;

        // If the length of the read is long enough, return it
        if seq_len >= min_length {
            valid_bases += seq_len as u64;
            compress_temp_buffer.clear();
            CompressedRead::compress_from_plain(&fasta_seq.seq[start..end], |part| {
                compress_temp_buffer.extend_from_slice(part)
            });
            process_fn(
                CompressedRead::new_from_compressed(compress_temp_buffer.as_slice(), seq_len),
                (start..end).into(),
            );
        }
    }
    valid_bases
}
