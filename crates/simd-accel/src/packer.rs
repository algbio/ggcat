//! Fills SIMD lanes with valid DNA, one record at a time.
//!
//! The packer receives 64-bit blocks of classified bytes (2-bit codes plus a
//! non-DNA mask) and lays the DNA out across eight equally long lanes. Two
//! rules make every lane fragment a self-contained sequence:
//!
//! * a non-DNA character ends the current fragment, and a fragment shorter
//!   than `k` is removed again, so no fragment can be too short to hold a k-mer;
//! * a record that does not fit in the rest of a lane continues in the next
//!   lane after repeating its last `k - 1` bases, so no k-mer is lost.

use crate::batch::{
    BatchSink, LaneFragment, NO_RECORD, RECORD_CONTINUED, RECORD_CONTINUES, RecordEntry,
};
use crate::hashing::SIMD_LANES;
use crate::masks::{extract_fasta_masks, low_mask_u32, low_mask_u64};

/// Why a packer could not be created.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PackerError {
    InvalidK,
    LaneCapacityTooSmall { k: usize, bases_per_lane: usize },
    IgnoredLengthTooLarge { k: usize, ignored_length: usize },
}

impl std::fmt::Display for PackerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidK => write!(f, "k must be greater than zero"),
            Self::LaneCapacityTooSmall { k, bases_per_lane } => write!(
                f,
                "bases_per_lane ({bases_per_lane}) must be at least k ({k})"
            ),
            Self::IgnoredLengthTooLarge { k, ignored_length } => write!(
                f,
                "the minimum record length ({ignored_length}) must not exceed k ({k})"
            ),
        }
    }
}

impl std::error::Error for PackerError {}

struct OpenRecord<X> {
    extra: X,
    read_index: u64,
    bases_total: u64,
    source_pos: u64,
    /// Index of this record inside the batch being filled, once it has one.
    entry: Option<u32>,
    /// The record already has an entry in an earlier batch.
    continued: bool,
}

pub struct LanePacker<X: Clone> {
    k: usize,
    bases_per_lane: usize,
    ignored_length: usize,
    max_header_bytes: usize,
    copy_ident: bool,

    lane: usize,
    pos: usize,
    batch_has_content: bool,

    next_read_index: u64,

    overlap_buf: Vec<u32>,
    header_buf: Vec<u8>,

    record: Option<OpenRecord<X>>,
    fragment: Option<(u32, u64)>,
}

impl<X: Clone> LanePacker<X> {
    pub fn new(
        k: usize,
        bases_per_lane: usize,
        ignored_length: usize,
        copy_ident: bool,
        max_header_bytes: usize,
    ) -> Result<Self, PackerError> {
        if k == 0 {
            return Err(PackerError::InvalidK);
        }
        if bases_per_lane < k {
            return Err(PackerError::LaneCapacityTooSmall { k, bases_per_lane });
        }
        // A record below the cutoff must never leave a committed fragment
        // behind, and a committed fragment always holds at least k bases.
        if ignored_length > k {
            return Err(PackerError::IgnoredLengthTooLarge { k, ignored_length });
        }
        Ok(Self {
            k,
            bases_per_lane,
            ignored_length,
            max_header_bytes,
            copy_ident,
            lane: 0,
            pos: 0,
            batch_has_content: false,
            next_read_index: 0,
            overlap_buf: Vec::with_capacity(k.div_ceil(16)),
            header_buf: Vec::new(),
            record: None,
            fragment: None,
        })
    }

    pub fn k(&self) -> usize {
        self.k
    }

    pub fn bases_per_lane(&self) -> usize {
        self.bases_per_lane
    }

    pub fn wants_ident(&self) -> bool {
        self.copy_ident
    }

    /// Restarts the per-record numbering, at the start of every input block.
    pub fn reset_read_index(&mut self) {
        self.next_read_index = 0;
    }

    pub fn has_open_record(&self) -> bool {
        self.record.is_some()
    }

    pub fn begin_record(&mut self, extra: X) {
        debug_assert!(self.record.is_none(), "a record is already open");
        self.header_buf.clear();
        self.record = Some(OpenRecord {
            extra,
            read_index: self.next_read_index,
            bases_total: 0,
            source_pos: 0,
            entry: None,
            continued: false,
        });
    }

    pub fn push_header_bytes(&mut self, bytes: &[u8]) {
        if self.copy_ident {
            self.header_buf.extend_from_slice(bytes);
        }
    }

    /// Appends bases `[first_bit, first_bit + len)` of one classified block.
    ///
    /// `two_bits` holds two bits per input byte and `non_acgt` one bit per
    /// input byte, both as produced by [`extract_fasta_masks`].
    pub fn push_bases(
        &mut self,
        sink: &mut impl BatchSink<X>,
        two_bits: u128,
        non_acgt: u64,
        first_bit: usize,
        len: usize,
    ) {
        debug_assert!(first_bit + len <= 64);
        let end = first_bit + len;
        let mut index = first_bit;
        while index < end {
            let remaining = end - index;
            let invalid = (non_acgt >> index) & low_mask_u64(remaining);
            let run = if invalid == 0 {
                remaining
            } else {
                invalid.trailing_zeros() as usize
            };
            if run != 0 {
                self.push_acgt_run(sink, two_bits, index, run);
                index += run;
            }
            if index < end {
                // A stretch of non-DNA characters: they end the fragment but
                // still count towards the record's logical length.
                let valid = !(non_acgt >> index) & low_mask_u64(end - index);
                let skipped = if valid == 0 {
                    end - index
                } else {
                    valid.trailing_zeros() as usize
                };
                self.close_fragment(sink);
                let record = self.record.as_mut().expect("bases outside of a record");
                record.bases_total += skipped as u64;
                record.source_pos += skipped as u64;
                index += skipped;
            }
        }
    }

    /// Ends the open record. `allow_empty` keeps zero-length records, matching
    /// what the legacy FASTQ reader does and what the FASTA reader does not.
    pub fn end_record(&mut self, sink: &mut impl BatchSink<X>, allow_empty: bool) {
        self.close_fragment(sink);
        let Some(record) = self.record.as_ref() else {
            return;
        };
        let bases_total = record.bases_total;
        if bases_total >= self.ignored_length as u64 && (allow_empty || bases_total > 0) {
            let index = self.materialize(sink);
            sink.current().records[index as usize].bases_total = bases_total;
            self.next_read_index += 1;
        } else {
            debug_assert!(self.record.as_ref().unwrap().entry.is_none());
        }
        self.record = None;
    }

    /// Packs a whole record whose sequence is already in memory.
    pub fn push_record(
        &mut self,
        sink: &mut impl BatchSink<X>,
        extra: X,
        ident: &[u8],
        sequence: &[u8],
        allow_empty: bool,
    ) {
        self.begin_record(extra);
        self.push_header_bytes(ident);
        let mut blocks = sequence.chunks_exact(64);
        for block in &mut blocks {
            let masks = extract_fasta_masks(block.try_into().unwrap());
            self.push_bases(sink, masks.two_bits, masks.mask_non_acgt, 0, 64);
        }
        let tail = blocks.remainder();
        if !tail.is_empty() {
            let mut block = [0u8; 64];
            block[..tail.len()].copy_from_slice(tail);
            // The zero padding classifies as non-DNA, so it must stay outside
            // the scanned range instead of being read as an ambiguity.
            let masks = extract_fasta_masks(&block);
            self.push_bases(sink, masks.two_bits, masks.mask_non_acgt, 0, tail.len());
        }
        self.end_record(sink, allow_empty);
    }

    /// Emits the partially filled batch, if it holds anything.
    pub fn finish(&mut self, sink: &mut impl BatchSink<X>) {
        debug_assert!(self.record.is_none(), "a record is still open");
        if self.batch_has_content {
            self.emit_batch(sink, true);
        }
    }

    fn push_acgt_run(
        &mut self,
        sink: &mut impl BatchSink<X>,
        two_bits: u128,
        mut source: usize,
        mut len: usize,
    ) {
        self.record
            .as_mut()
            .expect("bases outside of a record")
            .bases_total += len as u64;
        while len != 0 {
            if self.fragment.is_none() {
                self.start_fragment(sink);
            } else if self.pos == self.bases_per_lane {
                self.continue_in_next_lane(sink);
            }
            let take = len.min(self.bases_per_lane - self.pos);
            let lane = self.lane;
            let pos = self.pos;
            copy_packed_bits(&mut sink.current().words, lane, pos, two_bits, source, take);
            self.pos += take;
            self.record.as_mut().unwrap().source_pos += take as u64;
            source += take;
            len -= take;
        }
    }

    fn start_fragment(&mut self, sink: &mut impl BatchSink<X>) {
        self.header_guard(sink);
        // Starting a fragment that could not hold a single k-mer only to rewind
        // it later would waste the tail of the lane, so move on right away.
        if self.bases_per_lane - self.pos < self.k {
            self.advance_lane(sink);
        }
        let source_start = self.record.as_ref().unwrap().source_pos;
        self.fragment = Some((self.pos as u32, source_start));
    }

    fn continue_in_next_lane(&mut self, sink: &mut impl BatchSink<X>) {
        // The fragment fills the lane to its end, so it holds at least k bases
        // and is always kept.
        self.commit_fragment(sink);
        self.save_overlap(sink);
        self.advance_lane(sink);
        let overlap = self.k - 1;
        let source_start = self.record.as_ref().unwrap().source_pos - overlap as u64;
        self.fragment = Some((0, source_start));
        self.restore_overlap(sink);
    }

    fn save_overlap(&mut self, sink: &mut impl BatchSink<X>) {
        let overlap = self.k - 1;
        self.overlap_buf.clear();
        if overlap == 0 {
            return;
        }
        self.overlap_buf.resize(overlap.div_ceil(16), 0);
        let start = self.pos - overlap;
        let lane = self.lane;
        let batch = sink.current();
        for index in 0..overlap {
            let position = start + index;
            let base =
                (batch.words[(position / 16) * SIMD_LANES + lane] >> (2 * (position % 16))) & 3;
            self.overlap_buf[index / 16] |= base << (2 * (index % 16));
        }
    }

    fn restore_overlap(&mut self, sink: &mut impl BatchSink<X>) {
        debug_assert_eq!(self.pos, 0);
        let lane = self.lane;
        let batch = sink.current();
        // The overlap starts at base zero of the lane, so its words line up
        // with the lane's words one to one.
        for (index, &word) in self.overlap_buf.iter().enumerate() {
            batch.words[index * SIMD_LANES + lane] |= word;
        }
        self.pos = self.k - 1;
        self.batch_has_content = true;
    }

    fn close_fragment(&mut self, sink: &mut impl BatchSink<X>) {
        let Some((lane_start, _)) = self.fragment else {
            return;
        };
        let start = lane_start as usize;
        if self.pos - start >= self.k {
            self.commit_fragment(sink);
            return;
        }
        // Too short to hold a k-mer: take the bases back out, so that
        // everything past the lane fill level stays zero.
        let lane = self.lane;
        let batch = sink.current();
        let mut position = start;
        while position < self.pos {
            let offset = position % 16;
            let take = (self.pos - position).min(16 - offset);
            let mask = low_mask_u32(2 * take) << (2 * offset);
            batch.words[(position / 16) * SIMD_LANES + lane] &= !mask;
            position += take;
        }
        self.pos = start;
        self.fragment = None;
    }

    fn commit_fragment(&mut self, sink: &mut impl BatchSink<X>) {
        let (lane_start, source_start) = self.fragment.take().unwrap();
        let record_idx = self.materialize(sink);
        let lane = self.lane;
        sink.current().lanes[lane].push(LaneFragment {
            lane_start,
            record_idx,
            source_start,
        });
        self.batch_has_content = true;
    }

    fn materialize(&mut self, sink: &mut impl BatchSink<X>) -> u32 {
        let record = self.record.as_ref().expect("no open record");
        if let Some(index) = record.entry {
            return index;
        }
        let read_index = record.read_index;
        let continued = record.continued;
        let extra = record.extra.clone();
        let stream_index = sink.stream_index();
        let batch = sink.current();
        let header = if self.copy_ident {
            let start = batch.headers.len() as u32;
            batch.headers.extend_from_slice(&self.header_buf);
            (start, self.header_buf.len() as u32)
        } else {
            (0, 0)
        };
        let index = batch.records.len() as u32;
        batch.records.push(RecordEntry {
            header,
            read_index,
            bases_total: 0,
            flags: if continued { RECORD_CONTINUED } else { 0 },
            stream_index,
            extra,
        });
        self.record.as_mut().unwrap().entry = Some(index);
        self.batch_has_content = true;
        index
    }

    fn header_guard(&mut self, sink: &mut impl BatchSink<X>) {
        if !self.copy_ident || !self.batch_has_content {
            return;
        }
        if sink.current().headers.len() + self.header_buf.len() > self.max_header_bytes {
            self.emit_batch(sink, false);
        }
    }

    fn advance_lane(&mut self, sink: &mut impl BatchSink<X>) {
        let lane = self.lane;
        let pos = self.pos;
        sink.current().lane_fill[lane] = pos as u32;
        if lane + 1 < SIMD_LANES {
            self.lane += 1;
            self.pos = 0;
        } else {
            self.emit_batch(sink, false);
        }
    }

    fn emit_batch(&mut self, sink: &mut impl BatchSink<X>, is_last: bool) {
        debug_assert!(self.fragment.is_none(), "a fragment crosses a batch");
        let lane = self.lane;
        let pos = self.pos;
        {
            let batch = sink.current();
            batch.lane_fill[lane] = pos as u32;
            for other in lane + 1..SIMD_LANES {
                batch.lane_fill[other] = 0;
            }
            for index in 0..SIMD_LANES {
                batch.lanes[index].push(LaneFragment {
                    lane_start: batch.lane_fill[index],
                    record_idx: NO_RECORD,
                    source_start: 0,
                });
            }
        }
        if let Some(record) = self.record.as_mut() {
            if let Some(index) = record.entry.take() {
                sink.current().records[index as usize].flags |= RECORD_CONTINUES;
                record.continued = true;
            }
        }
        sink.emit(is_last);
        self.lane = 0;
        self.pos = 0;
        self.batch_has_content = false;
    }
}

#[inline(always)]
fn copy_packed_bits(
    words: &mut [u32],
    lane: usize,
    mut destination: usize,
    packed: u128,
    mut source: usize,
    mut len: usize,
) {
    while len != 0 {
        let offset = destination % 16;
        let take = len.min(16 - offset);
        let bits = ((packed >> (2 * source)) as u32) & low_mask_u32(2 * take);
        words[(destination / 16) * SIMD_LANES + lane] |= bits << (2 * offset);
        destination += take;
        source += take;
        len -= take;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::batch::testing::{Reconstructed, check_padding_is_zero, reconstruct};
    use crate::batch::{LaneBatch, VecBatchSink};
    use crate::fasta_lexer::FastaSimdLexer;

    struct Harness {
        packer: LanePacker<u8>,
        sink: VecBatchSink<u8>,
    }

    impl Harness {
        fn new(k: usize, bases_per_lane: usize, ignored_length: usize, copy_ident: bool) -> Self {
            Self {
                packer: LanePacker::new(k, bases_per_lane, ignored_length, copy_ident, 1 << 20)
                    .unwrap(),
                sink: VecBatchSink::new(bases_per_lane),
            }
        }

        fn fasta(&mut self, input: &[u8], chunks: &[usize]) {
            let mut lexer = FastaSimdLexer::new();
            lexer.begin_stream(0u8);
            let mut position = 0;
            let mut index = 0;
            while position < input.len() {
                let take = chunks[index % chunks.len()].min(input.len() - position);
                lexer.push(
                    &mut self.packer,
                    &mut self.sink,
                    &input[position..position + take],
                );
                position += take;
                index += 1;
            }
            lexer.end_stream(&mut self.packer, &mut self.sink);
        }

        fn finish(mut self, k: usize) -> Vec<LaneBatch<u8>> {
            self.packer.finish(&mut self.sink);
            for batch in &self.sink.batches {
                batch.debug_check(k);
                check_padding_is_zero(batch);
            }
            self.sink.batches
        }
    }

    fn fragments(records: &[Reconstructed]) -> Vec<Vec<(u64, String)>> {
        records
            .iter()
            .map(|record| record.fragments.clone())
            .collect()
    }

    #[test]
    fn parses_wrapping_comments_crlf_and_chunkings_identically() {
        let input =
            b";before\r\n>r1 description\r\nacgTT\nAAcgt\r\n;ignored\r\nAA\n>empty\n>r3\r\nTtGAA\n";
        for chunks in [&[1][..], &[63, 1, 64, 2][..], &[65, 7][..], &[4096][..]] {
            let mut harness = Harness::new(4, 64, 0, true);
            harness.fasta(input, chunks);
            let batches = harness.finish(4);
            let records = reconstruct(&batches);
            assert_eq!(
                records
                    .iter()
                    .map(|record| String::from_utf8(record.header.clone()).unwrap())
                    .collect::<Vec<_>>(),
                // An empty record produces no sequence, and the legacy reader
                // drops it, so it also consumes no record index.
                vec![">r1 description", ">r3"],
                "chunks {chunks:?}"
            );
            assert_eq!(
                fragments(&records),
                vec![
                    vec![(0, "ACGTTAACGTAA".to_string())],
                    vec![(0, "TTGAA".to_string())],
                ],
                "chunks {chunks:?}"
            );
            assert_eq!(
                records.iter().map(|r| r.bases_total).collect::<Vec<_>>(),
                vec![12, 5]
            );
            assert_eq!(
                records.iter().map(|r| r.read_index).collect::<Vec<_>>(),
                vec![0, 1]
            );
        }
    }

    #[test]
    fn ambiguous_characters_split_records_into_fragments() {
        let mut harness = Harness::new(4, 64, 0, false);
        harness.fasta(b">a\nACGTACGTNNACGTAC\n>b\nACGNACG\n", &[7]);
        let batches = harness.finish(4);
        let records = reconstruct(&batches);
        assert_eq!(
            fragments(&records),
            vec![
                vec![(0, "ACGTACGT".to_string()), (10, "ACGTAC".to_string())],
                vec![],
            ]
        );
        assert_eq!(
            records.iter().map(|r| r.bases_total).collect::<Vec<_>>(),
            vec![16, 7]
        );
    }

    #[test]
    fn fragments_shorter_than_k_are_removed_without_leaving_bases() {
        let mut harness = Harness::new(5, 32, 0, false);
        harness.fasta(b">a\nACGTNACGTACGT\n", &[64]);
        let batches = harness.finish(5);
        let records = reconstruct(&batches);
        // The four bases before the N cannot hold a 5-mer and are taken back
        // out, so the surviving fragment starts at the lane's first base.
        assert_eq!(fragments(&records), vec![vec![(5, "ACGTACGT".to_string())]]);
        assert_eq!(batches[0].lanes[0][0].lane_start, 0);
        assert_eq!(batches[0].lane_fill[0], 8);
    }

    #[test]
    fn short_records_keep_their_numbering_unless_they_are_ignored() {
        let mut harness = Harness::new(5, 32, 0, false);
        harness.fasta(b">a\nACG\n>b\nACGTACGT\n>c\nAC\n", &[64]);
        let records = reconstruct(&harness.finish(5));
        assert_eq!(records.len(), 3);
        assert_eq!(
            records.iter().map(|r| r.read_index).collect::<Vec<_>>(),
            vec![0, 1, 2]
        );
        assert_eq!(fragments(&records)[0], Vec::new());

        let mut harness = Harness::new(5, 32, 5, false);
        harness.fasta(b">a\nACG\n>b\nACGTACGT\n>c\nAC\n", &[64]);
        let records = reconstruct(&harness.finish(5));
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].read_index, 0);
        assert_eq!(fragments(&records)[0], vec![(0, "ACGTACGT".to_string())]);
    }

    #[test]
    fn long_records_continue_in_the_next_lane_with_a_k_minus_one_overlap() {
        const K: usize = 5;
        let sequence: Vec<u8> = (0..200).map(|index| b"ACGTTGCA"[index % 8]).collect();
        let mut input = b">long\n".to_vec();
        input.extend_from_slice(&sequence);
        input.push(b'\n');
        let mut harness = Harness::new(K, 16, K, false);
        harness.fasta(&input, &[7, 64, 3]);
        let batches = harness.finish(K);
        let records = reconstruct(&batches);
        assert_eq!(records.len(), 1);
        let pieces = &records[0].fragments;
        assert!(
            pieces.len() > 8,
            "expected several lanes, got {}",
            pieces.len()
        );
        let mut rebuilt = String::new();
        for (index, (source_start, bases)) in pieces.iter().enumerate() {
            if index == 0 {
                assert_eq!(*source_start, 0);
                rebuilt.push_str(bases);
            } else {
                let previous = &pieces[index - 1];
                assert_eq!(
                    *source_start,
                    previous.0 + previous.1.len() as u64 - (K - 1) as u64,
                    "fragment {index} does not overlap by k-1"
                );
                rebuilt.push_str(&bases[K - 1..]);
            }
        }
        assert_eq!(rebuilt.as_bytes(), sequence.as_slice());
        assert_eq!(records[0].bases_total, 200);
    }

    #[test]
    fn records_are_packed_into_consecutive_lanes() {
        let mut harness = Harness::new(4, 8, 4, false);
        harness.fasta(b">a\nAAAAAA\n>b\nCCCCC\n>c\nTTTT\n", &[5, 11, 2]);
        let batches = harness.finish(4);
        assert_eq!(batches.len(), 1);
        let batch = &batches[0];
        // `a` fills six of the eight bases, leaving too little room for a
        // 4-mer of `b`, which therefore starts the next lane.
        assert_eq!(batch.lane_fill[0], 6);
        assert_eq!(batch.lanes[0][0].record_idx, 0);
        assert_eq!(batch.lanes[1][0].record_idx, 1);
        assert_eq!(batch.lane_fill[1], 5);
        assert_eq!(batch.lanes[2][0].record_idx, 2);
    }

    #[test]
    fn in_memory_records_handle_every_tail_length() {
        for length in [0usize, 1, 5, 63, 64, 65, 127, 128, 129] {
            let sequence: Vec<u8> = (0..length).map(|index| b"ACGT"[index % 4]).collect();
            let mut harness = Harness::new(4, 256, 0, true);
            harness
                .packer
                .push_record(&mut harness.sink, 0u8, b">x", &sequence, true);
            let batches = harness.finish(4);
            let records = reconstruct(&batches);
            assert_eq!(records.len(), 1, "length {length}");
            assert_eq!(records[0].bases_total, length as u64);
            let expected: Vec<(u64, String)> = if length >= 4 {
                vec![(0, String::from_utf8(sequence).unwrap())]
            } else {
                Vec::new()
            };
            assert_eq!(records[0].fragments, expected, "length {length}");
        }
    }

    #[test]
    fn in_memory_records_split_on_ambiguity_at_block_boundaries() {
        for position in [62usize, 63, 64, 65] {
            let mut sequence: Vec<u8> = (0..130).map(|index| b"ACGT"[index % 4]).collect();
            sequence[position] = b'N';
            let mut harness = Harness::new(4, 256, 0, false);
            harness
                .packer
                .push_record(&mut harness.sink, 0u8, b"", &sequence, true);
            let batches = harness.finish(4);
            let records = reconstruct(&batches);
            assert_eq!(
                records[0].fragments.len(),
                2,
                "an N at {position} must split the record"
            );
            assert_eq!(records[0].fragments[0].0, 0);
            assert_eq!(records[0].fragments[0].1.len(), position);
            assert_eq!(records[0].fragments[1].0, position as u64 + 1);
            assert_eq!(records[0].fragments[1].1.len(), 130 - position - 1);
        }
    }

    #[test]
    fn rejects_impossible_configurations() {
        assert_eq!(
            LanePacker::<u8>::new(0, 8, 0, false, 0).err(),
            Some(PackerError::InvalidK)
        );
        assert_eq!(
            LanePacker::<u8>::new(9, 8, 0, false, 0).err(),
            Some(PackerError::LaneCapacityTooSmall {
                k: 9,
                bases_per_lane: 8
            })
        );
        assert_eq!(
            LanePacker::<u8>::new(4, 8, 5, false, 0).err(),
            Some(PackerError::IgnoredLengthTooLarge {
                k: 4,
                ignored_length: 5
            })
        );
    }
}
