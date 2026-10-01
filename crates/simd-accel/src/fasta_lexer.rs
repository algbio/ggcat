//! Incremental FASTA lexer feeding a [`LanePacker`].
//!
//! Adapted from helicase (`helicase/src/simd`), MIT License, Copyright (c) 2025 Igor Martayan.

use crate::batch::BatchSink;
use crate::masks::{RawFastaMasks, extract_fasta_masks, next_set_bit};
use crate::packer::LanePacker;

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
enum Mode {
    Header,
    Sequence,
    Comment,
}

/// Where the lexer's parsed output goes: straight into a [`LanePacker`], or into one
/// through the tagging of `crate::fasta_copyback`.
///
/// The positions are absolute stream offsets inside a tracked span
/// ([`FastaSimdLexer::parse_span`]), and meaningless otherwise.
pub trait LexOutput<X> {
    /// A record starts, at the `>` at `pos` (or at its first base, for bases before any
    /// header).
    fn begin_record(&mut self, extra: X, pos: u64);
    /// Header bytes, the first of which is at `pos`; carriage returns are left out.
    fn push_header_bytes(&mut self, bytes: &[u8], pos: u64);
    /// Sequence bytes `[first, first + len)` of a classified block whose byte 0 is at
    /// `block_pos`: its ACGT bases, and the non-ACGT characters of
    /// `masks.mask_non_acgt` between them.
    fn push_bases(&mut self, masks: &RawFastaMasks, first: usize, len: usize, block_pos: u64);
    fn end_record(&mut self);
}

/// The direct output: every call goes to the packer at once.
struct PackerOutput<'a, X: Clone, S: BatchSink<X>> {
    packer: &'a mut LanePacker<X>,
    sink: &'a mut S,
}

impl<X: Clone, S: BatchSink<X>> LexOutput<X> for PackerOutput<'_, X, S> {
    #[inline(always)]
    fn begin_record(&mut self, extra: X, _pos: u64) {
        self.packer.begin_record(extra);
    }

    #[inline(always)]
    fn push_header_bytes(&mut self, bytes: &[u8], _pos: u64) {
        self.packer.push_header_bytes(bytes);
    }

    #[inline(always)]
    fn push_bases(&mut self, masks: &RawFastaMasks, first: usize, len: usize, _block_pos: u64) {
        self.packer
            .push_bases(self.sink, masks.two_bits, masks.mask_non_acgt, first, len);
    }

    #[inline(always)]
    fn end_record(&mut self) {
        self.packer.end_record(self.sink, false);
    }
}

pub struct FastaSimdLexer<X: Clone> {
    tail: [u8; 64],
    tail_len: usize,
    mode: Mode,
    at_line_start: bool,
    in_record: bool,
    extra: Option<X>,
}

impl<X: Clone> Default for FastaSimdLexer<X> {
    fn default() -> Self {
        Self::new()
    }
}

impl<X: Clone> FastaSimdLexer<X> {
    pub fn new() -> Self {
        Self {
            tail: [0; 64],
            tail_len: 0,
            mode: Mode::Sequence,
            at_line_start: true,
            in_record: false,
            extra: None,
        }
    }

    /// Whether the lexer stands inside a sequence line of an open record: the state that
    /// lets `crate::fasta_copyback` skip a stretch whose source was parsed in that state.
    pub fn in_sequence_record(&self) -> bool {
        self.mode == Mode::Sequence && self.in_record
    }

    /// Resumes after a skipped stretch that ends just before a base of an open record.
    /// Whether that base starts its line does not matter: a base is lexed the same way
    /// either way once a record is open.
    pub fn resume_in_sequence(&mut self) {
        debug_assert!(self.in_record && self.tail_len == 0);
        self.mode = Mode::Sequence;
        self.at_line_start = false;
    }

    /// What the records of the current stream carry.
    pub fn stream_extra(&self) -> X {
        self.extra.clone().expect("no active stream")
    }

    /// Starts a new FASTA byte stream; `extra` is attached to all its records.
    pub fn begin_stream(&mut self, extra: X) {
        self.tail_len = 0;
        self.mode = Mode::Sequence;
        self.at_line_start = true;
        self.in_record = false;
        self.extra = Some(extra);
    }

    pub fn push(&mut self, packer: &mut LanePacker<X>, sink: &mut impl BatchSink<X>, bytes: &[u8]) {
        let mut out = PackerOutput { packer, sink };
        self.push_into(&mut out, bytes);
    }

    fn push_into(&mut self, out: &mut impl LexOutput<X>, mut bytes: &[u8]) {
        if self.tail_len != 0 {
            let take = bytes.len().min(64 - self.tail_len);
            self.tail[self.tail_len..self.tail_len + take].copy_from_slice(&bytes[..take]);
            self.tail_len += take;
            bytes = &bytes[take..];
            if self.tail_len == 64 {
                let block = self.tail;
                self.process_block(out, &block, 64, 0);
                self.tail_len = 0;
            }
        }
        let mut blocks = bytes.chunks_exact(64);
        for block in &mut blocks {
            self.process_block(out, block.try_into().unwrap(), 64, 0);
        }
        let remainder = blocks.remainder();
        if !remainder.is_empty() {
            self.tail[..remainder.len()].copy_from_slice(remainder);
            self.tail_len = remainder.len();
        }
    }

    /// Lexes a stretch of an LZ-tracked stream into `out`, whose first byte is at absolute
    /// stream offset `base`, telling `out` where each block sits.
    ///
    /// Unlike [`Self::push`] nothing is held back: the last partial block is lexed as
    /// well, so everything has been handed over when this returns, and the next stretch
    /// starts a fresh block, possibly after a skipped one. The lexer only ever looks at
    /// bytes already seen, so cutting the blocks there changes nothing. A record left
    /// open stays open.
    pub fn parse_span(&mut self, out: &mut impl LexOutput<X>, base: u64, bytes: &[u8]) {
        debug_assert_eq!(self.tail_len, 0, "a tracked span must start a fresh block");
        let mut blocks = bytes.chunks_exact(64);
        let mut position = base;
        for block in &mut blocks {
            self.process_block(out, block.try_into().unwrap(), 64, position);
            position += 64;
        }
        let remainder = blocks.remainder();
        if !remainder.is_empty() {
            let mut block = [0u8; 64];
            block[..remainder.len()].copy_from_slice(remainder);
            self.process_block(out, &block, remainder.len(), position);
        }
    }

    /// Ends the stream: flushes the pending bytes and closes the open record.
    pub fn end_stream(&mut self, packer: &mut LanePacker<X>, sink: &mut impl BatchSink<X>) {
        let mut out = PackerOutput { packer, sink };
        if self.tail_len != 0 {
            let valid_len = self.tail_len;
            self.tail[valid_len..].fill(0);
            let block = self.tail;
            self.process_block(&mut out, &block, valid_len, 0);
            self.tail_len = 0;
        }
        if self.in_record {
            out.end_record();
            self.in_record = false;
        }
        self.extra = None;
    }

    fn process_block(
        &mut self,
        out: &mut impl LexOutput<X>,
        block: &[u8; 64],
        valid_len: usize,
        block_pos: u64,
    ) {
        let masks = extract_fasta_masks(block);
        let mut position = 0usize;
        while position < valid_len {
            if masks.line_feeds & (1u64 << position) != 0 {
                if self.mode != Mode::Sequence {
                    self.mode = Mode::Sequence;
                }
                self.at_line_start = true;
                position += 1;
                continue;
            }
            if masks.carriage_returns & (1u64 << position) != 0 {
                position += 1;
                continue;
            }

            if self.at_line_start {
                self.at_line_start = false;
                if masks.open_brackets & (1u64 << position) != 0 {
                    if self.in_record {
                        out.end_record();
                    }
                    let at = block_pos + position as u64;
                    out.begin_record(self.extra.clone().expect("no active stream"), at);
                    out.push_header_bytes(b">", at);
                    self.in_record = true;
                    self.mode = Mode::Header;
                    position += 1;
                    continue;
                }
                if block[position] == b';' {
                    self.mode = Mode::Comment;
                    position += 1;
                    continue;
                }
                if !self.in_record {
                    // Bases before any header: the legacy reader keeps them as
                    // one record without an identifier.
                    out.begin_record(
                        self.extra.clone().expect("no active stream"),
                        block_pos + position as u64,
                    );
                    self.in_record = true;
                }
                self.mode = Mode::Sequence;
            }

            // Carriage returns must end a run too, or they would be packed as
            // ambiguity characters and split the fragment.
            let line_end = next_set_bit(masks.line_feeds, position).unwrap_or(valid_len);
            let carriage = next_set_bit(masks.carriage_returns, position).unwrap_or(valid_len);
            let end = line_end.min(carriage).min(valid_len);
            match self.mode {
                Mode::Header => {
                    out.push_header_bytes(&block[position..end], block_pos + position as u64)
                }
                Mode::Comment => {}
                Mode::Sequence => out.push_bases(&masks, position, end - position, block_pos),
            }
            position = end;
        }
    }
}
