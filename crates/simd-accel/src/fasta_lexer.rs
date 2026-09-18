//! Incremental FASTA lexer feeding a [`LanePacker`].
//!
//! Adapted from helicase (`helicase/src/simd`), MIT License, Copyright (c) 2025 Igor Martayan.

use crate::batch::BatchSink;
use crate::masks::{extract_fasta_masks, next_set_bit};
use crate::packer::LanePacker;

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
enum Mode {
    Header,
    Sequence,
    Comment,
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

    /// Starts a new FASTA byte stream; `extra` is attached to all its records.
    pub fn begin_stream(&mut self, extra: X) {
        self.tail_len = 0;
        self.mode = Mode::Sequence;
        self.at_line_start = true;
        self.in_record = false;
        self.extra = Some(extra);
    }

    pub fn push(
        &mut self,
        packer: &mut LanePacker<X>,
        sink: &mut impl BatchSink<X>,
        mut bytes: &[u8],
    ) {
        if self.tail_len != 0 {
            let take = bytes.len().min(64 - self.tail_len);
            self.tail[self.tail_len..self.tail_len + take].copy_from_slice(&bytes[..take]);
            self.tail_len += take;
            bytes = &bytes[take..];
            if self.tail_len == 64 {
                let block = self.tail;
                self.process_block(packer, sink, &block, 64);
                self.tail_len = 0;
            }
        }
        let mut blocks = bytes.chunks_exact(64);
        for block in &mut blocks {
            self.process_block(packer, sink, block.try_into().unwrap(), 64);
        }
        let remainder = blocks.remainder();
        if !remainder.is_empty() {
            self.tail[..remainder.len()].copy_from_slice(remainder);
            self.tail_len = remainder.len();
        }
    }

    /// Ends the stream: flushes the pending bytes and closes the open record.
    pub fn end_stream(&mut self, packer: &mut LanePacker<X>, sink: &mut impl BatchSink<X>) {
        if self.tail_len != 0 {
            let valid_len = self.tail_len;
            self.tail[valid_len..].fill(0);
            let block = self.tail;
            self.process_block(packer, sink, &block, valid_len);
            self.tail_len = 0;
        }
        if self.in_record {
            packer.end_record(sink, false);
            self.in_record = false;
        }
        self.extra = None;
    }

    fn process_block(
        &mut self,
        packer: &mut LanePacker<X>,
        sink: &mut impl BatchSink<X>,
        block: &[u8; 64],
        valid_len: usize,
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
                        packer.end_record(sink, false);
                    }
                    packer.begin_record(self.extra.clone().expect("no active stream"));
                    packer.push_header_bytes(b">");
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
                    packer.begin_record(self.extra.clone().expect("no active stream"));
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
                Mode::Header => packer.push_header_bytes(&block[position..end]),
                Mode::Comment => {}
                Mode::Sequence => packer.push_bases(
                    sink,
                    masks.two_bits,
                    masks.mask_non_acgt,
                    position,
                    end - position,
                ),
            }
            position = end;
        }
    }
}
