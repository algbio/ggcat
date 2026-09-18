//! Where the input readers deliver their sequences.
//!
//! The byte-oriented entry points let a vectorized parser see whole blocks of
//! the input, while `push_record` keeps working for the sources that can only
//! provide one record at a time.

use crate::sequences_reader::{DnaSequence, DnaSequencesFileType};
use crate::sequences_stream::SequenceInfo;

pub trait SequencesSink {
    /// Whether record identifiers are needed; sources that have to copy them
    /// can skip the work when they are not.
    fn wants_ident(&self) -> bool;

    /// Starts one file or one archive member.
    fn begin_stream(&mut self, format: DnaSequencesFileType, info: SequenceInfo);

    /// Feeds raw decompressed bytes of the current stream.
    fn push_bytes(&mut self, bytes: &[u8]);

    /// Ends the current stream, flushing whatever it left pending.
    fn end_stream(&mut self);

    /// Delivers a complete record, outside of any byte stream.
    fn push_record(&mut self, sequence: DnaSequence<&[u8]>, info: SequenceInfo);
}
