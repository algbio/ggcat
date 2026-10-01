//! Where the input readers deliver their sequences.
//!
//! The byte-oriented entry points let a vectorized parser see whole blocks of
//! the input, while `push_record` keeps working for the sources that can only
//! provide one record at a time.

use crate::sequences_reader::{DnaSequence, DnaSequencesFileType};
use crate::sequences_stream::SequenceInfo;
/// Re-exported so a sink can name the copies without depending on the decoder crate.
pub use lz_copyback::LzCopy;

pub trait SequencesSink {
    /// Whether record identifiers are needed; sources that have to copy them
    /// can skip the work when they are not.
    fn wants_ident(&self) -> bool;

    /// Whether this sink can use the LZ copies of its input. When it cannot, the
    /// reader picks the cheaper plain decoder and never calls the methods below.
    fn wants_copies(&self) -> bool {
        false
    }

    /// Copies shorter than this are not worth reporting. The parser knows what it needs
    /// and the reader only passes it on.
    fn copyback_min_len(&self) -> u64 {
        1
    }

    /// Starts one *tracked input*: a whole archive, or one loose file, named by `path`.
    /// This is coarser than [`Self::begin_stream`], which fires once per archive member,
    /// because the copies of a `.tar.xz` mostly cross members and the offsets below are
    /// absolute in the decompressed archive.
    ///
    /// `retain` is how many bytes behind the latest one a copy can still reach: the
    /// stream's largest LZ distance plus the decoder's windows.
    fn begin_tracked_input(&mut self, path: &std::path::Path, retain: u64) {
        let _ = (path, retain);
    }

    /// Ends the tracked input begun by [`Self::begin_tracked_input`], after its last
    /// stream has ended.
    fn end_tracked_input(&mut self) {}

    /// The `len` bytes at absolute offset `at` are not part of any FASTA stream: tar
    /// headers and padding, members of another format, members that are themselves
    /// compressed. Together with [`Self::push_bytes_tracked`] these must cover the
    /// tracked input contiguously.
    fn mark_opaque(&mut self, at: u64, len: u64) {
        let _ = (at, len);
    }

    /// Feeds raw decompressed bytes whose first byte is at absolute offset `base`,
    /// together with the LZ copies of the current window: `forward` are the ones whose
    /// *source* lies in `bytes`, `backward` the ones whose *destination* does. Both are
    /// clipped to `bytes`.
    fn push_bytes_tracked(
        &mut self,
        base: u64,
        bytes: &[u8],
        forward: &[LzCopy],
        backward: &[LzCopy],
    ) {
        let _ = (base, forward, backward);
        self.push_bytes(bytes);
    }

    /// Starts one file or one archive member.
    fn begin_stream(&mut self, format: DnaSequencesFileType, info: SequenceInfo);

    /// Feeds raw decompressed bytes of the current stream.
    fn push_bytes(&mut self, bytes: &[u8]);

    /// Ends the current stream, flushing whatever it left pending.
    fn end_stream(&mut self);

    /// Delivers a complete record, outside of any byte stream.
    fn push_record(&mut self, sequence: DnaSequence<&[u8]>, info: SequenceInfo);
}
