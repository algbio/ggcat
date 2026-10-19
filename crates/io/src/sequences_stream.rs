pub mod file;
pub mod general;

use crate::simd::{ParserSettings, SequenceBuffer};
use config::ColorIndexType;

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct SequenceInfo {
    pub color: Option<ColorIndexType>,
}

pub trait GenericSequencesStream: Sync + Send + 'static {
    type SequenceBlockData: Sync + Send + 'static;

    fn new() -> Self;

    /// Reads `block` through a parser of `settings`, handing `callback` each buffer it fills,
    /// with the information of all its sequences: `callback` gives back a buffer to fill later,
    /// as `SequenceParser::parse_data` asks. Returns how many records the block held, as the
    /// parser counts them.
    ///
    /// As each file of the block starts, the block's own or a member of the archive it is,
    /// `file_info` gets its name, `archive:member` for a member, and the information the block
    /// gives its sequences, and returns the information of that file's sequences. Blocks whose
    /// records bring their own information do not call it.
    fn read_block(
        &mut self,
        block: &Self::SequenceBlockData,
        settings: &ParserSettings,
        file_info: impl FnMut(&str, SequenceInfo) -> SequenceInfo,
        callback: impl FnMut(SequenceBuffer, SequenceInfo) -> SequenceBuffer,
    ) -> u64;
}
