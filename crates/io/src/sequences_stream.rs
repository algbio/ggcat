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
    fn read_block(
        &mut self,
        block: &Self::SequenceBlockData,
        settings: &ParserSettings,
        callback: impl FnMut(SequenceBuffer, SequenceInfo) -> SequenceBuffer,
    ) -> u64;
}
