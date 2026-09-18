pub mod fasta;
pub mod general;
pub mod tar;

use crate::sequences_reader::DnaSequence;
use crate::sequences_sink::SequencesSink;
use config::ColorIndexType;

#[derive(Copy, Clone)]
pub struct SequenceInfo {
    pub color: Option<ColorIndexType>,
}

pub trait GenericSequencesStream: Sync + Send + 'static {
    type SequenceBlockData: Sync + Send + 'static;

    fn new() -> Self;

    fn read_block(
        &mut self,
        block: &Self::SequenceBlockData,
        copy_ident_data: bool,
        partial_read_copyback: Option<usize>,
        callback: impl FnMut(DnaSequence<&[u8]>, SequenceInfo),
    );

    /// Reads a block straight into a sink, without cutting it into records
    /// first, so that a vectorized parser can see whole blocks of input.
    fn read_block_into(
        &mut self,
        block: &Self::SequenceBlockData,
        sink: &mut impl SequencesSink,
    ) -> anyhow::Result<()>;
}
