pub mod fasta;
pub mod general;

use crate::sequences_reader::DnaSequence;
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
        callback: impl FnMut(DnaSequence, SequenceInfo),
    );
}
