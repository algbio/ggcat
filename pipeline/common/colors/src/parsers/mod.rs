pub mod graph;
pub mod separate;

pub enum SequenceIdent<'a> {
    FASTA(&'a [u8]),
    GFA { colors: &'a [u8] },
}

pub struct SingleSequenceInfo<'a> {
    pub file_index: usize,
    pub sequence_ident: SequenceIdent<'a>,
}
