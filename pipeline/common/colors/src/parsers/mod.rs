pub mod graph;
pub mod separate;

pub enum SequenceIdent<'a> {
    Fasta(&'a [u8]),
    Gfa { colors: &'a [u8] },
}

pub struct SingleSequenceInfo<'a> {
    pub file_index: usize,
    pub sequence_ident: SequenceIdent<'a>,
}
