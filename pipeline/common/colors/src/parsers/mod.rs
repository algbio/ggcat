pub mod graph;
pub mod separate;

pub struct SingleSequenceInfo<'a> {
    pub file_index: usize,
    pub sequence_ident: &'a [u8],
}
