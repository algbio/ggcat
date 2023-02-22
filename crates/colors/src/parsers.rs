use config::ColorIndexType;

pub mod graph;
pub mod separate;

pub enum SequenceIdent<'a> {
    FASTA(&'a [u8]),
    GFA { colors: &'a [u8] },
}

pub struct SingleSequenceInfo<'a> {
    pub static_color: ColorIndexType,
    pub sequence_ident: SequenceIdent<'a>,
}
