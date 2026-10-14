//! A sequence as the input readers hand it out, and the formats it comes from.
//!
//! Kept below both the readers and the parsers that produce it, so neither has to depend on
//! the other to agree on it.

use std::path::Path;

#[derive(Copy, Clone)]
pub enum DnaSequencesFileType {
    FASTA,
    FASTQ,
    GFA,
}

impl DnaSequencesFileType {
    /// The format a file name implies, looking past compression extensions (`x.fq.gz`). A
    /// member of an archive (`x.tar:dir/a.fa`) is recognized by its own name.
    pub fn from_name(name: &str) -> Option<Self> {
        const FASTQ_EXTS: &[&str] = &["fq", "fastq"];
        const FASTA_EXTS: &[&str] = &["fa", "fasta", "fna", "ffn"];

        let mut tmp = Path::new(name).file_name()?.to_str()?;
        let mut path: &Path = tmp.as_ref();

        while let Some(ext) = path.extension() {
            if FASTQ_EXTS.contains(&ext.to_str().unwrap()) {
                return Some(DnaSequencesFileType::FASTQ);
            }
            if FASTA_EXTS.contains(&ext.to_str().unwrap()) {
                return Some(DnaSequencesFileType::FASTA);
            }
            tmp = &tmp[0..tmp.len() - ext.len() - 1];
            path = tmp.as_ref()
        }
        None
    }
}

/// `seq` is the bases, as plain bytes or packed: whatever the code using it needs of them, it
/// asks for itself, as `utils` cannot depend on `hashes`.
#[derive(Copy, Clone)]
pub struct DnaSequence<'a, S: 'a> {
    pub ident_data: &'a [u8],
    pub seq: S,
    pub format: DnaSequencesFileType,
}
