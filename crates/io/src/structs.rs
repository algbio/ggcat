use bincode::enc::write::Writer;

pub mod hash_entry;
pub mod unitig_link;

#[repr(transparent)]
pub struct VecWriterMut<'a>(pub &'a mut Vec<u8>);

impl<'a> Writer for VecWriterMut<'a> {
    fn write(&mut self, bytes: &[u8]) -> Result<(), bincode::error::EncodeError> {
        self.0.extend_from_slice(bytes);
        Ok(())
    }
}
