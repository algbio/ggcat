use core::fmt::Debug;
use std::io::{Cursor, Read, Write};

struct PointerDecoder {
    ptr: *const u8,
}

impl Read for PointerDecoder {
    #[inline(always)]
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        unsafe {
            std::ptr::copy_nonoverlapping(self.ptr, buf.as_mut_ptr(), buf.len());
            self.ptr = self.ptr.add(buf.len());
        }
        Ok(buf.len())
    }

    #[inline(always)]
    fn read_exact(&mut self, buf: &mut [u8]) -> std::io::Result<()> {
        unsafe {
            std::ptr::copy_nonoverlapping(self.ptr, buf.as_mut_ptr(), buf.len());
            self.ptr = self.ptr.add(buf.len());
        }
        Ok(())
    }
}

pub trait SequenceExtraData: Sized + Send + Debug {
    fn decode_from_slice(slice: &[u8]) -> Option<Self> {
        let mut cursor = Cursor::new(slice);
        Self::decode(&mut cursor)
    }

    unsafe fn decode_from_pointer(ptr: *const u8) -> Option<Self> {
        let mut stream = PointerDecoder { ptr };
        Self::decode(&mut stream)
    }

    fn decode<'a>(reader: &'a mut impl Read) -> Option<Self>;
    fn encode<'a>(&self, writer: &'a mut impl Write);

    fn max_size(&self) -> usize;
}

impl SequenceExtraData for () {
    #[inline(always)]
    fn decode<'a>(_reader: &'a mut impl Read) -> Option<Self> {
        Some(())
    }

    #[inline(always)]
    fn encode<'a>(&self, _writer: &'a mut impl Write) {}

    #[inline(always)]
    fn max_size(&self) -> usize {
        0
    }
}
