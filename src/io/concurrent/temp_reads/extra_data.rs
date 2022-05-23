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

pub trait SequenceExtraDataTempBufferManagement<T> {
    fn new_temp_buffer() -> T;
    fn clear_temp_buffer(buffer: &mut T);

    fn copy_extra_from(extra: Self, src: &T, dst: &mut T) -> Self;
}

impl<T: SequenceExtraData<TempBuffer = ()>> SequenceExtraDataTempBufferManagement<()> for T {
    #[inline(always)]
    fn new_temp_buffer() -> () {
        ()
    }

    #[inline(always)]
    fn clear_temp_buffer(_buffer: &mut ()) {}

    #[inline(always)]
    fn copy_extra_from(extra: Self, _src: &(), _dst: &mut ()) -> Self {
        extra
    }
}

pub trait SequenceExtraData:
    Sized + Sync + Send + Debug + Clone + SequenceExtraDataTempBufferManagement<Self::TempBuffer>
{
    type TempBuffer: Sync + Send;

    #[inline(always)]
    fn decode_from_slice_extended(buffer: &mut Self::TempBuffer, slice: &[u8]) -> Option<Self> {
        let mut cursor = Cursor::new(slice);
        Self::decode_extended(buffer, &mut cursor)
    }

    #[inline(always)]
    unsafe fn decode_from_pointer_extended(
        buffer: &mut Self::TempBuffer,
        ptr: *const u8,
    ) -> Option<Self> {
        let mut stream = PointerDecoder { ptr };
        Self::decode_extended(buffer, &mut stream)
    }

    fn decode_extended(buffer: &mut Self::TempBuffer, reader: &mut impl Read) -> Option<Self>;
    fn encode_extended(&self, buffer: &Self::TempBuffer, writer: &mut impl Write);

    fn max_size(&self) -> usize;
}

pub trait SequenceExtraDataOwned: SequenceExtraData {
    #[inline(always)]
    fn decode_from_slice(slice: &[u8]) -> Option<Self>;

    #[inline(always)]
    unsafe fn decode_from_pointer(ptr: *const u8) -> Option<Self>;

    fn decode(reader: &mut impl Read) -> Option<Self>;
    fn encode(&self, writer: &mut impl Write);
}
impl<T: SequenceExtraData<TempBuffer = ()>> SequenceExtraDataOwned for T {
    #[inline(always)]
    fn decode_from_slice(slice: &[u8]) -> Option<Self> {
        Self::decode_from_slice_extended(&mut (), slice)
    }

    #[inline(always)]
    unsafe fn decode_from_pointer(ptr: *const u8) -> Option<Self> {
        Self::decode_from_pointer_extended(&mut (), ptr)
    }

    fn decode(reader: &mut impl Read) -> Option<Self> {
        Self::decode_extended(&mut (), reader)
    }

    fn encode(&self, writer: &mut impl Write) {
        self.encode_extended(&mut (), writer)
    }
}

impl SequenceExtraData for () {
    type TempBuffer = ();

    fn decode_extended(_buffer: &mut Self::TempBuffer, _reader: &mut impl Read) -> Option<Self> {
        Some(())
    }

    fn encode_extended(&self, _buffer: &Self::TempBuffer, _writer: &mut impl Write) {}

    #[inline(always)]
    fn max_size(&self) -> usize {
        0
    }
}
