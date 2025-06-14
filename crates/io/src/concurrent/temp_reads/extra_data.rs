use crate::varint::{VARINT_MAX_SIZE, decode_varint, encode_varint};
use byteorder::ReadBytesExt;
use config::ColorIndexType;
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

pub trait SequenceExtraDataTempBufferManagement: Sized + Sync + Send + Debug + Clone {
    type TempBuffer: Sync + Send;

    fn new_temp_buffer() -> Self::TempBuffer;
    fn clear_temp_buffer(buffer: &mut Self::TempBuffer);
    fn copy_temp_buffer(dest: &mut Self::TempBuffer, src: &Self::TempBuffer);

    fn copy_extra_from(extra: Self, src: &Self::TempBuffer, dst: &mut Self::TempBuffer) -> Self;
}

pub trait HasEmptyExtraBuffer: Sized + Sync + Send + Debug + Clone {}
impl<T: HasEmptyExtraBuffer> SequenceExtraDataTempBufferManagement for T {
    type TempBuffer = ();

    #[inline(always)]
    fn new_temp_buffer() -> () {
        ()
    }

    #[inline(always)]
    fn clear_temp_buffer(_buffer: &mut ()) {}

    fn copy_temp_buffer(_dest: &mut (), _src: &()) {}

    #[inline(always)]
    fn copy_extra_from(extra: Self, _src: &(), _dst: &mut ()) -> Self {
        extra
    }
}

pub trait SequenceExtraDataConsecutiveCompression: SequenceExtraDataTempBufferManagement {
    type LastData: Default + Copy + Sync + Send;

    #[inline(always)]
    fn decode_from_slice_extended(
        buffer: &mut Self::TempBuffer,
        slice: &[u8],
        last_data: Self::LastData,
    ) -> Option<Self> {
        let mut cursor = Cursor::new(slice);
        Self::decode_extended(buffer, &mut cursor, last_data)
    }

    #[inline(always)]
    unsafe fn decode_from_pointer_extended(
        buffer: &mut Self::TempBuffer,
        ptr: *const u8,
        last_data: Self::LastData,
    ) -> Option<Self> {
        let mut stream = PointerDecoder { ptr };
        Self::decode_extended(buffer, &mut stream, last_data)
    }

    fn decode_extended(
        buffer: &mut Self::TempBuffer,
        reader: &mut impl Read,
        last_data: Self::LastData,
    ) -> Option<Self>;
    fn encode_extended(
        &self,
        buffer: &Self::TempBuffer,
        writer: &mut impl Write,
        last_data: Self::LastData,
    );

    fn obtain_last_data(&self, last_data: Self::LastData) -> Self::LastData;

    fn max_size(&self) -> usize;
}

pub trait SequenceExtraDataCombiner: SequenceExtraDataConsecutiveCompression {
    type SingleDataType: SequenceExtraDataConsecutiveCompression;
    fn combine_entries(
        &mut self,
        out_buffer: &mut Self::TempBuffer,
        color: Self,
        in_buffer: &Self::TempBuffer,
    );

    fn to_single(
        &self,
        in_buffer: &Self::TempBuffer,
        out_buffer: &mut <Self::SingleDataType as SequenceExtraDataTempBufferManagement>::TempBuffer,
    ) -> Self::SingleDataType;

    fn prepare_for_serialization(&mut self, buffer: &mut Self::TempBuffer);

    fn from_single_entry<'a>(
        out_buffer: &'a mut Self::TempBuffer,
        single: Self::SingleDataType,
        in_buffer: &'a mut <Self::SingleDataType as SequenceExtraDataTempBufferManagement>::TempBuffer,
    ) -> (Self, &'a mut Self::TempBuffer);
}

pub trait SequenceExtraData: SequenceExtraDataTempBufferManagement {
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

pub trait SequenceExtraDataOwned: SequenceExtraDataConsecutiveCompression {
    fn decode_from_slice(slice: &[u8], last_data: Self::LastData) -> Option<Self>;

    unsafe fn decode_from_pointer(ptr: *const u8, last_data: Self::LastData) -> Option<Self>;

    fn decode(reader: &mut impl Read, last_data: Self::LastData) -> Option<Self>;
    fn encode(&self, writer: &mut impl Write, last_data: Self::LastData);
}
impl<T: SequenceExtraDataConsecutiveCompression<TempBuffer = ()>> SequenceExtraDataOwned for T {
    #[inline(always)]
    fn decode_from_slice(slice: &[u8], last_data: Self::LastData) -> Option<Self> {
        Self::decode_from_slice_extended(&mut (), slice, last_data)
    }

    #[inline(always)]
    unsafe fn decode_from_pointer(ptr: *const u8, last_data: Self::LastData) -> Option<Self> {
        unsafe { Self::decode_from_pointer_extended(&mut (), ptr, last_data) }
    }

    fn decode(reader: &mut impl Read, last_data: Self::LastData) -> Option<Self> {
        Self::decode_extended(&mut (), reader, last_data)
    }

    fn encode(&self, writer: &mut impl Write, last_data: Self::LastData) {
        self.encode_extended(&mut (), writer, last_data)
    }
}

impl<T: SequenceExtraData> SequenceExtraDataConsecutiveCompression for T {
    type LastData = ();

    #[inline(always)]
    fn decode_extended(
        buffer: &mut Self::TempBuffer,
        reader: &mut impl Read,
        _last_data: Self::LastData,
    ) -> Option<Self> {
        <Self as SequenceExtraData>::decode_extended(buffer, reader)
    }

    #[inline(always)]
    fn encode_extended(
        &self,
        buffer: &Self::TempBuffer,
        writer: &mut impl Write,
        _last_data: Self::LastData,
    ) {
        <Self as SequenceExtraData>::encode_extended(&self, buffer, writer)
    }

    #[inline(always)]
    fn max_size(&self) -> usize {
        <Self as SequenceExtraData>::max_size(self)
    }

    fn obtain_last_data(&self, _last_data: Self::LastData) -> Self::LastData {
        ()
    }
}

impl HasEmptyExtraBuffer for () {}
impl SequenceExtraData for () {
    #[inline(always)]
    fn decode_extended(_buffer: &mut Self::TempBuffer, _reader: &mut impl Read) -> Option<Self> {
        Some(())
    }

    #[inline(always)]
    fn encode_extended(&self, _buffer: &Self::TempBuffer, _writer: &mut impl Write) {}

    #[inline(always)]
    fn max_size(&self) -> usize {
        0
    }
}

impl HasEmptyExtraBuffer for ColorIndexType {}
impl SequenceExtraData for ColorIndexType {
    fn decode_extended(_: &mut Self::TempBuffer, reader: &mut impl Read) -> Option<Self> {
        decode_varint(|| reader.read_u8().ok()).map(|x| x as ColorIndexType)
    }

    fn encode_extended(&self, _: &Self::TempBuffer, writer: &mut impl Write) {
        encode_varint(|b| writer.write_all(b).unwrap(), *self as u64);
    }

    fn max_size(&self) -> usize {
        VARINT_MAX_SIZE
    }
}
