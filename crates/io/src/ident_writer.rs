use std::io::Write;

use crate::concurrent::temp_reads::extra_data::SequenceExtraDataConsecutiveCompression;

pub trait IdentSequenceWriter: SequenceExtraDataConsecutiveCompression + Sized {
    fn write_as_ident(&self, stream: &mut impl Write, extra_buffer: &Self::TempBuffer);
    fn write_as_gfa<const VERSION: u32>(
        &self,
        k: u64,
        index: u64,
        length: u64,
        stream: &mut impl Write,
        extra_buffer: &Self::TempBuffer,
    );

    fn parse_as_ident<'a>(ident: &[u8], extra_buffer: &mut Self::TempBuffer) -> Option<Self>;

    fn parse_as_gfa<'a>(ident: &[u8], extra_buffer: &mut Self::TempBuffer) -> Option<Self>;
}

impl IdentSequenceWriter for () {
    fn write_as_ident(&self, _stream: &mut impl Write, _extra_buffer: &Self::TempBuffer) {}

    fn write_as_gfa<const VERSION: u32>(
        &self,
        _k: u64,
        _index: u64,
        _length: u64,
        _stream: &mut impl Write,
        _extra_buffer: &Self::TempBuffer,
    ) {
    }

    fn parse_as_ident<'a>(_ident: &[u8], _extra_buffer: &mut Self::TempBuffer) -> Option<Self> {
        Some(())
    }

    fn parse_as_gfa<'a>(_ident: &[u8], _extra_buffer: &mut Self::TempBuffer) -> Option<Self> {
        Some(())
    }
}
