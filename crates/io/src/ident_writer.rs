use std::io::Write;

use crate::concurrent::temp_reads::extra_data::SequenceExtraDataConsecutiveCompression;

pub trait IdentSequenceWriter: SequenceExtraDataConsecutiveCompression + Sized {
    type PartialConnectionData: Default;
    fn write_as_ident(
        &self,
        partial_data: &mut Self::PartialConnectionData,
        write_range: (usize, Option<usize>),
        reverse_complement: bool,
        stream: &mut impl Write,
        extra_buffer: &Self::TempBuffer,
    );
    fn flush_partial_as_ident(partial_data: Self::PartialConnectionData, stream: &mut impl Write);

    fn write_as_gfa<const VERSION: u32>(
        &self,
        k: u64,
        index: u64,
        length: u64,
        partial_data: &mut Self::PartialConnectionData,
        write_range: (usize, Option<usize>),
        reverse_complement: bool,
        stream: &mut impl Write,
        extra_buffer: &Self::TempBuffer,
    );
    fn flush_partial_as_gfa<const VERSION: u32>(
        k: u64,
        index: u64,
        length: u64,
        partial_data: Self::PartialConnectionData,
        stream: &mut impl Write,
    );

    fn parse_as_ident<'a>(
        ident: &[u8],
        extra_buffer: &mut Self::TempBuffer,
    ) -> Option<(Self, usize)>;

    fn parse_as_gfa<'a>(ident: &[u8], extra_buffer: &mut Self::TempBuffer)
    -> Option<(Self, usize)>;
}

impl IdentSequenceWriter for () {
    type PartialConnectionData = ();

    #[inline(always)]
    fn parse_as_ident<'a>(
        _ident: &[u8],
        _extra_buffer: &mut Self::TempBuffer,
    ) -> Option<(Self, usize)> {
        Some(((), 0))
    }

    #[inline(always)]
    fn parse_as_gfa<'a>(
        _ident: &[u8],
        _extra_buffer: &mut Self::TempBuffer,
    ) -> Option<(Self, usize)> {
        Some(((), 0))
    }

    #[inline(always)]
    fn write_as_ident(
        &self,
        _partial_data: &mut Self::PartialConnectionData,
        _write_range: (usize, Option<usize>),
        _reverse_complement: bool,
        _stream: &mut impl Write,
        _extra_buffer: &Self::TempBuffer,
    ) {
    }

    #[inline(always)]
    fn flush_partial_as_ident(
        _partial_data: Self::PartialConnectionData,
        _stream: &mut impl Write,
    ) {
    }

    #[inline(always)]
    fn write_as_gfa<const VERSION: u32>(
        &self,
        _k: u64,
        _index: u64,
        _length: u64,
        _partial_data: &mut Self::PartialConnectionData,
        _write_range: (usize, Option<usize>),
        _reverse_complement: bool,
        _stream: &mut impl Write,
        _extra_buffer: &Self::TempBuffer,
    ) {
    }

    #[inline(always)]
    fn flush_partial_as_gfa<const VERSION: u32>(
        _k: u64,
        _index: u64,
        _length: u64,
        _partial_data: Self::PartialConnectionData,
        _stream: &mut impl Write,
    ) {
    }
}
