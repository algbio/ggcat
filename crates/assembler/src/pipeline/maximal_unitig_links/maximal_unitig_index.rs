use byteorder::ReadBytesExt;
use io::concurrent::structured_sequences::IdentSequenceWriter;
use io::concurrent::temp_reads::extra_data::{
    HasEmptyExtraBuffer, SequenceExtraData, SequenceExtraDataTempBufferManagement,
};
use io::varint::{decode_varint, encode_varint, VARINT_MAX_SIZE};
use parallel_processor::buckets::bucket_writer::BucketItemSerializer;
use std::fmt;
use std::fmt::{Debug, Formatter};
use std::hash::{Hash, Hasher};
use std::io::{Read, Write};
use utils::vec_slice::VecSlice;

#[repr(transparent)]
#[derive(Copy, Clone, Eq, PartialEq)]
pub struct MaximalUnitigFlags(u8);

impl MaximalUnitigFlags {
    const FLIP_CURRENT: usize = 0;
    const FLIP_OTHER: usize = 1;

    #[inline(always)]
    fn get_bit(&self, pos: usize) -> bool {
        (self.0 & (1 << pos)) != 0
    }

    #[inline(always)]
    pub const fn new_direction(flip_current: bool, flip_other: bool) -> MaximalUnitigFlags {
        MaximalUnitigFlags(
            ((flip_current as u8) << Self::FLIP_CURRENT) | ((flip_other as u8) << Self::FLIP_OTHER),
        )
    }

    pub fn flip_other(&self) -> bool {
        self.get_bit(Self::FLIP_OTHER)
    }

    #[inline(always)]
    pub fn flip_current(&self) -> bool {
        self.get_bit(Self::FLIP_CURRENT)
    }
}

#[derive(Copy, Clone, Eq)]
pub struct MaximalUnitigIndex {
    index: u64,
    // The non reverse-complemented start of the link overlap (used for gfa2)
    overlap_start: u64,
    pub flags: MaximalUnitigFlags,
}

impl Hash for MaximalUnitigIndex {
    fn hash<H: Hasher>(&self, state: &mut H) {
        state.write_u64(self.index());
    }
}

impl PartialEq for MaximalUnitigIndex {
    fn eq(&self, other: &Self) -> bool {
        other.index() == self.index()
    }
}

impl HasEmptyExtraBuffer for MaximalUnitigIndex {}
impl SequenceExtraData for MaximalUnitigIndex {
    fn decode_extended(_: &mut (), reader: &mut impl Read) -> Option<Self> {
        let index = decode_varint(|| reader.read_u8().ok())?;
        let overlap_start = decode_varint(|| reader.read_u8().ok())?;
        let flags = reader.read_u8().ok()?;
        Some(MaximalUnitigIndex::new(
            index,
            overlap_start,
            MaximalUnitigFlags(flags),
        ))
    }

    fn encode_extended(&self, _: &(), writer: &mut impl Write) {
        encode_varint(|b| writer.write_all(b).ok(), self.index() as u64).unwrap();
        encode_varint(|b| writer.write_all(b).ok(), self.overlap_start).unwrap();
        writer.write_all(&[self.flags.0]).unwrap();
    }

    fn max_size(&self) -> usize {
        VARINT_MAX_SIZE * 2
    }
}

impl Debug for MaximalUnitigIndex {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_fmt(format_args!(
            "MaximalUnitigIndex {{ index: {} }}",
            self.index()
        ))
    }
}

impl MaximalUnitigIndex {
    #[inline]
    pub fn new(index: u64, overlap_start: u64, flags: MaximalUnitigFlags) -> Self {
        Self {
            index,
            overlap_start,
            flags,
        }
    }

    #[inline]
    pub fn index(&self) -> u64 {
        self.index
    }
}

#[derive(Clone, Debug)]
pub struct MaximalUnitigLink {
    index: u64,
    pub entries: VecSlice<MaximalUnitigIndex>,
}

impl MaximalUnitigLink {
    pub const fn new(index: u64, entries: VecSlice<MaximalUnitigIndex>) -> Self {
        Self { index, entries }
    }

    pub fn index(&self) -> u64 {
        self.index
    }
}

pub struct MaximalUnitigLinkSerializer;

impl BucketItemSerializer for MaximalUnitigLinkSerializer {
    type InputElementType<'a> = MaximalUnitigLink;
    type ExtraData = Vec<MaximalUnitigIndex>;
    type ReadBuffer = Vec<MaximalUnitigIndex>;
    type ExtraDataBuffer = ();
    type ReadType<'a> = MaximalUnitigLink;

    type CheckpointData = ();

    #[inline(always)]
    fn new() -> Self {
        Self
    }

    #[inline(always)]
    fn reset(&mut self) {}

    #[inline(always)]
    fn write_to(
        &mut self,
        element: &Self::InputElementType<'_>,
        bucket: &mut Vec<u8>,
        extra_data: &Self::ExtraData,
        _: &(),
    ) {
        encode_varint(|b| bucket.write_all(b), element.index()).unwrap();

        let entries = element.entries.get_slice(extra_data);
        encode_varint(|b| bucket.write_all(b), entries.len() as u64).unwrap();

        for entry in entries {
            encode_varint(|b| bucket.write_all(b), entry.index() as u64).unwrap();
            encode_varint(|b| bucket.write_all(b), entry.overlap_start).unwrap();
            bucket.push(entry.flags.0);
        }
    }

    fn read_from<'a, S: Read>(
        &mut self,
        mut stream: S,
        read_buffer: &'a mut Self::ReadBuffer,
        _: &mut (),
    ) -> Option<Self::ReadType<'a>> {
        let entry = decode_varint(|| stream.read_u8().ok())?;

        let len = decode_varint(|| stream.read_u8().ok())? as usize;

        let start = read_buffer.len();
        for _i in 0..len {
            let index = decode_varint(|| stream.read_u8().ok())?;
            let overlap_start = decode_varint(|| stream.read_u8().ok())?;
            let flags = stream.read_u8().ok()?;
            read_buffer.push(MaximalUnitigIndex::new(
                index,
                overlap_start,
                MaximalUnitigFlags(flags),
            ));
        }

        Some(MaximalUnitigLink::new(entry, VecSlice::new(start, len)))
    }

    fn get_size(&self, element: &Self::InputElementType<'_>, _: &Vec<MaximalUnitigIndex>) -> usize {
        16 + element.entries.len() * (VARINT_MAX_SIZE + 1)
    }
}

#[derive(Clone, Debug)]
pub struct DoubleMaximalUnitigLinks {
    pub links: [MaximalUnitigLink; 2],
    pub is_self_complemental: bool,
}

impl DoubleMaximalUnitigLinks {
    pub const EMPTY: Self = Self {
        links: [
            MaximalUnitigLink::new(0, VecSlice::new(0, 0)),
            MaximalUnitigLink::new(0, VecSlice::new(0, 0)),
        ],
        is_self_complemental: false,
    };
}

impl SequenceExtraDataTempBufferManagement for DoubleMaximalUnitigLinks {
    type TempBuffer = Vec<MaximalUnitigIndex>;

    fn new_temp_buffer() -> Vec<MaximalUnitigIndex> {
        vec![]
    }

    fn clear_temp_buffer(buffer: &mut Vec<MaximalUnitigIndex>) {
        buffer.clear()
    }

    fn copy_temp_buffer(dest: &mut Vec<MaximalUnitigIndex>, src: &Vec<MaximalUnitigIndex>) {
        dest.clear();
        dest.extend_from_slice(&src);
    }

    fn copy_extra_from(
        extra: Self,
        src: &Vec<MaximalUnitigIndex>,
        dst: &mut Vec<MaximalUnitigIndex>,
    ) -> Self {
        Self {
            links: [
                {
                    let entries = extra.links[0].entries.get_slice(src);
                    let start = dst.len();
                    dst.extend_from_slice(entries);
                    MaximalUnitigLink::new(
                        extra.links[0].index(),
                        VecSlice::new(start, entries.len()),
                    )
                },
                {
                    let entries = extra.links[1].entries.get_slice(src);
                    let start = dst.len();
                    dst.extend_from_slice(entries);
                    MaximalUnitigLink::new(
                        extra.links[1].index(),
                        VecSlice::new(start, entries.len()),
                    )
                },
            ],
            is_self_complemental: extra.is_self_complemental,
        }
    }
}

impl SequenceExtraData for DoubleMaximalUnitigLinks {
    fn decode_extended(_buffer: &mut Self::TempBuffer, _reader: &mut impl Read) -> Option<Self> {
        unimplemented!()
    }

    fn encode_extended(&self, _buffer: &Self::TempBuffer, _writer: &mut impl Write) {
        unimplemented!()
    }

    fn max_size(&self) -> usize {
        unimplemented!()
    }
}

impl IdentSequenceWriter for DoubleMaximalUnitigLinks {
    fn write_as_ident(&self, stream: &mut impl Write, extra_buffer: &Self::TempBuffer) {
        for entries in &self.links {
            let entries = entries.entries.get_slice(extra_buffer);
            for entry in entries {
                write!(
                    stream,
                    " L:{}:{}:{}",
                    if entry.flags.flip_current() { "-" } else { "+" },
                    entry.index,
                    if entry.flags.flip_other() { "-" } else { "+" },
                )
                .unwrap();
            }
        }
    }

    #[allow(unused_variables)]
    fn write_as_gfa<const VERSION: u32>(
        &self,
        k: u64,
        index: u64,
        length: u64,
        stream: &mut impl Write,
        extra_buffer: &Self::TempBuffer,
    ) {
        for entries in &self.links {
            let entries = entries.entries.get_slice(extra_buffer);
            if VERSION == 1 {
                for entry in entries {
                    // L <index> < +/- > <other_index> < +/- > <overlap>
                    writeln!(
                        stream,
                        "L\t{}\t{}\t{}\t{}\t{}M",
                        index,
                        if entry.flags.flip_current() { '-' } else { '+' },
                        entry.index,
                        if entry.flags.flip_other() { '-' } else { '+' },
                        k - 1
                    )
                    .unwrap();
                }
            } else if VERSION == 2 {
                for entry in entries {
                    let (b1, e1, is_end1) = if entry.flags.flip_current() {
                        (0, k - 1, false)
                    } else {
                        (length - k + 1, length, true)
                    };

                    let (b2, e2, is_end2) = (
                        entry.overlap_start,
                        entry.overlap_start + k - 1,
                        entry.overlap_start > 0,
                    );

                    // E * <index>< +/- > <other_index>< +/- > <b1> <e1> <b2> <e2>
                    writeln!(
                        stream,
                        "E\t*\t{}{}\t{}{}\t{}\t{}{}\t{}\t{}{}",
                        index,
                        if entry.flags.flip_current() { '-' } else { '+' },
                        entry.index,
                        if entry.flags.flip_other() { '-' } else { '+' },
                        b1,
                        e1,
                        if is_end1 { "$" } else { "" },
                        b2,
                        e2,
                        if is_end2 { "$" } else { "" }
                    )
                    .unwrap();
                }
            }
        }
    }

    fn parse_as_ident<'a>(_ident: &[u8], _extra_buffer: &mut Self::TempBuffer) -> Option<Self> {
        unimplemented!()
    }

    fn parse_as_gfa<'a>(_ident: &[u8], _extra_buffer: &mut Self::TempBuffer) -> Option<Self> {
        unimplemented!()
    }
}
