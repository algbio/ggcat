use crate::concurrent::temp_reads::extra_data::{HasEmptyExtraBuffer, SequenceExtraData};
use crate::varint::{decode_varint, encode_varint, VARINT_MAX_SIZE};
use byteorder::ReadBytesExt;
use config::BucketIndexType;
use parallel_processor::buckets::bucket_writer::BucketItemSerializer;
use std::fmt;
use std::fmt::{Debug, Formatter};
use std::hash::{Hash, Hasher};
use std::io::{Read, Write};
use utils::vec_slice::VecSlice;

#[repr(transparent)]
#[derive(Copy, Clone)]
pub struct UnitigFlags(u8);

impl Debug for UnitigFlags {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str("UnitigFlags(")?;
        if self.is_forward() {
            f.write_str("Forward")?;
        } else {
            f.write_str("Backward")?;
        }

        if self.begin_sealed() {
            f.write_str(", BeginSealed")?;
        }

        if self.end_sealed() {
            f.write_str(", EndSealed")?;
        }

        if self.is_reverse_complemented() {
            f.write_str(", ReverseComplemented")?;
        }

        f.write_str(")")
    }
}

impl UnitigFlags {
    const DIRECTION_FLAG: usize = 0;
    const BEGIN_SEALED_FLAG: usize = 1;
    const END_SEALED_FLAG: usize = 2;
    const REVERSE_COMPLEMENT_FLAG: usize = 3;

    pub fn combine(a: UnitigFlags, b: UnitigFlags) -> UnitigFlags {
        // assert_ne!(a.is_forward(), b.is_forward());

        UnitigFlags(
            ((a.is_forward() as u8) << Self::DIRECTION_FLAG)
                | ((a.end_sealed() as u8) << Self::END_SEALED_FLAG)
                | ((b.end_sealed() as u8) << Self::BEGIN_SEALED_FLAG)
                | ((b.is_reverse_complemented() as u8) << Self::REVERSE_COMPLEMENT_FLAG),
        )
    }

    pub fn reverse_complement(&self) -> UnitigFlags {
        UnitigFlags(self.0 ^ (1 << Self::REVERSE_COMPLEMENT_FLAG))
    }

    pub fn flipped(&self) -> UnitigFlags {
        UnitigFlags(
            ((!self.is_forward() as u8) << Self::DIRECTION_FLAG)
                | ((self.begin_sealed() as u8) << Self::END_SEALED_FLAG)
                | ((self.end_sealed() as u8) << Self::BEGIN_SEALED_FLAG)
                | ((self.is_reverse_complemented() as u8) << Self::REVERSE_COMPLEMENT_FLAG),
        )
    }

    #[inline(always)]
    fn set_bit(&mut self, pos: usize) {
        self.0 |= 1 << pos;
    }

    #[inline(always)]
    fn clr_bit(&mut self, pos: usize) {
        self.0 &= !(1 << pos);
    }

    #[inline(always)]
    fn get_bit(&self, pos: usize) -> bool {
        (self.0 & (1 << pos)) != 0
    }

    #[inline(always)]
    pub const fn new_empty() -> UnitigFlags {
        UnitigFlags(0)
    }

    #[inline(always)]
    pub const fn new_direction(forward: bool, complement: bool) -> UnitigFlags {
        UnitigFlags(
            ((forward as u8) << Self::DIRECTION_FLAG)
                | ((complement as u8) << Self::REVERSE_COMPLEMENT_FLAG),
        )
    }

    #[inline(always)]
    pub fn set_forward(&mut self, value: bool) {
        if value {
            self.set_bit(Self::DIRECTION_FLAG);
        } else {
            self.clr_bit(Self::DIRECTION_FLAG);
        }
    }

    #[inline(always)]
    pub fn set_reverse_complement(&mut self, value: bool) {
        if value {
            self.set_bit(Self::REVERSE_COMPLEMENT_FLAG);
        } else {
            self.clr_bit(Self::REVERSE_COMPLEMENT_FLAG);
        }
    }

    pub fn is_reverse_complemented(&self) -> bool {
        self.get_bit(Self::REVERSE_COMPLEMENT_FLAG)
    }

    #[inline(always)]
    pub fn is_forward(&self) -> bool {
        self.get_bit(Self::DIRECTION_FLAG)
    }

    #[inline(always)]
    pub fn seal_beginning(&mut self) {
        self.set_bit(Self::BEGIN_SEALED_FLAG)
    }

    #[inline(always)]
    pub fn begin_sealed(&self) -> bool {
        self.get_bit(Self::BEGIN_SEALED_FLAG)
    }

    // #[inline(always)]
    // pub fn seal_ending(&mut self) {
    //     self.set_bit(Self::END_SEALED_FLAG)
    // }

    #[inline(always)]
    pub fn end_sealed(&self) -> bool {
        self.get_bit(Self::END_SEALED_FLAG)
    }
}

#[derive(Copy, Clone, Eq)]
pub struct UnitigIndex {
    index: usize,
}

impl Hash for UnitigIndex {
    fn hash<H: Hasher>(&self, state: &mut H) {
        state.write_u16(self.bucket());
        state.write_usize(self.index());
    }
}

impl PartialEq for UnitigIndex {
    fn eq(&self, other: &Self) -> bool {
        (other.bucket() == self.bucket()) && (other.index() == self.index())
    }
}

impl HasEmptyExtraBuffer for UnitigIndex {}
impl SequenceExtraData for UnitigIndex {
    fn decode_extended(_: &mut (), reader: &mut impl Read) -> Option<Self> {
        let bucket = decode_varint(|| reader.read_u8().ok())? as BucketIndexType;
        let index = decode_varint(|| reader.read_u8().ok())?;
        Some(UnitigIndex::new_raw(bucket, index as usize))
    }

    fn encode_extended(&self, _: &(), writer: &mut impl Write) {
        encode_varint(
            |b| writer.write_all(b).ok(),
            self.raw_bucket_revcomplemented() as u64,
        );
        encode_varint(|b| writer.write_all(b).ok(), self.index() as u64);
    }

    fn max_size(&self) -> usize {
        VARINT_MAX_SIZE * 2
    }
}

impl Debug for UnitigIndex {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!(
            "UnitigIndex {{ bucket: {}, index: {} }}",
            self.bucket(),
            self.index()
        ))
    }
}

impl UnitigIndex {
    const INDEX_MASK: usize = (1 << 48) - 1;

    #[inline]
    pub fn new(bucket: BucketIndexType, index: usize, complemented: bool) -> Self {
        Self {
            index: (((bucket as usize) << 1 | (complemented as usize)) << 48)
                | (index & Self::INDEX_MASK),
        }
    }

    #[inline]
    pub fn new_raw(bucket_revcomplement: BucketIndexType, index: usize) -> Self {
        Self {
            index: ((bucket_revcomplement as usize) << 48) | (index & Self::INDEX_MASK),
        }
    }

    #[inline]
    pub fn raw_bucket_revcomplemented(&self) -> BucketIndexType {
        (self.index >> 48) as BucketIndexType
    }

    #[inline]
    pub fn bucket(&self) -> BucketIndexType {
        (self.index >> (48 + 1)) as BucketIndexType
    }

    pub fn is_reverse_complemented(&self) -> bool {
        (self.index >> 48) & 0x1 == 1
    }

    pub fn change_reverse_complemented(&mut self) {
        self.index ^= 0x1 << 48;
    }

    #[inline]
    pub fn index(&self) -> usize {
        self.index & Self::INDEX_MASK
    }
}

#[derive(Clone, Debug)]
pub struct UnitigLink {
    encoded: u64,
    pub entries: VecSlice<UnitigIndex>,
}

impl UnitigLink {
    const ENTRY_OFFSET: usize = 8;

    pub fn new(entry: u64, flags: UnitigFlags, entries: VecSlice<UnitigIndex>) -> Self {
        Self {
            encoded: (entry << Self::ENTRY_OFFSET) | (flags.0 as u64),
            entries,
        }
    }

    pub fn entry(&self) -> u64 {
        self.encoded >> Self::ENTRY_OFFSET
    }

    pub fn flags(&self) -> UnitigFlags {
        UnitigFlags(self.encoded as u8)
    }

    pub fn change_flags(&mut self, change_fn: impl FnOnce(&mut UnitigFlags)) {
        let mut flags = self.flags();
        change_fn(&mut flags);
        self.encoded = (self.encoded & !(u8::MAX as u64)) | (flags.0 as u64);
    }
}

pub struct UnitigLinkSerializer;

impl BucketItemSerializer for UnitigLinkSerializer {
    type InputElementType<'a> = UnitigLink;
    type ExtraData = Vec<UnitigIndex>;
    type ReadBuffer = Vec<UnitigIndex>;
    type ExtraDataBuffer = ();
    type ReadType<'a> = UnitigLink;

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
        element: &UnitigLink,
        bucket: &mut Vec<u8>,
        extra_data: &Self::ExtraData,
        _: &(),
    ) {
        encode_varint(|b| bucket.write_all(b), element.entry()).unwrap();
        bucket.write_all(&[element.flags().0]).unwrap();

        let entries = element.entries.get_slice(extra_data);
        encode_varint(|b| bucket.write_all(b), entries.len() as u64).unwrap();

        for entry in entries {
            encode_varint(
                |b| bucket.write_all(b),
                entry.raw_bucket_revcomplemented() as u64,
            )
            .unwrap();
            encode_varint(|b| bucket.write_all(b), entry.index() as u64).unwrap();
        }
    }

    fn read_from<'a, S: Read>(
        &mut self,
        mut stream: S,
        read_buffer: &'a mut Self::ReadBuffer,
        _: &mut (),
    ) -> Option<Self::ReadType<'a>> {
        let entry = decode_varint(|| stream.read_u8().ok())?;
        let flags = stream.read_u8().ok()?;

        let len = decode_varint(|| stream.read_u8().ok())? as usize;

        let start = read_buffer.len();
        for _i in 0..len {
            let bucket = decode_varint(|| stream.read_u8().ok())? as BucketIndexType;
            let index = decode_varint(|| stream.read_u8().ok())?;
            read_buffer.push(UnitigIndex::new_raw(bucket, index as usize));
        }

        Some(UnitigLink::new(
            entry,
            UnitigFlags(flags),
            VecSlice::new(start, len),
        ))
    }

    fn get_size(&self, element: &UnitigLink, _: &Vec<UnitigIndex>) -> usize {
        16 + element.entries.len() * VARINT_MAX_SIZE * 2
    }
}
