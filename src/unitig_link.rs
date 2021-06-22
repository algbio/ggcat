use std::io::{Read, Write};
use std::marker::PhantomData;

use byteorder::{ByteOrder, LittleEndian, ReadBytesExt, WriteBytesExt};
use serde::ser::SerializeTuple;
use serde::{Deserialize, Serialize, Serializer};

use crate::hash_entry::Direction;
use crate::intermediate_storage::{SequenceExtraData, VecReader};
use crate::types::BucketIndexType;
use crate::varint::{decode_varint, encode_varint};
use crate::vec_slice::VecSlice;
use parallel_processor::binary_writer::BinaryWriter;
use parallel_processor::multi_thread_buckets::BucketWriter;
use std::fmt::{Debug, Formatter};
use std::hash::{Hash, Hasher};
use std::{fmt, io};

#[repr(transparent)]
#[derive(Copy, Clone)]
pub struct UnitigFlags(u8);

impl Debug for UnitigFlags {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str("UnitigFlags(");
        if self.is_forward() {
            f.write_str("Forward");
        } else {
            f.write_str("Backward");
        }

        if self.begin_sealed() {
            f.write_str(", BeginSealed");
        }

        if self.end_sealed() {
            f.write_str(", EndSealed");
        }

        if self.is_reverse_complemented() {
            f.write_str(", ReverseComplemented");
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
        self.0 |= (1 << pos);
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
    pub const fn new_backward() -> UnitigFlags {
        UnitigFlags(0)
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
        state.write_u32(self.bucket());
        state.write_usize(self.index());
    }
}

impl PartialEq for UnitigIndex {
    fn eq(&self, other: &Self) -> bool {
        (other.bucket() == self.bucket()) && (other.index() == self.index())
    }
}

impl SequenceExtraData for UnitigIndex {
    fn decode(mut reader: impl Read) -> Option<Self> {
        let bucket = decode_varint(|| reader.read_u8().ok())? as BucketIndexType;
        let index = decode_varint(|| reader.read_u8().ok())?;
        Some(UnitigIndex::new_raw(bucket, index as usize))
    }

    fn encode(&self, mut writer: impl Write) {
        encode_varint(
            |b| writer.write_all(b).ok(),
            self.raw_bucket_revcomplemented() as u64,
        );
        encode_varint(|b| writer.write_all(b).ok(), self.index() as u64);
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
        self.index ^= (0x1 << 48);
    }

    #[inline]
    pub fn index(&self) -> usize {
        self.index & Self::INDEX_MASK
    }
}

#[derive(Clone, Debug)]
pub struct UnitigLink {
    pub entry: u64,
    pub flags: UnitigFlags,
    pub entries: VecSlice<UnitigIndex>,
}

struct UnitigLinkSerializer {
    link: UnitigLink,
    indexes: Vec<UnitigIndex>,
}

#[derive(Copy, Clone)]
pub struct UnitigPointer {
    pub entry: u64,
    pub link_index: u64,
}

impl UnitigLink {
    pub fn read_from(mut reader: impl Read, out_vec: &mut Vec<UnitigIndex>) -> Option<Self> {
        let entry = decode_varint(|| reader.read_u8().ok())?;
        let flags = reader.read_u8().ok()?;

        let len = decode_varint(|| reader.read_u8().ok())? as usize;

        let start = out_vec.len();
        for _i in 0..len {
            let bucket = decode_varint(|| reader.read_u8().ok())? as BucketIndexType;
            let index = decode_varint(|| reader.read_u8().ok())?;
            out_vec.push(UnitigIndex::new_raw(bucket, index as usize));
        }

        Some(Self {
            entry,
            flags: UnitigFlags(flags),
            entries: VecSlice::new(start, len),
        })
    }
}

impl BucketWriter for UnitigLink {
    type ExtraData = Vec<UnitigIndex>;

    #[inline(always)]
    fn write_to(&self, mut bucket: impl Write, extra_data: &Self::ExtraData) {
        encode_varint(|b| bucket.write_all(b), self.entry);
        bucket.write_all(&[self.flags.0]);

        let entries = self.entries.get_slice(extra_data);
        encode_varint(|b| bucket.write_all(b), entries.len() as u64);

        for entry in entries {
            encode_varint(
                |b| bucket.write_all(b),
                entry.raw_bucket_revcomplemented() as u64,
            );
            encode_varint(|b| bucket.write_all(b), entry.index() as u64);
        }
    }

    fn get_size(&self) -> usize {
        16 + self.entries.len() * 8
    }
}
