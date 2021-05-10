use std::io::{Read, Write};
use std::marker::PhantomData;

use byteorder::{ByteOrder, LittleEndian, ReadBytesExt, WriteBytesExt};
use serde::ser::SerializeTuple;
use serde::{Deserialize, Serialize, Serializer};

use crate::binary_writer::BinaryWriter;
use crate::hash_entry::Direction;
use crate::intermediate_storage::{SequenceExtraData, VecReader};
use crate::multi_thread_buckets::BucketWriter;
use crate::varint::{decode_varint, encode_varint};
use crate::vec_slice::VecSlice;
use std::fmt::{Debug, Formatter};

#[repr(transparent)]
#[derive(Copy, Clone, Debug)]
pub struct UnitigFlags(u8);

impl UnitigFlags {
    const DIRECTION_FLAG: usize = 0;
    const BEGIN_SEALED_FLAG: usize = 1;
    const END_SEALED_FLAG: usize = 2;

    pub fn combine(a: UnitigFlags, b: UnitigFlags) -> UnitigFlags {
        assert_ne!(a.is_forward(), b.is_forward());

        UnitigFlags(
            ((a.is_forward() as u8) << Self::DIRECTION_FLAG)
                | ((a.end_sealed() as u8) << Self::END_SEALED_FLAG)
                | ((b.end_sealed() as u8) << Self::BEGIN_SEALED_FLAG),
        )
    }

    pub fn reversed(&self) -> UnitigFlags {
        UnitigFlags(
            ((!self.is_forward() as u8) << Self::DIRECTION_FLAG)
                | ((self.begin_sealed() as u8) << Self::END_SEALED_FLAG)
                | ((self.end_sealed() as u8) << Self::BEGIN_SEALED_FLAG),
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
    pub const fn new_direction(forward: bool) -> UnitigFlags {
        UnitigFlags(((forward as u8) << Self::DIRECTION_FLAG))
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

#[derive(Copy, Clone, Eq, PartialEq, Hash)]
pub struct UnitigIndex {
    index: usize,
}

impl SequenceExtraData for UnitigIndex {
    fn decode(mut reader: impl Read) -> Option<Self> {
        let bucket = decode_varint(|| reader.read_u8().ok())?;
        let index = decode_varint(|| reader.read_u8().ok())?;
        Some(UnitigIndex::new(bucket as usize, index as usize))
    }

    fn encode(&self, mut writer: impl Write) {
        encode_varint(|b| writer.write(b).ok(), self.bucket() as u64);
        encode_varint(|b| writer.write(b).ok(), self.index() as u64);
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
    pub fn new(bucket: usize, index: usize) -> Self {
        Self {
            index: (bucket << 48) | (index & Self::INDEX_MASK),
        }
    }
    #[inline]
    pub fn bucket(&self) -> usize {
        self.index >> 48
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
            let bucket = decode_varint(|| reader.read_u8().ok())?;
            let index = decode_varint(|| reader.read_u8().ok())?;
            out_vec.push(UnitigIndex::new(bucket as usize, index as usize));
        }

        Some(Self {
            entry,
            flags: UnitigFlags(flags),
            entries: VecSlice::new(start, len),
        })
    }
}

impl BucketWriter for UnitigLink {
    type BucketType = BinaryWriter;
    type ExtraData = Vec<UnitigIndex>;

    #[inline(always)]
    fn write_to(&self, bucket: &mut Self::BucketType, extra_data: &Self::ExtraData) {
        let writer = bucket.get_writer();
        encode_varint(|b| writer.write(b), self.entry);
        writer.write(&[self.flags.0]);

        let entries = self.entries.get_slice(extra_data);
        encode_varint(|b| writer.write(b), entries.len() as u64);

        for entry in entries {
            encode_varint(|b| writer.write(b), entry.bucket() as u64);
            encode_varint(|b| writer.write(b), entry.index() as u64);
        }
    }
}
