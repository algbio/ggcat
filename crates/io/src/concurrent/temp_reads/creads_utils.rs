use crate::compressed_read::CompressedRead;
use crate::varint::{
    decode_varint, decode_varint_flags, encode_varint, encode_varint_flags, VARINT_FLAGS_MAX_SIZE,
    VARINT_MAX_SIZE,
};
use byteorder::ReadBytesExt;
use config::{BucketIndexType, MultiplicityCounterType};
use parallel_processor::buckets::bucket_writer::BucketItemSerializer;
use serde::{Deserialize, Serialize};
use std::io::Read;
use std::marker::PhantomData;

use super::extra_data::SequenceExtraDataConsecutiveCompression;

enum ReadData<'a> {
    Plain(&'a [u8]),
    Packed(CompressedRead<'a>),
}

pub struct CompressedReadsBucketData<'a> {
    read: ReadData<'a>,
    multiplicity: MultiplicityCounterType,
    extra_bucket: u8,
    flags: u8,
}

impl<'a> CompressedReadsBucketData<'a> {
    #[inline(always)]
    pub fn new(read: &'a [u8], flags: u8, extra_bucket: u8) -> Self {
        Self {
            read: ReadData::Plain(read),
            extra_bucket,
            flags,
            multiplicity: 1,
        }
    }

    #[inline(always)]
    pub fn new_with_multiplicity(
        read: &'a [u8],
        flags: u8,
        extra_bucket: u8,
        multiplicity: MultiplicityCounterType,
    ) -> Self {
        Self {
            read: ReadData::Plain(read),
            extra_bucket,
            flags,
            multiplicity,
        }
    }

    #[inline(always)]
    pub fn new_packed(read: CompressedRead<'a>, flags: u8, extra_bucket: u8) -> Self {
        Self {
            read: ReadData::Packed(read),
            flags,
            extra_bucket,
            multiplicity: 1,
        }
    }

    #[inline(always)]
    pub fn new_packed_with_multiplicity(
        read: CompressedRead<'a>,
        flags: u8,
        extra_bucket: u8,
        multiplicity: MultiplicityCounterType,
    ) -> Self {
        Self {
            read: ReadData::Packed(read),
            flags,
            extra_bucket,
            multiplicity,
        }
    }
}

pub struct NoSecondBucket;
pub struct WithSecondBucket;

pub trait BucketModeOption {
    const ENABLED: bool;
}
impl BucketModeOption for NoSecondBucket {
    const ENABLED: bool = false;
}
impl BucketModeOption for WithSecondBucket {
    const ENABLED: bool = true;
}

pub struct BucketModeFromBoolean<const ENABLED: bool>;
impl<const ENABLED: bool> BucketModeOption for BucketModeFromBoolean<ENABLED> {
    const ENABLED: bool = ENABLED;
}

pub struct NoMultiplicity;
pub struct WithMultiplicity;

pub trait MultiplicityModeOption {
    const ENABLED: bool;
}
impl MultiplicityModeOption for NoMultiplicity {
    const ENABLED: bool = false;
}
impl MultiplicityModeOption for WithMultiplicity {
    const ENABLED: bool = true;
}

pub struct MultiplicityModeFromBoolean<const ENABLED: bool>;
impl<const ENABLED: bool> MultiplicityModeOption for MultiplicityModeFromBoolean<ENABLED> {
    const ENABLED: bool = ENABLED;
}

pub struct CompressedReadsBucketDataSerializer<
    E: SequenceExtraDataConsecutiveCompression,
    FlagsCount: typenum::Unsigned,
    BucketMode: BucketModeOption,
    MultiplicityMode: MultiplicityModeOption,
> {
    last_data: E::LastData,
    _phantom: PhantomData<(FlagsCount, BucketMode, MultiplicityMode)>,
}

#[derive(Serialize, Deserialize)]
pub struct ReadsCheckpointData {
    pub target_subbucket: BucketIndexType,
}

impl<
        'a,
        E: SequenceExtraDataConsecutiveCompression,
        FlagsCount: typenum::Unsigned,
        BucketMode: BucketModeOption,
        MultiplicityMode: MultiplicityModeOption,
    > BucketItemSerializer
    for CompressedReadsBucketDataSerializer<E, FlagsCount, BucketMode, MultiplicityMode>
{
    type InputElementType<'b> = CompressedReadsBucketData<'b>;
    type ExtraData = E;
    type ReadBuffer = Vec<u8>;
    type ExtraDataBuffer = E::TempBuffer;
    type ReadType<'b> = (u8, u8, E, CompressedRead<'b>, MultiplicityCounterType);

    type CheckpointData = ReadsCheckpointData;

    #[inline(always)]
    fn new() -> Self {
        Self {
            last_data: Default::default(),
            _phantom: PhantomData,
        }
    }

    #[inline(always)]
    fn reset(&mut self) {
        self.last_data = Default::default();
    }

    #[inline(always)]
    fn write_to(
        &mut self,
        element: &Self::InputElementType<'_>,
        bucket: &mut Vec<u8>,
        extra_data: &Self::ExtraData,
        extra_data_buffer: &Self::ExtraDataBuffer,
    ) {
        if BucketMode::ENABLED {
            bucket.push(element.extra_bucket);
        }

        if MultiplicityMode::ENABLED {
            encode_varint(|b| bucket.extend_from_slice(b), element.multiplicity as u64);
        }

        extra_data.encode_extended(extra_data_buffer, bucket, self.last_data);
        self.last_data = extra_data.obtain_last_data(self.last_data);

        match element.read {
            ReadData::Plain(read) => {
                CompressedRead::from_plain_write_directly_to_buffer_with_flags::<FlagsCount>(
                    read,
                    bucket,
                    element.flags,
                );
            }
            ReadData::Packed(read) => {
                encode_varint_flags::<_, _, FlagsCount>(
                    |b| bucket.extend_from_slice(b),
                    read.size as u64,
                    element.flags,
                );
                read.copy_to_buffer(bucket);
            }
        }
    }

    #[inline]
    fn read_from<'b, S: Read>(
        &mut self,
        mut stream: S,
        read_buffer: &'b mut Self::ReadBuffer,
        extra_read_buffer: &mut Self::ExtraDataBuffer,
    ) -> Option<Self::ReadType<'b>> {
        let second_bucket = if BucketMode::ENABLED {
            stream.read_u8().ok()?
        } else {
            0
        };

        let multiplicity = if MultiplicityMode::ENABLED {
            decode_varint(|| stream.read_u8().ok())?
        } else {
            1
        };

        let extra = E::decode_extended(extra_read_buffer, &mut stream, self.last_data)?;
        self.last_data = extra.obtain_last_data(self.last_data);

        let (size, flags) = decode_varint_flags::<_, FlagsCount>(|| stream.read_u8().ok())?;

        if size == 0 {
            return None;
        }

        read_buffer.clear();

        let bytes = ((size + 3) / 4) as usize;
        read_buffer.reserve(bytes);
        let buffer_start = read_buffer.len();
        unsafe {
            read_buffer.set_len(buffer_start + bytes);
        }

        stream.read_exact(&mut read_buffer[buffer_start..]).unwrap();

        Some((
            flags,
            second_bucket,
            extra,
            CompressedRead::new_from_compressed(&read_buffer[buffer_start..], size as usize),
            multiplicity as MultiplicityCounterType,
        ))
    }

    #[inline(always)]
    fn get_size(&self, element: &Self::InputElementType<'_>, extra: &Self::ExtraData) -> usize {
        let bases_count = match element.read {
            ReadData::Plain(read) => read.len(),
            ReadData::Packed(read) => read.size,
        };

        ((bases_count + 3) / 4)
            + extra.max_size()
            + VARINT_FLAGS_MAX_SIZE
            + if BucketMode::ENABLED { 1 } else { 0 }
            + if MultiplicityMode::ENABLED {
                VARINT_MAX_SIZE
            } else {
                0
            }
    }
}
pub mod helpers {

    // use crate::concurrent::temp_reads::extra_data::SequenceExtraDataConsecutiveCompression;

    #[macro_export]
    macro_rules! creads_helper {
        (
            helper_read_bucket_with_opt_multiplicity::<$E:ty, $FlagsCount:ty, $BucketMode:ty>(
                $reader:expr,
                $read_thread:expr,
                $with_multiplicity:expr,
                |$data: ident, $extra_buffer: ident| $f:expr
            );

        ) => {
            use $crate::concurrent::temp_reads::creads_utils::{
                BucketModeOption, CompressedReadsBucketDataSerializer, NoMultiplicity,
                WithMultiplicity,
            };

            let reader = $reader;
            let read_thread = $read_thread;
            let with_multiplicity = $with_multiplicity;

            if $with_multiplicity {
                let mut items =
                    reader.get_items_stream::<CompressedReadsBucketDataSerializer<
                        $E,
                        $FlagsCount,
                        $BucketMode,
                        WithMultiplicity,
                    >, false>(read_thread, Vec::new(), <$E>::new_temp_buffer());
                while let Some((items, _)) = items.get_next_checkpoint() {
                    while let Some(($data, $extra_buffer)) = items.next() {
                        $f
                    }
                }
            } else {
                let mut items =
                    reader.get_items_stream::<CompressedReadsBucketDataSerializer<
                        $E,
                        $FlagsCount,
                        $BucketMode,
                        NoMultiplicity,
                    >, false>(read_thread, Vec::new(), <$E>::new_temp_buffer());
                while let Some((items, _)) = items.get_next_checkpoint() {
                    while let Some(($data, $extra_buffer)) = items.next() {
                        $f
                    }
                }
            }
        };
    }

    // TODO: Restore this function when async closures are stable!
    // pub async fn helper_read_bucket_with_opt_multiplicity<
    //     E: SequenceExtraDataConsecutiveCompression,
    //     FlagsCount: typenum::Unsigned,
    //     BucketMode: BucketModeOption,
    //     F: Future<Output = ()>,
    // >(
    //     reader: &AsyncBinaryReader,
    //     read_thread: Arc<AsyncReaderThread>,
    //     with_multiplicity: bool,
    //     mut f: impl FnMut(
    //         (
    //             u8,
    //             u8,
    //             E,
    //             crate::compressed_read::CompressedRead,
    //             MultiplicityCounterType,
    //         ),
    //         &mut E::TempBuffer,
    //     ) -> F,
    // ) {

    // }
}
