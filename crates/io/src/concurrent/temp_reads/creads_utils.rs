use crate::compressed_read::CompressedRead;
use crate::varint::{VARINT_FLAGS_MAX_SIZE, VARINT_MAX_SIZE, decode_varint, encode_varint};
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
    PlainRc(&'a [u8]),
    #[allow(dead_code)]
    PackedRc(CompressedRead<'a>),
}

pub struct CompressedReadsBucketData<'a> {
    read: ReadData<'a>,
    multiplicity: MultiplicityCounterType,
    minimizer_pos: u16,
    extra_bucket: u8,
    flags: u8,
    is_window_duplicate: bool,
}

impl<'a> CompressedReadsBucketData<'a> {
    #[inline(always)]
    pub fn new(
        read: &'a [u8],
        flags: u8,
        extra_bucket: u8,
        minimizer_pos: u16,
        is_window_duplicate: bool,
    ) -> Self {
        Self {
            read: ReadData::Plain(read),
            extra_bucket,
            multiplicity: 1,
            minimizer_pos,
            flags,
            is_window_duplicate,
        }
    }

    #[inline(always)]
    pub fn new_plain_opt_rc(
        read: &'a [u8],
        flags: u8,
        extra_bucket: u8,
        rc: bool,
        minimizer_pos: u16,
        is_window_duplicate: bool,
    ) -> Self {
        Self {
            read: if rc {
                ReadData::PlainRc(read)
            } else {
                ReadData::Plain(read)
            },
            extra_bucket,
            flags,
            multiplicity: 1,
            minimizer_pos,
            is_window_duplicate,
        }
    }

    #[inline(always)]
    pub fn new_with_multiplicity(
        read: &'a [u8],
        flags: u8,
        extra_bucket: u8,
        multiplicity: MultiplicityCounterType,
        minimizer_pos: u16,
        is_window_duplicate: bool,
    ) -> Self {
        Self {
            read: ReadData::Plain(read),
            extra_bucket,
            flags,
            multiplicity,
            minimizer_pos,
            is_window_duplicate,
        }
    }

    #[inline(always)]
    pub fn new_packed(
        read: CompressedRead<'a>,
        flags: u8,
        extra_bucket: u8,
        minimizer_pos: u16,
        is_window_duplicate: bool,
    ) -> Self {
        Self {
            read: ReadData::Packed(read),
            flags,
            extra_bucket,
            multiplicity: 1,
            minimizer_pos,
            is_window_duplicate,
        }
    }

    #[inline(always)]
    pub fn new_packed_with_multiplicity(
        read: CompressedRead<'a>,
        flags: u8,
        extra_bucket: u8,
        multiplicity: MultiplicityCounterType,
        minimizer_pos: u16,
        is_window_duplicate: bool,
    ) -> Self {
        Self {
            read: ReadData::Packed(read),
            flags,
            extra_bucket,
            multiplicity,
            minimizer_pos,
            is_window_duplicate,
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

pub struct NoMinimizerPosition;
pub struct AssemblerMinimizerPosition;

pub trait MinimizerModeOption {
    const ENABLED: bool;
}
impl MinimizerModeOption for NoMinimizerPosition {
    const ENABLED: bool = false;
}
impl MinimizerModeOption for AssemblerMinimizerPosition {
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
    MinimizerMode: MinimizerModeOption,
> {
    min_size: usize,
    last_data: E::LastData,
    min_size_log: u8,
    _phantom: PhantomData<(FlagsCount, BucketMode, MultiplicityMode, MinimizerMode)>,
}

#[derive(Serialize, Deserialize, Clone, Copy)]
pub struct ReadsCheckpointData {
    pub target_subbucket: BucketIndexType,
    pub sequences_count: usize,
}

pub struct DeserializedRead<'a, E> {
    pub read: CompressedRead<'a>,
    pub extra: E,
    pub multiplicity: MultiplicityCounterType,
    pub flags: u8,
    pub second_bucket: u8,
    pub minimizer_pos: u16,
    pub is_window_duplicate: bool,
}

impl<
    'a,
    E: SequenceExtraDataConsecutiveCompression,
    FlagsCount: typenum::Unsigned,
    BucketMode: BucketModeOption,
    MultiplicityMode: MultiplicityModeOption,
    MinimizerMode: MinimizerModeOption,
> BucketItemSerializer
    for CompressedReadsBucketDataSerializer<
        E,
        FlagsCount,
        BucketMode,
        MultiplicityMode,
        MinimizerMode,
    >
{
    type InputElementType<'b> = CompressedReadsBucketData<'b>;
    type ExtraData = E;
    type ReadBuffer = Vec<u8>;
    type ExtraDataBuffer = E::TempBuffer;
    type ReadType<'b> = DeserializedRead<'b, E>;
    type InitData = usize;

    type CheckpointData = ReadsCheckpointData;

    #[inline(always)]
    fn new(min_size: Self::InitData) -> Self {
        Self {
            min_size,
            min_size_log: min_size.next_power_of_two().ilog2() as u8,
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
            ReadData::Plain(read) | ReadData::PlainRc(read) => {
                let is_rc = matches!(element.read, ReadData::PlainRc(_));
                CompressedRead::from_plain_write_directly_to_buffer_with_flags::<
                    MinimizerMode,
                    FlagsCount,
                >(
                    read,
                    bucket,
                    self.min_size,
                    element.minimizer_pos,
                    self.min_size_log,
                    element.flags,
                    is_rc,
                    element.is_window_duplicate,
                );
            }
            ReadData::Packed(read) | ReadData::PackedRc(read) => {
                let is_rc = matches!(element.read, ReadData::PackedRc(_));
                CompressedRead::encode_length::<MinimizerMode, FlagsCount>(
                    bucket,
                    read.size,
                    self.min_size,
                    element.minimizer_pos,
                    self.min_size_log,
                    element.flags,
                    element.is_window_duplicate,
                );

                if is_rc {
                    read.copy_to_buffer_rc(bucket);
                } else {
                    read.copy_to_buffer(bucket);
                }
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

        read_buffer.clear();
        let (read, minimizer_pos, flags, is_window_duplicate) =
            CompressedRead::read_from_stream::<_, MinimizerMode, FlagsCount>(
                read_buffer,
                &mut stream,
                self.min_size,
                self.min_size_log,
            )?;

        Some(DeserializedRead {
            read,
            extra,
            multiplicity: multiplicity as MultiplicityCounterType,
            flags,
            second_bucket,

            minimizer_pos,
            is_window_duplicate,
        })
    }

    #[inline(always)]
    fn get_size(&self, element: &Self::InputElementType<'_>, extra: &Self::ExtraData) -> usize {
        let bases_count = match element.read {
            ReadData::Plain(read) | ReadData::PlainRc(read) => read.len(),
            ReadData::Packed(read) | ReadData::PackedRc(read) => read.size,
        };

        ((bases_count + 3) / 4)
            + extra.max_size()
            + VARINT_FLAGS_MAX_SIZE
            + if BucketMode::ENABLED { 1 } else { 0 }
            + if MinimizerMode::ENABLED {
                VARINT_FLAGS_MAX_SIZE * 2
            } else {
                0
            }
            + if MultiplicityMode::ENABLED {
                VARINT_MAX_SIZE
            } else {
                0
            }
    }
}
pub mod helpers {

    // use crate::concurrent::temp_reads::extra_data::SequenceExtraDataConsecutiveCompression;

    use std::sync::Arc;

    use parallel_processor::{
        buckets::readers::async_binary_reader::{
            AllowedCheckpointStrategy, AsyncBinaryReader, AsyncBinaryReaderIteratorData,
            AsyncReaderThread,
        },
        memory_fs::file::reader::FileRangeReference,
        scheduler::ThreadPriorityHandle,
    };

    use crate::concurrent::temp_reads::{
        creads_utils::NoSecondBucket,
        extra_data::{SequenceExtraDataCombiner, SequenceExtraDataConsecutiveCompression},
    };

    use super::{
        BucketModeOption, CompressedReadsBucketDataSerializer, DeserializedRead,
        MinimizerModeOption, NoMultiplicity, ReadsCheckpointData, WithMultiplicity,
    };

    pub fn helper_read_bucket_with_opt_multiplicity<
        E: SequenceExtraDataConsecutiveCompression,
        EM: SequenceExtraDataConsecutiveCompression + SequenceExtraDataCombiner<SingleDataType = E>,
        FlagsCount: typenum::Unsigned,
        BucketMode: BucketModeOption,
        MinimizerMode: MinimizerModeOption,
    >(
        reader: &AsyncBinaryReader,
        read_thread: Arc<AsyncReaderThread>,
        with_multiplicity: bool,
        allowed_passtrough: AllowedCheckpointStrategy<ReadsCheckpointData>,
        mut passtrough_callback: impl FnMut(FileRangeReference),
        mut checkpoint_callback: impl FnMut(Option<ReadsCheckpointData>),
        mut data_callback: impl FnMut(DeserializedRead<EM>, &mut EM::TempBuffer),
        thread_handle: &ThreadPriorityHandle,
        k: usize,
    ) {
        if with_multiplicity {
            let mut items = reader.get_items_stream::<CompressedReadsBucketDataSerializer<
                E,
                FlagsCount,
                NoSecondBucket,
                WithMultiplicity,
                MinimizerMode,
            >>(
                read_thread,
                Vec::new(),
                <E>::new_temp_buffer(),
                allowed_passtrough,
                thread_handle,
                k,
            );
            let mut tmp_mult_buffer = EM::new_temp_buffer();

            while let Some(checkpoint) = items.get_next_checkpoint_extended() {
                match checkpoint {
                    AsyncBinaryReaderIteratorData::Stream(items, checkpoint_data) => {
                        checkpoint_callback(checkpoint_data);
                        while let Some((data, extra_buffer)) = items.next() {
                            let (extra, extra_buffer) = EM::from_single_entry(
                                &mut tmp_mult_buffer,
                                data.extra,
                                extra_buffer,
                            );
                            let data = DeserializedRead {
                                read: data.read,
                                extra,
                                multiplicity: data.multiplicity,
                                flags: data.flags,
                                second_bucket: data.second_bucket,
                                minimizer_pos: data.minimizer_pos,
                                is_window_duplicate: data.is_window_duplicate,
                            };
                            data_callback(data, extra_buffer);
                            EM::clear_temp_buffer(extra_buffer);
                        }
                    }
                    AsyncBinaryReaderIteratorData::Passtrough {
                        file_range: passtrough_info,
                        checkpoint_data,
                    } => {
                        checkpoint_callback(checkpoint_data);
                        passtrough_callback(passtrough_info)
                    }
                }
            }
        } else {
            let mut items = reader.get_items_stream::<CompressedReadsBucketDataSerializer<
                E,
                FlagsCount,
                BucketMode,
                NoMultiplicity,
                MinimizerMode,
            >>(
                read_thread,
                Vec::new(),
                <E>::new_temp_buffer(),
                allowed_passtrough,
                &thread_handle,
                k,
            );
            let mut tmp_mult_buffer = EM::new_temp_buffer();

            while let Some(checkpoint) = items.get_next_checkpoint_extended() {
                match checkpoint {
                    AsyncBinaryReaderIteratorData::Stream(items, checkpoint_data) => {
                        checkpoint_callback(checkpoint_data);
                        while let Some((data, extra_buffer)) = items.next() {
                            let (extra, extra_buffer) = EM::from_single_entry(
                                &mut tmp_mult_buffer,
                                data.extra,
                                extra_buffer,
                            );
                            let data = DeserializedRead {
                                read: data.read,
                                extra,
                                multiplicity: data.multiplicity,
                                flags: data.flags,
                                second_bucket: data.second_bucket,
                                minimizer_pos: data.minimizer_pos,
                                is_window_duplicate: data.is_window_duplicate,
                            };
                            data_callback(data, extra_buffer);
                            EM::clear_temp_buffer(extra_buffer);
                        }
                    }
                    AsyncBinaryReaderIteratorData::Passtrough {
                        file_range: passtrough_info,
                        checkpoint_data,
                    } => {
                        checkpoint_callback(checkpoint_data);
                        passtrough_callback(passtrough_info)
                    }
                }
            }
        }
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
