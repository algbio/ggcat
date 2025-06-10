use crate::concurrent::structured_sequences::{IdentSequenceWriter, StructuredSequenceBackend};
use crate::concurrent::temp_reads::creads_utils::{
    CompressedReadsBucketData, CompressedReadsBucketDataSerializer, NoMinimizerPosition,
    NoMultiplicity, NoSecondBucket,
};
use crate::concurrent::temp_reads::extra_data::{
    SequenceExtraData, SequenceExtraDataConsecutiveCompression,
    SequenceExtraDataTempBufferManagement,
};
use crate::varint::{VARINT_MAX_SIZE, decode_varint, encode_varint};
use bincode::Encode;
use byteorder::ReadBytesExt;
use config::DEFAULT_PER_CPU_BUFFER_SIZE;
use parallel_processor::buckets::LockFreeBucket;
use parallel_processor::buckets::bucket_writer::BucketItemSerializer;
use parallel_processor::buckets::writers::compressed_binary_writer::{
    CompressedBinaryWriter, CompressedCheckpointSize, CompressionLevelInfo,
};
use parallel_processor::memory_fs::file::internal::MemoryFileMode;
use std::io::{Read, Write};
use std::marker::PhantomData;
use std::path::{Path, PathBuf};

use super::SequenceAbundanceType;

pub struct StructSeqBinaryWriter<
    ColorInfo: IdentSequenceWriter + SequenceExtraDataConsecutiveCompression,
    LinksInfo: IdentSequenceWriter + SequenceExtraData,
> {
    writer: CompressedBinaryWriter,
    _phantom: PhantomData<(ColorInfo, LinksInfo)>,
}

unsafe impl<
    ColorInfo: IdentSequenceWriter + SequenceExtraDataConsecutiveCompression,
    LinksInfo: IdentSequenceWriter + SequenceExtraData,
> Send for StructSeqBinaryWriter<ColorInfo, LinksInfo>
{
}

unsafe impl<
    ColorInfo: IdentSequenceWriter + SequenceExtraDataConsecutiveCompression,
    LinksInfo: IdentSequenceWriter + SequenceExtraData,
> Sync for StructSeqBinaryWriter<ColorInfo, LinksInfo>
{
}

impl<
    ColorInfo: IdentSequenceWriter + SequenceExtraDataConsecutiveCompression,
    LinksInfo: IdentSequenceWriter + SequenceExtraData,
> StructSeqBinaryWriter<ColorInfo, LinksInfo>
{
    pub fn new<T: Encode>(
        path: impl AsRef<Path>,
        file_mode: &(
            MemoryFileMode,
            CompressedCheckpointSize,
            CompressionLevelInfo,
        ),
        data_format: &T,
    ) -> Self {
        Self {
            writer: CompressedBinaryWriter::new(path.as_ref(), file_mode, 0, data_format),
            _phantom: Default::default(),
        }
    }
}

#[derive(Clone, Debug)]
pub struct SequenceDataWithAbundance<CX, LX> {
    pub index: u64,
    pub color: CX,
    pub link: LX,
    pub abundance: SequenceAbundanceType,
}

impl<CX: SequenceExtraDataTempBufferManagement, LX: SequenceExtraDataTempBufferManagement>
    SequenceExtraDataTempBufferManagement for SequenceDataWithAbundance<CX, LX>
{
    type TempBuffer = (CX::TempBuffer, LX::TempBuffer);

    fn new_temp_buffer() -> (CX::TempBuffer, LX::TempBuffer) {
        (CX::new_temp_buffer(), LX::new_temp_buffer())
    }

    fn clear_temp_buffer(buffer: &mut Self::TempBuffer) {
        CX::clear_temp_buffer(&mut buffer.0);
        LX::clear_temp_buffer(&mut buffer.1);
    }

    fn copy_temp_buffer(dest: &mut Self::TempBuffer, src: &(CX::TempBuffer, LX::TempBuffer)) {
        CX::copy_temp_buffer(&mut dest.0, &src.0);
        LX::copy_temp_buffer(&mut dest.1, &src.1);
    }

    fn copy_extra_from(
        extra: Self,
        src: &(CX::TempBuffer, LX::TempBuffer),
        dst: &mut Self::TempBuffer,
    ) -> Self {
        let color = CX::copy_extra_from(extra.color, &src.0, &mut dst.0);
        let link = LX::copy_extra_from(extra.link, &src.1, &mut dst.1);
        Self {
            index: extra.index,
            color,
            link,
            abundance: extra.abundance,
        }
    }
}

impl<CX: SequenceExtraDataConsecutiveCompression, LX: SequenceExtraData>
    SequenceExtraDataConsecutiveCompression for SequenceDataWithAbundance<CX, LX>
{
    type LastData = CX::LastData;

    fn decode_extended(
        buffer: &mut Self::TempBuffer,
        reader: &mut impl Read,
        last_data: Self::LastData,
    ) -> Option<Self> {
        let index = decode_varint(|| reader.read_u8().ok())?;
        #[cfg(feature = "support_kmer_counters")]
        let (abundance_first, abundance_sum, abundance_last) = {
            (
                decode_varint(|| reader.read_u8().ok())?,
                decode_varint(|| reader.read_u8().ok())?,
                decode_varint(|| reader.read_u8().ok())?,
            )
        };

        Some(Self {
            index,
            color: CX::decode_extended(&mut buffer.0, reader, last_data)?,
            link: LX::decode_extended(&mut buffer.1, reader)?,
            abundance: match () {
                #[cfg(feature = "support_kmer_counters")]
                () => SequenceAbundanceType {
                    first: abundance_first,
                    sum: abundance_sum,
                    last: abundance_last,
                },
                #[cfg(not(feature = "support_kmer_counters"))]
                () => (),
            },
        })
    }

    fn encode_extended(
        &self,
        buffer: &Self::TempBuffer,
        writer: &mut impl Write,
        last_data: Self::LastData,
    ) {
        encode_varint(|b| writer.write_all(b).ok(), self.index).unwrap();
        #[cfg(feature = "support_kmer_counters")]
        {
            encode_varint(|b| writer.write_all(b).ok(), self.abundance.first).unwrap();
            encode_varint(|b| writer.write_all(b).ok(), self.abundance.sum).unwrap();
            encode_varint(|b| writer.write_all(b).ok(), self.abundance.last).unwrap();
        }

        self.color.encode_extended(&buffer.0, writer, last_data);
        self.link.encode_extended(&buffer.1, writer);
    }

    fn max_size(&self) -> usize {
        VARINT_MAX_SIZE * 4 + self.color.max_size() + self.link.max_size()
    }

    fn obtain_last_data(&self, last_data: Self::LastData) -> Self::LastData {
        self.color.obtain_last_data(last_data)
    }
}

impl<
    ColorInfo: IdentSequenceWriter + SequenceExtraDataConsecutiveCompression,
    LinksInfo: IdentSequenceWriter + SequenceExtraData,
> StructuredSequenceBackend<ColorInfo, LinksInfo> for StructSeqBinaryWriter<ColorInfo, LinksInfo>
{
    type SequenceTempBuffer = (
        Vec<u8>,
        CompressedReadsBucketDataSerializer<
            SequenceDataWithAbundance<ColorInfo, LinksInfo>,
            typenum::consts::U0,
            NoSecondBucket,
            NoMultiplicity,
            NoMinimizerPosition,
        >,
    );

    fn alloc_temp_buffer(k: usize) -> Self::SequenceTempBuffer {
        (
            Vec::with_capacity(DEFAULT_PER_CPU_BUFFER_SIZE.as_bytes()),
            CompressedReadsBucketDataSerializer::new(k),
        )
    }

    fn write_sequence(
        _k: usize,
        buffer: &mut Self::SequenceTempBuffer,
        sequence_index: u64,
        sequence: &[u8],

        color_info: ColorInfo,
        links_info: LinksInfo,
        extra_buffers: &(ColorInfo::TempBuffer, LinksInfo::TempBuffer),
        #[cfg(feature = "support_kmer_counters")] abundance: SequenceAbundanceType,
    ) {
        buffer.1.write_to(
            &CompressedReadsBucketData::new(sequence, 0, 0, 0, false),
            &mut buffer.0,
            &SequenceDataWithAbundance {
                index: sequence_index,
                color: color_info,
                link: links_info,
                abundance: match () {
                    #[cfg(feature = "support_kmer_counters")]
                    () => abundance,
                    #[cfg(not(feature = "support_kmer_counters"))]
                    () => (),
                },
            },
            &extra_buffers,
        );
    }

    fn get_path(&self) -> PathBuf {
        self.writer.get_path()
    }

    fn flush_temp_buffer(&mut self, buffer: &mut Self::SequenceTempBuffer) {
        self.writer.write_data(&buffer.0);
        buffer.0.clear();
        buffer.1.reset();
    }

    fn finalize(self) {
        self.writer.finalize();
    }
}
