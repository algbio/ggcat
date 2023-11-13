use crate::concurrent::structured_sequences::{IdentSequenceWriter, StructuredSequenceBackend};
use crate::concurrent::temp_reads::creads_utils::{
    CompressedReadsBucketData, CompressedReadsBucketDataSerializer,
};
use crate::concurrent::temp_reads::extra_data::{
    SequenceExtraData, SequenceExtraDataConsecutiveCompression,
    SequenceExtraDataTempBufferManagement,
};
use crate::varint::{decode_varint, encode_varint, VARINT_MAX_SIZE};
use byteorder::ReadBytesExt;
use config::DEFAULT_PER_CPU_BUFFER_SIZE;
use parallel_processor::buckets::bucket_writer::BucketItemSerializer;
use parallel_processor::buckets::writers::compressed_binary_writer::{
    CompressedBinaryWriter, CompressedCheckpointSize, CompressionLevelInfo,
};
use parallel_processor::buckets::LockFreeBucket;
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
    pub fn new(
        path: impl AsRef<Path>,
        file_mode: &(
            MemoryFileMode,
            CompressedCheckpointSize,
            CompressionLevelInfo,
        ),
    ) -> Self {
        Self {
            writer: CompressedBinaryWriter::new(path.as_ref(), file_mode, 0),
            _phantom: Default::default(),
        }
    }
}

impl<CX: SequenceExtraDataTempBufferManagement, LX: SequenceExtraDataTempBufferManagement>
    SequenceExtraDataTempBufferManagement for (u64, CX, LX, SequenceAbundanceType)
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
        let (index, cx, lx, abundance) = extra;
        let cx = CX::copy_extra_from(cx, &src.0, &mut dst.0);
        let lx = LX::copy_extra_from(lx, &src.1, &mut dst.1);
        (index, cx, lx, abundance)
    }
}

impl<CX: SequenceExtraDataConsecutiveCompression, LX: SequenceExtraData>
    SequenceExtraDataConsecutiveCompression for (u64, CX, LX, SequenceAbundanceType)
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

        Some((
            index,
            CX::decode_extended(&mut buffer.0, reader, last_data)?,
            LX::decode_extended(&mut buffer.1, reader)?,
            match () {
                #[cfg(feature = "support_kmer_counters")]
                () => SequenceAbundanceType {
                    first: abundance_first,
                    sum: abundance_sum,
                    last: abundance_last,
                },
                #[cfg(not(feature = "support_kmer_counters"))]
                () => (),
            },
        ))
    }

    fn encode_extended(
        &self,
        buffer: &Self::TempBuffer,
        writer: &mut impl Write,
        last_data: Self::LastData,
    ) {
        encode_varint(|b| writer.write_all(b).ok(), self.0).unwrap();
        #[cfg(feature = "support_kmer_counters")]
        {
            encode_varint(|b| writer.write_all(b).ok(), self.3.first).unwrap();
            encode_varint(|b| writer.write_all(b).ok(), self.3.sum).unwrap();
            encode_varint(|b| writer.write_all(b).ok(), self.3.last).unwrap();
        }

        self.1.encode_extended(&buffer.0, writer, last_data);
        self.2.encode_extended(&buffer.1, writer);
    }

    fn max_size(&self) -> usize {
        VARINT_MAX_SIZE * 4 + self.1.max_size() + self.2.max_size()
    }

    fn obtain_last_data(&self, last_data: Self::LastData) -> Self::LastData {
        self.1.obtain_last_data(last_data)
    }
}

impl<
        ColorInfo: IdentSequenceWriter + SequenceExtraDataConsecutiveCompression,
        LinksInfo: IdentSequenceWriter + SequenceExtraData,
    > StructuredSequenceBackend<ColorInfo, LinksInfo>
    for StructSeqBinaryWriter<ColorInfo, LinksInfo>
{
    type SequenceTempBuffer = (
        Vec<u8>,
        CompressedReadsBucketDataSerializer<
            (u64, ColorInfo, LinksInfo, SequenceAbundanceType),
            typenum::consts::U0,
            false,
        >,
    );

    fn alloc_temp_buffer() -> Self::SequenceTempBuffer {
        (
            Vec::with_capacity(DEFAULT_PER_CPU_BUFFER_SIZE.as_bytes()),
            CompressedReadsBucketDataSerializer::new(),
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
            &CompressedReadsBucketData::new(sequence, 0, 0),
            &mut buffer.0,
            &(
                sequence_index,
                color_info,
                links_info,
                match () {
                    #[cfg(feature = "support_kmer_counters")]
                    () => abundance,
                    #[cfg(not(feature = "support_kmer_counters"))]
                    () => (),
                },
            ),
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
