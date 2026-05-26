use crate::indirect_reads_extractor::ReadExtractWorkData;
use crate::structured_sequences::{
    StructuredSequenceBackend, StructuredSequenceBackendInit, StructuredSequenceBackendWrapper,
};
use bincode::Encode;
use byteorder::ReadBytesExt;
use colors::colors_manager::ColorsManager;
use colors::colors_manager::color_types::PartialUnitigsColorStructure;
use config::DEFAULT_PER_CPU_BUFFER_SIZE;
use dynamic_dispatch::dynamic_dispatch;
use io::compressed_read::CompressedRead;
use io::concurrent::temp_reads::creads_utils::{
    CompressedReadsBucketData, CompressedReadsBucketDataSerializer, NoMinimizerPosition,
    NoMultiplicity, NoSecondBucket,
};
use io::concurrent::temp_reads::extra_data::{
    SequenceExtraData, SequenceExtraDataConsecutiveCompression,
    SequenceExtraDataTempBufferManagement,
};
use io::concurrent_filewriter::ConcurrentFileWriter;
use io::ident_writer::IdentSequenceWriter;
use io::partial_unitigs_extra_data::PartialUnitigExtraData;
use io::varint::{VARINT_MAX_SIZE, decode_varint, encode_varint};
use parallel_processor::buckets::LockFreeBucket;
use parallel_processor::buckets::bucket_writer::BucketItemSerializer;
use parallel_processor::buckets::writers::compressed_binary_writer::{
    CompressedBinaryWriter, CompressedCheckpointSize, CompressionLevelInfo,
};
use parallel_processor::memory_fs::file::internal::MemoryFileMode;
use std::io::{Read, Write};
use std::marker::PhantomData;
use std::path::{Path, PathBuf};

pub struct StructSeqBinaryWriter<
    CX: ColorsManager,
    LinksInfo: IdentSequenceWriter + SequenceExtraData,
> {
    writer: CompressedBinaryWriter,
    _phantom: PhantomData<(CX, LinksInfo)>,
}

pub struct StructSeqBinaryWriterWrapper;

#[dynamic_dispatch]
impl StructuredSequenceBackendWrapper for StructSeqBinaryWriterWrapper {
    type Backend<CX: ColorsManager, LinksInfo: IdentSequenceWriter + SequenceExtraData> =
        StructSeqBinaryWriter<CX, LinksInfo>;
}

unsafe impl<CX: ColorsManager, LinksInfo: IdentSequenceWriter + SequenceExtraData> Send
    for StructSeqBinaryWriter<CX, LinksInfo>
{
}

unsafe impl<CX: ColorsManager, LinksInfo: IdentSequenceWriter + SequenceExtraData> Sync
    for StructSeqBinaryWriter<CX, LinksInfo>
{
}

impl<CX: ColorsManager, LinksInfo: IdentSequenceWriter + SequenceExtraData>
    StructSeqBinaryWriter<CX, LinksInfo>
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

impl<CX: ColorsManager, LinksInfo: IdentSequenceWriter + SequenceExtraData>
    StructuredSequenceBackendInit for StructSeqBinaryWriter<CX, LinksInfo>
{
    fn new_compressed_gzip(_path: impl AsRef<Path>, _level: u32) -> Self {
        unimplemented!()
    }

    fn new_compressed_lz4(_path: impl AsRef<Path>, _level: u32) -> Self {
        unimplemented!()
    }

    fn new_plain(_path: impl AsRef<Path>) -> Self {
        unimplemented!()
    }
}

#[derive(Clone, Debug)]
pub struct SequenceDataWithLinks<CX: SequenceExtraDataConsecutiveCompression, LX> {
    pub index: u64,
    pub extra_data: PartialUnitigExtraData<CX>,
    pub link: LX,
}

impl<CX: SequenceExtraDataConsecutiveCompression, LX: SequenceExtraDataTempBufferManagement>
    SequenceExtraDataTempBufferManagement for SequenceDataWithLinks<CX, LX>
{
    type TempBuffer = (
        <PartialUnitigExtraData<CX> as SequenceExtraDataTempBufferManagement>::TempBuffer,
        LX::TempBuffer,
    );

    fn new_temp_buffer() -> (
        <PartialUnitigExtraData<CX> as SequenceExtraDataTempBufferManagement>::TempBuffer,
        LX::TempBuffer,
    ) {
        (
            PartialUnitigExtraData::<CX>::new_temp_buffer(),
            LX::new_temp_buffer(),
        )
    }

    fn clear_temp_buffer(buffer: &mut Self::TempBuffer) {
        PartialUnitigExtraData::<CX>::clear_temp_buffer(&mut buffer.0);
        LX::clear_temp_buffer(&mut buffer.1);
    }

    fn copy_temp_buffer(
        dest: &mut Self::TempBuffer,
        src: &(
            <PartialUnitigExtraData<CX> as SequenceExtraDataTempBufferManagement>::TempBuffer,
            LX::TempBuffer,
        ),
    ) {
        PartialUnitigExtraData::<CX>::copy_temp_buffer(&mut dest.0, &src.0);
        LX::copy_temp_buffer(&mut dest.1, &src.1);
    }

    fn copy_extra_from(
        extra: Self,
        src: &(
            <PartialUnitigExtraData<CX> as SequenceExtraDataTempBufferManagement>::TempBuffer,
            LX::TempBuffer,
        ),
        dst: &mut Self::TempBuffer,
    ) -> Self {
        let extra_data =
            PartialUnitigExtraData::<CX>::copy_extra_from(extra.extra_data, &src.0, &mut dst.0);
        let link = LX::copy_extra_from(extra.link, &src.1, &mut dst.1);
        Self {
            index: extra.index,
            extra_data,
            link,
        }
    }
}

impl<CX: SequenceExtraDataConsecutiveCompression, LX: SequenceExtraData>
    SequenceExtraDataConsecutiveCompression for SequenceDataWithLinks<CX, LX>
{
    type LastData = CX::LastData;

    fn decode_extended(
        buffer: &mut Self::TempBuffer,
        reader: &mut impl Read,
        last_data: Self::LastData,
        read_flags: u8,
    ) -> Option<Self> {
        let index = decode_varint(|| reader.read_u8().ok())?;
        Some(Self {
            index,
            extra_data: PartialUnitigExtraData::<CX>::decode_extended(
                &mut buffer.0,
                reader,
                last_data,
                read_flags,
            )?,
            link: LX::decode_extended(&mut buffer.1, reader)?,
        })
    }

    fn encode_extended(
        &self,
        buffer: &Self::TempBuffer,
        writer: &mut impl Write,
        last_data: Self::LastData,
        sequence_length: usize,
        reverse_complement: bool,
        read_flags: u8,
    ) {
        encode_varint(|b| writer.write_all(b).ok(), self.index).unwrap();
        self.extra_data.encode_extended(
            &buffer.0,
            writer,
            last_data,
            sequence_length,
            reverse_complement,
            read_flags,
        );
        self.link.encode_extended(
            &buffer.1,
            writer,
            sequence_length,
            reverse_complement,
            read_flags,
        );
    }

    fn max_size(&self) -> usize {
        VARINT_MAX_SIZE + self.extra_data.max_size() + self.link.max_size()
    }

    fn obtain_last_data(
        &self,
        last_data: Self::LastData,
        reverse_complement: bool,
    ) -> Self::LastData {
        self.extra_data
            .obtain_last_data(last_data, reverse_complement)
    }
}

impl<CX: ColorsManager, LinksInfo: IdentSequenceWriter + SequenceExtraData>
    StructuredSequenceBackend<CX, LinksInfo> for StructSeqBinaryWriter<CX, LinksInfo>
{
    type SequenceTempBuffer = (
        Vec<u8>,
        CompressedReadsBucketDataSerializer<
            SequenceDataWithLinks<PartialUnitigsColorStructure<CX>, LinksInfo>,
            NoSecondBucket,
            NoMultiplicity,
            NoMinimizerPosition,
            typenum::U0,
        >,
    );

    fn alloc_temp_buffer(k: usize) -> Self::SequenceTempBuffer {
        (
            Vec::with_capacity(DEFAULT_PER_CPU_BUFFER_SIZE.as_bytes()),
            CompressedReadsBucketDataSerializer::new(k),
        )
    }

    fn write_sequence(
        _extract_workdata: &mut ReadExtractWorkData<CX>,
        _k: usize,
        buffer: &mut Self::SequenceTempBuffer,
        sequence_index: u64,
        sequence: CompressedRead,

        extra_info: PartialUnitigExtraData<PartialUnitigsColorStructure<CX>>,
        links_info: LinksInfo,
        extra_buffers: &(<PartialUnitigExtraData<PartialUnitigsColorStructure<CX>> as SequenceExtraDataTempBufferManagement>::TempBuffer, LinksInfo::TempBuffer),
        _indirect_file: Option<&ConcurrentFileWriter>,
        _flush_callback: impl FnMut(&mut Self::SequenceTempBuffer),
    ) {
        buffer.1.write_to(
            &CompressedReadsBucketData::new(sequence, 0, 0, 0),
            &mut buffer.0,
            &SequenceDataWithLinks {
                index: sequence_index,
                extra_data: extra_info,
                link: links_info,
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
