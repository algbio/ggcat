use crate::indirect_reads_extractor::ReadExtractWorkData;
use crate::structured_sequences::{IdentSequenceWriter, StructuredSequenceBackend};
use colors::colors_manager::ColorsManager;
use colors::colors_manager::color_types::PartialUnitigsColorStructure;
use config::{DEFAULT_OUTPUT_BUFFER_SIZE, DEFAULT_PER_CPU_BUFFER_SIZE};
use dynamic_dispatch::dynamic_dispatch;
use flate2::Compression;
use flate2::write::GzEncoder;
use hashes::HashableSequence;
use io::compressed_read::CompressedRead;
use io::concurrent::temp_reads::extra_data::{
    SequenceExtraData, SequenceExtraDataConsecutiveCompression,
    SequenceExtraDataTempBufferManagement,
};
use io::concurrent_filewriter::ConcurrentFileWriter;
use io::partial_unitigs_extra_data::PartialUnitigExtraData;
use lz4::{BlockMode, BlockSize, ContentChecksum};
use std::fs::File;
use std::io::{BufWriter, Write};
use std::marker::PhantomData;
use std::path::{Path, PathBuf};

#[cfg(feature = "support_kmer_counters")]
use super::SequenceAbundance;
use super::stream_finish::SequencesWriterWrapper;
use super::{StructuredSequenceBackendInit, StructuredSequenceBackendWrapper};

pub struct GFAWriterWrapperV1;

#[dynamic_dispatch]
impl StructuredSequenceBackendWrapper for GFAWriterWrapperV1 {
    type Backend<CX: ColorsManager, LinksInfo: IdentSequenceWriter + SequenceExtraData> =
        GFAWriter<CX, LinksInfo, 1>;
}

pub struct GFAWriterWrapperV2;

#[dynamic_dispatch]
impl StructuredSequenceBackendWrapper for GFAWriterWrapperV2 {
    type Backend<CX: ColorsManager, LinksInfo: IdentSequenceWriter + SequenceExtraData> =
        GFAWriter<CX, LinksInfo, 2>;
}

pub struct GFAWriter<CX: ColorsManager, LinksInfo: IdentSequenceWriter, const VERSION: u32> {
    writer: Box<dyn Write>,
    path: PathBuf,
    _phantom: PhantomData<(CX, LinksInfo)>,
}

unsafe impl<const VERSION: u32, CX: ColorsManager, LinksInfo: IdentSequenceWriter> Send
    for GFAWriter<CX, LinksInfo, VERSION>
{
}

unsafe impl<const VERSION: u32, CX: ColorsManager, LinksInfo: IdentSequenceWriter> Sync
    for GFAWriter<CX, LinksInfo, VERSION>
{
}

impl<const VERSION: u32, CX: ColorsManager, LinksInfo: IdentSequenceWriter>
    StructuredSequenceBackendInit for GFAWriter<CX, LinksInfo, VERSION>
{
    fn new_compressed_gzip(path: impl AsRef<Path>, level: u32) -> Self {
        let compress_stream = GzEncoder::new(
            BufWriter::with_capacity(DEFAULT_OUTPUT_BUFFER_SIZE, File::create(&path).unwrap()),
            Compression::new(level),
        );

        GFAWriter {
            writer: Box::new(SequencesWriterWrapper::new(BufWriter::with_capacity(
                DEFAULT_OUTPUT_BUFFER_SIZE,
                compress_stream,
            ))),
            path: path.as_ref().to_path_buf(),
            _phantom: PhantomData,
        }
    }

    fn new_compressed_lz4(path: impl AsRef<Path>, level: u32) -> Self {
        let compress_stream = lz4::EncoderBuilder::new()
            .level(level)
            .checksum(ContentChecksum::NoChecksum)
            .block_mode(BlockMode::Linked)
            .block_size(BlockSize::Max1MB)
            .build(BufWriter::with_capacity(
                DEFAULT_OUTPUT_BUFFER_SIZE,
                File::create(&path).unwrap(),
            ))
            .unwrap();

        GFAWriter {
            writer: Box::new(SequencesWriterWrapper::new(BufWriter::with_capacity(
                DEFAULT_OUTPUT_BUFFER_SIZE,
                compress_stream,
            ))),
            path: path.as_ref().to_path_buf(),
            _phantom: PhantomData,
        }
    }

    fn new_plain(path: impl AsRef<Path>) -> Self {
        GFAWriter {
            writer: Box::new(SequencesWriterWrapper::new(BufWriter::with_capacity(
                DEFAULT_OUTPUT_BUFFER_SIZE,
                File::create(&path).unwrap(),
            ))),
            path: path.as_ref().to_path_buf(),
            _phantom: PhantomData,
        }
    }
}

impl<const VERSION: u32, CX: ColorsManager, LinksInfo: IdentSequenceWriter>
    StructuredSequenceBackend<CX, LinksInfo> for GFAWriter<CX, LinksInfo, VERSION>
{
    type SequenceTempBuffer = Vec<u8>;

    fn alloc_temp_buffer(_: usize) -> Self::SequenceTempBuffer {
        Vec::with_capacity(DEFAULT_PER_CPU_BUFFER_SIZE.as_bytes())
    }

    fn write_sequence(
        extract_workdata: &mut ReadExtractWorkData<CX>,
        k: usize,
        buffer: &mut Self::SequenceTempBuffer,
        sequence_index: u64,
        sequence: CompressedRead,

        extra_info: PartialUnitigExtraData<PartialUnitigsColorStructure<CX>>,
        links_info: LinksInfo,
        extra_buffers: &(<PartialUnitigExtraData<PartialUnitigsColorStructure<CX>> as SequenceExtraDataTempBufferManagement>::TempBuffer, LinksInfo::TempBuffer),
        indirect_file: Option<&ConcurrentFileWriter>,
        flush_callback: impl FnMut(&mut Self::SequenceTempBuffer),
    ) {
        // Sequence line
        if VERSION == 1 {
            // S <index> <sequence> LN:i:<length> ...
            write!(buffer, "S\t{}\t", sequence_index).unwrap();
            buffer.extend(sequence.as_bases_iter());
            write!(buffer, "\tLN:i:{}", sequence.bases_count()).unwrap();
        } else if VERSION == 2 {
            // S <index> <len> <sequence> ...
            write!(
                buffer,
                "S\t{}\t{}\t",
                sequence_index,
                sequence.bases_count()
            )
            .unwrap();
            buffer.extend(sequence.as_bases_iter());
        }

        #[cfg(feature = "support_kmer_counters")]
        {
            // Continuation of the S line
            // ... KC:i:<kmer_count>    km:f:<kmer_coverage>
            write!(buffer, "\tKC:i:{}", abundance.sum).unwrap();
            write!(
                buffer,
                "\tkm:f:{:.1}",
                abundance.sum as f64 / (sequence.len() - k + 1) as f64
            )
            .unwrap();
        }

        // End of the S line
        buffer.push(b'\n');

        let mut links_partial_data = Default::default();

        // color_info.write_as_ident(buffer, &extra_buffers.0);
        links_info.write_as_gfa::<VERSION>(
            k as u64,
            sequence_index,
            sequence.bases_count() as u64,
            &mut links_partial_data,
            (0, None),
            false,
            buffer,
            &extra_buffers.1,
        );
        LinksInfo::flush_partial_as_ident(links_partial_data, buffer);
    }

    fn get_path(&self) -> PathBuf {
        self.path.clone()
    }

    fn flush_temp_buffer(&mut self, buffer: &mut Self::SequenceTempBuffer) {
        self.writer.write_all(buffer).unwrap();
        buffer.clear();
    }

    fn finalize(self) {}
}

impl<const VERSION: u32, CX: ColorsManager, LinksInfo: IdentSequenceWriter> Drop
    for GFAWriter<CX, LinksInfo, VERSION>
{
    fn drop(&mut self) {
        self.writer.flush().unwrap();
    }
}
