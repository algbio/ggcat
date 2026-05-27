use crate::indirect_reads_extractor::{
    ReadExtractWorkData, indirect_read_extract_all, indirect_read_extract_parts,
};
use crate::structured_sequences::{IdentSequenceWriter, StructuredSequenceBackend};
use colors::colors_manager::ColorsManager;
use colors::colors_manager::color_types::PartialUnitigsColorStructure;
use config::{DEFAULT_OUTPUT_BUFFER_SIZE, DEFAULT_PER_CPU_BUFFER_SIZE, MAX_INLINE_UNITIG_SIZE};
use dynamic_dispatch::dynamic_dispatch;
use flate2::Compression;
use flate2::write::GzEncoder;
#[cfg(not(feature = "support_kmer_counters"))]
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

use super::stream_finish::SequencesWriterWrapper;

#[cfg(feature = "support_kmer_counters")]
use super::SequenceAbundance;
use super::{StructuredSequenceBackendInit, StructuredSequenceBackendWrapper};

pub struct FastaWriterWrapper;

#[dynamic_dispatch]
impl StructuredSequenceBackendWrapper for FastaWriterWrapper {
    type Backend<CX: ColorsManager, LinksInfo: IdentSequenceWriter + SequenceExtraData> =
        FastaWriter<CX, LinksInfo>;
}

pub struct FastaWriter<CX: ColorsManager, LinksInfo: IdentSequenceWriter> {
    writer: Box<dyn Write>,
    path: PathBuf,
    _phantom: PhantomData<(CX, LinksInfo)>,
}

unsafe impl<CX: ColorsManager, LinksInfo: IdentSequenceWriter> Send for FastaWriter<CX, LinksInfo> {}

unsafe impl<CX: ColorsManager, LinksInfo: IdentSequenceWriter> Sync for FastaWriter<CX, LinksInfo> {}

impl<CX: ColorsManager, LinksInfo: IdentSequenceWriter> StructuredSequenceBackendInit
    for FastaWriter<CX, LinksInfo>
{
    fn new_compressed_gzip(path: impl AsRef<Path>, level: u32) -> Self {
        let compress_stream = GzEncoder::new(
            BufWriter::with_capacity(DEFAULT_OUTPUT_BUFFER_SIZE, File::create(&path).unwrap()),
            Compression::new(level),
        );

        FastaWriter {
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

        FastaWriter {
            writer: Box::new(SequencesWriterWrapper::new(BufWriter::with_capacity(
                DEFAULT_OUTPUT_BUFFER_SIZE,
                compress_stream,
            ))),
            path: path.as_ref().to_path_buf(),
            _phantom: PhantomData,
        }
    }

    fn new_plain(path: impl AsRef<Path>) -> Self {
        FastaWriter {
            writer: Box::new(SequencesWriterWrapper::new(BufWriter::with_capacity(
                DEFAULT_OUTPUT_BUFFER_SIZE,
                File::create(&path).unwrap(),
            ))),
            path: path.as_ref().to_path_buf(),
            _phantom: PhantomData,
        }
    }
}

impl<CX: ColorsManager, LinksInfo: IdentSequenceWriter> StructuredSequenceBackend<CX, LinksInfo>
    for FastaWriter<CX, LinksInfo>
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
        mut flush_callback: impl FnMut(&mut Self::SequenceTempBuffer),
    ) {
        let bases_count =
            sequence.bases_count() + extra_info.mode.get_total_length(&extra_buffers.0.1);

        #[cfg(feature = "support_kmer_counters")]
        write!(
            buffer,
            ">{} LN:i:{} KC:i:{} km:f:{:.1}",
            sequence_index,
            bases_count,
            abundance.sum,
            abundance.sum as f64 / (sequence.len() - _k + 1) as f64
        )
        .unwrap();

        #[cfg(not(feature = "support_kmer_counters"))]
        write!(buffer, ">{} LN:i:{}", sequence_index, bases_count,).unwrap();

        let mut links_partial_data = Default::default();
        let mut colors_partial_data = Default::default();

        match extra_info.mode {
            io::partial_unitigs_extra_data::PartialUnitigMode::Inline => {
                extra_info.colors.write_as_ident(
                    &mut colors_partial_data,
                    (0, None),
                    false,
                    buffer,
                    &extra_buffers.0.0,
                );
                PartialUnitigsColorStructure::<CX>::flush_partial_as_ident(
                    colors_partial_data,
                    buffer,
                );

                let mut links_partial_data = Default::default();
                links_info.write_as_ident(
                    &mut links_partial_data,
                    (0, None),
                    false,
                    buffer,
                    &extra_buffers.1,
                );
                LinksInfo::flush_partial_as_ident(links_partial_data, buffer);

                buffer.extend_from_slice(b"\n");
                buffer.extend(sequence.as_bases_iter());
                buffer.extend_from_slice(b"\n");
            }
            io::partial_unitigs_extra_data::PartialUnitigMode::Indirect { .. } => {
                indirect_read_extract_parts::<CX, true, false>(
                    &mut extract_workdata.file_buffer,
                    &mut extract_workdata.file_color_buffer,
                    k,
                    sequence,
                    &extra_info,
                    &extra_buffers.0,
                    indirect_file.as_ref().unwrap(),
                    |_, color, color_range, is_rc, color_buffer| {
                        // TODO: FIX COLORS
                        color.write_as_ident(
                            &mut colors_partial_data,
                            color_range,
                            is_rc,
                            buffer,
                            color_buffer,
                        );
                        if buffer.len() > DEFAULT_OUTPUT_BUFFER_SIZE {
                            flush_callback(buffer)
                        }
                    },
                );

                PartialUnitigsColorStructure::<CX>::flush_partial_as_ident(
                    colors_partial_data,
                    buffer,
                );

                links_info.write_as_ident(
                    &mut links_partial_data,
                    (0, None),
                    false,
                    buffer,
                    &extra_buffers.1,
                );
                LinksInfo::flush_partial_as_ident(links_partial_data, buffer);
                buffer.extend_from_slice(b"\n");
                indirect_read_extract_parts::<CX, false, true>(
                    &mut extract_workdata.file_buffer,
                    &mut extract_workdata.file_color_buffer,
                    k,
                    sequence,
                    &extra_info,
                    &extra_buffers.0,
                    indirect_file.as_ref().unwrap(),
                    |part, _, _, is_rc, _| {
                        if is_rc {
                            buffer.extend(part.as_reverse_complement_bases_iter());
                        } else {
                            buffer.extend(part.as_bases_iter())
                        }
                        if buffer.len() > DEFAULT_OUTPUT_BUFFER_SIZE {
                            flush_callback(buffer)
                        }
                    },
                );
                buffer.extend_from_slice(b"\n");
            }
        }
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

impl<CX: ColorsManager, LinksInfo: IdentSequenceWriter> Drop for FastaWriter<CX, LinksInfo> {
    fn drop(&mut self) {
        self.writer.flush().unwrap();
    }
}
