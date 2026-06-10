use crate::indirect_reads_extractor::{ReadExtractWorkData, indirect_read_extract_parts};
use crate::structured_sequences::{IdentSequenceWriter, StructuredSequenceBackend};
use bzip2::Compression as BzipCompression;
use colors::colors_manager::ColorsManager;
use colors::colors_manager::color_types::PartialUnitigsColorStructure;
use config::{DEFAULT_OUTPUT_BUFFER_SIZE, DEFAULT_PER_CPU_BUFFER_SIZE};
use dynamic_dispatch::dynamic_dispatch;
use flate2::Compression as FlateCompression;
use flate2::write::GzEncoder;
use hashes::HashableSequence;
use io::compressed_read::CompressedRead;
use io::concurrent::temp_reads::extra_data::{SequenceExtraData, TempBuffer};
use io::concurrent_filewriter::ConcurrentFileWriter;
use io::partial_unitigs_extra_data::PartialUnitigExtraData;
use lz4::{BlockMode, BlockSize, ContentChecksum};
use std::fs::File;
use std::io::{BufWriter, Write};
use std::marker::PhantomData;
use std::path::{Path, PathBuf};

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
            FlateCompression::new(level),
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

    fn new_compressed_zstd(path: impl AsRef<Path>, level: u32) -> Self {
        let compress_stream = zstd::stream::write::Encoder::new(
            BufWriter::with_capacity(DEFAULT_OUTPUT_BUFFER_SIZE, File::create(&path).unwrap()),
            level as i32,
        )
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

    fn new_compressed_bz2(path: impl AsRef<Path>, level: u32) -> Self {
        let compress_stream = bzip2::write::BzEncoder::new(
            BufWriter::with_capacity(DEFAULT_OUTPUT_BUFFER_SIZE, File::create(&path).unwrap()),
            BzipCompression::new(level),
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

    fn new_compressed_xz(path: impl AsRef<Path>, level: u32) -> Self {
        let compress_stream = xz2::write::XzEncoder::new(
            BufWriter::with_capacity(DEFAULT_OUTPUT_BUFFER_SIZE, File::create(&path).unwrap()),
            level,
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
        extra_buffers: &(
            TempBuffer<PartialUnitigExtraData<PartialUnitigsColorStructure<CX>>>,
            LinksInfo::TempBuffer,
        ),
        indirect_file: Option<&ConcurrentFileWriter>,
        mut flush_callback: impl FnMut(&mut Self::SequenceTempBuffer),
    ) {
        let bases_count =
            sequence.bases_count() + extra_info.mode.get_total_length(&extra_buffers.0.1);

        let mut write_sequence_bases =
            |buffer: &mut Vec<u8>, flush_callback: &mut dyn FnMut(&mut Vec<u8>)| match extra_info
                .mode
            {
                io::partial_unitigs_extra_data::PartialUnitigMode::Inline => {
                    buffer.extend(sequence.as_bases_iter());
                }
                io::partial_unitigs_extra_data::PartialUnitigMode::Indirect { .. } => {
                    indirect_read_extract_parts::<CX, false, true>(
                        &mut extract_workdata.file_buffer,
                        &mut extract_workdata.file_color_buffer,
                        k,
                        sequence,
                        &extra_info,
                        &extra_buffers.0,
                        indirect_file.as_ref().unwrap(),
                        |part, _, _, is_rc, _, _| {
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
                }
            };

        // Sequence line
        if VERSION == 1 {
            // S <index> <sequence> LN:i:<length> ...
            write!(buffer, "S\t{}\t", sequence_index).unwrap();
            write_sequence_bases(buffer, &mut flush_callback);
            write!(buffer, "\tLN:i:{}", bases_count).unwrap();
        } else if VERSION == 2 {
            // S <index> <len> <sequence> ...
            write!(buffer, "S\t{}\t{}\t", sequence_index, bases_count).unwrap();
            write_sequence_bases(buffer, &mut flush_callback);
        }

        #[cfg(feature = "support_kmer_counters")]
        {
            // Continuation of the S line
            // ... KC:i:<kmer_count>    km:f:<kmer_coverage>
            write!(buffer, "\tKC:i:{}", extra_info.counters.sum).unwrap();
            write!(
                buffer,
                "\tkm:f:{:.1}",
                extra_info.counters.sum as f64 / (bases_count - k + 1) as f64
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
