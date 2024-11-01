use crate::concurrent::structured_sequences::{IdentSequenceWriter, StructuredSequenceBackend};
use config::{DEFAULT_OUTPUT_BUFFER_SIZE, DEFAULT_PER_CPU_BUFFER_SIZE};
use dynamic_dispatch::dynamic_dispatch;
use flate2::write::GzEncoder;
use flate2::Compression;
use lz4::{BlockMode, BlockSize, ContentChecksum};
use std::fs::File;
use std::io::{BufWriter, Write};
use std::marker::PhantomData;
use std::path::{Path, PathBuf};

use super::stream_finish::SequencesWriterWrapper;
#[cfg(feature = "support_kmer_counters")]
use super::SequenceAbundance;
use super::{StructuredSequenceBackendInit, StructuredSequenceBackendWrapper};

pub struct GFAWriterWrapper;

#[dynamic_dispatch]
impl StructuredSequenceBackendWrapper for GFAWriterWrapper {
    type Backend<ColorInfo: IdentSequenceWriter, LinksInfo: IdentSequenceWriter> =
        GFAWriter<ColorInfo, LinksInfo>;
}
pub struct GFAWriter<ColorInfo: IdentSequenceWriter, LinksInfo: IdentSequenceWriter> {
    writer: Box<dyn Write>,
    path: PathBuf,
    _phantom: PhantomData<(ColorInfo, LinksInfo)>,
}

unsafe impl<ColorInfo: IdentSequenceWriter, LinksInfo: IdentSequenceWriter> Send
    for GFAWriter<ColorInfo, LinksInfo>
{
}

unsafe impl<ColorInfo: IdentSequenceWriter, LinksInfo: IdentSequenceWriter> Sync
    for GFAWriter<ColorInfo, LinksInfo>
{
}

impl<ColorInfo: IdentSequenceWriter, LinksInfo: IdentSequenceWriter> StructuredSequenceBackendInit
    for GFAWriter<ColorInfo, LinksInfo>
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

impl<ColorInfo: IdentSequenceWriter, LinksInfo: IdentSequenceWriter>
    StructuredSequenceBackend<ColorInfo, LinksInfo> for GFAWriter<ColorInfo, LinksInfo>
{
    type SequenceTempBuffer = Vec<u8>;

    fn alloc_temp_buffer() -> Self::SequenceTempBuffer {
        Vec::with_capacity(DEFAULT_PER_CPU_BUFFER_SIZE.as_bytes())
    }

    fn write_sequence(
        k: usize,
        buffer: &mut Self::SequenceTempBuffer,
        sequence_index: u64,
        sequence: &[u8],

        _color_info: ColorInfo,
        links_info: LinksInfo,
        extra_buffers: &(ColorInfo::TempBuffer, LinksInfo::TempBuffer),

        #[cfg(feature = "support_kmer_counters")] abundance: SequenceAbundance,
    ) {
        write!(buffer, "S\t{}\t{}\t", sequence_index, sequence.len()).unwrap();
        buffer.extend_from_slice(sequence);
        write!(buffer, "\tLN:i:{}", sequence.len()).unwrap();

        #[cfg(feature = "support_kmer_counters")]
        {
            write!(buffer, "\tKC:i:{}", abundance.sum).unwrap();
            write!(
                buffer,
                "\tkm:f:{:.1}",
                abundance.sum as f64 / (sequence.len() - k + 1) as f64
            )
            .unwrap();
        }

        buffer.push(b'\n');

        // color_info.write_as_ident(buffer, &extra_buffers.0);
        links_info.write_as_gfa(k as u64, sequence_index, buffer, &extra_buffers.1);
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

impl<ColorInfo: IdentSequenceWriter, LinksInfo: IdentSequenceWriter> Drop
    for GFAWriter<ColorInfo, LinksInfo>
{
    fn drop(&mut self) {
        self.writer.flush().unwrap();
    }
}
