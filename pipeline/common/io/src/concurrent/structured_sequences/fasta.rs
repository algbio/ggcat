use crate::concurrent::structured_sequences::{IdentSequenceWriter, StructuredSequenceBackend};
use config::{DEFAULT_OUTPUT_BUFFER_SIZE, DEFAULT_PER_CPU_BUFFER_SIZE};
use flate2::write::GzEncoder;
use flate2::Compression;
use lz4::{BlockMode, BlockSize, ContentChecksum};
use std::fs::File;
use std::io::{BufWriter, Write};
use std::marker::PhantomData;
use std::path::{Path, PathBuf};

pub struct FastaWriter<ColorInfo: IdentSequenceWriter, LinksInfo: IdentSequenceWriter> {
    writer: Box<dyn Write>,
    path: PathBuf,
    _phantom: PhantomData<(ColorInfo, LinksInfo)>,
}

unsafe impl<ColorInfo: IdentSequenceWriter, LinksInfo: IdentSequenceWriter> Send
    for FastaWriter<ColorInfo, LinksInfo>
{
}

unsafe impl<ColorInfo: IdentSequenceWriter, LinksInfo: IdentSequenceWriter> Sync
    for FastaWriter<ColorInfo, LinksInfo>
{
}

impl<ColorInfo: IdentSequenceWriter, LinksInfo: IdentSequenceWriter>
    FastaWriter<ColorInfo, LinksInfo>
{
    pub fn new_compressed_gzip(path: impl AsRef<Path>, level: u32) -> Self {
        let compress_stream = GzEncoder::new(
            BufWriter::with_capacity(DEFAULT_OUTPUT_BUFFER_SIZE, File::create(&path).unwrap()),
            Compression::new(level),
        );

        FastaWriter {
            writer: Box::new(BufWriter::with_capacity(
                DEFAULT_OUTPUT_BUFFER_SIZE,
                compress_stream,
            )),
            path: path.as_ref().to_path_buf(),
            _phantom: PhantomData,
        }
    }

    pub fn new_compressed_lz4(path: impl AsRef<Path>, level: u32) -> Self {
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
            writer: Box::new(BufWriter::with_capacity(
                DEFAULT_OUTPUT_BUFFER_SIZE,
                compress_stream,
            )),
            path: path.as_ref().to_path_buf(),
            _phantom: PhantomData,
        }
    }

    pub fn new_plain(path: impl AsRef<Path>) -> Self {
        FastaWriter {
            writer: Box::new(BufWriter::with_capacity(
                DEFAULT_OUTPUT_BUFFER_SIZE,
                File::create(&path).unwrap(),
            )),
            path: path.as_ref().to_path_buf(),
            _phantom: PhantomData,
        }
    }

    #[allow(dead_code)]
    pub fn get_path(&self) -> PathBuf {
        self.path.clone()
    }
}

impl<ColorInfo: IdentSequenceWriter, LinksInfo: IdentSequenceWriter>
    StructuredSequenceBackend<ColorInfo, LinksInfo> for FastaWriter<ColorInfo, LinksInfo>
{
    type SequenceTempBuffer = Vec<u8>;

    fn alloc_temp_buffer() -> Self::SequenceTempBuffer {
        Vec::with_capacity(DEFAULT_PER_CPU_BUFFER_SIZE.as_bytes())
    }

    fn write_sequence(
        buffer: &mut Self::SequenceTempBuffer,
        sequence_index: u64,
        sequence: &[u8],

        color_info: ColorInfo,
        links_info: LinksInfo,
        extra_buffers: &(ColorInfo::TempBuffer, LinksInfo::TempBuffer),
    ) {
        write!(buffer, ">{} LN:i:{}", sequence_index, sequence.len()).unwrap();
        color_info.write_as_ident(buffer, &extra_buffers.0);
        links_info.write_as_ident(buffer, &extra_buffers.1);
        buffer.extend_from_slice(b"\n");
        buffer.extend_from_slice(sequence);
        buffer.extend_from_slice(b"\n");
    }

    fn flush_temp_buffer(&mut self, buffer: &mut Self::SequenceTempBuffer) {
        self.writer.write_all(buffer).unwrap();
        buffer.clear();
    }

    fn finalize(self) {}
}

impl<ColorInfo: IdentSequenceWriter, LinksInfo: IdentSequenceWriter> Drop
    for FastaWriter<ColorInfo, LinksInfo>
{
    fn drop(&mut self) {
        self.writer.flush().unwrap();
    }
}
