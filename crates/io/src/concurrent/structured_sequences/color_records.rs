use crate::concurrent::structured_sequences::{
    IdentSequenceWriter, StructuredSequenceBackend, StructuredSequenceBackendInit,
    StructuredSequenceBackendWrapper,
};
use crate::concurrent::temp_reads::extra_data::{SequenceExtraData, SequenceExtraDataConsecutiveCompression};
use config::{ColorIndexType, DEFAULT_OUTPUT_BUFFER_SIZE, DEFAULT_PER_CPU_BUFFER_SIZE};
use dynamic_dispatch::dynamic_dispatch;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::marker::PhantomData;
use std::path::{Path, PathBuf};

pub struct ColorRecordsWriterWrapper;

#[dynamic_dispatch]
impl StructuredSequenceBackendWrapper for ColorRecordsWriterWrapper {
    type Backend<
        ColorInfo: IdentSequenceWriter + SequenceExtraDataConsecutiveCompression,
        LinksInfo: IdentSequenceWriter + SequenceExtraData,
    > = ColorRecordsWriter<ColorInfo, LinksInfo>;
}

pub struct ColorRecordsWriter<ColorInfo: IdentSequenceWriter, LinksInfo: IdentSequenceWriter> {
    writer: BufWriter<File>,
    path: PathBuf,
    _phantom: PhantomData<(ColorInfo, LinksInfo)>,
}

pub struct ColorRecordsTempBuffer {
    data: Vec<u8>,
    ident: Vec<u8>,
    subsets: Vec<ColorIndexType>,
}

fn parse_hex_u32(bytes: &[u8]) -> Option<u32> {
    if bytes.is_empty() {
        return None;
    }

    let mut value = 0u32;
    for b in bytes {
        let nibble = match *b {
            b'0'..=b'9' => b - b'0',
            b'a'..=b'f' => 10 + (b - b'a'),
            b'A'..=b'F' => 10 + (b - b'A'),
            _ => return None,
        };
        value = value.checked_mul(16)?.checked_add(nibble as u32)?;
    }
    Some(value)
}

fn parse_color_subsets_from_ident(ident: &[u8], out: &mut Vec<ColorIndexType>) {
    out.clear();

    for token in ident.split(|b| *b == b' ') {
        if token.len() < 4 || token[0] != b'C' || token[1] != b':' {
            continue;
        }

        let Some(color_sep) = token[2..].iter().position(|b| *b == b':') else {
            continue;
        };
        let color_hex = &token[2..(2 + color_sep)];
        if let Some(color_subset) = parse_hex_u32(color_hex) {
            out.push(color_subset as ColorIndexType);
        }
    }

    out.sort_unstable();
    out.dedup();
}

impl<ColorInfo: IdentSequenceWriter, LinksInfo: IdentSequenceWriter> StructuredSequenceBackendInit
    for ColorRecordsWriter<ColorInfo, LinksInfo>
{
    fn new_compressed_gzip(path: impl AsRef<Path>, _level: u32) -> Self {
        Self::new_plain(path)
    }

    fn new_compressed_lz4(path: impl AsRef<Path>, _level: u32) -> Self {
        Self::new_plain(path)
    }

    fn new_compressed_zstd(path: impl AsRef<Path>, _level: u32) -> Self {
        Self::new_plain(path)
    }

    fn new_plain(path: impl AsRef<Path>) -> Self {
        Self {
            writer: BufWriter::with_capacity(
                DEFAULT_OUTPUT_BUFFER_SIZE,
                File::create(&path).unwrap(),
            ),
            path: path.as_ref().to_path_buf(),
            _phantom: PhantomData,
        }
    }
}

impl<ColorInfo: IdentSequenceWriter, LinksInfo: IdentSequenceWriter>
    StructuredSequenceBackend<ColorInfo, LinksInfo> for ColorRecordsWriter<ColorInfo, LinksInfo>
{
    type SequenceTempBuffer = ColorRecordsTempBuffer;

    fn alloc_temp_buffer(_: usize) -> Self::SequenceTempBuffer {
        Self::SequenceTempBuffer {
            data: Vec::with_capacity(DEFAULT_PER_CPU_BUFFER_SIZE.as_bytes()),
            ident: Vec::with_capacity(256),
            subsets: Vec::with_capacity(8),
        }
    }

    fn write_sequence(
        _k: usize,
        buffer: &mut Self::SequenceTempBuffer,
        _sequence_index: u64,
        sequence: &[u8],

        color_info: ColorInfo,
        _links_info: LinksInfo,
        extra_buffers: &(ColorInfo::TempBuffer, LinksInfo::TempBuffer),

        #[cfg(feature = "support_kmer_counters")] _abundance: super::SequenceAbundance,
    ) {
        buffer.ident.clear();
        color_info.write_as_ident(&mut buffer.ident, &extra_buffers.0);
        parse_color_subsets_from_ident(&buffer.ident, &mut buffer.subsets);

        let subset_count = buffer.subsets.len() as u32;
        let seq_len = sequence.len() as u32;

        buffer.data.extend_from_slice(&subset_count.to_le_bytes());
        buffer.data.extend_from_slice(&seq_len.to_le_bytes());
        for subset in buffer.subsets.iter().copied() {
            buffer.data.extend_from_slice(&subset.to_le_bytes());
        }
        buffer.data.extend_from_slice(sequence);
    }

    fn get_path(&self) -> PathBuf {
        self.path.clone()
    }

    fn flush_temp_buffer(&mut self, buffer: &mut Self::SequenceTempBuffer) {
        self.writer.write_all(&buffer.data).unwrap();
        buffer.data.clear();
        buffer.ident.clear();
        buffer.subsets.clear();
    }

    fn finalize(mut self) {
        self.writer.flush().unwrap();
    }
}
