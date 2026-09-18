//! Decompressed bytes, straight from a file or a stream.
//!
//! This is the input side of the vectorized parsers: they classify whole
//! 64-byte blocks, so they want the decompressed bytes as they come, without
//! being cut into lines first.

use anyhow::{Context, Result, anyhow};
use config::DEFAULT_OUTPUT_BUFFER_SIZE;
use parallel_processor::mt_debug_counters::counter::{AtomicCounter, AvgMode, SumMode};
use parallel_processor::mt_debug_counters::{declare_avg_counter_i64, declare_counter_i64};
use std::fs::File;
use std::io::Read;
use std::path::Path;
use streaming_libdeflate_rs::decompress_file_buffered_callback;

static COUNTER_THREADS_BUSY_READING: AtomicCounter<SumMode> =
    declare_counter_i64!("line_reading_threads", SumMode, false);

static COUNTER_THREADS_PROCESSING_READS: AtomicCounter<SumMode> =
    declare_counter_i64!("line_processing_threads", SumMode, false);

static COUNTER_THREADS_READ_BYTES: AtomicCounter<SumMode> =
    declare_counter_i64!("line_read_bytes", SumMode, false);
static COUNTER_THREADS_READ_BYTES_AVG: AtomicCounter<AvgMode> =
    declare_avg_counter_i64!("line_read_bytes_avg", false);

/// Opens an input file, keeping the historical failure message.
pub(crate) fn open_input(path: &Path) -> File {
    File::open(path).unwrap_or_else(|_| panic!("Cannot open file {}", path.display()))
}

/// The compression a file name implies.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(crate) enum Codec {
    /// Read through a dedicated path-based decompressor, not as a stream.
    Gzip,
    Lz4,
    Bzip2,
    Xz,
    Zstd,
    Plain,
}

pub(crate) fn codec_of(path: &Path) -> Codec {
    match path.extension().and_then(|value| value.to_str()) {
        Some("gz") => Codec::Gzip,
        Some("lz4") => Codec::Lz4,
        Some("bz2") => Codec::Bzip2,
        Some("xz") => Codec::Xz,
        Some("zst") | Some("zstd") => Codec::Zstd,
        _ => Codec::Plain,
    }
}

/// Wraps `reader` in the requested decoder.
pub(crate) fn wrap_decoder<'a>(codec: Codec, reader: impl Read + 'a) -> Result<Box<dyn Read + 'a>> {
    Ok(match codec {
        Codec::Gzip => unreachable!("gzip files are read through their own decompressor"),
        Codec::Lz4 => Box::new(lz4::Decoder::new(reader)?),
        Codec::Bzip2 => Box::new(bzip2::read::BzDecoder::new(reader)),
        Codec::Xz => Box::new(liblzma::read::XzDecoder::new(reader)),
        Codec::Zstd => Box::new(zstd::stream::read::Decoder::new(reader)?),
        Codec::Plain => Box::new(reader),
    })
}

pub struct RawBytesReader {
    buffer: Vec<u8>,
}

impl Default for RawBytesReader {
    fn default() -> Self {
        Self::new()
    }
}

impl RawBytesReader {
    pub fn new() -> Self {
        Self {
            buffer: vec![0; DEFAULT_OUTPUT_BUFFER_SIZE],
        }
    }

    /// Reads `stream` to its end, handing out the bytes in whole buffers.
    pub fn read_stream(
        &mut self,
        stream: &mut dyn Read,
        mut callback: impl FnMut(&[u8]),
    ) -> Result<()> {
        COUNTER_THREADS_BUSY_READING.inc();
        loop {
            let count = match stream.read(self.buffer.as_mut_slice()) {
                Ok(count) => count,
                Err(error) if error.kind() == std::io::ErrorKind::Interrupted => continue,
                Err(error) => {
                    COUNTER_THREADS_BUSY_READING.sub(1);
                    return Err(error.into());
                }
            };
            COUNTER_THREADS_READ_BYTES.inc_by(count as i64);
            COUNTER_THREADS_READ_BYTES_AVG.add_value(count as i64);
            COUNTER_THREADS_BUSY_READING.sub(1);
            if count == 0 {
                return Ok(());
            }
            COUNTER_THREADS_PROCESSING_READS.inc();
            callback(&self.buffer[0..count]);
            COUNTER_THREADS_PROCESSING_READS.sub(1);
            COUNTER_THREADS_BUSY_READING.inc();
        }
    }

    /// Reads and decompresses a whole file, by the codec its name implies.
    pub fn read_file(&mut self, path: &Path, mut callback: impl FnMut(&[u8])) -> Result<()> {
        let codec = codec_of(path);
        if codec == Codec::Gzip {
            decompress_file_buffered_callback(
                path,
                |data| {
                    callback(data);
                    Ok(())
                },
                DEFAULT_OUTPUT_BUFFER_SIZE,
            )
            .map_err(|error| anyhow!("{error:?}"))
            .with_context(|| format!("Cannot decompress {}", path.display()))?;
            return Ok(());
        }
        let mut reader = wrap_decoder(codec, open_input(path))
            .with_context(|| format!("Cannot decompress {}", path.display()))?;
        self.read_stream(&mut reader, callback)
    }
}
