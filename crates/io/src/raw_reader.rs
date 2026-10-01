//! Decompressed bytes, straight from a file or a stream.
//!
//! This is the input side of the vectorized parsers: they classify whole
//! 64-byte blocks, so they want the decompressed bytes as they come, without
//! being cut into lines first.

use crate::sequences_sink::SequencesSink;
use anyhow::{Context, Result, anyhow};
use config::DEFAULT_OUTPUT_BUFFER_SIZE;
use parallel_processor::mt_debug_counters::counter::{AtomicCounter, AvgMode, SumMode};
use parallel_processor::mt_debug_counters::{declare_avg_counter_i64, declare_counter_i64};
use std::fs::File;
use std::io::Read;
use std::path::Path;
use streaming_libdeflate_rs::decompress_file_buffered_callback;

/// Reads compressed inputs through the LZ-tracking decoder of `lz_copyback`, which hands
/// the decompressed bytes out one window at a time together with the copies of that
/// window, at both ends. The parser uses the forward ones to record what kind of data
/// each repeated range holds, and the backward ones to skip the DNA it has already seen.
///
/// Off by default: the plain decoders are used and nothing changes.
pub const LZ_COPYBACK_ENABLED: bool = true;

/// Whether the parser actually drops the repeated bases. With this off and
/// [`LZ_COPYBACK_ENABLED`] on, everything is tracked and reported but the output is
/// unchanged, which is the configuration the equivalence tests use.
///
/// With it, the input given to the parser changes: a skipped stretch is deleted, so the
/// bases around the hole fuse. Additional copyback range info are provided to correctly handle this.
pub const LZ_COPYBACK_SKIP: bool = true;

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

    /// The tracker configuration the whole extension uses. `min_len` comes from the
    /// sink, which is the only thing that knows `k` and `m`.
    pub fn copyback_config(min_len: u64) -> lz_copyback::TrackerConfig {
        lz_copyback::TrackerConfig {
            direction: lz_copyback::Direction::Both,
            min_len: min_len.max(1),
            ..lz_copyback::TrackerConfig::default()
        }
    }

    /// Reads a whole file through the tracking decoder into `sink`, window by window,
    /// with the copies of each window at both ends.
    ///
    /// A loose file is one FASTA stream from end to end, so there is nothing opaque in
    /// it and the windows cover it contiguously.
    pub fn read_file_tracked_into(
        &mut self,
        path: &Path,
        sink: &mut impl SequencesSink,
    ) -> Result<()> {
        let mut decoder = lz_copyback::open(path, Self::copyback_config(sink.copyback_min_len()))
            .with_context(|| format!("Cannot open {}", path.display()))?;
        // A copy reaches back by up to the stream's maximum distance, from anywhere in
        // the window being emitted.
        let retain = decoder
            .max_distance()
            .saturating_add(2 * decoder.window_size() as u64);
        sink.begin_tracked_input(path, retain);
        loop {
            let Some(window) = decoder
                .next_window()
                .with_context(|| format!("Cannot decompress {}", path.display()))?
            else {
                break;
            };
            COUNTER_THREADS_READ_BYTES.inc_by(window.data.len() as i64);
            sink.push_bytes_tracked(
                window.start_abs,
                window.data,
                window.src_copies(),
                window.dst_copies(),
            );
            if window.is_last {
                break;
            }
        }
        Ok(())
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
