//! Decompressed bytes, straight from a file or a stream.
//!
//! A [`BinaryDecoder`] writes its output into a [`DecodedOutput`] one buffer at a time, with
//! some extra data of its own attached to each buffer. A [`GenericBinaryReader`] hands both out
//! as [`BinaryEvent`]s, in stream order: the bytes either in the buffers the decoder produced
//! or, with a buffer size, in chunks of exactly that size, each aligned to it.
//!
//! A tar archive is read as it is decompressed, never loaded whole: each member file is
//! decoded on its own, by the codec its name implies, as if it were a file of its own.

use anyhow::{Context, Result, anyhow};
use config::DEFAULT_OUTPUT_BUFFER_SIZE;
use std::fs::File;
use std::io::{BufRead, ErrorKind, Read};
use std::path::Path;
use streaming_libdeflate_rs::{
    LibdeflateDecompressResult, LibdeflateGzipFileDecompressor, LibdeflateGzipPushDecompressor,
};
use tar::EntryType;

/// Opens an input file, keeping the historical failure message.
pub(crate) fn open_input(path: &Path) -> File {
    File::open(path).unwrap_or_else(|_| panic!("Cannot open file {}", path.display()))
}

/// The compression a file name implies.
#[derive(Copy, Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub(crate) enum Codec {
    /// Read through a dedicated path-based decompressor, not as a stream.
    Gzip,
    Lz4,
    Bzip2,
    Xz,
    Zstd,
    Plain,
}

/// The short names of compressed tar archives: GNU tar's, plus `tb2` and `tlz4`.
const COMPRESSED_ARCHIVE_EXTENSIONS: &[&str] = &[
    "tgz", "taz", "tbz", "tbz2", "tb2", "tz2", "txz", "tzst", "tlz4",
];

pub(crate) fn codec_of(path: &Path) -> Codec {
    match path.extension().and_then(|value| value.to_str()) {
        Some("gz" | "tgz" | "taz") => Codec::Gzip,
        Some("lz4" | "tlz4") => Codec::Lz4,
        Some("bz2" | "tbz" | "tbz2" | "tb2" | "tz2") => Codec::Bzip2,
        Some("xz" | "txz") => Codec::Xz,
        Some("zst" | "zstd" | "tzst") => Codec::Zstd,
        _ => Codec::Plain,
    }
}

/// Whether `path` names a tar archive, compressed or not: `x.tar`, `x.tar.gz`, `x.tgz`, ...
pub(crate) fn is_archive(path: &Path) -> bool {
    let Some(extension) = path.extension().and_then(|value| value.to_str()) else {
        return false;
    };
    if extension == "tar" || COMPRESSED_ARCHIVE_EXTENSIONS.contains(&extension) {
        return true;
    }
    codec_of(path) != Codec::Plain
        && Path::new(path.file_stem().unwrap_or_default())
            .extension()
            .is_some_and(|extension| extension == "tar")
}

/// `source` decompressed by `codec`, as a reader. Every decoder but lz4's reads on past the end
/// of a stream into the next one, as concatenated files and parallel compressors have them.
fn decompressed<'a>(codec: Codec, source: impl Read + 'a) -> Result<Box<dyn Read + 'a>> {
    Ok(match codec {
        Codec::Gzip => Box::new(GzipDecoder::new(source)),
        Codec::Lz4 => Box::new(Lz4Decoder::new(source)?),
        Codec::Bzip2 => Box::new(bzip2::read::MultiBzDecoder::new(source)),
        Codec::Xz => Box::new(liblzma::read::XzDecoder::new_multi_decoder(source)),
        Codec::Zstd => Box::new(zstd::stream::read::Decoder::new(source)?),
        Codec::Plain => Box::new(source),
    })
}

/// lz4's decoder, with an input that ends inside its frame made an error. The decoder itself
/// just stops returning bytes then, and only tells it when finishing.
pub struct Lz4Decoder<R: Read>(Option<lz4::Decoder<R>>);

impl<R: Read> Lz4Decoder<R> {
    pub fn new(source: R) -> std::io::Result<Self> {
        Ok(Self(Some(lz4::Decoder::new(source)?)))
    }
}

impl<R: Read> Read for Lz4Decoder<R> {
    fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
        let Some(decoder) = self.0.as_mut().filter(|_| !buffer.is_empty()) else {
            return Ok(0);
        };
        let count = decoder.read(buffer)?;
        if count == 0 {
            // Not lz4's own error: it is `Interrupted`, which callers retry, and the retry
            // would then find the decoder finished and report a clean end.
            if self.0.take().unwrap().finish().1.is_err() {
                return Err(std::io::Error::new(
                    ErrorKind::UnexpectedEof,
                    "lz4: the input ends inside a frame",
                ));
            }
        }
        Ok(count)
    }
}

/// A decompressor, writing its output into a [`DecodedOutput`] one buffer at a time, with some
/// extra data attached to each buffer.
pub trait BinaryDecoder {
    /// The extra data of an output buffer. It can borrow from the decoder, for the length of
    /// the call that hands it over.
    type ExtraData<'a>;

    /// Decodes the input to its end.
    fn decode(&mut self, output: &mut impl DecodedOutput<Self>) -> Result<()>;
}

/// Where a [`BinaryDecoder`] writes. A decoder that can write into a buffer it is given fills
/// [`Self::free_space`] and calls [`Self::commit`]; one that writes into a buffer of its own
/// hands its output over with [`Self::push`].
pub trait DecodedOutput<D: BinaryDecoder + ?Sized> {
    /// Where the next bytes can be written. Never empty, so a read into it returning 0 means
    /// the end of the input.
    fn free_space(&mut self) -> &mut [u8];

    /// Takes the `count` bytes written at the start of [`Self::free_space`].
    fn commit(&mut self, count: usize, extra: D::ExtraData<'_>);

    /// Takes `data`, the next bytes of the stream.
    fn push(&mut self, data: &[u8], extra: D::ExtraData<'_>);
}

/// Decodes `reader` straight into the output's free space, with no extra data.
fn decode_read<D>(
    reader: &mut (impl Read + ?Sized),
    output: &mut impl DecodedOutput<D>,
) -> Result<()>
where
    D: for<'a> BinaryDecoder<ExtraData<'a> = ()> + ?Sized,
{
    loop {
        match reader.read(output.free_space()) {
            Ok(0) => return Ok(()),
            Ok(count) => output.commit(count, ()),
            Err(error) if error.kind() == ErrorKind::Interrupted => {}
            Err(error) => return Err(error.into()),
        }
    }
}

/// The decoders that write into a buffer they are given, with no extra data.
macro_rules! read_decoders {
    ($(impl[$($generics:tt)*] for $decoder:ty;)*) => {$(
        impl<$($generics)*> BinaryDecoder for $decoder {
            type ExtraData<'a> = ();

            fn decode(&mut self, output: &mut impl DecodedOutput<Self>) -> Result<()> {
                decode_read(self, output)
            }
        }
    )*};
}

read_decoders! {
    impl[] for File;
    impl['r] for dyn Read + 'r;
    impl[R: Read] for Lz4Decoder<R>;
    impl[R: Read] for bzip2::read::MultiBzDecoder<R>;
    impl[R: Read] for liblzma::read::XzDecoder<R>;
    impl[R: BufRead] for zstd::stream::read::Decoder<'_, R>;
}

/// Gzip's decompressor writes into a buffer of its own, which it also needs as history, so its
/// output is pushed rather than written in place.
impl BinaryDecoder for LibdeflateGzipFileDecompressor {
    type ExtraData<'a> = ();

    fn decode(&mut self, output: &mut impl DecodedOutput<Self>) -> Result<()> {
        loop {
            match self.decompress().map_err(|error| anyhow!("{error:?}"))? {
                LibdeflateDecompressResult::MoreData => {
                    let pending = self.pending_output();
                    let count = pending.len();
                    if count > 0 {
                        output.push(pending, ());
                    }
                    self.consume_output(count);
                }
                LibdeflateDecompressResult::NeedInput => {
                    unreachable!("file decompressors never run out of input")
                }
                LibdeflateDecompressResult::EndOfStream => return Ok(()),
            }
        }
    }
}

/// Gzip from any reader, through the push decompressor: the compressed bytes are read straight
/// into its input buffer, and its output is handed over from its own buffer.
pub struct GzipDecoder<R> {
    source: R,
    decompressor: LibdeflateGzipPushDecompressor,
    /// How much of the decompressor's current output buffer [`Read::read`] already handed out.
    pending_read: usize,
}

impl<R: Read> GzipDecoder<R> {
    pub fn new(source: R) -> Self {
        Self {
            source,
            decompressor: LibdeflateGzipPushDecompressor::new(DEFAULT_OUTPUT_BUFFER_SIZE),
            pending_read: 0,
        }
    }

    /// Decodes until there is pending output, returning false at the end of the stream.
    fn next_output(&mut self) -> std::io::Result<bool> {
        loop {
            let result = self.decompressor.decompress().map_err(|error| {
                std::io::Error::new(ErrorKind::InvalidData, format!("gzip: {error:?}"))
            })?;
            match result {
                LibdeflateDecompressResult::EndOfStream => return Ok(false),
                _ if !self.decompressor.pending_output().is_empty() => return Ok(true),
                LibdeflateDecompressResult::NeedInput => self.read_input()?,
                LibdeflateDecompressResult::MoreData => {}
            }
        }
    }

    /// Reads more compressed bytes into the decompressor, or marks the end of the input.
    fn read_input(&mut self) -> std::io::Result<()> {
        let space = self.decompressor.input_space_mut();
        if space.is_empty() {
            return Err(std::io::Error::other("gzip: the input buffer is full"));
        }
        let count = loop {
            match self.source.read(space) {
                Ok(count) => break count,
                Err(error) if error.kind() == ErrorKind::Interrupted => {}
                Err(error) => return Err(error),
            }
        };
        if count == 0 {
            self.decompressor.finish_input();
        } else {
            self.decompressor.commit_input(count);
        }
        Ok(())
    }
}

impl<R: Read> BinaryDecoder for GzipDecoder<R> {
    type ExtraData<'a> = ();

    fn decode(&mut self, output: &mut impl DecodedOutput<Self>) -> Result<()> {
        while self.next_output()? {
            let pending = self.decompressor.pending_output();
            let count = pending.len();
            output.push(pending, ());
            self.decompressor.consume_output(count);
        }
        Ok(())
    }
}

impl<R: Read> Read for GzipDecoder<R> {
    fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
        if self.pending_read == self.decompressor.pending_output().len() {
            // The decompressor's output buffer is released only once all of it was read:
            // releasing part of it would make the decompressor move the rest back to the
            // front of its buffer at every read.
            self.decompressor.consume_output(self.pending_read);
            self.pending_read = 0;
            if !self.next_output()? {
                return Ok(0);
            }
        }
        let pending = &self.decompressor.pending_output()[self.pending_read..];
        let count = pending.len().min(buffer.len());
        buffer[..count].copy_from_slice(&pending[..count]);
        self.pending_read += count;
        Ok(count)
    }
}

/// What a [`GenericBinaryReader`] hands out, in stream order.
pub enum BinaryEvent<'a, E> {
    /// The file the following events come from, sent first and exactly once per input: the
    /// path as given for a file (converted lossily if it is not UTF-8), `None` for an input
    /// that is not a file. A member of a tar archive is named after the archive and its path
    /// inside it, joined by `:`, as `/data/x.tar.gz:dir/a.fa`, with one more for each archive
    /// inside an archive, as `/data/x.tar:inner.tgz:a.fa`.
    FilePath(Option<&'a str>),
    /// Decoded bytes, never empty. With a buffer size, exactly that many and aligned to it,
    /// except for the last chunk of an input, which can be shorter.
    Chunk(&'a [u8]),
    /// The extra data the decoder attached to its buffer starting at stream offset `start`.
    /// It comes after every chunk ending at or before `start`, and before every chunk holding
    /// bytes of that buffer.
    Extra { start: u64, extra: E },
}

/// A zeroed heap buffer whose start is aligned to `align`, carved out of a slightly larger
/// allocation. The slack in front is never written, so it is never backed by memory.
struct AlignedBuffer {
    storage: Vec<u8>,
    start: usize,
    len: usize,
}

impl AlignedBuffer {
    fn new(len: usize, align: usize) -> Self {
        let storage = vec![0; len + align - 1];
        let address = storage.as_ptr() as usize;
        Self {
            start: address.next_multiple_of(align) - address,
            storage,
            len,
        }
    }

    fn as_mut_slice(&mut self) -> &mut [u8] {
        &mut self.storage[self.start..self.start + self.len]
    }
}

/// Hands out the bytes written into a buffer: without a chunk size, everything as soon as it
/// is written; with one, each chunk of exactly that size as soon as it is complete.
///
/// The buffer is a whole number of chunks and is always filled up to its end before starting
/// over, so a chunk never wraps around and nothing is ever moved back to the start.
struct ChunkRing<'a> {
    buffer: &'a mut [u8],
    chunk_size: Option<usize>,
    /// Start of the first chunk not handed out yet.
    emitted: usize,
    /// End of the bytes written so far.
    filled: usize,
}

impl<'a> ChunkRing<'a> {
    fn new(buffer: &'a mut [u8], chunk_size: Option<usize>) -> Self {
        Self {
            buffer,
            chunk_size,
            emitted: 0,
            filled: 0,
        }
    }

    /// Where the next bytes go: never empty.
    fn free_space(&mut self) -> &mut [u8] {
        &mut self.buffer[self.filled..]
    }

    /// Takes `count` bytes written at the start of [`Self::free_space`].
    fn commit(&mut self, count: usize, callback: &mut impl FnMut(&[u8])) {
        self.filled += count;
        match self.chunk_size {
            None => {
                callback(&self.buffer[..self.filled]);
                self.filled = 0;
            }
            Some(chunk_size) => {
                while self.filled - self.emitted >= chunk_size {
                    callback(&self.buffer[self.emitted..self.emitted + chunk_size]);
                    self.emitted += chunk_size;
                }
                // The buffer is a whole number of chunks: once full, all of it went out.
                if self.filled == self.buffer.len() {
                    self.filled = 0;
                    self.emitted = 0;
                }
            }
        }
    }

    /// Takes `data` from a buffer of the decoder's own: without a chunk size it goes out as it
    /// is, with one it is copied in.
    fn push(&mut self, mut data: &[u8], callback: &mut impl FnMut(&[u8])) {
        if self.chunk_size.is_none() {
            if !data.is_empty() {
                callback(data);
            }
            return;
        }
        while !data.is_empty() {
            let space = self.free_space();
            let count = space.len().min(data.len());
            space[..count].copy_from_slice(&data[..count]);
            data = &data[count..];
            self.commit(count, callback);
        }
    }

    /// Hands out the last chunk, shorter than the others.
    fn finish(&mut self, callback: &mut impl FnMut(&[u8])) {
        if self.filled > self.emitted {
            callback(&self.buffer[self.emitted..self.filled]);
        }
        self.filled = 0;
        self.emitted = 0;
    }
}

/// Turns a decoder's output into events: the extra data right away, the bytes through a
/// [`ChunkRing`].
struct EventSink<'a, F> {
    ring: ChunkRing<'a>,
    /// Stream offset of the next byte the decoder hands over.
    position: u64,
    callback: F,
}

impl<D, F> DecodedOutput<D> for EventSink<'_, F>
where
    D: BinaryDecoder + ?Sized,
    F: FnMut(BinaryEvent<'_, D::ExtraData<'_>>),
{
    fn free_space(&mut self) -> &mut [u8] {
        self.ring.free_space()
    }

    fn commit(&mut self, count: usize, extra: D::ExtraData<'_>) {
        (self.callback)(BinaryEvent::Extra {
            start: self.position,
            extra,
        });
        self.position += count as u64;
        let callback = &mut self.callback;
        self.ring
            .commit(count, &mut |chunk| callback(BinaryEvent::Chunk(chunk)));
    }

    fn push(&mut self, data: &[u8], extra: D::ExtraData<'_>) {
        (self.callback)(BinaryEvent::Extra {
            start: self.position,
            extra,
        });
        self.position += data.len() as u64;
        let callback = &mut self.callback;
        self.ring
            .push(data, &mut |chunk| callback(BinaryEvent::Chunk(chunk)));
    }
}

pub struct GenericBinaryReader {
    buffer: AlignedBuffer,
    buffer_size: Option<usize>,
}

impl GenericBinaryReader {
    /// With `buffer_size`, which must be a power of two, every chunk handed out is exactly
    /// that many bytes and aligned to it, except the last one of each input, which can be
    /// shorter. Without it, chunks are the buffers the decoder produces.
    pub fn new(buffer_size: Option<usize>) -> Self {
        let (capacity, align) = match buffer_size {
            None => (DEFAULT_OUTPUT_BUFFER_SIZE, 1),
            Some(size) => {
                assert!(
                    size.is_power_of_two(),
                    "The buffer size must be a power of two, got {size}"
                );
                (DEFAULT_OUTPUT_BUFFER_SIZE.next_multiple_of(size), size)
            }
        };
        Self {
            buffer: AlignedBuffer::new(capacity, align),
            buffer_size,
        }
    }

    /// Runs `decoder` to the end of its input, as an input that is not a file. The bytes
    /// decoded before an error are still handed out.
    pub fn read_decoder<D: BinaryDecoder + ?Sized>(
        &mut self,
        decoder: &mut D,
        mut callback: impl FnMut(BinaryEvent<'_, D::ExtraData<'_>>),
    ) -> Result<()> {
        callback(BinaryEvent::FilePath(None));
        self.decode(decoder, callback)
    }

    fn decode<D: BinaryDecoder + ?Sized>(
        &mut self,
        decoder: &mut D,
        callback: impl FnMut(BinaryEvent<'_, D::ExtraData<'_>>),
    ) -> Result<()> {
        let mut sink = EventSink {
            ring: ChunkRing::new(self.buffer.as_mut_slice(), self.buffer_size),
            position: 0,
            callback,
        };
        let result = decoder.decode(&mut sink);
        let EventSink {
            mut ring,
            mut callback,
            ..
        } = sink;
        ring.finish(&mut |chunk| callback(BinaryEvent::Chunk(chunk)));
        result
    }

    /// Reads `stream` to its end.
    pub fn read_stream(
        &mut self,
        stream: &mut dyn Read,
        callback: impl FnMut(BinaryEvent<'_, ()>),
    ) -> Result<()> {
        self.read_decoder(stream, callback)
    }

    /// Reads and decompresses a whole file, by the codec its name implies. A tar archive gets
    /// no [`BinaryEvent::FilePath`] of its own: each member file is read as a file, named
    /// after the archive and its path inside it, and archives inside it are read the same way.
    pub fn read_file(
        &mut self,
        path: &Path,
        mut callback: impl FnMut(BinaryEvent<'_, ()>),
    ) -> Result<()> {
        let callback = &mut callback;
        let name = path.to_string_lossy();
        let result = if codec_of(path) == Codec::Gzip && !is_archive(path) {
            // A gzip file keeps the decompressor that reads the file by itself.
            callback(BinaryEvent::FilePath(Some(&name)));
            let mut decoder =
                LibdeflateGzipFileDecompressor::try_new(path, DEFAULT_OUTPUT_BUFFER_SIZE)
                    .unwrap_or_else(|_| panic!("Cannot open file {}", path.display()));
            self.decode(&mut decoder, callback)
        } else {
            self.read_input(&name, path, open_input(path), callback)
        };
        result.with_context(|| format!("Cannot decompress {}", path.display()))
    }

    /// Reads the input `name`, whose own path is `path`, from `source`: an archive member by
    /// member, anything else as a single file.
    fn read_input<F: FnMut(BinaryEvent<'_, ()>)>(
        &mut self,
        name: &str,
        path: &Path,
        mut source: impl Read,
        callback: &mut F,
    ) -> Result<()> {
        let codec = codec_of(path);
        if is_archive(path) {
            return self.read_archive(name, &mut *decompressed(codec, source)?, callback);
        }
        // Sent before the decoder starts, so that a file failing there still gets one.
        callback(BinaryEvent::FilePath(Some(name)));
        match codec {
            Codec::Gzip => self.decode(&mut GzipDecoder::new(source), callback),
            Codec::Lz4 => self.decode(&mut Lz4Decoder::new(source)?, callback),
            Codec::Bzip2 => self.decode(&mut bzip2::read::MultiBzDecoder::new(source), callback),
            Codec::Xz => self.decode(
                &mut liblzma::read::XzDecoder::new_multi_decoder(source),
                callback,
            ),
            Codec::Zstd => self.decode(&mut zstd::stream::read::Decoder::new(source)?, callback),
            Codec::Plain => self.decode(&mut source as &mut dyn Read, callback),
        }
    }

    /// Reads the tar archive `name` as it comes out of `source`, decoding each member file on
    /// its own as `name:path`. Directories, links and other special entries are skipped.
    fn read_archive<F: FnMut(BinaryEvent<'_, ()>)>(
        &mut self,
        name: &str,
        source: &mut dyn Read,
        callback: &mut F,
    ) -> Result<()> {
        let mut archive = tar::Archive::new(source);
        for entry in archive.entries()? {
            let mut entry = entry?;
            let path = entry.path()?.into_owned();
            let member = format!("{name}:{}", path.to_string_lossy());
            let is_file = matches!(
                entry.header().entry_type(),
                EntryType::Regular | EntryType::Continuous | EntryType::GNUSparse
            );
            if !is_file {
                ggcat_logging::info!("Skipping {member}: not a regular file");
                continue;
            }
            self.read_input(&member, &path, &mut entry, callback)
                .with_context(|| format!("Cannot read archive member {member}"))?;
        }
        // The tar end marker is not the end of the stream: reading on to it lets the
        // decompressor check what follows, its checksums included.
        std::io::copy(&mut archive.into_inner(), &mut std::io::sink())
            .with_context(|| format!("Cannot read the end of archive {name}"))?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    /// Pseudo-random DNA lines, compressible like real inputs.
    fn sample(len: usize) -> Vec<u8> {
        let mut state = 0x2545_f491_4f6c_dd1du64;
        (0..len)
            .map(|index| {
                if index % 81 == 80 {
                    return b'\n';
                }
                state ^= state << 13;
                state ^= state >> 7;
                state ^= state << 17;
                b"ACGT"[(state & 3) as usize]
            })
            .collect()
    }

    /// Returns a few bytes per read, in irregular amounts.
    struct Trickle<'a> {
        data: &'a [u8],
        step: usize,
    }

    impl Read for Trickle<'_> {
        fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
            self.step = (self.step * 7 + 3) % 5003 + 1;
            let count = self.step.min(buffer.len()).min(self.data.len());
            buffer[..count].copy_from_slice(&self.data[..count]);
            self.data = &self.data[count..];
            Ok(count)
        }
    }

    /// The events a reader handed out.
    #[derive(Default)]
    struct Recorded {
        events: usize,
        /// Each file path, with the number of events before it.
        file_paths: Vec<(Option<String>, usize)>,
        /// Each chunk with its address.
        chunks: Vec<(usize, Vec<u8>)>,
        chunk_bytes: usize,
        /// The start of each extra data, its value, and the bytes of the chunks before it.
        extras: Vec<(u64, u64, usize)>,
    }

    impl Recorded {
        fn file_path(&mut self, path: Option<&str>) {
            self.file_paths.push((path.map(str::to_owned), self.events));
            self.events += 1;
        }

        fn chunk(&mut self, chunk: &[u8]) {
            self.chunks.push((chunk.as_ptr() as usize, chunk.to_vec()));
            self.chunk_bytes += chunk.len();
            self.events += 1;
        }

        fn extra(&mut self, start: u64, value: u64) {
            self.extras.push((start, value, self.chunk_bytes));
            self.events += 1;
        }
    }

    /// Records the events of a decoder with no extra data.
    fn recorder(recorded: &mut Recorded) -> impl FnMut(BinaryEvent<'_, ()>) + '_ {
        |event| match event {
            BinaryEvent::FilePath(path) => recorded.file_path(path),
            BinaryEvent::Chunk(chunk) => recorded.chunk(chunk),
            BinaryEvent::Extra { start, .. } => recorded.extra(start, 0),
        }
    }

    fn check(
        recorded: &Recorded,
        expected: &[u8],
        buffer_size: Option<usize>,
        file_path: Option<&str>,
    ) {
        // Exactly one file path, before everything else.
        assert_eq!(recorded.file_paths, [(file_path.map(str::to_owned), 0)]);

        let chunks = &recorded.chunks;
        assert!(chunks.iter().all(|(_, chunk)| !chunk.is_empty()));
        let joined: Vec<u8> = chunks.iter().flat_map(|(_, chunk)| chunk.clone()).collect();
        assert!(joined == expected, "the bytes differ from the input");

        for &(start, _, before) in &recorded.extras {
            let start = start as usize;
            let chunks_before = match buffer_size {
                None => start,
                Some(size) => start - start % size,
            };
            assert_eq!(before, chunks_before, "extra data at {start} out of order");
        }

        let Some(size) = buffer_size else {
            return;
        };
        for (index, (address, chunk)) in chunks.iter().enumerate() {
            assert_eq!(address % size, 0, "chunk {index} is not aligned to {size}");
            if index + 1 < chunks.len() {
                assert_eq!(chunk.len(), size, "chunk {index} is not whole");
            } else {
                assert!(chunk.len() <= size);
            }
        }
    }

    #[test]
    fn streams_split_into_exact_aligned_chunks() {
        for size in [64, 4096, 1 << 20, 2 * DEFAULT_OUTPUT_BUFFER_SIZE] {
            let mut reader = GenericBinaryReader::new(Some(size));
            for len in [
                0,
                1,
                size - 1,
                size,
                size + 1,
                2 * DEFAULT_OUTPUT_BUFFER_SIZE + 4099,
            ] {
                let data = sample(len);
                let mut recorded = Recorded::default();
                let mut stream = Trickle {
                    data: &data,
                    step: len,
                };
                reader
                    .read_stream(&mut stream, recorder(&mut recorded))
                    .unwrap();
                check(&recorded, &data, Some(size), None);
            }
        }
    }

    #[test]
    fn streams_pass_through_without_buffer_size() {
        let data = sample(DEFAULT_OUTPUT_BUFFER_SIZE + 12345);
        let mut recorded = Recorded::default();
        let mut stream = Trickle {
            data: &data,
            step: 0,
        };
        GenericBinaryReader::new(None)
            .read_stream(&mut stream, recorder(&mut recorded))
            .unwrap();
        check(&recorded, &data, None, None);
        // Each read goes out on its own, as it came, after its extra data.
        assert!(recorded.chunks.len() > data.len() / 5003);
        assert_eq!(recorded.extras.len(), recorded.chunks.len());
    }

    #[test]
    #[should_panic(expected = "power of two")]
    fn buffer_size_must_be_a_power_of_two() {
        GenericBinaryReader::new(Some(3000));
    }

    /// Hands `data` over in windows of irregular sizes, alternately pushed from its own memory
    /// and written in place, each with its index borrowed from a table of its own.
    struct Windows {
        data: Vec<u8>,
        bounds: Vec<(usize, usize)>,
        indices: Vec<u64>,
    }

    impl BinaryDecoder for Windows {
        type ExtraData<'a> = &'a u64;

        fn decode(&mut self, output: &mut impl DecodedOutput<Self>) -> Result<()> {
            for (index, &(start, end)) in self.indices.iter().zip(&self.bounds) {
                if index % 2 == 0 {
                    output.push(&self.data[start..end], index);
                    continue;
                }
                let mut written = start;
                while written < end {
                    let space = output.free_space();
                    let count = space.len().min(end - written);
                    space[..count].copy_from_slice(&self.data[written..written + count]);
                    output.commit(count, index);
                    written += count;
                }
            }
            Ok(())
        }
    }

    #[test]
    fn extra_data_comes_in_stream_order() {
        let data = sample(2 * DEFAULT_OUTPUT_BUFFER_SIZE + 777);
        let mut bounds = Vec::new();
        let (mut start, mut step) = (0, 1);
        while start < data.len() {
            step = (step * 31 + 7) % 300_000 + 1;
            let end = (start + step).min(data.len());
            bounds.push((start, end));
            start = end;
        }
        let mut decoder = Windows {
            indices: (0..bounds.len() as u64).collect(),
            data: data.clone(),
            bounds: bounds.clone(),
        };

        for buffer_size in [None, Some(64), Some(1 << 16)] {
            let mut recorded = Recorded::default();
            GenericBinaryReader::new(buffer_size)
                .read_decoder(&mut decoder, |event| match event {
                    BinaryEvent::FilePath(path) => recorded.file_path(path),
                    BinaryEvent::Chunk(chunk) => recorded.chunk(chunk),
                    BinaryEvent::Extra { start, extra } => recorded.extra(start, *extra),
                })
                .unwrap();
            check(&recorded, &data, buffer_size, None);

            let mut seen = vec![false; bounds.len()];
            for &(start, index, _) in &recorded.extras {
                let (begin, end) = bounds[index as usize];
                assert!(begin as u64 <= start && start < end as u64);
                seen[index as usize] = true;
            }
            assert!(seen.iter().all(|seen| *seen), "a window has no extra data");
        }
    }

    fn compress(data: &[u8], extension: &str) -> Vec<u8> {
        match extension {
            "gz" => {
                let mut encoder =
                    flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::fast());
                encoder.write_all(data).unwrap();
                encoder.finish().unwrap()
            }
            "lz4" => {
                let mut encoder = lz4::EncoderBuilder::new().build(Vec::new()).unwrap();
                encoder.write_all(data).unwrap();
                let (bytes, result) = encoder.finish();
                result.unwrap();
                bytes
            }
            "bz2" => {
                let mut encoder =
                    bzip2::write::BzEncoder::new(Vec::new(), bzip2::Compression::fast());
                encoder.write_all(data).unwrap();
                encoder.finish().unwrap()
            }
            "xz" => {
                let mut encoder = liblzma::write::XzEncoder::new(Vec::new(), 1);
                encoder.write_all(data).unwrap();
                encoder.finish().unwrap()
            }
            "zst" | "zstd" => zstd::stream::encode_all(data, 1).unwrap(),
            "fa" => data.to_vec(),
            _ => unreachable!(),
        }
    }

    #[test]
    fn files_decode_every_codec() {
        let directory =
            std::env::temp_dir().join(format!("ggcat-binary-reader-{}", std::process::id()));
        std::fs::create_dir_all(&directory).unwrap();
        let data = sample(DEFAULT_OUTPUT_BUFFER_SIZE + 54321);
        for extension in ["gz", "lz4", "bz2", "xz", "zst", "zstd", "fa"] {
            let path = directory.join(format!("input.{extension}"));
            std::fs::write(&path, compress(&data, extension)).unwrap();
            for buffer_size in [None, Some(4096), Some(DEFAULT_OUTPUT_BUFFER_SIZE)] {
                let mut recorded = Recorded::default();
                GenericBinaryReader::new(buffer_size)
                    .read_file(&path, recorder(&mut recorded))
                    .unwrap();
                check(&recorded, &data, buffer_size, path.to_str());
            }
        }
        std::fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn archives_are_recognized_by_name() {
        for name in [
            "x.tar",
            "x.tar.gz",
            "dir/x.tar.xz",
            "x.fa.tar.zst",
            "x.tar.zstd",
            "x.tgz",
            "x.taz",
            "x.tbz",
            "x.tbz2",
            "x.tb2",
            "x.tz2",
            "x.txz",
            "x.tzst",
            "x.tlz4",
        ] {
            assert!(is_archive(Path::new(name)), "{name}");
        }
        for name in ["x.fa", "x.fa.gz", "tar", "x.tar.fa", "x.gz", "x.tgz.fa"] {
            assert!(!is_archive(Path::new(name)), "{name}");
        }
    }

    #[test]
    fn gzip_decoder_reads_from_small_reads() {
        let first = sample(DEFAULT_OUTPUT_BUFFER_SIZE + 999);
        let second = sample(70_000);
        // Two members, as concatenated gzip files are.
        let mut compressed = compress(&first, "gz");
        compressed.extend(compress(&second, "gz"));
        let expected = [first, second].concat();

        for buffer_size in [None, Some(4096)] {
            let mut recorded = Recorded::default();
            let mut decoder = GzipDecoder::new(Trickle {
                data: &compressed,
                step: 0,
            });
            GenericBinaryReader::new(buffer_size)
                .read_decoder(&mut decoder, recorder(&mut recorded))
                .unwrap();
            check(&recorded, &expected, buffer_size, None);
        }

        let mut decoded = Vec::new();
        GzipDecoder::new(Trickle {
            data: &compressed,
            step: 0,
        })
        .read_to_end(&mut decoded)
        .unwrap();
        assert!(decoded == expected);
    }

    /// `data` split in two and each half compressed on its own, back to back, as concatenating
    /// two compressed files, or pbzip2, produces.
    fn concatenated(data: &[u8], extension: &str) -> Vec<u8> {
        let (first, second) = data.split_at(data.len() / 2);
        [compress(first, extension), compress(second, extension)].concat()
    }

    #[test]
    fn concatenated_streams_are_read_to_the_end() {
        let directory = std::env::temp_dir().join(format!(
            "ggcat-binary-reader-concatenated-{}",
            std::process::id()
        ));
        std::fs::create_dir_all(&directory).unwrap();
        let data = sample(DEFAULT_OUTPUT_BUFFER_SIZE + 54321);
        let member = sample(100_000);
        for extension in ["gz", "bz2", "xz", "zst"] {
            let path = directory.join(format!("input.fa.{extension}"));
            std::fs::write(&path, concatenated(&data, extension)).unwrap();
            let mut recorded = Recorded::default();
            GenericBinaryReader::new(None)
                .read_file(&path, recorder(&mut recorded))
                .unwrap();
            check(&recorded, &data, None, path.to_str());

            // Both the archive and its member made of two streams.
            let name = format!("a.fa.{extension}");
            let tar = archive(&[(name.clone(), concatenated(&member, extension))]);
            let path = directory.join(format!("input.tar.{extension}"));
            std::fs::write(&path, concatenated(&tar, extension)).unwrap();
            let mut files = Vec::new();
            GenericBinaryReader::new(None)
                .read_file(&path, file_recorder(&mut files))
                .unwrap();
            assert_eq!(files.len(), 1, "{extension}");
            let name = format!("{}:{name}", path.display());
            check(&files[0], &member, None, Some(&name));
        }
        std::fs::remove_dir_all(directory).unwrap();
    }

    /// What follows the tar end marker is read too, so a damaged end of the compressed stream
    /// is found: here a wrong gzip checksum, and a broken xz stream footer.
    #[test]
    fn the_end_of_an_archive_is_checked() {
        let directory = std::env::temp_dir().join(format!(
            "ggcat-binary-reader-archive-end-{}",
            std::process::id()
        ));
        std::fs::create_dir_all(&directory).unwrap();
        let tar = archive(&[("a.fa".into(), sample(100_000))]);
        // How far from the end the damaged byte is: the first byte of gzip's CRC-32, the
        // last of xz's footer magic.
        for (extension, from_end) in [("gz", 8), ("xz", 1)] {
            let mut compressed = compress(&tar, extension);
            let at = compressed.len() - from_end;
            compressed[at] ^= 0xff;
            let path = directory.join(format!("input.tar.{extension}"));
            std::fs::write(&path, compressed).unwrap();
            let result = GenericBinaryReader::new(None).read_file(&path, |_| {});
            assert!(result.is_err(), "{extension}: the damage went unnoticed");
        }
        std::fs::remove_dir_all(directory).unwrap();
    }

    /// An lz4 input that stops inside its frame is an error, for a file of its own and for an
    /// archive member alike.
    #[test]
    fn truncated_lz4_is_an_error() {
        let directory = std::env::temp_dir().join(format!(
            "ggcat-binary-reader-truncated-lz4-{}",
            std::process::id()
        ));
        std::fs::create_dir_all(&directory).unwrap();
        let mut truncated = compress(&sample(200_000), "lz4");
        truncated.truncate(truncated.len() - 100);

        let path = directory.join("input.fa.lz4");
        std::fs::write(&path, &truncated).unwrap();
        let result = GenericBinaryReader::new(None).read_file(&path, |_| {});
        assert!(result.is_err(), "a truncated file went unnoticed");

        let path = directory.join("input.tar");
        std::fs::write(&path, archive(&[("a.fa.lz4".into(), truncated)])).unwrap();
        let result = GenericBinaryReader::new(None).read_file(&path, |_| {});
        assert!(result.is_err(), "a truncated member went unnoticed");
        std::fs::remove_dir_all(directory).unwrap();
    }

    /// `data` compressed as the extension of `name` says.
    fn encoded(name: &str, data: &[u8]) -> Vec<u8> {
        let extension = match codec_of(Path::new(name)) {
            Codec::Gzip => "gz",
            Codec::Lz4 => "lz4",
            Codec::Bzip2 => "bz2",
            Codec::Xz => "xz",
            Codec::Zstd => "zst",
            Codec::Plain => return data.to_vec(),
        };
        compress(data, extension)
    }

    /// A tar archive of `members`, with a directory and a symbolic link around them.
    fn archive(members: &[(String, Vec<u8>)]) -> Vec<u8> {
        let mut builder = tar::Builder::new(Vec::new());
        let mut directory = tar::Header::new_gnu();
        directory.set_entry_type(EntryType::Directory);
        directory.set_size(0);
        directory.set_mode(0o755);
        builder
            .append_data(&mut directory, "dir/", std::io::empty())
            .unwrap();
        for (path, data) in members {
            let mut header = tar::Header::new_gnu();
            header.set_size(data.len() as u64);
            header.set_mode(0o644);
            builder
                .append_data(&mut header, path, data.as_slice())
                .unwrap();
        }
        let mut link = tar::Header::new_gnu();
        link.set_entry_type(EntryType::Symlink);
        link.set_size(0);
        link.set_mode(0o777);
        builder.append_link(&mut link, "link.fa", "a.fa").unwrap();
        builder.into_inner().unwrap()
    }

    /// Records the events of each file apart, starting a new record at each file path.
    fn file_recorder(files: &mut Vec<Recorded>) -> impl FnMut(BinaryEvent<'_, ()>) + '_ {
        |event| {
            if matches!(event, BinaryEvent::FilePath(_)) {
                files.push(Recorded::default());
            }
            let recorded = files.last_mut().expect("an event before any file path");
            match event {
                BinaryEvent::FilePath(path) => recorded.file_path(path),
                BinaryEvent::Chunk(chunk) => recorded.chunk(chunk),
                BinaryEvent::Extra { start, .. } => recorded.extra(start, 0),
            }
        }
    }

    #[test]
    fn archives_are_read_member_by_member() {
        let inner: Vec<(String, Vec<u8>)> = vec![
            ("g.fa".into(), sample(20_000)),
            ("h.fa.gz".into(), sample(10_000)),
        ];
        let mut members: Vec<(String, Vec<u8>)> = vec![
            ("a.fa".into(), sample(100_000)),
            ("dir/b.fq.gz".into(), sample(3 << 20)),
            ("c.fa.xz".into(), sample(50_000)),
            ("d.fa.zst".into(), sample(60_000)),
            ("e.fa.lz4".into(), sample(70_000)),
            ("f.fa.bz2".into(), sample(80_000)),
            ("empty.fa".into(), Vec::new()),
            // Longer than a tar header holds, so stored as a GNU long name.
            (
                format!("{}/long.fa", "nested-directory".repeat(8)),
                sample(1000),
            ),
        ];
        let encode = |members: &[(String, Vec<u8>)]| -> Vec<(String, Vec<u8>)> {
            members
                .iter()
                .map(|(name, data)| (name.clone(), encoded(name, data)))
                .collect()
        };
        let mut stored = encode(&members);
        stored.push((
            "inner.tgz".into(),
            encoded("inner.tgz", &archive(&encode(&inner))),
        ));
        stored.push(("readme.txt".into(), b"not a sequence\n".to_vec()));
        let tar = archive(&stored);

        // The archives have no file path, their members are named after them.
        members.extend(
            inner
                .into_iter()
                .map(|(name, data)| (format!("inner.tgz:{name}"), data)),
        );
        members.push(("readme.txt".into(), b"not a sequence\n".to_vec()));

        let directory = std::env::temp_dir().join(format!(
            "ggcat-binary-reader-archives-{}",
            std::process::id()
        ));
        std::fs::create_dir_all(&directory).unwrap();
        for extension in [
            "tar", "tar.gz", "tgz", "taz", "tar.xz", "txz", "tar.zst", "tzst", "tar.lz4", "tlz4",
            "tar.bz2", "tbz", "tbz2", "tb2", "tz2",
        ] {
            let path = directory.join(format!("input.{extension}"));
            std::fs::write(&path, encoded(path.to_str().unwrap(), &tar)).unwrap();
            for buffer_size in [None, Some(4096)] {
                let mut files = Vec::new();
                GenericBinaryReader::new(buffer_size)
                    .read_file(&path, file_recorder(&mut files))
                    .unwrap();
                assert_eq!(files.len(), members.len(), "{extension}");
                for (recorded, (name, data)) in files.iter().zip(&members) {
                    let name = format!("{}:{name}", path.display());
                    check(recorded, data, buffer_size, Some(&name));
                }
            }
        }
        std::fs::remove_dir_all(directory).unwrap();
    }
}
