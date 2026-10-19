//! Decompressed bytes, straight from a file or a stream.
//!
//! A [`GenericBinaryReader`] hands them out as [`BinaryEvent`]s, in stream order. The LZ codecs
//! (gzip, lz4, xz, zstd) are decoded in place by lz_copyback, and their bytes handed out of the
//! decoder's own ring, where it decoded them. The others (bzip2, uncompressed bytes) come from a
//! [`BinaryDecoder`], which writes its output into a [`DecodedOutput`] one buffer at a time.
//!
//! A tar archive is read as it is decompressed, never loaded whole: each member file is
//! decoded on its own, by the codec its name implies, as if it were a file of its own.
//!
//! A reader made with [`GenericBinaryReader::with_copybacks`] also hands out the LZ matches
//! ("copybacks") of the files that compress enough: see [`BinaryEvent::ChunkWithCopybacks`].

mod in_place;

use anyhow::{Context, Result};
use config::DEFAULT_OUTPUT_BUFFER_SIZE;
use in_place::{InPlace, Tracking, in_place_decoder};
use std::collections::VecDeque;
use std::fs::File;
use std::io::{ErrorKind, Read};
use std::path::{Path, PathBuf};
use tar::EntryType;

pub use in_place::CopybacksSettings;
pub use lz_copyback::Copyback;

/// With a buffer size, every chunk but the last of an input is a whole number of these bytes:
/// the blocks the SIMD parser reads.
pub const CHUNK_BLOCK: usize = 64;

/// Opens an input file, keeping the historical failure message.
pub(crate) fn open_input(path: &Path) -> File {
    File::open(path).unwrap_or_else(|_| panic!("Cannot open file {}", path.display()))
}

/// The compression a file name implies.
#[derive(Copy, Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub(crate) enum Codec {
    Gzip,
    Lz4,
    Bzip2,
    Xz,
    Zstd,
    Plain,
}

impl Codec {
    /// Whether it is an LZ codec, which lz_copyback decodes in place, reporting its matches as
    /// copybacks.
    fn is_lz(self) -> bool {
        matches!(self, Codec::Gzip | Codec::Lz4 | Codec::Xz | Codec::Zstd)
    }
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

/// The archives that the name of a member, as [`BinaryEvent::FilePath`] names them, is nested
/// in, outermost first: the prefixes of `name` that are an archive's name followed by `:`. For
/// `x.tar:dir/inner.tgz:a.fa`, `x.tar` and `x.tar:dir/inner.tgz`. The colons that follow no
/// archive's name, as a directory's, do not end one.
pub fn archive_prefixes(name: &str) -> impl Iterator<Item = &str> {
    name.match_indices(':')
        .map(|(index, _)| &name[..index])
        .filter(|prefix| is_archive(Path::new(prefix)))
}

/// The archive `path` names, and its member if it names one, as colour lists write them:
/// `x.tar.gz:dir/a.fa` is the member `dir/a.fa` of `x.tar.gz`, and `x.tar.gz` the whole archive.
/// The split is at the first of its [`archive_prefixes`], so that the colons of the directories
/// before it, and those of the archives nested in it (`x.tar:inner.tgz:a.fa`), are kept. None if
/// `path` names no archive, or is not UTF-8.
pub fn split_archive_member(path: &Path) -> Option<(PathBuf, Option<PathBuf>)> {
    let name = path.to_str()?;
    if let Some(archive) = archive_prefixes(name).next() {
        let member = PathBuf::from(&name[archive.len() + 1..]);
        return Some((PathBuf::from(archive), Some(member)));
    }
    is_archive(path).then(|| (path.to_owned(), None))
}

/// A decompressor, writing its output into a [`DecodedOutput`] one buffer at a time.
pub trait BinaryDecoder {
    /// Decodes the input to its end.
    fn decode(&mut self, output: &mut impl DecodedOutput) -> Result<()>;
}

/// Where a [`BinaryDecoder`] writes. A decoder that can write into a buffer it is given fills
/// [`Self::free_space`] and calls [`Self::commit`]; one that writes into a buffer of its own
/// hands its output over with [`Self::push`].
pub trait DecodedOutput {
    /// Where the next bytes can be written. Never empty, so a read into it returning 0 means
    /// the end of the input.
    fn free_space(&mut self) -> &mut [u8];

    /// Takes the `count` bytes written at the start of [`Self::free_space`].
    fn commit(&mut self, count: usize);

    /// Takes `data`, the next bytes of the stream.
    fn push(&mut self, data: &[u8]);
}

/// Decodes `reader` straight into the output's free space.
fn decode_read(reader: &mut (impl Read + ?Sized), output: &mut impl DecodedOutput) -> Result<()> {
    loop {
        match reader.read(output.free_space()) {
            Ok(0) => return Ok(()),
            Ok(count) => output.commit(count),
            Err(error) if error.kind() == ErrorKind::Interrupted => {}
            Err(error) => return Err(error.into()),
        }
    }
}

/// The decoders that write into a buffer they are given.
macro_rules! read_decoders {
    ($(impl[$($generics:tt)*] for $decoder:ty;)*) => {$(
        impl<$($generics)*> BinaryDecoder for $decoder {
            fn decode(&mut self, output: &mut impl DecodedOutput) -> Result<()> {
                decode_read(self, output)
            }
        }
    )*};
}

read_decoders! {
    impl[] for File;
    impl['r] for dyn Read + 'r;
}

/// Whether a file's bytes come with copybacks, and which stream their offsets count in.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum OffsetBase {
    /// No copybacks: the bytes come as [`BinaryEvent::Chunk`]s.
    None,
    /// The bytes come as [`BinaryEvent::ChunkWithCopybacks`], from a stream that starts with
    /// this file: its offsets and copybacks have nothing to do with those of earlier files.
    New,
    /// As [`Self::New`], from the stream of the last file before it with copybacks: both are
    /// members of the same compressed archive, and the copybacks can reach into that file.
    Continued,
}

/// What a [`GenericBinaryReader`] hands out, in stream order.
pub enum BinaryEvent<'a> {
    /// The file the following events come from, sent first and exactly once per input.
    FilePath {
        /// The path as given for a file (converted lossily if it is not UTF-8), `None` for an
        /// input that is not a file. A member of a tar archive is named after the archive and
        /// its path inside it, joined by `:`, as `/data/x.tar.gz:dir/a.fa`, with one more for
        /// each archive inside an archive, as `/data/x.tar:inner.tgz:a.fa`.
        name: Option<&'a str>,
        /// Whether the file's bytes come with copybacks, and in which stream.
        base: OffsetBase,
    },
    /// Decoded bytes, never empty. With a buffer size, at most that many and a whole number of
    /// [`CHUNK_BLOCK`] bytes, except for the last chunk of an input, which can be any shorter
    /// length (see [`GenericBinaryReader::new`]).
    Chunk(&'a [u8]),
    /// A [`Self::Chunk`] of a file with copybacks.
    ChunkWithCopybacks {
        chunk: &'a [u8],
        /// Where the chunk is in its stream: the decompressed bytes of the compressed file it is
        /// in, counted from the first as the copybacks count theirs. All the files of a stream,
        /// as the members of a compressed archive, share it, and its offsets go on across them,
        /// skipping the bytes between them (tar headers, members without copybacks).
        offset: u64,
        /// The copybacks the stream's decoder has found so far, in stream order. The decoder
        /// has gone at least a window (see [`CopybacksSettings::window`]) past the end of the
        /// chunk, unless the stream ended earlier, so every copyback whose destination is up
        /// to there is in. The decoder only pushes to the back: removing the copybacks is up
        /// to the consumer, those landing in bytes it never gets as copyback chunks included
        /// (tar headers, members without copybacks, files it skips), else they pile up.
        copybacks: &'a mut VecDeque<Copyback>,
    },
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

/// Hands the bytes a decoder writes into a buffer to `on_chunk`: without a chunk size,
/// everything as soon as it is written; with one, each chunk of exactly that size as soon as it
/// is complete.
///
/// The buffer is a whole number of chunks and is always filled up to its end before starting
/// over, so a chunk never wraps around and nothing is ever moved back to the start.
struct ChunkRing<'a, F> {
    buffer: &'a mut [u8],
    chunk_size: Option<usize>,
    /// Start of the first chunk not handed out yet.
    emitted: usize,
    /// End of the bytes written so far.
    filled: usize,
    on_chunk: F,
}

impl<'a, F: FnMut(&[u8])> ChunkRing<'a, F> {
    fn new(buffer: &'a mut [u8], chunk_size: Option<usize>, on_chunk: F) -> Self {
        Self {
            buffer,
            chunk_size,
            emitted: 0,
            filled: 0,
            on_chunk,
        }
    }

    /// Hands out the last chunk, shorter than the others.
    fn finish(&mut self) {
        if self.filled > self.emitted {
            (self.on_chunk)(&self.buffer[self.emitted..self.filled]);
        }
        self.filled = 0;
        self.emitted = 0;
    }
}

impl<F: FnMut(&[u8])> DecodedOutput for ChunkRing<'_, F> {
    fn free_space(&mut self) -> &mut [u8] {
        &mut self.buffer[self.filled..]
    }

    fn commit(&mut self, count: usize) {
        self.filled += count;
        match self.chunk_size {
            None => {
                (self.on_chunk)(&self.buffer[..self.filled]);
                self.filled = 0;
            }
            Some(chunk_size) => {
                while self.filled - self.emitted >= chunk_size {
                    (self.on_chunk)(&self.buffer[self.emitted..self.emitted + chunk_size]);
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

    /// Without a chunk size, `data` goes out as it is; with one, it is copied in.
    fn push(&mut self, mut data: &[u8]) {
        if self.chunk_size.is_none() {
            if !data.is_empty() {
                (self.on_chunk)(data);
            }
            return;
        }
        while !data.is_empty() {
            let space = self.free_space();
            let count = space.len().min(data.len());
            space[..count].copy_from_slice(&data[..count]);
            data = &data[count..];
            self.commit(count);
        }
    }
}

pub struct GenericBinaryReader {
    /// Where the chunks of the inputs that are not decoded in place are cut.
    buffer: AlignedBuffer,
    buffer_size: Option<usize>,
    /// How the inputs decoded in place are handed out.
    in_place: InPlace,
    /// The settings of a reader with copybacks.
    copybacks: Option<CopybacksSettings>,
}

impl GenericBinaryReader {
    /// With `buffer_size`, which must be a power of two of at least [`CHUNK_BLOCK`], every
    /// chunk handed out has at most that many bytes and is a whole number of [`CHUNK_BLOCK`]
    /// bytes, except the last one of each input, which can be shorter. Without it, chunks are
    /// the buffers the decoders produce.
    ///
    /// The LZ inputs (gzip, lz4, xz, zstd) are decoded in place by lz_copyback: their chunks
    /// are handed out of the decoder's own ring, where it decoded them, at any address. A block
    /// split between two pieces of the ring, where it wraps around, is the only part copied,
    /// into a chunk of its own. Without a buffer size the chunks are the pieces of the ring.
    ///
    /// The other inputs (bzip2, uncompressed) are filled in a ring of only two chunks, so that
    /// it stays in the caches: each byte is written there and read back right after, by every
    /// reader at once. Their chunks are exactly `buffer_size` bytes, and aligned to it.
    pub fn new(buffer_size: Option<usize>) -> Self {
        let (capacity, align) = match buffer_size {
            None => (DEFAULT_OUTPUT_BUFFER_SIZE, 1),
            Some(size) => {
                assert!(
                    size.is_power_of_two() && size >= CHUNK_BLOCK,
                    "The buffer size must be a power of two of at least {CHUNK_BLOCK}, got {size}"
                );
                (2 * size, size)
            }
        };
        Self {
            buffer: AlignedBuffer::new(capacity, align),
            buffer_size,
            in_place: InPlace::new(buffer_size, None),
            copybacks: None,
        }
    }

    /// As [`Self::new`], with the bytes of the LZ files that compress at least
    /// `settings.min_compression_ratio` times handed out with their copybacks, as
    /// [`BinaryEvent::ChunkWithCopybacks`]. Their chunks go out once the decoder has decoded
    /// `settings.window` bytes past them, so that their copybacks are known up to there;
    /// without a buffer size, they are at most a window long.
    ///
    /// Such a file gets copybacks if its compression ratio is enough: the one its metadata
    /// records, if it is a file of its own and records how much it decompresses to; otherwise,
    /// as for the members of an archive, the one of the bytes decoded before its first chunk
    /// goes out.
    ///
    /// The members of a compressed archive with copybacks share its stream: the uncompressed
    /// ones get its copybacks, at offsets going on across the archive, and the compressed ones,
    /// whose bytes are not the archive's, none. Only a compressed file read outside any stream
    /// with copybacks can start one: a file of its own, or a member of an archive without them,
    /// such as an uncompressed one.
    pub fn with_copybacks(buffer_size: Option<usize>, settings: CopybacksSettings) -> Self {
        assert!(settings.window > 0, "The copybacks window cannot be empty");
        let mut reader = Self::new(buffer_size);
        reader.in_place = InPlace::new(buffer_size, Some(settings.window));
        reader.copybacks = Some(settings);
        reader
    }

    /// Runs `decoder` to the end of its input, as an input that is not a file. The bytes
    /// decoded before an error are still handed out.
    pub fn read_decoder<D: BinaryDecoder + ?Sized>(
        &mut self,
        decoder: &mut D,
        mut callback: impl FnMut(BinaryEvent<'_>),
    ) -> Result<()> {
        callback(BinaryEvent::FilePath {
            name: None,
            base: OffsetBase::None,
        });
        self.decode(decoder, |chunk| callback(BinaryEvent::Chunk(chunk)))
    }

    /// Runs `decoder` to its end, cutting its output into chunks in the ring.
    fn decode<D: BinaryDecoder + ?Sized>(
        &mut self,
        decoder: &mut D,
        on_chunk: impl FnMut(&[u8]),
    ) -> Result<()> {
        let mut ring = ChunkRing::new(self.buffer.as_mut_slice(), self.buffer_size, on_chunk);
        let result = decoder.decode(&mut ring);
        ring.finish();
        result
    }

    /// Reads `stream` to its end.
    pub fn read_stream(
        &mut self,
        stream: &mut dyn Read,
        callback: impl FnMut(BinaryEvent<'_>),
    ) -> Result<()> {
        self.read_decoder(stream, callback)
    }

    /// Reads and decompresses a whole file, by the codec its name implies. A tar archive gets
    /// no [`BinaryEvent::FilePath`] of its own: each member file is read as a file, named
    /// after the archive and its path inside it, and archives inside it are read the same way.
    pub fn read_file(
        &mut self,
        path: &Path,
        mut callback: impl FnMut(BinaryEvent<'_>),
    ) -> Result<()> {
        self.read_path(path, &mut callback)
            .with_context(|| format!("Cannot decompress {}", path.display()))
    }

    fn read_path<F: FnMut(BinaryEvent<'_>)>(
        &mut self,
        path: &Path,
        callback: &mut F,
    ) -> Result<()> {
        let name = path.to_string_lossy();
        let codec = codec_of(path);
        if !codec.is_lz() {
            let may_track = self.copybacks.is_some();
            return self.read_stock(&name, path, open_input(path), may_track, callback);
        }
        let tracking = match self.copybacks {
            None => Tracking::Off,
            Some(settings) => match crate::input_size::recorded_ratio(path) {
                Some(ratio) if ratio >= settings.min_compression_ratio => Tracking::On,
                // It compresses too little, as its metadata tells already.
                Some(_) => Tracking::Off,
                None => Tracking::Measured,
            },
        };
        let decoder = in_place_decoder(codec, open_input(path), self.retain(tracking))?;
        self.read_in_place(&name, path, decoder, tracking, callback)
    }

    /// Reads the input `name`, whose own path is `path`, from `source`: an LZ input decoded in
    /// place, with copybacks if it compresses enough, only if `may_track` (it is read outside
    /// any stream with copybacks, in a reader with them); anything else as
    /// [`Self::read_stock`] does.
    fn read_input<F: FnMut(BinaryEvent<'_>)>(
        &mut self,
        name: &str,
        path: &Path,
        source: impl Read,
        may_track: bool,
        callback: &mut F,
    ) -> Result<()> {
        let codec = codec_of(path);
        if !codec.is_lz() {
            return self.read_stock(name, path, source, may_track, callback);
        }
        let tracking = if may_track {
            Tracking::Measured
        } else {
            Tracking::Off
        };
        let decoder = in_place_decoder(codec, source, self.retain(tracking))?;
        self.read_in_place(name, path, decoder, tracking, callback)
    }

    /// Reads the input `name`, whose own path is `path`, from `source`, bzip2 decoded by its
    /// stock decoder, or uncompressed: an archive member by member, its members read as by
    /// [`Self::read_input`] with `may_track`, anything else as a single file.
    fn read_stock<F: FnMut(BinaryEvent<'_>)>(
        &mut self,
        name: &str,
        path: &Path,
        mut source: impl Read,
        may_track: bool,
        callback: &mut F,
    ) -> Result<()> {
        let codec = codec_of(path);
        let mut decoder;
        let source: &mut dyn Read = match codec {
            Codec::Bzip2 => {
                decoder = bzip2::read::MultiBzDecoder::new(source);
                &mut decoder
            }
            Codec::Plain => &mut source,
            _ => unreachable!("{codec:?} is decoded in place"),
        };
        if is_archive(path) {
            return self.read_archive(name, source, may_track, callback);
        }
        // Sent before the decoder starts, so that a file failing there still gets one.
        callback(BinaryEvent::FilePath {
            name: Some(name),
            base: OffsetBase::None,
        });
        self.decode(source, |chunk| callback(BinaryEvent::Chunk(chunk)))
    }

    /// Reads the tar archive `name` as it comes out of `source`, decoding each member file on
    /// its own as `name:path`, as [`Self::read_input`] does with `may_track`. Directories, links
    /// and other special entries are skipped.
    fn read_archive<F: FnMut(BinaryEvent<'_>)>(
        &mut self,
        name: &str,
        source: &mut dyn Read,
        may_track: bool,
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
            self.read_input(&member, &path, &mut entry, may_track, callback)
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
    pub(super) fn sample(len: usize) -> Vec<u8> {
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
    pub(super) struct Recorded {
        events: usize,
        /// Each file path, with the number of events before it.
        pub(super) file_paths: Vec<(Option<String>, usize)>,
        /// The offset base of each file path.
        pub(super) bases: Vec<OffsetBase>,
        /// Each chunk with its address.
        pub(super) chunks: Vec<(usize, Vec<u8>)>,
        /// The offset of each chunk, if it came with copybacks.
        pub(super) offsets: Vec<Option<u64>>,
    }

    impl Recorded {
        pub(super) fn event(&mut self, event: BinaryEvent<'_>) {
            match event {
                BinaryEvent::FilePath { name, base } => {
                    self.file_paths.push((name.map(str::to_owned), self.events));
                    self.bases.push(base);
                }
                BinaryEvent::Chunk(chunk) => self.chunk(chunk, None),
                BinaryEvent::ChunkWithCopybacks { chunk, offset, .. } => {
                    self.chunk(chunk, Some(offset))
                }
            }
            self.events += 1;
        }

        fn chunk(&mut self, chunk: &[u8], offset: Option<u64>) {
            self.chunks.push((chunk.as_ptr() as usize, chunk.to_vec()));
            self.offsets.push(offset);
        }
    }

    fn recorder(recorded: &mut Recorded) -> impl FnMut(BinaryEvent<'_>) + '_ {
        |event| recorded.event(event)
    }

    /// Copybacks for every file that can have them, a few small windows at a time.
    pub(super) fn copybacks_settings() -> CopybacksSettings {
        CopybacksSettings {
            window: 16 * 1024,
            min_len: 16,
            min_compression_ratio: 0.0,
        }
    }

    /// A reader without copybacks, and one with them for every LZ-compressed file: both must
    /// hand out the same bytes.
    pub(super) fn readers(buffer_size: Option<usize>) -> [GenericBinaryReader; 2] {
        [
            GenericBinaryReader::new(buffer_size),
            GenericBinaryReader::with_copybacks(buffer_size, copybacks_settings()),
        ]
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

        // Offsets exactly for a file with copybacks, one chunk right after the other.
        let mut next_offset = None;
        for ((_, chunk), &offset) in chunks.iter().zip(&recorded.offsets) {
            assert_eq!(offset.is_some(), recorded.bases[0] != OffsetBase::None);
            if let (Some(offset), Some(next_offset)) = (offset, next_offset) {
                assert_eq!(
                    offset, next_offset,
                    "the chunks are not one after the other"
                );
            }
            next_offset = offset.map(|offset| offset + chunk.len() as u64);
        }

        // At most a buffer size, whole blocks but the last: the chunks of the inputs decoded in
        // place are where the decoder left them, at any address.
        let Some(size) = buffer_size else {
            return;
        };
        for (index, (_, chunk)) in chunks.iter().enumerate() {
            assert!(chunk.len() <= size, "chunk {index} is longer than {size}");
            if index + 1 < chunks.len() {
                assert_eq!(
                    chunk.len() % CHUNK_BLOCK,
                    0,
                    "chunk {index} is not whole blocks"
                );
            }
        }
    }

    /// As [`check`], for an input that is not decoded in place: with a buffer size, every chunk
    /// but the last is exactly that many bytes, and all of them are aligned to it.
    fn check_exact(
        recorded: &Recorded,
        expected: &[u8],
        buffer_size: Option<usize>,
        file_path: Option<&str>,
    ) {
        check(recorded, expected, buffer_size, file_path);
        let Some(size) = buffer_size else {
            return;
        };
        let chunks = &recorded.chunks;
        for (index, (address, chunk)) in chunks.iter().enumerate() {
            assert_eq!(address % size, 0, "chunk {index} is not aligned to {size}");
            if index + 1 < chunks.len() {
                assert_eq!(chunk.len(), size, "chunk {index} is not whole");
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
                check_exact(&recorded, &data, Some(size), None);
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
        // Each read goes out on its own, as it came.
        assert!(recorded.chunks.len() > data.len() / 5003);
    }

    #[test]
    #[should_panic(expected = "power of two")]
    fn buffer_size_must_be_a_power_of_two() {
        GenericBinaryReader::new(Some(3000));
    }

    /// Hands `data` over in windows of irregular sizes, alternately pushed from its own memory
    /// and written in place.
    struct Windows {
        data: Vec<u8>,
        bounds: Vec<(usize, usize)>,
    }

    impl BinaryDecoder for Windows {
        fn decode(&mut self, output: &mut impl DecodedOutput) -> Result<()> {
            for (index, &(start, end)) in self.bounds.iter().enumerate() {
                if index % 2 == 0 {
                    output.push(&self.data[start..end]);
                    continue;
                }
                let mut written = start;
                while written < end {
                    let space = output.free_space();
                    let count = space.len().min(end - written);
                    space[..count].copy_from_slice(&self.data[written..written + count]);
                    output.commit(count);
                    written += count;
                }
            }
            Ok(())
        }
    }

    #[test]
    fn pushed_and_written_output_come_in_stream_order() {
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
            data: data.clone(),
            bounds,
        };

        for buffer_size in [None, Some(64), Some(1 << 16)] {
            let mut recorded = Recorded::default();
            GenericBinaryReader::new(buffer_size)
                .read_decoder(&mut decoder, recorder(&mut recorded))
                .unwrap();
            check_exact(&recorded, &data, buffer_size, None);
        }
    }

    pub(super) fn compress(data: &[u8], extension: &str) -> Vec<u8> {
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
                for mut reader in readers(buffer_size) {
                    let mut recorded = Recorded::default();
                    reader.read_file(&path, recorder(&mut recorded)).unwrap();
                    if matches!(extension, "bz2" | "fa") {
                        check_exact(&recorded, &data, buffer_size, path.to_str());
                    } else {
                        check(&recorded, &data, buffer_size, path.to_str());
                    }
                }
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
    fn a_member_is_split_from_its_archive_at_the_first_archive_name() {
        let split = |name: &str| split_archive_member(Path::new(name));
        let member = |archive: &str, member: &str| {
            Some((PathBuf::from(archive), Some(PathBuf::from(member))))
        };
        assert_eq!(split("/d/x.tar.gz:dir/a.fa"), member("/d/x.tar.gz", "dir/a.fa"));
        // The colons of the directories before it are not splits.
        assert_eq!(split("/d/a:b/x.tgz:a.fa"), member("/d/a:b/x.tgz", "a.fa"));
        // Those of nested archives stay in the member, as the reader names it.
        assert_eq!(split("x.tar:in.tar.xz:a.fa"), member("x.tar", "in.tar.xz:a.fa"));
        assert_eq!(split("x.tar:"), member("x.tar", ""));
        assert_eq!(split("/d/x.tar.zst"), Some((PathBuf::from("/d/x.tar.zst"), None)));
        assert_eq!(split("/d/x.fa"), None);
        assert_eq!(split("/d/a:b/x.fa.gz"), None);
    }

    #[test]
    fn a_members_name_has_a_prefix_for_each_archive_it_is_nested_in() {
        let prefixes = |name| archive_prefixes(name).collect::<Vec<_>>();
        assert_eq!(
            prefixes("/d/x.tar:dir/in.tgz:a:b/deep.tar.xz:c.fa"),
            ["/d/x.tar", "/d/x.tar:dir/in.tgz", "/d/x.tar:dir/in.tgz:a:b/deep.tar.xz"]
        );
        assert_eq!(prefixes("in.tgz:a.fa"), ["in.tgz"]);
        assert!(prefixes("/d/a:b/x.fa").is_empty());
        assert!(prefixes("/d/x.tar").is_empty());
    }

    /// The decoders in place, fed a few bytes per read: two streams back to back, as
    /// concatenated files are, but for lz4, whose decoder stops after its first frame.
    #[test]
    fn in_place_decoders_read_from_small_reads() {
        let first = sample(DEFAULT_OUTPUT_BUFFER_SIZE + 999);
        let second = sample(70_000);
        let expected = [first.as_slice(), &second].concat();
        for extension in ["gz", "lz4", "xz", "zst"] {
            let compressed = match extension {
                "lz4" => compress(&expected, extension),
                _ => [compress(&first, extension), compress(&second, extension)].concat(),
            };
            let name = format!("input.fa.{extension}");
            for buffer_size in [None, Some(4096)] {
                for mut reader in readers(buffer_size) {
                    let mut recorded = Recorded::default();
                    let source = Trickle {
                        data: &compressed,
                        step: 0,
                    };
                    let may_track = reader.copybacks.is_some();
                    let mut callback = recorder(&mut recorded);
                    reader
                        .read_input(&name, Path::new(&name), source, may_track, &mut callback)
                        .unwrap();
                    drop(callback);
                    check(&recorded, &expected, buffer_size, Some(&name));
                }
            }
        }
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
            for mut reader in readers(None) {
                let mut recorded = Recorded::default();
                reader.read_file(&path, recorder(&mut recorded)).unwrap();
                check(&recorded, &data, None, path.to_str());
            }

            // Both the archive and its member made of two streams.
            let name = format!("a.fa.{extension}");
            let tar = archive(&[(name.clone(), concatenated(&member, extension))]);
            let path = directory.join(format!("input.tar.{extension}"));
            std::fs::write(&path, concatenated(&tar, extension)).unwrap();
            for mut reader in readers(None) {
                let mut files = Vec::new();
                reader.read_file(&path, file_recorder(&mut files)).unwrap();
                assert_eq!(files.len(), 1, "{extension}");
                let name = format!("{}:{name}", path.display());
                check(&files[0], &member, None, Some(&name));
            }
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
            for mut reader in readers(None) {
                let result = reader.read_file(&path, |_| {});
                assert!(result.is_err(), "{extension}: the damage went unnoticed");
            }
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
        for mut reader in readers(None) {
            let result = reader.read_file(&path, |_| {});
            assert!(result.is_err(), "a truncated file went unnoticed");
        }

        let path = directory.join("input.tar");
        std::fs::write(&path, archive(&[("a.fa.lz4".into(), truncated)])).unwrap();
        for mut reader in readers(None) {
            let result = reader.read_file(&path, |_| {});
            assert!(result.is_err(), "a truncated member went unnoticed");
        }
        std::fs::remove_dir_all(directory).unwrap();
    }

    /// `data` compressed as the extension of `name` says.
    pub(super) fn encoded(name: &str, data: &[u8]) -> Vec<u8> {
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
    pub(super) fn archive(members: &[(String, Vec<u8>)]) -> Vec<u8> {
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
    pub(super) fn file_recorder(files: &mut Vec<Recorded>) -> impl FnMut(BinaryEvent<'_>) + '_ {
        |event| {
            if matches!(event, BinaryEvent::FilePath { .. }) {
                files.push(Recorded::default());
            }
            let recorded = files.last_mut().expect("an event before any file path");
            recorded.event(event);
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
                for mut reader in readers(buffer_size) {
                    let mut files = Vec::new();
                    reader.read_file(&path, file_recorder(&mut files)).unwrap();
                    assert_eq!(files.len(), members.len(), "{extension}");
                    for (recorded, (name, data)) in files.iter().zip(&members) {
                        let name = format!("{}:{name}", path.display());
                        check(recorded, data, buffer_size, Some(&name));
                    }
                }
            }
        }
        std::fs::remove_dir_all(directory).unwrap();
    }
}
