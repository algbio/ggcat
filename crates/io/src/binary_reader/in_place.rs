//! The LZ inputs (gzip, lz4, xz, zstd), decoded in place by lz_copyback: their bytes are handed
//! out where the decoder left them, in its ring, and, in a stream that records them, along with
//! the copybacks it found at least a window past them. The archives in them are walked here,
//! over the ring, a member at a time.

use super::{
    BinaryEvent, CHUNK_BLOCK, Codec, GenericBinaryReader, OffsetBase, codec_of, is_archive,
};
use anyhow::{Context, Result, bail};
use lz_copyback::{InPlaceDecoder, gzip, lz4, xz, zstd};
use std::collections::VecDeque;
use std::io::{self, Read};
use std::path::{Path, PathBuf};
use tar::EntryType;

/// How a reader made with [`GenericBinaryReader::with_copybacks`] gets copybacks.
#[derive(Clone, Copy, Debug)]
pub struct CopybacksSettings {
    /// How far the decoder has gone past the end of a chunk when the chunk is handed out: the
    /// copybacks with a destination up to that far after it are known along with it.
    pub window: usize,
    /// The shortest match recorded.
    pub min_len: u32,
    /// The compression ratio, decoded over compressed bytes, a file needs to get copybacks.
    pub min_compression_ratio: f64,
}

/// How much a stream decoded in place hands out at once without a buffer size: little enough to
/// still be in the caches when it is read, even with a reader on every core sharing the last
/// level.
const IN_PLACE_CHUNK_SIZE: usize = 256 * 1024;

/// Whether a stream decoded in place records its copybacks.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum Tracking {
    Off,
    On,
    /// If the bytes decoded before its first chunk goes out compress enough.
    Measured,
}

/// How the streams decoded in place are handed out: the size of their chunks, and room for a
/// block split between two pieces of a decoder's ring, the only bytes copied.
pub(super) struct InPlace {
    /// The most bytes handed out at once.
    chunk: usize,
    /// Whether every chunk but the last of a file is a whole number of blocks: with a buffer
    /// size.
    blocks: bool,
    side: [u8; CHUNK_BLOCK],
    /// The bytes handed out, and the blocks copied into `side`.
    #[cfg(test)]
    served: usize,
    #[cfg(test)]
    copied: usize,
}

impl InPlace {
    /// Chunks of at most `buffer_size` bytes, as whole blocks. Without it, the pieces of the
    /// ring, at most `window` long in a reader with copybacks, at most [`IN_PLACE_CHUNK_SIZE`]
    /// otherwise.
    pub(super) fn new(buffer_size: Option<usize>, window: Option<usize>) -> Self {
        Self {
            chunk: buffer_size.or(window).unwrap_or(IN_PLACE_CHUNK_SIZE),
            blocks: buffer_size.is_some(),
            side: [0; CHUNK_BLOCK],
            #[cfg(test)]
            served: 0,
            #[cfg(test)]
            copied: 0,
        }
    }
}

/// `source` decoded in place by lz_copyback's decoder for `codec`, keeping `retain` bytes. All
/// of them but lz4's read on past the end of a stream into the next one, as concatenated files
/// and parallel compressors have them. The source is boxed, so that each decoder is compiled
/// once, whatever it reads from.
pub(super) fn in_place_decoder<'a>(
    codec: Codec,
    source: impl Read + 'a,
    retain: usize,
) -> io::Result<Box<dyn InPlaceDecoder + 'a>> {
    let source: Box<dyn Read + 'a> = Box::new(source);
    Ok(match codec {
        Codec::Gzip => Box::new(gzip::GzipInPlaceDecoder::new(source, retain)),
        Codec::Lz4 => Box::new(lz4::Lz4InPlaceDecoder::new(source, retain)?),
        Codec::Xz => Box::new(xz::XzInPlaceDecoder::new_multi_decoder(source, retain)),
        Codec::Zstd => Box::new(zstd::ZstdInPlaceDecoder::new(source, retain)?),
        Codec::Bzip2 | Codec::Plain => unreachable!("{codec:?} is not an LZ codec"),
    })
}

/// Hands out the bytes of `pieces`, from stream offset `offset` on, to `emit` in whole
/// [`CHUNK_BLOCK`]s: straight from the pieces, but for a block split between two of them, which
/// is copied into `side` and handed out on its own. The bytes start at a block boundary of their
/// file and, unless they end it, end at one too, so that the last piece goes out to its end.
/// Returns how many blocks were copied.
fn emit_blocks<'a>(
    pieces: impl Iterator<Item = &'a [u8]>,
    mut offset: u64,
    side: &mut [u8; CHUNK_BLOCK],
    mut emit: impl FnMut(&[u8], u64),
) -> usize {
    let mut pieces = pieces.peekable();
    // The bytes of a split block copied into `side` so far.
    let mut filled = 0;
    let mut copied = 0;
    while let Some(mut piece) = pieces.next() {
        if filled > 0 {
            let count = (CHUNK_BLOCK - filled).min(piece.len());
            side[filled..filled + count].copy_from_slice(&piece[..count]);
            filled += count;
            piece = &piece[count..];
            // The block can go on into the next piece, else it is the last, shorter one.
            if filled < CHUNK_BLOCK && pieces.peek().is_some() {
                continue;
            }
            emit(&side[..filled], offset);
            offset += filled as u64;
            copied += 1;
        }
        let whole = match pieces.peek() {
            Some(_) => piece.len() - piece.len() % CHUNK_BLOCK,
            None => piece.len(),
        };
        if whole > 0 {
            emit(&piece[..whole], offset);
            offset += whole as u64;
        }
        let rest = &piece[whole..];
        side[..rest.len()].copy_from_slice(rest);
        filled = rest.len();
    }
    copied
}

/// A stream decoded in place, read forward from `position`: its offsets are those of its
/// decoder, which count the decoded bytes as the copybacks do.
struct InPlaceStream<'a> {
    decoder: Box<dyn InPlaceDecoder + 'a>,
    position: u64,
    /// How far past the bytes it hands out the stream is decoded: a window with copybacks, so
    /// that they are known up to there, none otherwise.
    window: u64,
    chunk: usize,
    /// Whether a file was read from the stream already, see [`Self::declare_file`].
    declared: bool,
    /// The decoder's error, returned once the bytes decoded before it are handed out.
    error: Option<io::Error>,
}

impl InPlaceStream<'_> {
    /// Decodes the next `len` bytes (at most a chunk) and a window past them; returns how many
    /// of them there are, fewer only at the end of the stream, or before an error, which is
    /// returned once they are all handed out.
    fn fill(&mut self, len: usize) -> io::Result<usize> {
        if self.error.is_none() {
            let target = self.position + len as u64 + self.window;
            if let Err(error) = self.decoder.decode_to(target) {
                self.error = Some(error);
            }
        }
        let decoded = self.decoder.decoded_len();
        let available = decoded.saturating_sub(self.position).min(len as u64) as usize;
        match self.error.take() {
            Some(error) if available == 0 => Err(error),
            error => {
                self.error = error;
                Ok(available)
            }
        }
    }

    /// Decodes the rest of the stream without handing it out, so that the decoder checks it to
    /// its end.
    fn finish(&mut self) -> io::Result<()> {
        match self.error.take() {
            Some(error) => Err(error),
            None => self.decoder.decode_to(u64::MAX).map(drop),
        }
    }

    /// Moves on to `position`, past bytes that are not read.
    fn skip_to(&mut self, position: u64) {
        debug_assert!(position >= self.position);
        self.position = position;
    }

    /// Reads exactly `len` bytes out of the stream: the extensions of tar headers.
    fn read_vec(&mut self, len: usize) -> io::Result<Vec<u8>> {
        let mut bytes = vec![0; len];
        self.read_exact(&mut bytes)?;
        Ok(bytes)
    }

    /// The offset base of the next file read from the stream: a new one for the first.
    fn declare_file(&mut self) -> OffsetBase {
        if std::mem::replace(&mut self.declared, true) {
            OffsetBase::New
        } else {
            OffsetBase::Continued
        }
    }
}

/// Copies the bytes out: what is read through it is metadata, or compressed, as the members of
/// an archive whose bytes are not the stream's.
impl Read for InPlaceStream<'_> {
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        let len = self.fill(buffer.len().min(self.chunk))?;
        let range = self.position..self.position + len as u64;
        let mut filled = 0;
        for piece in self.decoder.bytes(range) {
            buffer[filled..filled + piece.len()].copy_from_slice(piece);
            filled += piece.len();
        }
        self.position += len as u64;
        Ok(len)
    }
}

/// The largest tar header extension read: a long name or PAX records.
const MAX_HEADER_EXTENSION: u64 = 1 << 20;

/// The path and the size PAX records give, if they do.
fn pax_fields(records: &[u8]) -> Result<(Option<Vec<u8>>, Option<u64>)> {
    let (mut path, mut size) = (None, None);
    for record in tar::PaxExtensions::new(records) {
        let record = record?;
        match record.key_bytes() {
            b"path" => path = Some(record.value_bytes().to_vec()),
            b"size" => size = Some(record.value()?.parse()?),
            _ => {}
        }
    }
    Ok((path, size))
}

impl GenericBinaryReader {
    /// How far past the bytes it hands out a stream decoded in place is decoded: a window if it
    /// can record copybacks, none otherwise.
    fn window(&self, tracking: Tracking) -> usize {
        match (tracking, self.copybacks) {
            (Tracking::Off, _) | (_, None) => 0,
            (_, Some(settings)) => settings.window,
        }
    }

    /// How many bytes a decoder in place keeps readable behind the end of what it was asked to
    /// decode: a chunk and the window past it.
    pub(super) fn retain(&self, tracking: Tracking) -> usize {
        self.in_place.chunk + self.window(tracking)
    }

    /// Reads `name`, whose own path is `path`, decoded in place by `decoder`, with copybacks as
    /// `tracking` says: for [`Tracking::Measured`], if the bytes decoded before the first chunk
    /// goes out compress enough. An archive is walked member by member, its uncompressed members
    /// getting the stream's copybacks and offsets, if it has them.
    pub(super) fn read_in_place<F: FnMut(BinaryEvent<'_>)>(
        &mut self,
        name: &str,
        path: &Path,
        decoder: Box<dyn InPlaceDecoder + '_>,
        tracking: Tracking,
        callback: &mut F,
    ) -> Result<()> {
        let mut stream = InPlaceStream {
            decoder,
            position: 0,
            window: self.window(tracking) as u64,
            chunk: self.in_place.chunk,
            declared: false,
            error: None,
        };
        // The decoders record copybacks from the start.
        if tracking == Tracking::Off {
            stream.decoder.set_copybacks_tracking(false);
        } else {
            let settings = self.copybacks.expect("a reader with copybacks");
            stream.decoder.set_copybacks_min_len(settings.min_len);
        }

        let tracked = match tracking {
            Tracking::Off => false,
            Tracking::On => true,
            Tracking::Measured => {
                // The bytes decoded before the first chunk goes out: a chunk and the window. An
                // error there is returned after the bytes decoded before it.
                if let Err(error) = stream.fill(stream.chunk) {
                    stream.error = Some(error);
                }
                let settings = self.copybacks.expect("a reader with copybacks");
                let decoded = stream.decoder.decoded_len() as f64;
                let ratio = decoded / stream.decoder.total_in().max(1) as f64;
                let tracked = ratio >= settings.min_compression_ratio;
                if !tracked {
                    stream.decoder.set_copybacks_tracking(false);
                    *stream.decoder.peek_copybacks() = VecDeque::new();
                    stream.window = 0;
                }
                tracked
            }
        };

        if is_archive(path) {
            // One stream with copybacks at a time: in a stream without them, compressed members
            // can start their own.
            let may_track = !tracked && self.copybacks.is_some();
            self.walk_archive(name, &mut stream, None, tracked, may_track, callback)?;
            // The tar end marker is not the end of the stream: decoding on to it lets the
            // decoder check what follows, its checksums included.
            return stream
                .finish()
                .with_context(|| format!("Cannot read the end of archive {name}"));
        }
        callback(BinaryEvent::FilePath {
            name: Some(name),
            base: if tracked {
                OffsetBase::New
            } else {
                OffsetBase::None
            },
        });
        self.serve(&mut stream, None, tracked, callback)
    }

    /// Hands out the bytes of `stream` from its position up to `end`, or to the end of the
    /// stream, where the decoder left them, a chunk at a time: with a buffer size, as whole
    /// blocks, see [`emit_blocks`], without one as the pieces of the ring.
    fn serve<F: FnMut(BinaryEvent<'_>)>(
        &mut self,
        stream: &mut InPlaceStream,
        end: Option<u64>,
        tracked: bool,
        callback: &mut F,
    ) -> Result<()> {
        let in_place = &mut self.in_place;
        loop {
            let left = end.map_or(u64::MAX, |end| end - stream.position);
            if left == 0 {
                return Ok(());
            }
            // A chunk is a whole number of blocks, so the bytes of each but the last start and
            // end at block boundaries of the file.
            let len = stream.fill(left.min(in_place.chunk as u64) as usize)?;
            if len == 0 {
                if end.is_some() {
                    bail!("The stream ends {left} bytes before the end of the file");
                }
                return Ok(());
            }
            let start = stream.position;
            let (pieces, queue) = stream
                .decoder
                .bytes_and_copybacks(start..start + len as u64);
            let mut emit = |chunk: &[u8], offset: u64| {
                callback(if tracked {
                    BinaryEvent::ChunkWithCopybacks {
                        chunk,
                        offset,
                        copybacks: &mut *queue,
                    }
                } else {
                    BinaryEvent::Chunk(chunk)
                })
            };
            if in_place.blocks {
                let _copied = emit_blocks(pieces, start, &mut in_place.side, emit);
                #[cfg(test)]
                {
                    in_place.copied += _copied;
                }
            } else {
                let mut offset = start;
                for piece in pieces {
                    emit(piece, offset);
                    offset += piece.len() as u64;
                }
            }
            #[cfg(test)]
            {
                in_place.served += len;
            }
            stream.position += len as u64;
        }
    }

    /// Reads the tar archive `name` out of `stream`, from its position up to `end` (an archive
    /// inside the stream's archive) or to its end marker, reading each member file as
    /// `name:path`. Directories, links and other special entries are skipped, and so are
    /// sparse files.
    fn walk_archive<F: FnMut(BinaryEvent<'_>)>(
        &mut self,
        name: &str,
        stream: &mut InPlaceStream,
        end: Option<u64>,
        tracked: bool,
        may_track: bool,
        callback: &mut F,
    ) -> Result<()> {
        // The extensions of the header that follows them.
        let mut long_name: Option<Vec<u8>> = None;
        let mut pax: Option<(Option<Vec<u8>>, Option<u64>)> = None;
        loop {
            if end == Some(stream.position) {
                return Ok(());
            }
            let mut block = [0u8; 512];
            let mut filled = 0;
            while filled < block.len() {
                match stream.read(&mut block[filled..])? {
                    0 => break,
                    count => filled += count,
                }
            }
            match filled {
                0 => return Ok(()),
                512 => {}
                _ => bail!("Truncated tar header in {name}"),
            }
            if block.iter().all(|byte| *byte == 0) {
                return Ok(());
            }
            let header = tar::Header::from_byte_slice(&block);
            let sum = block[..148]
                .iter()
                .chain(&block[156..])
                .fold(8 * 32, |sum, byte| sum + *byte as u32);
            if sum != header.cksum()? {
                bail!("Tar header checksum mismatch in {name}");
            }

            let entry_type = header.entry_type();
            let (pax_path, pax_size) = pax.take().unwrap_or_default();
            let size = match pax_size {
                Some(size) => size,
                None => header.entry_size()?,
            };
            let data_end = stream.position + size.next_multiple_of(512);
            let extension = matches!(entry_type, EntryType::GNULongName | EntryType::XHeader);
            if extension && size > MAX_HEADER_EXTENSION {
                bail!("Tar header extension of {size} bytes in {name}");
            }
            match entry_type {
                EntryType::GNULongName => {
                    let mut bytes = stream.read_vec(size as usize)?;
                    while bytes.last() == Some(&0) {
                        bytes.pop();
                    }
                    long_name = Some(bytes);
                }
                EntryType::XHeader => pax = Some(pax_fields(&stream.read_vec(size as usize)?)?),
                EntryType::GNULongLink | EntryType::XGlobalHeader => {}
                _ => {
                    let bytes = match long_name.take().or(pax_path) {
                        Some(bytes) => bytes,
                        None => header.path_bytes().into_owned(),
                    };
                    let path = PathBuf::from(String::from_utf8_lossy(&bytes).as_ref());
                    let member = format!("{name}:{}", path.to_string_lossy());
                    match entry_type {
                        EntryType::Regular | EntryType::Continuous => self
                            .read_member(&member, &path, stream, size, tracked, may_track, callback)
                            .with_context(|| format!("Cannot read archive member {member}"))?,
                        EntryType::GNUSparse => ggcat_logging::info!(
                            "Skipping {member}: a sparse file, not read in an archive decoded in place"
                        ),
                        _ => ggcat_logging::info!("Skipping {member}: not a regular file"),
                    }
                }
            }
            // Whatever the entry left unread, and its padding.
            stream.skip_to(data_end);
        }
    }

    /// Reads the archive member `name`, whose own path is `path`, out of the next `size` bytes
    /// of `stream`. An uncompressed one is part of the stream: a file gets its chunks in place,
    /// an archive is walked in turn. A compressed one is a stream of its own, read as
    /// [`Self::read_input`] does: with copybacks of its own, if it compresses enough, only if
    /// `may_track`.
    #[allow(clippy::too_many_arguments)]
    fn read_member<F: FnMut(BinaryEvent<'_>)>(
        &mut self,
        name: &str,
        path: &Path,
        stream: &mut InPlaceStream,
        size: u64,
        tracked: bool,
        may_track: bool,
        callback: &mut F,
    ) -> Result<()> {
        let end = stream.position + size;
        if codec_of(path) != Codec::Plain {
            return self.read_input(name, path, (&mut *stream).take(size), may_track, callback);
        }
        if is_archive(path) {
            return self.walk_archive(name, stream, Some(end), tracked, may_track, callback);
        }
        let base = if tracked {
            stream.declare_file()
        } else {
            OffsetBase::None
        };
        callback(BinaryEvent::FilePath {
            name: Some(name),
            base,
        });
        self.serve(stream, Some(end), tracked, callback)
    }
}

#[cfg(test)]
mod tests {
    use super::super::Copyback;
    use super::super::tests::{archive, copybacks_settings, encoded, readers, sample};
    use super::*;

    /// DNA with repeats: stretches copied from earlier on, at all kinds of distances.
    fn repetitive(len: usize) -> Vec<u8> {
        let mut data = sample(len);
        let mut state = 0x9e37_79b9_7f4a_7c15u64;
        let mut next = |bound: usize| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            (state % bound as u64) as usize
        };
        let mut destination = 1000;
        while destination + 3000 < len {
            let count = 100 + next(2900);
            let source = next(destination);
            data.copy_within(source..source + count, destination);
            destination += count + next(2000);
        }
        data
    }

    /// Bytes no codec can compress.
    fn random_bytes(len: usize) -> Vec<u8> {
        let mut state = 0x2545_f491_4f6c_dd1du64;
        (0..len)
            .map(|_| {
                state ^= state << 13;
                state ^= state >> 7;
                state ^= state << 17;
                (state >> 56) as u8
            })
            .collect()
    }

    /// The bytes split into pieces of `lengths`, as a ring holds them, handed out by
    /// [`emit_blocks`] from offset 1000: the bytes come back whole, in whole blocks but the last,
    /// and only the blocks a split between pieces falls into are copied.
    #[test]
    fn only_the_blocks_split_between_pieces_are_copied() {
        let data = sample(4096);
        for lengths in [
            &[64, 64][..],
            &[128, 64, 256],
            &[1, 63, 64],
            &[65, 63],
            &[100, 28, 130, 2, 1, 1, 62, 200],
            &[3, 2, 1],
            &[30],
            &[130, 7],
            &[64, 10],
            &[70, 70, 70],
            &[1; 200],
        ] {
            let total: usize = lengths.iter().sum();
            let mut pieces = Vec::new();
            let (mut start, mut splits) = (0, Vec::new());
            for &len in lengths {
                pieces.push(&data[start..start + len]);
                start += len;
                if start % CHUNK_BLOCK != 0 && start < total {
                    splits.push(start / CHUNK_BLOCK);
                }
            }
            splits.dedup();

            let mut side = [0; CHUNK_BLOCK];
            let side_address = side.as_ptr() as usize;
            let mut chunks = Vec::new();
            let copied = emit_blocks(pieces.into_iter(), 1000, &mut side, |chunk, offset| {
                chunks.push((chunk.as_ptr() as usize, offset, chunk.to_vec()))
            });

            let joined: Vec<u8> = chunks
                .iter()
                .flat_map(|(_, _, chunk)| chunk.clone())
                .collect();
            assert!(joined == data[..total], "{lengths:?}: the bytes differ");
            let mut next_offset = 1000;
            for (index, (address, offset, chunk)) in chunks.iter().enumerate() {
                assert_eq!(
                    *offset, next_offset,
                    "{lengths:?}: chunk {index} is not next"
                );
                next_offset += chunk.len() as u64;
                assert!(!chunk.is_empty());
                if index + 1 < chunks.len() {
                    assert_eq!(chunk.len() % CHUNK_BLOCK, 0, "{lengths:?}: chunk {index}");
                }
                let in_data =
                    (data.as_ptr() as usize..data.as_ptr() as usize + total).contains(address);
                assert!(in_data || *address == side_address, "{lengths:?}");
            }
            let from_side = chunks
                .iter()
                .filter(|(address, ..)| *address == side_address);
            assert_eq!(from_side.count(), copied, "{lengths:?}");
            assert_eq!(
                copied,
                splits.len(),
                "{lengths:?}: not only the split blocks copied"
            );
        }
    }

    /// A stream with copybacks as a whole: its decoded bytes and every copyback in it, as
    /// lz_copyback's decoder for the codec of `name` finds them in `data` compressed.
    fn stream(name: &str, data: &[u8]) -> (Vec<u8>, Vec<Copyback>) {
        let compressed = encoded(name, data);
        let codec = codec_of(Path::new(name));
        let chunk = 1 << 16;
        let mut decoder = in_place_decoder(codec, compressed.as_slice(), chunk).unwrap();
        decoder.set_copybacks_min_len(copybacks_settings().min_len);
        let mut decoded = Vec::new();
        loop {
            let position = decoded.len() as u64;
            let end = decoder.decode_to(position + chunk as u64).unwrap();
            if end == position {
                break;
            }
            decoded.extend(decoder.bytes(position..end).flatten());
        }
        assert!(decoded == data);
        (decoded, decoder.peek_copybacks().drain(..).collect())
    }

    /// Reads `path` with `reader`, checking each chunk with copybacks against `streams`, the
    /// streams with copybacks it holds, in the order of their first files: the chunk is the
    /// bytes at its offset, and the copybacks, never removed, are those the stream has up to
    /// at least `window` bytes past the chunk. Returns each file's name, base and bytes.
    fn read_checked(
        reader: &mut GenericBinaryReader,
        path: &Path,
        window: usize,
        streams: &[(Vec<u8>, Vec<Copyback>)],
    ) -> Vec<(String, OffsetBase, Vec<u8>)> {
        let mut files: Vec<(String, OffsetBase, Vec<u8>)> = Vec::new();
        let mut current = None;
        reader
            .read_file(path, |event| match event {
                BinaryEvent::FilePath { name, base } => {
                    if base == OffsetBase::New {
                        current = Some(current.map_or(0, |index| index + 1));
                    }
                    files.push((name.unwrap().to_owned(), base, Vec::new()));
                }
                BinaryEvent::Chunk(chunk) => files.last_mut().unwrap().2.extend_from_slice(chunk),
                BinaryEvent::ChunkWithCopybacks {
                    chunk,
                    offset,
                    copybacks,
                } => {
                    let (bytes, all) = &streams[current.expect("a chunk with copybacks first")];
                    let start = offset as usize;
                    assert!(
                        &bytes[start..start + chunk.len()] == chunk,
                        "not at {offset}"
                    );
                    let found = all
                        .get(..copybacks.len())
                        .expect("more copybacks than found");
                    assert!(copybacks.iter().eq(found), "not the copybacks found");
                    let end = (start + chunk.len() + window).min(bytes.len()) as u64;
                    let needed = all.partition_point(|copyback| copyback.destination < end);
                    assert!(
                        needed <= copybacks.len(),
                        "less than a window ahead at {offset}"
                    );
                    files.last_mut().unwrap().2.extend_from_slice(chunk);
                }
            })
            .unwrap();
        files
    }

    fn directory(test: &str) -> std::path::PathBuf {
        let directory = std::env::temp_dir().join(format!(
            "ggcat-binary-reader-in-place-{test}-{}",
            std::process::id()
        ));
        std::fs::create_dir_all(&directory).unwrap();
        directory
    }

    #[test]
    fn chunks_come_with_every_copyback_up_to_a_window_past_them() {
        let directory = directory("files");
        let data = repetitive(600_000);
        for buffer_size in [None, Some(4096)] {
            // One reader for all the files: each gets the windows back from the one before.
            let mut reader = GenericBinaryReader::with_copybacks(buffer_size, copybacks_settings());
            for name in ["a.fa.gz", "a.fa.lz4", "a.fa.xz", "a.fa.zst"] {
                let path = directory.join(name);
                std::fs::write(&path, encoded(name, &data)).unwrap();
                let window = copybacks_settings().window;
                let files = read_checked(&mut reader, &path, window, &[stream(name, &data)]);
                let path = path.to_str().unwrap().to_owned();
                assert!(files == [(path, OffsetBase::New, data.clone())], "{name}");

                // Only the blocks split where the ring wraps around are copied.
                let in_place = &mut reader.in_place;
                let (served, copied) = (in_place.served, in_place.copied);
                assert_eq!(served, data.len(), "{name}");
                assert!(
                    copied * CHUNK_BLOCK * 100 <= served,
                    "{name}: {copied} blocks copied out of {served} bytes"
                );
                (in_place.served, in_place.copied) = (0, 0);
            }
        }
        std::fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn members_of_a_compressed_archive_share_its_stream() {
        let directory = directory("compressed-archive");
        let (a, c, d, e) = (
            repetitive(100_000),
            repetitive(30_000),
            repetitive(40_000),
            repetitive(50_000),
        );
        let b = sample(20_000);
        let inner = archive(&[("c.fa".into(), c.clone()), ("d.fa".into(), d.clone())]);
        let tar = archive(&[
            ("a.fa".into(), a.clone()),
            ("b.fa.gz".into(), encoded("b.fa.gz", &b)),
            ("inner.tar".into(), inner),
            ("e.fa".into(), e.clone()),
        ]);
        for extension in ["tar.gz", "tar.lz4", "tar.xz", "tzst"] {
            let name = format!("input.{extension}");
            let path = directory.join(&name);
            std::fs::write(&path, encoded(&name, &tar)).unwrap();
            for buffer_size in [None, Some(4096)] {
                let mut reader =
                    GenericBinaryReader::with_copybacks(buffer_size, copybacks_settings());
                let window = copybacks_settings().window;
                let files = read_checked(&mut reader, &path, window, &[stream(&name, &tar)]);
                let member = |member: &str, base, data: &Vec<u8>| {
                    (format!("{}:{member}", path.display()), base, data.clone())
                };
                // The bytes of a compressed member are not the archive's.
                let expected = [
                    member("a.fa", OffsetBase::New, &a),
                    member("b.fa.gz", OffsetBase::None, &b),
                    member("inner.tar:c.fa", OffsetBase::Continued, &c),
                    member("inner.tar:d.fa", OffsetBase::Continued, &d),
                    member("e.fa", OffsetBase::Continued, &e),
                ];
                assert!(files == expected, "{extension}");
            }
        }
        std::fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn compressed_members_of_other_archives_start_streams_of_their_own() {
        let directory = directory("uncompressed-archive");
        let data: Vec<Vec<u8>> = (0..10)
            .map(|index| repetitive(20_000 + index * 7_000))
            .collect();
        let inner = archive(&[
            ("p.fa".into(), data[5].clone()),
            ("q.fa".into(), data[6].clone()),
            ("r.fa.gz".into(), encoded("r.fa.gz", &data[7])),
        ]);
        let other = archive(&[
            ("s.fa.zst".into(), encoded("s.fa.zst", &data[8])),
            ("t.fa".into(), data[9].clone()),
        ]);
        let tar = archive(&[
            ("x.fa.xz".into(), encoded("x.fa.xz", &data[0])),
            ("y.fa.zst".into(), encoded("y.fa.zst", &data[1])),
            ("z.fa".into(), data[2].clone()),
            ("w.fa.lz4".into(), encoded("w.fa.lz4", &data[3])),
            ("v.fa.gz".into(), encoded("v.fa.gz", &data[4])),
            ("inner.tar.xz".into(), encoded("inner.tar.xz", &inner)),
            // Without LZ matches, so without a stream with copybacks to its name.
            ("other.tar.bz2".into(), encoded("other.tar.bz2", &other)),
        ]);
        let path = directory.join("input.tar");
        std::fs::write(&path, &tar).unwrap();
        let streams = [
            stream("x.fa.xz", &data[0]),
            stream("y.fa.zst", &data[1]),
            stream("w.fa.lz4", &data[3]),
            stream("v.fa.gz", &data[4]),
            stream("inner.tar.xz", &inner),
            stream("s.fa.zst", &data[8]),
        ];
        let member = |member: &str, base, index: usize| {
            (
                format!("{}:{member}", path.display()),
                base,
                data[index].clone(),
            )
        };
        let expected = [
            member("x.fa.xz", OffsetBase::New, 0),
            member("y.fa.zst", OffsetBase::New, 1),
            member("z.fa", OffsetBase::None, 2),
            member("w.fa.lz4", OffsetBase::New, 3),
            member("v.fa.gz", OffsetBase::New, 4),
            member("inner.tar.xz:p.fa", OffsetBase::New, 5),
            member("inner.tar.xz:q.fa", OffsetBase::Continued, 6),
            member("inner.tar.xz:r.fa.gz", OffsetBase::None, 7),
            member("other.tar.bz2:s.fa.zst", OffsetBase::New, 8),
            member("other.tar.bz2:t.fa", OffsetBase::None, 9),
        ];
        for buffer_size in [None, Some(4096)] {
            let mut reader = GenericBinaryReader::with_copybacks(buffer_size, copybacks_settings());
            let window = copybacks_settings().window;
            let files = read_checked(&mut reader, &path, window, &streams);
            assert!(files == expected);
        }
        std::fs::remove_dir_all(directory).unwrap();
    }

    /// A decoding error comes after the bytes decoded before it.
    #[test]
    fn the_bytes_before_an_error_are_handed_out() {
        let directory = directory("error");
        let data = repetitive(600_000);
        for name in ["a.fa.gz", "a.fa.lz4", "a.fa.xz", "a.fa.zst"] {
            let mut compressed = encoded(name, &data);
            compressed.truncate(compressed.len() * 3 / 4);
            let path = directory.join(name);
            std::fs::write(&path, compressed).unwrap();
            for buffer_size in [None, Some(4096)] {
                for mut reader in readers(buffer_size) {
                    let mut bytes = Vec::new();
                    let result = reader.read_file(&path, |event| match event {
                        BinaryEvent::Chunk(chunk)
                        | BinaryEvent::ChunkWithCopybacks { chunk, .. } => {
                            bytes.extend_from_slice(chunk)
                        }
                        BinaryEvent::FilePath { .. } => {}
                    });
                    assert!(result.is_err(), "{name}: the truncation went unnoticed");
                    assert!(!bytes.is_empty() && data.starts_with(&bytes), "{name}");
                }
            }
        }
        std::fs::remove_dir_all(directory).unwrap();
    }

    /// The ratio comes from the metadata of a file of its own where it records the decoded size
    /// (xz always, gzip here), and is measured on the bytes decoded before the first chunk
    /// otherwise (zstd and lz4 without content sizes, the members of an archive).
    #[test]
    fn files_that_compress_too_little_get_no_copybacks() {
        let directory = directory("ratio");
        let incompressible = random_bytes(300_000);
        // A short period, which even the fastest gzip finds.
        let compressible = b"GATTACA\n".repeat(75_000);
        let settings = CopybacksSettings {
            window: 64 * 1024,
            min_len: 16,
            min_compression_ratio: 3.0,
        };
        let mut members = Vec::new();
        let mut expected = Vec::new();
        for extension in ["gz", "lz4", "xz", "zst"] {
            for (kind, data, base) in [
                ("incompressible", &incompressible, OffsetBase::None),
                ("compressible", &compressible, OffsetBase::New),
            ] {
                let name = format!("{kind}.fa.{extension}");
                let path = directory.join(&name);
                std::fs::write(&path, encoded(&name, data)).unwrap();
                let mut reader = GenericBinaryReader::with_copybacks(Some(4096), settings.clone());
                let streams = [stream(&name, data)];
                let files = read_checked(&mut reader, &path, settings.window, &streams);
                let path = path.to_str().unwrap().to_owned();
                assert!(files == [(path, base, data.clone())], "{name}");

                members.push((name.clone(), encoded(&name, data)));
                expected.push((name, base, data.clone()));
            }
        }

        let path = directory.join("input.tar");
        std::fs::write(&path, archive(&members)).unwrap();
        let streams: Vec<_> = (expected.iter())
            .filter(|(_, base, _)| *base == OffsetBase::New)
            .map(|(name, _, data)| stream(name, data))
            .collect();
        let mut reader = GenericBinaryReader::with_copybacks(Some(4096), settings.clone());
        let files = read_checked(&mut reader, &path, settings.window, &streams);
        for (name, _, _) in &mut expected {
            *name = format!("{}:{name}", path.display());
        }
        assert!(files == expected);
        std::fs::remove_dir_all(directory).unwrap();
    }
}
