//! The temporary color buffers shared by every phase that accumulates colors.
//!
//! A set is held as one expanded `u32` per color. The
//! representation is the same everywhere: the reads-bucketing compactor merges
//! the colors of every repeated superkmer occurrence into a [`ColorArena`], and
//! the kmers-merge manager accumulates one arena set per hash-map entry.
//! Compression is applied on the way out, never in memory: [`ColorArena::
//! write_to`] emits the run-encoded bucket format, and the color map applies
//! its own run-length encoding.
//!
//! A handle is stored inline for every superkmer the compactor holds, because
//! `memstorage_encode_read` reserves `size_of::<E>()` per entry, so the handle
//! is exactly the allocator's two-word vector and nothing more.
//!
//! Both directions of the encoding are written against a byte sink and a byte
//! source rather than `Write` and `Read`
use config::ColorIndexType;
use io::varint::{
    BufVarintSource, PointerVarintSource, SliceVarintSource, VARINT_MAX_SIZE, VarintSource,
    encode_varint,
};
use std::io::{BufRead, Write};
use utils::inline_vec::{Allocator, InlineVec};

/// Exactly one inline color. A second inline slot would put a color's high bit
/// where the allocator keeps its heap flag, so any color at or above 2^31 would
/// be misread as a pointer; with a single slot the upper half of the union
/// stays zero. A second slot measured no faster.
type ColorAllocator = Allocator<ColorIndexType, 1>;
type ColorVec = InlineVec<ColorIndexType, 1>;

/// The longest varint a [`ColorIndexType`] can produce.
const COLOR_VARINT_MAX_SIZE: usize = 5;

/// A set of colors owned by a [`ColorArena`].
///
/// Sorted and deduplicated once [`ColorArena::prepare`] has run, and already
/// so for a set that was just decoded from a bucket.
#[derive(Copy, Clone, Debug, Default)]
pub struct ColorHandle(ColorVec);

impl ColorHandle {
    /// An upper bound on the serialized size.
    ///
    /// Every color costs at most one `u32` varint: a color that stands alone
    /// pays exactly that, a color that opens a run of four or more pays the
    /// delta plus a marker and a run length spread over at least four colors,
    /// and the two unit deltas of a shorter run are a byte each. The bound is
    /// what the bucket dispatcher reserves before every write, so keeping it
    /// close to the truth is what keeps the bucket buffers full.
    pub fn encoded_len(&self) -> usize {
        VARINT_MAX_SIZE + self.0.len() * COLOR_VARINT_MAX_SIZE
    }
}

/// Destination for the canonical encoding.
///
/// The point of the trait is that [`VecSink`] can write into capacity that is
/// already reserved, so a varint costs a store and nothing else.
trait ByteSink {
    fn put_byte(&mut self, byte: u8);
    fn put(&mut self, bytes: &[u8]);
}

/// Writes into the spare capacity of a `Vec`, which the bucket dispatcher has
/// already sized with [`ColorHandle::encoded_len`], so no write checks capacity.
struct VecSink<'a> {
    out: &'a mut Vec<u8>,
    len: usize,
    limit: usize,
}

impl<'a> VecSink<'a> {
    fn new(out: &'a mut Vec<u8>, budget: usize) -> Self {
        // A no-op whenever the caller reserved, which the bucket dispatcher
        // always does; it is what makes every write below sound.
        out.reserve(budget);
        let len = out.len();
        Self {
            out,
            len,
            limit: len + budget,
        }
    }

    fn finish(self) {
        // SAFETY: every byte in `len` was written through the pointer below,
        // and `len` never passed `limit`, which is within the reserved capacity.
        unsafe { self.out.set_len(self.len) };
    }
}

impl ByteSink for VecSink<'_> {
    #[inline(always)]
    fn put_byte(&mut self, byte: u8) {
        debug_assert!(self.len < self.limit);
        // SAFETY: `limit` bytes were reserved and `len` is below it.
        unsafe { self.out.as_mut_ptr().add(self.len).write(byte) };
        self.len += 1;
    }

    #[inline(always)]
    fn put(&mut self, bytes: &[u8]) {
        debug_assert!(self.len + bytes.len() <= self.limit);
        // SAFETY: as above, for a run of bytes that still fits in `limit`.
        unsafe {
            std::ptr::copy_nonoverlapping(
                bytes.as_ptr(),
                self.out.as_mut_ptr().add(self.len),
                bytes.len(),
            );
        }
        self.len += bytes.len();
    }
}

/// How much of a set is batched before a generic writer is called at all.
const WRITE_BLOCK: usize = 512;

/// Fallback for a writer that is not a `Vec`: one `write_all` per block instead
/// of one per varint.
struct WriteSink<'a, W: Write> {
    writer: &'a mut W,
    buffer: [u8; WRITE_BLOCK],
    len: usize,
}

impl<'a, W: Write> WriteSink<'a, W> {
    fn new(writer: &'a mut W) -> Self {
        Self {
            writer,
            buffer: [0; WRITE_BLOCK],
            len: 0,
        }
    }

    fn flush(&mut self) {
        self.writer.write_all(&self.buffer[..self.len]).unwrap();
        self.len = 0;
    }

    fn finish(mut self) {
        self.flush();
    }
}

impl<W: Write> ByteSink for WriteSink<'_, W> {
    #[inline(always)]
    fn put_byte(&mut self, byte: u8) {
        if self.len == WRITE_BLOCK {
            self.flush();
        }
        self.buffer[self.len] = byte;
        self.len += 1;
    }

    #[inline(always)]
    fn put(&mut self, bytes: &[u8]) {
        // A single varint never approaches a block, so one flush always frees
        // enough room.
        debug_assert!(bytes.len() <= WRITE_BLOCK);
        if self.len + bytes.len() > WRITE_BLOCK {
            self.flush();
        }
        self.buffer[self.len..self.len + bytes.len()].copy_from_slice(bytes);
        self.len += bytes.len();
    }
}

/// Emits a varint, taking the one-byte case first: almost every delta of a
/// color set is below 128, and the general encoder is a loop over a scratch
/// array that is pure overhead for them.
#[inline(always)]
fn put_varint(sink: &mut impl ByteSink, value: u64) {
    if value < 0x80 {
        sink.put_byte(value as u8);
    } else {
        encode_varint(|bytes| sink.put(bytes), value);
    }
}

/// Writes the canonical bucket encoding of a sorted, deduplicated set: the
/// expanded length minus one, the absolute first color, then positive deltas,
/// with a zero marker introducing the tail of every run of four or more.
fn encode_canonical(colors: &[ColorIndexType], sink: &mut impl ByteSink) {
    debug_assert!(colors.windows(2).all(|pair| pair[0] < pair[1]));
    debug_assert!(!colors.is_empty());
    put_varint(sink, (colors.len() - 1) as u64);
    let mut last = 0;
    let mut start = 0;
    while start < colors.len() {
        let first = colors[start];
        put_varint(sink, (first - last) as u64);
        // Measuring every color against the one that opened the run, rather
        // than against its predecessor, leaves no dependency between successive
        // tests, so the scan can run several colors ahead at once.
        let mut end = start + 1;
        while end < colors.len() && colors[end] - first == (end - start) as ColorIndexType {
            end += 1;
        }
        let run = end - start;
        if run >= 4 {
            sink.put_byte(0);
            put_varint(sink, (run - 4) as u64);
        } else if run > 1 {
            // At most two unit deltas, written together. A run of one writes
            // nothing at all, which is the common case and worth not calling
            // the sink for.
            sink.put(&[1, 1][..run - 1]);
        }
        last = colors[end - 1];
        start = end;
    }
}

/// Expands one encoded set into `slice`, whose length is the set's own.
fn decode_body(source: &mut impl VarintSource, slice: &mut [ColorIndexType]) -> Option<()> {
    // A decoded length is always at least one, but this is the one precondition
    // the writes below do not check for themselves.
    if slice.is_empty() {
        return None;
    }
    // The colors accumulate in a `u64` so that the loop carries no overflow
    // check: `last` only ever grows, so the single test at the end rejects
    // exactly the sets a check on every delta would have rejected. Saturating
    // keeps that true without wrapping past the test.
    let mut last = source.next_varint()?;
    let start = slice.as_mut_ptr();
    // SAFETY: `slice` holds exactly the colors of the set, so `end` is its one
    // past the end pointer and every write below is guarded against it.
    let end = unsafe { start.add(slice.len()) };
    let mut cursor = start;
    unsafe { cursor.write(last as ColorIndexType) };
    cursor = unsafe { cursor.add(1) };

    while cursor < end {
        let delta = source.next_varint()?;
        if delta != 0 {
            last = last.saturating_add(delta);
            // SAFETY: the loop condition leaves room for one color.
            unsafe { cursor.write(last as ColorIndexType) };
            cursor = unsafe { cursor.add(1) };
        } else {
            // Zero cannot be a delta in a deduplicated set. It introduces the
            // tail of a run whose first color was already emitted.
            let additional = source.next_varint()?.checked_add(3)?;
            // SAFETY: both pointers are into `slice` and `cursor` is below `end`.
            if additional > unsafe { end.offset_from(cursor) } as u64 {
                return None;
            }
            let additional = additional as usize;
            let first = last.saturating_add(1) as ColorIndexType;
            last = last.saturating_add(additional as u64);
            for offset in 0..additional {
                // SAFETY: the check above left room for `additional` colors.
                unsafe {
                    cursor
                        .add(offset)
                        .write(first.wrapping_add(offset as ColorIndexType))
                };
            }
            cursor = unsafe { cursor.add(additional) };
        }
    }
    (last <= ColorIndexType::MAX as u64).then_some(())
}

/// Slab holding the colors of every set in one sub-bucket, reclaimed as a whole
/// at reset so that its capacity is reused by the next one.
pub struct ColorArena {
    colors: ColorAllocator,
}

impl Default for ColorArena {
    fn default() -> Self {
        Self::new(0)
    }
}

impl ColorArena {
    pub fn new(capacity: usize) -> Self {
        Self {
            colors: ColorAllocator::new(capacity),
        }
    }

    pub fn reset(&mut self) {
        self.colors.reset();
    }

    pub fn copy_from(&mut self, source: &Self) {
        self.colors.copy_from(&source.colors);
    }

    /// The set's colors, sorted and deduplicated for a decoded or prepared set.
    pub fn colors<'a>(&'a self, handle: &ColorHandle) -> &'a [ColorIndexType] {
        self.colors.slice_vec(&handle.0)
    }

    /// The only color of a single-color set.
    pub fn unique_color(&self, handle: &ColorHandle) -> ColorIndexType {
        self.colors(handle)[0]
    }

    pub fn singleton(&mut self, color: ColorIndexType) -> ColorHandle {
        self.from_colors(&[color])
    }

    /// Builds a set from an already sorted, deduplicated color list.
    pub fn from_colors(&mut self, colors: &[ColorIndexType]) -> ColorHandle {
        let mut vec = self.colors.new_vec(colors.len());
        self.colors.slice_vec_mut(&mut vec).copy_from_slice(colors);
        ColorHandle(vec)
    }

    /// Reads one set in the canonical bucket encoding, expanding it in a single
    /// pass.
    fn decode_from(&mut self, source: &mut impl VarintSource) -> Option<ColorHandle> {
        let len = usize::try_from(source.next_varint()?)
            .ok()?
            .checked_add(1)?;
        let mut vec = self.colors.new_vec(len);
        decode_body(source, self.colors.slice_vec_mut(&mut vec))?;
        Some(ColorHandle(vec))
    }

    /// Reads one set from a stream. Prefer [`Self::decode_from_slice`] wherever
    /// the record is already in memory: a stream costs one call per byte and
    /// gives up the word-at-a-time varint decode.
    pub fn decode(&mut self, reader: &mut impl BufRead) -> Option<ColorHandle> {
        self.decode_from(&mut BufVarintSource::new(reader))
    }

    /// Reads one set out of a buffer, returning it with the number of bytes it
    /// occupied so the caller can walk on to the next record.
    pub fn decode_from_slice(&mut self, bytes: &[u8]) -> Option<(ColorHandle, usize)> {
        let mut source = SliceVarintSource::new(bytes);
        let handle = self.decode_from(&mut source)?;
        Some((handle, source.position()))
    }

    /// Reads one set from a buffer whose end is not known.
    ///
    /// # Safety
    /// A whole encoded set must start at `ptr`.
    pub unsafe fn decode_from_pointer(&mut self, ptr: *const u8) -> Option<ColorHandle> {
        self.decode_from(&mut unsafe { PointerVarintSource::new(ptr) })
    }

    /// Fresh handle holding a copy of another arena's set.
    pub fn copy_entry(&mut self, source: &Self, other: &ColorHandle) -> ColorHandle {
        self.from_colors(source.colors(other))
    }

    /// Merges a set held by another arena into this one. `source` must not be
    /// this arena; the compactor always merges an input buffer into an output.
    pub fn append_from(&mut self, handle: &mut ColorHandle, source: &Self, other: &ColorHandle) {
        self.colors.extend_vec(&mut handle.0, source.colors(other));
    }

    /// Appends a color list to a set. The list must not be borrowed from this
    /// arena: appending can move the set, invalidating any slice into the slab.
    pub fn extend(&mut self, handle: &mut ColorHandle, colors: &[ColorIndexType]) {
        self.colors.extend_vec(&mut handle.0, colors);
    }

    /// Copies a set held by this arena and appends `extra` to the copy, so that
    /// the original is left intact. Used when several k-mers share an entry and
    /// one of them diverges. `extra` must not be borrowed from this arena.
    ///
    /// The copy and the append share one allocation: this is on the path taken
    /// once per k-mer occurrence that splits off a shared set.
    pub fn branch_extended(
        &mut self,
        handle: &ColorHandle,
        extra: &[ColorIndexType],
    ) -> ColorHandle {
        let copied = handle.0.len();
        // The slab reallocates on growth, so reserve the destination before
        // reading the source, exactly as the allocator requires.
        let mut vec = self.colors.new_vec(copied + extra.len());
        let source = if copied == 0 {
            &[][..]
        } else {
            unsafe { self.colors.slice_vec_static(&handle.0) }
        };
        let target = self.colors.slice_vec_mut(&mut vec);
        target[..copied].copy_from_slice(source);
        target[copied..].copy_from_slice(extra);
        ColorHandle(vec)
    }

    /// Returns a set's storage to the slab freelist.
    pub fn free(&mut self, handle: &mut ColorHandle) {
        self.colors.free_vec(&mut handle.0);
    }

    /// Sorts and deduplicates in place, leaving the set ready to serialize.
    pub fn prepare(&mut self, handle: &mut ColorHandle) {
        let size = sort_dedup(self.colors.slice_vec_mut(&mut handle.0)).len();
        unsafe { handle.0.set_len(size) };
    }

    /// Writes the canonical encoding, streaming straight from the colors.
    pub fn write_to(&self, handle: &ColorHandle, writer: &mut impl Write) {
        let colors = self.colors(handle);
        if colors.is_empty() {
            return;
        }
        let mut sink = WriteSink::new(writer);
        encode_canonical(colors, &mut sink);
        sink.finish();
    }

    /// Writes the canonical encoding into a buffer, the form the bucket
    /// dispatcher uses: it has already reserved [`ColorHandle::encoded_len`],
    /// so nothing on this path checks capacity.
    pub fn write_to_vec(&self, handle: &ColorHandle, out: &mut Vec<u8>) {
        let colors = self.colors(handle);
        if colors.is_empty() {
            return;
        }
        let mut sink = VecSink::new(out, handle.encoded_len());
        encode_canonical(colors, &mut sink);
        sink.finish();
    }
}

/// Accumulator for the one color set a unitig is built from, distinct from the
/// arena because a single growing set needs no slab: it is filled from the
/// superkmers of a unitig and completed once, right before it is interned.
#[derive(Default, Debug)]
pub struct ColorAccumulator {
    colors: Vec<ColorIndexType>,
}

impl ColorAccumulator {
    pub fn clear(&mut self) {
        self.colors.clear();
    }

    pub fn append_colors(&mut self, colors: &[ColorIndexType]) {
        self.colors.extend_from_slice(colors);
    }

    /// Sorts and deduplicates the accumulated colors, leaving the set ready to
    /// be interned.
    pub fn finish(&mut self) -> &[ColorIndexType] {
        sort_dedup(&mut self.colors)
    }
}

/// Sorts and deduplicates in place, returning the surviving prefix.
fn sort_dedup(colors: &mut [ColorIndexType]) -> &[ColorIndexType] {
    if colors.is_empty() {
        return colors;
    }
    // Occurrences usually arrive in color order, so the sort is skippable.
    if !colors.is_sorted() {
        colors.sort_unstable();
    }
    let mut size = 1;
    for index in 1..colors.len() {
        if colors[size - 1] != colors[index] {
            colors[size] = colors[index];
            size += 1;
        }
    }
    &colors[..size]
}

#[cfg(test)]
mod tests {
    use super::*;

    /// One handle is stored inline per superkmer the compactor holds, so its
    /// size is a direct per-superkmer memory and I/O cost.
    #[test]
    fn handle_stays_two_words() {
        assert_eq!(std::mem::size_of::<ColorHandle>(), 16);
    }

    fn round_trip(arena: &ColorArena, handle: &ColorHandle) -> Vec<ColorIndexType> {
        let mut bytes = Vec::new();
        arena.write_to(handle, &mut bytes);
        let mut decoded = ColorArena::default();
        let handle = decoded.decode(&mut bytes.as_slice()).unwrap();
        decoded.colors(&handle).to_vec()
    }

    #[test]
    fn prepare_sorts_deduplicates_and_round_trips() {
        let mut arena = ColorArena::default();
        let single = arena.singleton(7);
        assert_eq!(arena.unique_color(&single), 7);
        assert_eq!(round_trip(&arena, &single), [7]);

        let mut set = arena.from_colors(&[9, 4, 4, 6, 5]);
        arena.prepare(&mut set);
        assert_eq!(arena.colors(&set), [4, 5, 6, 9]);
        assert_eq!(round_trip(&arena, &set), [4, 5, 6, 9]);
    }

    #[test]
    fn merging_across_arenas_unions_the_colors() {
        let mut source = ColorArena::default();
        let left = source.from_colors(&[4, 6]);
        let right = source.from_colors(&[5, 6, 7]);

        let mut output = ColorArena::default();
        let mut merged = output.copy_entry(&source, &left);
        output.append_from(&mut merged, &source, &right);
        output.prepare(&mut merged);
        assert_eq!(output.colors(&merged), [4, 5, 6, 7]);
        // Four consecutive colors are a maximal run, not four unit deltas.
        assert_eq!(round_trip(&output, &merged), [4, 5, 6, 7]);
    }

    #[test]
    fn long_runs_stay_compact_and_boundaries_round_trip() {
        let mut arena = ColorArena::default();
        let run = arena.from_colors(&(0..1_000_000).collect::<Vec<_>>());
        let mut bytes = Vec::new();
        arena.write_to(&run, &mut bytes);
        assert_eq!(bytes.len(), 8);

        let boundary = arena.from_colors(&[u32::MAX - 1, u32::MAX]);
        assert_eq!(round_trip(&arena, &boundary), [u32::MAX - 1, u32::MAX]);
    }

    #[test]
    fn rejects_truncated_and_overflowing_streams() {
        let mut arena = ColorArena::default();
        assert!(arena.decode(&mut &[5u8, 1, 0][..]).is_none());
        assert!(arena.decode(&mut &[][..]).is_none());
        assert!(arena.decode_from_slice(&[5u8, 1, 0]).is_none());
        assert!(arena.decode_from_slice(&[]).is_none());
    }

    /// A delta that runs past the color space is rejected even though the
    /// decoder only tests the accumulator once, at the end of the set.
    #[test]
    fn rejects_colors_past_the_color_space() {
        let mut bytes = Vec::new();
        encode_varint(|b| bytes.extend_from_slice(b), 1);
        encode_varint(|b| bytes.extend_from_slice(b), u32::MAX as u64);
        encode_varint(|b| bytes.extend_from_slice(b), u32::MAX as u64);
        let mut arena = ColorArena::default();
        assert!(arena.decode_from_slice(&bytes).is_none());
        assert!(arena.decode(&mut bytes.as_slice()).is_none());

        // A run whose tail leaves the color space is rejected the same way:
        // the set is complete, so only the final test can catch it.
        let mut bytes = Vec::new();
        encode_varint(|b| bytes.extend_from_slice(b), 3);
        encode_varint(|b| bytes.extend_from_slice(b), u32::MAX as u64 - 1);
        bytes.push(0);
        encode_varint(|b| bytes.extend_from_slice(b), 0);
        let mut arena = ColorArena::default();
        assert!(arena.decode_from_slice(&bytes).is_none());
        // The same run one color lower fits, so the rejection above is the
        // overflow check and not a short read.
        let mut bytes = Vec::new();
        encode_varint(|b| bytes.extend_from_slice(b), 3);
        encode_varint(|b| bytes.extend_from_slice(b), u32::MAX as u64 - 3);
        bytes.push(0);
        encode_varint(|b| bytes.extend_from_slice(b), 0);
        let (handle, consumed) = arena.decode_from_slice(&bytes).unwrap();
        assert_eq!(consumed, bytes.len());
        assert_eq!(arena.colors(&handle).last(), Some(&u32::MAX));
    }

    /// Both decoders must agree, and the slice decoder must report exactly the
    /// bytes it consumed so a caller can walk to the next record. The sets are
    /// built to straddle the eight-byte look-ahead: multi-byte deltas near the
    /// end of a buffer take the byte-at-a-time fallback instead.
    #[test]
    fn decoders_agree_and_report_the_consumed_length() {
        let mut arena = ColorArena::default();
        let cases: Vec<Vec<ColorIndexType>> = vec![
            vec![0],
            vec![u32::MAX],
            vec![1, 2, 3, 4, 7, 8, 9, 10, 12, 13],
            vec![0, 1 << 7, 1 << 14, 1 << 21, 1 << 28],
            vec![1 << 28, (1 << 28) + 1, (1 << 28) + 2, (1 << 28) + 3],
            (0..300).map(|index| index * 1_000_003).collect(),
            (0..300).collect(),
        ];
        for colors in cases {
            let handle = arena.from_colors(&colors);
            let mut bytes = Vec::new();
            arena.write_to(&handle, &mut bytes);
            assert!(bytes.len() <= handle.encoded_len(), "{colors:?}");

            let mut vec_bytes = Vec::new();
            arena.write_to_vec(&handle, &mut vec_bytes);
            assert_eq!(vec_bytes, bytes, "{colors:?}");

            let mut decoded = ColorArena::default();
            let (slice_handle, consumed) = decoded.decode_from_slice(&bytes).unwrap();
            assert_eq!(decoded.colors(&slice_handle), &colors[..], "{colors:?}");
            assert_eq!(consumed, bytes.len(), "{colors:?}");

            let stream_handle = decoded.decode(&mut bytes.as_slice()).unwrap();
            assert_eq!(decoded.colors(&stream_handle), &colors[..], "{colors:?}");

            let pointer_handle = unsafe { decoded.decode_from_pointer(bytes.as_ptr()) }.unwrap();
            assert_eq!(decoded.colors(&pointer_handle), &colors[..], "{colors:?}");
        }
    }

    /// `encoded_len` is what the bucket dispatcher reserves, so a set that
    /// encodes larger than its bound would overrun a bucket buffer.
    #[test]
    fn encoded_length_bounds_the_worst_case() {
        let mut arena = ColorArena::default();
        // Sparse colors are the worst case: every one of them pays a full
        // five-byte delta and shares in no run.
        for count in [1usize, 2, 3, 4, 5, 64, 1000] {
            let colors: Vec<ColorIndexType> = (0..count as u32)
                .map(|index| index.wrapping_mul(0x0010_0001) | 0x8000_0000)
                .collect();
            let mut sorted = colors.clone();
            sorted.sort_unstable();
            sorted.dedup();
            let handle = arena.from_colors(&sorted);
            let mut bytes = Vec::new();
            arena.write_to(&handle, &mut bytes);
            assert!(
                bytes.len() <= handle.encoded_len(),
                "{} colors encoded to {} over a bound of {}",
                sorted.len(),
                bytes.len(),
                handle.encoded_len()
            );
        }
    }
}
