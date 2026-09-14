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
use byteorder::ReadBytesExt;
use config::ColorIndexType;
use io::varint::{VARINT_MAX_SIZE, decode_varint, encode_varint};
use std::io::{Read, Write};
use utils::inline_vec::{Allocator, InlineVec};

/// Exactly one inline color. A second inline slot would put a color's high bit
/// where the allocator keeps its heap flag, so any color at or above 2^31 would
/// be misread as a pointer; with a single slot the upper half of the union
/// stays zero. A second slot measured no faster.
type ColorAllocator = Allocator<ColorIndexType, 1>;
type ColorVec = InlineVec<ColorIndexType, 1>;

/// A set of colors owned by a [`ColorArena`].
///
/// Sorted and deduplicated once [`ColorArena::prepare`] has run, and already
/// so for a set that was just decoded from a bucket.
#[derive(Copy, Clone, Debug, Default)]
pub struct ColorHandle(ColorVec);

impl ColorHandle {
    /// An upper bound on the serialized size: a length prefix plus at most one
    /// varint per color.
    pub fn encoded_len(&self) -> usize {
        (self.0.len() + 1) * VARINT_MAX_SIZE
    }
}

/// Writes the canonical bucket encoding of a sorted, deduplicated set: the
/// expanded length minus one, the absolute first color, then positive deltas,
/// with a zero marker introducing the tail of every run of four or more.
fn encode_canonical(colors: &[ColorIndexType], mut emit: impl FnMut(&[u8])) {
    debug_assert!(colors.windows(2).all(|pair| pair[0] < pair[1]));
    debug_assert!(!colors.is_empty());
    encode_varint(&mut emit, (colors.len() - 1) as u64);
    let mut last = 0;
    let mut start = 0;
    while start < colors.len() {
        encode_varint(&mut emit, (colors[start] - last) as u64);
        let mut end = start + 1;
        while end < colors.len() && colors[end] - colors[end - 1] == 1 {
            end += 1;
        }
        let run = end - start;
        if run >= 4 {
            emit(&[0]);
            encode_varint(&mut emit, (run - 4) as u64);
        } else {
            // At most two unit deltas, written together.
            emit(&[1, 1][..run - 1]);
        }
        last = colors[end - 1];
        start = end;
    }
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
    /// pass. The workspace builds release at opt-level 1, so this stays a flat
    /// loop over a byte closure rather than a layered reader.
    pub fn decode(&mut self, reader: &mut impl Read) -> Option<ColorHandle> {
        let len = usize::try_from(decode_varint(|| reader.read_u8().ok())?)
            .ok()?
            .checked_add(1)?;
        let mut vec = self.colors.new_vec(len);
        let slice = self.colors.slice_vec_mut(&mut vec);

        let mut last = ColorIndexType::try_from(decode_varint(|| reader.read_u8().ok())?).ok()?;
        slice[0] = last;
        let mut index = 1;
        while index < len {
            let delta = ColorIndexType::try_from(decode_varint(|| reader.read_u8().ok())?).ok()?;
            if delta != 0 {
                last = last.checked_add(delta)?;
                slice[index] = last;
                index += 1;
            } else {
                // Zero cannot be a delta in a deduplicated set. It introduces
                // the tail of a run whose first color was already emitted.
                let additional =
                    usize::try_from(decode_varint(|| reader.read_u8().ok())?.checked_add(3)?)
                        .ok()?;
                if additional > len - index {
                    return None;
                }
                let first = last.checked_add(1)?;
                last = last.checked_add(ColorIndexType::try_from(additional).ok()?)?;
                for offset in 0..additional {
                    slice[index + offset] = first + offset as ColorIndexType;
                }
                index += additional;
            }
        }
        Some(ColorHandle(vec))
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
        encode_canonical(colors, |part| writer.write_all(part).unwrap());
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
    }
}
