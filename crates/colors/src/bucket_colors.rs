//! The temporary color buffers shared by every phase that accumulates colors.
//!
//! A set is held as a sorted list of maximal [`ColorRun`]s, never as one `u32`
//! per color. The representation is the same everywhere and at every moment:
//! the reads-bucketing compactor merges the runs of every repeated superkmer
//! occurrence into a [`ColorArena`], the kmers-merge manager accumulates one
//! arena set per hash-map entry, and the bucket codec reads and writes runs
//! directly. Nothing on any path expands a run into the colors it stands for.
//!
//! A handle is stored inline for every superkmer the compactor holds, because
//! `memstorage_encode_read` reserves `size_of::<E>()` per entry, so the handle
//! is exactly the allocator's two-word vector and nothing more. A set of one
//! run -- three sets in four -- lives entirely in that handle and never touches
//! the slab.
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

/// A maximal run of consecutive colors, packed so that the first color occupies
/// the high half of the word.
///
/// That placement is the whole reason the type is a packed integer rather than a
/// pair of fields: sorting an array of runs is a plain `sort_unstable`, which
/// orders by first color and breaks ties by length, and no comparator or key
/// extractor is needed on the one path that has to sort. The length is stored
/// one less than it is so that a run covering the entire color space still fits.
#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Default, Hash)]
#[repr(transparent)]
pub struct ColorRun(u64);

const RUN_START_SHIFT: u32 = 32;
const RUN_LENGTH_MASK: u64 = u32::MAX as u64;

impl ColorRun {
    /// `len` colors starting at `start`. A length is at least one and at most
    /// the whole color space.
    #[inline(always)]
    pub fn new(start: ColorIndexType, len: u64) -> Self {
        debug_assert!(len >= 1 && len <= (RUN_LENGTH_MASK + 1));
        debug_assert!((start as u64) + len <= RUN_LENGTH_MASK + 1);
        Self(((start as u64) << RUN_START_SHIFT) | (len - 1))
    }

    /// The run covering `start .. end`, with `end` exclusive and in `u64`
    /// because a run may reach one past the last color.
    #[inline(always)]
    pub fn from_bounds(start: ColorIndexType, end: u64) -> Self {
        Self::new(start, end - start as u64)
    }

    #[inline(always)]
    pub fn single(color: ColorIndexType) -> Self {
        Self::new(color, 1)
    }

    #[inline(always)]
    pub fn start(&self) -> ColorIndexType {
        (self.0 >> RUN_START_SHIFT) as ColorIndexType
    }

    #[inline(always)]
    pub fn len(&self) -> u64 {
        (self.0 & RUN_LENGTH_MASK) + 1
    }

    /// One past the last color, in `u64` so that a run ending at the top of the
    /// color space does not wrap.
    #[inline(always)]
    pub fn end(&self) -> u64 {
        self.start() as u64 + self.len()
    }

    #[inline(always)]
    pub fn last(&self) -> ColorIndexType {
        (self.end() - 1) as ColorIndexType
    }

    #[inline(always)]
    pub fn contains(&self, color: ColorIndexType) -> bool {
        color >= self.start() && (color as u64) < self.end()
    }

    /// Takes `self` by value so the returned iterator borrows nothing.
    #[inline(always)]
    pub fn colors(self) -> impl Iterator<Item = ColorIndexType> {
        let start = self.start();
        (0..self.len()).map(move |offset| start + offset as ColorIndexType)
    }
}

/// The colors of a run list, one at a time. Only for callers that genuinely
/// need individual identifiers, such as the text output paths.
#[inline]
pub fn expand_runs(runs: &[ColorRun]) -> impl Iterator<Item = ColorIndexType> + '_ {
    runs.iter().flat_map(|run| run.colors())
}

/// Total number of colors a run list stands for.
#[inline]
pub fn runs_cardinality(runs: &[ColorRun]) -> u64 {
    runs.iter().map(|run| run.len()).sum()
}

/// Rewrites a color list as runs.
///
/// A sorted, deduplicated list becomes its maximal runs directly. Anything else
/// still comes out as a correct, if not maximal, cover of the same set, which
/// [`canonicalize`] then reduces -- so a caller may build a set from whatever
/// order it has and prepare it afterwards.
pub fn runs_from_colors(colors: &[ColorIndexType], out: &mut Vec<ColorRun>) {
    out.clear();
    let mut index = 0;
    while index < colors.len() {
        let first = colors[index];
        // Measuring every color against the one that opened the run, rather
        // than against its predecessor, leaves no dependency between successive
        // tests, so the scan can run several colors ahead at once. The widening
        // is what keeps an unsorted list from wrapping the subtraction.
        let mut end = index + 1;
        while end < colors.len() && colors[end] as u64 == first as u64 + (end - index) as u64 {
            end += 1;
        }
        out.push(ColorRun::new(first, (end - index) as u64));
        index = end;
    }
}

/// One inline run, which is a whole set for three sets in four. The allocator
/// keeps its size class in the vector's length word rather than in the union, so
/// every bit of the run is payload and a first color at or above 2^31 is stored
/// like any other.
type ColorAllocator = Allocator<ColorRun, 1>;
type ColorVec = InlineVec<ColorRun, 1>;

/// The longest varint either half of a run record can produce. A gap is shifted
/// up by one before it is written, so the widest value is just under 2^33, and a
/// length just under 2^32; five seven-bit groups cover both.
const RUN_VARINT_MAX_SIZE: usize = 5;

/// Below this the compaction that bounds an out-of-order accumulation is not
/// worth its sort, and the inline run alone keeps the set small.
const COMPACT_FLOOR: usize = 8;

/// A set of colors owned by a [`ColorArena`].
///
/// Sorted, disjoint and non-adjacent once [`ColorArena::prepare`] has run, and
/// already so for a set that was just decoded from a bucket.
#[derive(Copy, Clone, Debug, Default)]
pub struct ColorHandle(ColorVec);

impl ColorHandle {
    /// An upper bound on the serialized size.
    ///
    /// Every run costs a gap varint and, when it holds more than one color, a
    /// length varint. The bound is what the bucket dispatcher reserves before
    /// every write, so keeping it close to the truth is what keeps the bucket
    /// buffers full -- and with a handful of runs where there used to be a
    /// couple of hundred colors, it now is.
    pub fn encoded_len(&self) -> usize {
        VARINT_MAX_SIZE + self.0.len() * 2 * RUN_VARINT_MAX_SIZE
    }

    /// Number of runs, not of colors.
    pub fn runs_count(&self) -> usize {
        self.0.len()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
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

/// Emits a varint, taking the one-byte case first: almost every gap of a color
/// set is below 128, and the general encoder is a loop over a scratch array that
/// is pure overhead for them.
#[inline(always)]
fn put_varint(sink: &mut impl ByteSink, value: u64) {
    if value < 0x80 {
        sink.put_byte(value as u8);
    } else {
        encode_varint(|bytes| sink.put(bytes), value);
    }
}

/// Writes the canonical bucket encoding of a sorted, disjoint run list: the run
/// count minus one, then one record per run.
///
/// A record is the gap from the previous run's last color -- the first run's
/// gap is its absolute first color -- shifted up by one bit, with the freed low
/// bit saying whether a length follows. Spending that bit rather than a whole
/// varint is what keeps a one-colour set at two bytes while still letting the
/// decoder frame every record on its own, which is what reading runs back
/// without expanding them requires.
fn encode_canonical(runs: &[ColorRun], sink: &mut impl ByteSink) {
    debug_assert!(!runs.is_empty());
    debug_assert!(
        runs.windows(2)
            .all(|pair| pair[0].end() < pair[1].start() as u64)
    );
    put_varint(sink, (runs.len() - 1) as u64);
    let mut last = 0u64;
    for run in runs {
        let gap = run.start() as u64 - last;
        let len = run.len();
        put_varint(sink, (gap << 1) | (len > 1) as u64);
        if len > 1 {
            put_varint(sink, len - 2);
        }
        last = run.last() as u64;
    }
}

/// Reads one encoded set into `slice`, whose length is the set's run count.
fn decode_body(source: &mut impl VarintSource, slice: &mut [ColorRun]) -> Option<()> {
    // A decoded run count is always at least one, but this is the one
    // precondition the writes below do not check for themselves.
    if slice.is_empty() {
        return None;
    }
    // Positions accumulate in a `u64` so that a record running off the end of
    // the color space is caught by the one test below rather than by a check on
    // every field.
    let mut last = 0u64;
    for slot in slice.iter_mut() {
        let head = source.next_varint()?;
        let start = last.checked_add(head >> 1)?;
        let len = if head & 1 != 0 {
            source.next_varint()?.checked_add(2)?
        } else {
            1
        };
        last = start.checked_add(len)?.checked_sub(1)?;
        if last > ColorIndexType::MAX as u64 {
            return None;
        }
        *slot = ColorRun::new(start as ColorIndexType, len);
    }
    Some(())
}

/// Sorts runs and merges every overlapping or adjacent pair, leaving the one
/// canonical form of the set: sorted, disjoint, non-adjacent, maximal.
///
/// The merge has to take the further of the two ends rather than extend the
/// tail by the incoming length. Sorting orders equal first colors by length, so
/// a run nested inside another arrives immediately after it, and extending by
/// the length would invent colors past the end of both.
pub fn canonicalize(runs: &mut [ColorRun]) -> &[ColorRun] {
    if runs.len() <= 1 {
        return runs;
    }

    // Runs usually arrive in color order, so the sort is skippable. The test is
    // strict because two adjacent runs are already ordered but still have to be
    // merged into one.
    if runs
        .windows(2)
        .all(|pair| pair[0].end() < pair[1].start() as u64)
    {
        return runs;
    }

    runs.sort_unstable();

    let mut size = 1;
    for index in 1..runs.len() {
        let run = runs[index];
        let tail = runs[size - 1];
        if (run.start() as u64) <= tail.end() {
            runs[size - 1] = ColorRun::from_bounds(tail.start(), tail.end().max(run.end()));
        } else {
            runs[size] = run;
            size += 1;
        }
    }
    &runs[..size]
}

/// Slab holding the runs of every set in one sub-bucket, reclaimed as a whole
/// at reset so that its capacity is reused by the next one.
pub struct ColorArena {
    colors: ColorAllocator,
}

impl io::concurrent::temp_reads::extra_data::BoundedTempBuffer for ColorArena {
    /// A set of one run lives inline in its handle, so a producer that only ever
    /// builds singletons touches no slab at all and a budget of zero is right.
    fn with_budget(bytes: usize) -> Self {
        Self::with_slab_capacity(bytes / size_of::<ColorRun>())
    }
    fn live_bytes(&self) -> usize {
        self.used_capacity() * size_of::<ColorRun>()
    }
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

    /// An arena whose slab starts at `elements` runs rather than at the
    /// allocator's default, for a caller working to a memory budget. See
    /// [`Allocator::with_slab_capacity`]: [`Self::new`]'s argument sizes only
    /// the freelists.
    pub fn with_slab_capacity(elements: usize) -> Self {
        Self {
            colors: ColorAllocator::with_slab_capacity(elements),
        }
    }

    pub fn reset(&mut self) {
        self.colors.reset();
    }

    pub fn copy_from(&mut self, source: &Self) {
        self.colors.copy_from(&source.colors);
    }

    pub fn used_capacity(&self) -> usize {
        self.colors.used_capacity()
    }

    /// The set's runs, canonical for a decoded or prepared set.
    ///
    /// The handle shares the returned lifetime because a one-run set lives in
    /// the handle itself, not in the slab: borrowing it for only as long as the
    /// arena would hand out a pointer into a handle that may already be gone.
    pub fn runs<'a>(&'a self, handle: &'a ColorHandle) -> &'a [ColorRun] {
        self.colors.slice_vec(&handle.0)
    }

    /// The colors of a set, one at a time. Only for callers that need
    /// individual identifiers; nothing on the accumulation paths does.
    pub fn colors<'a>(
        &'a self,
        handle: &'a ColorHandle,
    ) -> impl Iterator<Item = ColorIndexType> + 'a {
        expand_runs(self.runs(handle))
    }

    /// Appends the set's colors to `out`, for the boundaries that still take an
    /// expanded slice.
    pub fn expand_into(&self, handle: &ColorHandle, out: &mut Vec<ColorIndexType>) {
        out.clear();
        out.reserve(runs_cardinality(self.runs(handle)) as usize);
        out.extend(self.colors(handle));
    }

    /// The only color of a single-color set.
    pub fn unique_color(&self, handle: &ColorHandle) -> ColorIndexType {
        let runs = self.runs(handle);
        // The compactor takes this path for every multiplicity-one superkmer
        // and deliberately skips `prepare` first, so the invariant it relies on
        // is worth stating: reading a wider set here would silently return its
        // lowest color rather than fail.
        debug_assert!(runs.len() == 1 && runs[0].len() == 1);
        runs[0].start()
    }

    pub fn singleton(&mut self, color: ColorIndexType) -> ColorHandle {
        self.from_runs(&[ColorRun::single(color)])
    }

    /// Builds a set from an already canonical run list.
    pub fn from_runs(&mut self, runs: &[ColorRun]) -> ColorHandle {
        let mut vec = self.colors.new_vec(runs.len());
        self.colors.slice_vec_mut(&mut vec).copy_from_slice(runs);
        ColorHandle(vec)
    }

    /// Builds a set from an already sorted, deduplicated color list, coalescing
    /// it into runs on the way in.
    pub fn from_colors(&mut self, colors: &[ColorIndexType]) -> ColorHandle {
        let mut runs = Vec::new();
        runs_from_colors(colors, &mut runs);
        self.from_runs(&runs)
    }

    /// Reads one set in the canonical bucket encoding, straight into runs.
    fn decode_from(&mut self, source: &mut impl VarintSource) -> Option<ColorHandle> {
        let count = usize::try_from(source.next_varint()?)
            .ok()?
            .checked_add(1)?;
        let mut vec = self.colors.new_vec(count);
        if decode_body(source, self.colors.slice_vec_mut(&mut vec)).is_none() {
            self.colors.free_vec(&mut vec);
            return None;
        }
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

    /// Fresh handle holding a copy of another arena's set. The runs cross
    /// directly; nothing is encoded or decoded on the way.
    pub fn copy_entry(&mut self, source: &Self, other: &ColorHandle) -> ColorHandle {
        self.from_runs(source.runs(other))
    }

    /// Merges a set held by another arena into this one. `source` must not be
    /// this arena; the compactor always merges an input buffer into an output.
    pub fn append_from(&mut self, handle: &mut ColorHandle, source: &Self, other: &ColorHandle) {
        self.extend(handle, source.runs(other));
    }

    /// Appends runs to a set, keeping it canonical without sorting wherever it
    /// can. The runs must not be borrowed from this arena: appending can move
    /// the set, invalidating any slice into the slab.
    ///
    /// The tail fast path absorbs a run that continues or overlaps the last one
    /// stored, which is what the ordinary ascending feed looks like; a set built
    /// one color at a time in order therefore never holds more than one run at
    /// any moment. Anything else is appended as it comes and compacted when the
    /// vector would otherwise have to grow.
    /// The compactor folds chunks newest first, so blocks of ascending
    /// runs arrive in descending order, and without it a set would hold one
    /// record per append until the single `prepare` before serialization --
    /// twice the memory of storing the colors outright.
    pub fn extend(&mut self, handle: &mut ColorHandle, runs: &[ColorRun]) {
        if runs.is_empty() {
            return;
        }
        let mut runs = runs;

        let len = handle.0.len();
        if len > 0 {
            let tail = self.colors.slice_vec(&handle.0)[len - 1];
            let first = runs[0];
            if first.start() >= tail.start() && (first.start() as u64) <= tail.end() {
                let merged = ColorRun::from_bounds(tail.start(), tail.end().max(first.end()));
                self.colors.slice_vec_mut(&mut handle.0)[len - 1] = merged;
                runs = &runs[1..];
                if runs.is_empty() {
                    return;
                }
            }
        }

        if handle.0.len() + runs.len() > handle.0.capacity().max(COMPACT_FLOOR) {
            self.prepare(handle);
        }
        self.colors.extend_vec(&mut handle.0, runs);
    }

    /// Appends a sorted, deduplicated color list to a set.
    pub fn extend_colors(&mut self, handle: &mut ColorHandle, colors: &[ColorIndexType]) {
        let mut runs = Vec::new();
        runs_from_colors(colors, &mut runs);
        self.extend(handle, &runs);
    }

    /// Copies a set held by this arena and appends `extra` to the copy, so that
    /// the original is left intact. Used when several k-mers share an entry and
    /// one of them diverges. `extra` must not be borrowed from this arena.
    ///
    /// The copy and the append share one allocation: this is on the path taken
    /// once per k-mer occurrence that splits off a shared set.
    pub fn branch_extended(&mut self, handle: &ColorHandle, extra: &[ColorRun]) -> ColorHandle {
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

    /// Sorts and merges in place, leaving the set canonical and ready to
    /// serialize.
    pub fn prepare(&mut self, handle: &mut ColorHandle) {
        let size = canonicalize(self.colors.slice_vec_mut(&mut handle.0)).len();
        unsafe { handle.0.set_len(size) };
    }

    /// Writes the canonical encoding, streaming straight from the runs.
    pub fn write_to(&self, handle: &ColorHandle, writer: &mut impl Write) {
        let runs = self.runs(handle);
        if runs.is_empty() {
            return;
        }
        let mut sink = WriteSink::new(writer);
        encode_canonical(runs, &mut sink);
        sink.finish();
    }

    /// Writes the canonical encoding into a buffer, the form the bucket
    /// dispatcher uses: it has already reserved [`ColorHandle::encoded_len`],
    /// so nothing on this path checks capacity.
    pub fn write_to_vec(&self, handle: &ColorHandle, out: &mut Vec<u8>) {
        let runs = self.runs(handle);
        if runs.is_empty() {
            return;
        }
        let mut sink = VecSink::new(out, handle.encoded_len());
        encode_canonical(runs, &mut sink);
        sink.finish();
    }
}

/// Accumulator for the one color set a unitig is built from, distinct from the
/// arena because a single growing set needs no slab: it is filled from the
/// superkmers of a unitig and completed once, right before it is interned.
#[derive(Default, Debug)]
pub struct ColorAccumulator {
    runs: Vec<ColorRun>,
}

impl ColorAccumulator {
    pub fn clear(&mut self) {
        self.runs.clear();
    }

    /// Appends a run list, absorbing it into the tail when it continues it, the
    /// same fast path [`ColorArena::extend`] takes and for the same reason.
    pub fn append_runs(&mut self, runs: &[ColorRun]) {
        let mut runs = runs;
        if let Some(tail) = self.runs.last_mut() {
            if let Some(first) = runs.first() {
                if first.start() >= tail.start() && (first.start() as u64) <= tail.end() {
                    *tail = ColorRun::from_bounds(tail.start(), tail.end().max(first.end()));
                    runs = &runs[1..];
                }
            }
        }
        self.runs.extend_from_slice(runs);
    }

    pub fn append_colors(&mut self, colors: &[ColorIndexType]) {
        let mut runs = Vec::new();
        runs_from_colors(colors, &mut runs);
        self.append_runs(&runs);
    }

    /// Sorts and merges the accumulated runs, leaving the set canonical and
    /// ready to be interned.
    pub fn finish(&mut self) -> &[ColorRun] {
        canonicalize(&mut self.runs)
    }
}

#[cfg(test)]
mod tests;
