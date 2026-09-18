//! A memory-bounded, parallelism-aware super-kmer deduplicator.
//!
//! One of these sits in front of every uncompacted bucket: the bucketing threads
//! hand it the per-thread buffers they would otherwise have written straight to
//! the bucket file, and it folds repeated super-kmers together before they reach
//! disk. [`BucketsCompactor`](crate::compactor::BucketsCompactor) still does the
//! full job later; this only takes the cheap duplicates out of the way first.
//!
//! Unlike the compactor, it holds to a fixed budget of 3.25 M — two M/2 input
//! buffers, an M bases storage, an M/4 hashmap and an M colour arena — and
//! accepts records from any number of threads without a lock on the fast path.
//! The buffer a drain builds its output in belongs to the draining thread, not
//! to the deduplicator; see [`STAGING`].
//!
//! # Input and output
//!
//! [`BoundedDeduplicator::add`] takes a byte slice holding a whole number of
//! records in the *narrow* form the bucketing threads produce anyway — one
//! colour, no multiplicity:
//!
//! ```text
//! CompressedReadsBucketDataSerializer<
//!     SingleData, WithSecondBucket, NoMultiplicity,
//!     AssemblerMinimizerPosition, FlagsCount, _>
//! ```
//!
//! and writes the *wide* one the compactor reads, carrying the combined colour
//! set and the multiplicity it stands for:
//!
//! ```text
//! CompressedReadsBucketDataSerializer<
//!     MultipleData, WithSecondBucket, WithMultiplicity,
//!     AssemblerMinimizerPosition, FlagsCount, _>
//! ```
//!
//! # Concurrency
//!
//! Two buffers of M/2 hold incoming bytes. `active_epoch` is monotone and the
//! active buffer is `active_epoch & 1`; it is monotone rather than a flag so
//! that "this is still the buffer I saw" stays decidable, which is what rules
//! out an ABA on the index. An append takes the buffer's gate for *reading*,
//! carves out a range with one bounded `fetch_update` — the reservation idiom of
//! `AllocatedChunk::write_bytes_noextend` — and copies into it. A reservation
//! that would pass the end is refused rather than taken, so the cursor is at
//! every moment exactly the number of valid bytes, with no burnt space to
//! recover.
//!
//! The thread whose reservation is refused seals: it takes the state lock, and
//! if the epoch is still the one it saw it is the sealer, bumps the epoch so no
//! new appender enters, then takes the gate for *writing*, which waits out the
//! appenders already inside. Only then is the buffer walked. The other buffer
//! stays writable throughout, which is the point of having two.
//!
//! **Lock order is `state` then `gate`, and only the state holder ever takes a
//! gate write lock.** An appender holds a gate and never asks for `state` — it
//! drops the guard before sealing, inside [`Slot::try_append`], so no caller can
//! hold one across a seal. That is what keeps the two locks from closing a
//! cycle.

use arc_swap::ArcSwapOption;
use config::MultiplicityCounterType;
use hashes::HashableSequence;
use io::concurrent::temp_reads::creads_utils::{
    AssemblerMinimizerPosition, CompressedReadsBucketData, CompressedReadsBucketDataSerializer,
    DeserializedRead, NoAlignmentWithOverflow, NoMultiplicity, WithFixedMultiplicity,
    WithMultiplicity, WithSecondBucket,
};
use io::concurrent::temp_reads::extra_data::{
    BoundedTempBuffer, SequenceExtraDataCombiner, SequenceExtraDataConsecutiveCompression,
    SequenceExtraDataTempBufferManagement, TempBuffer,
};
use io::memstorage::memvarint::compute_memvarint_bytes_count;
use io::memstorage::{memstorage_decode_read, memstorage_encode_read};
use parallel_processor::buckets::bucket_writer::BucketItemSerializer;
use parallel_processor::buckets::{ChunkingStatus, LockFreeBucket, MultiThreadBuckets};
use parking_lot::{Mutex, RwLock};
use std::cell::{RefCell, UnsafeCell};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, AtomicUsize, Ordering};

/// How many times an append retries the buffers before processing itself
/// inline. Bounds the starvation of a thread that keeps losing the seal race;
/// the inline path it falls back to has to exist anyway for oversized slices.
const MAX_APPEND_ROUNDS: usize = 2;

thread_local! {
    /// Where a drain builds its output before handing it to the bucket.
    ///
    /// It belongs to the thread rather than to the deduplicator because nothing
    /// in it outlives the call that fills it: every operation that writes into
    /// it flushes it before returning, so it never carries bytes from one call
    /// to the next, and two deduplicators on one thread can share it. That is
    /// also what makes it safe to be per thread -- bytes left behind by one
    /// thread would otherwise never be flushed, since `finalize` runs on one.
    static STAGING: RefCell<Vec<u8>> = const { RefCell::new(Vec::new()) };
}

/// Spare capacity kept past the bases storage. `memstorage_encode_read` asks for
/// `needed + 8` because the memvarint decoder reads a whole eight byte word past
/// the record it frames.
const STORAGE_SLACK: usize = 32;

/// The single-entry extra data the records arrive carrying.
type SingleOf<MultipleData> = <MultipleData as SequenceExtraDataCombiner>::SingleDataType;

/// What the bucketing threads hand over: one colour and no multiplicity, which
/// is the narrow form the non-deduplicating path writes anyway.
/// Both sides reset their codec per record, because a single colour is written
/// as a delta against the previous one and a sealed buffer holds runs from
/// several threads' buffers back to back. A reset is one store, and it makes a
/// record decodable wherever it ends up.
type InputCodec<MultipleData, FlagsCount> = CompressedReadsBucketDataSerializer<
    SingleOf<MultipleData>,
    WithSecondBucket,
    NoMultiplicity,
    AssemblerMinimizerPosition,
    FlagsCount,
    NoAlignmentWithOverflow,
>;

/// What a drain writes: the combined colour set and the multiplicity it stands
/// for, which is what the compactor reads. `NoAlignmentWithOverflow` differs
/// from `NoAlignment` only in reserving `HASH_MAX_OVERREAD` bytes of slack past
/// a decoded read, which is what makes `compute_hash_aligned_overflow16` — which
/// reads sixteen bytes at a time and so runs past the packed slice — stay inside
/// the allocation. The wire format is identical, and the write path ignores the
/// alignment mode entirely.
type OutputCodec<MultipleData, FlagsCount> = CompressedReadsBucketDataSerializer<
    MultipleData,
    WithSecondBucket,
    WithMultiplicity,
    AssemblerMinimizerPosition,
    FlagsCount,
    NoAlignmentWithOverflow,
>;

/// Everything the deduplicator needs of its extra data type.
pub trait DedupExtraData: SequenceExtraDataCombiner + Sync + Send + Copy + 'static {}

impl<T> DedupExtraData for T where T: SequenceExtraDataCombiner + Sync + Send + Copy + 'static {}

/// How much slab a colour set could cost, used to decide whether an arena
/// operation still fits the budget.
///
/// `max_size` is the set's serialized upper bound, which grows with its run
/// count -- for a run list it is `9 + runs * 10`, comfortably above the
/// `runs * 8` bytes the slab actually holds, so using it as the estimate is
/// conservative. It costs nothing on an extra data type that serializes to
/// nothing.
#[inline(always)]
fn arena_cost<E: DedupExtraData>(extra: &E) -> usize {
    extra.max_size()
}

/// Where a drain's output goes.
///
/// The pipeline wants a bucket of a [`MultiThreadBuckets`], because that is what
/// decides when a chunk is full and therefore when the compactor runs; a test
/// wants a bare writer. Both answer with a [`ChunkingStatus`] so the caller can
/// act on a new chunk.
pub trait DedupOutput: Sync + Send {
    fn write_records(&self, data: &[u8]) -> ChunkingStatus;
}

/// One bucket of a [`MultiThreadBuckets`], which is what the bucketing phase
/// writes through.
pub struct BucketOutput<B: LockFreeBucket> {
    buckets: Arc<MultiThreadBuckets<B>>,
    index: u16,
}

impl<B: LockFreeBucket> BucketOutput<B> {
    pub fn new(buckets: Arc<MultiThreadBuckets<B>>, index: u16) -> Self {
        Self { buckets, index }
    }
}

impl<B: LockFreeBucket + Sync + Send> DedupOutput for BucketOutput<B> {
    #[inline]
    fn write_records(&self, data: &[u8]) -> ChunkingStatus {
        self.buckets.add_data(self.index, data)
    }
}

/// The bucket set bypassed records go to, built on first use.
///
/// [`MultiThreadBuckets::new`] creates every bucket's file up front, and a full
/// set of writer states is a sizeable share of this phase's resident memory, so
/// a run whose deduplicators all keep earning their keep should not pay for one
/// it never writes to.
pub struct LazyBuckets<B: LockFreeBucket> {
    /// `None` until some bucket stands down.
    ///
    /// Read on every bypassed write -- every bucket, every thread -- so it must
    /// not be a lock. It was one, briefly, and a single mutex shared by a
    /// thousand buckets cost more than the deduplication it was avoiding.
    buckets: ArcSwapOption<MultiThreadBuckets<B>>,
    /// Held only while building, so two threads racing to be first cannot
    /// create two sets.
    build_lock: Mutex<()>,
    build: Box<dyn Fn() -> Arc<MultiThreadBuckets<B>> + Sync + Send>,
}

impl<B: LockFreeBucket> LazyBuckets<B> {
    pub fn new(build: impl Fn() -> Arc<MultiThreadBuckets<B>> + Sync + Send + 'static) -> Self {
        Self {
            buckets: ArcSwapOption::empty(),
            build_lock: Mutex::new(()),
            build: Box::new(build),
        }
    }

    /// Builds the set if this is the first bypass anywhere. Off the hot path:
    /// callers try [`Self::peek`] first.
    #[cold]
    fn get_or_build(&self) -> Arc<MultiThreadBuckets<B>> {
        let _guard = self.build_lock.lock();
        // Re-check: another thread may have built it while we waited.
        if let Some(buckets) = self.buckets.load_full() {
            return buckets;
        }
        let built = (self.build)();
        self.buckets.store(Some(built.clone()));
        built
    }

    /// Whether anything ever bypassed.
    pub fn is_created(&self) -> bool {
        self.buckets.load().is_some()
    }

    /// The set if it exists, without building one. For a caller that only wants
    /// to compact what is already there.
    pub fn peek(&self) -> Option<Arc<MultiThreadBuckets<B>>> {
        self.buckets.load_full()
    }

    /// Hands the set over for finalization, leaving nothing behind. The caller
    /// must hold the only other references, which
    /// [`MultiThreadBuckets::finalize`] checks for.
    pub fn take(&self) -> Option<Arc<MultiThreadBuckets<B>>> {
        self.buckets.swap(None)
    }
}

/// One bucket of a [`LazyBuckets`].
pub struct LazyBucketOutput<B: LockFreeBucket> {
    buckets: Arc<LazyBuckets<B>>,
    index: u16,
}

impl<B: LockFreeBucket> LazyBucketOutput<B> {
    pub fn new(buckets: Arc<LazyBuckets<B>>, index: u16) -> Self {
        Self { buckets, index }
    }
}

impl<B: LockFreeBucket + Sync + Send> DedupOutput for LazyBucketOutput<B> {
    #[inline]
    fn write_records(&self, data: &[u8]) -> ChunkingStatus {
        // An atomic load, not a lock: every bypassed buffer from every thread
        // passes through here.
        if let Some(buckets) = self.buckets.buckets.load().as_ref() {
            return buckets.add_data(self.index, data);
        }
        self.buckets.get_or_build().add_data(self.index, data)
    }
}

/// A writer of its own, for tests and for any caller that owns one bucket.
impl<B: LockFreeBucket + Sync + Send> DedupOutput for Arc<B> {
    #[inline]
    fn write_records(&self, data: &[u8]) -> ChunkingStatus {
        self.write_data(data);
        ChunkingStatus::SameChunk
    }
}

/// One half of the double buffer: M/2 bytes, allocated once and never resized.
///
/// `Box<[UnsafeCell<u8>]>` rather than `UnsafeCell<Box<[u8]>>`, because the
/// latter would force a transient `&mut Box<[u8]>` on every append and so
/// invalidate the pointers the other appenders are writing through.
struct Slot {
    /// Reservation cursor, always within `0..=capacity`.
    len: AtomicUsize,
    /// Read held while an append copies in, write held while the buffer is
    /// walked.
    gate: RwLock<()>,
    data: Box<[UnsafeCell<u8>]>,
}

impl Slot {
    fn new(capacity: usize) -> Self {
        Self {
            len: AtomicUsize::new(0),
            gate: RwLock::new(()),
            data: (0..capacity).map(|_| UnsafeCell::new(0)).collect(),
        }
    }

    /// Reserves room for `slice` and copies it in, or reports that the buffer is
    /// full. The gate guard is taken and dropped inside, so no caller can hold
    /// it across a seal — which is what would invert the lock order and
    /// deadlock.
    fn try_append(&self, epoch: usize, active: &AtomicUsize, slice: &[u8]) -> bool {
        let _guard = self.gate.read();
        // Re-check under the gate: this slot may have been sealed between the
        // caller's load and the acquisition. Nothing has been written yet, so
        // backing out is free.
        if active.load(Ordering::Acquire) != epoch {
            return false;
        }
        let capacity = self.data.len();
        let reserved = self
            .len
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |len| {
                (len + slice.len() <= capacity).then_some(len + slice.len())
            });
        match reserved {
            Ok(offset) => {
                // SAFETY: the range was carved out of `len` by a single
                // read-modify-write, so it overlaps no other appender's range,
                // and the read guard keeps the processor out until the copy has
                // finished.
                unsafe {
                    std::ptr::copy_nonoverlapping(
                        slice.as_ptr(),
                        self.data.as_ptr().cast::<u8>().cast_mut().add(offset),
                        slice.len(),
                    );
                }
                true
            }
            Err(_) => false,
        }
    }
}

// SAFETY: every write into `data` goes into a range reserved by one atomic
// read-modify-write, so appenders never overlap, and it happens under the gate's
// read guard. Every read of the whole buffer happens under the gate's write
// guard, which excludes all appenders.
unsafe impl Sync for Slot {}
unsafe impl Send for Slot {}

/// One entry of the open-addressing table. `tag == 0` marks an empty slot, so a
/// computed tag of zero is mapped to one.
///
/// Eight bytes rather than sixteen with a full hash: the M/4 allowance then buys
/// twice the entries, which is what keeps the M-byte bases storage from sitting
/// half empty at every drain. A tag collision costs one extra full comparison,
/// which a hash collision would have cost anyway.
#[derive(Clone, Copy, PartialEq, Eq)]
struct Entry {
    tag: u32,
    offset: u32,
}

const EMPTY: Entry = Entry {
    tag: 0,
    offset: u32::MAX,
};

#[derive(Default, Clone, Copy, Debug, PartialEq, Eq)]
pub struct DedupStats {
    /// Records handed to `add`.
    pub records_in: u64,
    /// Records written to the output bucket.
    pub records_out: u64,
    pub bytes_out: u64,
    /// Drains, and which bound triggered each.
    pub drains: u64,
    pub drains_storage: u64,
    pub drains_arena: u64,
    pub drains_table: u64,
    /// Buffers sealed, and slices that bypassed the buffers.
    pub seals: u64,
    pub inline_slices: u64,
    /// Records too large to ever be stored, written straight out.
    pub oversized_records: u64,
    /// Bytes forwarded untouched while standing down, and how many windows
    /// decided to stand down. Both zero on input the deduplicator is earning
    /// its keep on, which is what makes a silent regression visible.
    pub bypassed_bytes: u64,
    pub bypass_windows: u64,
    /// Peak live bytes of each bounded structure, for the budget assertions.
    pub peak_storage: usize,
    pub peak_arena: usize,
    pub table_bytes: usize,
}

/// Everything a walk touches. Held behind one mutex, so processing is
/// single-threaded however many threads are appending.
struct DedupState<MultipleData: DedupExtraData, FlagsCount: typenum::Unsigned> {
    /// Deduplicated records, each prefixed with its sub-bucket byte.
    storage: Vec<u8>,
    storage_cap: usize,
    table: Box<[Entry]>,
    occupied: usize,
    /// Colours of the stored records.
    out_arena: TempBuffer<MultipleData>,
    /// Colours of the record being decoded. Separate from `out_arena` because
    /// `copy_extra_from` and `combine_entries` both move runs from one arena
    /// into another.
    in_arena: TempBuffer<MultipleData>,
    arena_cap: usize,
    in_arena_cap: usize,
    /// Reset before every record; see [`InputCodec`].
    in_codec: InputCodec<MultipleData, FlagsCount>,
    /// Whatever the incoming extra data decodes through. Empty for both
    /// concrete single-entry types, which hold their colour inline.
    in_single_buffer: TempBuffer<SingleOf<MultipleData>>,
    /// One codec for the whole lifetime, never reset: the delta state a reset
    /// would clear is zero-sized for both concrete extra data types (asserted in
    /// `new`), and a mid-stream reset without a matching reader restart is what
    /// desynchronises a stateful one.
    out_codec: OutputCodec<MultipleData, FlagsCount>,
    read_buf: Vec<u8>,
    /// Target size of the thread-local drain buffer; see [`STAGING`].
    staging_capacity: usize,
    stats: DedupStats,
    /// Set while a drain is in flight, so a panic leaves a flag the next
    /// acquirer trips on rather than a half-drained storage. `parking_lot` does
    /// not poison.
    torn: bool,
    /// Raised when a write started a new chunk, so `add` can hand that back to
    /// the caller, which is what makes the compactor run.
    new_chunk: bool,
    /// Counters as of the last bypass decision, so a window can be measured
    /// against the cumulative ones without a second set of increments.
    window_records_in: u64,
    window_records_out: u64,
    policy: BypassPolicy,
}

/// When a bucket should stop deduplicating.
///
/// A deduplicator only pays for itself if it actually folds records together.
/// On input with no repeated super-kmers it collapses nothing while still
/// decoding, widening, hashing, storing and re-encoding every record, so it is
/// measured against its own output and stood down when it is not earning its
/// keep.
#[derive(Copy, Clone, Debug)]
pub struct BypassPolicy {
    /// Percent of records a window must remove to be worth continuing.
    pub min_collapse_percent: u64,
    /// Windows smaller than this decide nothing.
    pub min_records: u64,
    /// Storage-fulls one bypass covers before the bucket is measured again.
    pub skip_factor: u32,
    /// Whether to stand down before seeing any input at all, rather than after
    /// the first window has been judged.
    pub start_bypassing: bool,
}

impl BypassPolicy {
    pub fn from_config() -> Self {
        Self {
            min_collapse_percent: config::MINIMIZER_DEDUP_BYPASS_COLLAPSE_PERCENT,
            min_records: config::MINIMIZER_DEDUP_BYPASS_MIN_RECORDS,
            skip_factor: config::MINIMIZER_DEDUP_BYPASS_SKIP_FACTOR,
            start_bypassing: false,
        }
    }

    /// Bypasses at the first opportunity, for tests.
    pub fn always() -> Self {
        Self {
            min_collapse_percent: 101,
            min_records: 0,
            skip_factor: 1 << 20,
            start_bypassing: true,
        }
    }

    /// Never bypasses, for tests.
    pub fn never() -> Self {
        Self {
            min_collapse_percent: 0,
            min_records: u64::MAX,
            skip_factor: 1,
            start_bypassing: false,
        }
    }
}

pub struct BoundedDeduplicator<
    MultipleData: DedupExtraData,
    FlagsCount: typenum::Unsigned,
    O: DedupOutput,
    /// Where bypassed records go. It differs from `O` in the pipeline, where
    /// the narrow buckets are built only once some bucket actually stands down.
    Bypass: DedupOutput = O,
> {
    slots: [Slot; 2],
    /// Monotone; the active slot is `active_epoch & 1`.
    active_epoch: AtomicUsize,
    /// The swap lock and the dedup state are deliberately the same lock: the
    /// epoch is then bumped and the buffer walked inside one critical section,
    /// so the epoch never runs more than one ahead of processing and exactly two
    /// buffers are live at any instant.
    state: Mutex<DedupState<MultipleData, FlagsCount>>,
    output: O,
    /// Where records go while this bucket is bypassing. They are forwarded in
    /// the format they arrived in, so nothing is decoded on that path.
    bypass_output: Option<Bypass>,
    /// Set for an extra data type that cannot be combined at all, in which case
    /// deduplicating is not merely unprofitable but meaningless.
    force_bypass: bool,
    bypass: AtomicBool,
    /// Bytes this bypass still covers before the bucket is measured again.
    bypass_bytes_left: AtomicI64,
    /// Counted outside the state lock, because the bypass path deliberately
    /// never takes it. Folded into the stats at `finish`.
    bypassed_bytes: AtomicU64,
    slot_capacity: usize,
}

impl<
    MultipleData: DedupExtraData,
    FlagsCount: typenum::Unsigned,
    O: DedupOutput,
    Bypass: DedupOutput,
> BoundedDeduplicator<MultipleData, FlagsCount, O, Bypass>
{
    /// `memory` is M and must be a power of two of at least 64 KiB. `k` is the
    /// codec's minimum sequence length.
    pub fn new(memory: usize, k: usize, output: O) -> Self {
        Self::new_with_bypass(memory, k, output, None, BypassPolicy::never())
    }

    /// As [`Self::new`], but able to stand down: records are then forwarded to
    /// `bypass_output` in the narrow form they arrived in.
    pub fn new_with_bypass(
        memory: usize,
        k: usize,
        output: O,
        bypass_output: Option<Bypass>,
        policy: BypassPolicy,
    ) -> Self {
        assert!(
            memory.is_power_of_two(),
            "the memory budget must be a power of two"
        );
        assert!(
            memory >= 64 * 1024,
            "a budget below 64 KiB leaves no room for a table"
        );
        // A permanently bypassing deduplicator never writes through `out_codec`,
        // so its delta state is never used and need not be stateless.
        let force_bypass = !MultipleData::ALLOW_COMBINE;
        assert!(
            !force_bypass || bypass_output.is_some(),
            "an extra data type that cannot be combined needs somewhere to bypass to"
        );
        // Otherwise a stateful `LastData` would need every codec reset paired
        // with a checkpoint the reader restarts at. Both combinable extra data
        // types have a zero-sized one, and this keeps it that way.
        assert!(
            force_bypass
                || size_of::<<MultipleData as SequenceExtraDataConsecutiveCompression>::LastData>()
                    == 0,
            "a stateful LastData needs checkpoint-paired codec resets"
        );

        let start_bypassing = policy.start_bypassing;
        let slot_capacity = memory / 2;
        let table_len = (memory / 4 / size_of::<Entry>()).next_power_of_two();
        let arena_cap = memory;
        let in_arena_cap = memory / 16;

        Self {
            slots: [Slot::new(slot_capacity), Slot::new(slot_capacity)],
            active_epoch: AtomicUsize::new(0),
            state: Mutex::new(DedupState {
                storage: Vec::with_capacity(memory + STORAGE_SLACK),
                storage_cap: memory,
                table: vec![EMPTY; table_len].into_boxed_slice(),
                occupied: 0,
                out_arena: TempBuffer::<MultipleData>::with_budget(arena_cap),
                in_arena: TempBuffer::<MultipleData>::with_budget(in_arena_cap),
                arena_cap,
                in_arena_cap,
                in_codec: InputCodec::<MultipleData, FlagsCount>::new(k),
                in_single_buffer: SingleOf::<MultipleData>::new_temp_buffer(),
                out_codec: OutputCodec::<MultipleData, FlagsCount>::new(k),
                read_buf: Vec::new(),
                // Small and fixed: ggcat's DEFAULT_OUTPUT_BUFFER_SIZE is 4 MiB,
                // larger than M itself for a 512 KiB budget.
                staging_capacity: memory / 8,
                stats: DedupStats {
                    table_bytes: table_len * size_of::<Entry>(),
                    ..DedupStats::default()
                },
                torn: false,
                new_chunk: false,
                window_records_in: 0,
                window_records_out: 0,
                policy,
            }),
            output,
            bypass_output,
            force_bypass,
            bypass: AtomicBool::new(force_bypass || start_bypassing),
            // Starting stood down still needs a budget, or the first slice
            // would spend it and fold everything until the next drain judged
            // the bucket.
            bypass_bytes_left: AtomicI64::new(if start_bypassing {
                (memory as i64).saturating_mul(policy.skip_factor as i64)
            } else {
                0
            }),
            bypassed_bytes: AtomicU64::new(0),
            slot_capacity,
        }
    }

    /// Whether this bucket is currently standing its deduplicator down.
    pub fn is_bypassing(&self) -> bool {
        self.force_bypass || self.bypass.load(Ordering::Relaxed)
    }

    /// Appends a slice holding a whole number of encoded records. Callable from
    /// any thread; never blocks on another appender, only behind a buffer that
    /// is being walked.
    pub fn add(&self, slice: &[u8]) -> ChunkingStatus {
        if slice.is_empty() {
            return ChunkingStatus::SameChunk;
        }

        // Standing down: the slice already holds exactly the records the
        // compactor's narrow reader expects, so it goes out untouched. Not
        // decoding it is the whole point -- a decode and re-encode here would
        // give back most of what bypassing saves.
        if let Some(bypass) = &self.bypass_output {
            if self.force_bypass || self.bypass.load(Ordering::Relaxed) {
                let status = bypass.write_records(slice);
                self.bypassed_bytes
                    .fetch_add(slice.len() as u64, Ordering::Relaxed);
                if !self.force_bypass
                    && self
                        .bypass_bytes_left
                        .fetch_sub(slice.len() as i64, Ordering::Relaxed)
                        <= slice.len() as i64
                {
                    // Budget spent: the next window is measured again, so a
                    // bucket that becomes redundant later is not written off.
                    self.bypass.store(false, Ordering::Relaxed);
                }
                return status;
            }
        }

        if slice.len() > self.slot_capacity {
            // Walked straight out of the caller's memory: the decoder never
            // reads past what it consumes, so no copy and no resize.
            return self.process_inline(slice);
        }

        let mut chunked = false;
        for round in 0.. {
            let epoch = self.active_epoch.load(Ordering::Acquire);
            if self.slots[epoch & 1].try_append(epoch, &self.active_epoch, slice) {
                break;
            }
            if round >= MAX_APPEND_ROUNDS {
                // Stop competing and make guaranteed progress.
                chunked |= matches!(self.process_inline(slice), ChunkingStatus::NewChunk);
                break;
            }
            chunked |= matches!(self.seal(epoch), ChunkingStatus::NewChunk);
        }
        if chunked {
            ChunkingStatus::NewChunk
        } else {
            ChunkingStatus::SameChunk
        }
    }

    #[cold]
    fn process_inline(&self, slice: &[u8]) -> ChunkingStatus {
        let mut state = self.state.lock();
        state.stats.inline_slices += 1;
        self.walk(&mut state, slice);
        Self::take_chunking(&mut state)
    }

    #[inline]
    fn take_chunking(state: &mut DedupState<MultipleData, FlagsCount>) -> ChunkingStatus {
        if std::mem::take(&mut state.new_chunk) {
            ChunkingStatus::NewChunk
        } else {
            ChunkingStatus::SameChunk
        }
    }

    /// Retires the buffer that was active at `observed_epoch`, if nobody else
    /// already has.
    #[cold]
    fn seal(&self, observed_epoch: usize) -> ChunkingStatus {
        let mut state = self.state.lock();
        if self.active_epoch.load(Ordering::Relaxed) != observed_epoch {
            // Somebody sealed it already; the caller retries against whatever is
            // active now.
            return Self::take_chunking(&mut state);
        }
        // Past this point no new appender enters the old slot.
        self.active_epoch
            .store(observed_epoch + 1, Ordering::Release);
        self.seal_locked(&mut state, observed_epoch);
        Self::take_chunking(&mut state)
    }

    /// Walks and empties one slot. The caller must hold `state` and must already
    /// have moved the epoch past `epoch`.
    fn seal_locked(&self, state: &mut DedupState<MultipleData, FlagsCount>, epoch: usize) {
        let slot = &self.slots[epoch & 1];
        // Waits out every appender that entered before the bump, which is what
        // makes the `len` read below final and the bytes stable.
        let gate = slot.gate.write();
        let len = slot.len.load(Ordering::Relaxed);
        if len > 0 {
            state.stats.seals += 1;
            // SAFETY: the write guard excludes every appender, and the `len`
            // bytes were written by appends that have all completed.
            let data = unsafe { std::slice::from_raw_parts(slot.data.as_ptr().cast::<u8>(), len) };
            self.walk(state, data);
        }
        slot.len.store(0, Ordering::Relaxed);
        drop(gate);
    }

    /// Processes both buffers, drains what is left and flushes the output.
    ///
    /// Takes `&self` so a deduplicator that lives in a shared array can be
    /// finished in place. Every appender must have been joined first: this makes
    /// no attempt to exclude one.
    pub fn finish(&self) -> DedupStats {
        let mut state = self.state.lock();
        let epoch = self.active_epoch.load(Ordering::Relaxed);
        // The inactive buffer first, so records keep their relative order.
        self.seal_locked(&mut state, epoch + 1);
        self.seal_locked(&mut state, epoch);
        self.drain(&mut state);
        state.stats.bypassed_bytes = self.bypassed_bytes.load(Ordering::Relaxed);
        state.stats
    }

    /// Consuming counterpart of [`Self::finish`], which also hands the output
    /// back.
    pub fn finalize(self) -> (O, DedupStats) {
        let stats = self.finish();
        (self.output, stats)
    }

    /// As [`Self::finalize`], for a deduplicator that was given somewhere to
    /// bypass to.
    pub fn finalize_with_bypass(self) -> (O, Option<Bypass>, DedupStats) {
        let stats = self.finish();
        (self.output, self.bypass_output, stats)
    }

    /// Decodes every record of `data` into the dedup state, draining whenever a
    /// bound would be passed.
    fn walk(&self, state: &mut DedupState<MultipleData, FlagsCount>, data: &[u8]) {
        assert!(!state.torn, "a previous drain panicked partway through");
        // Taken out so that a decoded record, which borrows `read_buf`, does not
        // keep `state` borrowed while it is being inserted.
        let mut read_buf = std::mem::take(&mut state.read_buf);
        let mut in_arena = std::mem::take(&mut state.in_arena);
        let mut single_buffer = std::mem::take(&mut state.in_single_buffer);

        let mut cursor = data;
        loop {
            // `read_from` appends without ever truncating, so the buffer has to
            // be cleared per record or it grows by one record each time and a
            // reallocation dangles the read that was just decoded.
            read_buf.clear();
            if in_arena.live_bytes() > state.in_arena_cap {
                MultipleData::clear_temp_buffer(&mut in_arena);
            }
            SingleOf::<MultipleData>::clear_temp_buffer(&mut single_buffer);
            state.in_codec.reset();
            let Some(narrow) =
                state
                    .in_codec
                    .read_from(&mut cursor, &mut read_buf, &mut single_buffer)
            else {
                break;
            };
            // The one place a record is widened, and only ever once.
            let extra =
                MultipleData::from_single_entry(&mut in_arena, narrow.extra, &single_buffer).0;
            let record = DeserializedRead {
                read: narrow.read,
                extra,
                multiplicity: narrow.multiplicity,
                minimizer_pos: narrow.minimizer_pos,
                flags: narrow.flags,
                second_bucket: narrow.second_bucket,
            };
            state.stats.records_in += 1;
            self.insert(state, &mut in_arena, record);
        }
        debug_assert!(
            cursor.is_empty(),
            "a slice handed to add() must hold a whole number of records"
        );

        state.read_buf = read_buf;
        state.in_arena = in_arena;
        state.in_single_buffer = single_buffer;
    }

    /// Bytes one record occupies in the bases storage: our sub-bucket prefix
    /// plus exactly what `memstorage_encode_read` writes for
    /// `WithFixedMultiplicity`, `AssemblerMinimizerPosition` and `ALIGNED`.
    #[inline]
    fn stored_bytes(bases_count: usize) -> usize {
        1 + size_of::<MultipleData>()
            + 2
            + size_of::<MultiplicityCounterType>()
            + (compute_memvarint_bytes_count(bases_count as u64) + 1)
            + bases_count.div_ceil(4)
    }

    fn insert(
        &self,
        state: &mut DedupState<MultipleData, FlagsCount>,
        in_arena: &mut TempBuffer<MultipleData>,
        record: DeserializedRead<MultipleData>,
    ) {
        let needed = Self::stored_bytes(record.read.bases_count());

        // A record too large for an empty storage can never be held, so it goes
        // straight out. At that size it is unique by construction.
        if needed + STORAGE_SLACK > state.storage_cap {
            state.stats.oversized_records += 1;
            self.emit_alone(state, in_arena, &record);
            return;
        }

        // SAFETY: decoded with NoAlignmentWithOverflow, so sixteen readable
        // bytes follow the packed slice.
        let hash = unsafe { record.read.compute_hash_aligned_overflow16() };
        let tag = ((hash >> 32) as u32).max(1);
        let incoming_cost = arena_cost(&record.extra);

        let mut drained = false;
        loop {
            let mask = state.table.len() - 1;
            let mut index = (hash as usize) & mask;
            let found = loop {
                let entry = state.table[index];
                if entry == EMPTY {
                    break None;
                }
                if entry.tag == tag && Self::matches(state, entry.offset as usize, &record) {
                    break Some(entry.offset as usize);
                }
                index = (index + 1) & mask;
            };

            if let Some(offset) = found {
                // A combine can grow the arena without adding a byte of storage
                // or a table slot, so the arena bound is checked here too. A
                // grow reallocates to the next power of two and copies, so twice
                // the resulting run count is the conservative estimate.
                let cost = incoming_cost + Self::stored_cost(state, offset);
                if !drained && state.out_arena.live_bytes() + 2 * cost > state.arena_cap {
                    state.stats.drains_arena += 1;
                    self.drain(state);
                    drained = true;
                    continue;
                }
                Self::combine(state, in_arena, offset, &record);
                return;
            }

            // Miss. Every bound is checked before committing, because a drain
            // invalidates every offset in the table and every handle in the
            // arena.
            let over_storage = state.storage.len() + needed + STORAGE_SLACK > state.storage_cap;
            let over_arena = state.out_arena.live_bytes() + incoming_cost > state.arena_cap;
            let over_table = state.occupied + 1 > state.table.len() * 3 / 4;
            if over_storage || over_arena || over_table {
                if drained {
                    // Already drained once and it still does not fit: the record
                    // alone exceeds a bound, so write it straight out.
                    state.stats.oversized_records += 1;
                    self.emit_alone(state, in_arena, &record);
                    return;
                }
                state.stats.drains_storage += over_storage as u64;
                state.stats.drains_arena += (!over_storage && over_arena) as u64;
                state.stats.drains_table += (!over_storage && !over_arena && over_table) as u64;
                self.drain(state);
                drained = true;
                continue;
            }

            let offset = state.storage.len();
            state.storage.push(record.second_bucket);
            let extra = MultipleData::copy_extra_from(record.extra, in_arena, &mut state.out_arena);
            let stored = DeserializedRead {
                read: record.read,
                extra,
                multiplicity: record.multiplicity,
                minimizer_pos: record.minimizer_pos,
                flags: record.flags,
                second_bucket: record.second_bucket,
            };
            let storage = &mut state.storage;
            memstorage_encode_read::<
                MultipleData,
                WithFixedMultiplicity,
                AssemblerMinimizerPosition,
                true,
            >(&stored, |needed, reserved| {
                let start = storage.len();
                storage.reserve(reserved);
                // SAFETY: the capacity was just reserved; the surplus past
                // `needed` stays uninitialised, which is what the decoder's
                // eight byte word read needs.
                unsafe {
                    storage.set_len(start + needed);
                    storage.as_mut_ptr().add(start)
                }
            });
            debug_assert_eq!(state.storage.len() - offset, needed);

            state.table[index] = Entry {
                tag,
                offset: offset as u32,
            };
            state.occupied += 1;
            state.stats.peak_storage = state.stats.peak_storage.max(state.storage.len());
            state.stats.peak_arena = state.stats.peak_arena.max(state.out_arena.live_bytes());
            return;
        }
    }

    /// Whether the record stored at `offset` is the same super-kmer, colours
    /// aside. `second_bucket` is part of the key: it is the sub-bucket routing
    /// field, and records bound for different sub-buckets must not be merged.
    fn matches(
        state: &DedupState<MultipleData, FlagsCount>,
        offset: usize,
        record: &DeserializedRead<MultipleData>,
    ) -> bool {
        if state.storage[offset] != record.second_bucket {
            return false;
        }
        let stored = Self::stored_at(state, offset);
        stored.flags == record.flags
            && stored.read.bases_count() == record.read.bases_count()
            && stored.read.get_packed_slice() == record.read.get_packed_slice()
    }

    /// Decodes the record stored at `offset`. The returned read points into
    /// `state.storage`, which must not be grown or cleared while it is alive.
    fn stored_at(
        state: &DedupState<MultipleData, FlagsCount>,
        offset: usize,
    ) -> DeserializedRead<'static, MultipleData> {
        // SAFETY: `offset` was produced by an encode into this storage, which
        // has not been cleared since; the sub-bucket prefix is one byte.
        unsafe {
            memstorage_decode_read::<
                MultipleData,
                WithFixedMultiplicity,
                AssemblerMinimizerPosition,
                true,
            >(state.storage.as_ptr().add(offset + 1))
            .0
        }
    }

    fn stored_cost(state: &DedupState<MultipleData, FlagsCount>, offset: usize) -> usize {
        arena_cost(&Self::stored_at(state, offset).extra)
    }

    /// Folds a repeat into the record stored at `offset`: multiplicity summed,
    /// colours merged, both patched in place. The two mutable fields sit at
    /// known offsets from the start of the memstorage record, which begins after
    /// our sub-bucket byte.
    fn combine(
        state: &mut DedupState<MultipleData, FlagsCount>,
        in_arena: &TempBuffer<MultipleData>,
        offset: usize,
        record: &DeserializedRead<MultipleData>,
    ) {
        // SAFETY: the layout is the one `memstorage_encode_read` wrote — extra,
        // then the minimizer position, then the fixed width multiplicity.
        unsafe {
            let base = state.storage.as_mut_ptr().add(offset + 1);
            let extra_ptr = base as *mut MultipleData;
            let multiplicity_ptr =
                base.add(size_of::<MultipleData>() + 2) as *mut MultiplicityCounterType;
            std::ptr::write_unaligned(
                multiplicity_ptr,
                std::ptr::read_unaligned(multiplicity_ptr).saturating_add(record.multiplicity),
            );
            let mut extra = std::ptr::read_unaligned(extra_ptr);
            extra.combine_entries(&mut state.out_arena, record.extra, in_arena);
            std::ptr::write_unaligned(extra_ptr, extra);
        }
        state.stats.peak_arena = state.stats.peak_arena.max(state.out_arena.live_bytes());
    }

    /// Writes every stored record to the output bucket and empties the state.
    ///
    /// The bases storage is walked linearly rather than the table: each stored
    /// record has exactly one entry, so this emits each exactly once, reads
    /// sequentially, and is deterministic given the input order, which probe
    /// order is not.
    fn drain(&self, state: &mut DedupState<MultipleData, FlagsCount>) {
        state.torn = true;
        state.stats.drains += 1;
        // Taken out so `emit` can hold `&mut state` and the arena at once.
        let mut arena = std::mem::take(&mut state.out_arena);

        STAGING.with_borrow_mut(|staging| {
            Self::open_staging(staging, state.staging_capacity);
            let mut offset = 0;
            while offset < state.storage.len() {
                let second_bucket = state.storage[offset];
                let record = Self::stored_at(state, offset);
                let multiplicity = record.multiplicity;
                offset += Self::stored_bytes(record.read.bases_count());
                self.emit(
                    state,
                    &mut arena,
                    staging,
                    &record,
                    second_bucket,
                    multiplicity,
                );
            }
            self.close_staging(state, staging);
        });

        state.storage.clear();
        state.table.fill(EMPTY);
        state.occupied = 0;
        MultipleData::clear_temp_buffer(&mut arena);
        state.out_arena = arena;
        state.torn = false;

        self.decide_bypass(state);
    }

    /// Judges the window that just drained and stands the deduplicator down if
    /// it removed too little to be worth its cost.
    ///
    /// Deciding here rather than in `add` means it runs under the lock that
    /// already exists, from counters that are already maintained, and that a
    /// flip cannot split a slice: `walk` never re-reads the flag, so the records
    /// still in flight finish through the folding path and the ones already
    /// stored have just been emitted by this very drain.
    fn decide_bypass(&self, state: &mut DedupState<MultipleData, FlagsCount>) {
        if self.bypass_output.is_none() || self.force_bypass {
            return;
        }

        let window_in = state.stats.records_in - state.window_records_in;
        let window_out = state.stats.records_out - state.window_records_out;
        state.window_records_in = state.stats.records_in;
        state.window_records_out = state.stats.records_out;

        if window_in < state.policy.min_records {
            // Too short a window to conclude anything from.
            return;
        }

        // Collapsed below the threshold, stated without dividing: kept records
        // times 100 against the fraction of the input allowed to survive.
        let too_little = window_out * 100 > window_in * (100 - state.policy.min_collapse_percent);
        if too_little {
            self.bypass_bytes_left.store(
                (state.storage_cap as i64).saturating_mul(state.policy.skip_factor as i64),
                Ordering::Relaxed,
            );
            self.bypass.store(true, Ordering::Relaxed);
            state.stats.bypass_windows += 1;
        }
    }

    /// Readies this thread's staging buffer for a run of records.
    fn open_staging(staging: &mut Vec<u8>, capacity: usize) {
        staging.clear();
        staging.reserve(capacity);
    }

    /// Flushes whatever the run left behind, so the buffer never carries bytes
    /// out of the call that wrote them, and gives back any capacity a single
    /// oversized record forced it to grow by rather than letting a thread hold
    /// that for the rest of its life.
    fn close_staging(
        &self,
        state: &mut DedupState<MultipleData, FlagsCount>,
        staging: &mut Vec<u8>,
    ) {
        if !staging.is_empty() {
            if matches!(self.output.write_records(staging), ChunkingStatus::NewChunk) {
                state.new_chunk = true;
            }
            state.stats.bytes_out += staging.len() as u64;
            staging.clear();
        }
        if staging.capacity() > 2 * state.staging_capacity {
            *staging = Vec::with_capacity(state.staging_capacity);
        }
    }

    /// Writes one record on its own, for a record no drain could make room for.
    fn emit_alone(
        &self,
        state: &mut DedupState<MultipleData, FlagsCount>,
        arena: &mut TempBuffer<MultipleData>,
        record: &DeserializedRead<MultipleData>,
    ) {
        STAGING.with_borrow_mut(|staging| {
            Self::open_staging(staging, state.staging_capacity);
            self.emit(
                state,
                arena,
                staging,
                record,
                record.second_bucket,
                record.multiplicity,
            );
            self.close_staging(state, staging);
        });
    }

    /// Serializes one deduplicated record into the staging buffer, flushing it
    /// first if the record would not fit.
    fn emit(
        &self,
        state: &mut DedupState<MultipleData, FlagsCount>,
        arena: &mut TempBuffer<MultipleData>,
        staging: &mut Vec<u8>,
        record: &DeserializedRead<MultipleData>,
        second_bucket: u8,
        multiplicity: MultiplicityCounterType,
    ) {
        let mut extra = record.extra;
        // Multiplicity is raised only by `combine_entries`, so a multiplicity of
        // one means the set is still the canonical one it was copied from and
        // `prepare` would be a no-op. Same reasoning the compactor uses.
        if multiplicity > 1 {
            extra.prepare_for_serialization(arena);
        }

        let element = CompressedReadsBucketData::new_packed_with_multiplicity(
            record.read,
            record.flags,
            second_bucket,
            multiplicity,
            record.minimizer_pos,
        );
        let needed = state.out_codec.get_size(&element, &extra);
        if !staging.is_empty() && staging.len() + needed > staging.capacity() {
            if matches!(self.output.write_records(staging), ChunkingStatus::NewChunk) {
                state.new_chunk = true;
            }
            state.stats.bytes_out += staging.len() as u64;
            staging.clear();
        }
        state.out_codec.write_to(&element, staging, &extra, arena);
        state.stats.records_out += 1;
    }
}

impl DedupStats {
    /// Folds another bucket's figures in, for a summary over the whole array.
    pub fn accumulate(&mut self, other: &Self) {
        self.records_in += other.records_in;
        self.records_out += other.records_out;
        self.bytes_out += other.bytes_out;
        self.drains += other.drains;
        self.drains_storage += other.drains_storage;
        self.drains_arena += other.drains_arena;
        self.drains_table += other.drains_table;
        self.seals += other.seals;
        self.inline_slices += other.inline_slices;
        self.oversized_records += other.oversized_records;
        self.bypassed_bytes += other.bypassed_bytes;
        self.bypass_windows += other.bypass_windows;
        self.peak_storage = self.peak_storage.max(other.peak_storage);
        self.peak_arena = self.peak_arena.max(other.peak_arena);
        self.table_bytes = self.table_bytes.max(other.table_bytes);
    }

    /// One line per figure, for a test or a benchmark to print.
    pub fn report(&self) -> String {
        format!(
            "records {} -> {} ({:.1}% collapsed), {} bytes out, {} drains \
             (storage {}, arena {}, table {}), {} seals, {} inline slices, \
             {} oversized, {} bytes bypassed over {} windows, \
             peak storage {}, arena {}, table {}",
            self.records_in,
            self.records_out,
            if self.records_in == 0 {
                0.0
            } else {
                100.0 * self.records_in.saturating_sub(self.records_out) as f64
                    / self.records_in as f64
            },
            self.bytes_out,
            self.drains,
            self.drains_storage,
            self.drains_arena,
            self.drains_table,
            self.seals,
            self.inline_slices,
            self.oversized_records,
            self.bypassed_bytes,
            self.bypass_windows,
            self.peak_storage,
            self.peak_arena,
            self.table_bytes,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::MinimizerBucketMode;
    use colors::bucket_colors::expand_runs;
    use colors::colors_manager::{
        MinimizerBucketingSeqColorData, MinimizerBucketingSeqColorDataIterable,
    };
    use colors::non_colored::NonColoredManager;
    use colors::parsers::separate::{MinBkMultipleColors, MinBkSingleColor};
    use colors::parsers::{SequenceIdent, SingleSequenceInfo};
    use config::ColorIndexType;
    use io::concurrent::temp_reads::creads_utils::helpers::helper_read_bucket;
    use io::concurrent::temp_reads::creads_utils::{NoAlignment, NoMultiplicity};
    use parallel_processor::buckets::readers::binary_reader::ChunkedBinaryReaderIndex;
    use parallel_processor::buckets::writers::lock_free_binary_writer::LockFreeBinaryWriter;
    use parallel_processor::memory_fs::file::internal::MemoryFileMode;
    use parallel_processor::memory_fs::{MemoryFs, RemoveFileMode};
    use std::collections::BTreeMap;
    use std::path::{Path, PathBuf};

    const K: usize = 31;
    const M: usize = 64 * 1024;
    type Flags = typenum::U2;

    /// `MemoryFs` is process global and `flush_to_disk` flushes every file any
    /// test created, so two tests that both write buckets would read each
    /// other's half-flushed state. Every test here holds this for its body.
    fn memory_fs() -> parking_lot::MutexGuard<'static, ()> {
        static LOCK: parking_lot::Mutex<()> = parking_lot::Mutex::new(());
        static INIT: std::sync::Once = std::sync::Once::new();
        let guard = LOCK.lock();
        INIT.call_once(|| {
            MemoryFs::init(
                parallel_processor::memory_data_size::MemoryDataSize::from_bytes(64 * 1024 * 1024),
                4,
                1,
                16,
                None,
            );
        });
        guard
    }

    fn root(name: &str) -> PathBuf {
        let path = std::env::temp_dir().join(format!("ggcat-dedup-{}-{name}", std::process::id()));
        std::fs::create_dir_all(&path).unwrap();
        path
    }

    fn writer(path: &Path) -> Arc<LockFreeBinaryWriter> {
        Arc::new(LockFreeBinaryWriter::new(
            path,
            &(
                MemoryFileMode::DiskOnly,
                LockFreeBinaryWriter::CHECKPOINT_SIZE_UNLIMITED,
            ),
            0,
            &MinimizerBucketMode::Single,
        ))
    }

    /// One input record in the format `add` expects.
    #[derive(Clone)]
    struct Record {
        sequence: Vec<u8>,
        flags: u8,
        second_bucket: u8,
        minimizer_pos: u16,
        multiplicity: MultiplicityCounterType,
        colors: Vec<ColorIndexType>,
    }

    impl Record {
        fn new(sequence: &[u8], color: ColorIndexType) -> Self {
            Self {
                sequence: sequence.to_vec(),
                flags: 1,
                second_bucket: 3,
                minimizer_pos: 0,
                multiplicity: 1,
                colors: vec![color],
            }
        }
        fn key(&self) -> (Vec<u8>, u8, u8) {
            (self.sequence.clone(), self.flags, self.second_bucket)
        }
    }

    /// Encodes records the way a producer does: one narrow record per
    /// occurrence, each on its own, so where a slice ends cannot matter.
    ///
    /// A fixture record standing for a multiplicity of `m` over `c` colours is
    /// written as `m` occurrences taking those colours in turn, which is what a
    /// producer would have emitted for it.
    fn encode(records: &[Record]) -> Vec<u8> {
        let mut codec = InputCodec::<MinBkMultipleColors, Flags>::new(K);
        let mut out = Vec::new();
        for record in records {
            for occurrence in 0..record.multiplicity as usize {
                codec.reset();
                codec.write_to(
                    &CompressedReadsBucketData::new(
                        record.sequence.as_slice(),
                        record.flags,
                        record.second_bucket,
                        record.minimizer_pos,
                    ),
                    &mut out,
                    &MinBkSingleColor::create(
                        SingleSequenceInfo {
                            static_color: record.colors[occurrence % record.colors.len()],
                            sequence_ident: SequenceIdent::FASTA(b"fixture"),
                        },
                        &mut (),
                    ),
                    &(),
                );
            }
        }
        out
    }

    /// Reads the output bucket back and groups it the way the input was built.
    fn decode(path: &Path) -> BTreeMap<(Vec<u8>, u8, u8), (u64, Vec<ColorIndexType>)> {
        MemoryFs::flush_to_disk(true);
        MemoryFs::ensure_flushed(path);
        let index = ChunkedBinaryReaderIndex::from_file(path, RemoveFileMode::Keep);
        let mut out: BTreeMap<_, (u64, Vec<ColorIndexType>)> = BTreeMap::new();
        helper_read_bucket::<
            MinBkMultipleColors,
            WithSecondBucket,
            WithMultiplicity,
            AssemblerMinimizerPosition,
            Flags,
            NoAlignment,
        >(
            index.into_chunks(),
            None,
            |read, buffer| {
                let key = (
                    read.read.to_string().into_bytes(),
                    read.flags,
                    read.second_bucket,
                );
                let entry = out.entry(key).or_default();
                entry.0 += read.multiplicity as u64;
                entry
                    .1
                    .extend(expand_runs(read.extra.get_unique_color(buffer)));
            },
            K,
        );
        for value in out.values_mut() {
            value.1.sort_unstable();
            value.1.dedup();
        }
        out
    }

    /// What the deduplicator should produce, computed independently.
    fn expected(records: &[Record]) -> BTreeMap<(Vec<u8>, u8, u8), (u64, Vec<ColorIndexType>)> {
        let mut out: BTreeMap<_, (u64, Vec<ColorIndexType>)> = BTreeMap::new();
        for record in records {
            let entry = out.entry(record.key()).or_default();
            entry.0 += record.multiplicity as u64;
            entry.1.extend(record.colors.iter().copied());
        }
        for value in out.values_mut() {
            value.1.sort_unstable();
            value.1.dedup();
        }
        out
    }

    fn sequence(seed: usize, len: usize) -> Vec<u8> {
        const BASES: &[u8] = b"ACGT";
        let mut state = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
        (0..len)
            .map(|_| {
                state = state
                    .wrapping_mul(6364136223846793005)
                    .wrapping_add(1442695040888963407);
                BASES[(state >> 33) as usize % 4]
            })
            .collect()
    }

    fn run(name: &str, memory: usize, slices: &[Vec<u8>]) -> (PathBuf, DedupStats) {
        let path = root(name).join("out");
        let bucket = writer(&path);
        let dedup = BoundedDeduplicator::<MinBkMultipleColors, Flags, _>::new(memory, K, bucket);
        for slice in slices {
            dedup.add(slice);
        }
        let (bucket, stats) = dedup.finalize();
        let file = bucket.get_path();
        Arc::try_unwrap(bucket).ok().unwrap().finalize();
        (file, stats)
    }

    /// The shipped policy, but with a window floor that fits the 64 KiB test
    /// budget: a real bucket drains every ~6000 records, a test one every ~2000.
    fn test_policy() -> BypassPolicy {
        BypassPolicy {
            min_records: 64,
            ..BypassPolicy::from_config()
        }
    }

    /// A writer tagged for the narrow format the bypass forwards.
    fn narrow_writer(path: &Path) -> Arc<LockFreeBinaryWriter> {
        Arc::new(LockFreeBinaryWriter::new(
            path,
            &(
                MemoryFileMode::DiskOnly,
                LockFreeBinaryWriter::CHECKPOINT_SIZE_UNLIMITED,
            ),
            0,
            &MinimizerBucketMode::UncompactedNarrow,
        ))
    }

    /// Reads a bypass bucket, which holds the producer's own narrow records.
    fn decode_narrow(path: &Path) -> BTreeMap<(Vec<u8>, u8, u8), (u64, Vec<ColorIndexType>)> {
        MemoryFs::flush_to_disk(true);
        MemoryFs::ensure_flushed(path);
        let index = ChunkedBinaryReaderIndex::from_file(path, RemoveFileMode::Keep);
        let mut out: BTreeMap<_, (u64, Vec<ColorIndexType>)> = BTreeMap::new();
        helper_read_bucket::<
            MinBkSingleColor,
            WithSecondBucket,
            NoMultiplicity,
            AssemblerMinimizerPosition,
            Flags,
            NoAlignment,
        >(
            index.into_chunks(),
            None,
            |read, _buffer| {
                let key = (
                    read.read.to_string().into_bytes(),
                    read.flags,
                    read.second_bucket,
                );
                let entry = out.entry(key).or_default();
                // The narrow format carries no multiplicity, so each record
                // stands for exactly one occurrence.
                assert_eq!(read.multiplicity, 1);
                entry.0 += 1;
                entry.1.push(read.extra.get_unique_color(&()));
            },
            K,
        );
        for value in out.values_mut() {
            value.1.sort_unstable();
            value.1.dedup();
        }
        out
    }

    /// As [`run`], but with somewhere to bypass to. Returns both sides.
    fn run_with_bypass(
        name: &str,
        memory: usize,
        policy: BypassPolicy,
        slices: &[Vec<u8>],
    ) -> (PathBuf, PathBuf, DedupStats) {
        let root = root(name);
        let wide_path = root.join("wide");
        let narrow_path = root.join("narrow");
        let wide = writer(&wide_path);
        let narrow = narrow_writer(&narrow_path);
        let dedup = BoundedDeduplicator::<MinBkMultipleColors, Flags, _>::new_with_bypass(
            memory,
            K,
            wide,
            Some(narrow),
            policy,
        );
        for slice in slices {
            dedup.add(slice);
        }
        let (wide, narrow, stats) = dedup.finalize_with_bypass();
        let (wide_file, narrow_file) = (wide.get_path(), narrow.as_ref().unwrap().get_path());
        Arc::try_unwrap(wide).ok().unwrap().finalize();
        Arc::try_unwrap(narrow.unwrap()).ok().unwrap().finalize();
        (wide_file, narrow_file, stats)
    }

    /// Merges what the two sides hold, the way the compactor does.
    fn union(
        wide: BTreeMap<(Vec<u8>, u8, u8), (u64, Vec<ColorIndexType>)>,
        narrow: BTreeMap<(Vec<u8>, u8, u8), (u64, Vec<ColorIndexType>)>,
    ) -> BTreeMap<(Vec<u8>, u8, u8), (u64, Vec<ColorIndexType>)> {
        let mut out = wide;
        for (key, (multiplicity, colors)) in narrow {
            let entry = out.entry(key).or_default();
            entry.0 += multiplicity;
            entry.1.extend(colors);
        }
        for value in out.values_mut() {
            value.1.sort_unstable();
            value.1.dedup();
        }
        out
    }

    #[test]
    fn bypass_forwards_records_untouched() {
        let _guard = memory_fs();
        let records: Vec<_> = (0..200)
            .map(|i| Record::new(&sequence(i, 40 + i % 25), (i % 17) as ColorIndexType))
            .collect();

        // One slice per record, then the whole lot in one slice: where a slice
        // ends must not change what comes out, which is what lets the bypass
        // forward the producer's bytes without looking at them.
        for (name, slices) in [
            (
                "bypass-per-record",
                records
                    .iter()
                    .map(|r| encode(std::slice::from_ref(r)))
                    .collect::<Vec<_>>(),
            ),
            ("bypass-one-slice", vec![encode(&records)]),
            (
                "bypass-uneven",
                records.chunks(7).map(encode).collect::<Vec<_>>(),
            ),
        ] {
            let (wide, narrow, stats) = run_with_bypass(name, M, BypassPolicy::always(), &slices);
            assert_eq!(stats.records_in, 0, "{name}: nothing should be decoded");
            assert_eq!(stats.records_out, 0, "{name}");
            assert!(stats.bypassed_bytes > 0, "{name}");
            assert!(
                decode(&wide).is_empty(),
                "{name}: wide side should be empty"
            );
            assert_eq!(decode_narrow(&narrow), expected(&records), "{name}");
        }
    }

    #[test]
    fn bypass_triggers_on_unique_records() {
        let _guard = memory_fs();
        // All distinct, so nothing ever collapses.
        let records: Vec<_> = (0..6000)
            .map(|i| Record::new(&sequence(i, 40 + i % 25), (i % 97) as ColorIndexType))
            .collect();
        let slices: Vec<_> = records.chunks(64).map(encode).collect();

        let (wide, narrow, stats) = run_with_bypass("bypass-unique", M, test_policy(), &slices);

        assert!(
            stats.bypass_windows > 0,
            "a bucket that collapses nothing should stand down: {}",
            stats.report()
        );
        assert!(stats.bypassed_bytes > 0, "{}", stats.report());
        // Whatever the split, together they are the input.
        assert_eq!(
            union(decode(&wide), decode_narrow(&narrow)),
            expected(&records)
        );
    }

    #[test]
    fn bypass_does_not_trigger_on_redundant_records() {
        let _guard = memory_fs();
        // Sixty distinct super-kmers, each repeated a hundred times.
        let records: Vec<_> = (0..6000)
            .map(|i| Record::new(&sequence(i % 60, 45), (i % 13) as ColorIndexType))
            .collect();
        let slices: Vec<_> = records.chunks(64).map(encode).collect();

        let (wide, narrow, stats) = run_with_bypass("bypass-redundant", M, test_policy(), &slices);

        assert_eq!(
            stats.bypass_windows,
            0,
            "a bucket collapsing 99% must keep folding: {}",
            stats.report()
        );
        assert_eq!(stats.bypassed_bytes, 0, "{}", stats.report());
        assert!(decode_narrow(&narrow).is_empty());
        assert_eq!(decode(&wide), expected(&records));
    }

    #[test]
    fn records_survive_a_mid_stream_flip() {
        let _guard = memory_fs();
        // Redundant first, then unique: the bucket should keep folding, then
        // stand down, and nothing may be lost or counted twice at the seam.
        let redundant: Vec<_> = (0..3000)
            .map(|i| Record::new(&sequence(i % 40, 45), (i % 11) as ColorIndexType))
            .collect();
        let unique: Vec<_> = (0..6000)
            .map(|i| {
                Record::new(
                    &sequence(100_000 + i, 40 + i % 25),
                    (i % 23) as ColorIndexType,
                )
            })
            .collect();
        let all: Vec<_> = redundant.iter().chain(unique.iter()).cloned().collect();
        let slices: Vec<_> = all.chunks(64).map(encode).collect();

        let (wide, narrow, stats) = run_with_bypass("bypass-flip", M, test_policy(), &slices);

        assert!(stats.bypass_windows > 0, "{}", stats.report());
        assert!(
            stats.records_in > 0,
            "the redundant prefix should have been folded"
        );
        assert_eq!(
            union(decode(&wide), decode_narrow(&narrow)),
            expected(&all),
            "the two sides together must be exactly the input: {}",
            stats.report()
        );
    }

    /// An extra data type that cannot be combined at all must never reach the
    /// folding path, because its `combine_entries` is `unimplemented!()`. That
    /// is the querier and dumper configuration, where a repeated super-kmer
    /// used to panic.
    #[test]
    fn a_non_combinable_type_always_bypasses() {
        use colors::parsers::graph::MinBkMultipleColors as NonCombinable;
        let _guard = memory_fs();
        assert!(
            !<NonCombinable as SequenceExtraDataCombiner>::ALLOW_COMBINE,
            "fixture must be a type that cannot be combined"
        );

        let root = root("bypass-no-combine");
        let dedup = BoundedDeduplicator::<NonCombinable, Flags, _>::new_with_bypass(
            M,
            K,
            writer(&root.join("wide")),
            Some(narrow_writer(&root.join("narrow"))),
            // Even a policy that would never choose to stand down must not
            // override this: the decision is not the heuristic's to make.
            BypassPolicy::never(),
        );
        assert!(
            dedup.is_bypassing(),
            "a non-combinable type has to bypass from the first record"
        );
        let (wide, narrow, _) = dedup.finalize_with_bypass();
        Arc::try_unwrap(wide).ok().unwrap().finalize();
        Arc::try_unwrap(narrow.unwrap()).ok().unwrap().finalize();
    }

    #[test]
    fn deduplicates_and_unions_colors() {
        let _guard = memory_fs();
        let a = b"ACGTACGTACGTACGTACGTACGTACGTACGTA".to_vec();
        let b = b"TTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTTT".to_vec();
        let records: Vec<_> = [(&a, 1), (&b, 7), (&a, 2), (&a, 1), (&b, 8), (&a, 3)]
            .into_iter()
            .map(|(sequence, color)| Record::new(sequence, color))
            .collect();

        let (path, stats) = run("union", M, &[encode(&records)]);
        assert_eq!(stats.records_in, 6);
        assert_eq!(stats.records_out, 2);
        assert_eq!(stats.drains, 1);
        assert_eq!(decode(&path), expected(&records));
    }

    // Records that differ only in a field of the key stay separate. second_bucket
    // matters most: memstorage does not serialize it, so it is carried
    // separately and could silently collapse or be zeroed on output.
    #[test]
    fn key_fields_separate_records() {
        let _guard = memory_fs();
        let sequence = b"ACGTACGTACGTACGTACGTACGTACGTACGTA";
        let mut records = Vec::new();
        for (flags, second_bucket, minimizer_pos) in
            [(1u8, 3u8, 0u16), (2, 3, 0), (1, 4, 0), (1, 3, 0)]
        {
            let mut record = Record::new(sequence, 5);
            record.flags = flags;
            record.second_bucket = second_bucket;
            record.minimizer_pos = minimizer_pos;
            records.push(record);
        }
        let (path, stats) = run("keys", M, &[encode(&records)]);
        assert_eq!(stats.records_out, 3);
        let decoded = decode(&path);
        assert_eq!(decoded, expected(&records));
        // The sub-bucket byte really is reproduced, not zeroed.
        assert!(
            decoded
                .keys()
                .any(|(_, _, second_bucket)| *second_bucket == 4)
        );
        assert!(
            decoded
                .keys()
                .all(|(_, _, second_bucket)| *second_bucket != 0)
        );
    }

    #[test]
    fn multiplicity_is_summed() {
        let _guard = memory_fs();
        let sequence = b"ACGTACGTACGTACGTACGTACGTACGTACGTA";
        let records: Vec<_> = (0..4)
            .map(|i| {
                let mut record = Record::new(sequence, 5);
                record.multiplicity = 10 + i;
                record
            })
            .collect();
        let (path, stats) = run("mult", M, &[encode(&records)]);
        assert_eq!(stats.records_out, 1);
        assert_eq!(decode(&path).values().next().unwrap().0, 10 + 11 + 12 + 13);
    }

    // Enough distinct super-kmers to force repeated drains. A super-kmer that
    // spans two drains is emitted twice with partial multiplicity, which is the
    // defining semantic of a bounded deduplicator -- the union still has to
    // reconstruct exactly.
    #[test]
    fn drains_repeatedly_and_stays_within_budget() {
        let _guard = memory_fs();
        let records: Vec<_> = (0..4000)
            .map(|i| Record::new(&sequence(i, 40 + i % 25), (i % 97) as ColorIndexType))
            .collect();
        let slices: Vec<_> = records.chunks(64).map(encode).collect();
        let (path, stats) = run("drain", M, &slices);

        assert!(
            stats.drains > 1,
            "expected several drains, got {}",
            stats.drains
        );
        assert!(stats.records_out > 0);
        assert!(
            stats.peak_storage <= M,
            "storage peaked at {}",
            stats.peak_storage
        );
        assert!(
            stats.peak_arena <= M,
            "arena peaked at {}",
            stats.peak_arena
        );
        assert!(
            stats.table_bytes <= M / 4,
            "table is {} bytes",
            stats.table_bytes
        );
        assert_eq!(decode(&path), expected(&records));
    }

    // Long super-kmers make the bases storage, rather than the table, the first
    // bound to be reached. With short records the table always binds first, so
    // without this the storage branch is never taken.
    #[test]
    fn long_records_drain_on_the_storage_bound() {
        let _guard = memory_fs();
        let records: Vec<_> = (0..1200)
            .map(|i| Record::new(&sequence(i, 250), (i % 7) as ColorIndexType))
            .collect();
        let slices: Vec<_> = records.chunks(32).map(encode).collect();
        let (path, stats) = run("storage", M, &slices);
        assert!(
            stats.drains_storage > 0,
            "expected the storage bound to bind: {}",
            stats.report()
        );
        assert!(
            stats.peak_storage <= M,
            "storage peaked at {}",
            stats.peak_storage
        );
        assert_eq!(decode(&path), expected(&records));
    }

    // A record too large to ever be stored bypasses the table and the storage
    // and is written straight out, instead of looping forever on a drain that
    // cannot make room.
    #[test]
    fn a_record_larger_than_the_storage_is_written_straight_out() {
        let _guard = memory_fs();
        let huge = Record::new(&sequence(1, 4 * M), 5);
        let small = Record::new(&sequence(2, 40), 6);
        let records = vec![small.clone(), huge.clone(), small];
        let (path, stats) = run("huge", M, &[encode(&records)]);
        assert_eq!(stats.oversized_records, 1, "{}", stats.report());
        assert_eq!(decode(&path), expected(&records));
    }

    // A slice larger than one buffer bypasses them entirely.
    #[test]
    fn oversized_slice_is_processed_inline() {
        let _guard = memory_fs();
        let records: Vec<_> = (0..3000)
            .map(|i| Record::new(&sequence(i, 60), (i % 11) as ColorIndexType))
            .collect();
        let slice = encode(&records);
        assert!(slice.len() > M / 2, "fixture is not oversized");
        let (path, stats) = run("inline", M, &[slice]);
        assert_eq!(stats.inline_slices, 1);
        assert_eq!(stats.seals, 0);
        assert_eq!(decode(&path), expected(&records));
    }

    // Many slices that each fit, so the buffers fill and swap.
    #[test]
    fn buffers_swap_without_losing_records() {
        let _guard = memory_fs();
        let records: Vec<_> = (0..6000)
            .map(|i| Record::new(&sequence(i % 500, 45), (i % 31) as ColorIndexType))
            .collect();
        let slices: Vec<_> = records.chunks(16).map(encode).collect();
        let (path, stats) = run("swap", M, &slices);
        assert!(
            stats.seals > 2,
            "expected several seals, got {}",
            stats.seals
        );
        assert_eq!(stats.records_in, 6000);
        assert_eq!(decode(&path), expected(&records));
    }

    // The lock-free append path, hammered from several threads. Total
    // multiplicity and the colour union must come out exact.
    #[test]
    fn concurrent_appends_lose_nothing() {
        let _guard = memory_fs();
        let records: Vec<_> = (0..8000)
            .map(|i| Record::new(&sequence(i % 700, 40 + i % 30), (i % 53) as ColorIndexType))
            .collect();
        let slices: Vec<Vec<u8>> = records.chunks(8).map(encode).collect();

        let path = root("threads").join("out");
        let bucket = writer(&path);
        let dedup = Arc::new(BoundedDeduplicator::<MinBkMultipleColors, Flags, _>::new(
            M, K, bucket,
        ));
        std::thread::scope(|scope| {
            for shard in 0..8 {
                let dedup = &dedup;
                let slices = &slices;
                scope.spawn(move || {
                    for slice in slices.iter().skip(shard).step_by(8) {
                        dedup.add(slice);
                    }
                });
            }
        });
        let dedup = Arc::try_unwrap(dedup).ok().unwrap();
        let (bucket, stats) = dedup.finalize();
        let file = bucket.get_path();
        Arc::try_unwrap(bucket).ok().unwrap().finalize();

        assert!(
            stats.seals > 2,
            "expected several seals, got {}",
            stats.seals
        );
        assert_eq!(stats.records_in, 8000);
        assert_eq!(decode(&file), expected(&records));
    }

    // The drain buffer belongs to the thread, not the deduplicator, so a record
    // written by one thread must never be left sitting in that thread's buffer
    // for another thread's drain to miss. Oversized records take the one path
    // that writes outside a drain, so they are what exercises it.
    #[test]
    fn thread_local_staging_loses_nothing_across_threads() {
        let _guard = memory_fs();
        // Enough distinct super-kmers to force several drains while the
        // oversized records are going out.
        let mut records: Vec<Record> = (0..6000)
            .map(|i| Record::new(&sequence(i % 3000, 45), (i % 23) as ColorIndexType))
            .collect();
        // One record per thread that no drain could make room for.
        for shard in 0..4 {
            records.insert(
                shard * 1500,
                Record::new(
                    &sequence(9000 + shard, 4 * M),
                    (shard + 90) as ColorIndexType,
                ),
            );
        }
        let slices: Vec<Vec<u8>> = records.chunks(4).map(|c| encode(c)).collect();

        let path = root("tls").join("out");
        let bucket = writer(&path);
        let dedup = Arc::new(BoundedDeduplicator::<MinBkMultipleColors, Flags, _>::new(
            M, K, bucket,
        ));
        std::thread::scope(|scope| {
            for shard in 0..4 {
                let dedup = &dedup;
                let slices = &slices;
                scope.spawn(move || {
                    for slice in slices.iter().skip(shard).step_by(4) {
                        dedup.add(slice);
                    }
                });
            }
        });
        let (bucket, stats) = Arc::try_unwrap(dedup).ok().unwrap().finalize();
        let file = bucket.get_path();
        Arc::try_unwrap(bucket).ok().unwrap().finalize();

        assert_eq!(stats.oversized_records, 4, "{}", stats.report());
        assert!(stats.drains > 1, "{}", stats.report());
        assert_eq!(decode(&file), expected(&records));
    }

    // The deduplicator's own output is valid input, and feeding it back changes
    // nothing.
    #[test]
    fn slicing_does_not_change_the_result() {
        let _guard = memory_fs();
        let records: Vec<_> = (0..1500)
            .map(|i| Record::new(&sequence(i % 200, 50), (i % 17) as ColorIndexType))
            .collect();
        let (whole, _) = run("slice-whole", M, &[encode(&records)]);

        // One slice per record is the worst case for a format that carried
        // state from one record to the next.
        let per_record: Vec<_> = records
            .iter()
            .map(|record| encode(std::slice::from_ref(record)))
            .collect();
        let (split, _) = run("slice-each", M, &per_record);

        // And a few uneven groupings, which is what threads actually produce.
        let grouped: Vec<_> = records.chunks(7).map(encode).collect();
        let (chunked, _) = run("slice-groups", M, &grouped);

        let reference = decode(&whole);
        assert_eq!(decode(&split), reference);
        assert_eq!(decode(&chunked), reference);
    }

    // The non-coloured instantiation has to compile and behave, with every arena
    // operation monomorphized away.
    #[test]
    fn non_colored_deduplicates() {
        let _guard = memory_fs();
        let sequence = b"ACGTACGTACGTACGTACGTACGTACGTACGTA";
        let mut codec = InputCodec::<NonColoredManager, Flags>::new(K);
        let mut bytes = Vec::new();
        for _ in 0..10 {
            codec.reset();
            codec.write_to(
                &CompressedReadsBucketData::new(sequence.as_slice(), 1, 3, 0),
                &mut bytes,
                &NonColoredManager,
                &(),
            );
        }
        let path = root("plain").join("out");
        let bucket = writer(&path);
        let dedup = BoundedDeduplicator::<NonColoredManager, Flags, _>::new(M, K, bucket);
        dedup.add(&bytes);
        let (bucket, stats) = dedup.finalize();
        assert_eq!(stats.records_in, 10);
        assert_eq!(stats.records_out, 1);
        assert_eq!(
            stats.peak_arena, 0,
            "the plain path must not touch an arena"
        );
        let file = bucket.get_path();
        Arc::try_unwrap(bucket).ok().unwrap().finalize();

        MemoryFs::flush_to_disk(true);
        MemoryFs::ensure_flushed(&file);
        let index = ChunkedBinaryReaderIndex::from_file(&file, RemoveFileMode::Keep);
        let mut seen = 0;
        helper_read_bucket::<
            NonColoredManager,
            WithSecondBucket,
            WithMultiplicity,
            AssemblerMinimizerPosition,
            Flags,
            NoAlignment,
        >(
            index.into_chunks(),
            None,
            |read, _| {
                assert_eq!(read.read.to_string().as_bytes(), sequence);
                assert_eq!(read.multiplicity, 10);
                assert_eq!(read.second_bucket, 3);
                seen += 1;
            },
            K,
        );
        assert_eq!(seen, 1);
    }

    #[test]
    fn empty_input_writes_nothing() {
        let _guard = memory_fs();
        let (_, stats) = run("empty", M, &[Vec::new()]);
        assert_eq!(stats.records_in, 0);
        assert_eq!(stats.records_out, 0);
    }

    #[test]
    #[should_panic(expected = "power of two")]
    fn rejects_a_non_power_of_two_budget() {
        let _guard = memory_fs();
        let bucket = writer(&root("bad").join("out"));
        let _ = BoundedDeduplicator::<MinBkMultipleColors, Flags, _>::new(100_000, K, bucket);
    }

    // Silence the unused warnings for helpers only some instantiations use.
    #[allow(dead_code)]
    fn _assert_no_multiplicity_alias(_: NoMultiplicity) {}
}

/// One deduplicator per bucket, plus the per-thread buffers that feed them.
///
/// This is `BucketsThreadDispatcher` with a different sink: a full per-thread
/// buffer goes to its bucket's deduplicator rather than straight to the bucket
/// file, and the deduplicator writes what survives. The buffers are the same
/// size and are filled the same way, so the only change a producer sees is where
/// a flush lands.
pub struct DeduplicatingDispatcher<
    'a,
    MultipleData: DedupExtraData,
    FlagsCount: typenum::Unsigned,
    O: DedupOutput,
    Bypass: DedupOutput = O,
> {
    deduplicators: &'a [BoundedDeduplicator<MultipleData, FlagsCount, O, Bypass>],
    buffers: Vec<Vec<u8>>,
    /// One codec per bucket, reset per record to match the decoder.
    codecs: Vec<InputCodec<MultipleData, FlagsCount>>,
    finalized: bool,
}

impl<
    'a,
    MultipleData: DedupExtraData,
    FlagsCount: typenum::Unsigned,
    O: DedupOutput,
    Bypass: DedupOutput,
> DeduplicatingDispatcher<'a, MultipleData, FlagsCount, O, Bypass>
{
    pub fn new(
        deduplicators: &'a [BoundedDeduplicator<MultipleData, FlagsCount, O, Bypass>],
        buffer_size: usize,
        k: usize,
    ) -> Self {
        Self {
            buffers: (0..deduplicators.len())
                .map(|_| Vec::with_capacity(buffer_size))
                .collect(),
            codecs: (0..deduplicators.len())
                .map(|_| InputCodec::<MultipleData, FlagsCount>::new(k))
                .collect(),
            deduplicators,
            finalized: false,
        }
    }

    pub fn add_element_extended(
        &mut self,
        bucket: u16,
        extra_data: &SingleOf<MultipleData>,
        extra_data_buffer: &TempBuffer<SingleOf<MultipleData>>,
        element: &CompressedReadsBucketData<'_>,
    ) -> ChunkingStatus {
        let index = bucket as usize;
        let buffer = &mut self.buffers[index];
        let codec = &mut self.codecs[index];
        let mut status = ChunkingStatus::SameChunk;
        // Every record stands on its own, so a sealed buffer can hold runs from
        // several threads back to back.
        codec.reset();
        // Flush before overflowing, so a buffer never reallocates.
        if codec.get_size(element, extra_data) + buffer.len() > buffer.capacity()
            && !buffer.is_empty()
        {
            status = self.deduplicators[index].add(buffer);
            buffer.clear();
        }
        codec.write_to(element, buffer, extra_data, extra_data_buffer);
        status
    }

    /// Hands every partial buffer over. The deduplicators themselves are
    /// finished separately, once every thread's dispatcher has been finalized.
    pub fn finalize(mut self) {
        for (index, buffer) in self.buffers.iter_mut().enumerate() {
            if !buffer.is_empty() {
                self.deduplicators[index].add(buffer);
                buffer.clear();
            }
        }
        self.finalized = true;
    }
}

impl<
    MultipleData: DedupExtraData,
    FlagsCount: typenum::Unsigned,
    O: DedupOutput,
    Bypass: DedupOutput,
> Drop for DeduplicatingDispatcher<'_, MultipleData, FlagsCount, O, Bypass>
{
    fn drop(&mut self) {
        // Dropping with records still buffered would lose them silently, which
        // is the one failure mode that produces a plausible but wrong graph.
        // Not during an unwind, though: panicking there would abort and bury
        // whatever went wrong first.
        assert!(
            self.finalized
                || std::thread::panicking()
                || self.buffers.iter().all(|buffer| buffer.is_empty()),
            "a deduplicating dispatcher was dropped with buffered records"
        );
    }
}

/// Per-bucket memory for the deduplicators, chosen so the whole array stays
/// within [`MINIMIZER_DEDUPLICATION_MEMORY`] and every deduplicator still gets a
/// window worth having. A power of two, as the constructor requires.
pub fn deduplicator_memory(buckets_count: usize, budget: usize) -> usize {
    const MIN: usize = 64 * 1024;
    const MAX: usize = 1024 * 1024;
    // A deduplicator costs 3.25 M; round that up to 4 so the array stays inside
    // the budget, then take the largest power of two that fits.
    let per_bucket = budget / buckets_count.max(1) / 4;
    let fitting = if per_bucket == 0 {
        MIN
    } else {
        1usize << per_bucket.ilog2()
    };
    fitting.clamp(MIN, MAX)
}
