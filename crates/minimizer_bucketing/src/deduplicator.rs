//! A memory-bounded, parallelism-aware super-kmer deduplicator.
//!
//! One of these sits in front of every uncompacted bucket: the bucketing threads
//! hand it the per-thread buffers they would otherwise have written straight to
//! the bucket file, and it folds repeated super-kmers together before they reach
//! disk. [`BucketsCompactor`](crate::compactor::BucketsCompactor) still does the
//! full job later; this only takes the cheap duplicates out of the way first.
//!
//! Unlike the compactor, it holds to a fixed budget of 3.25 M — two M/2 intake
//! buffers, an M bases storage, an M/4 hashmap and an M colour arena — and
//! accepts records from any number of threads without a lock on the fast path.
//!
//! # How a record travels
//!
//! This file holds the steps in the order a record meets them; each step calls
//! into a file under `deduplicator/` that can be read on its own.
//!
//! 1. `dispatcher.rs`: a bucketing thread fills one buffer per bucket with
//!    *narrow* records (one colour, no multiplicity; see `formats.rs`) and hands
//!    a full one to that bucket's [`BoundedDeduplicator::add`].
//! 2. `bypass.rs`: if the bucket is standing down, `add` forwards the slice
//!    untouched to the narrow bucket, and that is the end of it.
//! 3. `double_buffer.rs`: otherwise the slice is copied into the active half of
//!    a double buffer, without a lock. The thread that finds it full seals it
//!    and walks it, under the state lock, while the other half takes appends.
//! 4. `decoder.rs`: walking decodes the sealed bytes one record at a time and
//!    widens each record's colour to a colour set.
//! 5. `store.rs`: each record is folded into the one already stored for it, or
//!    stored if it is new. When the store is full it is drained and the record
//!    offered again.
//! 6. `encoder.rs`: a drain writes every stored record to the output bucket in
//!    the *wide* form the compactor reads, and empties the store.
//! 7. `bypass.rs`: the drain also closes a window, and a window that folded too
//!    few records stands the bucket down for a while (step 2).
//!
//! `output.rs` holds the places a drain or a bypass writes to.

mod bypass;
mod decoder;
mod dispatcher;
mod double_buffer;
mod encoder;
mod formats;
mod output;
mod store;
#[cfg(test)]
mod tests;

pub use bypass::BypassPolicy;
pub use dispatcher::DeduplicatingDispatcher;
pub use formats::DedupExtraData;
pub use output::{BucketOutput, DedupOutput, LazyBucketOutput, LazyBuckets};

use bypass::{BypassSwitch, Window};
use decoder::NarrowDecoder;
use double_buffer::DoubleBuffer;
use encoder::WideEncoder;
use io::concurrent::temp_reads::extra_data::SequenceExtraDataConsecutiveCompression;
use parallel_processor::buckets::ChunkingStatus;
use parking_lot::Mutex;
use store::FoldStore;

/// How many times an append retries the buffers before processing itself
/// inline. Bounds the starvation of a thread that keeps losing the seal race;
/// the inline path it falls back to has to exist anyway for oversized slices.
const MAX_APPEND_ROUNDS: usize = 2;

pub struct BoundedDeduplicator<
    MultipleData: DedupExtraData,
    FlagsCount: typenum::Unsigned,
    O: DedupOutput,
    // Where bypassed records go. It differs from `O` in the pipeline, where
    // the narrow buckets are built only once some bucket actually stands down.
    Bypass: DedupOutput = O,
> {
    /// Step 2.
    bypass: BypassSwitch<Bypass>,
    /// Step 3.
    intake: DoubleBuffer,
    /// Steps 4 to 7. The seal lock and the folding state are deliberately the
    /// same lock: the epoch is then bumped and the buffer walked inside one
    /// critical section, so the epoch never runs more than one ahead of
    /// processing and exactly two buffers are live at any instant.
    state: Mutex<DedupState<MultipleData, FlagsCount>>,
    /// Where drains write.
    output: O,
}

/// Everything a walk touches. Held behind one mutex, so processing is
/// single-threaded however many threads are appending.
struct DedupState<MultipleData: DedupExtraData, FlagsCount: typenum::Unsigned> {
    decoder: NarrowDecoder<MultipleData, FlagsCount>,
    store: FoldStore<MultipleData>,
    encoder: WideEncoder<MultipleData, FlagsCount>,
    window: Window,
}

impl<
    MultipleData: DedupExtraData,
    FlagsCount: typenum::Unsigned,
    O: DedupOutput,
    Bypass: DedupOutput,
> BoundedDeduplicator<MultipleData, FlagsCount, O, Bypass>
{
    /// `memory` is M and must be a power of two of at least 64 KiB. `k` is the
    /// format's minimum sequence length.
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
        // A permanently bypassing deduplicator never writes through the wide
        // format, so its delta state is never used and need not be stateless.
        let force_bypass = !MultipleData::ALLOW_COMBINE;
        assert!(
            !force_bypass || bypass_output.is_some(),
            "an extra data type that cannot be combined needs somewhere to bypass to"
        );
        // Otherwise a stateful `LastData` would need every format reset paired
        // with a checkpoint the reader restarts at. Both combinable extra data
        // types have a zero-sized one, and this keeps it that way.
        assert!(
            force_bypass
                || size_of::<<MultipleData as SequenceExtraDataConsecutiveCompression>::LastData>()
                    == 0,
            "a stateful LastData needs checkpoint-paired format resets"
        );

        Self {
            bypass: BypassSwitch::new(bypass_output, force_bypass, &policy, memory),
            intake: DoubleBuffer::new(memory / 2),
            state: Mutex::new(DedupState {
                decoder: NarrowDecoder::new(k, memory / 16),
                store: FoldStore::new(memory),
                // Small and fixed: ggcat's DEFAULT_OUTPUT_BUFFER_SIZE is 4 MiB,
                // larger than M itself for a 512 KiB budget.
                encoder: WideEncoder::new(k, memory / 8),
                window: Window::new(policy),
            }),
            output,
        }
    }

    /// Whether this bucket is currently standing its deduplicator down.
    pub fn is_bypassing(&self) -> bool {
        self.bypass.is_engaged()
    }

    /// Steps 2 and 3: appends a slice holding a whole number of encoded records.
    /// Callable from any thread; never blocks on another appender, only behind a
    /// buffer that is being walked.
    pub fn add(&self, slice: &[u8]) -> ChunkingStatus {
        if slice.is_empty() {
            return ChunkingStatus::SameChunk;
        }

        // Standing down: the slice already holds exactly the records the
        // compactor's narrow reader expects, so it goes out untouched. Not
        // decoding it is the whole point -- a decode and re-encode here would
        // give back most of what bypassing saves.
        if let Some(status) = self.bypass.forward(slice) {
            return status;
        }

        if slice.len() > self.intake.slot_capacity() {
            // Walked straight out of the caller's memory: the decoder never
            // reads past what it consumes, so no copy and no resize.
            return self.process_inline(slice);
        }

        let mut chunked = false;
        for round in 0.. {
            let Err(full_epoch) = self.intake.try_append(slice) else {
                break;
            };
            if round >= MAX_APPEND_ROUNDS {
                // Stop competing and make guaranteed progress.
                chunked |= matches!(self.process_inline(slice), ChunkingStatus::NewChunk);
                break;
            }
            chunked |= matches!(self.seal(full_epoch), ChunkingStatus::NewChunk);
        }
        if chunked {
            ChunkingStatus::NewChunk
        } else {
            ChunkingStatus::SameChunk
        }
    }

    /// Retires the buffer that was full at `full_epoch` and walks it, unless
    /// another thread already has.
    #[cold]
    fn seal(&self, full_epoch: usize) -> ChunkingStatus {
        let mut state = self.state.lock();
        self.intake
            .seal(full_epoch, |data| self.walk(&mut state, data));
        state.encoder.take_chunking()
    }

    #[cold]
    fn process_inline(&self, slice: &[u8]) -> ChunkingStatus {
        let mut state = self.state.lock();
        self.walk(&mut state, slice);
        state.encoder.take_chunking()
    }

    /// Steps 4 and 5: decodes every record of `data` and folds it into the
    /// store, draining whenever the store is full.
    fn walk(&self, state: &mut DedupState<MultipleData, FlagsCount>, data: &[u8]) {
        let DedupState {
            decoder,
            store,
            encoder,
            window,
        } = state;
        decoder.for_each(data, |record, arena| {
            window.records_in += 1;
            if store.can_ever_hold(&record) {
                if store.try_insert(&record, arena) {
                    return;
                }
                self.drain(store, encoder, window);
                if store.try_insert(&record, arena) {
                    return;
                }
            }
            // Alone it passes a bound of an empty store, so no drain can make
            // room for it: it goes straight out.
            window.records_out +=
                encoder.write_batch(&self.output, |batch| batch.write(&record, arena));
        });
    }

    /// Steps 6 and 7: writes every stored record out, empties the store and
    /// judges the window that just closed.
    ///
    /// Judging here rather than in `add` means it runs under the lock that
    /// already exists, and that a switch cannot split a slice: `walk` never
    /// re-reads it, so the records still in flight finish through the folding
    /// path and the ones already stored have just been written by this very
    /// drain.
    fn drain(
        &self,
        store: &mut FoldStore<MultipleData>,
        encoder: &mut WideEncoder<MultipleData, FlagsCount>,
        window: &mut Window,
    ) {
        window.records_out += encoder.write_batch(&self.output, |batch| {
            store.drain(|record, arena| batch.write(record, arena))
        });
        if window.close() {
            self.bypass.engage();
        }
    }

    /// Processes both buffers, drains what is left and flushes the output.
    ///
    /// Takes `&self` so a deduplicator that lives in a shared array can be
    /// finished in place. Every appender must have been joined first: this makes
    /// no attempt to exclude one.
    pub fn finish(&self) {
        let mut state = self.state.lock();
        self.intake.seal_both(|data| self.walk(&mut state, data));
        let DedupState {
            store,
            encoder,
            window,
            ..
        } = &mut *state;
        self.drain(store, encoder, window);
    }

    /// Consuming counterpart of [`Self::finish`], which also hands the output
    /// back.
    pub fn finalize(self) -> O {
        self.finish();
        self.output
    }

    /// As [`Self::finalize`], for a deduplicator that was given somewhere to
    /// bypass to.
    pub fn finalize_with_bypass(self) -> (O, Option<Bypass>) {
        self.finish();
        (self.output, self.bypass.into_output())
    }
}

/// Per-bucket memory for the deduplicators, chosen so the whole array stays
/// within [`MINIMIZER_DEDUPLICATION_MEMORY`] and every deduplicator still gets a
/// window worth having. A power of two, as the constructor requires.
///
/// [`MINIMIZER_DEDUPLICATION_MEMORY`]: config::MINIMIZER_DEDUPLICATION_MEMORY
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
