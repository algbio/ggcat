//! Standing a bucket's deduplicator down while it is not paying for itself.
//!
//! A deduplicator only pays for itself if it actually folds records together.
//! On input with no repeated super-kmers it collapses nothing while still
//! decoding, widening, hashing, storing and re-encoding every record. So every
//! drain closes a [`Window`], judged by how many of its records were folded
//! away; one that folded too few engages the [`BypassSwitch`], which forwards
//! the next `skip_factor` storage-fulls of input to the narrow bucket
//! untouched. Once that is spent the bucket folds again and its next window is
//! judged afresh, so a bucket that becomes redundant later is not written off.
//!
//! The switch is read on every `add` from every thread, so it is atomics only;
//! the window lives behind the state lock with the rest of the folding state.

use super::output::DedupOutput;
use parallel_processor::buckets::ChunkingStatus;
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};

/// When a bucket should stop deduplicating.
#[derive(Copy, Clone, Debug)]
pub struct BypassPolicy {
    /// Percent of records a window must remove to be worth continuing. Above
    /// 100 it is unreachable, so any window with a surviving record bypasses.
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

/// Whether `add` forwards its slice instead of folding it, and where to.
pub(super) struct BypassSwitch<B: DedupOutput> {
    /// Where records go while the switch is engaged. They are forwarded in the
    /// format they arrived in, so nothing is decoded on that path.
    output: Option<B>,
    /// Set for an extra data type that cannot be combined at all, in which case
    /// deduplicating is not merely unprofitable but meaningless: the switch is
    /// engaged for good.
    forced: bool,
    engaged: AtomicBool,
    /// Bytes this engagement still covers before the bucket is measured again.
    bytes_left: AtomicI64,
    /// Bytes one engagement covers: `skip_factor` storage-fulls.
    span: i64,
}

impl<B: DedupOutput> BypassSwitch<B> {
    pub(super) fn new(
        output: Option<B>,
        forced: bool,
        policy: &BypassPolicy,
        storage_bytes: usize,
    ) -> Self {
        let span = (storage_bytes as i64).saturating_mul(policy.skip_factor as i64);
        Self {
            output,
            forced,
            engaged: AtomicBool::new(forced || policy.start_bypassing),
            // Starting stood down still needs a budget, or the first slice
            // would spend it and fold everything until the next drain judged
            // the bucket.
            bytes_left: AtomicI64::new(if policy.start_bypassing { span } else { 0 }),
            span,
        }
    }

    pub(super) fn is_engaged(&self) -> bool {
        self.forced || self.engaged.load(Ordering::Relaxed)
    }

    /// Forwards `slice` untouched if the switch is engaged, or returns `None`
    /// if it has to be folded.
    #[inline]
    pub(super) fn forward(&self, slice: &[u8]) -> Option<ChunkingStatus> {
        let output = self.output.as_ref()?;
        if !self.is_engaged() {
            return None;
        }
        let status = output.write_records(slice);
        if !self.forced
            && self
                .bytes_left
                .fetch_sub(slice.len() as i64, Ordering::Relaxed)
                <= slice.len() as i64
        {
            // Budget spent: the next window is measured again, so a bucket that
            // becomes redundant later is not written off.
            self.engaged.store(false, Ordering::Relaxed);
        }
        Some(status)
    }

    /// Engages the switch for the next `span` bytes. Does nothing without
    /// somewhere to forward to, or when it is engaged for good already.
    pub(super) fn engage(&self) {
        if self.output.is_none() || self.forced {
            return;
        }
        self.bytes_left.store(self.span, Ordering::Relaxed);
        self.engaged.store(true, Ordering::Relaxed);
    }

    pub(super) fn into_output(self) -> Option<B> {
        self.output
    }
}

/// The records that went in and came out since the last drain, and the policy
/// that judges them.
pub(super) struct Window {
    policy: BypassPolicy,
    /// Records decoded.
    pub(super) records_in: u64,
    /// Records written, by drains or on their own.
    pub(super) records_out: u64,
}

impl Window {
    pub(super) fn new(policy: BypassPolicy) -> Self {
        Self {
            policy,
            records_in: 0,
            records_out: 0,
        }
    }

    /// Ends the window, starting an empty one, and says whether it folded too
    /// few records to be worth continuing.
    pub(super) fn close(&mut self) -> bool {
        let records_in = std::mem::take(&mut self.records_in);
        let records_out = std::mem::take(&mut self.records_out);
        if records_in < self.policy.min_records {
            // Too short a window to conclude anything from.
            return false;
        }
        // Collapsed below the threshold, stated without dividing: kept records
        // times 100 against the fraction of the input allowed to survive. A
        // threshold above 100 allows none to survive, rather than wrapping.
        let surviving_percent = 100u64.saturating_sub(self.policy.min_collapse_percent);
        records_out * 100 > records_in * surviving_percent
    }
}
