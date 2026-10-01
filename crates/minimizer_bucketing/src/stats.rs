//! Counters of the bucketing phase: the deduplicator's, and the LZ copyback
//! extension's totals over every reader thread. The super-k-mers range manager keeps
//! its own in [`crate::skm`].
//!
//! Every counter is a [`Stat`], compiled out without the `stats` feature of
//! `ggcat-logging`; the log lines that print them are then skipped as well.

use ggcat_logging::Stat;
use parking_lot::Mutex;
use simd_accel::stats::CopybackStats;

#[derive(Default, Clone, Copy, Debug, PartialEq, Eq)]
pub struct DedupStats {
    /// Records handed to `add`.
    pub records_in: Stat,
    /// Records written to the output bucket.
    pub records_out: Stat,
    pub bytes_out: Stat,
    /// Drains, and which bound triggered each.
    pub drains: Stat,
    pub drains_storage: Stat,
    pub drains_arena: Stat,
    pub drains_table: Stat,
    /// Buffers sealed, and slices that bypassed the buffers.
    pub seals: Stat,
    pub inline_slices: Stat,
    /// Records too large to ever be stored, written straight out.
    pub oversized_records: Stat,
    /// Bytes forwarded untouched while standing down, and how many windows
    /// decided to stand down. Both zero on input the deduplicator is earning
    /// its keep on, which is what makes a silent regression visible.
    pub bypassed_bytes: Stat,
    pub bypass_windows: Stat,
    /// Bytes of wide records written straight to the output, never deduplicated
    /// (the super-k-mers the LZ copyback manager lists, with their multiplicity).
    pub direct_bytes: Stat,
    /// Peak live bytes of each bounded structure, for the budget assertions.
    pub peak_storage: Stat,
    pub peak_arena: Stat,
    pub table_bytes: Stat,
}

impl DedupStats {
    pub fn new(table_bytes: u64) -> Self {
        let mut stats = Self::default();
        stats.table_bytes.set(table_bytes);
        stats
    }

    /// Folds another bucket's figures in, for a summary over the whole array.
    pub fn accumulate(&mut self, other: &Self) {
        self.records_in += other.records_in.get();
        self.records_out += other.records_out.get();
        self.bytes_out += other.bytes_out.get();
        self.drains += other.drains.get();
        self.drains_storage += other.drains_storage.get();
        self.drains_arena += other.drains_arena.get();
        self.drains_table += other.drains_table.get();
        self.seals += other.seals.get();
        self.inline_slices += other.inline_slices.get();
        self.oversized_records += other.oversized_records.get();
        self.bypassed_bytes += other.bypassed_bytes.get();
        self.bypass_windows += other.bypass_windows.get();
        self.direct_bytes += other.direct_bytes.get();
        self.peak_storage.max_with(other.peak_storage.get());
        self.peak_arena.max_with(other.peak_arena.get());
        self.table_bytes.max_with(other.table_bytes.get());
    }

    /// One line per figure, for a test or a benchmark to print.
    pub fn report(&self) -> String {
        let (records_in, records_out) = (self.records_in.get(), self.records_out.get());
        format!(
            "records {} -> {} ({:.1}% collapsed), {} bytes out, {} drains \
             (storage {}, arena {}, table {}), {} seals, {} inline slices, \
             {} oversized, {} bytes bypassed over {} windows, {} bytes direct, \
             peak storage {}, arena {}, table {}",
            records_in,
            records_out,
            if records_in == 0 {
                0.0
            } else {
                100.0 * records_in.saturating_sub(records_out) as f64 / records_in as f64
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
            self.direct_bytes,
            self.peak_storage,
            self.peak_arena,
            self.table_bytes,
        )
    }
}

/// Totals of the LZ copyback extension over every reader thread.
static COPYBACK_TOTALS: Mutex<Option<CopybackStats>> = Mutex::new(None);

/// Folds one reader thread's counters into the totals.
pub fn accumulate_copyback_stats(stats: CopybackStats) {
    if !Stat::ENABLED {
        return;
    }
    let mut totals = COPYBACK_TOTALS.lock();
    totals.get_or_insert_with(CopybackStats::default).merge(&stats);
}

/// Logs and clears the totals; does nothing when the extension is off.
pub fn log_copyback_stats() {
    if let Some(totals) = COPYBACK_TOTALS.lock().take() {
        ggcat_logging::info!("{}", totals.report());
    }
}
