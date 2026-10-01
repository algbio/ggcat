//! Counters of the LZ copyback extension ([`crate::fasta_copyback`]). Every field is a
//! [`Stat`], so the whole struct is zero-sized without the `stats` feature of
//! `ggcat-logging`.

use ggcat_logging::Stat;

/// What the forward half of the extension found, and what the backward half did.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct CopybackStats {
    /// Bytes of the tracked FASTA spans; of them, lexed and skipped without a look.
    pub bytes_seen: Stat,
    pub bytes_lexed: Stat,
    pub bytes_skipped: Stat,
    /// Bytes between the FASTA streams of a tracked input.
    pub opaque_bytes: Stat,
    /// Bases of the tracked spans, lexed or skipped: the absolute coordinates handed out.
    pub bases_seen: Stat,
    pub fwd_copies: Stat,
    pub fwd_bytes: Stat,
    /// Bases inside the forward copies' sources, and the copies that held none.
    pub fwd_bases: Stat,
    pub fwd_no_bases: Stat,
    pub back_copies: Stat,
    pub back_bytes: Stat,
    /// Backward copies skipped, as a hole.
    pub back_skipped: Stat,
    /// Backward copies whose destination the lexer had already passed: lexed, or
    /// skipped for an earlier copy.
    pub back_passed: Stat,
    /// Backward copies with too few bases to drop any beyond the `2k - m` kept at each end.
    pub back_too_short: Stat,
    /// Backward copies whose first line parses differently at the destination (a
    /// header at one end, sequence at the other).
    pub back_state_mismatch: Stat,
    /// Backward copies whose source was already forgotten; never, while the retained
    /// history covers the input's LZ distance.
    pub back_unknown_source: Stat,
    /// Skips cut short by an opaque stretch at the source; the rest was planned again.
    pub back_opaque_cut: Stat,
    /// Holes made, and their bases.
    pub holes: Stat,
    pub hole_bases: Stat,
    /// Tags remembered at the peak: runs of bases, and marks.
    pub runs_peak: Stat,
    pub marks_peak: Stat,
}

impl CopybackStats {
    /// Folds another reader thread's counters in. Destructured exhaustively so that a
    /// new counter cannot be forgotten here.
    pub fn merge(&mut self, other: &CopybackStats) {
        let CopybackStats {
            bytes_seen,
            bytes_lexed,
            bytes_skipped,
            opaque_bytes,
            bases_seen,
            fwd_copies,
            fwd_bytes,
            fwd_bases,
            fwd_no_bases,
            back_copies,
            back_bytes,
            back_skipped,
            back_passed,
            back_too_short,
            back_state_mismatch,
            back_unknown_source,
            back_opaque_cut,
            holes,
            hole_bases,
            runs_peak,
            marks_peak,
        } = *other;
        self.bytes_seen += bytes_seen.get();
        self.bytes_lexed += bytes_lexed.get();
        self.bytes_skipped += bytes_skipped.get();
        self.opaque_bytes += opaque_bytes.get();
        self.bases_seen += bases_seen.get();
        self.fwd_copies += fwd_copies.get();
        self.fwd_bytes += fwd_bytes.get();
        self.fwd_bases += fwd_bases.get();
        self.fwd_no_bases += fwd_no_bases.get();
        self.back_copies += back_copies.get();
        self.back_bytes += back_bytes.get();
        self.back_skipped += back_skipped.get();
        self.back_passed += back_passed.get();
        self.back_too_short += back_too_short.get();
        self.back_state_mismatch += back_state_mismatch.get();
        self.back_unknown_source += back_unknown_source.get();
        self.back_opaque_cut += back_opaque_cut.get();
        self.holes += holes.get();
        self.hole_bases += hole_bases.get();
        // A peak, not a sum.
        self.runs_peak.max_with(runs_peak.get());
        // A peak, not a sum.
        self.marks_peak.max_with(marks_peak.get());
    }

    /// One line per group, for the log.
    pub fn report(&self) -> String {
        let pct = |part: Stat, whole: Stat| {
            let (part, whole) = (part.get(), whole.get());
            if whole == 0 {
                0.0
            } else {
                100.0 * part as f64 / whole as f64
            }
        };
        format!(
            "lz copyback: {} bytes seen ({} opaque), {} lexed, {} skipped ({:.1}%), {} bases\n\
             lz copyback forward: {} copies / {} bytes, {} bases in their sources, \
             {} with no base\n\
             lz copyback backward: {} copies / {} bytes, {} skipped; not skipped: {} passed, \
             {} too short, {} first line differs, {} forgotten source, {} cut at an opaque stretch\n\
             lz copyback holes: {} holes / {} bases ({:.1}% of the bases)\n\
             lz copyback tags: {} runs and {} marks at the peak",
            self.bytes_seen,
            self.opaque_bytes,
            self.bytes_lexed,
            self.bytes_skipped,
            pct(self.bytes_skipped, self.bytes_seen),
            self.bases_seen,
            self.fwd_copies,
            self.fwd_bytes,
            self.fwd_bases,
            self.fwd_no_bases,
            self.back_copies,
            self.back_bytes,
            self.back_skipped,
            self.back_passed,
            self.back_too_short,
            self.back_state_mismatch,
            self.back_unknown_source,
            self.back_opaque_cut,
            self.holes,
            self.hole_bases,
            pct(self.hole_bases, self.bases_seen),
            self.runs_peak,
            self.marks_peak,
        )
    }
}
