//! The two wire formats a deduplicator sits between.
//!
//! Records come in *narrow*, one colour and no multiplicity, which is what the
//! bucketing threads would have written to the bucket anyway, and go out
//! *wide*, carrying the combined colour set and the multiplicity it stands for,
//! which is what the compactor reads.

use io::concurrent::temp_reads::creads_utils::{
    AssemblerMinimizerPosition, CompressedReadsBucketDataSerializer, NoAlignmentWithOverflow,
    NoMultiplicity, WithMultiplicity, WithSecondBucket,
};
use io::concurrent::temp_reads::extra_data::SequenceExtraDataCombiner;

/// Everything the deduplicator needs of its extra data type.
pub trait DedupExtraData: SequenceExtraDataCombiner + Sync + Send + Copy + 'static {}

impl<T> DedupExtraData for T where T: SequenceExtraDataCombiner + Sync + Send + Copy + 'static {}

/// The single-entry extra data the records arrive carrying.
pub(super) type SingleOf<MultipleData> =
    <MultipleData as SequenceExtraDataCombiner>::SingleDataType;

/// What the bucketing threads hand over: one colour and no multiplicity, which
/// is the narrow form the non-deduplicating path writes anyway.
///
/// Both sides reset their format per record, because a single colour is written
/// as a delta against the previous one and a sealed buffer holds runs from
/// several threads' buffers back to back. A reset is one store, and it makes a
/// record decodable wherever it ends up.
///
/// `NoAlignmentWithOverflow` differs from `NoAlignment` only in reserving
/// `HASH_MAX_OVERREAD` bytes of slack past a decoded read, which is what makes
/// `compute_hash_aligned_overflow16` — which reads sixteen bytes at a time and
/// so runs past the packed slice — stay inside the allocation. The wire format
/// is identical.
pub(super) type InputFormat<MultipleData, FlagsCount> = CompressedReadsBucketDataSerializer<
    SingleOf<MultipleData>,
    WithSecondBucket,
    NoMultiplicity,
    AssemblerMinimizerPosition,
    FlagsCount,
    NoAlignmentWithOverflow,
>;

/// What a drain writes: the combined colour set and the multiplicity it stands
/// for, which is what the compactor reads. The write path ignores the alignment
/// mode entirely.
pub(super) type OutputFormat<MultipleData, FlagsCount> = CompressedReadsBucketDataSerializer<
    MultipleData,
    WithSecondBucket,
    WithMultiplicity,
    AssemblerMinimizerPosition,
    FlagsCount,
    NoAlignmentWithOverflow,
>;
