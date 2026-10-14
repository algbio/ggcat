//! Where a deduplicator writes: the wide bucket its drains fill, and the narrow
//! one a bypassing bucket forwards to.

use arc_swap::ArcSwapOption;
use parallel_processor::buckets::{ChunkingStatus, LockFreeBucket, MultiThreadBuckets};
use parking_lot::Mutex;
use std::sync::Arc;

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
