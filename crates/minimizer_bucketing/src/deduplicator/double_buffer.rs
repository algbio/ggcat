//! Where appended slices wait to be walked: two buffers of M/2 that take
//! appends from any number of threads without a lock.
//!
//! `active_epoch` is monotone and the active buffer is `active_epoch & 1`; it is
//! monotone rather than a flag so that "this is still the buffer I saw" stays
//! decidable, which is what rules out an ABA on the index. An append takes the
//! buffer's gate for *reading*, carves out a range with one bounded
//! `fetch_update` — the reservation idiom of
//! `AllocatedChunk::write_bytes_noextend` — and copies into it. A reservation
//! that would pass the end is refused rather than taken, so the cursor is at
//! every moment exactly the number of valid bytes, with no burnt space to
//! recover.
//!
//! The thread whose reservation is refused seals: under the deduplicator's
//! state lock, if the epoch is still the one it saw it is the sealer, bumps the
//! epoch so no new appender enters, then takes the gate for *writing*, which
//! waits out the appenders already inside. Only then is the buffer walked. The
//! other buffer stays writable throughout, which is the point of having two.
//!
//! **Lock order is `state` then `gate`, and only the state holder ever takes a
//! gate write lock.** An appender holds a gate and never asks for `state` — it
//! drops the guard before sealing, inside [`Slot::try_append`], so no caller can
//! hold one across a seal. That is what keeps the two locks from closing a
//! cycle.

use parking_lot::RwLock;
use std::cell::UnsafeCell;
use std::sync::atomic::{AtomicUsize, Ordering};

/// Two [`Slot`]s and the epoch that says which one takes appends.
pub(super) struct DoubleBuffer {
    slots: [Slot; 2],
    /// Monotone; the active slot is `active_epoch & 1`.
    active_epoch: AtomicUsize,
}

impl DoubleBuffer {
    pub(super) fn new(slot_capacity: usize) -> Self {
        Self {
            slots: [Slot::new(slot_capacity), Slot::new(slot_capacity)],
            active_epoch: AtomicUsize::new(0),
        }
    }

    /// The largest slice an append can take.
    pub(super) fn slot_capacity(&self) -> usize {
        self.slots[0].data.len()
    }

    /// Copies `slice` into the active buffer, or hands back the epoch it found
    /// full, which is what the caller then passes to [`Self::seal`].
    #[inline]
    pub(super) fn try_append(&self, slice: &[u8]) -> Result<(), usize> {
        let epoch = self.active_epoch.load(Ordering::Acquire);
        if self.slots[epoch & 1].try_append(epoch, &self.active_epoch, slice) {
            Ok(())
        } else {
            Err(epoch)
        }
    }

    /// Retires the buffer that was active at `observed_epoch` and passes its
    /// bytes to `walk`, unless another thread already has.
    ///
    /// The caller must hold the deduplicator's state lock: that is what makes
    /// the check and the bump one step, and what keeps the epoch from running
    /// more than one ahead of the walking.
    pub(super) fn seal(&self, observed_epoch: usize, walk: impl FnOnce(&[u8])) {
        if self.active_epoch.load(Ordering::Relaxed) != observed_epoch {
            // Somebody sealed it already; the caller retries against whatever is
            // active now.
            return;
        }
        // Past this point no new appender enters the old slot.
        self.active_epoch
            .store(observed_epoch + 1, Ordering::Release);
        self.empty_slot(observed_epoch, walk);
    }

    /// Passes the bytes of both buffers to `walk`, the inactive one first so
    /// records keep their relative order.
    ///
    /// Every appender must have been joined first: this makes no attempt to
    /// exclude one.
    pub(super) fn seal_both(&self, mut walk: impl FnMut(&[u8])) {
        let epoch = self.active_epoch.load(Ordering::Relaxed);
        self.empty_slot(epoch + 1, &mut walk);
        self.empty_slot(epoch, &mut walk);
    }

    /// Walks and empties the slot of `epoch`, which no appender may enter any
    /// more: either the epoch has moved past it, or every appender is joined.
    fn empty_slot(&self, epoch: usize, walk: impl FnOnce(&[u8])) {
        let slot = &self.slots[epoch & 1];
        // Waits out every appender that entered before the bump, which is what
        // makes the `len` read below final and the bytes stable.
        let gate = slot.gate.write();
        let len = slot.len.load(Ordering::Relaxed);
        if len > 0 {
            // SAFETY: the write guard excludes every appender, and the `len`
            // bytes were written by appends that have all completed.
            let data = unsafe { std::slice::from_raw_parts(slot.data.as_ptr().cast::<u8>(), len) };
            walk(data);
        }
        slot.len.store(0, Ordering::Relaxed);
        drop(gate);
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
