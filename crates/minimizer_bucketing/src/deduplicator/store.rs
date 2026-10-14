//! The bounded store records are folded into.
//!
//! Three parts, each with its own bound, for a budget M:
//!
//! - the bases storage, M bytes: every distinct record once, in memstorage
//!   form, prefixed with its sub-bucket byte;
//! - an open-addressing table over it, M/4 bytes, keyed by the super-kmer's
//!   hash;
//! - a colour arena, M bytes, holding the colour sets of the stored records.
//!
//! A repeat is folded into the record already stored for it, in place:
//! multiplicity summed, colours merged. When storing or folding a record would
//! pass any bound the store refuses it, changing nothing, and the caller drains
//! the store and offers the record again.

use super::formats::DedupExtraData;
use config::MultiplicityCounterType;
use hashes::HashableSequence;
use io::concurrent::temp_reads::creads_utils::{
    AssemblerMinimizerPosition, DeserializedRead, WithFixedMultiplicity,
};
use io::concurrent::temp_reads::extra_data::{BoundedTempBuffer, TempBuffer};
use io::memstorage::memvarint::compute_memvarint_bytes_count;
use io::memstorage::{memstorage_decode_read, memstorage_encode_read};

/// Spare capacity kept past the bases storage. `memstorage_encode_read` asks for
/// `needed + 8` because the memvarint decoder reads a whole eight byte word past
/// the record it frames.
const STORAGE_SLACK: usize = 32;

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

pub(super) struct FoldStore<MultipleData: DedupExtraData> {
    /// Deduplicated records, each prefixed with its sub-bucket byte.
    storage: Vec<u8>,
    storage_cap: usize,
    table: Box<[Entry]>,
    occupied: usize,
    /// Colours of the stored records.
    arena: TempBuffer<MultipleData>,
    arena_cap: usize,
    /// Set while a drain is in flight, so a panic leaves a flag the next
    /// acquirer trips on rather than a half-drained storage. `parking_lot` does
    /// not poison.
    torn: bool,
}

impl<MultipleData: DedupExtraData> FoldStore<MultipleData> {
    pub(super) fn new(memory: usize) -> Self {
        Self {
            storage: Vec::with_capacity(memory + STORAGE_SLACK),
            storage_cap: memory,
            table: vec![EMPTY; (memory / 4 / size_of::<Entry>()).next_power_of_two()]
                .into_boxed_slice(),
            occupied: 0,
            arena: TempBuffer::<MultipleData>::with_budget(memory),
            arena_cap: memory,
            torn: false,
        }
    }

    /// Whether even an empty store could hold `record`.
    pub(super) fn can_ever_hold(&self, record: &DeserializedRead<MultipleData>) -> bool {
        Self::stored_bytes(record.read.bases_count()) + STORAGE_SLACK <= self.storage_cap
    }

    /// Folds `record` into the record stored for it, or stores it if there is
    /// none. Returns false, having changed nothing, if either would pass a
    /// bound. `in_arena` is the arena the record's colours live in.
    pub(super) fn try_insert(
        &mut self,
        record: &DeserializedRead<MultipleData>,
        in_arena: &mut TempBuffer<MultipleData>,
    ) -> bool {
        assert!(!self.torn, "a previous drain panicked partway through");
        // SAFETY: decoded with NoAlignmentWithOverflow (see `InputFormat`), so
        // sixteen readable bytes follow the packed slice.
        let hash = unsafe { record.read.compute_hash_aligned_overflow16() };
        let tag = ((hash >> 32) as u32).max(1);
        match self.probe(hash, tag, record) {
            Ok(offset) => self.try_fold(offset, record, in_arena),
            Err(index) => self.try_store(index, tag, record, in_arena),
        }
    }

    /// The storage offset of the record stored for `record`, or the empty table
    /// slot it would take.
    fn probe(
        &self,
        hash: u64,
        tag: u32,
        record: &DeserializedRead<MultipleData>,
    ) -> Result<usize, usize> {
        let mask = self.table.len() - 1;
        let mut index = (hash as usize) & mask;
        loop {
            let entry = self.table[index];
            if entry == EMPTY {
                return Err(index);
            }
            if entry.tag == tag && self.matches(entry.offset as usize, record) {
                return Ok(entry.offset as usize);
            }
            index = (index + 1) & mask;
        }
    }

    /// Whether the record stored at `offset` is the same super-kmer, colours
    /// aside. `second_bucket` is part of the key: it is the sub-bucket routing
    /// field, and records bound for different sub-buckets must not be merged.
    fn matches(&self, offset: usize, record: &DeserializedRead<MultipleData>) -> bool {
        if self.storage[offset] != record.second_bucket {
            return false;
        }
        let stored = self.stored_at(offset);
        stored.flags == record.flags
            && stored.read.bases_count() == record.read.bases_count()
            && stored.read.get_packed_slice() == record.read.get_packed_slice()
    }

    /// Folds a repeat into the record stored at `offset`: multiplicity summed,
    /// colours merged, both patched in place. The two mutable fields sit at
    /// known offsets from the start of the memstorage record, which begins after
    /// our sub-bucket byte.
    fn try_fold(
        &mut self,
        offset: usize,
        record: &DeserializedRead<MultipleData>,
        in_arena: &TempBuffer<MultipleData>,
    ) -> bool {
        // A fold can grow the arena without adding a byte of storage or a table
        // slot, so the arena bound is checked here too. A grow reallocates to
        // the next power of two and copies, so twice the resulting run count is
        // the conservative estimate.
        let cost = arena_cost(&record.extra) + arena_cost(&self.stored_at(offset).extra);
        if self.arena.live_bytes() + 2 * cost > self.arena_cap {
            return false;
        }

        // SAFETY: the layout is the one `memstorage_encode_read` wrote — extra,
        // then the minimizer position, then the fixed width multiplicity.
        unsafe {
            let base = self.storage.as_mut_ptr().add(offset + 1);
            let extra_ptr = base as *mut MultipleData;
            let multiplicity_ptr =
                base.add(size_of::<MultipleData>() + 2) as *mut MultiplicityCounterType;
            std::ptr::write_unaligned(
                multiplicity_ptr,
                std::ptr::read_unaligned(multiplicity_ptr).saturating_add(record.multiplicity),
            );
            let mut extra = std::ptr::read_unaligned(extra_ptr);
            extra.combine_entries(&mut self.arena, record.extra, in_arena);
            std::ptr::write_unaligned(extra_ptr, extra);
        }
        true
    }

    /// Stores a record seen for the first time, in the empty table slot `index`.
    fn try_store(
        &mut self,
        index: usize,
        tag: u32,
        record: &DeserializedRead<MultipleData>,
        in_arena: &TempBuffer<MultipleData>,
    ) -> bool {
        let needed = Self::stored_bytes(record.read.bases_count());
        // Every bound is checked before committing, because a drain invalidates
        // every offset in the table and every handle in the arena.
        let over_storage = self.storage.len() + needed + STORAGE_SLACK > self.storage_cap;
        let over_arena = self.arena.live_bytes() + arena_cost(&record.extra) > self.arena_cap;
        let over_table = self.occupied + 1 > self.table.len() * 3 / 4;
        if over_storage || over_arena || over_table {
            return false;
        }

        let offset = self.storage.len();
        self.storage.push(record.second_bucket);
        let extra = MultipleData::copy_extra_from(record.extra, in_arena, &mut self.arena);
        let stored = DeserializedRead {
            read: record.read,
            extra,
            multiplicity: record.multiplicity,
            minimizer_pos: record.minimizer_pos,
            flags: record.flags,
            second_bucket: record.second_bucket,
        };
        let storage = &mut self.storage;
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
        debug_assert_eq!(self.storage.len() - offset, needed);

        self.table[index] = Entry {
            tag,
            offset: offset as u32,
        };
        self.occupied += 1;
        true
    }

    /// Calls `each` with every stored record and the arena its colours live
    /// in, then empties the store.
    ///
    /// The bases storage is walked linearly rather than the table: each stored
    /// record has exactly one entry, so this yields each exactly once, reads
    /// sequentially, and is deterministic given the input order, which probe
    /// order is not.
    pub(super) fn drain(
        &mut self,
        mut each: impl FnMut(&DeserializedRead<MultipleData>, &mut TempBuffer<MultipleData>),
    ) {
        assert!(!self.torn, "a previous drain panicked partway through");
        self.torn = true;
        let mut offset = 0;
        while offset < self.storage.len() {
            let mut record = self.stored_at(offset);
            // memstorage does not serialize the sub-bucket, which is why it is
            // kept in the byte before the record.
            record.second_bucket = self.storage[offset];
            offset += Self::stored_bytes(record.read.bases_count());
            each(&record, &mut self.arena);
        }

        self.storage.clear();
        self.table.fill(EMPTY);
        self.occupied = 0;
        MultipleData::clear_temp_buffer(&mut self.arena);
        self.torn = false;
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

    /// Decodes the record stored at `offset`. The returned read points into
    /// `storage`, which must not be grown or cleared while it is alive.
    fn stored_at(&self, offset: usize) -> DeserializedRead<'static, MultipleData> {
        // SAFETY: `offset` was produced by an encode into this storage, which
        // has not been cleared since; the sub-bucket prefix is one byte.
        unsafe {
            memstorage_decode_read::<
                MultipleData,
                WithFixedMultiplicity,
                AssemblerMinimizerPosition,
                true,
            >(self.storage.as_ptr().add(offset + 1))
            .0
        }
    }
}
