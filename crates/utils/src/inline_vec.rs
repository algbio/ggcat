use std::{
    fmt::Debug,
    mem::MaybeUninit,
    slice::{from_raw_parts, from_raw_parts_mut},
};

use crate::resize_containers::ResizableVec;

#[derive(Clone, Copy)]
pub union AllocatedData<T: Copy, const LOCAL_FITTING: usize> {
    pub index: usize,
    data: [T; LOCAL_FITTING],
}
impl<T: Copy, const LOCAL_FITTING: usize> AllocatedData<T, LOCAL_FITTING> {
    pub const ZERO: Self = AllocatedData { index: 0 };
}

const DEFAULT_ALLOCATOR_SIZE: usize = 1024 * 1024 * 8;

/// One freelist per power-of-two block size. Every block a vector can own has a
/// class here, so [`Allocator::free_block`] never drops one on the floor.
const SIZE_CLASSES: usize = usize::BITS as usize;

/// [`InlineVec::size`] packs the length with the size class of the heap block
/// the vector owns. Storing the class is what lets a block be returned to the
/// list it came from even after `set_len` has shrunk the length below it, and
/// it keeps the whole union free for payload: nothing is stolen from a value's
/// high bit any more, so an element may use all of its bits.
const CLASS_BITS: u32 = 7;
const LENGTH_BITS: u32 = usize::BITS - CLASS_BITS;
const LENGTH_MASK: usize = (1usize << LENGTH_BITS) - 1;

pub struct Allocator<T, const LOCAL_FITTING: usize> {
    data: ResizableVec<MaybeUninit<T>, DEFAULT_ALLOCATOR_SIZE>,
    freelist: [Vec<usize>; SIZE_CLASSES],
}

impl<T: Copy, const LOCAL_FITTING: usize> Default for Allocator<T, LOCAL_FITTING> {
    fn default() -> Self {
        Self::new(0)
    }
}

pub type AllocatorU32 = Allocator<u32, 2>;
pub type AllocatorU64 = Allocator<u64, 1>;

#[derive(Copy, Clone)]
pub struct InlineVec<T: Copy, const LOCAL_FITTING: usize> {
    pub data: AllocatedData<T, LOCAL_FITTING>,
    size: usize,
}

impl<T: Copy + Debug, const LOCAL_FITTING: usize> Debug for InlineVec<T, LOCAL_FITTING> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut debug_struct = f.debug_struct("InlineVec");
        debug_struct.field("size", &self.len());
        if !self.is_heap() {
            let data = unsafe { &self.data.data[0..self.len()] };
            debug_struct.field("inline_data", &data);
        }

        debug_struct.finish()
    }
}

impl<T: Copy + PartialEq, const LOCAL_FITTING: usize> PartialEq for InlineVec<T, LOCAL_FITTING> {
    fn eq(&self, other: &Self) -> bool {
        self.len() == other.len()
    }
}

impl<T: Copy, const LOCAL_FITTING: usize> InlineVec<T, LOCAL_FITTING> {
    pub const fn new() -> Self {
        InlineVec {
            data: AllocatedData::ZERO,
            size: 0,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    #[inline(always)]
    pub fn len(&self) -> usize {
        self.size & LENGTH_MASK
    }

    /// Sets the length, leaving the size class of the owned block alone.
    ///
    /// # Safety
    /// `size` must not exceed the capacity the vector was last grown to; the
    /// elements below it must be initialized.
    #[inline(always)]
    pub unsafe fn set_len(&mut self, size: usize) {
        debug_assert!(size <= LENGTH_MASK);
        self.size = (self.size & !LENGTH_MASK) | size;
    }

    /// Zero while the elements live in the union, otherwise one more than the
    /// base-two logarithm of the owned block's element count.
    #[inline(always)]
    fn size_class(&self) -> usize {
        self.size >> LENGTH_BITS
    }

    #[inline(always)]
    fn is_heap(&self) -> bool {
        self.size_class() != 0
    }

    #[inline(always)]
    fn set_size_class(&mut self, class: usize) {
        debug_assert!(class < (1 << CLASS_BITS));
        self.size = (self.size & LENGTH_MASK) | (class << LENGTH_BITS);
    }

    /// How many elements fit before the vector has to move to a larger block.
    /// Public so a caller can compact its own contents on the growth boundary
    /// rather than after the slab has already doubled.
    #[inline(always)]
    pub fn capacity(&self) -> usize {
        let class = self.size_class();
        if class == 0 {
            LOCAL_FITTING
        } else {
            1usize << (class - 1)
        }
    }

    /// A real vector never reaches this: a size class is at most
    /// `usize::BITS`, well below the all-ones pattern.
    pub fn is_poisoned(&self) -> bool {
        self.size == usize::MAX
    }

    pub fn poison(&mut self) {
        self.size = usize::MAX;
    }
}

impl<T: Copy, const LOCAL_FITTING: usize> Default for InlineVec<T, LOCAL_FITTING> {
    fn default() -> Self {
        InlineVec::new()
    }
}

impl<T: Copy, const LOCAL_FITTING: usize> Allocator<T, LOCAL_FITTING> {
    pub const LOCAL_FITTING: usize = LOCAL_FITTING;

    const SUPPORTS_LOCAL: bool = LOCAL_FITTING > 0;

    pub fn new(capacity: usize) -> Self {
        Self::build(ResizableVec::new(), capacity)
    }

    /// Starts the slab at `elements` rather than at [`DEFAULT_ALLOCATOR_SIZE`],
    /// for a caller that has a memory budget rather than a throughput target.
    ///
    /// [`Self::new`]'s argument only sizes the freelists -- the slab it makes is
    /// always the default, which is 8Mi elements however small the argument. It
    /// stays that way because its callers pass byte counts into what is an
    /// element parameter, so honouring it there would shrink every arena in the
    /// pipeline. This constructor is the opt-in.
    ///
    /// The slab still grows on demand; what this sets is where it starts, and
    /// [`Self::reset`] keeps it there, since the reinit ceiling only applies
    /// above the default size.
    pub fn with_slab_capacity(elements: usize) -> Self {
        Self::build(ResizableVec::with_capacity(elements), elements)
    }

    fn build(data: ResizableVec<MaybeUninit<T>, DEFAULT_ALLOCATOR_SIZE>, capacity: usize) -> Self {
        Allocator {
            data,
            freelist: (0..SIZE_CLASSES)
                .map(|i| Vec::with_capacity(capacity / SIZE_CLASSES / (1 << i.min(31))))
                .collect::<Vec<_>>()
                .try_into()
                .unwrap(),
        }
    }

    pub fn reset(&mut self) {
        self.data.clear();
        self.freelist.iter_mut().for_each(|v| v.clear());
    }

    pub fn used_capacity(&self) -> usize {
        self.data.len()
    }

    pub fn capacity(&self) -> usize {
        self.data.capacity()
    }

    pub fn get_heap_ptr(&self) -> *const T {
        self.data.as_ptr() as *const T
    }

    pub fn reserve_additional(&mut self, additional: usize) {
        self.data.reserve(additional);
    }

    #[inline]
    pub fn new_vec(&mut self, size: usize) -> InlineVec<T, LOCAL_FITTING> {
        let mut vec = InlineVec::new();
        if size > LOCAL_FITTING {
            let (index, class) = self.alloc_block(size.next_power_of_two());
            vec.data = AllocatedData { index };
            vec.set_size_class(class);
        }
        unsafe { vec.set_len(size) };
        vec
    }

    #[inline]
    pub fn reserve_vec(&mut self, vec: &mut InlineVec<T, LOCAL_FITTING>, count: usize) -> *mut T {
        let len = vec.len();
        let new_len = len + count;

        if new_len > vec.capacity() {
            self.grow(vec, new_len);
        }

        unsafe {
            vec.set_len(new_len);
            self.vec_ptr_mut(vec).add(len)
        }
    }

    /// Moves a vector into a block large enough for `needed` elements. Always
    /// allocates: the caller has already found the current capacity too small.
    #[cold]
    fn grow(&mut self, vec: &mut InlineVec<T, LOCAL_FITTING>, needed: usize) {
        let len = vec.len();
        let was_heap = vec.is_heap();
        let old_class = vec.size_class();
        let old_data = vec.data;

        // Allocating can move the slab, so no pointer into it is taken before.
        let (index, class) = self.alloc_block(needed.next_power_of_two());
        unsafe {
            let destination = self.heap_ptr_mut(index);
            let source = if was_heap {
                self.heap_ptr(old_data.index)
            } else {
                old_data.data.as_ptr()
            };
            std::ptr::copy_nonoverlapping(source, destination, len);
        }
        if was_heap {
            self.free_block(unsafe { old_data.index }, old_class);
        }

        vec.data = AllocatedData { index };
        vec.set_size_class(class);
    }

    /// Appends a slice. `values` must not borrow from this allocator: growing
    /// the vector can move the slab out from under it.
    #[inline]
    pub fn extend_vec(&mut self, vec: &mut InlineVec<T, LOCAL_FITTING>, values: &[T]) {
        let ptr = self.reserve_vec(vec, values.len());
        unsafe { std::ptr::copy_nonoverlapping(values.as_ptr(), ptr, values.len()) };
    }

    #[inline]
    pub fn push_vec(&mut self, vec: &mut InlineVec<T, LOCAL_FITTING>, value: T) {
        let ptr = self.reserve_vec(vec, 1);
        unsafe { std::ptr::write(ptr, value) };
    }

    pub fn free_vec(&mut self, vec: &mut InlineVec<T, LOCAL_FITTING>) {
        if vec.is_heap() {
            self.free_block(unsafe { vec.data.index }, vec.size_class());
        }
        *vec = InlineVec::new();
    }

    #[inline(always)]
    pub fn slice_vec<'a>(&'a self, vec: &'a InlineVec<T, LOCAL_FITTING>) -> &'a [T] {
        unsafe { from_raw_parts(self.vec_ptr(vec), vec.len()) }
    }

    /// # Safety
    /// The returned slice is only valid while both the allocator and `vec` are
    /// alive and neither is mutated.
    #[inline(always)]
    pub unsafe fn slice_vec_static(&self, vec: &InlineVec<T, LOCAL_FITTING>) -> &'static [T] {
        unsafe { from_raw_parts(self.vec_ptr(vec), vec.len()) }
    }

    #[inline(always)]
    pub fn slice_vec_mut<'a>(
        &'a mut self,
        vec: &'a mut InlineVec<T, LOCAL_FITTING>,
    ) -> &'a mut [T] {
        let len = vec.len();
        unsafe { from_raw_parts_mut(self.vec_ptr_mut(vec), len) }
    }

    #[inline(always)]
    pub fn iter_vec<'a>(
        &'a self,
        vec: &'a InlineVec<T, LOCAL_FITTING>,
    ) -> impl Iterator<Item = &'a T> {
        self.slice_vec(vec).iter()
    }

    /// The elements of a vector, wherever they live. An inline vector's pointer
    /// is into `vec` itself, which is why both borrows share a lifetime above.
    #[inline(always)]
    fn vec_ptr(&self, vec: &InlineVec<T, LOCAL_FITTING>) -> *const T {
        if Self::SUPPORTS_LOCAL && !vec.is_heap() {
            unsafe { vec.data.data.as_ptr() }
        } else {
            self.heap_ptr(unsafe { vec.data.index })
        }
    }

    #[inline(always)]
    fn vec_ptr_mut(&mut self, vec: &mut InlineVec<T, LOCAL_FITTING>) -> *mut T {
        if Self::SUPPORTS_LOCAL && !vec.is_heap() {
            unsafe { vec.data.data.as_mut_ptr() }
        } else {
            let index = unsafe { vec.data.index };
            self.heap_ptr_mut(index)
        }
    }

    #[inline(always)]
    fn heap_ptr(&self, index: usize) -> *const T {
        debug_assert!(index <= self.data.len());
        unsafe { self.data.as_ptr().add(index).cast::<T>() }
    }

    #[inline(always)]
    fn heap_ptr_mut(&mut self, index: usize) -> *mut T {
        debug_assert!(index <= self.data.len());
        unsafe { self.data.as_mut_ptr().add(index).cast::<T>() }
    }

    /// Reserves a block of exactly `capacity` elements, returning its slab index
    /// and the size class it must later be freed under.
    #[inline]
    fn alloc_block(&mut self, capacity: usize) -> (usize, usize) {
        debug_assert!(capacity.is_power_of_two());
        let logsize = capacity.trailing_zeros() as usize;

        let index = match self.freelist[logsize].pop() {
            Some(index) => index,
            None => {
                let index = self.data.len();
                self.data.reserve(capacity);
                unsafe {
                    self.data.set_len(index + capacity);
                }
                index
            }
        };
        (index, logsize + 1)
    }

    /// Returns a block to the list it was taken from. The class travels with the
    /// vector rather than being recomputed from its length, so a block whose
    /// vector was shrunk still lands in the right list instead of being filed
    /// under a smaller one and leaking the difference.
    #[inline]
    fn free_block(&mut self, index: usize, class: usize) {
        debug_assert!(class > 0);
        self.freelist[class - 1].push(index);
    }

    pub fn copy_from(&mut self, src: &Allocator<T, LOCAL_FITTING>) {
        self.data.clear();
        self.data.reserve(src.data.len());
        self.data.extend_from_slice(&src.data);

        for (dst, src) in self.freelist.iter_mut().zip(src.freelist.iter()) {
            dst.clear();
            dst.extend_from_slice(src);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::fmt::Debug;
    use std::sync::{Mutex, MutexGuard};

    use super::{Allocator, InlineVec};

    #[derive(Eq, PartialEq, Debug, Copy, Clone)]
    struct Huge([u32; 32]);

    impl From<u32> for Huge {
        fn from(value: u32) -> Self {
            Self([value; 32])
        }
    }

    fn test_inlinevec<T: Copy + From<u32> + Eq + Debug, const LOCAL_FITTING: usize>(
        count: usize,
        size: usize,
    ) {
        let _guard = exclusive();
        let start = std::time::Instant::now();

        let mut allocator = Allocator::<T, LOCAL_FITTING>::new(size);

        let mut vecs = (0..count)
            .map(|_| InlineVec::<T, { LOCAL_FITTING }>::default())
            .collect::<Vec<_>>();

        let mut reference = vec![vec![]; count];

        let mut filled = 0;

        for i in 0..size {
            let target: u64 = rand::random::<u64>() % count as u64;
            let vec_target = &mut vecs[filled.min(target) as usize];
            if true {
                allocator.push_vec(vec_target, T::from(i as u32));
                allocator.extend_vec(vec_target, &[T::from(i as u32 + 1), T::from(i as u32)]);
            }
            if true {
                reference[filled.min(target) as usize].push(T::from(i as u32));
                reference[filled.min(target) as usize].push(T::from(i as u32 + 1));
                reference[filled.min(target) as usize].push(T::from(i as u32));
            }
            filled += (filled < target) as u64;
        }

        for (i, (v, r)) in vecs.iter().zip(reference.iter()).enumerate() {
            assert_eq!(v.len(), r.len());

            let mut count = 0;
            for (i, j) in allocator.iter_vec(v).zip(r.iter()) {
                assert_eq!(*i, *j);
                count += 1;
            }
            assert_eq!(
                count,
                v.len(),
                "Mismatch in vector {}: expected {}, got {}",
                i,
                v.len(),
                count
            );
        }

        println!("Benchmark duration: {:?}", start.elapsed());
        println!(
            "Final size: {} vs expected {} filled: {}",
            allocator.data.len(),
            size,
            filled
        );
    }

    /// The cases below allocate a few hundred megabytes each. Cargo runs tests
    /// in parallel, so without this lock their peaks add up: a full-size run of
    /// the whole module once drove the user session into `systemd-oomd`, which
    /// kills the largest cgroup rather than the allocating process. Holding it
    /// for the body of each case keeps the peak at one case's worth.
    static EXCLUSIVE: Mutex<()> = Mutex::new(());

    fn exclusive() -> MutexGuard<'static, ()> {
        // A panicking case must not disable the others.
        EXCLUSIVE
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// Two inline slots, the width `AllocatorU32` uses.
    #[test]
    fn test_inlinevec_u32() {
        test_inlinevec::<u32, 2>(1000000, 1000000);
    }

    /// One inline slot with a four-byte element: what the color arenas use, and
    /// the width where an element shares the union with the heap flag.
    #[test]
    fn test_inlinevec_u32_single_slot() {
        test_inlinevec::<u32, 1>(1000000, 1000000);
    }

    #[test]
    fn test_inlinevec_u64() {
        test_inlinevec::<u64, 1>(1000000, 1000000);
    }

    /// No inline slot, and an element far wider than a pointer, so every vector
    /// is slab-backed. Sized down further because `Huge` is 128 bytes.
    #[test]
    fn test_inlinevec_huge() {
        test_inlinevec::<Huge, 0>(100000, 100000);
    }

    /// Repeatedly regrowing one vector must recycle the slab through the
    /// freelist instead of leaving the freed blocks stranded.
    #[test]
    fn test_multiple_realloc() {
        let _guard = exclusive();
        let mut allocator = Allocator::<u32, 2>::new(1024 * 1024 * 64);

        let mut vec = allocator.new_vec(193);
        for i in 0..1000000 {
            allocator.extend_vec(&mut vec, &[i, i + 1]);
        }

        // Growth doubles and the old block is freed, so the slab holds at most
        // the geometric sum of the blocks allocated along the way.
        assert!(
            allocator.used_capacity() <= 4 * vec.len(),
            "Slab grew to {} for a vector of {} elements",
            allocator.used_capacity(),
            vec.len()
        );
    }

    /// Every bit of an element is payload. The allocator used to keep its heap
    /// marker in the top bit of the union, so a single inline value with its
    /// high bit set was read back as a slab index.
    #[test]
    fn inline_values_may_use_every_bit() {
        let mut allocator = Allocator::<u32, 1>::new(16);
        for value in [u32::MAX, 1 << 31, (1 << 31) | 7] {
            let mut vec = allocator.new_vec(0);
            allocator.push_vec(&mut vec, value);
            assert_eq!(allocator.slice_vec(&vec), &[value]);
            allocator.free_vec(&mut vec);
        }

        let mut allocator = Allocator::<u64, 1>::new(16);
        let mut vec = allocator.new_vec(0);
        allocator.push_vec(&mut vec, u64::MAX);
        assert_eq!(allocator.slice_vec(&vec), &[u64::MAX]);
    }

    /// A vector shrunk with `set_len` still owns the block it was grown to, so
    /// freeing it has to return that block and not the smaller one its new
    /// length suggests. Recycling the freed block is what proves it went back to
    /// the right list.
    #[test]
    fn a_shrunk_vector_frees_the_block_it_owns() {
        let mut allocator = Allocator::<u32, 1>::new(64);

        let mut vec = allocator.new_vec(0);
        allocator.extend_vec(&mut vec, &(0..64u32).collect::<Vec<_>>());
        let after_growth = allocator.used_capacity();

        unsafe { vec.set_len(1) };
        allocator.free_vec(&mut vec);

        // The 64-element block is back on its own list, so asking for another
        // one of the same size must reuse it rather than extend the slab.
        let mut reused = allocator.new_vec(64);
        allocator.slice_vec_mut(&mut reused).fill(7);
        assert_eq!(allocator.used_capacity(), after_growth);
    }

    /// Shrinking and regrowing repeatedly must not strand blocks either: every
    /// cycle hands its block back under the class it was taken from.
    #[test]
    fn shrink_and_regrow_recycles_the_slab() {
        let mut allocator = Allocator::<u64, 1>::new(64);

        let mut vec = allocator.new_vec(0);
        allocator.extend_vec(&mut vec, &(0..128u64).collect::<Vec<_>>());
        let after_first_growth = allocator.used_capacity();

        for _ in 0..1000 {
            unsafe { vec.set_len(1) };
            allocator.extend_vec(&mut vec, &(0..127u64).collect::<Vec<_>>());
        }

        assert_eq!(
            allocator.used_capacity(),
            after_first_growth,
            "regrowing into the same size class must reuse the same block"
        );
    }
}
