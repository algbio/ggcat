use std::{
    fmt::Debug,
    mem::MaybeUninit,
    slice::{from_raw_parts, from_raw_parts_mut},
};

#[derive(Clone, Copy)]
union AllocatedData<T: Copy, const LOCAL_FITTING: usize> {
    index: usize,
    data: [T; LOCAL_FITTING],
}
impl<T: Copy, const LOCAL_FITTING: usize> AllocatedData<T, LOCAL_FITTING> {
    pub const ZERO: Self = AllocatedData { index: 0 };
}

pub struct Allocator<T, const LOCAL_FITTING: usize> {
    data: Vec<MaybeUninit<T>>,
    freelist: [Vec<usize>; 32],
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
    data: AllocatedData<T, LOCAL_FITTING>,
    size: usize,
}

impl<T: Copy, const LOCAL_FITTING: usize> Debug for InlineVec<T, LOCAL_FITTING> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InlineVec")
            .field("size", &self.size)
            .finish()
    }
}

impl<T: Copy + PartialEq, const LOCAL_FITTING: usize> PartialEq for InlineVec<T, LOCAL_FITTING> {
    fn eq(&self, other: &Self) -> bool {
        self.size == other.size
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
        self.size == 0
    }

    pub fn len(&self) -> usize {
        self.size
    }

    pub unsafe fn set_len(&mut self, size: usize) {
        self.size = size;
    }
}

impl<T: Copy, const LOCAL_FITTING: usize> Default for InlineVec<T, LOCAL_FITTING> {
    fn default() -> Self {
        InlineVec {
            data: AllocatedData::ZERO,
            size: 0,
        }
    }
}

impl<T: Copy, const LOCAL_FITTING: usize> Allocator<T, LOCAL_FITTING> {
    pub const LOCAL_FITTING: usize = LOCAL_FITTING;
    const PTR_FLAG: usize = 0x8000000000000000;

    pub fn new(capacity: usize) -> Self {
        Allocator {
            data: Vec::with_capacity(capacity),
            freelist: (0..32)
                .map(|i| Vec::with_capacity(capacity / 32 / (1 << i)))
                .collect::<Vec<_>>()
                .try_into()
                .unwrap(),
        }
    }

    pub fn reset(&mut self) {
        self.data.clear();
        self.freelist.iter_mut().for_each(|v| v.clear());
    }

    #[inline]
    pub fn new_vec(&mut self, size: usize) -> InlineVec<T, LOCAL_FITTING> {
        if size <= LOCAL_FITTING {
            InlineVec {
                data: AllocatedData::ZERO,
                size,
            }
        } else {
            InlineVec {
                data: self.alloc(size.next_power_of_two()),
                size,
            }
        }
    }

    #[inline]
    pub fn extend_vec(&mut self, vec: &mut InlineVec<T, LOCAL_FITTING>, values: &[T]) {
        let mut ptr = if vec.size + values.len() > LOCAL_FITTING {
            let npt = vec.size.next_power_of_two();
            if vec.size + values.len() > npt {
                let mut old_data = vec.data.clone();

                let new_size = (vec.size + values.len()).next_power_of_two();
                vec.data = self.alloc(new_size);
                unsafe {
                    std::ptr::copy_nonoverlapping(
                        self.get_mut_ptr(&mut old_data),
                        self.get_mut_ptr(&mut vec.data),
                        vec.size,
                    );
                }

                self.free(old_data, npt);
            }
            self.get_mut(&mut vec.data, vec.size) as *mut _
        } else {
            unsafe { vec.data.data.as_mut_ptr().add(vec.size) }
        };

        unsafe {
            for value in values {
                std::ptr::write(ptr, *value);
                ptr = ptr.add(1);
            }
        }
        vec.size += values.len();
    }

    #[inline]
    pub fn push_vec(&mut self, vec: &mut InlineVec<T, LOCAL_FITTING>, value: T) {
        if vec.size < LOCAL_FITTING {
            unsafe {
                let ptr = vec.data.data.as_mut_ptr().add(vec.size);
                std::ptr::write(ptr, value);
            }
        } else {
            let npt = vec.size.next_power_of_two();
            if vec.size >= npt {
                let mut old_data = vec.data.clone();
                vec.data = self.alloc(npt * 2);
                unsafe {
                    std::hint::assert_unchecked(npt >= LOCAL_FITTING);
                    std::ptr::copy_nonoverlapping(
                        self.get_mut_ptr(&mut old_data),
                        self.get_mut_ptr(&mut vec.data),
                        npt,
                    );
                }

                self.free(old_data, npt);
            }
            unsafe {
                std::ptr::write(self.get_mut(&mut vec.data, vec.size) as *mut _, value);
            }
        }
        vec.size += 1;
    }

    #[inline(always)]
    pub fn slice_vec(&self, vec: &InlineVec<T, LOCAL_FITTING>) -> &[T] {
        unsafe { from_raw_parts(self.get_ptr(&vec.data), vec.size) }
    }

    #[inline(always)]
    pub fn slice_vec_mut(&mut self, vec: &mut InlineVec<T, LOCAL_FITTING>) -> &mut [T] {
        unsafe { from_raw_parts_mut(self.get_mut_ptr(&mut vec.data), vec.size) }
    }

    #[inline(always)]
    pub fn iter_vec(&self, vec: &InlineVec<T, LOCAL_FITTING>) -> impl Iterator<Item = &T> {
        self.slice_vec(vec).iter()
    }

    #[inline(always)]
    fn get_ptr(&self, data: &AllocatedData<T, LOCAL_FITTING>) -> *const T {
        if unsafe { data.index } & Self::PTR_FLAG != 0 {
            self.get_ptr_heap(data)
        } else {
            unsafe { data.data.as_ptr() }
        }
    }

    #[inline(always)]
    fn get_mut_ptr(&mut self, data: &mut AllocatedData<T, LOCAL_FITTING>) -> *mut T {
        if unsafe { data.index } & Self::PTR_FLAG != 0 {
            self.get_mut_ptr_heap(data)
        } else {
            unsafe { data.data.as_mut_ptr() }
        }
    }

    #[inline]
    fn get_ptr_heap(&self, data: &AllocatedData<T, LOCAL_FITTING>) -> *const T {
        unsafe {
            self.data
                .as_ptr()
                .add(data.index & !Self::PTR_FLAG)
                .cast::<T>()
        }
    }

    #[inline]
    fn get_mut_ptr_heap(&mut self, data: &mut AllocatedData<T, LOCAL_FITTING>) -> *mut T {
        debug_assert!(unsafe { data.index & !Self::PTR_FLAG } < self.data.len());
        unsafe {
            self.data
                .as_mut_ptr()
                .add(data.index & !Self::PTR_FLAG)
                .cast::<T>()
        }
    }

    #[inline(always)]
    fn get_mut(&mut self, data: &mut AllocatedData<T, LOCAL_FITTING>, index: usize) -> &mut T {
        let ptr = unsafe {
            let ptr = self.get_mut_ptr(data);
            ptr.offset(index as isize).cast::<T>()
        };
        unsafe { &mut *ptr }
    }

    #[inline]
    fn alloc(&mut self, size: usize) -> AllocatedData<T, LOCAL_FITTING> {
        if size <= LOCAL_FITTING {
            return AllocatedData::ZERO;
        }

        unsafe {
            std::hint::assert_unchecked(size > 0);
        }
        let logsize = size.ilog2() as usize;

        if let Some(index) = self
            .freelist
            .get_mut(logsize as usize)
            .map(|f| f.pop())
            .flatten()
        {
            AllocatedData {
                index: index | Self::PTR_FLAG,
            }
        } else {
            let index = self.data.len();
            self.data.reserve(size);
            unsafe {
                self.data.set_len(index + size);
            }
            AllocatedData {
                index: index | Self::PTR_FLAG,
            }
        }
    }

    fn free(&mut self, ptr: AllocatedData<T, LOCAL_FITTING>, size: usize) {
        if size > LOCAL_FITTING {
            unsafe {
                std::hint::assert_unchecked(size > 0);
            }
            let logsize = size.ilog2() as usize;
            self.freelist
                .get_mut(logsize)
                .map(|f| f.push(unsafe { ptr.index } & !Self::PTR_FLAG));
        }
    }

    pub fn copy_from(&mut self, src: &Allocator<T, LOCAL_FITTING>) {
        self.data.clear();
        self.data.reserve(src.data.len());
        self.data.copy_from_slice(&src.data);

        for (dst, src) in self.freelist.iter_mut().zip(src.freelist.iter()) {
            dst.clear();
            dst.copy_from_slice(&src);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::fmt::Debug;

    use super::{Allocator, InlineVec};

    fn test_inlinevec<T: Copy + From<u32> + Eq + Debug, const LOCAL_FITTING: usize>(
        count: usize,
        size: usize,
    ) {
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

    #[test]
    fn test_inlinevec_u32() {
        test_inlinevec::<u32, 2>(10000000, 10000000);
    }

    #[test]
    fn test_inlinevec_u64() {
        test_inlinevec::<u64, 1>(10000000, 10000000);
    }
}
