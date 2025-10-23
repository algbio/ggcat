use crate::inline_vec::{Allocator, InlineVec};

pub struct FuzzyHashmap<T: Copy, const LOCAL_FITTING: usize> {
    occupation: Vec<usize>,
    hashmap: Vec<InlineVec<T, LOCAL_FITTING>>,
    allocator: Allocator<T, LOCAL_FITTING>,
    capacity_mask: usize,
}

pub(crate) fn resize_vec_default<T: Copy + Default>(new_capacity: usize, vec: &mut Vec<T>) {
    unsafe {
        if vec.capacity() < new_capacity {
            let previous = std::ptr::read(vec as *const Vec<T>);
            drop(previous);
            let mut new_vec: Vec<T> = Vec::with_capacity(new_capacity);
            let default = T::default();
            for i in 0..new_vec.capacity() {
                std::ptr::write(new_vec.as_mut_ptr().add(i), default);
            }
            std::ptr::write(vec as *mut Vec<T>, new_vec);
        }
        vec.set_len(new_capacity);
    }
}

impl<T: Copy, const LOCAL_FITTING: usize> FuzzyHashmap<T, LOCAL_FITTING> {
    pub fn new(capacity: usize) -> Self {
        let hashmap_capacity = capacity.next_power_of_two();

        let mut hashmap: Vec<InlineVec<T, LOCAL_FITTING>> = vec![];
        resize_vec_default(hashmap_capacity, &mut hashmap);

        Self {
            occupation: Vec::with_capacity(capacity),
            hashmap,
            allocator: Allocator::new(capacity),
            capacity_mask: hashmap_capacity - 1, // hashmap_capacity is a power of 2
        }
    }

    pub fn initialize(&mut self, capacity: usize) {
        assert!(self.occupation.is_empty());
        let capacity = capacity.next_power_of_two();
        resize_vec_default(capacity, &mut self.hashmap);
        self.capacity_mask = capacity - 1;
    }

    pub fn get_elements_mut(&mut self, hash: u64) -> &mut [T] {
        let position = (hash as usize) & self.capacity_mask;
        // Safety: position is in and with capacity_mask
        let element_bucket = unsafe { self.hashmap.get_unchecked_mut(position) };
        self.allocator.slice_vec_mut(element_bucket)
    }

    #[inline]
    pub fn add_element(&mut self, hash: u64, element: T) -> usize {
        let position = (hash as usize) & self.capacity_mask;
        // Safety: position is in and with capacity_mask
        let element_bucket = unsafe { self.hashmap.get_unchecked_mut(position) };

        unsafe {
            // Branchless push
            self.occupation.reserve(1);
            *self.occupation.as_mut_ptr().add(self.occupation.len()) = position;
            self.occupation
                .set_len(self.occupation.len() + (element_bucket.len() == 0) as usize);
        }

        self.allocator.push_vec(element_bucket, element);
        element_bucket.len()
    }

    #[inline(always)]
    pub fn process_elements(&mut self, mut elements_cb: impl FnMut(&mut [T])) {
        for position in self.occupation.drain(..) {
            let elements = unsafe { self.hashmap.get_unchecked_mut(position) };
            if elements.is_poisoned() {
                continue;
            }
            elements_cb(self.allocator.slice_vec_mut(elements));
            *elements = InlineVec::new();
        }

        self.allocator.reset();
    }
}
