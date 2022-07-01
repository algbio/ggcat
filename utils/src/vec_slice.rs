use std::marker::PhantomData;

#[derive(Clone, Debug)]
pub struct VecSlice<T> {
    pub pos: usize,
    len: usize,
    _phantom: PhantomData<T>,
}

impl<T> VecSlice<T> {
    pub const EMPTY: Self = Self::new(0, 0);

    pub const fn new(pos: usize, len: usize) -> Self {
        Self {
            pos,
            len,
            _phantom: PhantomData,
        }
    }

    pub fn new_extend_iter(ref_vec: &mut Vec<T>, iter: impl Iterator<Item = T>) -> Self {
        let pos = ref_vec.len();
        ref_vec.extend(iter);
        Self {
            pos,
            len: ref_vec.len() - pos,
            _phantom: Default::default(),
        }
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn get_slice<'a>(&self, vec: &'a Vec<T>) -> &'a [T] {
        &vec[self.pos..self.pos + self.len]
    }
    pub fn get_slice_mut<'a>(&self, vec: &'a mut Vec<T>) -> &'a mut [T] {
        &mut vec[self.pos..self.pos + self.len]
    }
}

impl<T: Clone> VecSlice<T> {
    pub fn new_extend(ref_vec: &mut Vec<T>, slice: &[T]) -> Self {
        let pos = ref_vec.len();
        ref_vec.extend_from_slice(slice);
        Self {
            pos,
            len: slice.len(),
            _phantom: Default::default(),
        }
    }
}
