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

    pub fn iter(&self) -> impl Iterator<Item = usize> + use<T> {
        (self.pos..self.pos + self.len).into_iter()
    }

    pub fn get_slice<'a>(&self, vec: &'a Vec<T>) -> &'a [T] {
        &vec[self.pos..self.pos + self.len]
    }
    pub fn get_slice_mut<'a>(&self, vec: &'a mut Vec<T>) -> &'a mut [T] {
        &mut vec[self.pos..self.pos + self.len]
    }
}

impl<T: Clone> VecSlice<T> {
    pub fn new_extend<E: ExactSizeIterator<Item = T>>(ref_vec: &mut Vec<T>, elements: E) -> Self {
        let pos = ref_vec.len();
        let len = elements.len();
        ref_vec.extend(elements);
        Self {
            pos,
            len,
            _phantom: Default::default(),
        }
    }
}
