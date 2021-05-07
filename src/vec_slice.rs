use std::marker::PhantomData;

#[derive(Copy, Clone)]
pub struct VecSlice<T> {
    pos: usize,
    len: usize,
    _phantom: PhantomData<T>,
}

impl<T> VecSlice<T> {
    pub fn new(pos: usize, len: usize) -> Self {
        Self {
            pos,
            len,
            _phantom: Default::default(),
        }
    }
    fn get_slice<'a>(&self, vec: &'a Vec<T>) -> &'a [T] {
        &vec[self.pos..self.pos + self.len]
    }
    fn get_slice_mut<'a>(&self, vec: &'a mut Vec<T>) -> &'a mut [T] {
        &mut vec[self.pos..self.pos + self.len]
    }
}
