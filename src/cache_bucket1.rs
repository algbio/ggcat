
pub const BULK_SIZE: usize = 16;//65535;

trait Test {
    const constant: usize;
}

#[derive(Copy, Clone)]
pub struct CacheBucket<T: Default + Copy, const size: usize> {
    size: u16,
    data: [T; size]
}

//impl Test for abc {
//    const constant: usize = 3213;
//}


impl<T: Default + Copy + Test> CacheBucket<T> {

    pub fn new() -> CacheBucket<T> {
        CacheBucket {
            size: (T::constant as u16),
            data: [Default::default(); BULK_SIZE]
        }
    }

    pub fn push<F: FnMut(T)>(&mut self, val: T, mut lambda: F) {
        self.data[self.size as usize] = T::from(val);
        self.size += 1;
        if self.size >= (BULK_SIZE as u16) {
            self.flush(lambda)
        }
    }

    #[inline(always)]
    pub fn flush<F: FnMut(T)>(&mut self, mut lambda: F) {
        for hash in &self.data[0..(self.size as usize)] {
            lambda(*hash);
        }
        self.size = 0;
    }
}