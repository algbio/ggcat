
pub trait RollingKmerImpl<T> {
    fn clear(&mut self, ksize: usize);
    fn init(&mut self, index: usize, base: u8);
    fn iter(&mut self, index: usize, out_base: u8, in_base: u8) -> T;
}

#[derive(Debug)]
pub struct RollingKmersIterator<'a> {
    seq: &'a [u8],
    k_minus1: usize,
}

impl<'a> RollingKmersIterator<'a> {
    pub fn new(seq: &'a [u8], k: usize) -> RollingKmersIterator<'a> {
        RollingKmersIterator {
            seq,
            k_minus1: k - 1
        }
    }

    pub fn iter<T>(mut self, iter_impl: &'a mut (impl RollingKmerImpl<T> + 'a)) -> impl Iterator<Item = T> + 'a {
        iter_impl.clear(self.k_minus1 + 1);
        for (i, v) in self.seq[0..self.k_minus1].iter().enumerate() {
            iter_impl.init(i, *v);
        }

        (self.k_minus1..self.seq.len())
            .map(move |idx| iter_impl.iter(idx,
                                           unsafe { *self.seq.get_unchecked(idx - self.k_minus1) },
                                           unsafe { *self.seq.get_unchecked(idx) }
            ))
    }
}