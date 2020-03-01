
pub trait RollingKseqImpl<T: Copy, U: Copy> {
    fn clear(&mut self, ksize: usize);
    fn init(&mut self, index: usize, base: T);
    fn iter(&mut self, index: usize, out_base: T, in_base: T) -> U;
}

#[derive(Debug)]
pub struct RollingKseqIterator<'a, T: Copy> {
    seq: &'a [T],
    k_minus1: usize,
}

impl<'a, T: Copy> RollingKseqIterator<'a, T> {
    pub fn new(seq: &'a [T], k: usize) -> RollingKseqIterator<'a, T> {
        RollingKseqIterator {
            seq,
            k_minus1: k - 1
        }
    }

    pub fn iter<U: Copy>(mut self, iter_impl: &'a mut (impl RollingKseqImpl<T, U> + 'a)) -> impl Iterator<Item = U> + 'a {

        let maxv;

        if self.seq.len() > self.k_minus1 {
            iter_impl.clear(self.k_minus1 + 1);
            for (i, v) in self.seq[0..self.k_minus1].iter().enumerate() {
                iter_impl.init(i, *v);
            }
            maxv = self.seq.len();
        }
        else {
            maxv = 0;
        }

        (self.k_minus1..maxv)
            .map(move |idx| iter_impl.iter(idx,
                                           unsafe { *self.seq.get_unchecked(idx - self.k_minus1) },
                                           unsafe { *self.seq.get_unchecked(idx) }
            ))
    }
}