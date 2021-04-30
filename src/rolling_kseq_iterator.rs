pub trait RollingKseqImpl<T: Copy, U: Copy> {
    fn clear(&mut self, ksize: usize);
    fn init(&mut self, index: usize, base: T);
    fn iter(&mut self, index: usize, out_base: T, in_base: T) -> U;
}

#[derive(Debug)]
pub struct RollingKseqIterator {}

impl RollingKseqIterator {
    pub fn iter_seq<'a, T: Copy, U: Copy>(
        seq: &'a [T],
        k: usize,
        iter_impl: &'a mut (impl RollingKseqImpl<T, U> + 'a),
    ) -> impl Iterator<Item = U> + 'a {
        let k_minus1 = k - 1;

        let maxv = if seq.len() > k_minus1 {
            iter_impl.clear(k);
            for (i, v) in seq[0..k_minus1].iter().enumerate() {
                iter_impl.init(i, *v);
            }
            seq.len()
        } else {
            0
        };

        (k_minus1..maxv).map(move |idx| {
            iter_impl.iter(idx, unsafe { *seq.get_unchecked(idx - k_minus1) }, unsafe {
                *seq.get_unchecked(idx)
            })
        })
    }
}
