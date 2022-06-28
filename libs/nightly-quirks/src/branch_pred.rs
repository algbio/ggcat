#[cold]
#[inline]
fn cold_fn() {}

#[inline(always)]
pub fn likely(cond: bool) -> bool {
    if !cond {
        cold_fn();
    }
    cond
}

#[inline(always)]
pub fn unlikely(cond: bool) -> bool {
    if cond {
        cold_fn();
    }
    cond
}
