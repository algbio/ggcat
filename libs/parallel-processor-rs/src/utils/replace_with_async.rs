use std::future::Future;
use std::ptr;

pub async fn replace_with_async<'a, R, F: Future<Output = R> + 'a, C: FnOnce(R) -> F>(
    dest: &mut R,
    fun: C,
) {
    unsafe {
        let old = ptr::read(dest);
        let new = fun(old).await;
        ptr::write(dest, new);
    }
}
