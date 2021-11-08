use std::sync::atomic::{AtomicU64, Ordering};

pub static KCOUNTER: AtomicU64 = AtomicU64::new(0);

pub fn debug_increase() {
    KCOUNTER.fetch_add(1, Ordering::Relaxed);
}

pub fn debug_print() {
    println!("COUNTER: {:?}", KCOUNTER.load(Ordering::Relaxed));
}
