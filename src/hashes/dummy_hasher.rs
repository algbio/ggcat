use std::hash::{BuildHasher, Hasher};

#[derive(Copy, Clone)]
pub struct DummyHasherBuilder;

impl BuildHasher for DummyHasherBuilder {
    type Hasher = DummyHasher;

    #[inline(always)]
    fn build_hasher(&self) -> Self::Hasher {
        DummyHasher { 0: 0 }
    }
}

pub struct DummyHasher(u64);

impl Hasher for DummyHasher {
    #[inline(always)]
    fn finish(&self) -> u64 {
        self.0
    }

    fn write(&mut self, bytes: &[u8]) {
        panic!("Not supported!");
    }

    #[inline(always)]
    fn write_u32(&mut self, i: u32) {
        self.0 = i as u64 | ((i as u64) << 32)
    }

    #[inline(always)]
    fn write_u64(&mut self, i: u64) {
        self.0 = i;
    }

    #[inline(always)]
    fn write_u128(&mut self, i: u128) {
        self.0 = i as u64;
    }

    #[inline(always)]
    fn write_usize(&mut self, i: usize) {
        self.0 = i as u64;
    }
}
