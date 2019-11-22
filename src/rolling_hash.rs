

pub struct RollingHash {
    value: u64,
    k: u32,
    cnt: u32
}

impl RollingHash {

    pub fn new(k: u32) -> RollingHash {
        RollingHash {
            value: 0,
            k,
            cnt: 0
        }
    }

    pub fn update(&mut self, base: u8) {
        self.value = (self.value << 2) ^ (base as u64);
    }

    pub fn get(&mut self) -> Option<u64> {
        if self.cnt < self.k {
            self.cnt += 1;
            None
        }
        else {
            Some(self.value)
        }
    }
}