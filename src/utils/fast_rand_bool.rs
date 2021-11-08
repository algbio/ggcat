use rand::rngs::ThreadRng;
use rand::{thread_rng, RngCore};

pub struct FastRandBool {
    random: ThreadRng,
    randidx: usize,
    randval: u64,
}

impl FastRandBool {
    pub fn new() -> Self {
        let mut random = thread_rng();
        let randval = random.next_u64();
        Self {
            random,
            randidx: 64,
            randval,
        }
    }

    pub fn get_randbool(&mut self) -> bool {
        if self.randidx == 0 {
            self.randval = self.random.next_u64();
            self.randidx = 64;
        }
        self.randidx -= 1;
        let result = (self.randval & 0x1) == 1;
        self.randval >>= 1;
        result
    }
}
