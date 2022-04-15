use rand::rngs::ThreadRng;
use rand::{thread_rng, RngCore};

// Increasing PROB_ITERS decreases the probability that a true value happens,
// by combining with and multiple random values
pub struct FastRandBool<const PROB_ITERS: usize> {
    random: ThreadRng,
    randidx: usize,
    randval: u64,
}

impl<const PROB_ITERS: usize> FastRandBool<PROB_ITERS> {
    fn get_random(&mut self) -> u64 {
        let mut val = u64::MAX;
        for _ in 0..PROB_ITERS {
            val &= self.random.next_u64();
        }

        val
    }

    pub fn new() -> Self {
        let random = thread_rng();
        Self {
            random,
            randidx: 0,
            randval: 0,
        }
    }

    pub fn get_randbool(&mut self) -> bool {
        if self.randidx == 0 {
            self.randval = self.get_random();
            self.randidx = 64;
        }
        self.randidx -= 1;
        let result = (self.randval & 0x1) == 1;
        self.randval >>= 1;
        result
    }
}
