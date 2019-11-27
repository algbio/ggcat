use stopwatch::Stopwatch;

pub struct Progress {
    stopwatch: Stopwatch,
    pcounter: u64,
    tcounter: u64
}

impl Progress {
    pub fn new() -> Progress {
        let mut progress = Progress {
            stopwatch: Stopwatch::new(),
            pcounter: 0,
            tcounter: 0
        };
        progress.stopwatch.start();
        progress
    }

    pub fn incr(&mut self, value: u64) {
        self.pcounter += value-1;
        self.tcounter += value-1;
    }

    #[inline(always)]
    pub fn event<C: FnOnce(u64, u64) -> bool, F: FnOnce(u64, u64, f64, f64)>(&mut self, condition: C, printing: F) {
        if condition(self.tcounter, self.pcounter) {
            printing(self.tcounter, self.pcounter, self.pcounter as f64 / self.stopwatch.elapsed().as_secs_f64(), self.stopwatch.elapsed().as_secs_f64());
            self.stopwatch.reset();
            self.stopwatch.start();
            self.pcounter = 0;
        }
        self.pcounter += 1;
        self.tcounter += 1;
    }

    pub fn elapsed(&self) -> f64 {
        self.stopwatch.elapsed().as_secs_f64()
    }
}