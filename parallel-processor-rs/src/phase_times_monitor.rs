use crate::memory_fs::allocator::CHUNKS_ALLOCATOR;
use parking_lot::RwLock;
use std::time::{Duration, Instant};
use parking_lot::lock_api::RawRwLock;

pub struct PhaseResult {
    name: String,
    time: Duration,
}

pub struct PhaseTimesMonitor {
    timer: Option<Instant>,
    phase: Option<(String, Instant)>,
    results: Vec<PhaseResult>,
}

pub static PHASES_TIMES_MONITOR: RwLock<PhaseTimesMonitor> = RwLock::const_new(parking_lot::RawRwLock::INIT, PhaseTimesMonitor::new());

impl PhaseTimesMonitor {
    const fn new() -> PhaseTimesMonitor {
        PhaseTimesMonitor {
            timer: None,
            phase: None,
            results: Vec::new(),
        }
    }

    pub fn init(&mut self) {
        self.timer = Some(Instant::now());
    }

    fn end_phase(&mut self) {
        if let Some((name, phase_timer)) = self.phase.take() {
            let elapsed = phase_timer.elapsed();
            println!(
                "Finished {}. phase duration: {:.2?} gtime: {:.2?} ", // memory: {:.2} {:.2}%
                name,
                &elapsed,
                self.get_wallclock()
            );
            self.results.push(PhaseResult {
                name,
                time: elapsed,
            })
        }
    }

    pub fn start_phase(&mut self, name: String) {
        self.end_phase();
        println!("Started {}", name);
        self.phase = Some((name, Instant::now()));
    }

    pub fn get_wallclock(&self) -> Duration {
        self.timer
            .as_ref()
            .map(|t| t.elapsed())
            .unwrap_or(Duration::from_millis(0))
    }

    pub fn get_phase_desc(&self) -> String {
        self.phase
            .as_ref()
            .map(|x| x.0.clone())
            .unwrap_or(String::new())
    }

    pub fn get_phase_timer(&self) -> Duration {
        self.phase
            .as_ref()
            .map(|x| x.1.elapsed())
            .unwrap_or(Duration::from_millis(0))
    }

    pub fn get_formatted_counter(&self) -> String {
        let total_mem = CHUNKS_ALLOCATOR.get_total_memory();
        let free_mem = CHUNKS_ALLOCATOR.get_free_memory();

        format!(
            " ptime: {:.2?} gtime: {:.2?} memory: {:.2} {:.2}%",
            self.phase
                .as_ref()
                .map(|pt| pt.1.elapsed())
                .unwrap_or(Duration::from_millis(0)),
            self.get_wallclock(),
            total_mem - free_mem,
            ((1.0 - free_mem / total_mem) * 100.0)
        )
    }

    pub fn get_formatted_counter_without_memory(&self) -> String {
        format!(
            " ptime: {:.2?} gtime: {:.2?}",
            self.phase
                .as_ref()
                .map(|pt| pt.1.elapsed())
                .unwrap_or(Duration::from_millis(0)),
            self.get_wallclock(),
        )
    }

    pub fn print_stats(&mut self, end_message: String) {
        self.end_phase();

        println!("{}", end_message);
        println!("TOTAL TIME: {:.2?}", self.get_wallclock());
        println!("Final stats:");

        for PhaseResult { name, time } in self.results.iter() {
            println!("\t{} \t=> {:.2?}", name, time);
        }
    }
}
