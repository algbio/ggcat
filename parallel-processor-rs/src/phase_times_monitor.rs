use parking_lot::RwLock;
use std::time::{Duration, Instant};

pub struct PhaseResult {
    name: String,
    time: Duration,
}

pub struct PhaseTimesMonitor {
    timer: Option<Instant>,
    phase: Option<(String, Instant)>,
    results: Vec<PhaseResult>,
}

pub static PHASES_TIMES_MONITOR: RwLock<PhaseTimesMonitor> = RwLock::new(PhaseTimesMonitor::new());

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
            println!("Finished {}. phase duration: {:.2?}", name, &elapsed);
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

    pub fn get_formatted_counter(&self) -> String {
        format!(
            " ptime: {:.2?} gtime: {:.2?}",
            self.phase
                .as_ref()
                .map(|pt| pt.1.elapsed())
                .unwrap_or(Duration::from_millis(0)),
            self.get_wallclock()
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
