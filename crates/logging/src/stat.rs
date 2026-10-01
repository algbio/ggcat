//! Counters that can be compiled out.
//!
//! With the `stats` feature every type here holds its value. Without it each one is a
//! zero-sized type, every update compiles to nothing and reading one gives zero, so the
//! code that keeps the counters needs no `cfg` of its own. [`Stat::ENABLED`] tells the
//! code that only reports them whether there is anything to report.
//!
//! Only figures that are reported belong here: a value the program also decides on must
//! stay a plain integer, or it would read zero in a build without the feature.
//!
//! [`Stat`] has the interface of `lz_copyback::Stat`, whose counters the `stats` feature
//! of `ggcat_io` switches together with these.

use std::fmt;
use std::ops::AddAssign;
use std::time::Duration;

/// One counter.
#[derive(Clone, Copy, Default, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Stat {
    #[cfg(feature = "stats")]
    value: u64,
}

#[cfg(feature = "stats")]
impl Stat {
    /// The value, or `0` when the counters are compiled out.
    #[inline(always)]
    pub fn get(&self) -> u64 {
        self.value
    }

    #[inline(always)]
    pub fn add(&mut self, n: u64) {
        self.value += n;
    }

    #[inline(always)]
    pub fn set(&mut self, n: u64) {
        self.value = n;
    }

    /// Keep the larger of the two.
    #[inline(always)]
    pub fn max_with(&mut self, n: u64) {
        if n > self.value {
            self.value = n;
        }
    }

    /// Keep the smaller of the two.
    #[inline(always)]
    pub fn min_with(&mut self, n: u64) {
        if n < self.value {
            self.value = n;
        }
    }
}

#[cfg(not(feature = "stats"))]
impl Stat {
    #[inline(always)]
    pub fn get(&self) -> u64 {
        0
    }

    #[inline(always)]
    pub fn add(&mut self, _n: u64) {}

    #[inline(always)]
    pub fn set(&mut self, _n: u64) {}

    #[inline(always)]
    pub fn max_with(&mut self, _n: u64) {}

    #[inline(always)]
    pub fn min_with(&mut self, _n: u64) {}
}

impl Stat {
    /// Whether the counters are compiled in at all.
    pub const ENABLED: bool = cfg!(feature = "stats");

    /// Zero, for statics.
    pub const ZERO: Stat = Stat {
        #[cfg(feature = "stats")]
        value: 0,
    };

    #[inline(always)]
    pub fn inc(&mut self) {
        self.add(1);
    }
}

impl AddAssign<u64> for Stat {
    #[inline(always)]
    fn add_assign(&mut self, n: u64) {
        self.add(n);
    }
}

impl fmt::Debug for Stat {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if Self::ENABLED {
            write!(f, "{}", self.get())
        } else {
            f.write_str("off")
        }
    }
}

impl fmt::Display for Stat {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.get(), f)
    }
}

impl From<Stat> for u64 {
    #[inline(always)]
    fn from(s: Stat) -> u64 {
        s.get()
    }
}

/// Time spent, summed. [`Duration::ZERO`] when the counters are compiled out.
#[derive(Clone, Copy, Default, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct StatDuration {
    #[cfg(feature = "stats")]
    value: Duration,
}

impl StatDuration {
    pub const ZERO: StatDuration = StatDuration {
        #[cfg(feature = "stats")]
        value: Duration::ZERO,
    };

    #[inline(always)]
    pub fn get(&self) -> Duration {
        #[cfg(feature = "stats")]
        return self.value;
        #[cfg(not(feature = "stats"))]
        return Duration::ZERO;
    }

    #[inline(always)]
    pub fn add(&mut self, _d: Duration) {
        #[cfg(feature = "stats")]
        {
            self.value += _d;
        }
    }

    #[inline(always)]
    pub fn set(&mut self, _d: Duration) {
        #[cfg(feature = "stats")]
        {
            self.value = _d;
        }
    }
}

impl AddAssign<Duration> for StatDuration {
    #[inline(always)]
    fn add_assign(&mut self, d: Duration) {
        self.add(d);
    }
}

impl fmt::Debug for StatDuration {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if Stat::ENABLED {
            fmt::Debug::fmt(&self.get(), f)
        } else {
            f.write_str("off")
        }
    }
}

/// A start time for a [`StatDuration`]. Without the counters it reads no clock and its
/// [`elapsed`](Self::elapsed) is zero.
#[derive(Clone, Copy)]
pub struct StatTimer {
    #[cfg(feature = "stats")]
    start: std::time::Instant,
}

impl StatTimer {
    #[inline(always)]
    pub fn start() -> Self {
        Self {
            #[cfg(feature = "stats")]
            start: std::time::Instant::now(),
        }
    }

    #[inline(always)]
    pub fn elapsed(&self) -> Duration {
        #[cfg(feature = "stats")]
        return self.start.elapsed();
        #[cfg(not(feature = "stats"))]
        return Duration::ZERO;
    }
}

/// A [`Stat`] several threads add to at once.
#[derive(Default)]
pub struct AtomicStat {
    #[cfg(feature = "stats")]
    value: std::sync::atomic::AtomicU64,
}

impl AtomicStat {
    pub const fn new() -> Self {
        Self {
            #[cfg(feature = "stats")]
            value: std::sync::atomic::AtomicU64::new(0),
        }
    }

    #[inline(always)]
    pub fn add(&self, _n: u64) {
        #[cfg(feature = "stats")]
        self.value
            .fetch_add(_n, std::sync::atomic::Ordering::Relaxed);
    }

    #[inline(always)]
    pub fn inc(&self) {
        self.add(1);
    }

    #[inline(always)]
    pub fn load(&self) -> Stat {
        Stat {
            #[cfg(feature = "stats")]
            value: self.value.load(std::sync::atomic::Ordering::Relaxed),
        }
    }
}

impl fmt::Debug for AtomicStat {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(&self.load(), f)
    }
}

#[cfg(not(feature = "stats"))]
const _: () = assert!(
    size_of::<Stat>() == 0
        && size_of::<StatDuration>() == 0
        && size_of::<StatTimer>() == 0
        && size_of::<AtomicStat>() == 0
);
