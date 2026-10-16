//! What runs on the vectors of the [`HashKernel`] picked at runtime: a [`KernelTask`], its method
//! generic over them, which [`HashKernel::run`] calls from a function compiled for the kernel's
//! instructions. And the tasks of the hashing loop: a [`LanesTask`] rolls the hashes, handing
//! each position's to its [`LanesConsumer`].

#[cfg(target_arch = "aarch64")]
use super::kernels::Neon;
use super::kernels::Portable;
#[cfg(target_arch = "x86_64")]
use super::kernels::{Avx2, Avx512, Ssse3};
use super::nthash::{NtHashLanes, roll};
use super::{BaseLanes, HashKernel, HashVector, SIMD_LANES};

/// Work to run on the vectors of whichever kernel is picked: its method is generic over them, so
/// that one value runs on any kernel.
pub(super) trait KernelTask {
    type Output;

    /// # Safety
    ///
    /// As [`HashVector`]'s.
    unsafe fn run<V: HashVector<SIMD_LANES>>(self) -> Self::Output;
}

impl HashKernel {
    /// Runs `task` on this kernel's vectors, in a function compiled for its instructions, which
    /// this CPU has to support.
    pub(super) fn run<T: KernelTask>(self, task: T) -> T::Output {
        assert!(self.is_supported(), "{self:?} is not supported here");
        match self {
            // SAFETY: just checked.
            #[cfg(target_arch = "x86_64")]
            Self::Avx512 => unsafe { run_avx512(task) },
            #[cfg(target_arch = "x86_64")]
            Self::Avx2 => unsafe { run_avx2(task) },
            #[cfg(target_arch = "x86_64")]
            Self::Ssse3 => unsafe { run_ssse3(task) },
            #[cfg(target_arch = "aarch64")]
            Self::Neon => unsafe { run_neon(task) },
            // SAFETY: plain arrays run anywhere.
            Self::Portable => unsafe { task.run::<Portable<SIMD_LANES>>() },
        }
    }
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2,avx512f,avx512vl")]
fn run_avx512<T: KernelTask>(task: T) -> T::Output {
    // SAFETY: this function only runs where AVX-512 is enabled.
    unsafe { task.run::<Avx512>() }
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
fn run_avx2<T: KernelTask>(task: T) -> T::Output {
    // SAFETY: this function only runs where AVX2 is enabled.
    unsafe { task.run::<Avx2>() }
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "ssse3")]
fn run_ssse3<T: KernelTask>(task: T) -> T::Output {
    // SAFETY: this function only runs where SSSE3 is enabled.
    unsafe { task.run::<Ssse3>() }
}

#[cfg(target_arch = "aarch64")]
#[target_feature(enable = "neon")]
fn run_neon<T: KernelTask>(task: T) -> T::Output {
    // SAFETY: this function only runs where NEON is enabled.
    unsafe { task.run::<Neon>() }
}

/// What runs on the hashes, on the vectors of whichever kernel is picked: its method is generic
/// over them, so that one value runs on any kernel. It rolls the hashes with [`roll`], handing
/// them to a [`LanesConsumer`] of its own.
pub(super) trait LanesTask<const LANES: usize> {
    /// # Safety
    ///
    /// As [`HashVector`]'s; the lanes are `m` bases long at least, `m` is not zero.
    unsafe fn run<S: BaseLanes<LANES>, V: HashVector<LANES>>(self, seq: S, m: usize);
}

/// What the hashing loop hands the hashes of each position to, in the vectors `V`. The loop
/// rolls them in runs, and asks the consumer before each how long it can be: the work it does
/// between runs stays out of the loop over a run, which keeps the hashes in registers.
pub(super) trait LanesConsumer<V, const LANES: usize> {
    /// Before rolling from position `index`, where `available` positions can be rolled without
    /// stopping, one at least: how many of them to roll before it is asked again, one at least.
    unsafe fn prepare(&mut self, index: usize, available: usize) -> usize;

    /// The hashes of the m-mers at position `index` of every lane, of their forward strands and
    /// of their reverse complements.
    unsafe fn consume(&mut self, index: usize, forward: V, reverse: V);

    /// After the last position.
    unsafe fn finish(&mut self);
}

/// Hands each position's canonical hashes to its callback: what `for_each` runs.
pub(super) struct HandOut<F>(pub(super) F);

impl<V: HashVector<LANES>, F: FnMut(usize, &NtHashLanes<LANES>), const LANES: usize>
    LanesConsumer<V, LANES> for HandOut<F>
{
    #[inline(always)]
    unsafe fn prepare(&mut self, _index: usize, available: usize) -> usize {
        available
    }

    #[inline(always)]
    unsafe fn consume(&mut self, index: usize, forward: V, reverse: V) {
        let hashes = unsafe {
            NtHashLanes {
                canonical: V::canonical(forward, reverse).to_array(),
                forward: V::forward_mask(forward, reverse),
            }
        };
        (self.0)(index, &hashes);
    }

    #[inline(always)]
    unsafe fn finish(&mut self) {}
}

impl<F: FnMut(usize, &NtHashLanes<LANES>), const LANES: usize> LanesTask<LANES> for HandOut<F> {
    #[inline(always)]
    unsafe fn run<S: BaseLanes<LANES>, V: HashVector<LANES>>(mut self, seq: S, m: usize) {
        unsafe { roll::<S, V, Self, LANES>(seq, m, &mut self) }
    }
}

/// A [`LanesTask`] with the lanes and the m-mers' length to run it on: a [`KernelTask`]. The
/// lanes are `m` bases long at least, and `m` is not zero.
pub(super) struct OnLanes<S, T> {
    pub(super) seq: S,
    pub(super) m: usize,
    pub(super) task: T,
}

impl<S: BaseLanes<SIMD_LANES>, T: LanesTask<SIMD_LANES>> KernelTask for OnLanes<S, T> {
    type Output = ();

    #[inline(always)]
    unsafe fn run<V: HashVector<SIMD_LANES>>(self) {
        // SAFETY: the caller's, and the lanes' and m's, as made.
        unsafe { self.task.run::<S, V>(self.seq, self.m) }
    }
}
