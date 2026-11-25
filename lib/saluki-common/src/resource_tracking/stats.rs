use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering::Relaxed};

const BATCH_BYTE_LIMIT: usize = 64 * 1024;
const BATCH_OBJECT_LIMIT: usize = 256;

pub(super) struct AllocationCounts {
    allocated_bytes: usize,
    allocated_objects: usize,
    deallocated_bytes: usize,
    deallocated_objects: usize,
}

impl AllocationCounts {
    pub(super) const EMPTY: Self = Self {
        allocated_bytes: 0,
        allocated_objects: 0,
        deallocated_bytes: 0,
        deallocated_objects: 0,
    };

    #[inline]
    pub(super) fn allocation(size: usize) -> Self {
        Self {
            allocated_bytes: size,
            allocated_objects: 1,
            ..Self::EMPTY
        }
    }

    #[inline]
    pub(super) fn deallocation(size: usize) -> Self {
        Self {
            deallocated_bytes: size,
            deallocated_objects: 1,
            ..Self::EMPTY
        }
    }

    #[inline]
    pub(super) fn resize(old_size: usize, new_size: usize) -> Self {
        Self {
            allocated_bytes: new_size.saturating_sub(old_size),
            deallocated_bytes: old_size.saturating_sub(new_size),
            ..Self::EMPTY
        }
    }

    #[inline]
    pub(super) fn accumulate(&mut self, other: &Self) -> bool {
        self.allocated_bytes += other.allocated_bytes;
        self.allocated_objects += other.allocated_objects;
        self.deallocated_bytes += other.deallocated_bytes;
        self.deallocated_objects += other.deallocated_objects;

        self.allocated_bytes + self.deallocated_bytes >= BATCH_BYTE_LIMIT
            || self.allocated_objects + self.deallocated_objects >= BATCH_OBJECT_LIMIT
    }
}

/// Statistics for an resource group.
pub struct ResourceStats {
    allocated_bytes: AtomicUsize,
    allocated_objects: AtomicUsize,
    deallocated_bytes: AtomicUsize,
    deallocated_objects: AtomicUsize,
    cpu_time_nanos: AtomicU64,
}

impl ResourceStats {
    pub(super) const fn new() -> Self {
        Self {
            allocated_bytes: AtomicUsize::new(0),
            allocated_objects: AtomicUsize::new(0),
            deallocated_bytes: AtomicUsize::new(0),
            deallocated_objects: AtomicUsize::new(0),
            cpu_time_nanos: AtomicU64::new(0),
        }
    }

    /// Returns `true` if the group has published any allocated bytes.
    pub fn has_allocated(&self) -> bool {
        self.allocated_bytes.load(Relaxed) > 0
    }

    #[inline]
    pub(super) fn publish_allocations(&self, counts: &AllocationCounts) {
        for (counter, delta) in [
            (&self.allocated_bytes, counts.allocated_bytes),
            (&self.allocated_objects, counts.allocated_objects),
            (&self.deallocated_bytes, counts.deallocated_bytes),
            (&self.deallocated_objects, counts.deallocated_objects),
        ] {
            if delta != 0 {
                counter.fetch_add(delta, Relaxed);
            }
        }
    }

    pub(super) fn track_cpu_time(&self, nanos: u64) {
        if nanos != 0 {
            self.cpu_time_nanos.fetch_add(nanos, Relaxed);
        }
    }

    /// Captures a snapshot of the current statistics based on the delta from a previous snapshot.
    ///
    /// This can be used to keep a single local snapshot of the last delta, and then both track the delta since that
    /// snapshot, as well as update the snapshot to the current statistics.
    ///
    /// Callers should generally create their snapshot via [`ResourceStatsSnapshot::empty`] and then use this method
    /// to get their snapshot delta, utilize those delta values in whatever way is necessary, and then merge the
    /// snapshot delta into the primary snapshot via [`ResourceStatsSnapshot::merge`] to make it current.
    ///
    /// Active groups can have unpublished counts. Snapshots defer published frees that exceed published allocations
    /// until the allocation counts catch up.
    pub fn snapshot_delta(&self, previous: &ResourceStatsSnapshot) -> ResourceStatsSnapshot {
        let deallocated_bytes = self.deallocated_bytes.load(Relaxed);
        let deallocated_objects = self.deallocated_objects.load(Relaxed);
        let allocated_bytes = self.allocated_bytes.load(Relaxed);
        let allocated_objects = self.allocated_objects.load(Relaxed);
        ResourceStatsSnapshot {
            allocated_bytes: allocated_bytes.saturating_sub(previous.allocated_bytes),
            allocated_objects: allocated_objects.saturating_sub(previous.allocated_objects),
            deallocated_bytes: deallocated_bytes
                .min(allocated_bytes)
                .saturating_sub(previous.deallocated_bytes),
            deallocated_objects: deallocated_objects
                .min(allocated_objects)
                .saturating_sub(previous.deallocated_objects),
            cpu_time_nanos: self
                .cpu_time_nanos
                .load(Relaxed)
                .saturating_sub(previous.cpu_time_nanos),
        }
    }
}

/// Snapshot of allocation statistics for a group.
pub struct ResourceStatsSnapshot {
    /// Number of allocated bytes since the last snapshot.
    pub allocated_bytes: usize,

    /// Number of allocated objects since the last snapshot.
    pub allocated_objects: usize,

    /// Number of deallocated bytes since the last snapshot.
    pub deallocated_bytes: usize,

    /// Number of deallocated objects since the last snapshot.
    pub deallocated_objects: usize,

    /// Cumulative CPU time in nanoseconds consumed by this group since the last snapshot.
    pub cpu_time_nanos: u64,
}

impl ResourceStatsSnapshot {
    /// Creates an empty `ResourceStatsSnapshot`.
    pub const fn empty() -> Self {
        Self {
            allocated_bytes: 0,
            allocated_objects: 0,
            deallocated_bytes: 0,
            deallocated_objects: 0,
            cpu_time_nanos: 0,
        }
    }

    /// Returns the number of live allocated bytes.
    ///
    /// Saturates at zero because a delta can contain frees for allocations from an earlier snapshot.
    pub fn live_bytes(&self) -> usize {
        self.allocated_bytes.saturating_sub(self.deallocated_bytes)
    }

    /// Returns the number of live allocated objects.
    ///
    /// Saturates at zero, for the same reason as [`live_bytes`][Self::live_bytes].
    pub fn live_objects(&self) -> usize {
        self.allocated_objects.saturating_sub(self.deallocated_objects)
    }

    /// Merges `other` into `self`.
    ///
    /// This accumulates the totals from successive calls to [`ResourceStats::snapshot_delta`].
    pub fn merge(&mut self, other: &Self) {
        self.allocated_bytes += other.allocated_bytes;
        self.allocated_objects += other.allocated_objects;
        self.deallocated_bytes += other.deallocated_bytes;
        self.deallocated_objects += other.deallocated_objects;
        self.cpu_time_nanos += other.cpu_time_nanos;
    }
}

/// Returns the current thread's CPU time in nanoseconds, or `None` if unavailable.
#[cfg(target_os = "linux")]
#[inline]
pub(crate) fn thread_cpu_time_nanos() -> Option<u64> {
    // `CLOCK_THREAD_CPUTIME_ID` requires a syscall. Sample only at active group changes so that nested guards for
    // the same group do not incur another pair of syscalls.

    let mut ts = libc::timespec { tv_sec: 0, tv_nsec: 0 };
    // SAFETY: We pass a valid reference to the `timespec` struct, and `CLOCK_THREAD_CPUTIME_ID` has been available
    // since Linux 2.6.12.
    let ret = unsafe { libc::clock_gettime(libc::CLOCK_THREAD_CPUTIME_ID, &mut ts) };
    if ret == 0 {
        Some(ts.tv_sec as u64 * 1_000_000_000 + ts.tv_nsec as u64)
    } else {
        None
    }
}

/// Returns the current thread's CPU time in nanoseconds, or `None` if unavailable.
#[cfg(not(target_os = "linux"))]
#[inline]
pub(crate) fn thread_cpu_time_nanos() -> Option<u64> {
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(target_os = "linux")]
    #[test]
    fn thread_cpu_time_returns_some() {
        let t = thread_cpu_time_nanos();
        assert!(t.is_some());
        assert!(t.unwrap() > 0);
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn thread_cpu_time_is_monotonic() {
        let t1 = thread_cpu_time_nanos().unwrap();
        // Do some work to consume CPU.
        let mut sum = 0u64;
        for i in 0..100_000 {
            sum = sum.wrapping_add(i);
        }
        std::hint::black_box(sum);
        let t2 = thread_cpu_time_nanos().unwrap();
        assert!(t2 > t1);
    }

    #[cfg(not(target_os = "linux"))]
    #[test]
    fn thread_cpu_time_returns_none_on_non_linux() {
        assert!(thread_cpu_time_nanos().is_none());
    }
}
