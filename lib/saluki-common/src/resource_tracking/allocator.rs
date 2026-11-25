//! Global allocator implementation that allows tracking allocations on a per-group basis.

// TODO: The current design does not allow for deregistering groups, which is currently fine and
// likely will be for a while, but would be a limitation in a world where we dynamically launched
// data pipelines and wanted to clean up removed components, and so on.

use std::{
    alloc::{GlobalAlloc, Layout},
    ptr,
    sync::OnceLock,
};

use super::{
    groups::track_memory,
    stats::{AllocationCounts, ResourceStats},
    ResourceGroupToken,
};

const STATS_LAYOUT: Layout = Layout::new::<*const ResourceStats>();
static DIRECT_BACKEND: OnceLock<&'static (dyn GlobalAlloc + Sync)> = OnceLock::new();

/// An allocator for storage whose owner is known without an allocation trailer.
///
/// Use the same allocator value to free an allocation, even from a different resource group.
#[derive(Clone, Copy)]
pub struct ComponentAllocator {
    group: ResourceGroupToken,
    backend: &'static (dyn GlobalAlloc + Sync),
}

impl ComponentAllocator {
    /// Creates a trailer-free allocator for `group` if the process has installed a direct backend.
    pub fn new(group: ResourceGroupToken) -> Option<Self> {
        Some(Self {
            group,
            backend: *DIRECT_BACKEND.get()?,
        })
    }
}

// SAFETY: The backend is fixed for the life of this value. Caller-provided layout and pointer are unchanged.
unsafe impl GlobalAlloc for ComponentAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        // SAFETY: The caller supplies a valid layout for allocation.
        let ptr = unsafe { self.backend.alloc(layout) };
        if !ptr.is_null() {
            track_memory(Some(self.group.stats()), AllocationCounts::allocation(layout.size()));
        }
        ptr
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        track_memory(Some(self.group.stats()), AllocationCounts::deallocation(layout.size()));
        // SAFETY: The caller supplies a live pointer allocated by this backend with this layout.
        unsafe { self.backend.dealloc(ptr, layout) };
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        // SAFETY: The caller satisfies the backend's reallocation requirements.
        let resized = unsafe { self.backend.realloc(ptr, layout, new_size) };
        if !resized.is_null() && new_size != layout.size() {
            track_memory(
                Some(self.group.stats()),
                AllocationCounts::resize(layout.size(), new_size),
            );
        }
        resized
    }
}

/// A global allocator that tracks allocations on a per-group basis.
///
/// This allocator provides the ability to track the allocations/deallocations, both in bytes and objects, for
/// different, user-defined resource groups.
///
/// # Resource groups
///
/// Allocation (and deallocations) are tracked by **resource group**. When this allocator is used, every allocation is
/// associated with an resource group. Resource groups are user-defined, except for the default "root" resource
/// group which acts as a catch-all when a user-defined group isn't currently entered.
///
/// # Token guard
///
/// When an resource group is registered, an `ResourceGroupToken` is returned. This token can be used to "enter" the
/// group, which attribute all allocations on the current thread to that group. Entering the group returns a drop guard
/// that restores the previously entered allocation when it's dropped.
///
/// This allows for arbitrarily nested resource groups.
///
/// ## Changes to memory layout
///
/// In order to associate an allocation with the current resource group, a small trailer is added to the requested
/// allocation layout, in the form of a pointer to the statistics for the resource group. This allows updating the
/// statistics directly when an allocation is deallocated, without having to externally keep track of what group a given
/// allocation belongs to. A resize keeps its original owner and charges only the change in padded bytes.
/// The trailer adds one machine word, plus any padding required by the allocation and trailer alignments.
pub struct TrackingAllocator<A> {
    allocator: A,
}

impl<A> TrackingAllocator<A> {
    /// Creates a new `TrackingAllocator` that wraps another allocator.
    ///
    /// The wrapped allocator is used to actually allocate and deallocate memory, while this allocator is responsible
    /// purely for tracking the allocations and deallocations themselves.
    pub const fn new(allocator: A) -> Self {
        Self { allocator }
    }

    /// Makes the wrapped allocator available to component allocators created after this call.
    ///
    /// Call this on the global tracking allocator at application startup, before building components.
    pub fn install_component_backend(&'static self)
    where
        A: GlobalAlloc + Sync,
    {
        DIRECT_BACKEND.get_or_init(|| &self.allocator);
    }
}

// SAFETY: Allocations use the same padded layout for all backend operations. The group trailer remains private.
unsafe impl<A> GlobalAlloc for TrackingAllocator<A>
where
    A: GlobalAlloc,
{
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let Some((layout, trailer_start)) = get_layout_with_group_trailer(layout) else {
            return ptr::null_mut();
        };
        // SAFETY: The padded layout fits in `Layout` and has nonzero size.
        let ptr = unsafe { self.allocator.alloc(layout) };
        if !ptr.is_null() {
            let group = track_memory(None, AllocationCounts::allocation(layout.size()));
            // SAFETY: The padded allocation reserves aligned space for the trailer at this offset.
            unsafe { trailer_ptr_from(ptr, trailer_start).write(ptr::from_ref(group)) };
        }
        ptr
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        // SAFETY: A live allocation from this allocator must have a valid padded layout and initialized trailer.
        let (layout, trailer_start) = unsafe { get_layout_with_group_trailer(layout).unwrap_unchecked() };
        // SAFETY: The group trailer holds the original owner's static statistics pointer.
        let group = unsafe { &**trailer_ptr_from(ptr, trailer_start) };
        track_memory(Some(group), AllocationCounts::deallocation(layout.size()));

        // SAFETY: This is the layout used to allocate the block from the backend.
        unsafe { self.allocator.dealloc(ptr, layout) };
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        let Some((new_layout, new_trailer_start)) = Layout::from_size_align(new_size, layout.align())
            .ok()
            .and_then(get_layout_with_group_trailer)
        else {
            return ptr::null_mut();
        };
        // SAFETY: A live allocation from this allocator must have a valid padded layout and initialized trailer.
        let (old_layout, old_trailer_start) = unsafe { get_layout_with_group_trailer(layout).unwrap_unchecked() };
        if new_trailer_start == old_trailer_start {
            return ptr;
        }

        // SAFETY: The old padded layout reserves an initialized group pointer at this offset.
        let group = unsafe { *trailer_ptr_from(ptr, old_trailer_start) };
        if new_layout.size() == old_layout.size() {
            // SAFETY: The block already has room for the trailer at its new aligned offset.
            unsafe { trailer_ptr_from(ptr, new_trailer_start).write(group) };
            return ptr;
        }

        // SAFETY: The padded layouts retain the same alignment. The new padded size is valid and nonzero.
        let resized = unsafe { self.allocator.realloc(ptr, old_layout, new_layout.size()) };
        if !resized.is_null() {
            // SAFETY: The resized block has room for the trailer, which holds the original static group pointer.
            unsafe { trailer_ptr_from(resized, new_trailer_start).write(group) };
            track_memory(
                Some(unsafe { &*group }),
                AllocationCounts::resize(old_layout.size(), new_layout.size()),
            );
        }
        resized
    }
}

/// Returns a pointer to the group trailer of the allocation based at `ptr`.
///
/// # Provenance
///
/// The trailer sits past the end of the layout the caller asked for, and the caller's pointer does not carry provenance
/// that reaches it. `<*mut u8>::add` requires its result to stay within the allocated object, and the object the
/// compiler believes it is offsetting within is exactly `layout.size()` bytes: the allocation shim generated for a
/// global allocator advertises the requested size to the optimizer, not whatever larger block the allocator actually
/// carved out. Reaching the trailer with ordinary pointer arithmetic is therefore out of bounds, and the resulting
/// offset folds to a poison value wherever the optimizer can see both the allocation and the offset -- which it can
/// whenever the shim is inlined into the allocating function. A poisoned pointer then fails checks it should pass,
/// including the alignment check inserted under debug assertions.
///
/// Synthesizing the pointer from the address keeps the trailer reachable. `alloc` and `realloc` expose the provenance
/// of the whole padded block. The pointer recovered from the trailer address then carries that provenance.
fn trailer_ptr_from(ptr: *mut u8, trailer_start: usize) -> *mut *const ResourceStats {
    ptr::with_exposed_provenance_mut(ptr.expose_provenance().wrapping_add(trailer_start))
}

fn get_layout_with_group_trailer(layout: Layout) -> Option<(Layout, usize)> {
    let (new_layout, trailer_start) = layout.extend(STATS_LAYOUT).ok()?;
    Some((new_layout.pad_to_align(), trailer_start))
}

#[cfg(test)]
mod tests {
    use std::{alloc::System, cell::Cell, thread};

    use super::*;
    use crate::resource_tracking::{ResourceGroupRegistry, ResourceStatsSnapshot};

    struct ReallocBackend {
        calls: Cell<usize>,
        fail: bool,
    }

    // SAFETY: This forwards valid allocations to System. A rejected resize leaves the block untouched.
    unsafe impl GlobalAlloc for ReallocBackend {
        unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
            // SAFETY: The caller supplies the layout expected by System.
            unsafe { System.alloc(layout) }
        }

        unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
            // SAFETY: The caller supplies a live System allocation with its layout.
            unsafe { System.dealloc(ptr, layout) }
        }

        unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
            self.calls.set(self.calls.get().wrapping_add(1));
            if self.fail {
                return ptr::null_mut();
            }
            // SAFETY: The caller supplies a live System allocation and a valid resize request.
            unsafe { System.realloc(ptr, layout, new_size) }
        }
    }

    #[track_caller]
    fn assert_counts(group: ResourceGroupToken, expected: (usize, usize, usize, usize)) {
        let stats = group.stats().snapshot_delta(&ResourceStatsSnapshot::empty());
        assert_eq!(
            (
                stats.allocated_bytes,
                stats.allocated_objects,
                stats.deallocated_bytes,
                stats.deallocated_objects
            ),
            expected
        );
    }

    #[test]
    fn realloc_keeps_owner_and_relocates_aligned_trailer() {
        let registry = ResourceGroupRegistry::global();
        let owner = registry.register_resource_group("realloc-owner");
        let other = registry.register_resource_group("realloc-other");
        let allocator = TrackingAllocator::new(ReallocBackend {
            calls: Cell::new(0),
            fail: false,
        });
        let mut layout = Layout::from_size_align(65, 64).unwrap();
        // SAFETY: All operations use the current layout. Successful resizes replace the pointer and layout.
        unsafe {
            let mut ptr = {
                let _owner = owner.enter();
                let ptr = allocator.alloc(layout);
                assert!(!ptr.is_null());
                ptr.write_bytes(0xA5, layout.size());
                ptr
            };
            let _other = other.enter();
            // The first growth moves only the trailer, not the backend allocation. The other resizes change both.
            for (size, calls, expected) in [
                (81, 0, (128, 1, 0, 0)),
                (129, 1, (192, 1, 0, 0)),
                (7, 2, (192, 1, 128, 0)),
            ] {
                ptr = allocator.realloc(ptr, layout, size);
                assert!(!ptr.is_null());
                assert_eq!(ptr.addr() % 64, 0);
                for i in 0..size.min(layout.size()) {
                    assert_eq!(ptr.add(i).read(), 0xA5);
                }
                assert_eq!(allocator.allocator.calls.get(), calls);
                assert_counts(owner, expected);
                // The caller can overwrite the old trailer's location after growth.
                ptr.write_bytes(0xA5, size);
                layout = Layout::from_size_align(size, 64).unwrap();
            }
            allocator.dealloc(ptr, layout);
        }
        assert_counts(owner, (192, 1, 192, 1));
        assert_counts(other, (0, 0, 0, 0));
    }

    #[test]
    fn failed_realloc_leaves_contents_and_counts_unchanged() {
        let owner = ResourceGroupRegistry::global().register_resource_group("failed-realloc-owner");
        let allocator = TrackingAllocator::new(ReallocBackend {
            calls: Cell::new(0),
            fail: true,
        });
        let layout = Layout::from_size_align(8, 8).unwrap();
        // SAFETY: Failed resize requests leave the original System block live with the original layout.
        unsafe {
            let ptr = {
                let _owner = owner.enter();
                allocator.alloc(layout)
            };
            assert!(!ptr.is_null());
            ptr.cast::<u64>().write(0xA5);

            // The requested size fits in a Layout, but a padded block with that size cannot fit.
            assert!(allocator.realloc(ptr, layout, isize::MAX as usize - 7).is_null());
            assert_eq!(allocator.allocator.calls.get(), 0);
            assert!(allocator.realloc(ptr, layout, 24).is_null());
            assert_eq!(allocator.allocator.calls.get(), 1);
            assert_eq!(ptr.cast::<u64>().read(), 0xA5);
            assert_counts(owner, (16, 1, 0, 0));
            allocator.dealloc(ptr, layout);
        }
        assert_counts(owner, (16, 1, 16, 1));
    }

    #[test]
    fn component_realloc_charges_only_the_size_change_to_its_owner() {
        static BACKEND: System = System;
        let registry = ResourceGroupRegistry::global();
        let owner = registry.register_resource_group("component-realloc-owner");
        let other = registry.register_resource_group("component-realloc-current-group");
        let allocator = ComponentAllocator {
            group: owner,
            backend: &BACKEND,
        };
        let other_guard = other.enter();
        let old_layout = Layout::from_size_align(8, 8).unwrap();
        let new_layout = Layout::from_size_align(24, 8).unwrap();

        // SAFETY: Each operation uses the current live pointer and layout.
        unsafe {
            let ptr = allocator.alloc(old_layout);
            assert!(!ptr.is_null());
            let resized = allocator.realloc(ptr, old_layout, new_layout.size());
            assert!(!resized.is_null());
            assert_counts(owner, (24, 1, 0, 0));
            allocator.dealloc(resized, new_layout);
        }
        assert_counts(owner, (24, 1, 24, 1));
        drop(other_guard);
        assert_counts(other, (0, 0, 0, 0));
    }

    #[test]
    fn foreign_thread_can_free_before_owner_batch_flushes() {
        let registry = ResourceGroupRegistry::global();
        let owner = registry.register_resource_group("cross-thread-allocation-owner");
        let other = registry.register_resource_group("cross-thread-free-group");
        let allocator = TrackingAllocator::new(System);
        let layout = Layout::from_size_align(8, 8).unwrap();
        let guard = owner.enter();
        // SAFETY: The layout has a nonzero size and valid alignment.
        let ptr = unsafe { allocator.alloc(layout) };
        assert!(!ptr.is_null());
        let address = ptr.expose_provenance();

        thread::scope(|scope| {
            scope.spawn(|| {
                let _other = other.enter();
                // SAFETY: This thread receives exclusive ownership of the live allocation and frees it once.
                unsafe { allocator.dealloc(ptr::with_exposed_provenance_mut(address), layout) };
            });
        });
        assert_counts(owner, (0, 0, 0, 0));
        drop(guard);
        assert_counts(owner, (16, 1, 16, 1));
        assert_counts(other, (0, 0, 0, 0));
    }

    #[test]
    fn allocation_batches_publish_at_size_and_object_limits() {
        let owner = ResourceGroupRegistry::global().register_resource_group("allocation-batch-limits");
        let allocator = TrackingAllocator::new(System);
        let _owner = owner.enter();
        let small = Layout::from_size_align(1, 64).unwrap();
        for i in 0..128 {
            // SAFETY: Each pointer is freed once with the same allocator and layout.
            unsafe {
                let ptr = allocator.alloc(small);
                assert!(!ptr.is_null());
                allocator.dealloc(ptr, small);
            }
            if i == 126 {
                assert_counts(owner, (0, 0, 0, 0));
            }
        }
        assert_counts(owner, (8192, 128, 8192, 128));

        let large = Layout::from_size_align(65536, 64).unwrap();
        // SAFETY: The pointer is freed once with the same allocator and layout.
        unsafe {
            let ptr = allocator.alloc(large);
            assert!(!ptr.is_null());
            assert_counts(owner, (73792, 129, 8192, 128));
            allocator.dealloc(ptr, large);
        }
        assert_counts(owner, (73792, 129, 73792, 129));
    }
}
