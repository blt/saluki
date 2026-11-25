use std::{
    mem,
    sync::{Arc, Mutex, Weak},
};

use bytes::BytesMut;

// Leave room for the tracking allocator's trailer without crossing the 1 MiB size class.
const MAX_SLAB_BYTES: usize = 1024 * 1024 - 8;
const SLOT_ALIGNMENT: usize = 8;

pub(super) struct BufferSlabs {
    slabs: Mutex<Vec<Weak<Slab>>>,
    capacity: usize,
    stride: usize,
    slots: usize,
}

impl BufferSlabs {
    pub(super) fn new(capacity: usize, max_buffers: usize) -> Option<Self> {
        let stride = capacity.checked_next_multiple_of(SLOT_ALIGNMENT)?;
        let slots = (MAX_SLAB_BYTES.checked_div(stride)?).min(max_buffers);
        (slots > 1).then(|| Self {
            slabs: Mutex::new(Vec::new()),
            capacity,
            stride,
            slots,
        })
    }

    pub(super) fn take(&self) -> (BytesMut, Arc<Slab>) {
        let mut slabs = self.slabs.lock().unwrap();
        // Keep the last slab with a free slot at the end of the list.
        for idx in (0..slabs.len()).rev() {
            if let Some(slab) = slabs[idx].upgrade() {
                if let Some(buffer) = slab.take() {
                    let last = slabs.len() - 1;
                    slabs.swap(idx, last);
                    return (buffer, slab);
                }
            }
        }

        slabs.retain(|slab| slab.strong_count() != 0);
        let mut remaining = BytesMut::with_capacity(self.stride * self.slots);
        let mut free = Vec::with_capacity(self.slots);
        for _ in 0..self.slots {
            let tail = remaining.split_off(self.stride);
            let mut slot = mem::replace(&mut remaining, tail);
            if self.capacity != self.stride {
                drop(slot.split_off(self.capacity));
            }
            free.push(slot);
        }

        let buffer = free.pop().expect("a new slab has free slots");
        let slab = Arc::new(Slab { free: Mutex::new(free) });
        slabs.push(Arc::downgrade(&slab));
        (buffer, slab)
    }
}

// The builder holds only weak references. The last live buffer drops the slab and all free slices.
pub(super) struct Slab {
    free: Mutex<Vec<BytesMut>>,
}

impl Slab {
    fn take(&self) -> Option<BytesMut> {
        self.free.lock().unwrap().pop()
    }

    pub(super) fn release(&self, mut buffer: BytesMut) {
        buffer.clear();
        self.free.lock().unwrap().push(buffer);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reuses_released_slots_and_drops_empty_slabs() {
        let slabs = BufferSlabs::new(8196, 2).unwrap();
        let (first, first_slab) = slabs.take();
        let (second, second_slab) = slabs.take();
        assert!(Arc::ptr_eq(&first_slab, &second_slab));

        let (third, third_slab) = slabs.take();
        assert!(!Arc::ptr_eq(&first_slab, &third_slab));

        let retired = Arc::downgrade(&third_slab);
        third_slab.release(third);
        drop(third_slab);
        assert!(retired.upgrade().is_none());

        let first_ptr = first.as_ptr();
        first_slab.release(first);
        let (reused, reused_slab) = slabs.take();
        assert!(Arc::ptr_eq(&first_slab, &reused_slab));
        assert_eq!(reused.as_ptr(), first_ptr);
        assert_eq!(second.capacity(), 8196);
    }
}
