use std::{
    cell::{RefCell, UnsafeCell},
    hash::BuildHasher,
    mem,
    ops::Deref,
    ptr, slice, str,
    sync::{
        atomic::{AtomicPtr, AtomicU32, AtomicU64, AtomicUsize, Ordering},
        RwLock,
    },
};

use foldhash::fast::RandomState;
use hashbrown::HashTable;

use super::helpers::{ReclaimedEntries, ReclaimedEntry};

const SHARD_BITS: u32 = 4;
const SHARD_COUNT: usize = 1 << SHARD_BITS;
const CHUNK_BYTES: usize = 16 * 1024;
// A gap between chunks prevents their free ranges from merging.
const CHUNK_STRIDE: usize = CHUNK_BYTES * 2;
const RECORD_ALIGN: usize = mem::align_of::<Record>();
const HEADER_BYTES: usize = mem::size_of::<Record>();
const SHARD_ADDRESS_BYTES: usize = (1usize << (32 - SHARD_BITS)) * RECORD_ALIGN;
const MAX_SHARD_BYTES: usize = SHARD_ADDRESS_BYTES / 2;
const WITNESS_COUNT: usize = 256;
const WITNESS_BYTES: usize = 256;
const FILTER_WORDS: usize = 64;
const MIN_FILTER_REFRESH_REMOVALS: usize = 64;

static NEXT_TABLE: AtomicUsize = AtomicUsize::new(1);

thread_local! {
    static WITNESSES: RefCell<[Witness; WITNESS_COUNT]> = const { RefCell::new([Witness::EMPTY; WITNESS_COUNT]) };
}

#[derive(Clone, Copy)]
struct Witness {
    table: usize,
    hash: u64,
    len: usize,
    bytes: [u8; WITNESS_BYTES],
}

impl Witness {
    const EMPTY: Self = Self {
        table: 0,
        hash: 0,
        len: 0,
        bytes: [0; WITNESS_BYTES],
    };

    fn seen(table: usize, hash: u64, value: &str) -> bool {
        WITNESSES.with_borrow_mut(|slots| {
            let slot = &mut slots[hash as usize % WITNESS_COUNT];
            if slot.hash == hash && slot.table == table && slot.bytes[..slot.len] == *value.as_bytes() {
                return true;
            }
            slot.table = table;
            slot.hash = hash;
            slot.len = value.len();
            slot.bytes[..slot.len].copy_from_slice(value.as_bytes());
            false
        })
    }
}

#[repr(C)]
struct Record {
    refs: AtomicU32,
    len: u32,
}

impl Record {
    fn allocation_size(len: usize) -> usize {
        (HEADER_BYTES + len).next_multiple_of(RECORD_ALIGN)
    }

    // SAFETY: The caller owns a live reference to this record and its bytes.
    unsafe fn value<'a>(record: *const Self) -> &'a str {
        let bytes = slice::from_raw_parts(record.add(1).cast::<u8>(), (*record).len as usize);
        str::from_utf8_unchecked(bytes)
    }
}

struct Shard {
    // Chunks never move or deallocate while the table is live. Records have independent lifetimes.
    chunks: Box<[AtomicPtr<UnsafeCell<u32>>]>,
    capacity: usize,
    // False positives take a read lock but cannot admit a first observation.
    resident: [AtomicU64; FILTER_WORDS],
    state: RwLock<ShardState>,
}

struct ShardState {
    entries: HashTable<u32>,
    reclaimed: ReclaimedEntries,
    next: usize,
    used: usize,
    removed_since_refresh: usize,
}

impl Shard {
    fn new(capacity: usize) -> Self {
        Self {
            chunks: (0..capacity.div_ceil(CHUNK_BYTES))
                .map(|_| AtomicPtr::new(ptr::null_mut()))
                .collect(),
            capacity,
            resident: [const { AtomicU64::new(0) }; FILTER_WORDS],
            state: RwLock::new(ShardState {
                entries: HashTable::new(),
                reclaimed: ReclaimedEntries::new(),
                next: 0,
                used: 0,
                removed_since_refresh: 0,
            }),
        }
    }

    fn chunk_len(&self, index: usize) -> usize {
        (self.capacity - index * CHUNK_BYTES).min(CHUNK_BYTES)
    }

    fn record(&self, offset: usize) -> *mut Record {
        let chunk = self.chunks[offset / CHUNK_STRIDE].load(Ordering::Acquire);
        // SAFETY: Live and reserved offsets refer to an allocated range in this chunk.
        unsafe { chunk.cast::<u8>().add(offset % CHUNK_STRIDE).cast() }
    }

    fn reserve(&self, state: &mut ShardState, size: usize) -> Option<usize> {
        if let Some(mut free) = state.reclaimed.take_if(|free| free.capacity() >= size) {
            if free.capacity() > size {
                state.reclaimed.insert(free.split_off(size));
            }
            return Some(free.offset());
        }

        let mut chunk = state.next / CHUNK_STRIDE;
        if chunk >= self.chunks.len() {
            return None;
        }
        let mut position = state.next % CHUNK_STRIDE;
        let remaining = self.chunk_len(chunk) - position;
        if size > remaining {
            if position == 0 {
                return None;
            }
            if remaining > 0 {
                state.reclaimed.insert(ReclaimedEntry::new(state.next, remaining));
            }
            chunk += 1;
            position = 0;
            state.next = chunk * CHUNK_STRIDE;
            if chunk >= self.chunks.len() || size > self.chunk_len(chunk) {
                return None;
            }
        }

        let slot = &self.chunks[chunk];
        if slot.load(Ordering::Relaxed).is_null() {
            let storage: Box<[_]> = (0..self.chunk_len(chunk) / RECORD_ALIGN)
                .map(|_| UnsafeCell::new(0))
                .collect();
            slot.store(Box::into_raw(storage).cast::<UnsafeCell<u32>>(), Ordering::Release);
        }
        let offset = chunk * CHUNK_STRIDE + position;
        state.next = offset + size;
        Some(offset)
    }

    fn filter_location(hash: u64) -> (usize, u64) {
        let index = (hash >> 32) as usize % FILTER_WORDS;
        let bits = (1 << (hash & 63)) | (1 << ((hash >> 16) & 63));
        (index, bits)
    }

    fn refresh_filter(&self, state: &mut ShardState, hasher: &RandomState) {
        state.removed_since_refresh += 1;
        if state.removed_since_refresh < (state.entries.len() / 2).max(MIN_FILTER_REFRESH_REMOVALS) {
            return;
        }
        // Publish complete words so no live entry temporarily loses its bits.
        let mut words = [0; FILTER_WORDS];
        for &offset in &state.entries {
            // SAFETY: The shard write lock keeps indexed records live.
            let text = unsafe { Record::value(self.record(offset as usize)) };
            let (index, bits) = Self::filter_location(hasher.hash_one(text));
            words[index] |= bits;
        }
        for (slot, word) in self.resident.iter().zip(words) {
            slot.store(word, Ordering::Release);
        }
        state.removed_since_refresh = 0;
    }
}

impl Drop for Shard {
    fn drop(&mut self) {
        for (index, slot) in self.chunks.iter().enumerate() {
            let chunk = slot.load(Ordering::Relaxed);
            if !chunk.is_null() {
                // SAFETY: This is the original chunk allocation. No symbol can outlive the table.
                unsafe {
                    drop(Box::from_raw(ptr::slice_from_raw_parts_mut(
                        chunk,
                        self.chunk_len(index) / RECORD_ALIGN,
                    )))
                };
            }
        }
    }
}

/// A bounded string dictionary with exact, two-hit admission and lock-free ID reads.
///
/// IDs stay stable while a reference is live. Their storage is available for reuse when the last reference drops.
/// Admission requires two observations in the same thread-local window. Strings longer than 256 bytes stay out of the
/// dictionary.
pub struct SymbolTable {
    table_id: usize,
    hasher: RandomState,
    shards: [Shard; SHARD_COUNT],
}

impl SymbolTable {
    /// Creates a table with at most `capacity` bytes of string records, distributed across 16 shards.
    ///
    /// Index and admission-filter storage are separate. Record chunks are allocated on demand.
    pub fn new(capacity: usize) -> Self {
        let units = (capacity / RECORD_ALIGN).min(MAX_SHARD_BYTES / RECORD_ALIGN * SHARD_COUNT);
        Self {
            table_id: NEXT_TABLE.fetch_add(1, Ordering::Relaxed),
            hasher: RandomState::default(),
            shards: std::array::from_fn(|i| {
                Shard::new((units / SHARD_COUNT + usize::from(i < units % SHARD_COUNT)) * RECORD_ALIGN)
            }),
        }
    }

    /// Returns the number of active symbols.
    pub fn len(&self) -> usize {
        self.shards
            .iter()
            .map(|shard| shard.state.read().unwrap().entries.len())
            .sum()
    }

    /// Returns whether the dictionary has any active symbols.
    pub fn is_empty(&self) -> bool {
        self.shards
            .iter()
            .all(|shard| shard.state.read().unwrap().entries.is_empty())
    }

    /// Returns the number of bytes occupied by active records, including their headers and padding.
    pub fn len_bytes(&self) -> usize {
        self.shards.iter().map(|shard| shard.state.read().unwrap().used).sum()
    }

    /// Returns the capacity of the table in record bytes.
    pub fn capacity_bytes(&self) -> usize {
        self.shards.iter().map(|shard| shard.capacity).sum()
    }

    #[inline]
    fn split_id(&self, id: u32) -> (&Shard, usize) {
        let shard = &self.shards[(id >> (32 - SHARD_BITS)) as usize];
        let offset = id as usize % (SHARD_ADDRESS_BYTES / RECORD_ALIGN) * RECORD_ALIGN;
        (shard, offset)
    }

    #[inline]
    fn make_id(shard: usize, offset: usize) -> u32 {
        ((shard << (32 - SHARD_BITS)) | (offset / RECORD_ALIGN)) as u32
    }

    /// Returns the existing symbol, or admits a repeat if there is space.
    ///
    /// A first observation never takes a dictionary write lock.
    pub fn try_intern(&self, value: &str) -> Option<Symbol<'_>> {
        if value.len() > WITNESS_BYTES {
            return None;
        }
        let hash = self.hasher.hash_one(value);
        let shard_index = (hash >> (64 - SHARD_BITS)) as usize;
        let shard = &self.shards[shard_index];
        let size = Record::allocation_size(value.len());
        if size > shard.capacity {
            return None;
        }
        let (index, bits) = Shard::filter_location(hash);
        let filter = &shard.resident[index];
        let matches = |offset: &u32| {
            // SAFETY: The shard lock keeps all indexed records live during lookup.
            unsafe { Record::value(shard.record(*offset as usize)) == value }
        };
        if filter.load(Ordering::Acquire) & bits == bits {
            let state = shard.state.read().unwrap();
            if let Some(&offset) = state.entries.find(hash, matches) {
                let id = Self::make_id(shard_index, offset as usize);
                // SAFETY: The shard read lock prevents the record's last reference from dropping.
                unsafe { self.retain(id) };
                return Some(Symbol { table: self, id });
            }
        }
        if !Witness::seen(self.table_id, hash, value) {
            return None;
        }

        let mut state = shard.state.write().unwrap();
        if let Some(&offset) = state.entries.find(hash, matches) {
            let id = Self::make_id(shard_index, offset as usize);
            // SAFETY: The shard write lock prevents the record's last reference from dropping.
            unsafe { self.retain(id) };
            return Some(Symbol { table: self, id });
        }
        let offset = shard.reserve(&mut state, size)?;
        let record = shard.record(offset);
        // SAFETY: This range is reserved under the write lock, aligned, and large enough for the value.
        unsafe {
            record.write(Record {
                refs: AtomicU32::new(1),
                len: value.len() as u32,
            });
            record
                .add(1)
                .cast::<u8>()
                .copy_from_nonoverlapping(value.as_ptr(), value.len());
        }
        state.entries.insert_unique(hash, offset as u32, |&offset| {
            // SAFETY: Only live records are in the index.
            self.hasher
                .hash_one(unsafe { Record::value(shard.record(offset as usize)) })
        });
        state.used += size;
        filter.fetch_or(bits, Ordering::Release);
        Some(Symbol {
            table: self,
            id: Self::make_id(shard_index, offset),
        })
    }

    /// Borrows a string by ID without taking a lock.
    ///
    /// # Safety
    /// `id` must belong to this table, and an owned reference must remain live for the entire returned borrow.
    #[inline]
    pub unsafe fn get_unchecked(&self, id: u32) -> &str {
        let (shard, offset) = self.split_id(id);
        Record::value(shard.record(offset))
    }

    /// Adds one owned reference to a live ID.
    ///
    /// # Safety
    /// `id` must have a live reference in this table for the duration of the call. Release the new reference once.
    #[inline]
    pub unsafe fn retain(&self, id: u32) {
        let (shard, offset) = self.split_id(id);
        if (*shard.record(offset)).refs.fetch_add(1, Ordering::Relaxed) >= i32::MAX as u32 {
            std::process::abort();
        }
    }

    /// Releases one owned reference to an ID.
    ///
    /// # Safety
    /// The caller must own a reference to `id` in this table, and must not use borrows that depend on it afterwards.
    pub unsafe fn release(&self, id: u32) {
        let (shard, offset) = self.split_id(id);
        let record = shard.record(offset);
        let refs = &(*record).refs;
        // Only the last reference needs the shard lock. The count remains nonzero until that lock is held.
        if refs
            .fetch_update(Ordering::Release, Ordering::Relaxed, |n| (n > 1).then(|| n - 1))
            .is_ok()
        {
            return;
        }
        let mut state = shard.state.write().unwrap();
        if refs.fetch_sub(1, Ordering::AcqRel) != 1 {
            return;
        }
        let value = Record::value(record);
        let hash = self.hasher.hash_one(value);
        let size = Record::allocation_size(value.len());
        state
            .entries
            .find_entry(hash, |entry| *entry as usize == offset)
            .unwrap()
            .remove();
        state.used -= size;
        state.reclaimed.insert(ReclaimedEntry::new(offset, size));
        shard.refresh_filter(&mut state, &self.hasher);
    }
}

/// An owned reference to an interned symbol, tied to the lifetime of its table.
pub struct Symbol<'a> {
    table: &'a SymbolTable,
    id: u32,
}

impl Symbol<'_> {
    /// Returns the ID of this symbol in its table.
    pub fn id(&self) -> u32 {
        self.id
    }

    /// Transfers this reference to an ID. The caller must eventually release it on the same table.
    pub fn into_id(self) -> u32 {
        let id = self.id;
        mem::forget(self);
        id
    }
}

impl Deref for Symbol<'_> {
    type Target = str;

    fn deref(&self) -> &str {
        // SAFETY: The reference owned by this symbol outlives the borrow.
        unsafe { self.table.get_unchecked(self.id) }
    }
}

impl Clone for Symbol<'_> {
    fn clone(&self) -> Self {
        // SAFETY: This symbol owns a live reference.
        unsafe { self.table.retain(self.id) };
        Self {
            table: self.table,
            id: self.id,
        }
    }
}

impl Drop for Symbol<'_> {
    fn drop(&mut self) {
        // SAFETY: This symbol owns the reference it releases.
        unsafe { self.table.release(self.id) };
    }
}

#[cfg(test)]
mod tests {
    use std::thread;

    use super::*;

    #[test]
    fn singletons_leave_space_for_late_shared_symbols() {
        let table = SymbolTable::new(8 * 1024 * 1024);
        for n in 0..200_000 {
            let value = format!("correlation_id:12345678-1234-1234-1234-{n:012x}");
            assert_eq!(value.len(), 51);
            assert!(table.try_intern(&value).is_none());
        }
        assert_eq!(table.len_bytes(), 0);
        assert!(table
            .shards
            .iter()
            .all(|shard| shard.chunks.iter().all(|slot| slot.load(Ordering::Relaxed).is_null())));

        let shared = "deployment:service-shared-across-context";
        assert_eq!(shared.len(), 40);
        assert!(table.try_intern(shared).is_none());
        let a = table.try_intern(shared).unwrap();
        let b = table.try_intern(shared).unwrap();
        assert_eq!(a.id(), b.id());
        assert_eq!(&*b, shared);
        assert_eq!(table.len(), 1);
    }

    #[test]
    fn concurrent_id_reads_do_not_take_the_shard_lock() {
        let value = "deployment:read-while-the-writer-holds-the-shard";
        let table = SymbolTable::new(64 * 1024);
        assert!(table.try_intern(value).is_none());
        let symbol = table.try_intern(value).unwrap();
        let (shard, _) = table.split_id(symbol.id());
        let _writer = shard.state.write().unwrap();
        thread::scope(|scope| {
            for _ in 0..4 {
                scope.spawn(|| {
                    for _ in 0..10_000 {
                        assert_eq!(&*symbol, value);
                    }
                });
            }
        });
    }

    #[test]
    fn churn_clears_stale_bits_without_hiding_live_symbols() {
        let table = SymbolTable::new(64 * 1024);
        let value = "deployment:highly-reused";
        assert!(table.try_intern(value).is_none());
        let live = table.try_intern(value).unwrap();
        let shard_id = live.id() >> (32 - SHARD_BITS);

        let churn = (0..).map(|n| format!("deployment:one-live-at-a-time:{n}"));
        for text in churn
            .filter(|value| (table.hasher.hash_one(value.as_str()) >> (64 - SHARD_BITS)) as u32 == shard_id)
            .take(2 * MIN_FILTER_REFRESH_REMOVALS)
        {
            assert!(table.try_intern(&text).is_none());
            drop(table.try_intern(&text).unwrap());
        }

        let (shard, _) = table.split_id(live.id());
        let occupied: u32 = shard
            .resident
            .iter()
            .map(|word| word.load(Ordering::Relaxed).count_ones())
            .sum();
        assert!((1..=2).contains(&occupied));
        thread::scope(|scope| {
            scope
                .spawn(|| assert_eq!(table.try_intern(value).unwrap().id(), live.id()))
                .join()
                .unwrap();
        });
    }

    #[test]
    fn final_reference_reclaims_the_exact_record() {
        let value = "deployment:";
        let table = SymbolTable::new(2048);
        assert!(table.try_intern(value).is_none());
        let a = table.try_intern(value).unwrap();
        let id = a.id();
        let b = a.clone();
        let bytes = table.len_bytes();
        drop(a);
        assert_eq!(table.len_bytes(), bytes);
        assert_eq!(&*b, value);
        drop(b);
        assert!(table.is_empty());
        assert_eq!(table.len_bytes(), 0);
        let next = table.try_intern(value).unwrap();
        assert_eq!(next.id(), id);
        assert_eq!(table.len_bytes(), bytes);
    }
}
