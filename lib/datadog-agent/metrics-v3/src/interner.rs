//! Exact dictionary indexing over the writer's contiguous column buffers.

use std::{
    any::Any,
    hash::{BuildHasher, Hash},
    sync::Arc,
};

use hashbrown::{hash_table::Entry, HashTable};

#[derive(Clone, Copy, Debug)]
struct Record {
    hash: u64,
    start: usize,
    len: usize,
    id: i64,
}

type SymbolHint = (usize, u32);
const SYMBOL_CACHE_SIZE: usize = 256;

#[derive(Clone, Copy, Debug)]
struct SymbolAlias {
    hint: SymbolHint,
    record: Record,
}

#[derive(Debug, Default)]
pub(crate) struct Interner {
    index: HashTable<Record>,
    hasher: foldhash::fast::RandomState,
    symbols: Option<Box<[Option<SymbolAlias>; SYMBOL_CACHE_SIZE]>>,
}

impl Interner {
    /// Returns a 1-based ID; appends the prefix and key only on a miss.
    pub fn get_or_insert<T: Copy + Eq + Hash>(
        &mut self, key: &[T], arena: &mut Vec<T>, prefix: impl FnOnce(&mut Vec<T>),
    ) -> (i64, bool) {
        let (record, is_new) = self.intern(key, arena, prefix);
        (record.id, is_new)
    }

    pub fn intern_symbol(
        &mut self, hint: SymbolHint, key: &[u8], arena: &mut Vec<u8>, prefix: impl FnOnce(&mut Vec<u8>),
    ) -> i64 {
        let (table, id) = hint;
        let slot = ((table >> 6) ^ id as usize ^ (id >> 28) as usize) % SYMBOL_CACHE_SIZE;
        if let Some(alias) = self.symbols.as_ref().and_then(|symbols| symbols[slot]) {
            let record = alias.record;
            // A symbol ID may be reclaimed while the writer is still live.
            if alias.hint == hint && arena[record.start..record.start + record.len] == *key {
                return record.id;
            }
        }

        let (record, _) = self.intern(key, arena, prefix);
        self.symbols.get_or_insert_with(|| Box::new([None; SYMBOL_CACHE_SIZE]))[slot] =
            Some(SymbolAlias { hint, record });
        record.id
    }

    fn intern<T: Copy + Eq + Hash>(
        &mut self, key: &[T], arena: &mut Vec<T>, prefix: impl FnOnce(&mut Vec<T>),
    ) -> (Record, bool) {
        let hash = self.hasher.hash_one(key);
        let next_id = self.index.len() as i64 + 1;
        match self.index.entry(
            hash,
            |record| record.hash == hash && arena[record.start..record.start + record.len] == *key,
            |record| record.hash,
        ) {
            Entry::Occupied(entry) => (*entry.get(), false),
            Entry::Vacant(entry) => {
                prefix(arena);
                let start = arena.len();
                arena.extend_from_slice(key);
                let record = Record {
                    hash,
                    start,
                    len: key.len(),
                    id: next_id,
                };
                entry.insert(record);
                (record, true)
            }
        }
    }

    pub fn clear(&mut self) {
        self.index.clear();
        if let Some(symbols) = &mut self.symbols {
            symbols.fill(None);
        }
    }
}

const PINNED_SYMBOL_SLOTS: usize = 512;
const PINNED_SYMBOL_PROBES: usize = 4;

pub(crate) type SymbolOwner = Arc<dyn Any + Send + Sync>;

#[derive(Debug)]
struct PinnedSymbol {
    hint: SymbolHint,
    address: usize,
    len: usize,
    id: i64,
    _owner: SymbolOwner,
}

#[derive(Debug, Default)]
pub(crate) struct PinnedSymbols {
    slots: Vec<Option<PinnedSymbol>>,
}

impl PinnedSymbols {
    pub fn intern(
        &mut self, hint: SymbolHint, text: &str, intern: impl FnOnce() -> i64, retain: impl FnOnce() -> SymbolOwner,
    ) -> i64 {
        if self.slots.is_empty() {
            self.slots.resize_with(PINNED_SYMBOL_SLOTS, || None);
        }
        let (_, symbol) = hint;
        let mixed = (symbol ^ symbol.rotate_right(15)).wrapping_mul(0x9e37_79b9);
        let start = (mixed >> (u32::BITS - PINNED_SYMBOL_SLOTS.ilog2())) as usize;
        let address = text.as_ptr().addr();
        let mut destination = start;
        for probe in 0..PINNED_SYMBOL_PROBES {
            let idx = (start + probe) & (PINNED_SYMBOL_SLOTS - 1);
            match &self.slots[idx] {
                Some(slot) if slot.hint == hint && slot.address == address && slot.len == text.len() => return slot.id,
                Some(_) => {}
                None => {
                    destination = idx;
                    break;
                }
            }
        }
        let id = intern();
        self.slots[destination] = Some(PinnedSymbol {
            hint,
            address,
            len: text.len(),
            id,
            _owner: retain(),
        });
        id
    }

    pub fn clear(&mut self) {
        self.slots.iter_mut().for_each(|slot| *slot = None);
    }
}

/// Avoids hashing the small, repeating resource and origin dictionaries common in a payload.
#[derive(Debug)]
pub(crate) struct RecentInterner<const N: usize> {
    interner: Interner,
    recent: [Option<Record>; N],
    next: usize,
}

impl<const N: usize> Default for RecentInterner<N> {
    fn default() -> Self {
        Self {
            interner: Interner::default(),
            recent: [None; N],
            next: 0,
        }
    }
}

impl<const N: usize> RecentInterner<N> {
    pub fn get_or_insert<T: Copy + Eq + Hash>(
        &mut self, key: &[T], arena: &mut Vec<T>, prefix: impl FnOnce(&mut Vec<T>),
    ) -> (i64, bool) {
        for record in self.recent.iter().flatten() {
            if record.len == key.len() && arena[record.start..record.start + record.len] == *key {
                return (record.id, false);
            }
        }
        let (record, is_new) = self.interner.intern(key, arena, prefix);
        self.recent[self.next] = Some(record);
        self.next = (self.next + 1) % N;
        (record.id, is_new)
    }

    pub fn clear(&mut self) {
        self.interner.clear();
        self.recent.fill(None);
        self.next = 0;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn repeated_keys_keep_dictionary_ids_after_leaving_recent_cache() {
        let mut index = RecentInterner::<2>::default();
        let mut arena = Vec::new();
        for key in 1..=3 {
            assert_eq!(index.get_or_insert(&[key], &mut arena, |_| {}), (key, true));
        }
        assert_eq!(index.get_or_insert(&[1], &mut arena, |_| {}), (1, false));
        assert_eq!(index.get_or_insert(&[1], &mut arena, |_| {}), (1, false));
        assert_eq!(arena, [1, 2, 3]);

        index.clear();
        arena.clear();
        assert_eq!(index.get_or_insert(&[3], &mut arena, |_| {}), (1, true));
        assert_eq!(arena, [3]);
    }

    #[test]
    fn interns_exact_slices_across_growth_and_clear() {
        let mut index = Interner::default();
        let mut arena = Vec::new();
        let prefix = |arena: &mut Vec<i64>| arena.push(-1);
        assert_eq!(index.get_or_insert(&[1, 2], &mut arena, prefix), (1, true));
        assert_eq!(index.get_or_insert(&[1], &mut arena, prefix), (2, true));
        for n in 3..256 {
            assert_eq!(index.get_or_insert(&[n, n + 1], &mut arena, prefix), (n, true));
        }
        assert_eq!(index.get_or_insert(&[1, 2], &mut arena, prefix), (1, false));
        assert_eq!(&arena[..5], &[-1, 1, 2, -1, 1]);

        index.clear();
        arena.clear();
        assert_eq!(index.get_or_insert(&[1], &mut arena, prefix), (1, true));
        assert_eq!(arena, [-1, 1]);
    }

    #[test]
    fn recycled_symbol_id_does_not_alias_a_different_string() {
        let mut index = Interner::default();
        let mut arena = Vec::new();
        let hint = (11, 123);
        assert_eq!(index.intern_symbol(hint, b"old", &mut arena, |_| {}), 1);
        assert_eq!(index.intern_symbol(hint, b"new", &mut arena, |_| {}), 2);
        assert_eq!(index.get_or_insert(b"old", &mut arena, |_| {}), (1, false));
        assert_eq!(index.intern_symbol(hint, b"new", &mut arena, |_| {}), 2);
        assert_eq!(arena, b"oldnew");
    }
}
