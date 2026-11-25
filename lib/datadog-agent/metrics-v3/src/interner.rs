//! Exact dictionary indexing over the writer's contiguous column buffers.

use std::hash::{BuildHasher, Hash};

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

#[cfg(test)]
mod tests {
    use super::*;

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
