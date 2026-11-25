use std::{
    hash::BuildHasher as _,
    num::NonZeroUsize,
    sync::{
        atomic::{AtomicU32, Ordering::Relaxed},
        Arc, Mutex, RwLock,
    },
    time::{Duration, Instant},
};

use hashbrown::HashTable;
use saluki_common::{hash::NoopU64BuildHasher, sync::shutdown::ShutdownHandle};
use tokio::{pin, select, time::sleep};

use super::{super::packed::WeakPacked, Context, ContextKey};
use crate::{
    cache::Telemetry,
    runtime::{self, ScopeGuard},
};

const MAX_SHARDS: usize = 64;
const MIN_SHARD_CAPACITY: usize = 32;
const EVICTION_SAMPLE_SIZE: usize = 8;
const SWEEP_INTERVAL: Duration = Duration::from_secs(1);
// The high bit selects which slot vector the index entry points to.
const LIVE_BIT: u32 = 1 << 31;
const SHARD_ENTRY_LIMIT: usize = LIVE_BIT as usize;
const LIVE_PROBE_BUDGET: usize = 2;
const LIVE_SWEEP_MINIMUM: usize = 64;

#[derive(Clone)]
pub(super) struct ContextCache {
    state: Arc<State>,
    _drivers: Option<Arc<ScopeGuard>>,
}

impl ContextCache {
    pub(super) fn new(
        identifier: String, limit: NonZeroUsize, idle: Option<Duration>, telemetry_enabled: bool,
    ) -> Self {
        let limit = limit.get();
        let shard_count = 1 << (limit / MIN_SHARD_CAPACITY).clamp(1, MAX_SHARDS).ilog2();
        let shards = (0..shard_count)
            .map(|i| {
                let limit = limit / shard_count + usize::from(i < limit % shard_count);
                AlignedShard(RwLock::new(Shard::new(limit.min(SHARD_ENTRY_LIMIT))))
            })
            .collect();
        let telemetry = Telemetry::new(identifier.clone());
        telemetry.weight_limit().set(limit as f64);
        let state = Arc::new(State {
            shards,
            start: Instant::now(),
            telemetry,
            telemetry_update: Mutex::new(()),
            telemetry_enabled,
        });

        let drivers = (idle.is_some() || telemetry_enabled).then(|| {
            let mut drivers = runtime::scope::nested_or_detached(format!("cache_{identifier}"));
            if let Some(idle) = idle {
                let state = Arc::clone(&state);
                drivers.spawn(
                    runtime::worker_with_shutdown("expiration", move |shutdown| {
                        Arc::clone(&state).drive_expiration(idle, shutdown)
                    })
                    .build(),
                );
            }
            if telemetry_enabled {
                let state = Arc::clone(&state);
                drivers.spawn(
                    runtime::worker_with_shutdown("telemetry", move |shutdown| {
                        Arc::clone(&state).drive_telemetry(idle.is_none(), shutdown)
                    })
                    .build(),
                );
            }
            Arc::new(drivers)
        });

        Self {
            state,
            _drivers: drivers,
        }
    }

    /// Returns the context and whether it already exists.
    pub(super) fn get_or_insert_with(
        &self, key: ContextKey, create: impl FnOnce() -> Option<Context>,
    ) -> Option<(Context, bool)> {
        let state = &self.state;
        let hash = index_hash(key);
        let shard = &state.shards[hash.rotate_left(16) as usize & (state.shards.len() - 1)].0;
        {
            let shard = shard.read().unwrap();
            if let Some(slot) = shard.get(key, hash) {
                slot.touch(epoch(state.start.elapsed()));
                state.telemetry.hits_total().increment(1);
                return Some((slot.context.clone(), true));
            }
        }

        let mut shard = shard.write().unwrap();
        let position = shard.position(key, hash);
        if let Some(position) = position.filter(|&i| i & LIVE_BIT == 0) {
            let slot = &shard.slots[position as usize];
            slot.touch(epoch(state.start.elapsed()));
            state.telemetry.hits_total().increment(1);
            return Some((slot.context.clone(), true));
        }

        state.telemetry.misses_total().increment(1);
        let live = position.and_then(|i| shard.take_live(i, hash));
        if !shard.live.is_empty() {
            shard.reap_live(LIVE_PROBE_BUDGET);
        }
        let (context, existing) = if let Some(context) = live {
            (context, true)
        } else {
            // Every resident entry may need a live slot when the cache evicts it.
            if shard.index.len() == SHARD_ENTRY_LIMIT {
                return None;
            }
            (create()?, false)
        };
        let evicted = shard.insert(key, hash, context.clone(), epoch(state.start.elapsed()));
        state.telemetry.items_inserted_total().increment(1);
        if evicted && state.telemetry_enabled {
            state.telemetry.items_evicted_total().increment(1);
        }
        Some((context, existing))
    }

    /// Removes every resident whose only strong reference is the cache.
    pub(super) fn evict_idle_for_budget(&self) -> usize {
        let state = &self.state;
        let _telemetry = state.telemetry_update.lock().unwrap();
        let mut evicted = 0;
        let mut remaining = 0;
        for shard in &state.shards {
            let mut shard = shard.0.write().unwrap();
            evicted += shard.evict_idle_for_budget();
            remaining += shard.slots.len();
        }
        if state.telemetry_enabled {
            state.telemetry.current_items().set(remaining as f64);
            state.telemetry.current_weight().set(remaining as f64);
            state.telemetry.items_evicted_total().increment(evicted as u64);
        }
        evicted
    }
}

struct State {
    shards: Box<[AlignedShard]>,
    start: Instant,
    telemetry: Telemetry,
    telemetry_update: Mutex<()>,
    telemetry_enabled: bool,
}

impl State {
    async fn drive_expiration(self: Arc<Self>, idle: Duration, shutdown: ShutdownHandle) {
        pin!(shutdown);
        loop {
            select! {
                _ = &mut shutdown => break,
                _ = sleep(SWEEP_INTERVAL) => {}
            }

            let cutoff = expiration_cutoff(self.start.elapsed(), idle);
            let mut expired = 0;
            for shard in &self.shards {
                let (count, reaped) = shard.0.write().unwrap().sweep(cutoff);
                expired += count;
                if count != 0 || reaped != 0 {
                    tokio::task::yield_now().await;
                }
            }
            if expired != 0 {
                self.telemetry.items_expired_total().increment(expired as u64);
                self.telemetry.items_removed_total().increment(expired as u64);
                self.telemetry.items_expired_batch_size().record(expired as f64);
            }
        }
    }

    async fn drive_telemetry(self: Arc<Self>, sweep_live: bool, shutdown: ShutdownHandle) {
        pin!(shutdown);
        loop {
            select! {
                _ = &mut shutdown => break,
                _ = sleep(SWEEP_INTERVAL) => {}
            }

            let _update = self.telemetry_update.lock().unwrap();
            let items: usize = self
                .shards
                .iter()
                .map(|shard| {
                    let read = shard.0.read().unwrap();
                    if !sweep_live || read.live.is_empty() {
                        return read.slots.len();
                    }
                    drop(read);
                    let mut write = shard.0.write().unwrap();
                    write.sweep(None);
                    write.slots.len()
                })
                .sum();
            self.telemetry.current_items().set(items as f64);
            self.telemetry.current_weight().set(items as f64);
        }
    }
}

fn epoch(elapsed: Duration) -> u32 {
    u32::try_from(elapsed.as_secs()).unwrap_or(u32::MAX)
}

fn expiration_cutoff(elapsed: Duration, idle: Duration) -> Option<u32> {
    elapsed.checked_sub(idle).map(epoch)
}

fn index_hash(key: ContextKey) -> u64 {
    // Keep the shard bits out of the table's bucket and control-byte selection.
    NoopU64BuildHasher.hash_one(key).rotate_right(16)
}

#[repr(align(64))]
struct AlignedShard(RwLock<Shard>);

struct Slot {
    key: ContextKey,
    context: Context,
    last_seen: AtomicU32,
}

impl Slot {
    fn touch(&self, now: u32) {
        let _ = self
            .last_seen
            .fetch_update(Relaxed, Relaxed, |seen| (seen < now).then_some(now));
    }
}

struct LiveSlot {
    key: ContextKey,
    context: WeakPacked,
}

struct Shard {
    slots: Vec<Slot>,
    live: Vec<LiveSlot>,
    index: HashTable<u32>,
    limit: usize,
    next_eviction: usize,
    next_live_probe: usize,
}

impl Shard {
    fn new(limit: usize) -> Self {
        Self {
            slots: Vec::new(),
            live: Vec::new(),
            index: HashTable::new(),
            limit,
            next_eviction: 0,
            next_live_probe: 0,
        }
    }

    fn position(&self, key: ContextKey, hash: u64) -> Option<u32> {
        self.index
            .find(hash, |&i| slot_key(&self.slots, &self.live, i) == key)
            .copied()
    }

    fn get(&self, key: ContextKey, hash: u64) -> Option<&Slot> {
        let position = self.position(key, hash)?;
        self.slots.get(position as usize)
    }

    fn index_slot(&mut self, hash: u64, position: u32) {
        self.index
            .insert_unique(hash, position, |&i| index_hash(slot_key(&self.slots, &self.live, i)));
    }

    fn take_live(&mut self, position: u32, hash: u64) -> Option<Context> {
        let LiveSlot { context, .. } = self.remove_live(position, hash);
        context.upgrade().map(|inner| Context { inner })
    }

    fn insert(&mut self, key: ContextKey, hash: u64, context: Context, now: u32) -> bool {
        let evicted = self.slots.len() == self.limit;
        if evicted {
            let victim = self.select_victim();
            self.remove(victim);
        }
        let position = self.slots.len() as u32;
        self.slots.push(Slot {
            key,
            context,
            last_seen: AtomicU32::new(now),
        });
        self.index_slot(hash, position);
        evicted
    }

    fn select_victim(&mut self) -> usize {
        let len = self.slots.len();
        let start = self.next_eviction % len;
        let sample = len.min(EVICTION_SAMPLE_SIZE);
        self.next_eviction = start + sample;
        // Inspect a few entries so a busy context is less likely to lose its slot to a one-off context.
        (start..start + sample)
            .map(|i| i % len)
            .min_by_key(|&i| self.slots[i].last_seen.load(Relaxed))
            .unwrap()
    }

    fn remove(&mut self, position: usize) {
        let slot = &self.slots[position];
        let hash = index_hash(slot.key);
        let entry = self.index.find_entry(hash, |&i| i == position as u32).unwrap();
        // Keep weak pointers only for contexts that another owner can still use.
        if slot.context.inner.strong_count() > 1 {
            let live_position = self.live.len() as u32 | LIVE_BIT;
            self.live.push(LiveSlot {
                key: slot.key,
                context: slot.context.inner.downgrade(),
            });
            *entry.into_mut() = live_position;
        } else {
            entry.remove();
        }
        swap_remove_slot(&mut self.slots, &mut self.index, position as u32, |slot| slot.key);
    }

    fn remove_live(&mut self, position: u32, hash: u64) -> LiveSlot {
        self.index.find_entry(hash, |&i| i == position).unwrap().remove();
        swap_remove_slot(&mut self.live, &mut self.index, position, |slot| slot.key)
    }

    fn reap_live(&mut self, budget: usize) -> usize {
        let before = self.live.len();
        for _ in 0..before.min(budget) {
            if self.next_live_probe >= self.live.len() {
                self.next_live_probe = 0;
            }
            let slot = &self.live[self.next_live_probe];
            if slot.context.strong_count() == 0 {
                self.remove_live(self.next_live_probe as u32 | LIVE_BIT, index_hash(slot.key));
            } else {
                self.next_live_probe += 1;
            }
        }
        before - self.live.len()
    }

    fn sweep(&mut self, cutoff: Option<u32>) -> (usize, usize) {
        let expired = cutoff.map_or(0, |cutoff| self.expire(cutoff));
        // Check at least a quarter of the live slots during each sweep.
        let reaped = self.reap_live(self.live.len().div_ceil(4).max(LIVE_SWEEP_MINIMUM));
        self.trim();
        (expired, reaped)
    }

    fn evict_idle_for_budget(&mut self) -> usize {
        let before = self.slots.len();
        let mut i = 0;
        while let Some(slot) = self.slots.get(i) {
            if slot.context.inner.strong_count() == 1 {
                self.remove(i);
            } else {
                i += 1;
            }
        }
        self.reap_live(self.live.len());
        self.trim();
        before - self.slots.len()
    }

    fn trim(&mut self) {
        if self.slots.len() <= self.slots.capacity() / 4 {
            self.slots.shrink_to(self.slots.len() * 2);
        }
        if self.live.len() <= self.live.capacity() / 4 {
            self.live.shrink_to(self.live.len() * 2);
        }
        if self.index.len() <= self.index.capacity() / 4 {
            self.index
                .shrink_to_fit(|&i| index_hash(slot_key(&self.slots, &self.live, i)));
        }
    }

    fn expire(&mut self, cutoff: u32) -> usize {
        let before = self.slots.len();
        let mut i = 0;
        while let Some(slot) = self.slots.get_mut(i) {
            // Retain the cutoff epoch because the exact access time is unknown.
            if *slot.last_seen.get_mut() < cutoff {
                self.remove(i);
            } else {
                i += 1;
            }
        }
        before - self.slots.len()
    }
}

fn slot_key(slots: &[Slot], live: &[LiveSlot], position: u32) -> ContextKey {
    if position & LIVE_BIT == 0 {
        slots[position as usize].key
    } else {
        live[(position & !LIVE_BIT) as usize].key
    }
}

// The caller removes or redirects the index entry at `position` before it moves the last slot.
fn swap_remove_slot<T>(
    slots: &mut Vec<T>, index: &mut HashTable<u32>, position: u32, key: impl FnOnce(&T) -> ContextKey,
) -> T {
    let offset = (position & !LIVE_BIT) as usize;
    let slot = slots.swap_remove(offset);
    if let Some(moved) = slots.get(offset) {
        let old_position = slots.len() as u32 | (position & LIVE_BIT);
        *index.find_mut(index_hash(key(moved)), |&i| i == old_position).unwrap() = position;
    }
    slot
}

#[cfg(test)]
mod tests {
    use std::{sync::Barrier, thread};

    use super::*;
    use crate::data_model::event::metric::context::ContextResolverBuilder;

    fn insert(shard: &mut Shard, name: &str, now: u32) -> bool {
        let context = Context::from_parts(name, crate::data_model::tags::TagSet::default());
        let key = context.inner.key();
        shard.insert(key, index_hash(key), context, now)
    }

    fn get<'a>(shard: &'a Shard, name: &str) -> Option<&'a Slot> {
        let key = Context::from_parts(name, crate::data_model::tags::TagSet::default())
            .inner
            .key();
        shard.get(key, index_hash(key))
    }

    #[test]
    fn evicts_at_capacity_and_keeps_hot_entries() {
        let mut shard = Shard::new(3);
        assert!(!insert(&mut shard, "hot", 0));
        assert!(!insert(&mut shard, "cold", 1));
        assert!(!insert(&mut shard, "recent", 2));
        get(&shard, "hot").unwrap().touch(3);

        assert!(insert(&mut shard, "new", 3));

        assert_eq!(shard.slots.len(), 3);
        assert!(get(&shard, "cold").is_none());
        for name in ["hot", "recent", "new"] {
            assert_eq!(get(&shard, name).unwrap().context.name(), name);
        }
    }

    #[test]
    fn idle_expiration_uses_last_access_and_does_not_round_ttl_down() {
        let mut shard = Shard::new(3);
        let idle = Duration::from_millis(20_500);
        assert_eq!(expiration_cutoff(Duration::from_secs(20), idle), None);
        insert(&mut shard, "touched", 1);
        insert(&mut shard, "untouched", 9);
        get(&shard, "touched").unwrap().touch(10);
        get(&shard, "touched").unwrap().touch(10);

        let cutoff = expiration_cutoff(Duration::from_millis(31_499), idle).unwrap();
        assert_eq!(shard.expire(cutoff), 1);
        assert!(get(&shard, "untouched").is_none());
        assert!(get(&shard, "touched").is_some());

        let cutoff = expiration_cutoff(Duration::from_millis(31_500), idle).unwrap();
        assert_eq!(shard.expire(cutoff), 1);
        assert!(get(&shard, "touched").is_none());
    }

    #[test]
    fn drain_shrinks_storage_and_the_index_and_allows_regrowth() {
        let mut shard = Shard::new(256);
        for i in 0..256 {
            insert(&mut shard, &i.to_string(), u32::from(i < 8));
        }
        let slots = shard.slots.capacity();
        let index = shard.index.capacity();

        assert_eq!(shard.sweep(Some(1)), (248, 0));
        assert!(shard.slots.capacity() < slots / 2);
        assert!(shard.index.capacity() < index / 2);
        for i in 0..8 {
            assert!(get(&shard, &i.to_string()).is_some());
        }

        assert_eq!(shard.sweep(Some(2)), (8, 0));
        assert_eq!(shard.slots.capacity(), 0);
        assert_eq!(shard.index.capacity(), 0);
        assert!(!insert(&mut shard, "new", 2));
        assert!(get(&shard, "new").is_some());
    }

    #[test]
    fn pressure_evicts_idle_and_dead_weak_slots_but_preserves_live_identity() {
        let mut resolver = ContextResolverBuilder::for_tests()
            .with_cached_contexts_limit(64)
            .build();
        let held = resolver.resolve("held", ["env:prod"], None).unwrap();
        let idle = resolver.resolve("idle", ["env:prod"], None).unwrap();
        let idle_weak = idle.inner.downgrade();
        drop(idle);
        for i in 0..10 {
            resolver.resolve(format!("idle.{i}"), ["env:prod"], None).unwrap();
        }
        assert_eq!(resolver.evict_idle_for_budget(), 11);
        assert!(idle_weak.upgrade().is_none());
        assert!(held.ptr_eq(&resolver.resolve("held", ["env:prod"], None).unwrap()));
        assert_eq!(resolver.evict_idle_for_budget(), 0);
        drop(held);
        assert_eq!(resolver.evict_idle_for_budget(), 1);

        let mut resolver = ContextResolverBuilder::for_tests()
            .with_cached_contexts_limit(1)
            .build();
        let held = resolver.resolve("weak", ["env:prod"], None).unwrap();
        resolver.resolve("resident", ["env:prod"], None).unwrap();
        drop(held);
        assert_eq!(resolver.evict_idle_for_budget(), 1);
        let cache = resolver.context_cache.as_ref().unwrap();
        let shard = cache.state.shards[0].0.read().unwrap();
        assert!(shard.live.is_empty());
        assert!(shard.index.is_empty());
    }

    #[test]
    fn reuses_a_live_context_after_capacity_eviction() {
        let mut resolver = ContextResolverBuilder::for_tests()
            .with_cached_contexts_limit(1)
            .build();
        let held = resolver.resolve("metric.a", ["env:prod"], None).unwrap();
        resolver.resolve("metric.b", ["env:prod"], None).unwrap();

        let cache = resolver.context_cache.as_ref().unwrap();
        let key = held.inner.key();
        assert!(cache.state.shards[0]
            .0
            .read()
            .unwrap()
            .get(key, index_hash(key))
            .is_none());
        assert_eq!(held.inner.strong_count(), 1);

        let resolved = resolver.resolve("metric.a", ["env:prod"], None).unwrap();
        assert!(held.ptr_eq(&resolved));
    }

    #[test]
    fn reuses_a_live_context_after_idle_expiration() {
        let mut resolver = ContextResolverBuilder::for_tests()
            .with_cached_contexts_limit(1)
            .build();
        let held = resolver.resolve("metric.a", ["env:prod"], None).unwrap();
        let cache = resolver.context_cache.as_ref().unwrap();
        {
            let mut shard = cache.state.shards[0].0.write().unwrap();
            let last_seen = shard.slots[0].last_seen.load(Relaxed);
            let cutoff = expiration_cutoff(Duration::from_secs(u64::from(last_seen) + 21), Duration::from_secs(20));
            assert_eq!(shard.sweep(cutoff), (1, 0));
            assert_eq!(shard.slots.capacity(), 0);
            assert_eq!(shard.index.len(), 1);
        }
        assert_eq!(held.inner.strong_count(), 1);

        let resolved = resolver.resolve("metric.a", ["env:prod"], None).unwrap();
        assert!(held.ptr_eq(&resolved));
    }

    #[test]
    fn misses_and_sweeps_reap_dead_weak_entries() {
        let mut resolver = ContextResolverBuilder::for_tests()
            .with_cached_contexts_limit(1)
            .build();
        let held: Vec<_> = (0..10)
            .map(|i| resolver.resolve(format!("metric.{i}"), ["env:prod"], None).unwrap())
            .collect();
        let cache = resolver.context_cache.as_ref().unwrap().clone();
        let shard = &cache.state.shards[0].0;
        assert_eq!(shard.read().unwrap().live.len(), 9);
        drop(held);

        resolver.resolve("new", std::iter::empty::<&str>(), None).unwrap();
        assert_eq!(shard.read().unwrap().live.len(), 7);

        let mut shard = shard.write().unwrap();
        assert_eq!(shard.sweep(None), (0, 7));
        assert_eq!(shard.live.capacity(), 0);
        assert_eq!(shard.index.len(), 1);
        assert!(get(&shard, "new").is_some());
    }

    #[test]
    fn concurrent_hits_and_misses_share_one_context_per_key() {
        for capacity in [1, usize::MAX] {
            concurrent_resolve(capacity);
        }
    }

    fn concurrent_resolve(capacity: usize) {
        const THREADS: usize = 8;
        let resolver = ContextResolverBuilder::for_tests()
            .with_cached_contexts_limit(capacity)
            .build();
        let barrier = Barrier::new(THREADS);
        let names: Vec<_> = (0..128).map(|i| format!("metric.{i}")).collect();

        thread::scope(|scope| {
            let workers: Vec<_> = (0..THREADS)
                .map(|_| {
                    let mut resolver = resolver.clone();
                    let names = &names;
                    let barrier = &barrier;
                    scope.spawn(move || {
                        barrier.wait();
                        names
                            .iter()
                            .map(|name| resolver.resolve(name, ["env:prod"], None).unwrap())
                            .collect::<Vec<_>>()
                    })
                })
                .collect();
            let mut workers = workers.into_iter();
            let first = workers.next().unwrap().join().unwrap();
            for worker in workers {
                let others = worker.join().unwrap();
                assert!(first.iter().zip(&others).all(|(a, b)| a.ptr_eq(b)));
            }
        });
    }

    #[test]
    #[cfg(target_pointer_width = "64")]
    fn compact_context_cache_layout() {
        use std::mem::size_of;

        assert_eq!(size_of::<ContextCache>(), 16);
        assert_eq!(size_of::<Slot>(), 24);
        assert_eq!(size_of::<LiveSlot>(), 16);
    }
}
