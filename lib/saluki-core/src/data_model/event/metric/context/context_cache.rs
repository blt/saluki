use std::{
    hash::BuildHasher as _,
    num::NonZeroUsize,
    sync::{
        atomic::{AtomicU32, Ordering::Relaxed},
        Arc, RwLock,
    },
    time::{Duration, Instant},
};

use hashbrown::HashTable;
use saluki_common::{hash::NoopU64BuildHasher, sync::shutdown::ShutdownHandle};
use tokio::{pin, select, time::sleep};

use super::{Context, ContextKey};
use crate::{
    cache::Telemetry,
    runtime::{self, ScopeGuard},
};

const MAX_SHARDS: usize = 64;
const MIN_SHARD_CAPACITY: usize = 32;
const EVICTION_SAMPLE_SIZE: usize = 8;
const SWEEP_INTERVAL: Duration = Duration::from_secs(1);

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
                AlignedShard(RwLock::new(Shard::new(limit.min(u32::MAX as usize))))
            })
            .collect();
        let telemetry = Telemetry::new(identifier.clone());
        telemetry.weight_limit().set(limit as f64);
        let state = Arc::new(State {
            shards,
            start: Instant::now(),
            telemetry,
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
                        Arc::clone(&state).drive_telemetry(shutdown)
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
        if let Some(slot) = shard.get(key, hash) {
            slot.touch(epoch(state.start.elapsed()));
            state.telemetry.hits_total().increment(1);
            return Some((slot.context.clone(), true));
        }

        state.telemetry.misses_total().increment(1);
        let context = create()?;
        let evicted = shard.insert(key, hash, context.clone(), epoch(state.start.elapsed()));
        state.telemetry.items_inserted_total().increment(1);
        if evicted && state.telemetry_enabled {
            state.telemetry.items_evicted_total().increment(1);
        }
        Some((context, false))
    }
}

struct State {
    shards: Box<[AlignedShard]>,
    start: Instant,
    telemetry: Telemetry,
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

            let Some(cutoff) = expiration_cutoff(self.start.elapsed(), idle) else {
                continue;
            };
            let mut expired = 0;
            for shard in &self.shards {
                let count = shard.0.write().unwrap().expire(cutoff);
                expired += count;
                if count != 0 {
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

    async fn drive_telemetry(self: Arc<Self>, shutdown: ShutdownHandle) {
        pin!(shutdown);
        loop {
            select! {
                _ = &mut shutdown => break,
                _ = sleep(SWEEP_INTERVAL) => {}
            }

            let items: usize = self
                .shards
                .iter()
                .map(|shard| shard.0.read().unwrap().slots.len())
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

struct Shard {
    slots: Vec<Slot>,
    index: HashTable<u32>,
    limit: usize,
    next_eviction: usize,
}

impl Shard {
    fn new(limit: usize) -> Self {
        Self {
            slots: Vec::new(),
            index: HashTable::new(),
            limit,
            next_eviction: 0,
        }
    }

    fn get(&self, key: ContextKey, hash: u64) -> Option<&Slot> {
        let position = *self.index.find(hash, |i| self.slots[*i as usize].key == key)?;
        Some(&self.slots[position as usize])
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
        self.index
            .insert_unique(hash, position, |i| index_hash(self.slots[*i as usize].key));
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
        let key = self.slots[position].key;
        self.index
            .find_entry(index_hash(key), |&i| i as usize == position)
            .unwrap()
            .remove();
        self.slots.swap_remove(position);
        if let Some(moved) = self.slots.get(position) {
            let old_position = self.slots.len();
            *self
                .index
                .find_mut(index_hash(moved.key), |&i| i as usize == old_position)
                .unwrap() = position as u32;
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
        let len = self.slots.len();
        if len <= self.slots.capacity() / 4 {
            self.slots.shrink_to(len * 2);
        }
        if len <= self.index.capacity() / 4 {
            self.index.shrink_to_fit(|i| index_hash(self.slots[*i as usize].key));
        }
        before - len
    }
}

#[cfg(test)]
mod tests {
    use std::{mem::size_of, sync::Barrier, thread};

    use super::*;
    use crate::data_model::event::metric::context::ContextResolverBuilder;

    fn insert(shard: &mut Shard, name: &str, now: u32) -> bool {
        let context = Context::from_parts(name, crate::data_model::tags::TagSet::default());
        let key = context.inner.key;
        shard.insert(key, index_hash(key), context, now)
    }

    fn get<'a>(shard: &'a Shard, name: &str) -> Option<&'a Slot> {
        let key = Context::from_parts(name, crate::data_model::tags::TagSet::default())
            .inner
            .key;
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

        assert_eq!(shard.expire(1), 248);
        assert!(shard.slots.capacity() < slots / 2);
        assert!(shard.index.capacity() < index / 2);
        for i in 0..8 {
            assert!(get(&shard, &i.to_string()).is_some());
        }

        assert_eq!(shard.expire(2), 8);
        assert_eq!(shard.slots.capacity(), 0);
        assert_eq!(shard.index.capacity(), 0);
        assert!(!insert(&mut shard, "new", 2));
        assert!(get(&shard, "new").is_some());
    }

    #[test]
    fn concurrent_hits_and_misses_share_one_context_per_key() {
        const THREADS: usize = 8;
        let resolver = ContextResolverBuilder::for_tests().build();
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
        assert_eq!(size_of::<ContextCache>(), 16);
        assert_eq!(size_of::<Slot>(), 24);
        assert_eq!(size_of::<u32>(), 4);
    }
}
