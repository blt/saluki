use std::hash::{Hash as _, Hasher as _};

use saluki_common::{
    collections::PrehashedHashSet,
    hash::{get_fast_hasher, hash_single_fast},
};

/// Hashes a metric context.
///
/// Takes a metric name, an iterator of tags, and an iterator of origin tags, and returns a tuple containing a unique
/// hash key for the overall context, and a unique hash key for the non-origin tags by themselves.
///
/// All tags are hashed in an order-oblivious (XOR) manner, which allows tags to be hashed in any order while still
/// resulting in the same overall hash. This function is _not_ oblivious to the actual tag values themselves, though, so
/// differences such as case (lower vs upper) or leading/trailing whitespace will influence the resulting hash.
///
/// If a tag is seen more than once, it will be ignored and not included in the overall hash. For large tag sets,
/// prefer to reuse a [`PrehashedHashSet`] with [`hash_context_with_host_and_seen`] to amortize its allocation.
///
/// Returns a hash that uniquely identifies the combination of name, tags, and origin of the value.
pub fn hash_context<I, I2, T, T2>(name: &str, tags: I, origin_tags: I2) -> (ContextKey, TagSetKey)
where
    I: IntoIterator<Item = T>,
    T: AsRef<str>,
    I2: IntoIterator<Item = T2>,
    T2: AsRef<str>,
{
    let mut seen = PrehashedHashSet::default();
    hash_context_with_host_and_seen(name, None, tags, origin_tags, &mut seen)
}

/// Hashes a metric context with an explicit host dimension, using a provided set to track duplicate tags.
///
/// Takes a metric name, optional host, an iterator of tags, and an iterator of origin tags, and returns a tuple
/// containing a unique hash key for the overall context, and a unique hash key for the non-origin tags by themselves.
/// The host is considered part of overall context identity, but not part of the non-origin tag set.
///
/// All tags are hashed in an order-oblivious (XOR) manner, which allows tags to be hashed in any order while still
/// resulting in the same overall hash. This function is _not_ oblivious to the actual tag values themselves, though, so
/// differences such as case (lower vs upper) or leading/trailing whitespace will influence the resulting hash. The host
/// and metric name are order-dependent scalar context fields, not tags.
///
/// If a tag is seen more than once, it will be ignored and not included in the overall hash.
///
/// Returns a hash that uniquely identifies the combination of name, host, tags, and origin of the value. An unset host
/// and an explicitly empty host are distinct contexts.
pub fn hash_context_with_host<I, I2, T, T2>(
    name: &str, host: Option<&str>, tags: I, origin_tags: I2,
) -> (ContextKey, TagSetKey)
where
    I: IntoIterator<Item = T>,
    T: AsRef<str>,
    I2: IntoIterator<Item = T2>,
    T2: AsRef<str>,
{
    let mut seen = PrehashedHashSet::default();
    hash_context_with_host_and_seen(name, host, tags, origin_tags, &mut seen)
}

pub(super) fn hash_context_with_host_and_seen<I, I2, T, T2>(
    name: &str, host: Option<&str>, tags: I, origin_tags: I2, seen: &mut PrehashedHashSet<u64>,
) -> (ContextKey, TagSetKey)
where
    I: IntoIterator<Item = T>,
    T: AsRef<str>,
    I2: IntoIterator<Item = T2>,
    T2: AsRef<str>,
{
    let mut hasher = get_fast_hasher();

    name.hash(&mut hasher);
    host.hash(&mut hasher);

    let combined_tags_hash = hash_tags(tags, seen);
    hasher.write_u64(combined_tags_hash);

    hasher.write_u64(hash_tags(origin_tags, seen));

    let context_key = ContextKey { hash: hasher.finish() };
    let tagset_key = TagSetKey {
        hash: combined_tags_hash,
    };

    (context_key, tagset_key)
}

/// XORs the distinct tag hashes. Tags and origin tags use independent deduplication scopes.
#[inline]
fn hash_tags<I, T>(tags: I, seen: &mut PrehashedHashSet<u64>) -> u64
where
    I: IntoIterator<Item = T>,
    T: AsRef<str>,
{
    const STACK_CAPACITY: usize = 16;

    let mut hashes = tags.into_iter().map(|tag| hash_single_fast(tag.as_ref()));
    let mut combined = 0;

    if hashes.size_hint().0 <= STACK_CAPACITY {
        let mut inline = [0; STACK_CAPACITY];
        let mut len = 0;
        loop {
            let Some(hash) = hashes.next() else {
                return combined;
            };
            if inline[..len].contains(&hash) {
                continue;
            }
            combined ^= hash;
            if len == STACK_CAPACITY {
                // Transfer the hashes already seen without rehashing their strings.
                seen.clear();
                seen.extend(inline);
                seen.insert(hash);
                break;
            }
            inline[len] = hash;
            len += 1;
        }
    } else {
        seen.clear();
    }

    for hash in hashes {
        if seen.insert(hash) {
            combined ^= hash;
        }
    }

    combined
}

/// A compact hash key that identifies a metric context.
///
/// The key is process-local and collision-prone by nature because it stores a 64-bit hash.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct ContextKey {
    hash: u64,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct TagSetKey {
    hash: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn context_hash_matches_hash_set_deduplication() {
        fn baseline_tags(tags: &[String]) -> u64 {
            tags.iter()
                .map(|tag| hash_single_fast(tag.as_str()))
                .collect::<PrehashedHashSet<_>>()
                .into_iter()
                .fold(0, |combined, hash| combined ^ hash)
        }

        let name = "sample.metric";
        let mut seen = PrehashedHashSet::default();
        for (regular_count, origin_count) in [(0, 0), (16, 17), (17, 16), (17, 18), (2, 1)] {
            let build_tags = |count| {
                let mut tags: Vec<_> = (0..count).map(|i| format!("tag:{i}")).collect();
                if let Some(first) = tags.first() {
                    tags.push(first.clone());
                }
                tags
            };
            let tags = build_tags(regular_count);
            let mut origin_tags = build_tags(origin_count);
            origin_tags.reverse();

            for host in [None, Some(""), Some("my-host")] {
                let tags_hash = baseline_tags(&tags);
                let mut hasher = get_fast_hasher();
                name.hash(&mut hasher);
                host.hash(&mut hasher);
                hasher.write_u64(tags_hash);
                hasher.write_u64(baseline_tags(&origin_tags));

                let (context_key, tagset_key) =
                    hash_context_with_host_and_seen(name, host, &tags, &origin_tags, &mut seen);
                assert_eq!(context_key.hash, hasher.finish());
                assert_eq!(tagset_key.hash, tags_hash);

                // Filters hide the length, forcing large inputs to spill from the stack while hashing.
                let streaming = hash_context_with_host_and_seen(
                    name,
                    host,
                    tags.iter().filter(|_| true),
                    origin_tags.iter().filter(|_| true),
                    &mut seen,
                );
                assert_eq!(streaming, (context_key, tagset_key));
            }
        }
    }
}
