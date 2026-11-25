use std::{
    alloc::{alloc, dealloc, handle_alloc_error, GlobalAlloc, Layout},
    mem::ManuallyDrop,
    ptr::NonNull,
    slice, str,
    sync::{
        atomic::{fence, AtomicUsize, Ordering},
        Arc, LazyLock,
    },
};

use hashbrown::HashSet;
use metrics::Gauge;
use saluki_common::{hash::FastBuildHasher, resource_tracking::ComponentAllocator};
use smallvec::SmallVec;
use stringtheory::{
    interning::{Symbol, SymbolTable},
    MetaString,
};

use super::ContextKey;
use crate::data_model::tags::{SharedTagSet, Tag, TagRef, TagSet};

const ORIGIN: u16 = 1 << 0;
const HOST: u16 = 1 << 1;
const NAME_ID: u16 = 1 << 2;
const HOST_ID: u16 = 1 << 3;
const ALL_IDS: u16 = 1 << 4;
const MIXED: u16 = 1 << 5;
const TRACKED: u16 = 1 << 6;
const MODIFIED: u16 = 1 << 7;
const LEASE: u16 = 1 << 8;
pub(super) const INLINE_TAG_CAPACITY: usize = 16;
const MIN_SYMBOL_LEN: usize = 8;

static EMPTY_ORIGIN: LazyLock<TagSet> = LazyLock::new(TagSet::default);

pub(super) struct Domain {
    pub(super) symbols: Arc<SymbolTable>,
    pub(super) active: Gauge,
    pub(super) allocator: Option<ComponentAllocator>,
}

// Preserve the legacy interner quota when the resolver disallows string heap spill.
pub(super) struct StrictLease {
    pub(super) name: MetaString,
    pub(super) host: Option<MetaString>,
    pub(super) tags: SharedTagSet,
}

impl StrictLease {
    pub(super) fn rewrite<'a>(&self, name: &str, host: Option<&str>, tags: impl Iterator<Item = &'a str>) -> Arc<Self> {
        let text = |old: Option<&MetaString>, value: &str| {
            old.filter(|old| old.as_ref() == value)
                .cloned()
                .unwrap_or_else(|| MetaString::from(value))
        };
        let tags = tags
            .map(|value| {
                (&self.tags)
                    .into_iter()
                    .find(|tag| tag.as_str() == value)
                    .cloned()
                    .unwrap_or_else(|| Tag::from(value))
            })
            .collect();
        Arc::new(Self {
            name: text(Some(&self.name), name),
            host: host.map(|host| text(self.host.as_ref(), host)),
            tags,
        })
    }
}

pub(super) struct Parts<'a> {
    pub(super) key: ContextKey,
    pub(super) name: &'a str,
    pub(super) host: Option<&'a str>,
    pub(super) origin: TagSet,
    pub(super) domain: Option<&'a Arc<Domain>>,
    pub(super) tracked: bool,
    pub(super) metadata: TagMetadata,
    pub(super) lease: Option<&'a Arc<StrictLease>>,
}

#[derive(Clone, Copy, Default)]
pub(super) struct TagMetadata {
    pub(super) additions: usize,
    pub(super) modified: bool,
    pub(super) deduplicate: bool,
}

impl TagMetadata {
    pub(super) fn from_tag_set(tags: &TagSet) -> Self {
        Self {
            additions: tags.addition_count(),
            modified: tags.is_modified(),
            deduplicate: false,
        }
    }
}

// Optional origin handle, inline name/host bytes, mixed bitmap, then tag records.
// Weak references keep the allocation metadata and the owner's allocator live.
#[repr(C)]
struct Header {
    strong: AtomicUsize,
    weak: AtomicUsize,
    key: ContextKey,
    domain: ManuallyDrop<Option<Arc<Domain>>>,
    bytes: u32,
    name: u32,
    host: u32,
    tag_count: u32,
    additions: u32,
    flags: u16,
}

impl Header {
    fn layout(bytes: usize) -> Layout {
        Layout::from_size_align(bytes, align_of::<Self>())
            .unwrap()
            .pad_to_align()
    }

    fn has(&self, flag: u16) -> bool {
        self.flags & flag != 0
    }

    fn symbols(&self) -> &SymbolTable {
        &self.domain.as_ref().expect("symbol has a resolver domain").symbols
    }

    fn symbol_text(&self, id: u32) -> (&str, Option<(usize, u32)>) {
        let table = self.symbols();
        // SAFETY: The context owns this symbol reference.
        let text = unsafe { table.get_unchecked(id) };
        (text, Some((std::ptr::from_ref(table).addr(), id)))
    }

    fn prefix_size(&self) -> usize {
        usize::from(self.has(ORIGIN)) * size_of::<TagSet>()
            + usize::from(self.has(LEASE)) * size_of::<Arc<StrictLease>>()
    }
}

pub(super) struct Packed(NonNull<Header>);

// SAFETY: The allocation is immutable except for atomic reference counts. Owned payload is Send + Sync.
unsafe impl Send for Packed {}
unsafe impl Sync for Packed {}

impl Packed {
    pub(super) fn new<'a>(parts: Parts<'_>, tags: impl IntoIterator<Item = &'a str>) -> Self {
        let Parts {
            key,
            name,
            host,
            origin,
            domain,
            tracked,
            metadata,
            lease,
        } = parts;
        let intern = |text: &str| -> Option<Symbol<'_>> {
            if lease.is_some() || text.len() < MIN_SYMBOL_LEN {
                return None;
            }
            domain?.symbols.try_intern(text)
        };
        let name_symbol = intern(name);
        let host_symbol = host.and_then(intern);
        let tags = tags.into_iter();
        let min_tags = tags.size_hint().0;
        let mut prepared = SmallVec::<[(&str, Option<Symbol<'_>>); INLINE_TAG_CAPACITY]>::with_capacity(min_tags);
        let mut ids = 0;
        let mut all_inline_bytes = 0;
        let mut tag_bytes = 0;
        let mut seen = (metadata.deduplicate && min_tags > INLINE_TAG_CAPACITY)
            .then(|| HashSet::<&str, FastBuildHasher>::with_capacity_and_hasher(min_tags, FastBuildHasher::default()));
        for text in tags {
            if metadata.deduplicate {
                let duplicate = match &mut seen {
                    Some(seen) => !seen.insert(text),
                    None => prepared.iter().any(|(entry, _)| *entry == text),
                };
                if duplicate {
                    continue;
                }
            }
            let symbol = intern(text);
            let inline_bytes = leb_size(text.len()) + text.len();
            all_inline_bytes += inline_bytes;
            tag_bytes += if symbol.is_some() {
                ids += 1;
                4
            } else {
                inline_bytes
            };
            prepared.push((text, symbol));
        }
        let count = prepared.len();
        let mut flags = 0;
        let mut bitmap_bytes = 0;
        if ids > 0 {
            if ids == count {
                flags |= ALL_IDS;
            } else if tag_bytes + count.div_ceil(8) < all_inline_bytes {
                flags |= MIXED;
                bitmap_bytes = count.div_ceil(8);
            } else {
                ids = 0;
                tag_bytes = all_inline_bytes;
            }
        }
        let has_origin = !origin.is_empty() || origin.is_modified();
        for (enabled, flag) in [
            (has_origin, ORIGIN),
            (host.is_some(), HOST),
            (name_symbol.is_some(), NAME_ID),
            (host_symbol.is_some(), HOST_ID),
            (tracked, TRACKED),
            (metadata.modified, MODIFIED),
            (lease.is_some(), LEASE),
        ] {
            if enabled {
                flags |= flag;
            }
        }
        let name_bytes = if name_symbol.is_none() { name.len() } else { 0 };
        let host_bytes = if host_symbol.is_none() {
            host.map_or(0, str::len)
        } else {
            0
        };
        let origin_bytes = if has_origin { size_of::<TagSet>() } else { 0 };
        let lease_bytes = usize::from(lease.is_some()) * size_of::<Arc<StrictLease>>();
        let bytes =
            size_of::<Header>() + origin_bytes + lease_bytes + name_bytes + host_bytes + bitmap_bytes + tag_bytes;
        let layout = Header::layout(bytes);
        let recorded_bytes = u32::try_from(bytes).expect("packed context length fits in u32");
        let header = Header {
            strong: AtomicUsize::new(1),
            weak: AtomicUsize::new(1),
            key,
            domain: ManuallyDrop::new(domain.cloned()),
            bytes: recorded_bytes,
            name: name_symbol.map_or_else(
                || name.len().try_into().expect("name length fits in u32"),
                Symbol::into_id,
            ),
            host: host_symbol.map_or_else(
                || host_bytes.try_into().expect("host length fits in u32"),
                Symbol::into_id,
            ),
            tag_count: count.try_into().expect("tag count fits in u32"),
            additions: metadata
                .additions
                .min(count)
                .try_into()
                .expect("addition count fits in u32"),
            flags,
        };
        // SAFETY: The layout is nonzero; each tail region is disjoint and sized above.
        let ptr = unsafe {
            let ptr = match domain.and_then(|domain| domain.allocator.as_ref()) {
                Some(allocator) => allocator.alloc(layout),
                None => alloc(layout),
            };
            let header_ptr = NonNull::new(ptr.cast::<Header>()).unwrap_or_else(|| handle_alloc_error(layout));
            header_ptr.as_ptr().write(header);
            let mut cursor = ptr.add(size_of::<Header>());
            if has_origin {
                cursor.cast::<TagSet>().write(origin);
                cursor = cursor.add(origin_bytes);
            }
            if let Some(lease) = lease {
                cursor.cast::<Arc<StrictLease>>().write(Arc::clone(lease));
                cursor = cursor.add(lease_bytes);
            }
            if name_bytes != 0 {
                cursor.copy_from_nonoverlapping(name.as_ptr(), name_bytes);
                cursor = cursor.add(name_bytes);
            }
            if host_bytes != 0 {
                cursor.copy_from_nonoverlapping(host.unwrap().as_ptr(), host_bytes);
                cursor = cursor.add(host_bytes);
            }
            let bitmap = cursor;
            bitmap.write_bytes(0, bitmap_bytes);
            cursor = cursor.add(bitmap_bytes);
            for (index, (text, symbol)) in prepared.into_iter().enumerate() {
                if let Some(symbol) = symbol.filter(|_| ids != 0) {
                    if bitmap_bytes != 0 {
                        *bitmap.add(index / 8) |= 1 << (index % 8);
                    }
                    cursor.cast::<u32>().write_unaligned(symbol.into_id());
                    cursor = cursor.add(4);
                } else {
                    cursor = write_leb(cursor, text.len());
                    cursor.copy_from_nonoverlapping(text.as_ptr(), text.len());
                    cursor = cursor.add(text.len());
                }
            }
            debug_assert_eq!(cursor, ptr.add(bytes));
            header_ptr
        };
        if tracked {
            domain.unwrap().active.increment(1);
        }
        Self(ptr)
    }

    fn header(&self) -> &Header {
        // SAFETY: A strong reference keeps the header and payload live.
        unsafe { self.0.as_ref() }
    }

    pub(super) fn key(&self) -> ContextKey {
        self.header().key
    }

    pub(super) fn ptr_eq(&self, other: &Self) -> bool {
        self.0 == other.0
    }

    pub(super) fn allocation_bytes(&self) -> usize {
        Header::layout(self.header().bytes as usize).size()
    }

    pub(super) fn domain(&self) -> Option<&Arc<Domain>> {
        self.header().domain.as_ref()
    }

    pub(super) fn strong_count(&self) -> usize {
        self.header().strong.load(Ordering::Relaxed)
    }

    pub(super) fn downgrade(&self) -> WeakPacked {
        increment(&self.header().weak);
        WeakPacked(self.0)
    }

    pub(super) fn track_rewrite(&self) -> bool {
        self.header().has(TRACKED) && self.strong_count() == 1
    }

    pub(super) fn name_with_symbol_id(&self) -> (&str, Option<(usize, u32)>) {
        let header = self.header();
        if header.has(NAME_ID) {
            header.symbol_text(header.name)
        } else {
            (self.name(), None)
        }
    }

    pub(super) fn name(&self) -> &str {
        let header = self.header();
        if header.has(NAME_ID) {
            // SAFETY: The context owns this symbol reference.
            unsafe { header.symbols().get_unchecked(header.name) }
        } else {
            // SAFETY: The name consists of UTF-8 bytes copied into the tail.
            unsafe { str::from_utf8_unchecked(&self.tail()[..header.name as usize]) }
        }
    }

    pub(super) fn host(&self) -> Option<&str> {
        let header = self.header();
        if !header.has(HOST) {
            return None;
        }
        Some(if header.has(HOST_ID) {
            // SAFETY: The context owns this symbol reference.
            unsafe { header.symbols().get_unchecked(header.host) }
        } else {
            let start = self.name_bytes();
            // SAFETY: The host consists of UTF-8 bytes copied into the tail.
            unsafe { str::from_utf8_unchecked(&self.tail()[start..start + header.host as usize]) }
        })
    }

    pub(super) fn origin(&self) -> &TagSet {
        if !self.header().has(ORIGIN) {
            return &EMPTY_ORIGIN;
        }
        // SAFETY: A live origin handle, when present, is aligned immediately after the header.
        unsafe { &*self.0.as_ptr().add(1).cast::<TagSet>() }
    }

    fn lease_ptr(&self) -> *mut Arc<StrictLease> {
        let origin_size = usize::from(self.header().has(ORIGIN)) * size_of::<TagSet>();
        // SAFETY: The lease, when present, follows the optional origin handle with no padding needed.
        unsafe { self.0.as_ptr().add(1).cast::<u8>().add(origin_size).cast() }
    }

    pub(super) fn lease(&self) -> Option<&Arc<StrictLease>> {
        // SAFETY: The strong reference keeps the lease live, and the flag says it is present.
        self.header().has(LEASE).then(|| unsafe { &*self.lease_ptr() })
    }

    pub(super) fn tag_count(&self) -> usize {
        self.header().tag_count as usize
    }

    pub(super) fn metadata(&self) -> TagMetadata {
        TagMetadata {
            additions: self.header().additions as usize,
            modified: self.header().has(MODIFIED),
            deduplicate: false,
        }
    }

    pub(super) fn tag_iter(&self) -> TagIter<'_> {
        TagIter {
            header: self.header(),
            raw: self.raw_tags(),
        }
    }

    fn name_bytes(&self) -> usize {
        let header = self.header();
        if header.has(NAME_ID) {
            0
        } else {
            header.name as usize
        }
    }

    fn tail(&self) -> &[u8] {
        let header = self.header();
        let start = size_of::<Header>() + header.prefix_size();
        // SAFETY: The byte tail is fully initialized and excludes the optional origin handle and padding.
        unsafe { slice::from_raw_parts(self.0.as_ptr().cast::<u8>().add(start), header.bytes as usize - start) }
    }

    fn raw_tags(&self) -> RawTags<'_> {
        let header = self.header();
        let host_bytes = if header.has(HOST_ID) { 0 } else { header.host as usize };
        let tags = &self.tail()[self.name_bytes() + host_bytes..];
        let count = self.tag_count();
        let bitmap_bytes = if header.has(MIXED) { count.div_ceil(8) } else { 0 };
        let (bitmap, payload) = tags.split_at(bitmap_bytes);
        RawTags {
            bitmap,
            payload,
            count,
            index: 0,
            all_ids: header.has(ALL_IDS),
        }
    }
}

impl Clone for Packed {
    fn clone(&self) -> Self {
        increment(&self.header().strong);
        Self(self.0)
    }
}

impl Drop for Packed {
    fn drop(&mut self) {
        let header = self.header();
        if header.strong.fetch_sub(1, Ordering::Release) != 1 {
            return;
        }
        fence(Ordering::Acquire);
        // SAFETY: The last strong reference owns the payload. The implicit weak reference keeps allocation live.
        unsafe {
            if let Some(domain) = self.domain() {
                if header.has(NAME_ID) {
                    domain.symbols.release(header.name);
                }
                if header.has(HOST_ID) {
                    domain.symbols.release(header.host);
                }
                if header.has(ALL_IDS | MIXED) {
                    for tag in self.raw_tags() {
                        if let RawTag::Id(id) = tag {
                            domain.symbols.release(id);
                        }
                    }
                }
                if header.has(TRACKED) {
                    domain.active.decrement(1);
                }
            }
            if header.has(LEASE) {
                self.lease_ptr().drop_in_place();
            }
            if header.has(ORIGIN) {
                self.0.as_ptr().add(1).cast::<TagSet>().drop_in_place();
            }
        }
        drop(WeakPacked(self.0));
    }
}

pub(super) struct WeakPacked(NonNull<Header>);

// SAFETY: Weak references only inspect immutable layout metadata and atomic counts until they upgrade.
unsafe impl Send for WeakPacked {}
unsafe impl Sync for WeakPacked {}

impl WeakPacked {
    pub(super) fn strong_count(&self) -> usize {
        // SAFETY: The weak reference keeps the count live.
        unsafe { (*self.0.as_ptr()).strong.load(Ordering::Relaxed) }
    }

    pub(super) fn upgrade(&self) -> Option<Packed> {
        // SAFETY: The weak reference keeps the count live. A zero count cannot be resurrected.
        let strong = unsafe { &(*self.0.as_ptr()).strong };
        strong
            .fetch_update(Ordering::Acquire, Ordering::Relaxed, |count| {
                if count == 0 {
                    return None;
                }
                if count >= isize::MAX as usize {
                    std::process::abort();
                }
                Some(count + 1)
            })
            .ok()?;
        Some(Packed(self.0))
    }
}

impl Drop for WeakPacked {
    fn drop(&mut self) {
        // SAFETY: The weak reference keeps these fields live. The last reference can free the allocation.
        unsafe {
            let ptr = self.0.as_ptr();
            if (*ptr).weak.fetch_sub(1, Ordering::Release) == 1 {
                fence(Ordering::Acquire);
                let layout = Header::layout((*ptr).bytes as usize);
                let domain = ManuallyDrop::take(&mut (*ptr).domain);
                match domain.as_deref().and_then(|domain| domain.allocator.as_ref()) {
                    Some(allocator) => allocator.dealloc(ptr.cast(), layout),
                    None => dealloc(ptr.cast(), layout),
                }
            }
        }
    }
}

#[inline]
fn increment(count: &AtomicUsize) {
    if count.fetch_add(1, Ordering::Relaxed) >= isize::MAX as usize {
        std::process::abort();
    }
}

fn leb_size(n: usize) -> usize {
    ((usize::BITS - n.leading_zeros()).max(1) as usize).div_ceil(7)
}

// SAFETY: The caller reserves at least `leb_size(n)` bytes from `cursor`.
unsafe fn write_leb(mut cursor: *mut u8, mut n: usize) -> *mut u8 {
    while n >= 128 {
        cursor.write(n as u8 | 0x80);
        cursor = cursor.add(1);
        n >>= 7;
    }
    cursor.write(n as u8);
    cursor.add(1)
}

#[derive(Clone)]
struct RawTags<'a> {
    bitmap: &'a [u8],
    payload: &'a [u8],
    count: usize,
    index: usize,
    all_ids: bool,
}

enum RawTag<'a> {
    Id(u32),
    Inline(&'a str),
}

impl<'a> Iterator for RawTags<'a> {
    type Item = RawTag<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.index == self.count {
            return None;
        }
        let index = self.index;
        self.index += 1;
        if self.all_ids || (!self.bitmap.is_empty() && self.bitmap[index / 8] & (1 << (index % 8)) != 0) {
            let (id, rest) = self.payload.split_first_chunk::<4>().unwrap();
            self.payload = rest;
            return Some(RawTag::Id(u32::from_ne_bytes(*id)));
        }
        let mut len = 0;
        let mut shift = 0;
        loop {
            let (&byte, rest) = self.payload.split_first().unwrap();
            self.payload = rest;
            len |= usize::from(byte & 0x7f) << shift;
            if byte < 0x80 {
                break;
            }
            shift += 7;
        }
        let (text, rest) = self.payload.split_at(len);
        self.payload = rest;
        // SAFETY: Construction copies each tag from a valid UTF-8 string.
        Some(RawTag::Inline(unsafe { str::from_utf8_unchecked(text) }))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining = self.count - self.index;
        (remaining, Some(remaining))
    }
}

impl ExactSizeIterator for RawTags<'_> {}

#[derive(Clone)]
pub(super) struct TagIter<'a> {
    header: &'a Header,
    raw: RawTags<'a>,
}

impl<'a> TagIter<'a> {
    pub(super) fn strings_with_symbol_ids(
        self,
    ) -> impl ExactSizeIterator<Item = (&'a str, Option<(usize, u32)>)> + Clone {
        let header = self.header;
        self.raw.map(move |tag| match tag {
            RawTag::Inline(text) => (text, None),
            RawTag::Id(id) => header.symbol_text(id),
        })
    }
}

impl<'a> Iterator for TagIter<'a> {
    type Item = TagRef<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        Some(TagRef::from(match self.raw.next()? {
            RawTag::Inline(text) => text,
            // SAFETY: The context owns a reference to this symbol for the whole borrow.
            RawTag::Id(id) => unsafe { self.header.symbols().get_unchecked(id) },
        }))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.raw.size_hint()
    }
}

impl ExactSizeIterator for TagIter<'_> {}
