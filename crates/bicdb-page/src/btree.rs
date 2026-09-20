//! Page-backed B+ tree.
//!
//! Phase 2 of `docs/server-paged-storage-todo.md` needs "a persistent primary
//! B+ tree from logical record ID to the current tuple locator"; Phase 4 reuses
//! the same structure for secondary indexes. This is that tree, first-party, on
//! top of [`crate::BufferPool`].
//!
//! # Shape
//!
//! Keys and values are arbitrary byte strings, ordered bytewise. That matches
//! the existing `OrderedIndexStore` contract in `bicdb-core` (memcomparable keys
//! encoded by the caller), so the same tree serves the primary
//! `record id -> TupleLocator` mapping and, later, secondary indexes — without
//! the tree knowing anything about records.
//!
//! Nodes are slotted pages with entries kept **sorted by key**, so a lookup is a
//! binary search within each node rather than a scan.
//!
//! - **Leaf** entries are `(key, value)`. The header's `next_page` links to the
//!   right sibling, which is what makes a range scan a sibling walk rather than
//!   a repeated root descent.
//! - **Interior** entries are `(separator_key, child_page_id)`, where the child
//!   holds keys `< separator_key`. The header's `next_page` holds the rightmost
//!   child, covering keys `>= ` the last separator.
//!
//! # Descent pins one page at a time
//!
//! A lookup reads a node, decides where to go next, and releases the page before
//! reading the child. Memory is one page per level of concurrent descent, not
//! one page per level of the tree held simultaneously — a tree of any height
//! costs a bounded number of frames. Splits use a recorded path of page *ids*
//! rather than held guards for the same reason.
//!
//! # Root changes are atomic
//!
//! When the root splits, a new root page is written and made durable *before*
//! the superblock is updated to point at it. A crash between the two leaves the
//! old root intact and the new one unreferenced garbage, which is the safe
//! direction: the roadmap's "store root and free-page metadata durably with
//! atomic generation changes".

use std::sync::Arc;
use std::time::{Duration, Instant};

use rustc_hash::{FxHashMap, FxHashSet};

use crate::error::{PageError, Result};
use crate::page::{PageHeader, PageId, PageType, PAGE_HEADER_BYTES, PAGE_TRAILER_BYTES};
use crate::pool::BufferPool;

/// Directory entry: offset u16, key_len u16, value_len u16, reserved u16.
const ENTRY_BYTES: usize = 8;

/// Maximum entries one online structural-verification step may inspect.
pub const MAX_BTREE_VERIFY_ENTRIES_PER_STEP: u64 = 1_048_576;
/// Maximum leaf pages one online structural-verification step may inspect.
pub const MAX_BTREE_VERIFY_LEAF_PAGES_PER_STEP: u64 = 65_536;
/// Maximum logical page bytes one online structural-verification step may inspect.
pub const MAX_BTREE_VERIFY_PAGE_BYTES_PER_STEP: u64 = 1024 * 1024 * 1024;
/// Maximum aggregate key bytes one online structural-verification step may inspect.
pub const MAX_BTREE_VERIFY_KEY_BYTES_PER_STEP: u64 = 1024 * 1024 * 1024;
/// Maximum cooperative wall-clock budget for one structural-verification step.
pub const MAX_BTREE_VERIFY_DURATION_MILLIS_PER_STEP: u64 = 5 * 60 * 1_000;
/// Maximum serialized key retained by a structural-verification cursor.
pub const MAX_BTREE_VERIFY_CURSOR_KEY_BYTES: usize = 1024 * 1024;
/// Absolute root-to-leaf descent guard. A legitimate tree is many orders of
/// magnitude shallower than this even at petabyte scale.
pub const MAX_BTREE_VERIFY_HEIGHT: u32 = 128;

/// Restart position for one bounded structural B+ tree sweep.
///
/// `next_leaf` is a stable physical leaf page. Leaves are split in place and
/// are not merged or freed by this B+ tree generation, so the page remains a
/// valid restart anchor across writes and process restarts. `after_key` is the
/// last key already accounted for. When `resume_within_leaf` is true, keys at
/// or below it in `next_leaf` are deliberately skipped; otherwise the first key
/// in the leaf must compare strictly above it.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BTreeVerifyCursor {
    pub next_leaf: Option<PageId>,
    pub after_key: Option<Vec<u8>>,
    pub resume_within_leaf: bool,
    /// Leaf pages fully traversed by the complete sweep, across every step.
    /// Revisiting one partially consumed leaf in a later step does not advance
    /// this counter.
    pub leaf_pages_traversed: u64,
    /// File page count observed at the beginning of the sweep. It is a finite
    /// traversal guard for cycles longer than one step.
    pub page_count_bound: u64,
}

impl Default for BTreeVerifyCursor {
    fn default() -> Self {
        Self {
            next_leaf: None,
            after_key: None,
            resume_within_leaf: false,
            leaf_pages_traversed: 0,
            page_count_bound: 0,
        }
    }
}

/// Explicit resource envelope for one structural-verification step.
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BTreeVerifyLimits {
    pub max_entries: u64,
    pub max_leaf_pages: u64,
    pub max_page_bytes: u64,
    pub max_key_bytes: u64,
    pub max_duration_millis: u64,
    pub max_cursor_key_bytes: usize,
    pub max_height: u32,
}

impl Default for BTreeVerifyLimits {
    fn default() -> Self {
        Self {
            max_entries: 65_536,
            max_leaf_pages: 1_024,
            max_page_bytes: 128 * 1024 * 1024,
            max_key_bytes: 64 * 1024 * 1024,
            max_duration_millis: 100,
            max_cursor_key_bytes: 64 * 1024,
            max_height: 64,
        }
    }
}

impl BTreeVerifyLimits {
    pub fn validate(&self) -> Result<()> {
        if self.max_entries == 0 || self.max_entries > MAX_BTREE_VERIFY_ENTRIES_PER_STEP {
            return Err(PageError::InvalidMaintenanceLimits {
                reason: format!(
                    "max_entries must be between 1 and {MAX_BTREE_VERIFY_ENTRIES_PER_STEP}"
                ),
            });
        }
        if self.max_leaf_pages == 0 || self.max_leaf_pages > MAX_BTREE_VERIFY_LEAF_PAGES_PER_STEP {
            return Err(PageError::InvalidMaintenanceLimits {
                reason: format!(
                    "max_leaf_pages must be between 1 and {MAX_BTREE_VERIFY_LEAF_PAGES_PER_STEP}"
                ),
            });
        }
        if self.max_page_bytes == 0 || self.max_page_bytes > MAX_BTREE_VERIFY_PAGE_BYTES_PER_STEP {
            return Err(PageError::InvalidMaintenanceLimits {
                reason: format!(
                    "max_page_bytes must be between 1 and {MAX_BTREE_VERIFY_PAGE_BYTES_PER_STEP}"
                ),
            });
        }
        if self.max_key_bytes < self.max_cursor_key_bytes as u64
            || self.max_key_bytes > MAX_BTREE_VERIFY_KEY_BYTES_PER_STEP
        {
            return Err(PageError::InvalidMaintenanceLimits {
                reason: format!(
                    "max_key_bytes must cover max_cursor_key_bytes and not exceed {MAX_BTREE_VERIFY_KEY_BYTES_PER_STEP}"
                ),
            });
        }
        if self.max_duration_millis == 0
            || self.max_duration_millis > MAX_BTREE_VERIFY_DURATION_MILLIS_PER_STEP
        {
            return Err(PageError::InvalidMaintenanceLimits {
                reason: format!(
                    "max_duration_millis must be between 1 and {MAX_BTREE_VERIFY_DURATION_MILLIS_PER_STEP}"
                ),
            });
        }
        if self.max_cursor_key_bytes == 0
            || self.max_cursor_key_bytes > MAX_BTREE_VERIFY_CURSOR_KEY_BYTES
        {
            return Err(PageError::InvalidMaintenanceLimits {
                reason: format!(
                    "max_cursor_key_bytes must be between 1 and {MAX_BTREE_VERIFY_CURSOR_KEY_BYTES}"
                ),
            });
        }
        if self.max_height == 0 || self.max_height > MAX_BTREE_VERIFY_HEIGHT {
            return Err(PageError::InvalidMaintenanceLimits {
                reason: format!("max_height must be between 1 and {MAX_BTREE_VERIFY_HEIGHT}"),
            });
        }
        Ok(())
    }
}

/// Page kind required at a structural-verification edge.
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BTreePageExpectation {
    Node,
    Leaf,
}

/// A page-local structural invariant that failed without retaining page data.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum BTreeNodeViolation {
    PageIdMismatch { stored: PageId },
    DirectoryOutOfBounds,
    FreeSpaceOutOfBounds,
    EmptyKey { entry: u64 },
    EntryOutOfBounds { entry: u64 },
    OverlappingEntries { first: u64, second: u64 },
    KeysOutOfOrder { previous: u64, current: u64 },
    InteriorWithoutSeparator,
    InvalidChildWidth { entry: u64, found: u64 },
    MissingChild { entry: Option<u64> },
}

/// Classified structural fault. Page I/O, checksum and torn-page failures
/// remain ordinary [`PageError`] values and are never misreported as shape
/// faults.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum BTreeVerifyFault {
    UnexpectedPageType {
        page_id: PageId,
        found: u8,
        expected: BTreePageExpectation,
    },
    InvalidNode {
        page_id: PageId,
        violation: BTreeNodeViolation,
    },
    DescentCycle {
        page_id: PageId,
        first_seen_depth: u32,
        repeated_at_depth: u32,
    },
    HeightLimitExceeded {
        next_page: PageId,
        limit: u32,
    },
    LeafCycle {
        page_id: PageId,
    },
    LeafTraversalLimitExceeded {
        next_page: PageId,
        page_count_bound: u64,
    },
    OrderingViolation {
        page_id: PageId,
        entry: u64,
    },
}

/// Why one bounded structural-verification call returned.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BTreeVerifyStopReason {
    #[default]
    Complete,
    EntryLimit,
    LeafPageLimit,
    PageByteLimit,
    KeyByteLimit,
    TimeLimit,
    StructuralFault,
}

/// Outcome of one bounded structural-verification step.
#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BTreeVerifyStepReport {
    pub entries_examined: u64,
    pub leaf_pages_examined: u64,
    pub descent_pages_examined: u64,
    pub page_bytes_examined: u64,
    pub key_bytes_examined: u64,
    /// Present on the first step, which performs the bounded root descent.
    pub height: Option<u32>,
    pub next_cursor: BTreeVerifyCursor,
    pub stop_reason: BTreeVerifyStopReason,
    pub fault: Option<BTreeVerifyFault>,
    pub elapsed_millis: u64,
    pub complete: bool,
    /// Whether the portion inspected by this step had no structural fault.
    pub valid: bool,
}

struct VerifiedNode {
    leaf: bool,
    count: usize,
    next_page: PageId,
}

enum BoundedDescent {
    Leaf {
        page_id: PageId,
        height: u32,
        pages_examined: u64,
    },
    Fault {
        fault: BTreeVerifyFault,
        pages_examined: u64,
    },
}

/// A B+ tree over a page file.
#[derive(Debug)]
pub struct BTree {
    pool: Arc<BufferPool>,
    root: parking_lot::Mutex<PageId>,
    /// Set when the root moves, so the owner can persist the new one.
    root_changed: parking_lot::Mutex<bool>,
    /// Which buffer-pool budget this tree's pages compete in. The catalog
    /// declares `Metadata` so a deep posting traversal cannot evict the
    /// structure every other operation has to walk.
    cache_class: crate::pool::CacheClass,
}

impl BTree {
    /// Re-class this tree's pages. Used by the catalog, whose tree is small,
    /// hot, and touched by everything.
    pub fn in_cache_class(mut self, class: crate::pool::CacheClass) -> Self {
        self.cache_class = class;
        self
    }
}

/// A node view over a page buffer.
struct Node<'a> {
    bytes: &'a mut [u8],
}

struct NodeRef<'a> {
    bytes: &'a [u8],
}

macro_rules! node_reads {
    ($ty:ty) => {
        impl $ty {
            #[inline]
            fn read_u16(&self, at: usize) -> u16 {
                u16::from_le_bytes([self.bytes[at], self.bytes[at + 1]])
            }

            #[inline]
            fn count(&self) -> usize {
                self.read_u16(40) as usize
            }

            #[inline]
            fn free_start(&self) -> usize {
                self.read_u16(36) as usize
            }

            #[inline]
            fn free_end(&self) -> usize {
                self.read_u16(38) as usize
            }

            #[inline]
            fn page_id(&self) -> PageId {
                u64::from_le_bytes(self.bytes[8..16].try_into().unwrap())
            }

            #[inline]
            fn next_page(&self) -> PageId {
                u64::from_le_bytes(self.bytes[44..52].try_into().unwrap())
            }

            fn is_leaf(&self) -> bool {
                self.bytes[6] == PageType::BTreeLeaf as u8
            }

            #[inline]
            fn entry_at(&self, index: usize) -> (usize, usize, usize) {
                let at = PAGE_HEADER_BYTES + index * ENTRY_BYTES;
                (
                    self.read_u16(at) as usize,
                    self.read_u16(at + 2) as usize,
                    self.read_u16(at + 4) as usize,
                )
            }

            fn key(&self, index: usize) -> &[u8] {
                let (offset, key_len, _) = self.entry_at(index);
                &self.bytes[offset..offset + key_len]
            }

            fn value(&self, index: usize) -> &[u8] {
                let (offset, key_len, value_len) = self.entry_at(index);
                &self.bytes[offset + key_len..offset + key_len + value_len]
            }

            /// Index of the first entry with `key >= needle`, and whether it is
            /// an exact match.
            fn search(&self, needle: &[u8]) -> (usize, bool) {
                let mut low = 0usize;
                let mut high = self.count();
                while low < high {
                    let mid = (low + high) / 2;
                    match self.key(mid).cmp(needle) {
                        std::cmp::Ordering::Less => low = mid + 1,
                        std::cmp::Ordering::Greater => high = mid,
                        std::cmp::Ordering::Equal => return (mid, true),
                    }
                }
                (low, false)
            }

            /// The child to descend into for `needle` on an interior node.
            fn child_for(&self, needle: &[u8]) -> PageId {
                let (index, exact) = self.search(needle);
                // Separator semantics: child at `i` holds keys < key[i]. An
                // exact match therefore belongs to the RIGHT of that separator.
                let index = if exact { index + 1 } else { index };
                if index >= self.count() {
                    self.next_page()
                } else {
                    let value = self.value(index);
                    u64::from_le_bytes(value.try_into().unwrap())
                }
            }

            fn free_space(&self) -> usize {
                self.free_end().saturating_sub(self.free_start())
            }
        }
    };
}

node_reads!(Node<'_>);
node_reads!(NodeRef<'_>);

impl<'a> NodeRef<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes }
    }
}

impl<'a> Node<'a> {
    fn new(bytes: &'a mut [u8]) -> Self {
        Self { bytes }
    }

    fn init(bytes: &'a mut [u8], page_id: PageId, page_size: u32, leaf: bool) -> Self {
        bytes.fill(0);
        let page_type = if leaf {
            PageType::BTreeLeaf
        } else {
            PageType::BTreeInterior
        };
        PageHeader::new(page_id, page_type, page_size).encode(bytes);
        Self { bytes }
    }

    #[inline]
    fn write_u16(&mut self, at: usize, value: u16) {
        self.bytes[at..at + 2].copy_from_slice(&value.to_le_bytes());
    }

    fn set_count(&mut self, value: usize) {
        self.write_u16(40, value as u16);
    }

    fn set_free_start(&mut self, value: usize) {
        self.write_u16(36, value as u16);
    }

    fn set_free_end(&mut self, value: usize) {
        self.write_u16(38, value as u16);
    }

    fn set_next_page(&mut self, value: PageId) {
        self.bytes[44..52].copy_from_slice(&value.to_le_bytes());
    }

    fn write_entry(&mut self, index: usize, offset: usize, key_len: usize, value_len: usize) {
        let at = PAGE_HEADER_BYTES + index * ENTRY_BYTES;
        self.write_u16(at, offset as u16);
        self.write_u16(at + 2, key_len as u16);
        self.write_u16(at + 4, value_len as u16);
        self.write_u16(at + 6, 0);
    }

    fn required_space(key: &[u8], value: &[u8]) -> usize {
        key.len() + value.len() + ENTRY_BYTES
    }

    /// Insert or replace, keeping entries sorted. Returns false if it does not
    /// fit, leaving the node untouched.
    fn insert(&mut self, key: &[u8], value: &[u8]) -> bool {
        let (index, exact) = self.search(key);
        if exact {
            let (_, key_len, value_len) = self.entry_at(index);
            if value_len == value.len() {
                // Same size: overwrite in place.
                let (offset, _, _) = self.entry_at(index);
                let at = offset + key_len;
                self.bytes[at..at + value.len()].copy_from_slice(value);
                return true;
            }
            // Different size: remove then reinsert so the layout stays simple.
            self.remove_at(index);
        }

        if self.free_space() < Self::required_space(key, value) {
            return false;
        }

        let data_len = key.len() + value.len();
        let offset = self.free_end() - data_len;
        self.bytes[offset..offset + key.len()].copy_from_slice(key);
        self.bytes[offset + key.len()..offset + data_len].copy_from_slice(value);
        self.set_free_end(offset);

        // Shift the directory to keep entries sorted by key.
        let count = self.count();
        let (index, _) = self.search(key);
        let directory = PAGE_HEADER_BYTES;
        let from = directory + index * ENTRY_BYTES;
        let to = directory + count * ENTRY_BYTES;
        self.bytes.copy_within(from..to, from + ENTRY_BYTES);
        self.write_entry(index, offset, key.len(), value.len());
        self.set_count(count + 1);
        self.set_free_start(directory + (count + 1) * ENTRY_BYTES);
        true
    }

    /// Remove the entry at `index`. Leaves its bytes as garbage until the node
    /// is next rebuilt; nodes are rebuilt on split, and the wasted space is
    /// bounded by the page.
    fn remove_at(&mut self, index: usize) {
        let count = self.count();
        let (offset, key_len, value_len) = self.entry_at(index);
        // Zero the payload so a deleted key cannot be recovered from a page
        // image, matching the heap's behaviour.
        self.bytes[offset..offset + key_len + value_len].fill(0);

        let directory = PAGE_HEADER_BYTES;
        let from = directory + (index + 1) * ENTRY_BYTES;
        let to = directory + count * ENTRY_BYTES;
        self.bytes.copy_within(from..to, from - ENTRY_BYTES);
        self.set_count(count - 1);
        self.set_free_start(directory + (count - 1) * ENTRY_BYTES);
    }

    fn remove(&mut self, key: &[u8]) -> bool {
        let (index, exact) = self.search(key);
        if !exact {
            return false;
        }
        self.remove_at(index);
        true
    }

    /// All entries, for rebuilding during a split.
    fn entries(&self) -> Vec<(Vec<u8>, Vec<u8>)> {
        (0..self.count())
            .map(|index| (self.key(index).to_vec(), self.value(index).to_vec()))
            .collect()
    }

    /// Replace the node's contents with `entries`, which must be sorted.
    fn rebuild(&mut self, entries: &[(Vec<u8>, Vec<u8>)], page_size: u32) {
        let page_id = self.page_id();
        let leaf = self.is_leaf();
        let next = self.next_page();
        let data_top = page_size as usize - PAGE_TRAILER_BYTES;
        self.bytes[PAGE_HEADER_BYTES..data_top].fill(0);
        self.set_count(0);
        self.set_free_start(PAGE_HEADER_BYTES);
        self.set_free_end(data_top);
        self.set_next_page(next);
        debug_assert_eq!(self.page_id(), page_id);
        debug_assert_eq!(self.is_leaf(), leaf);

        for (key, value) in entries {
            let inserted = self.insert(key, value);
            debug_assert!(inserted, "rebuild overflowed a node");
        }
    }
}

fn structural_fault(page_id: PageId, violation: BTreeNodeViolation) -> BTreeVerifyFault {
    BTreeVerifyFault::InvalidNode { page_id, violation }
}

/// Validate a node without trusting its slot directory first.
///
/// The ordinary node accessors are intentionally lean and assume a page the
/// engine wrote. An integrity verifier has the opposite trust boundary: every
/// offset and length comes from potentially damaged storage, so it must prove
/// all ranges before taking a slice. Keeping this parser separate makes it
/// impossible for malformed directory data to turn a verification run into a
/// bounds panic.
fn validate_node(
    page_id: PageId,
    bytes: &[u8],
    expectation: BTreePageExpectation,
) -> std::result::Result<VerifiedNode, BTreeVerifyFault> {
    let found = bytes.get(6).copied().unwrap_or_default();
    let leaf = match PageType::from_u8(found) {
        Some(PageType::BTreeLeaf) => true,
        Some(PageType::BTreeInterior) => false,
        _ => {
            return Err(BTreeVerifyFault::UnexpectedPageType {
                page_id,
                found,
                expected: expectation,
            });
        }
    };
    if expectation == BTreePageExpectation::Leaf && !leaf {
        return Err(BTreeVerifyFault::UnexpectedPageType {
            page_id,
            found,
            expected: expectation,
        });
    }

    let stored_page_id = bytes
        .get(8..16)
        .and_then(|value| value.try_into().ok())
        .map(u64::from_le_bytes)
        .unwrap_or_default();
    if stored_page_id != page_id {
        return Err(structural_fault(
            page_id,
            BTreeNodeViolation::PageIdMismatch {
                stored: stored_page_id,
            },
        ));
    }

    let data_top = bytes.len().saturating_sub(PAGE_TRAILER_BYTES);
    let read_u16 = |at: usize| {
        bytes
            .get(at..at + 2)
            .and_then(|value| value.try_into().ok())
            .map(u16::from_le_bytes)
            .unwrap_or_default() as usize
    };
    let count = read_u16(40);
    let Some(directory_bytes) = count.checked_mul(ENTRY_BYTES) else {
        return Err(structural_fault(
            page_id,
            BTreeNodeViolation::DirectoryOutOfBounds,
        ));
    };
    let Some(directory_end) = PAGE_HEADER_BYTES.checked_add(directory_bytes) else {
        return Err(structural_fault(
            page_id,
            BTreeNodeViolation::DirectoryOutOfBounds,
        ));
    };
    let free_start = read_u16(36);
    let free_end = read_u16(38);
    if directory_end > data_top || free_start != directory_end {
        return Err(structural_fault(
            page_id,
            BTreeNodeViolation::DirectoryOutOfBounds,
        ));
    }
    if free_end < free_start || free_end > data_top {
        return Err(structural_fault(
            page_id,
            BTreeNodeViolation::FreeSpaceOutOfBounds,
        ));
    }
    if !leaf && count == 0 {
        return Err(structural_fault(
            page_id,
            BTreeNodeViolation::InteriorWithoutSeparator,
        ));
    }

    let mut intervals = Vec::with_capacity(count);
    let mut previous_key: Option<&[u8]> = None;
    for entry in 0..count {
        let directory = PAGE_HEADER_BYTES + entry * ENTRY_BYTES;
        let offset = read_u16(directory);
        let key_len = read_u16(directory + 2);
        let value_len = read_u16(directory + 4);
        if key_len == 0 {
            return Err(structural_fault(
                page_id,
                BTreeNodeViolation::EmptyKey {
                    entry: entry as u64,
                },
            ));
        }
        let Some(key_end) = offset.checked_add(key_len) else {
            return Err(structural_fault(
                page_id,
                BTreeNodeViolation::EntryOutOfBounds {
                    entry: entry as u64,
                },
            ));
        };
        let Some(entry_end) = key_end.checked_add(value_len) else {
            return Err(structural_fault(
                page_id,
                BTreeNodeViolation::EntryOutOfBounds {
                    entry: entry as u64,
                },
            ));
        };
        if offset < free_end || entry_end > data_top {
            return Err(structural_fault(
                page_id,
                BTreeNodeViolation::EntryOutOfBounds {
                    entry: entry as u64,
                },
            ));
        }
        let key = &bytes[offset..key_end];
        if previous_key.is_some_and(|previous| previous >= key) {
            return Err(structural_fault(
                page_id,
                BTreeNodeViolation::KeysOutOfOrder {
                    previous: entry.saturating_sub(1) as u64,
                    current: entry as u64,
                },
            ));
        }
        previous_key = Some(key);

        if !leaf {
            if value_len != std::mem::size_of::<PageId>() {
                return Err(structural_fault(
                    page_id,
                    BTreeNodeViolation::InvalidChildWidth {
                        entry: entry as u64,
                        found: value_len as u64,
                    },
                ));
            }
            let child = u64::from_le_bytes(bytes[key_end..entry_end].try_into().unwrap());
            if child == 0 {
                return Err(structural_fault(
                    page_id,
                    BTreeNodeViolation::MissingChild {
                        entry: Some(entry as u64),
                    },
                ));
            }
        }
        intervals.push((offset, entry_end, entry));
    }

    intervals.sort_unstable_by_key(|(start, _, _)| *start);
    for adjacent in intervals.windows(2) {
        let (_, first_end, first_entry) = adjacent[0];
        let (second_start, _, second_entry) = adjacent[1];
        if first_end > second_start {
            return Err(structural_fault(
                page_id,
                BTreeNodeViolation::OverlappingEntries {
                    first: first_entry as u64,
                    second: second_entry as u64,
                },
            ));
        }
    }

    let next_page = bytes
        .get(44..52)
        .and_then(|value| value.try_into().ok())
        .map(u64::from_le_bytes)
        .unwrap_or_default();
    if !leaf && next_page == 0 {
        return Err(structural_fault(
            page_id,
            BTreeNodeViolation::MissingChild { entry: None },
        ));
    }
    Ok(VerifiedNode {
        leaf,
        count,
        next_page,
    })
}

/// One step of a root-to-leaf descent.
struct PathStep {
    page_id: PageId,
}

impl BTree {
    /// Create an empty tree with a freshly allocated root.
    ///
    /// The root is returned via [`Self::root_page`] rather than published to the
    /// superblock: a page file holds several trees (the primary index, the
    /// catalog), and only the owner knows where each root belongs. Coupling the
    /// tree to the superblock's single root field would let two trees overwrite
    /// each other's root — which is a silent, total loss of one of them.
    pub fn create_at(pool: Arc<BufferPool>) -> Result<Self> {
        let page_size = pool.page_size();
        let root = pool.store().allocate(PageType::BTreeLeaf)?;
        {
            let mut guard = pool.get_mut(root)?;
            Node::init(guard.bytes_mut(), root, page_size, true);
        }
        Ok(Self {
            pool,
            root: parking_lot::Mutex::new(root),
            root_changed: parking_lot::Mutex::new(false),
            cache_class: crate::pool::CacheClass::Search,
        })
    }

    /// Open a tree whose root the caller already knows.
    pub fn open_at(pool: Arc<BufferPool>, root: PageId) -> Result<Self> {
        if root == 0 {
            return Self::create_at(pool);
        }
        Ok(Self {
            pool,
            root: parking_lot::Mutex::new(root),
            root_changed: parking_lot::Mutex::new(false),
            cache_class: crate::pool::CacheClass::Search,
        })
    }

    /// Whether the root has moved since the flag was last cleared.
    ///
    /// A root split changes the tree's entry point, and whoever records that
    /// root has to write the new one down or the next open finds the old,
    /// now-partial tree. Making the change *observable* means the owner cannot
    /// silently miss it.
    pub fn take_root_changed(&self) -> bool {
        std::mem::replace(&mut self.root_changed.lock(), false)
    }

    /// Create an empty tree and publish its root to the superblock.
    ///
    /// Only correct when the file holds exactly one tree. Retained for tests
    /// and simple single-tree uses.
    pub fn create(pool: Arc<BufferPool>) -> Result<Self> {
        let tree = Self::create_at(pool)?;
        tree.pool.flush_all()?;
        tree.pool.store().publish_root(tree.root_page(), 0)?;
        Ok(tree)
    }

    /// Open the single tree whose root is published in the superblock.
    pub fn open(pool: Arc<BufferPool>) -> Result<Self> {
        let root = pool.store().root_page();
        Self::open_at(pool, root)
    }

    pub fn root_page(&self) -> PageId {
        *self.root.lock()
    }

    /// Validate the immutable envelope for the first step of a structural
    /// sweep without touching a tree page.
    pub fn validate_new_verify_limits(&self, limits: &BTreeVerifyLimits) -> Result<()> {
        limits.validate()?;
        let page_size = u64::from(self.pool.page_size());
        let maximum_descent_bytes = u64::from(limits.max_height)
            .checked_mul(page_size)
            .ok_or_else(|| PageError::InvalidMaintenanceLimits {
                reason: "structural verification descent byte envelope overflowed".to_string(),
            })?;
        if limits.max_page_bytes < maximum_descent_bytes {
            return Err(PageError::InvalidMaintenanceLimits {
                reason: format!(
                    "a new sweep's max_page_bytes must cover the configured height guard ({maximum_descent_bytes} bytes)"
                ),
            });
        }
        Ok(())
    }

    /// Descend to the leaf that would hold `key`, recording the path.
    fn descend(&self, key: &[u8]) -> Result<Vec<PathStep>> {
        let mut path = Vec::new();
        let mut current = self.root_page();
        loop {
            path.push(PathStep { page_id: current });
            // The guard is dropped before descending, so a deep tree does not
            // hold one frame per level.
            let next = {
                let guard = self.pool.get_in_class(current, self.cache_class)?;
                let node = NodeRef::new(guard.bytes());
                if node.is_leaf() {
                    break;
                }
                node.child_for(key)
            };
            if next == 0 {
                return Err(PageError::UnexpectedPageType {
                    page_id: current,
                    found: PageType::BTreeInterior as u8,
                    expected: PageType::BTreeLeaf as u8,
                });
            }
            current = next;
            if path.len() > 64 {
                // A cycle would otherwise loop forever; 64 levels is far beyond
                // any real tree (a 4-level tree already holds billions of rows).
                return Err(PageError::UnexpectedPageType {
                    page_id: current,
                    found: 0,
                    expected: PageType::BTreeLeaf as u8,
                });
            }
        }
        Ok(path)
    }

    /// Look up a key.
    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let path = self.descend(key)?;
        let leaf = path.last().expect("descent always records a leaf").page_id;
        let guard = self.pool.get_in_class(leaf, self.cache_class)?;
        let node = NodeRef::new(guard.bytes());
        let (index, exact) = node.search(key);
        Ok(exact.then(|| node.value(index).to_vec()))
    }

    pub fn contains(&self, key: &[u8]) -> Result<bool> {
        Ok(self.get(key)?.is_some())
    }

    /// Insert or replace a key.
    pub fn insert(&self, key: &[u8], value: &[u8]) -> Result<()> {
        if key.is_empty() {
            return Err(PageError::ValueTooLarge {
                page_id: 0,
                len: 0,
                free: 0,
            });
        }
        let page_size = self.pool.page_size();
        let usable = crate::page::usable_bytes(page_size);
        if Node::required_space(key, value) * 2 > usable {
            // A node must hold at least two entries or a split cannot make
            // progress. Refusing here beats an infinite split loop later.
            return Err(PageError::ValueTooLarge {
                page_id: 0,
                len: key.len() + value.len(),
                free: usable / 2,
            });
        }

        let path = self.descend(key)?;
        let leaf = path.last().unwrap().page_id;

        {
            let mut guard = self.pool.get_mut(leaf)?;
            let mut node = Node::new(guard.bytes_mut());
            if node.insert(key, value) {
                return Ok(());
            }
        }

        // The leaf is full: split and retry.
        self.split_and_insert(&path, key, value)
    }

    /// Split the leaf at the end of `path`, propagating upward as needed.
    fn split_and_insert(&self, path: &[PathStep], key: &[u8], value: &[u8]) -> Result<()> {
        let page_size = self.pool.page_size();
        let leaf_id = path.last().unwrap().page_id;

        // Gather the leaf's entries plus the new one, in order.
        let mut entries = {
            let guard = self.pool.get_in_class(leaf_id, self.cache_class)?;
            Node::new(&mut guard.bytes().to_vec()).entries()
        };
        match entries.binary_search_by(|(existing, _)| existing.as_slice().cmp(key)) {
            Ok(index) => entries[index].1 = value.to_vec(),
            Err(index) => entries.insert(index, (key.to_vec(), value.to_vec())),
        }

        let middle = entries.len() / 2;
        let (left_entries, right_entries) = entries.split_at(middle);
        // The separator is the smallest key in the right half. Under this
        // tree's convention (child i holds keys < key i) it therefore points at
        // the LEFT sibling: everything below it stayed behind.
        let separator = right_entries[0].0.clone();

        // Allocate the right sibling and write it before touching the left, so
        // a crash mid-split leaves the original leaf complete and the new page
        // unreferenced.
        let right_id = self.pool.store().allocate(PageType::BTreeLeaf)?;
        let old_next = {
            let guard = self.pool.get_in_class(leaf_id, self.cache_class)?;
            NodeRef::new(guard.bytes()).next_page()
        };
        {
            let mut guard = self.pool.get_mut(right_id)?;
            let mut right = Node::init(guard.bytes_mut(), right_id, page_size, true);
            right.set_next_page(old_next);
            right.rebuild(right_entries, page_size);
        }
        {
            let mut guard = self.pool.get_mut(leaf_id)?;
            let mut left = Node::new(guard.bytes_mut());
            left.rebuild(left_entries, page_size);
            left.set_next_page(right_id);
        }

        self.insert_separator(path, path.len() - 1, separator, leaf_id, right_id)
    }

    /// Record in the parent that `left_child` was split into
    /// `left_child` (keys `< separator`) and `right_child` (keys `>= separator`).
    ///
    /// # The direction that is easy to get wrong
    ///
    /// Entry `i` means "child `i` holds keys `< key i`", so the child covered by
    /// entry `i` spans `[key i-1, key i)`. Splitting it at `separator` means:
    ///
    /// - the left half now covers `[key i-1, separator)` — a NEW entry
    ///   `(separator -> left_child)`;
    /// - the right half covers `[separator, key i)` — the EXISTING entry keeps
    ///   its key and is repointed to `right_child`.
    ///
    /// Inserting `(separator -> right_child)` instead is the natural-looking
    /// mistake, and it silently claims the right sibling holds keys below the
    /// separator. Lookups then descend into the wrong subtree for exactly the
    /// keys that moved, so the tree still passes an ordering check while losing
    /// rows.
    ///
    /// When the split child was the rightmost (reached through `next_page`
    /// rather than an entry), the new entry is appended and `next_page` takes
    /// the right half.
    fn insert_separator(
        &self,
        path: &[PathStep],
        level: usize,
        separator: Vec<u8>,
        left_child: PageId,
        right_child: PageId,
    ) -> Result<()> {
        let page_size = self.pool.page_size();
        let usable = crate::page::usable_bytes(page_size);

        if level == 0 {
            // The root split: build a new root above it.
            let new_root = self.pool.store().allocate(PageType::BTreeInterior)?;
            {
                let mut guard = self.pool.get_mut(new_root)?;
                let mut node = Node::init(guard.bytes_mut(), new_root, page_size, false);
                node.insert(&separator, &left_child.to_le_bytes());
                node.set_next_page(right_child);
            }
            // The new root is made durable BEFORE anyone is told about it. A
            // crash in between leaves the old root intact and the new one
            // unreferenced — garbage, but never a dangling root.
            self.pool.flush_all()?;
            *self.root.lock() = new_root;
            *self.root_changed.lock() = true;
            return Ok(());
        }

        let parent_id = path[level - 1].page_id;
        let (mut entries, rightmost) = {
            let guard = self.pool.get(parent_id)?;
            let node = NodeRef::new(guard.bytes());
            let entries: Vec<(Vec<u8>, Vec<u8>)> = (0..node.count())
                .map(|index| (node.key(index).to_vec(), node.value(index).to_vec()))
                .collect();
            (entries, node.next_page())
        };

        let child_bytes = |page: PageId| page.to_le_bytes().to_vec();
        let position = entries
            .iter()
            .position(|(_, value)| value.as_slice() == child_bytes(left_child).as_slice());

        let new_rightmost = match position {
            Some(index) => {
                entries.insert(index, (separator.clone(), child_bytes(left_child)));
                entries[index + 1].1 = child_bytes(right_child);
                rightmost
            }
            None if rightmost == left_child => {
                entries.push((separator.clone(), child_bytes(left_child)));
                right_child
            }
            None => {
                // The parent does not reference the child we just split. The
                // tree is inconsistent; refuse rather than write more damage.
                return Err(PageError::UnexpectedPageType {
                    page_id: parent_id,
                    found: PageType::BTreeInterior as u8,
                    expected: PageType::BTreeLeaf as u8,
                });
            }
        };

        let needed: usize = entries
            .iter()
            .map(|(key, value)| key.len() + value.len() + ENTRY_BYTES)
            .sum();

        if needed <= usable {
            let mut guard = self.pool.get_mut(parent_id)?;
            let mut node = Node::new(guard.bytes_mut());
            node.rebuild(&entries, page_size);
            node.set_next_page(new_rightmost);
            return Ok(());
        }

        // The parent is full too: split it, promoting its middle separator.
        //
        // An interior split MOVES the middle key up rather than copying it: the
        // child it pointed at becomes the left node's rightmost. Copying it
        // would leave the same separator at two levels and route a lookup into
        // a subtree that no longer covers it.
        let middle = entries.len() / 2;
        let promoted = entries[middle].0.clone();
        let promoted_child = u64::from_le_bytes(entries[middle].1.as_slice().try_into().unwrap());
        let left_entries = entries[..middle].to_vec();
        let right_entries = entries[middle + 1..].to_vec();

        let new_interior = self.pool.store().allocate(PageType::BTreeInterior)?;
        {
            let mut guard = self.pool.get_mut(new_interior)?;
            let mut node = Node::init(guard.bytes_mut(), new_interior, page_size, false);
            node.rebuild(&right_entries, page_size);
            node.set_next_page(new_rightmost);
        }
        {
            let mut guard = self.pool.get_mut(parent_id)?;
            let mut node = Node::new(guard.bytes_mut());
            node.rebuild(&left_entries, page_size);
            node.set_next_page(promoted_child);
        }

        self.insert_separator(path, level - 1, promoted, parent_id, new_interior)
    }

    /// Remove a key. Returns whether it was present.
    ///
    /// Leaves are not merged when they empty out: the space stays in the tree
    /// and is reused by later inserts into the same range. Merging is deferred
    /// to the roadmap's Phase 4 "split, merge, and root-change paths"; skipping
    /// it here means a tree that has had a large range deleted keeps its pages,
    /// which is a space cost rather than a correctness one.
    pub fn remove(&self, key: &[u8]) -> Result<bool> {
        let path = self.descend(key)?;
        let leaf = path.last().unwrap().page_id;
        let mut guard = self.pool.get_mut(leaf)?;
        let mut node = Node::new(guard.bytes_mut());
        Ok(node.remove(key))
    }

    /// The leftmost leaf, for a full scan.
    fn leftmost_leaf(&self) -> Result<PageId> {
        let mut current = self.root_page();
        loop {
            let next = {
                let guard = self.pool.get_in_class(current, self.cache_class)?;
                let node = NodeRef::new(guard.bytes());
                if node.is_leaf() {
                    return Ok(current);
                }
                if node.count() == 0 {
                    node.next_page()
                } else {
                    u64::from_le_bytes(node.value(0).try_into().unwrap())
                }
            };
            current = next;
        }
    }

    /// Iterate entries with `key >= start`, in ascending key order.
    ///
    /// Walks leaf sibling links, so it pins one leaf at a time regardless of how
    /// much the range covers — the bounded range cursor Phase 5 needs.
    pub fn range(&self, start: &[u8]) -> Result<BTreeRange<'_>> {
        let leaf = if start.is_empty() {
            self.leftmost_leaf()?
        } else {
            self.descend(start)?.last().unwrap().page_id
        };
        let (entries, next) = self.leaf_entries_from(leaf, start)?;
        Ok(BTreeRange {
            tree: self,
            entries,
            index: 0,
            next_leaf: next,
        })
    }

    /// Every entry, ascending.
    pub fn iter(&self) -> Result<BTreeRange<'_>> {
        self.range(b"")
    }

    /// Iterate entries with `key < bound` in DESCENDING key order; `None`
    /// starts at the greatest key. See [`BTreeRangeRev`] for the mechanism.
    pub fn range_rev_below(&self, bound: Option<&[u8]>) -> Result<BTreeRangeRev<'_>> {
        let (path, leaf) = match bound {
            Some(bound) => self.descend_with_indexes(bound)?,
            None => self.descend_rightmost_from(self.root_page(), Vec::new())?,
        };
        let (entries, _) = self.leaf_entries_from(leaf, b"")?;
        let index = match bound {
            Some(bound) => entries.partition_point(|(key, _)| key.as_slice() < bound),
            None => entries.len(),
        };
        Ok(BTreeRangeRev {
            tree: self,
            entries,
            index,
            path,
            exhausted: false,
        })
    }

    /// Descend to the leaf for `key`, recording the child index taken at each
    /// interior node (index `count()` is the rightmost `next_page` child).
    fn descend_with_indexes(&self, key: &[u8]) -> Result<(Vec<(PageId, usize)>, PageId)> {
        let mut path: Vec<(PageId, usize)> = Vec::new();
        let mut current = self.root_page();
        loop {
            let (is_leaf, child_index, next) = {
                let guard = self.pool.get_in_class(current, self.cache_class)?;
                let node = NodeRef::new(guard.bytes());
                if node.is_leaf() {
                    (true, 0, 0)
                } else {
                    let (index, exact) = node.search(key);
                    // Separator semantics: child at `i` holds keys < key[i];
                    // an exact match belongs to the right of its separator.
                    let index = if exact { index + 1 } else { index };
                    let child = Self::child_at(&node, index);
                    (false, index, child)
                }
            };
            if is_leaf {
                return Ok((path, current));
            }
            if next == 0 {
                return Err(PageError::UnexpectedPageType {
                    page_id: current,
                    found: PageType::BTreeInterior as u8,
                    expected: PageType::BTreeLeaf as u8,
                });
            }
            path.push((current, child_index));
            current = next;
            if path.len() > 64 {
                return Err(PageError::UnexpectedPageType {
                    page_id: current,
                    found: 0,
                    expected: PageType::BTreeLeaf as u8,
                });
            }
        }
    }

    /// Descend to the RIGHTMOST leaf under `from`, appending to `path`.
    fn descend_rightmost_from(
        &self,
        from: PageId,
        mut path: Vec<(PageId, usize)>,
    ) -> Result<(Vec<(PageId, usize)>, PageId)> {
        let mut current = from;
        loop {
            let (is_leaf, child_index, next) = {
                let guard = self.pool.get_in_class(current, self.cache_class)?;
                let node = NodeRef::new(guard.bytes());
                if node.is_leaf() {
                    (true, 0, 0)
                } else {
                    let index = node.count();
                    (false, index, Self::child_at(&node, index))
                }
            };
            if is_leaf {
                return Ok((path, current));
            }
            if next == 0 {
                return Err(PageError::UnexpectedPageType {
                    page_id: current,
                    found: PageType::BTreeInterior as u8,
                    expected: PageType::BTreeLeaf as u8,
                });
            }
            path.push((current, child_index));
            current = next;
            if path.len() > 64 {
                return Err(PageError::UnexpectedPageType {
                    page_id: current,
                    found: 0,
                    expected: PageType::BTreeLeaf as u8,
                });
            }
        }
    }

    /// Step `path` to the leaf immediately LEFT of the one it currently
    /// denotes and return that leaf's entries; `None` when the current leaf
    /// is the leftmost. `path` is updated in place for the next step.
    fn previous_leaf(
        &self,
        path: &mut Vec<(PageId, usize)>,
    ) -> Result<Option<Vec<(Vec<u8>, Vec<u8>)>>> {
        loop {
            let Some((page_id, child_index)) = path.pop() else {
                return Ok(None);
            };
            if child_index == 0 {
                continue;
            }
            let target = child_index - 1;
            let child = {
                let guard = self.pool.get(page_id)?;
                let node = NodeRef::new(guard.bytes());
                if node.is_leaf() {
                    // A concurrent structure change replaced this interior
                    // with a leaf; the recorded position is meaningless, so
                    // end the scan rather than yield from the wrong subtree.
                    return Ok(None);
                }
                // Clamp against a concurrent shrink of this node.
                let target = target.min(node.count());
                Self::child_at(&node, target)
            };
            if child == 0 {
                return Ok(None);
            }
            path.push((page_id, target));
            let (path_rest, leaf) = self.descend_rightmost_from(child, std::mem::take(path))?;
            *path = path_rest;
            let (entries, _) = self.leaf_entries_from(leaf, b"")?;
            if entries.is_empty() {
                // An empty leaf (post-merge residue): keep walking left.
                continue;
            }
            return Ok(Some(entries));
        }
    }

    /// Child page at `index`, where `index == count()` is the rightmost
    /// (`next_page`) child.
    fn child_at(node: &NodeRef<'_>, index: usize) -> PageId {
        if index >= node.count() {
            node.next_page()
        } else {
            u64::from_le_bytes(node.value(index).try_into().unwrap())
        }
    }

    fn leaf_entries_from(
        &self,
        leaf: PageId,
        start: &[u8],
    ) -> Result<(Vec<(Vec<u8>, Vec<u8>)>, PageId)> {
        let guard = self.pool.get_in_class(leaf, self.cache_class)?;
        let node = NodeRef::new(guard.bytes());
        let from = if start.is_empty() {
            0
        } else {
            node.search(start).0
        };
        let entries = (from..node.count())
            .map(|index| (node.key(index).to_vec(), node.value(index).to_vec()))
            .collect();
        let next_page = node.next_page();
        if next_page != 0 {
            // A sibling pointer is an exact physical dependency, not a range
            // guess. Queueing it performs no I/O on the query path and the
            // bounded host worker may have it ready when this leaf drains.
            self.pool.request_read_ahead([next_page]);
        }
        Ok((entries, next_page))
    }

    /// The first entry with `key >= start`, without materializing a leaf.
    ///
    /// [`Self::range`] copies every entry of each leaf it touches into owned
    /// `Vec`s so the cursor can outlive the page pin. That is the right shape
    /// for a scan and completely the wrong shape for "give me one entry": a
    /// caller that only wants the first match pays for the whole leaf.
    ///
    /// The free-space map calls this on EVERY insert, where it profiled as ~17%
    /// of ingest CPU (`leaf_entries_from` plus the allocations it drove). Here
    /// the entry is copied out and the pin released immediately, so the cost is
    /// one descent and one small copy.
    pub fn first_at_or_after(&self, start: &[u8]) -> Result<Option<(Vec<u8>, Vec<u8>)>> {
        let path = self.descend(start)?;
        let mut leaf = path.last().expect("descent always records a leaf").page_id;
        let mut first_leaf = true;

        loop {
            let next = {
                let guard = self.pool.get_in_class(leaf, self.cache_class)?;
                let node = NodeRef::new(guard.bytes());
                // Only the leaf the descent landed on needs the search; later
                // siblings hold strictly greater keys, so their first entry is
                // the answer.
                let index = if first_leaf { node.search(start).0 } else { 0 };
                if index < node.count() {
                    return Ok(Some((node.key(index).to_vec(), node.value(index).to_vec())));
                }
                let next_page = node.next_page();
                if next_page != 0 {
                    self.pool.request_read_ahead([next_page]);
                }
                next_page
            };
            if next == 0 {
                return Ok(None);
            }
            leaf = next;
            first_leaf = false;
        }
    }

    /// Number of entries in the tree. Walks every leaf — a diagnostic, not a
    /// hot path.
    pub fn len(&self) -> Result<usize> {
        Ok(self.iter()?.count())
    }

    pub fn is_empty(&self) -> Result<bool> {
        Ok(self.len()? == 0)
    }

    /// Height of the tree, root to leaf.
    pub fn height(&self) -> Result<usize> {
        let mut height = 1;
        let mut current = self.root_page();
        loop {
            let next = {
                let guard = self.pool.get_in_class(current, self.cache_class)?;
                let node = NodeRef::new(guard.bytes());
                if node.is_leaf() {
                    return Ok(height);
                }
                if node.count() == 0 {
                    node.next_page()
                } else {
                    u64::from_le_bytes(node.value(0).try_into().unwrap())
                }
            };
            current = next;
            height += 1;
        }
    }

    fn bounded_leftmost_descent(&self, max_height: u32) -> Result<BoundedDescent> {
        let mut current = self.root_page();
        let mut seen = FxHashMap::default();
        let mut pages_examined = 0_u64;

        for depth in 1..=max_height {
            if let Some(first_seen_depth) = seen.insert(current, depth) {
                return Ok(BoundedDescent::Fault {
                    fault: BTreeVerifyFault::DescentCycle {
                        page_id: current,
                        first_seen_depth,
                        repeated_at_depth: depth,
                    },
                    pages_examined,
                });
            }
            let guard = self.pool.get_in_class(current, self.cache_class)?;
            pages_examined = pages_examined.saturating_add(1);
            let node = match validate_node(current, guard.bytes(), BTreePageExpectation::Node) {
                Ok(node) => node,
                Err(fault) => {
                    return Ok(BoundedDescent::Fault {
                        fault,
                        pages_examined,
                    });
                }
            };
            if node.leaf {
                return Ok(BoundedDescent::Leaf {
                    page_id: current,
                    height: depth,
                    pages_examined,
                });
            }
            let node_ref = NodeRef::new(guard.bytes());
            current = u64::from_le_bytes(node_ref.value(0).try_into().unwrap());
        }

        Ok(BoundedDescent::Fault {
            fault: BTreeVerifyFault::HeightLimitExceeded {
                next_page: current,
                limit: max_height,
            },
            pages_examined,
        })
    }

    /// Verify one bounded, restartable slice of B+ tree structure.
    ///
    /// The caller must hold the store's structure read lock for this complete
    /// call. A call never retains more than one page guard, one page-sized set
    /// of live-entry intervals during validation, a step-bounded leaf cycle
    /// set, and one cursor key. Across calls, keys inserted behind the cursor
    /// intentionally belong to the next online sweep.
    pub fn verify_step(
        &self,
        cursor: BTreeVerifyCursor,
        limits: BTreeVerifyLimits,
    ) -> Result<BTreeVerifyStepReport> {
        limits.validate()?;
        validate_verify_cursor(&cursor, &limits, self.pool.store().page_count())?;

        let page_size = u64::from(self.pool.page_size());
        if limits.max_page_bytes < page_size {
            return Err(PageError::InvalidMaintenanceLimits {
                reason: format!(
                    "max_page_bytes must cover at least one {}-byte page",
                    self.pool.page_size()
                ),
            });
        }
        if cursor.next_leaf.is_none() {
            self.validate_new_verify_limits(&limits)?;
        }

        let started = Instant::now();
        let deadline = Duration::from_millis(limits.max_duration_millis);
        let mut report = BTreeVerifyStepReport::default();
        report.valid = true;

        let current_page_count = self.pool.store().page_count();
        let (mut current_leaf, mut page_count_bound) = match cursor.next_leaf {
            Some(page_id) => (page_id, cursor.page_count_bound.max(current_page_count)),
            None => match self.bounded_leftmost_descent(limits.max_height)? {
                BoundedDescent::Leaf {
                    page_id,
                    height,
                    pages_examined,
                } => {
                    report.height = Some(height);
                    report.descent_pages_examined = pages_examined;
                    report.page_bytes_examined = pages_examined.saturating_mul(page_size);
                    (page_id, current_page_count)
                }
                BoundedDescent::Fault {
                    fault,
                    pages_examined,
                } => {
                    report.descent_pages_examined = pages_examined;
                    report.page_bytes_examined = pages_examined.saturating_mul(page_size);
                    report.fault = Some(fault);
                    return Ok(finish_verify_step(
                        report,
                        started,
                        BTreeVerifyStopReason::StructuralFault,
                        BTreeVerifyCursor::default(),
                    ));
                }
            },
        };

        let mut previous_key = cursor.after_key;
        let mut resume_within_leaf = cursor.resume_within_leaf;
        let mut sweep_leaf_pages = cursor.leaf_pages_traversed;
        let mut seen_this_step = FxHashSet::default();
        let mut made_progress = cursor.next_leaf.is_none();

        loop {
            if current_leaf == 0 {
                return Ok(finish_verify_step(
                    report,
                    started,
                    BTreeVerifyStopReason::Complete,
                    BTreeVerifyCursor::default(),
                ));
            }
            if report.leaf_pages_examined >= limits.max_leaf_pages {
                return Ok(finish_verify_step(
                    report,
                    started,
                    BTreeVerifyStopReason::LeafPageLimit,
                    next_verify_cursor(
                        current_leaf,
                        previous_key,
                        resume_within_leaf,
                        sweep_leaf_pages,
                        page_count_bound,
                    ),
                ));
            }
            if limits
                .max_page_bytes
                .saturating_sub(report.page_bytes_examined)
                < page_size
            {
                return Ok(finish_verify_step(
                    report,
                    started,
                    BTreeVerifyStopReason::PageByteLimit,
                    next_verify_cursor(
                        current_leaf,
                        previous_key,
                        resume_within_leaf,
                        sweep_leaf_pages,
                        page_count_bound,
                    ),
                ));
            }
            if made_progress && started.elapsed() >= deadline {
                return Ok(finish_verify_step(
                    report,
                    started,
                    BTreeVerifyStopReason::TimeLimit,
                    next_verify_cursor(
                        current_leaf,
                        previous_key,
                        resume_within_leaf,
                        sweep_leaf_pages,
                        page_count_bound,
                    ),
                ));
            }

            if sweep_leaf_pages >= page_count_bound {
                let latest_page_count = self.pool.store().page_count();
                if latest_page_count > page_count_bound {
                    page_count_bound = latest_page_count;
                } else {
                    report.fault = Some(BTreeVerifyFault::LeafTraversalLimitExceeded {
                        next_page: current_leaf,
                        page_count_bound,
                    });
                    return Ok(finish_verify_step(
                        report,
                        started,
                        BTreeVerifyStopReason::StructuralFault,
                        BTreeVerifyCursor::default(),
                    ));
                }
            }
            if !seen_this_step.insert(current_leaf) {
                report.fault = Some(BTreeVerifyFault::LeafCycle {
                    page_id: current_leaf,
                });
                return Ok(finish_verify_step(
                    report,
                    started,
                    BTreeVerifyStopReason::StructuralFault,
                    BTreeVerifyCursor::default(),
                ));
            }

            let guard = self.pool.get(current_leaf)?;
            report.leaf_pages_examined = report.leaf_pages_examined.saturating_add(1);
            report.page_bytes_examined = report.page_bytes_examined.saturating_add(page_size);
            let node = match validate_node(current_leaf, guard.bytes(), BTreePageExpectation::Leaf)
            {
                Ok(node) => node,
                Err(fault) => {
                    report.fault = Some(fault);
                    return Ok(finish_verify_step(
                        report,
                        started,
                        BTreeVerifyStopReason::StructuralFault,
                        BTreeVerifyCursor::default(),
                    ));
                }
            };
            let node_ref = NodeRef::new(guard.bytes());
            let mut previous_is_in_current_leaf = resume_within_leaf;

            for entry in 0..node.count {
                let key = node_ref.key(entry);
                if resume_within_leaf
                    && previous_key
                        .as_deref()
                        .is_some_and(|previous| key <= previous)
                {
                    continue;
                }
                if previous_key
                    .as_deref()
                    .is_some_and(|previous| previous >= key)
                {
                    report.fault = Some(BTreeVerifyFault::OrderingViolation {
                        page_id: current_leaf,
                        entry: entry as u64,
                    });
                    return Ok(finish_verify_step(
                        report,
                        started,
                        BTreeVerifyStopReason::StructuralFault,
                        BTreeVerifyCursor::default(),
                    ));
                }
                if key.len() > limits.max_cursor_key_bytes {
                    return Err(PageError::InvalidMaintenanceLimits {
                        reason: format!(
                            "B-tree verification key is {} bytes, above the {}-byte cursor bound",
                            key.len(),
                            limits.max_cursor_key_bytes
                        ),
                    });
                }
                if report.entries_examined >= limits.max_entries {
                    return Ok(finish_verify_step(
                        report,
                        started,
                        BTreeVerifyStopReason::EntryLimit,
                        next_verify_cursor(
                            current_leaf,
                            previous_key,
                            previous_is_in_current_leaf,
                            sweep_leaf_pages,
                            page_count_bound,
                        ),
                    ));
                }
                if limits
                    .max_key_bytes
                    .saturating_sub(report.key_bytes_examined)
                    < key.len() as u64
                {
                    return Ok(finish_verify_step(
                        report,
                        started,
                        BTreeVerifyStopReason::KeyByteLimit,
                        next_verify_cursor(
                            current_leaf,
                            previous_key,
                            previous_is_in_current_leaf,
                            sweep_leaf_pages,
                            page_count_bound,
                        ),
                    ));
                }
                if made_progress && started.elapsed() >= deadline {
                    return Ok(finish_verify_step(
                        report,
                        started,
                        BTreeVerifyStopReason::TimeLimit,
                        next_verify_cursor(
                            current_leaf,
                            previous_key,
                            previous_is_in_current_leaf,
                            sweep_leaf_pages,
                            page_count_bound,
                        ),
                    ));
                }

                previous_key = Some(key.to_vec());
                previous_is_in_current_leaf = true;
                made_progress = true;
                report.entries_examined = report.entries_examined.saturating_add(1);
                report.key_bytes_examined =
                    report.key_bytes_examined.saturating_add(key.len() as u64);
            }

            sweep_leaf_pages = sweep_leaf_pages.saturating_add(1);
            current_leaf = node.next_page;
            resume_within_leaf = false;
            made_progress = true;
        }
    }

    /// Verify structural invariants: key ordering within and across leaves.
    ///
    /// Kept in the production build rather than behind cfg(test) so that a
    /// corrupt index can be diagnosed in the field, and so the roadmap's
    /// "corrupt derived indexes can be rebuilt" path has something to detect
    /// corruption with.
    pub fn verify(&self) -> Result<BTreeVerifyReport> {
        let mut report = BTreeVerifyReport {
            height: self.height()?,
            ..Default::default()
        };
        let mut previous: Option<Vec<u8>> = None;
        for entry in self.iter()? {
            let (key, _) = entry?;
            if let Some(previous) = &previous {
                if previous.as_slice() >= key.as_slice() {
                    report.ordering_violations += 1;
                }
            }
            previous = Some(key);
            report.entries += 1;
        }
        report.valid = report.ordering_violations == 0;
        Ok(report)
    }

    pub fn flush(&self) -> Result<()> {
        self.pool.flush_all()?;
        self.pool.store().publish_root(self.root_page(), 0)
    }
}

fn validate_verify_cursor(
    cursor: &BTreeVerifyCursor,
    limits: &BTreeVerifyLimits,
    page_count: u64,
) -> Result<()> {
    if cursor
        .after_key
        .as_ref()
        .is_some_and(|key| key.len() > limits.max_cursor_key_bytes)
    {
        return Err(PageError::InvalidMaintenanceLimits {
            reason: format!(
                "B-tree verification cursor key exceeds its {}-byte bound",
                limits.max_cursor_key_bytes
            ),
        });
    }
    match cursor.next_leaf {
        None => {
            if cursor.after_key.is_some()
                || cursor.resume_within_leaf
                || cursor.leaf_pages_traversed != 0
                || cursor.page_count_bound != 0
            {
                return Err(PageError::InvalidMaintenanceLimits {
                    reason: "a new B-tree verification cursor must be entirely empty".to_string(),
                });
            }
        }
        Some(0) => {
            return Err(PageError::InvalidMaintenanceLimits {
                reason: "B-tree verification cursor cannot reference page zero".to_string(),
            });
        }
        Some(next_leaf) => {
            if next_leaf >= page_count {
                return Err(PageError::InvalidMaintenanceLimits {
                    reason: format!(
                        "B-tree verification cursor page {next_leaf} is outside the current {page_count}-page file"
                    ),
                });
            }
            if cursor.page_count_bound == 0 {
                return Err(PageError::InvalidMaintenanceLimits {
                    reason:
                        "a resumed B-tree verification cursor requires a non-zero page-count bound"
                            .to_string(),
                });
            }
            if cursor.resume_within_leaf && cursor.after_key.is_none() {
                return Err(PageError::InvalidMaintenanceLimits {
                    reason: "resume_within_leaf requires an after_key".to_string(),
                });
            }
        }
    }
    Ok(())
}

fn next_verify_cursor(
    next_leaf: PageId,
    after_key: Option<Vec<u8>>,
    resume_within_leaf: bool,
    leaf_pages_traversed: u64,
    page_count_bound: u64,
) -> BTreeVerifyCursor {
    BTreeVerifyCursor {
        next_leaf: Some(next_leaf),
        after_key,
        resume_within_leaf,
        leaf_pages_traversed,
        page_count_bound,
    }
}

fn finish_verify_step(
    mut report: BTreeVerifyStepReport,
    started: Instant,
    stop_reason: BTreeVerifyStopReason,
    next_cursor: BTreeVerifyCursor,
) -> BTreeVerifyStepReport {
    report.next_cursor = next_cursor;
    report.stop_reason = stop_reason;
    report.elapsed_millis = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX);
    report.complete = stop_reason == BTreeVerifyStopReason::Complete;
    report.valid = report.fault.is_none();
    report
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct BTreeVerifyReport {
    pub entries: usize,
    pub height: usize,
    pub ordering_violations: usize,
    pub valid: bool,
}

/// Ascending cursor over a key range, holding one leaf at a time.
pub struct BTreeRange<'a> {
    tree: &'a BTree,
    entries: Vec<(Vec<u8>, Vec<u8>)>,
    index: usize,
    next_leaf: PageId,
}

impl Iterator for BTreeRange<'_> {
    type Item = Result<(Vec<u8>, Vec<u8>)>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if self.index < self.entries.len() {
                let entry = self.entries[self.index].clone();
                self.index += 1;
                return Some(Ok(entry));
            }
            if self.next_leaf == 0 {
                return None;
            }
            match self.tree.leaf_entries_from(self.next_leaf, b"") {
                Ok((entries, next)) => {
                    self.entries = entries;
                    self.index = 0;
                    self.next_leaf = next;
                }
                Err(error) => return Some(Err(error)),
            }
        }
    }
}

/// Descending cursor over entries with `key < bound` (no bound = from the
/// greatest key). Leaves are singly linked forward only, so stepping to the
/// PREVIOUS leaf uses the recorded descent path: at the deepest ancestor
/// whose taken child is not its leftmost, take the child one to the left and
/// descend rightmost from there — O(log n) per leaf step, one pinned page at
/// a time, no allocation proportional to the range.
pub struct BTreeRangeRev<'a> {
    tree: &'a BTree,
    /// The current leaf's entries; `index` is how many remain to yield, so
    /// the next item is `entries[index - 1]`.
    entries: Vec<(Vec<u8>, Vec<u8>)>,
    index: usize,
    /// `(interior page, taken child index)` from the root to the CURRENT
    /// leaf's parent. Child index `count()` is the rightmost (`next_page`)
    /// child.
    path: Vec<(PageId, usize)>,
    exhausted: bool,
}

impl Iterator for BTreeRangeRev<'_> {
    type Item = Result<(Vec<u8>, Vec<u8>)>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if self.index > 0 {
                self.index -= 1;
                return Some(Ok(self.entries[self.index].clone()));
            }
            if self.exhausted {
                return None;
            }
            match self.tree.previous_leaf(&mut self.path) {
                Ok(Some(entries)) => {
                    self.index = entries.len();
                    self.entries = entries;
                }
                Ok(None) => {
                    self.exhausted = true;
                    return None;
                }
                Err(error) => return Some(Err(error)),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::manager::{PageStore, PageStoreOptions};
    use crate::pool::BufferPoolOptions;
    use tempfile::TempDir;

    pub(super) fn tree(dir: &TempDir, page_size: u32, budget: u64) -> (Arc<PageStore>, BTree) {
        let store = Arc::new(
            PageStore::open(
                dir.path().join("t.pages"),
                PageStoreOptions::default()
                    .with_page_size(page_size)
                    .with_fsync(false),
            )
            .unwrap(),
        );
        let pool = Arc::new(
            BufferPool::new(
                store.clone(),
                BufferPoolOptions::default()
                    .with_budget_bytes(budget)
                    .with_shards(2),
            )
            .unwrap(),
        );
        let tree = BTree::create(pool).unwrap();
        (store, tree)
    }

    pub(super) fn key(index: usize) -> Vec<u8> {
        format!("key-{index:08}").into_bytes()
    }

    #[test]
    fn insert_and_get_a_single_key() {
        let dir = TempDir::new().unwrap();
        let (_store, tree) = tree(&dir, 1024, 64 * 1024);
        tree.insert(b"alpha", b"one").unwrap();
        assert_eq!(tree.get(b"alpha").unwrap().as_deref(), Some(&b"one"[..]));
        assert_eq!(tree.get(b"missing").unwrap(), None);
    }

    #[test]
    fn replacing_a_key_does_not_duplicate_it() {
        let dir = TempDir::new().unwrap();
        let (_store, tree) = tree(&dir, 1024, 64 * 1024);
        tree.insert(b"k", b"first").unwrap();
        tree.insert(b"k", b"second").unwrap();
        tree.insert(b"k", b"a-much-longer-third-value").unwrap();

        assert_eq!(
            tree.get(b"k").unwrap().as_deref(),
            Some(&b"a-much-longer-third-value"[..])
        );
        assert_eq!(tree.len().unwrap(), 1);
    }

    #[test]
    fn many_keys_round_trip_and_force_splits() {
        let dir = TempDir::new().unwrap();
        // Small pages so splits happen early and often.
        let (_store, tree) = tree(&dir, 512, 256 * 1024);

        for index in 0..2_000 {
            tree.insert(&key(index), &index.to_le_bytes()).unwrap();
        }
        assert!(
            tree.height().unwrap() >= 3,
            "tree never grew past one level"
        );

        for index in 0..2_000 {
            assert_eq!(
                tree.get(&key(index)).unwrap().as_deref(),
                Some(&index.to_le_bytes()[..]),
                "key {index} lost across splits"
            );
        }
        assert_eq!(tree.len().unwrap(), 2_000);
        assert!(tree.verify().unwrap().valid);
    }

    #[test]
    fn keys_inserted_in_reverse_order_still_sort() {
        let dir = TempDir::new().unwrap();
        let (_store, tree) = tree(&dir, 512, 256 * 1024);
        for index in (0..1_000).rev() {
            tree.insert(&key(index), &index.to_le_bytes()).unwrap();
        }

        let keys: Vec<Vec<u8>> = tree.iter().unwrap().map(|e| e.unwrap().0).collect();
        assert_eq!(keys.len(), 1_000);
        let mut sorted = keys.clone();
        sorted.sort();
        assert_eq!(keys, sorted, "iteration was not in key order");
    }

    #[test]
    fn keys_inserted_in_random_order_still_sort() {
        let dir = TempDir::new().unwrap();
        let (_store, tree) = tree(&dir, 512, 256 * 1024);

        // Deterministic shuffle: stepping by a value coprime to the modulus
        // visits every index exactly once in scattered order. (A multiplicative
        // walk would NOT: the multiplicative order of the base is usually far
        // smaller than the modulus, so it cycles early and silently tests a
        // fraction of the keys.)
        let count = 1_500usize;
        const STEP: usize = 977; // gcd(977, 1500) == 1
        let mut position = 0usize;
        let mut inserted = std::collections::BTreeSet::new();
        for _ in 0..count {
            position = (position + STEP) % count;
            tree.insert(&key(position), &position.to_le_bytes())
                .unwrap();
            inserted.insert(position);
        }
        assert_eq!(inserted.len(), count, "the shuffle did not cover every key");

        let report = tree.verify().unwrap();
        assert!(report.valid, "ordering violated: {report:?}");
        assert_eq!(report.entries, count);
    }

    #[test]
    fn range_starts_at_the_requested_key() {
        let dir = TempDir::new().unwrap();
        let (_store, tree) = tree(&dir, 512, 256 * 1024);
        for index in 0..500 {
            tree.insert(&key(index), &index.to_le_bytes()).unwrap();
        }

        let from = key(200);
        let collected: Vec<Vec<u8>> = tree.range(&from).unwrap().map(|e| e.unwrap().0).collect();
        assert_eq!(collected.len(), 300);
        assert_eq!(collected[0], from);
        assert_eq!(collected.last().unwrap(), &key(499));
    }

    #[test]
    fn range_cursor_submits_the_exact_next_leaf_without_reading_it_inline() {
        let dir = TempDir::new().unwrap();
        let (store, tree) = tree(&dir, 512, 256 * 1024);
        for index in 0..500 {
            tree.insert(&key(index), &index.to_le_bytes()).unwrap();
        }
        tree.pool.flush_all().unwrap();
        let root = tree.root_page();
        drop(tree);
        let pool = Arc::new(
            BufferPool::new(
                store,
                BufferPoolOptions::default()
                    .with_budget_bytes(4 * 512)
                    .with_shards(1),
            )
            .unwrap(),
        );
        let tree = BTree::open_at(pool.clone(), root).unwrap();
        // Sibling hints are only queued while a read-ahead driver exists.
        pool.register_read_ahead_driver();

        let cursor = tree.range(b"").unwrap();
        assert_eq!(
            pool.read_ahead_queue_depth(),
            1,
            "opening the first leaf should queue exactly its sibling"
        );
        drop(cursor);
        assert_eq!(pool.snapshot().read_ahead_steps, 0);
    }

    #[test]
    fn a_range_scan_stays_within_the_buffer_pool_budget() {
        let dir = TempDir::new().unwrap();
        let store = Arc::new(
            PageStore::open(
                dir.path().join("t.pages"),
                PageStoreOptions::default()
                    .with_page_size(512)
                    .with_fsync(false),
            )
            .unwrap(),
        );
        let pool = Arc::new(
            BufferPool::new(
                store,
                BufferPoolOptions::default()
                    .with_budget_bytes(8 * 512)
                    .with_shards(1),
            )
            .unwrap(),
        );
        let tree = BTree::create(pool.clone()).unwrap();
        for index in 0..1_000 {
            tree.insert(&key(index), &index.to_le_bytes()).unwrap();
        }

        let mut count = 0;
        for entry in tree.iter().unwrap() {
            entry.unwrap();
            count += 1;
            let snapshot = pool.snapshot();
            assert!(
                snapshot.resident_bytes <= snapshot.budget_bytes,
                "scan exceeded the pool budget at entry {count}"
            );
        }
        assert_eq!(count, 1_000);
    }

    #[test]
    fn removed_keys_disappear_and_the_rest_survive() {
        let dir = TempDir::new().unwrap();
        let (_store, tree) = tree(&dir, 512, 256 * 1024);
        for index in 0..800 {
            tree.insert(&key(index), &index.to_le_bytes()).unwrap();
        }
        for index in (0..800).step_by(2) {
            assert!(tree.remove(&key(index)).unwrap());
        }

        assert_eq!(tree.len().unwrap(), 400);
        for index in 0..800 {
            let found = tree.get(&key(index)).unwrap();
            if index % 2 == 0 {
                assert!(found.is_none(), "key {index} survived removal");
            } else {
                assert_eq!(found.as_deref(), Some(&index.to_le_bytes()[..]));
            }
        }
        assert!(tree.verify().unwrap().valid);
    }

    #[test]
    fn removing_an_absent_key_reports_false() {
        let dir = TempDir::new().unwrap();
        let (_store, tree) = tree(&dir, 1024, 64 * 1024);
        tree.insert(b"present", b"v").unwrap();
        assert!(!tree.remove(b"absent").unwrap());
        assert!(tree.remove(b"present").unwrap());
        assert!(!tree.remove(b"present").unwrap());
    }

    #[test]
    fn removed_key_bytes_do_not_survive_in_the_page_image() {
        let dir = TempDir::new().unwrap();
        let (store, tree) = tree(&dir, 1024, 64 * 1024);
        tree.insert(b"SENSITIVE-KEY", b"SENSITIVE-VALUE").unwrap();
        tree.remove(b"SENSITIVE-KEY").unwrap();
        tree.flush().unwrap();

        let mut page = vec![0u8; 1024];
        store.read_page(tree.root_page(), &mut page).unwrap();
        assert!(!page.windows(13).any(|w| w == b"SENSITIVE-KEY"));
        assert!(!page.windows(15).any(|w| w == b"SENSITIVE-VALUE"));
    }

    #[test]
    fn the_tree_survives_reopen_through_the_published_root() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("t.pages");
        {
            let store = Arc::new(
                PageStore::open(
                    &path,
                    PageStoreOptions::default()
                        .with_page_size(512)
                        .with_fsync(false),
                )
                .unwrap(),
            );
            let pool = Arc::new(
                BufferPool::new(
                    store,
                    BufferPoolOptions::default().with_budget_bytes(256 * 1024),
                )
                .unwrap(),
            );
            let tree = BTree::create(pool).unwrap();
            for index in 0..1_200 {
                tree.insert(&key(index), &index.to_le_bytes()).unwrap();
            }
            tree.flush().unwrap();
        }

        let store = Arc::new(
            PageStore::open(
                &path,
                PageStoreOptions::default()
                    .with_page_size(512)
                    .with_fsync(false),
            )
            .unwrap(),
        );
        let pool = Arc::new(
            BufferPool::new(
                store,
                BufferPoolOptions::default().with_budget_bytes(256 * 1024),
            )
            .unwrap(),
        );
        let tree = BTree::open(pool).unwrap();

        assert_eq!(tree.len().unwrap(), 1_200);
        for index in 0..1_200 {
            assert_eq!(
                tree.get(&key(index)).unwrap().as_deref(),
                Some(&index.to_le_bytes()[..]),
                "key {index} did not survive reopen"
            );
        }
        assert!(tree.verify().unwrap().valid);
    }

    #[test]
    fn a_value_too_large_to_split_around_is_refused() {
        // A node must hold two entries or a split cannot make progress. Better
        // an error than an infinite split loop.
        let dir = TempDir::new().unwrap();
        let (_store, tree) = tree(&dir, 512, 64 * 1024);
        let huge = vec![b'x'; 400];
        assert!(matches!(
            tree.insert(b"k", &huge),
            Err(PageError::ValueTooLarge { .. })
        ));
    }

    #[test]
    fn empty_keys_are_refused() {
        let dir = TempDir::new().unwrap();
        let (_store, tree) = tree(&dir, 1024, 64 * 1024);
        assert!(tree.insert(b"", b"v").is_err());
    }

    #[test]
    fn variable_length_keys_sort_bytewise() {
        let dir = TempDir::new().unwrap();
        let (_store, tree) = tree(&dir, 512, 128 * 1024);
        let keys: Vec<&[u8]> = vec![b"a", b"ab", b"abc", b"b", b"ba", b"", b"z", b"zz"];
        for k in &keys {
            if k.is_empty() {
                continue;
            }
            tree.insert(k, b"v").unwrap();
        }

        let found: Vec<Vec<u8>> = tree.iter().unwrap().map(|e| e.unwrap().0).collect();
        let mut expected: Vec<Vec<u8>> = keys
            .iter()
            .filter(|k| !k.is_empty())
            .map(|k| k.to_vec())
            .collect();
        expected.sort();
        assert_eq!(found, expected);
    }

    #[test]
    fn tuple_locators_round_trip_as_values() {
        // The actual Phase 2 use: record id -> tuple locator.
        use crate::slotted::TupleLocator;
        let dir = TempDir::new().unwrap();
        let (_store, tree) = tree(&dir, 512, 256 * 1024);

        let mut expected = Vec::new();
        for index in 0..900u64 {
            let locator = TupleLocator::new(index * 3 + 1, (index % 400) as u16, index as u32 + 1);
            tree.insert(&key(index as usize), &locator.encode())
                .unwrap();
            expected.push((key(index as usize), locator));
        }

        for (k, locator) in &expected {
            let stored = tree.get(k).unwrap().unwrap();
            assert_eq!(TupleLocator::decode(&stored).unwrap(), *locator);
        }
    }

    #[test]
    fn interleaved_inserts_and_removes_keep_the_tree_valid() {
        let dir = TempDir::new().unwrap();
        let (_store, tree) = tree(&dir, 512, 256 * 1024);
        let mut live = std::collections::BTreeSet::new();

        for round in 0..3_000usize {
            let index = (round * 7919) % 1_000;
            if round % 3 == 2 {
                if live.remove(&index) {
                    assert!(tree.remove(&key(index)).unwrap());
                }
            } else {
                tree.insert(&key(index), &index.to_le_bytes()).unwrap();
                live.insert(index);
            }
        }

        let report = tree.verify().unwrap();
        assert!(report.valid, "{report:?}");
        assert_eq!(report.entries, live.len());
        for index in &live {
            assert_eq!(
                tree.get(&key(*index)).unwrap().as_deref(),
                Some(&index.to_le_bytes()[..])
            );
        }
    }

    #[test]
    fn bounded_verification_resumes_without_skipping_or_recounting_keys() {
        let dir = TempDir::new().unwrap();
        let (_store, tree) = tree(&dir, 512, 256 * 1024);
        for index in 0..777 {
            tree.insert(&key(index), &index.to_le_bytes()).unwrap();
        }

        let limits = BTreeVerifyLimits {
            max_entries: 7,
            max_leaf_pages: 2,
            max_page_bytes: 64 * 1024,
            max_duration_millis: 10_000,
            ..BTreeVerifyLimits::default()
        };
        let mut cursor = BTreeVerifyCursor::default();
        let mut entries = 0_u64;
        let mut steps = 0_u64;
        let mut saw_height = false;
        loop {
            let report = tree.verify_step(cursor, limits).unwrap();
            assert!(report.valid, "{report:?}");
            assert!(report.entries_examined <= limits.max_entries);
            assert!(report.leaf_pages_examined <= limits.max_leaf_pages);
            assert!(report.page_bytes_examined <= limits.max_page_bytes);
            assert!(report.key_bytes_examined <= limits.max_key_bytes);
            entries += report.entries_examined;
            steps += 1;
            saw_height |= report.height.is_some();
            if report.complete {
                assert_eq!(report.stop_reason, BTreeVerifyStopReason::Complete);
                assert_eq!(report.next_cursor, BTreeVerifyCursor::default());
                break;
            }
            assert!(matches!(
                report.stop_reason,
                BTreeVerifyStopReason::EntryLimit | BTreeVerifyStopReason::LeafPageLimit
            ));
            cursor = report.next_cursor;
            assert!(steps < 1_000, "bounded verifier did not converge");
        }
        assert!(saw_height);
        assert!(steps > 1);
        assert_eq!(entries, 777);
    }

    #[test]
    fn empty_leaf_runs_advance_a_physical_restart_cursor() {
        let dir = TempDir::new().unwrap();
        let (_store, tree) = tree(&dir, 512, 256 * 1024);
        for index in 0..500 {
            tree.insert(&key(index), &index.to_le_bytes()).unwrap();
        }
        for index in 0..500 {
            assert!(tree.remove(&key(index)).unwrap());
        }

        let limits = BTreeVerifyLimits {
            max_leaf_pages: 1,
            max_duration_millis: 10_000,
            ..BTreeVerifyLimits::default()
        };
        let mut cursor = BTreeVerifyCursor::default();
        let mut previous_leaf = None;
        let mut steps = 0_u64;
        loop {
            let report = tree.verify_step(cursor, limits).unwrap();
            assert!(report.valid, "{report:?}");
            assert_eq!(report.entries_examined, 0);
            steps += 1;
            if report.complete {
                break;
            }
            assert_eq!(report.stop_reason, BTreeVerifyStopReason::LeafPageLimit);
            assert_ne!(report.next_cursor.next_leaf, previous_leaf);
            previous_leaf = report.next_cursor.next_leaf;
            cursor = report.next_cursor;
            assert!(steps < 1_000, "empty leaf traversal stalled");
        }
        assert!(steps > 1);
    }

    #[test]
    fn a_leaf_sibling_cycle_is_a_bounded_structural_fault() {
        let dir = TempDir::new().unwrap();
        let (_store, tree) = tree(&dir, 512, 256 * 1024);
        for index in 0..200 {
            tree.insert(&key(index), &index.to_le_bytes()).unwrap();
        }
        let leaf = tree.leftmost_leaf().unwrap();
        {
            let mut guard = tree.pool.get_mut(leaf).unwrap();
            Node::new(guard.bytes_mut()).set_next_page(leaf);
        }

        let report = tree
            .verify_step(
                BTreeVerifyCursor::default(),
                BTreeVerifyLimits {
                    max_duration_millis: 10_000,
                    ..BTreeVerifyLimits::default()
                },
            )
            .unwrap();
        assert!(!report.valid);
        assert_eq!(report.stop_reason, BTreeVerifyStopReason::StructuralFault);
        assert!(matches!(
            report.fault,
            Some(BTreeVerifyFault::LeafCycle { page_id }) if page_id == leaf
        ));
        assert!(report.leaf_pages_examined <= 2);
    }

    #[test]
    fn a_root_cycle_is_classified_before_the_height_guard() {
        let dir = TempDir::new().unwrap();
        let (_store, tree) = tree(&dir, 512, 256 * 1024);
        for index in 0..500 {
            tree.insert(&key(index), &index.to_le_bytes()).unwrap();
        }
        let root = tree.root_page();
        {
            let mut guard = tree.pool.get_mut(root).unwrap();
            let bytes = guard.bytes_mut();
            let node = NodeRef::new(bytes);
            assert!(!node.is_leaf());
            let (offset, key_len, value_len) = node.entry_at(0);
            assert_eq!(value_len, 8);
            let value_at = offset + key_len;
            bytes[value_at..value_at + 8].copy_from_slice(&root.to_le_bytes());
        }

        let report = tree
            .verify_step(
                BTreeVerifyCursor::default(),
                BTreeVerifyLimits {
                    max_duration_millis: 10_000,
                    ..BTreeVerifyLimits::default()
                },
            )
            .unwrap();
        assert!(!report.valid);
        assert!(matches!(
            report.fault,
            Some(BTreeVerifyFault::DescentCycle {
                page_id,
                first_seen_depth: 1,
                repeated_at_depth: 2,
            }) if page_id == root
        ));
        assert!(report.descent_pages_examined <= 1);
    }

    #[test]
    fn page_and_key_byte_limits_yield_exact_resumable_boundaries() {
        let dir = TempDir::new().unwrap();
        let (_store, tree) = tree(&dir, 512, 64 * 1024);
        tree.insert(b"aaaaaaaaaaaa", b"v").unwrap();
        tree.insert(b"bbbbbbbbbbbb", b"v").unwrap();

        let page_limited = tree
            .verify_step(
                BTreeVerifyCursor::default(),
                BTreeVerifyLimits {
                    max_page_bytes: 512,
                    max_height: 1,
                    max_duration_millis: 10_000,
                    ..BTreeVerifyLimits::default()
                },
            )
            .unwrap();
        assert!(page_limited.valid);
        assert_eq!(
            page_limited.stop_reason,
            BTreeVerifyStopReason::PageByteLimit
        );
        assert_eq!(page_limited.page_bytes_examined, 512);
        assert_eq!(page_limited.entries_examined, 0);

        let key_limits = BTreeVerifyLimits {
            max_page_bytes: 512,
            max_key_bytes: 16,
            max_cursor_key_bytes: 16,
            max_height: 1,
            max_duration_millis: 10_000,
            ..BTreeVerifyLimits::default()
        };
        let first = tree
            .verify_step(page_limited.next_cursor, key_limits)
            .unwrap();
        assert!(first.valid);
        assert_eq!(first.stop_reason, BTreeVerifyStopReason::KeyByteLimit);
        assert_eq!(first.entries_examined, 1);
        assert_eq!(first.key_bytes_examined, 12);
        let second = tree.verify_step(first.next_cursor, key_limits).unwrap();
        assert!(second.complete);
        assert_eq!(second.entries_examined, 1);
        assert_eq!(second.key_bytes_examined, 12);
    }

    #[test]
    fn malformed_slot_offsets_are_reported_without_panicking() {
        let dir = TempDir::new().unwrap();
        let (_store, tree) = tree(&dir, 512, 64 * 1024);
        tree.insert(b"alpha", b"value").unwrap();
        let root = tree.root_page();
        {
            let mut guard = tree.pool.get_mut(root).unwrap();
            // Point the first live entry into the page header. The verifier
            // must classify this before a normal node accessor slices it.
            guard.bytes_mut()[PAGE_HEADER_BYTES..PAGE_HEADER_BYTES + 2]
                .copy_from_slice(&1_u16.to_le_bytes());
        }

        let report = tree
            .verify_step(
                BTreeVerifyCursor::default(),
                BTreeVerifyLimits {
                    max_duration_millis: 10_000,
                    ..BTreeVerifyLimits::default()
                },
            )
            .unwrap();
        assert!(!report.valid);
        assert!(matches!(
            report.fault,
            Some(BTreeVerifyFault::InvalidNode {
                page_id,
                violation: BTreeNodeViolation::EntryOutOfBounds { entry: 0 },
            }) if page_id == root
        ));
    }

    #[test]
    fn structural_verification_contracts_reject_unknown_fields_and_bad_cursors() {
        let limits = BTreeVerifyLimits::default();
        let encoded = serde_json::to_value(limits).unwrap();
        let mut object = encoded.as_object().unwrap().clone();
        object.insert("unbounded".to_string(), serde_json::json!(true));
        assert!(
            serde_json::from_value::<BTreeVerifyLimits>(serde_json::Value::Object(object)).is_err()
        );

        let mut cursor_value = serde_json::to_value(BTreeVerifyCursor::default()).unwrap();
        cursor_value
            .as_object_mut()
            .unwrap()
            .insert("skip_validation".to_string(), serde_json::json!(true));
        assert!(serde_json::from_value::<BTreeVerifyCursor>(cursor_value).is_err());

        let dir = TempDir::new().unwrap();
        let (_store, tree) = tree(&dir, 512, 64 * 1024);
        let error = tree
            .verify_step(
                BTreeVerifyCursor {
                    next_leaf: Some(1),
                    after_key: None,
                    resume_within_leaf: true,
                    leaf_pages_traversed: 0,
                    page_count_bound: 1,
                },
                limits,
            )
            .unwrap_err();
        assert!(matches!(error, PageError::InvalidMaintenanceLimits { .. }));
    }
}

#[cfg(test)]
mod reverse_scan_tests {
    use super::tests::{key, tree};
    use tempfile::TempDir;

    #[test]
    fn reverse_scan_matches_forward_scan_reversed_across_leaf_boundaries() {
        let dir = TempDir::new().unwrap();
        // Small pages force a multi-level tree so previous-leaf stepping and
        // rightmost descents are actually exercised.
        let (_store, tree) = tree(&dir, 512, 512 * 256);
        for index in 0..1_000usize {
            // Shuffled insertion order so structure comes from splits.
            let scrambled = (index * 7919) % 1_000;
            tree.insert(&key(scrambled), format!("v{scrambled}").as_bytes())
                .unwrap();
        }
        let forward: Vec<_> = tree.iter().unwrap().collect::<Result<Vec<_>, _>>().unwrap();
        assert_eq!(forward.len(), 1_000);
        let mut reversed: Vec<_> = tree
            .range_rev_below(None)
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        reversed.reverse();
        assert_eq!(forward, reversed, "descending scan must mirror ascending");
    }

    #[test]
    fn reverse_scan_bound_is_exclusive_and_exact() {
        let dir = TempDir::new().unwrap();
        let (_store, tree) = tree(&dir, 512, 512 * 256);
        for index in 0..500usize {
            tree.insert(&key(index), b"v").unwrap();
        }
        // Bound at an existing key: strictly-below semantics.
        let below: Vec<_> = tree
            .range_rev_below(Some(&key(250)))
            .unwrap()
            .map(|entry| entry.unwrap().0)
            .collect();
        assert_eq!(below.len(), 250);
        assert_eq!(below.first().unwrap(), &key(249));
        assert_eq!(below.last().unwrap(), &key(0));
        // Bound between keys behaves identically to the next key up.
        let mut between = key(250);
        between.push(0);
        let via_between: Vec<_> = tree
            .range_rev_below(Some(&between))
            .unwrap()
            .map(|entry| entry.unwrap().0)
            .collect();
        assert_eq!(via_between.first().unwrap(), &key(250));
        // Bound below the minimum yields nothing; above the maximum yields all.
        assert_eq!(tree.range_rev_below(Some(b"a")).unwrap().count(), 0);
        assert_eq!(
            tree.range_rev_below(Some(&key(9_999))).unwrap().count(),
            500
        );
    }
}
