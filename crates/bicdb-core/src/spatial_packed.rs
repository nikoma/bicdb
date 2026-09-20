//! Bulk-packed immutable R-tree for spatial indexes on the paged engine.
//!
//! A dynamic R-tree pays for split/merge logic and loose pages so that
//! single-record mutations stay cheap. A spatial corpus that arrives as a
//! release/snapshot doesn't need any of that: this module takes every entry
//! at once, orders it spatially (Hilbert curve or Sort-Tile-Recursive), packs
//! leaves to capacity, and builds parent levels bottom-up. The result is a
//! set of IMMUTABLE nodes — dense, deterministic, write-once — that the
//! caller stores as values in the durable index keyspace and publishes with
//! one atomic meta swap.
//!
//! This module is pure: it knows nothing about pages, transactions, or
//! record storage. The builder emits `(node_number, bytes)` through a sink
//! callback; the search walks nodes through a reader callback. `db.rs` owns
//! the binding to the paged keyspace (namespace `[0,0,13]`, see
//! `paged_collection.rs`) and the hybrid packed-base + resident-delta
//! composition.
//!
//! # Node encoding (version 1)
//!
//! ```text
//! [version u8 = 1] [kind u8: 0 = leaf, 1 = internal] [count u32 LE] entries…
//! internal entry: [min_x f64][min_y f64][max_x f64][max_y f64][child u64]   (all LE)
//! leaf entry:     [min_x][min_y][max_x][max_y][flags u8][point_x point_y if flags&1]
//!                 [id_len u16 LE][id bytes]
//! ```
//!
//! Node numbers start at 1 and are assigned in write order (leaves first,
//! then each parent level), so a build's node keys are appended to the
//! durable keyspace in ascending key order — the cheapest possible B-tree
//! insertion pattern.

use serde::{Deserialize, Serialize};

use crate::error::{BicDbError, Result};

/// Entries per node. Leaves and interior nodes share the capacity: at 32
/// bytes of MBR per entry a full interior node is ~10 KiB and a full leaf
/// with short record ids lands in the same range — a handful of 8 KiB pages
/// per descent level.
pub(crate) const PACKED_NODE_CAPACITY: usize = 256;

/// Hilbert curve order: 2^16 cells per axis. Finer than any realistic MBR
/// packing benefit; coarse enough that the curve index fits u32.
const HILBERT_ORDER: u32 = 16;

const NODE_VERSION: u8 = 1;
const NODE_KIND_LEAF: u8 = 0;
const NODE_KIND_INTERNAL: u8 = 1;

const META_VERSION: u8 = 1;

const DELTA_TOMBSTONE: u8 = 0;
const DELTA_UPSERT: u8 = 1;

/// How a pack orders entries before filling leaves.
///
/// Hilbert ordering preserves 2D locality along one curve; STR tiles the
/// plane into vertical slabs sorted by y. Both produce tight, low-overlap
/// leaves; which wins is workload-dependent, so both are first-class and a
/// benchmark decides (`spatial_packed` tests carry an ignored comparison).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SpatialPackStrategy {
    Hilbert,
    Str,
}

impl SpatialPackStrategy {
    pub fn label(&self) -> &'static str {
        match self {
            SpatialPackStrategy::Hilbert => "hilbert",
            SpatialPackStrategy::Str => "str",
        }
    }

    fn to_byte(self) -> u8 {
        match self {
            SpatialPackStrategy::Hilbert => 0,
            SpatialPackStrategy::Str => 1,
        }
    }

    fn from_byte(byte: u8) -> Result<Self> {
        match byte {
            0 => Ok(SpatialPackStrategy::Hilbert),
            1 => Ok(SpatialPackStrategy::Str),
            other => Err(BicDbError::Index(format!(
                "unknown packed spatial strategy byte {other}"
            ))),
        }
    }
}

/// One spatial entry as the packed tree stores it: the record's primary key,
/// its MBR, and the exact point when the geometry is a point (what haversine
/// refinement requires — radius/nearest queries error on non-point
/// geometries, and that contract must survive packing).
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct PackedSpatialEntry {
    pub record_id: String,
    pub min: [f64; 2],
    pub max: [f64; 2],
    pub point: Option<[f64; 2]>,
}

/// The published description of one packed generation — everything a reader
/// needs to walk the tree. Stored under the meta key; swapping this value in
/// one transaction is what atomically activates a new generation.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct PackedSpatialMeta {
    pub strategy: SpatialPackStrategy,
    pub generation: u64,
    pub root_node: u64,
    pub height: u32,
    pub node_count: u64,
    pub entry_count: u64,
}

impl PackedSpatialMeta {
    pub fn encode(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(38);
        bytes.push(META_VERSION);
        bytes.push(self.strategy.to_byte());
        bytes.extend_from_slice(&self.generation.to_le_bytes());
        bytes.extend_from_slice(&self.root_node.to_le_bytes());
        bytes.extend_from_slice(&self.height.to_le_bytes());
        bytes.extend_from_slice(&self.node_count.to_le_bytes());
        bytes.extend_from_slice(&self.entry_count.to_le_bytes());
        bytes
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let mut reader = Reader::new(bytes, "packed spatial meta");
        let version = reader.u8()?;
        if version != META_VERSION {
            return Err(BicDbError::Index(format!(
                "unsupported packed spatial meta version {version}"
            )));
        }
        let strategy = SpatialPackStrategy::from_byte(reader.u8()?)?;
        let generation = reader.u64()?;
        let root_node = reader.u64()?;
        let height = reader.u32()?;
        let node_count = reader.u64()?;
        let entry_count = reader.u64()?;
        Ok(Self {
            strategy,
            generation,
            root_node,
            height,
            node_count,
            entry_count,
        })
    }
}

/// A decoded node.
pub(crate) enum PackedNode {
    Internal(Vec<PackedChild>),
    Leaf(Vec<PackedSpatialEntry>),
}

pub(crate) struct PackedChild {
    pub min: [f64; 2],
    pub max: [f64; 2],
    pub node: u64,
}

/// Durable delta value for one primary key: either "this pk's packed entries
/// are superseded and this is its current entry" or "superseded with no
/// current entry" (deleted, or its geometry was removed). Any delta row masks
/// the packed base for its pk — that id-level masking is what closes the
/// stale-entry-at-unknown-position blind spot the incremental resident path
/// documents.
pub(crate) fn encode_delta_upsert(entry: &PackedSpatialEntry) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(50);
    bytes.push(DELTA_UPSERT);
    push_mbr(&mut bytes, entry.min, entry.max);
    match entry.point {
        Some(point) => {
            bytes.push(1);
            bytes.extend_from_slice(&point[0].to_le_bytes());
            bytes.extend_from_slice(&point[1].to_le_bytes());
        }
        None => bytes.push(0),
    }
    bytes
}

pub(crate) fn encode_delta_tombstone() -> Vec<u8> {
    vec![DELTA_TOMBSTONE]
}

/// Decode a delta value: `Ok(Some(...))` for an upsert carrying the current
/// entry geometry, `Ok(None)` for a bare tombstone.
pub(crate) fn decode_delta(bytes: &[u8]) -> Result<Option<([f64; 2], [f64; 2], Option<[f64; 2]>)>> {
    let mut reader = Reader::new(bytes, "packed spatial delta");
    match reader.u8()? {
        DELTA_TOMBSTONE => Ok(None),
        DELTA_UPSERT => {
            let (min, max) = reader.mbr()?;
            let point = match reader.u8()? {
                0 => None,
                1 => Some([reader.f64()?, reader.f64()?]),
                other => {
                    return Err(BicDbError::Index(format!(
                        "packed spatial delta has invalid point flag {other}"
                    )))
                }
            };
            Ok(Some((min, max, point)))
        }
        other => Err(BicDbError::Index(format!(
            "packed spatial delta has unknown tag {other}"
        ))),
    }
}

/// Build a packed tree from `entries`, emitting each finished node through
/// `sink(node_number, bytes)`. Nodes are numbered from 1 in emission order:
/// all leaves in curve order, then each parent level bottom-up; the last node
/// emitted is the root. Returns the meta describing the finished tree
/// (`generation` is left at 0 — the storage layer owns generation numbering).
///
/// Peak memory beyond the caller's entry vector is one level of `(node, MBR)`
/// summaries — for 74M entries at capacity 256 that is ~290k child summaries,
/// then ~1.1k, then 5.
pub(crate) fn build_packed_tree(
    mut entries: Vec<PackedSpatialEntry>,
    strategy: SpatialPackStrategy,
    sink: &mut dyn FnMut(u64, &[u8]) -> Result<()>,
) -> Result<PackedSpatialMeta> {
    match strategy {
        SpatialPackStrategy::Hilbert => order_by_hilbert(&mut entries),
        SpatialPackStrategy::Str => order_by_str(&mut entries),
    }
    build_packed_tree_from_sorted(entries.into_iter().map(Ok), strategy, sink)
}

/// Build from an ALREADY spatially-ordered entry stream, holding only one
/// leaf's entries plus one `(node, MBR)` summary per finished node of the
/// current level — for 74M entries at capacity 256 that is ~290k summaries,
/// then ~1.1k, then 5. This is what makes an external-sorted pack bounded:
/// the sorted stream comes off disk runs, never a corpus-sized vector.
pub(crate) fn build_packed_tree_from_sorted(
    entries: impl Iterator<Item = Result<PackedSpatialEntry>>,
    strategy: SpatialPackStrategy,
    sink: &mut dyn FnMut(u64, &[u8]) -> Result<()>,
) -> Result<PackedSpatialMeta> {
    let mut next_node: u64 = 1;
    let mut buffer = Vec::new();
    let mut entry_count: u64 = 0;
    let mut leaf: Vec<PackedSpatialEntry> = Vec::with_capacity(PACKED_NODE_CAPACITY);
    let mut level: Vec<PackedChild> = Vec::new();

    let mut emit_leaf = |leaf: &mut Vec<PackedSpatialEntry>,
                         level: &mut Vec<PackedChild>,
                         next_node: &mut u64,
                         buffer: &mut Vec<u8>,
                         sink: &mut dyn FnMut(u64, &[u8]) -> Result<()>|
     -> Result<()> {
        encode_leaf(buffer, leaf);
        let node = *next_node;
        *next_node += 1;
        sink(node, buffer)?;
        let (min, max) = chunk_bounds(leaf.iter().map(|entry| (entry.min, entry.max)));
        level.push(PackedChild { min, max, node });
        leaf.clear();
        Ok(())
    };

    for entry in entries {
        let entry = entry?;
        // The leaf codec length-prefixes ids with u16; a longer id would
        // silently truncate the prefix and corrupt every entry decoded after
        // it. Refuse loudly instead — no real corpus has 64 KiB primary keys.
        if entry.record_id.len() > usize::from(u16::MAX) {
            return Err(BicDbError::Index(format!(
                "packed spatial index cannot store record id of {} bytes (max {})",
                entry.record_id.len(),
                u16::MAX
            )));
        }
        entry_count += 1;
        leaf.push(entry);
        if leaf.len() == PACKED_NODE_CAPACITY {
            emit_leaf(&mut leaf, &mut level, &mut next_node, &mut buffer, sink)?;
        }
    }
    if !leaf.is_empty() {
        emit_leaf(&mut leaf, &mut level, &mut next_node, &mut buffer, sink)?;
    }
    if entry_count == 0 {
        return Ok(PackedSpatialMeta {
            strategy,
            generation: 0,
            root_node: 0,
            height: 0,
            node_count: 0,
            entry_count: 0,
        });
    }

    // Parent levels: children are already in spatial order, so sequential
    // grouping keeps sibling MBRs tight without re-sorting per level.
    let mut height: u32 = 1;
    while level.len() > 1 {
        let mut parents = Vec::with_capacity(level.len() / PACKED_NODE_CAPACITY + 1);
        for chunk in level.chunks(PACKED_NODE_CAPACITY) {
            encode_internal(&mut buffer, chunk);
            let node = next_node;
            next_node += 1;
            sink(node, &buffer)?;
            let (min, max) = chunk_bounds(chunk.iter().map(|child| (child.min, child.max)));
            parents.push(PackedChild { min, max, node });
        }
        level = parents;
        height += 1;
    }

    Ok(PackedSpatialMeta {
        strategy,
        generation: 0,
        root_node: level[0].node,
        height,
        node_count: next_node - 1,
        entry_count,
    })
}

/// Hilbert key of an MBR center on FIXED geographic bounds (lon −180..180,
/// lat −90..90). Fixed bounds make the external-sort scan SINGLE-PASS — no
/// bounds pre-pass over the corpus. Out-of-range planar coordinates clamp to
/// the edge cells: packing quality degrades there, query correctness never
/// depends on the ordering.
pub(crate) fn hilbert_key_lonlat(center_x: f64, center_y: f64) -> u64 {
    let cells = (1u32 << HILBERT_ORDER) as f64;
    let cell = |value: f64, low: f64, span: f64| -> u32 {
        let scaled = ((value - low) / span * cells).floor();
        (scaled.max(0.0) as u32).min((1 << HILBERT_ORDER) - 1)
    };
    hilbert_d(cell(center_x, -180.0, 360.0), cell(center_y, -90.0, 180.0))
}

/// Walk the tree pruning by envelope intersection, invoking `visit` for every
/// leaf entry whose MBR intersects `[min, max]`. `read` resolves one node
/// number to its stored bytes.
pub(crate) fn search_envelope(
    meta: &PackedSpatialMeta,
    min: [f64; 2],
    max: [f64; 2],
    read: &mut dyn FnMut(u64) -> Result<Vec<u8>>,
    visit: &mut dyn FnMut(PackedSpatialEntry) -> Result<()>,
) -> Result<()> {
    if meta.entry_count == 0 {
        return Ok(());
    }
    let mut stack = vec![meta.root_node];
    while let Some(node) = stack.pop() {
        let bytes = read(node)?;
        match decode_node(&bytes)? {
            PackedNode::Internal(children) => {
                for child in children {
                    if intersects(min, max, child.min, child.max) {
                        stack.push(child.node);
                    }
                }
            }
            PackedNode::Leaf(entries) => {
                for entry in entries {
                    if intersects(min, max, entry.min, entry.max) {
                        visit(entry)?;
                    }
                }
            }
        }
    }
    Ok(())
}

/// Best-first nearest-neighbor walk: a min-heap over lower-bound distances
/// drives the descent, so nodes are opened and entries emitted in
/// nondecreasing distance order — the classic bounded k-NN over an R-tree.
///
/// `bound(min, max)` must be ADMISSIBLE: never larger than the caller's true
/// distance to any entry inside that MBR. Because the builder makes every
/// parent MBR the union of its children, an admissible bound is automatically
/// monotone down the tree; the walk still clamps each child's bound to its
/// parent's so float noise can never reorder emissions.
///
/// Entries reach `visit` with their bound distance (exact for degenerate
/// point MBRs, where the bound collapses to the true metric). The walk stops
/// as soon as the next-nearest heap item's distance exceeds `cutoff()` — the
/// caller lowers the cutoff as it fills its top-k (comparison is strict, so
/// candidates tying the cutoff are still delivered), keeping the visit count
/// proportional to the result, not the corpus.
pub(crate) fn search_nearest(
    meta: &PackedSpatialMeta,
    read: &mut dyn FnMut(u64) -> Result<Vec<u8>>,
    bound: &mut dyn FnMut([f64; 2], [f64; 2]) -> f64,
    cutoff: &dyn Fn() -> f64,
    visit: &mut dyn FnMut(f64, PackedSpatialEntry) -> Result<()>,
) -> Result<()> {
    if meta.entry_count == 0 {
        return Ok(());
    }
    let mut sequence = 0u64;
    let mut heap = std::collections::BinaryHeap::new();
    heap.push(NearestCandidate {
        distance: 0.0,
        sequence,
        item: NearestItem::Node(meta.root_node),
    });
    while let Some(candidate) = heap.pop() {
        if candidate.distance > cutoff() {
            return Ok(());
        }
        match candidate.item {
            NearestItem::Node(node) => {
                let bytes = read(node)?;
                match decode_node(&bytes)? {
                    PackedNode::Internal(children) => {
                        for child in children {
                            sequence += 1;
                            heap.push(NearestCandidate {
                                distance: bound(child.min, child.max).max(candidate.distance),
                                sequence,
                                item: NearestItem::Node(child.node),
                            });
                        }
                    }
                    PackedNode::Leaf(entries) => {
                        for entry in entries {
                            sequence += 1;
                            heap.push(NearestCandidate {
                                distance: bound(entry.min, entry.max).max(candidate.distance),
                                sequence,
                                item: NearestItem::Entry(entry),
                            });
                        }
                    }
                }
            }
            NearestItem::Entry(entry) => visit(candidate.distance, entry)?,
        }
    }
    Ok(())
}

enum NearestItem {
    Node(u64),
    Entry(PackedSpatialEntry),
}

/// Heap element ordered so `BinaryHeap::pop` yields the SMALLEST distance;
/// equal distances pop in insertion order, keeping the walk deterministic.
struct NearestCandidate {
    distance: f64,
    sequence: u64,
    item: NearestItem,
}

impl PartialEq for NearestCandidate {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == std::cmp::Ordering::Equal
    }
}

impl Eq for NearestCandidate {}

impl PartialOrd for NearestCandidate {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for NearestCandidate {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        other
            .distance
            .total_cmp(&self.distance)
            .then_with(|| other.sequence.cmp(&self.sequence))
    }
}

/// Full traversal: every entry in the tree, no pruning. What verify's live
/// count runs on.
pub(crate) fn for_each_entry(
    meta: &PackedSpatialMeta,
    read: &mut dyn FnMut(u64) -> Result<Vec<u8>>,
    visit: &mut dyn FnMut(PackedSpatialEntry) -> Result<()>,
) -> Result<()> {
    search_envelope(
        meta,
        [f64::NEG_INFINITY, f64::NEG_INFINITY],
        [f64::INFINITY, f64::INFINITY],
        read,
        visit,
    )
}

pub(crate) fn decode_node(bytes: &[u8]) -> Result<PackedNode> {
    let mut reader = Reader::new(bytes, "packed spatial node");
    let version = reader.u8()?;
    if version != NODE_VERSION {
        return Err(BicDbError::Index(format!(
            "unsupported packed spatial node version {version}"
        )));
    }
    let kind = reader.u8()?;
    let count = reader.u32()? as usize;
    match kind {
        NODE_KIND_INTERNAL => {
            let mut children = Vec::with_capacity(count.min(PACKED_NODE_CAPACITY));
            for _ in 0..count {
                let (min, max) = reader.mbr()?;
                let node = reader.u64()?;
                children.push(PackedChild { min, max, node });
            }
            Ok(PackedNode::Internal(children))
        }
        NODE_KIND_LEAF => {
            let mut entries = Vec::with_capacity(count.min(PACKED_NODE_CAPACITY));
            for _ in 0..count {
                let (min, max) = reader.mbr()?;
                let flags = reader.u8()?;
                let point = if flags & 1 != 0 {
                    Some([reader.f64()?, reader.f64()?])
                } else {
                    None
                };
                let id_len = reader.u16()? as usize;
                let record_id = reader.string(id_len)?;
                entries.push(PackedSpatialEntry {
                    record_id,
                    min,
                    max,
                    point,
                });
            }
            Ok(PackedNode::Leaf(entries))
        }
        other => Err(BicDbError::Index(format!(
            "packed spatial node has unknown kind {other}"
        ))),
    }
}

fn encode_leaf(buffer: &mut Vec<u8>, entries: &[PackedSpatialEntry]) {
    buffer.clear();
    buffer.push(NODE_VERSION);
    buffer.push(NODE_KIND_LEAF);
    buffer.extend_from_slice(&(entries.len() as u32).to_le_bytes());
    for entry in entries {
        push_mbr(buffer, entry.min, entry.max);
        match entry.point {
            Some(point) => {
                buffer.push(1);
                buffer.extend_from_slice(&point[0].to_le_bytes());
                buffer.extend_from_slice(&point[1].to_le_bytes());
            }
            None => buffer.push(0),
        }
        buffer.extend_from_slice(&(entry.record_id.len() as u16).to_le_bytes());
        buffer.extend_from_slice(entry.record_id.as_bytes());
    }
}

fn encode_internal(buffer: &mut Vec<u8>, children: &[PackedChild]) {
    buffer.clear();
    buffer.push(NODE_VERSION);
    buffer.push(NODE_KIND_INTERNAL);
    buffer.extend_from_slice(&(children.len() as u32).to_le_bytes());
    for child in children {
        push_mbr(buffer, child.min, child.max);
        buffer.extend_from_slice(&child.node.to_le_bytes());
    }
}

fn push_mbr(buffer: &mut Vec<u8>, min: [f64; 2], max: [f64; 2]) {
    buffer.extend_from_slice(&min[0].to_le_bytes());
    buffer.extend_from_slice(&min[1].to_le_bytes());
    buffer.extend_from_slice(&max[0].to_le_bytes());
    buffer.extend_from_slice(&max[1].to_le_bytes());
}

fn intersects(a_min: [f64; 2], a_max: [f64; 2], b_min: [f64; 2], b_max: [f64; 2]) -> bool {
    a_min[0] <= b_max[0] && b_min[0] <= a_max[0] && a_min[1] <= b_max[1] && b_min[1] <= a_max[1]
}

fn chunk_bounds(items: impl Iterator<Item = ([f64; 2], [f64; 2])>) -> ([f64; 2], [f64; 2]) {
    let mut min = [f64::INFINITY, f64::INFINITY];
    let mut max = [f64::NEG_INFINITY, f64::NEG_INFINITY];
    for (item_min, item_max) in items {
        min[0] = min[0].min(item_min[0]);
        min[1] = min[1].min(item_min[1]);
        max[0] = max[0].max(item_max[0]);
        max[1] = max[1].max(item_max[1]);
    }
    (min, max)
}

/// Sort entries along the Hilbert curve of their MBR centers, normalized to
/// the dataset's bounding box on a 2^16 grid.
fn order_by_hilbert(entries: &mut [PackedSpatialEntry]) {
    let (min, max) = chunk_bounds(entries.iter().map(|entry| (entry.min, entry.max)));
    let cells = (1u32 << HILBERT_ORDER) as f64;
    let span_x = (max[0] - min[0]).max(f64::MIN_POSITIVE);
    let span_y = (max[1] - min[1]).max(f64::MIN_POSITIVE);
    let cell_of = |value: f64, low: f64, span: f64| -> u32 {
        let scaled = ((value - low) / span * cells).floor();
        (scaled.max(0.0) as u32).min((1 << HILBERT_ORDER) - 1)
    };
    entries.sort_by_cached_key(|entry| {
        let center_x = (entry.min[0] + entry.max[0]) / 2.0;
        let center_y = (entry.min[1] + entry.max[1]) / 2.0;
        let x = cell_of(center_x, min[0], span_x);
        let y = cell_of(center_y, min[1], span_y);
        hilbert_d(x, y)
    });
}

/// Hilbert distance of a grid cell (order [`HILBERT_ORDER`]): the classic
/// iterative xy→d conversion with quadrant rotation.
fn hilbert_d(mut x: u32, mut y: u32) -> u64 {
    let side = 1u32 << HILBERT_ORDER;
    let mut d: u64 = 0;
    let mut s = side / 2;
    while s > 0 {
        let rx = u32::from(x & s > 0);
        let ry = u32::from(y & s > 0);
        d += (s as u64) * (s as u64) * ((3 * rx) ^ ry) as u64;
        // Rotate the quadrant so the curve stays continuous.
        if ry == 0 {
            if rx == 1 {
                x = side - 1 - x;
                y = side - 1 - y;
            }
            std::mem::swap(&mut x, &mut y);
        }
        s /= 2;
    }
    d
}

/// Sort-Tile-Recursive at the leaf level: order by center x, cut into
/// `ceil(sqrt(leaf_count))` vertical slabs, order each slab by center y.
/// Consecutive capacity-sized runs then become leaves.
fn order_by_str(entries: &mut [PackedSpatialEntry]) {
    let count = entries.len();
    let leaf_count = count.div_ceil(PACKED_NODE_CAPACITY);
    let slabs = (leaf_count as f64).sqrt().ceil() as usize;
    let slab_len = count.div_ceil(slabs.max(1));
    entries.sort_by(|left, right| {
        let left_x = left.min[0] + left.max[0];
        let right_x = right.min[0] + right.max[0];
        left_x
            .total_cmp(&right_x)
            .then_with(|| left.record_id.cmp(&right.record_id))
    });
    for slab in entries.chunks_mut(slab_len.max(1)) {
        slab.sort_by(|left, right| {
            let left_y = left.min[1] + left.max[1];
            let right_y = right.min[1] + right.max[1];
            left_y
                .total_cmp(&right_y)
                .then_with(|| left.record_id.cmp(&right.record_id))
        });
    }
}

struct Reader<'a> {
    bytes: &'a [u8],
    offset: usize,
    what: &'static str,
}

impl<'a> Reader<'a> {
    fn new(bytes: &'a [u8], what: &'static str) -> Self {
        Self {
            bytes,
            offset: 0,
            what,
        }
    }

    fn take(&mut self, len: usize) -> Result<&'a [u8]> {
        let end = self
            .offset
            .checked_add(len)
            .filter(|end| *end <= self.bytes.len());
        let Some(end) = end else {
            return Err(BicDbError::Index(format!("{} is truncated", self.what)));
        };
        let slice = &self.bytes[self.offset..end];
        self.offset = end;
        Ok(slice)
    }

    fn u8(&mut self) -> Result<u8> {
        Ok(self.take(1)?[0])
    }

    fn u16(&mut self) -> Result<u16> {
        Ok(u16::from_le_bytes(self.take(2)?.try_into().expect("len 2")))
    }

    fn u32(&mut self) -> Result<u32> {
        Ok(u32::from_le_bytes(self.take(4)?.try_into().expect("len 4")))
    }

    fn u64(&mut self) -> Result<u64> {
        Ok(u64::from_le_bytes(self.take(8)?.try_into().expect("len 8")))
    }

    fn f64(&mut self) -> Result<f64> {
        Ok(f64::from_le_bytes(self.take(8)?.try_into().expect("len 8")))
    }

    fn mbr(&mut self) -> Result<([f64; 2], [f64; 2])> {
        Ok(([self.f64()?, self.f64()?], [self.f64()?, self.f64()?]))
    }

    fn string(&mut self, len: usize) -> Result<String> {
        String::from_utf8(self.take(len)?.to_vec())
            .map_err(|error| BicDbError::Index(format!("{} id not UTF-8: {error}", self.what)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    fn point_entry(id: &str, x: f64, y: f64) -> PackedSpatialEntry {
        PackedSpatialEntry {
            record_id: id.to_string(),
            min: [x, y],
            max: [x, y],
            point: Some([x, y]),
        }
    }

    fn build_in_memory(
        entries: Vec<PackedSpatialEntry>,
        strategy: SpatialPackStrategy,
    ) -> (PackedSpatialMeta, BTreeMap<u64, Vec<u8>>) {
        let mut nodes = BTreeMap::new();
        let meta = build_packed_tree(entries, strategy, &mut |node, bytes| {
            nodes.insert(node, bytes.to_vec());
            Ok(())
        })
        .expect("build");
        (meta, nodes)
    }

    fn search_ids(
        meta: &PackedSpatialMeta,
        nodes: &BTreeMap<u64, Vec<u8>>,
        min: [f64; 2],
        max: [f64; 2],
    ) -> Vec<String> {
        let mut ids = Vec::new();
        search_envelope(
            meta,
            min,
            max,
            &mut |node| Ok(nodes.get(&node).expect("node exists").clone()),
            &mut |entry| {
                ids.push(entry.record_id);
                Ok(())
            },
        )
        .expect("search");
        ids.sort();
        ids
    }

    /// Deterministic pseudo-random points (no external RNG dep).
    fn scattered(count: usize) -> Vec<PackedSpatialEntry> {
        let mut state: u64 = 0x9E37_79B9_7F4A_7C15;
        (0..count)
            .map(|index| {
                state = state
                    .wrapping_mul(6364136223846793005)
                    .wrapping_add(1442695040888963407);
                let x = ((state >> 20) & 0xFFFF) as f64 / 655.36 - 50.0;
                state = state
                    .wrapping_mul(6364136223846793005)
                    .wrapping_add(1442695040888963407);
                let y = ((state >> 20) & 0xFFFF) as f64 / 655.36 - 50.0;
                point_entry(&format!("r{index:06}"), x, y)
            })
            .collect()
    }

    #[test]
    fn hilbert_curve_is_a_bijection_on_a_small_grid() {
        // Every cell of an 8x8 sub-grid maps to a distinct distance, and
        // consecutive distances are adjacent cells (the defining property).
        let scale = 1u32 << (HILBERT_ORDER - 3);
        let mut seen = BTreeMap::new();
        for x in 0..8u32 {
            for y in 0..8u32 {
                seen.insert(hilbert_d(x * scale, y * scale), (x, y));
            }
        }
        assert_eq!(seen.len(), 64);
        let cells: Vec<(u32, u32)> = seen.into_values().collect();
        for pair in cells.windows(2) {
            let dx = pair[0].0.abs_diff(pair[1].0);
            let dy = pair[0].1.abs_diff(pair[1].1);
            assert_eq!(
                dx + dy,
                1,
                "curve jumps between {:?} and {:?}",
                pair[0],
                pair[1]
            );
        }
    }

    #[test]
    fn meta_and_node_codecs_roundtrip() {
        let meta = PackedSpatialMeta {
            strategy: SpatialPackStrategy::Str,
            generation: 42,
            root_node: 7,
            height: 3,
            node_count: 99,
            entry_count: 12345,
        };
        assert_eq!(PackedSpatialMeta::decode(&meta.encode()).unwrap(), meta);

        let entries = vec![
            point_entry("a", 1.5, -2.5),
            PackedSpatialEntry {
                record_id: "poly".to_string(),
                min: [-1.0, -1.0],
                max: [4.0, 4.0],
                point: None,
            },
        ];
        let mut buffer = Vec::new();
        encode_leaf(&mut buffer, &entries);
        match decode_node(&buffer).unwrap() {
            PackedNode::Leaf(decoded) => assert_eq!(decoded, entries),
            PackedNode::Internal(_) => panic!("expected leaf"),
        }

        let children = vec![PackedChild {
            min: [0.0, 0.0],
            max: [10.0, 10.0],
            node: 3,
        }];
        encode_internal(&mut buffer, &children);
        match decode_node(&buffer).unwrap() {
            PackedNode::Internal(decoded) => {
                assert_eq!(decoded.len(), 1);
                assert_eq!(decoded[0].node, 3);
                assert_eq!(decoded[0].min, [0.0, 0.0]);
                assert_eq!(decoded[0].max, [10.0, 10.0]);
            }
            PackedNode::Leaf(_) => panic!("expected internal"),
        }

        assert!(decode_node(&buffer[..buffer.len() - 1]).is_err());
    }

    #[test]
    fn delta_codec_roundtrips() {
        let entry = point_entry("d", 3.0, 4.0);
        assert_eq!(
            decode_delta(&encode_delta_upsert(&entry)).unwrap(),
            Some(([3.0, 4.0], [3.0, 4.0], Some([3.0, 4.0])))
        );
        assert_eq!(decode_delta(&encode_delta_tombstone()).unwrap(), None);
        assert!(decode_delta(&[9]).is_err());
    }

    #[test]
    fn empty_and_single_entry_trees() {
        let (meta, nodes) = build_in_memory(Vec::new(), SpatialPackStrategy::Hilbert);
        assert_eq!(meta.entry_count, 0);
        assert_eq!(meta.node_count, 0);
        assert!(nodes.is_empty());
        assert!(search_ids(&meta, &nodes, [-100.0, -100.0], [100.0, 100.0]).is_empty());

        let (meta, nodes) = build_in_memory(
            vec![point_entry("only", 1.0, 2.0)],
            SpatialPackStrategy::Str,
        );
        assert_eq!(meta.entry_count, 1);
        assert_eq!(meta.node_count, 1);
        assert_eq!(meta.height, 1);
        assert_eq!(
            search_ids(&meta, &nodes, [0.0, 0.0], [3.0, 3.0]),
            vec!["only".to_string()]
        );
        assert!(search_ids(&meta, &nodes, [5.0, 5.0], [6.0, 6.0]).is_empty());
    }

    #[test]
    fn packed_search_matches_brute_force_for_both_strategies() {
        let entries = scattered(3000);
        for strategy in [SpatialPackStrategy::Hilbert, SpatialPackStrategy::Str] {
            let (meta, nodes) = build_in_memory(entries.clone(), strategy);
            assert_eq!(meta.entry_count, 3000);
            assert!(meta.height >= 2, "3000 entries should stack levels");
            let windows = [
                ([-10.0, -10.0], [10.0, 10.0]),
                ([0.0, 0.0], [0.5, 0.5]),
                ([-50.0, -50.0], [50.0, 50.0]),
                ([49.0, 49.0], [60.0, 60.0]),
            ];
            for (min, max) in windows {
                let mut expected: Vec<String> = entries
                    .iter()
                    .filter(|entry| intersects(min, max, entry.min, entry.max))
                    .map(|entry| entry.record_id.clone())
                    .collect();
                expected.sort();
                assert_eq!(
                    search_ids(&meta, &nodes, min, max),
                    expected,
                    "strategy {strategy:?} window {min:?}..{max:?}"
                );
            }

            // Full traversal sees every entry exactly once.
            let mut all = Vec::new();
            for_each_entry(
                &meta,
                &mut |node| Ok(nodes.get(&node).expect("node").clone()),
                &mut |entry| {
                    all.push(entry.record_id);
                    Ok(())
                },
            )
            .unwrap();
            all.sort();
            let mut expected: Vec<String> = entries
                .iter()
                .map(|entry| entry.record_id.clone())
                .collect();
            expected.sort();
            assert_eq!(all, expected);
        }
    }

    fn planar_rect_distance_2(query: [f64; 2], min: [f64; 2], max: [f64; 2]) -> f64 {
        let dx = (min[0] - query[0]).max(0.0).max(query[0] - max[0]);
        let dy = (min[1] - query[1]).max(0.0).max(query[1] - max[1]);
        dx * dx + dy * dy
    }

    /// Bounded planar k-NN over an in-memory tree, mirroring how db.rs drives
    /// the walk: collect while distance <= kth-so-far, count node reads.
    fn nearest_ids(
        meta: &PackedSpatialMeta,
        nodes: &BTreeMap<u64, Vec<u8>>,
        query: [f64; 2],
        k: usize,
    ) -> (Vec<String>, usize) {
        use std::cell::Cell;
        let mut reads = 0usize;
        let mut hits: Vec<(f64, String)> = Vec::new();
        let cutoff = Cell::new(f64::INFINITY);
        search_nearest(
            meta,
            &mut |node| {
                reads += 1;
                Ok(nodes.get(&node).expect("node exists").clone())
            },
            &mut |min, max| planar_rect_distance_2(query, min, max),
            &{
                let cutoff = &cutoff;
                move || cutoff.get()
            },
            &mut |distance, entry| {
                hits.push((distance, entry.record_id));
                if hits.len() >= k {
                    let mut kth: Vec<f64> = hits.iter().map(|hit| hit.0).collect();
                    kth.sort_by(f64::total_cmp);
                    cutoff.set(kth[k - 1]);
                }
                Ok(())
            },
        )
        .expect("search");
        hits.sort_by(|left, right| {
            left.0
                .total_cmp(&right.0)
                .then_with(|| left.1.cmp(&right.1))
        });
        hits.truncate(k);
        (hits.into_iter().map(|(_, id)| id).collect(), reads)
    }

    #[test]
    fn nearest_matches_brute_force_for_both_strategies() {
        let entries = scattered(3000);
        for strategy in [SpatialPackStrategy::Hilbert, SpatialPackStrategy::Str] {
            let (meta, nodes) = build_in_memory(entries.clone(), strategy);
            let queries = [
                [0.0, 0.0],
                [-49.5, 49.5],
                [12.3, -7.7],
                [200.0, 200.0], // far outside the corpus
            ];
            for query in queries {
                for k in [1usize, 5, 64, 3500] {
                    let mut expected: Vec<(f64, String)> = entries
                        .iter()
                        .map(|entry| {
                            (
                                planar_rect_distance_2(query, entry.min, entry.max),
                                entry.record_id.clone(),
                            )
                        })
                        .collect();
                    expected.sort_by(|left, right| {
                        left.0
                            .total_cmp(&right.0)
                            .then_with(|| left.1.cmp(&right.1))
                    });
                    expected.truncate(k);
                    let expected: Vec<String> = expected.into_iter().map(|(_, id)| id).collect();
                    let (actual, _) = nearest_ids(&meta, &nodes, query, k);
                    assert_eq!(
                        actual, expected,
                        "strategy {strategy:?} query {query:?} k {k}"
                    );
                }
            }
        }
    }

    #[test]
    fn nearest_visits_a_bounded_slice_of_the_tree() {
        let entries = scattered(PACKED_NODE_CAPACITY * 200); // 51_200 entries
        let (meta, nodes) = build_in_memory(entries, SpatialPackStrategy::Hilbert);
        assert!(meta.node_count > 200);
        let (ids, reads) = nearest_ids(&meta, &nodes, [3.0, -11.0], 8);
        assert_eq!(ids.len(), 8);
        // Best-first must open a handful of nodes, not the corpus: root +
        // a few leaves around the query plus tie spillover.
        assert!(
            reads <= 12,
            "expected a bounded descent, read {reads} of {} nodes",
            meta.node_count
        );
    }

    #[test]
    fn nearest_delivers_all_candidates_tying_the_cutoff() {
        // Four points equidistant from the origin plus a nearer and a
        // farther one; k=2 must see every tie at the kth distance so the
        // caller's id tiebreak is deterministic.
        let entries = vec![
            point_entry("near", 0.1, 0.0),
            point_entry("tie-d", 5.0, 0.0),
            point_entry("tie-c", -5.0, 0.0),
            point_entry("tie-b", 0.0, 5.0),
            point_entry("tie-a", 0.0, -5.0),
            point_entry("far", 40.0, 40.0),
        ];
        let (meta, nodes) = build_in_memory(entries, SpatialPackStrategy::Hilbert);
        let (ids, _) = nearest_ids(&meta, &nodes, [0.0, 0.0], 2);
        assert_eq!(ids, vec!["near".to_string(), "tie-a".to_string()]);
        let (ids, _) = nearest_ids(&meta, &nodes, [0.0, 0.0], 4);
        assert_eq!(
            ids,
            vec![
                "near".to_string(),
                "tie-a".to_string(),
                "tie-b".to_string(),
                "tie-c".to_string()
            ]
        );
    }

    #[test]
    fn leaves_pack_to_capacity() {
        let entries = scattered(PACKED_NODE_CAPACITY * 4);
        let (meta, nodes) = build_in_memory(entries, SpatialPackStrategy::Hilbert);
        // 4 exactly-full leaves + 1 root.
        assert_eq!(meta.node_count, 5);
        assert_eq!(meta.height, 2);
        let mut leaf_sizes = Vec::new();
        for bytes in nodes.values() {
            if let PackedNode::Leaf(leaf) = decode_node(bytes).unwrap() {
                leaf_sizes.push(leaf.len());
            }
        }
        assert_eq!(leaf_sizes, vec![PACKED_NODE_CAPACITY; 4]);
    }

    /// Not a correctness test: prints the node-reads-per-query comparison the
    /// strategy choice should be made on. Run with
    /// `cargo test -p bicdb-core --lib packed_strategy_comparison -- --ignored --nocapture`.
    #[test]
    #[ignore]
    fn packed_strategy_comparison() {
        let entries = scattered(200_000);
        for strategy in [SpatialPackStrategy::Hilbert, SpatialPackStrategy::Str] {
            let (meta, nodes) = build_in_memory(entries.clone(), strategy);
            let mut reads = 0usize;
            let queries = 500;
            for query in 0..queries {
                let x = (query % 100) as f64 - 50.0;
                let y = ((query * 7) % 100) as f64 - 50.0;
                search_envelope(
                    &meta,
                    [x, y],
                    [x + 2.0, y + 2.0],
                    &mut |node| {
                        reads += 1;
                        Ok(nodes.get(&node).expect("node").clone())
                    },
                    &mut |_| Ok(()),
                )
                .unwrap();
            }
            println!(
                "strategy={} nodes={} height={} node_reads_per_query={:.2}",
                strategy.label(),
                meta.node_count,
                meta.height,
                reads as f64 / queries as f64
            );
        }
    }
}
