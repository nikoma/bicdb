//! Incrementally maintained aggregate projections — the G1–G4 core of
//! `docs/olap-cubes.md`, deliberately narrow.
//!
//! One projection maintains aggregate **cells** keyed by a declared dimension
//! tuple over one collection, fed by that collection's audit events.
//!
//! The whole design turns on one fact: the record-audit stream carries only
//! the **after**-image. Retracting an update therefore cannot read the old
//! row — the projection owns a per-record **input state** holding exactly the
//! tuple it last contributed, and retracts from that. The same structure
//! carries the version that makes application effectively-once.
//!
//! Not included here on purpose: MIN/MAX, sketches, rollups, rebuild/swap,
//! distribution, and any SQL surface. Those are later stages; this is the part
//! that has to be boring and provably correct first.

use crate::aggregate_sketch::{stable_hash_bytes, HyperLogLog, Quantiles};
use std::collections::{BTreeMap, BTreeSet};

/// Bytes per persistence page. Small enough that a handful of mutated rows
/// rewrites little, large enough that page bookkeeping stays cheap.
const PAGE_BYTES: usize = 32 * 1024;
use std::path::{Path, PathBuf};

use rustc_hash::{FxHashMap, FxHashSet};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::db::BicDb;
use crate::error::{BicDbError, Result};
use crate::event::{StoredEvent, RECORD_AUDIT_STREAM};

/// A dimension value. Absent/null fields collapse to `Missing` so a cell key
/// is always total — a row with no `hosting_provider` still has to land
/// somewhere, and silently dropping it would make cells disagree with the base
/// table's `COUNT(*)`.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum DimensionValue {
    Text(String),
    Int(i64),
    Bool(bool),
    Missing,
}

impl DimensionValue {
    fn from_json(value: Option<&serde_json::Value>) -> Self {
        match value {
            Some(serde_json::Value::String(text)) => Self::Text(text.clone()),
            Some(serde_json::Value::Bool(flag)) => Self::Bool(*flag),
            Some(serde_json::Value::Number(number)) => number
                .as_i64()
                .map(Self::Int)
                .unwrap_or_else(|| Self::Text(number.to_string())),
            _ => Self::Missing,
        }
    }
}

pub type CellKey = Vec<DimensionValue>;

/// Stable, build-independent hash of a dimension value for distinct-counting.
fn hash_dimension(value: &DimensionValue) -> u64 {
    match value {
        DimensionValue::Text(text) => stable_hash_bytes(1, text.as_bytes()),
        DimensionValue::Int(number) => stable_hash_bytes(2, &number.to_le_bytes()),
        DimensionValue::Bool(flag) => stable_hash_bytes(3, &[*flag as u8]),
        DimensionValue::Missing => stable_hash_bytes(4, &[]),
    }
}

/// A measure that is **not retractable**.
///
/// Every other measure in this engine is an abelian group: apply adds,
/// retract subtracts, and the two cancel exactly. Sketches are not. Removing
/// one row from a distinct-count cannot decrement anything, because the
/// sketch does not know whether some other row carried the same value; and
/// removing a sample from a bottom-k sketch cannot recover the sample that
/// should replace it.
///
/// So sketch measures get a different contract, stated here rather than
/// hidden: **retract marks the cell stale, and a stale cell is recomputed
/// from its own rows.** That is `O(cell)`, not `O(table)`, which is only
/// possible because each cell threads its input rows on a linked list.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum SketchKind {
    /// Approximate `COUNT(DISTINCT path)`.
    DistinctCount,
    /// Approximate percentiles of a numeric `path`.
    Quantile,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SketchSpec {
    pub name: String,
    pub path: String,
    pub kind: SketchKind,
}

/// At most four sketches, so their presence bits share the input row's
/// existing `present` byte with the four measures.
pub const MAX_SKETCHES: usize = 4;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum SketchState {
    Distinct(HyperLogLog),
    Quantile(Quantiles),
}

impl SketchState {
    fn empty(kind: SketchKind) -> Self {
        match kind {
            SketchKind::DistinctCount => Self::Distinct(HyperLogLog::default()),
            SketchKind::Quantile => Self::Quantile(Quantiles::default()),
        }
    }

    /// Fold one input row's stored sketch word in. The word is already
    /// reduced (a stable hash for distinct, the f64 bits for quantiles), so
    /// this never re-reads the source record.
    fn observe(&mut self, word: u64, identity: u64) {
        match self {
            Self::Distinct(hll) => hll.add_hash(word),
            Self::Quantile(quantiles) => quantiles.add_keyed(f64::from_bits(word), identity),
        }
    }

    /// Associative combine — the property G9 needs to merge shard partials.
    pub fn merge(&mut self, other: &Self) {
        match (self, other) {
            (Self::Distinct(left), Self::Distinct(right)) => left.merge(right),
            (Self::Quantile(left), Self::Quantile(right)) => left.merge(right),
            // Kinds are fixed by the projection definition, so a mismatch is
            // a programming error, not a data condition.
            _ => {}
        }
    }

    pub fn distinct_estimate(&self) -> Option<f64> {
        match self {
            Self::Distinct(hll) => Some(hll.estimate()),
            Self::Quantile(_) => None,
        }
    }

    pub fn percentile(&self, fraction: f64) -> Option<f64> {
        match self {
            Self::Quantile(quantiles) => quantiles.percentile(fraction),
            Self::Distinct(_) => None,
        }
    }
}

/// Maximum dimensions in a projection's grain. Fixed so a cell key is a
/// plain array rather than a `Vec` — at a million rows the Vec header and
/// per-element enum padding were most of the cost.
pub const MAX_DIMENSIONS: usize = 4;

/// A cell key after interning: one dictionary id per dimension.
/// 16 bytes, `Copy`, ordered — replaces a `Vec<DimensionValue>` that cost
/// ~150 bytes for the same three short strings.
#[derive(
    Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
)]
pub struct CompactKey([u32; MAX_DIMENSIONS]);

/// Per-dimension, projection-local string dictionary.
///
/// Local rather than global on purpose: ids stay small and dense, locality is
/// better, persistence and rebuild are simpler, and one dimension's churn
/// cannot fragment another's. Ids are **monotonic and never reused** — once
/// they are persisted, recycling an id would silently re-point existing cells
/// at a different value. Compaction happens only during a generation rebuild
/// (docs §10), which is exactly where a fresh dictionary is safe.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
struct Dictionary {
    values: Vec<DimensionValue>,
    #[serde(skip)]
    lookup: FxHashMap<DimensionValue, u32>,
}

impl Dictionary {
    /// Id 0 is permanently `Missing`, so an absent field needs no special
    /// case and still lands in a cell.
    fn new() -> Self {
        let mut dictionary = Self::default();
        dictionary.intern(&DimensionValue::Missing);
        dictionary
    }

    fn intern(&mut self, value: &DimensionValue) -> u32 {
        if let Some(id) = self.lookup.get(value) {
            return *id;
        }
        let id = self.values.len() as u32;
        self.values.push(value.clone());
        self.lookup.insert(value.clone(), id);
        id
    }

    /// Lookup without minting — used on the read path so a query for a value
    /// that was never written does not grow the dictionary.
    fn get(&self, value: &DimensionValue) -> Option<u32> {
        self.lookup.get(value).copied()
    }

    fn resolve(&self, id: u32) -> DimensionValue {
        self.values
            .get(id as usize)
            .cloned()
            .unwrap_or(DimensionValue::Missing)
    }

    fn resident_bytes(&self) -> u64 {
        let entry = |value: &DimensionValue| -> u64 {
            std::mem::size_of::<DimensionValue>() as u64
                + match value {
                    DimensionValue::Text(text) => text.capacity() as u64,
                    _ => 0,
                }
        };
        // Stored once in `values`, once as a `lookup` key, plus map overhead.
        self.values.iter().map(|value| entry(value) * 2 + 16).sum()
    }
}

/// Stable 128-bit identity for a source record.
///
/// The projection never needs to recover the primary key — only to recognise
/// the same record again — so it stores a digest instead of the string. At
/// 70M rows a UUID primary key would be ~4 GiB of `String` on its own; this
/// is 16 bytes. 128 bits puts collision probability around 1e-23 at that
/// scale, far below hardware error rates, and `reconcile` would surface one
/// regardless.
fn record_key(record_id: &str) -> u128 {
    let digest = Sha256::digest(record_id.as_bytes());
    let mut bytes = [0u8; 16];
    bytes.copy_from_slice(&digest[..16]);
    u128::from_le_bytes(bytes)
}

/// Additive measure state. Every field here is retractable and mergeable —
/// the two properties the rest of the roadmap depends on.
#[derive(Clone, Copy, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct CellState {
    pub count: i64,
    /// Parallel to the projection's `measure_paths`.
    sums: [f64; MAX_MEASURES],
    /// How many rows actually contributed a numeric value per measure, so
    /// AVG divides by the right denominator rather than by `count`.
    sum_counts: [i64; MAX_MEASURES],
}

pub const MAX_MEASURES: usize = 4;

impl CellState {
    pub fn sum(&self, measure: usize) -> f64 {
        self.sums.get(measure).copied().unwrap_or_default()
    }

    /// `None` when no row in this cell carried a value for the measure —
    /// which is different from an average of zero.
    pub fn avg(&self, measure: usize) -> Option<f64> {
        let denominator = self.sum_counts.get(measure).copied().unwrap_or_default();
        (denominator > 0).then(|| self.sums[measure] / denominator as f64)
    }

    /// Associative merge — the property that makes per-shard partial states
    /// combinable at a coordinator without re-scanning (docs §8).
    pub fn merge(&mut self, other: &Self) {
        self.count += other.count;
        for index in 0..MAX_MEASURES {
            self.sums[index] += other.sums[index];
            self.sum_counts[index] += other.sum_counts[index];
        }
    }

    fn apply(&mut self, input: &InputState) {
        self.count += 1;
        for index in 0..MAX_MEASURES {
            if let Some(value) = input.measure(index) {
                self.sums[index] += value;
                self.sum_counts[index] += 1;
            }
        }
    }

    fn retract(&mut self, input: &InputState) {
        self.count -= 1;
        for index in 0..MAX_MEASURES {
            if let Some(value) = input.measure(index) {
                self.sums[index] -= value;
                self.sum_counts[index] -= 1;
            }
        }
    }

    fn is_empty(&self) -> bool {
        self.count == 0 && self.sum_counts.iter().all(|count| *count == 0)
    }
}

/// Exactly what one source record last contributed. This is the retract
/// source of truth (docs §2) and the per-record ordering guard (docs §4).
/// The physical layout of one projection's input state — **format v1**.
///
/// Width is derived from the projection's declared grain, so a `COUNT`-only
/// projection does not carry room for measures it will never have. Cost
/// becomes `identity + projected dimensions + projected measure
/// contributions` rather than `MAX_EVERYTHING`.
///
/// Byte order is fixed and little-endian so G5 can persist the slab as-is:
///
/// ```text
/// offset 0                  digest     u128 (record identity)
/// offset 16                 version    u64  (u64::MAX = free slot)
/// offset 24                 present    u8   (bit i = measure i had a value)
/// offset 25                 dimensions u32 x n_dimensions
/// offset 25 + 4n            measures   f64 x n_measures
/// width  = 25 + 4n + 8m
/// ```
///
/// The row carries its own **identity** and a free-slot tombstone so the
/// digest->slot index and the free list are DERIVABLE by scanning the slab.
/// Without that, incremental page writes would still need an O(state)
/// serialization of those maps on every checkpoint, defeating the point.
///
/// **Format v1 is frozen once G5 persists it.** After that, changes are a
/// generation upgrade (rebuild as v2 → catch up → atomic swap), not a struct
/// edit — see docs §10.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProjectionLayout {
    pub dimensions: usize,
    pub measures: usize,
    /// Sketch input words appended after the measures — see `SketchKind`.
    #[serde(default)]
    pub sketches: usize,
}

impl ProjectionLayout {
    pub const IDENTITY_WIDTH: usize = std::mem::size_of::<u128>();
    const DIGEST_OFFSET: usize = 0;
    const VERSION_OFFSET: usize = 16;
    const PRESENT_OFFSET: usize = 24;
    const DIMENSIONS_OFFSET: usize = 25;
    /// Version value marking a vacated slot.
    const FREE_VERSION: u64 = u64::MAX;

    fn new(dimensions: usize, measures: usize, sketches: usize) -> Self {
        Self {
            dimensions,
            measures,
            sketches,
        }
    }

    fn sketches_offset(&self) -> usize {
        self.measures_offset() + self.measures * 8
    }

    fn measures_offset(&self) -> usize {
        Self::DIMENSIONS_OFFSET + self.dimensions * 4
    }

    /// Bytes of state stored per source record, excluding the identity key.
    pub fn state_width(&self) -> usize {
        self.sketches_offset() + self.sketches * 8
    }

    /// What one source row logically costs: identity + its own state.
    pub fn logical_bytes_per_row(&self) -> usize {
        // The identity digest is part of the row now (format v2), so the
        // state width already includes it.
        self.state_width()
    }

    /// Bytes of aggregate state per CELL — also derived from the declared
    /// grain, so a `COUNT`-only projection stores no measure storage at all.
    ///
    /// ```text
    /// offset 0        key         u32 x MAX_DIMENSIONS (count i64::MIN = free)
    /// offset 16       count       i64
    /// offset 24       sums        f64 x m
    /// offset 24+8m    sum_counts  i64 x m
    /// width = 24 + 16m
    /// ```
    ///
    /// Like input rows, a cell row carries its own key so the key->slot map
    /// is derivable from the pages rather than separately serialized.
    pub fn cell_width(&self) -> usize {
        24 + self.measures * 16
    }

    const CELL_FREE: i64 = i64::MIN;

    fn write_cell_key(&self, slot: &mut [u8], key: &CompactKey) {
        for index in 0..MAX_DIMENSIONS {
            let at = index * 4;
            slot[at..at + 4].copy_from_slice(&key.0[index].to_le_bytes());
        }
    }

    fn read_cell_key(&self, slot: &[u8]) -> CompactKey {
        let mut key = CompactKey::default();
        for index in 0..MAX_DIMENSIONS {
            let at = index * 4;
            key.0[index] = u32::from_le_bytes(slot[at..at + 4].try_into().expect("cell key"));
        }
        key
    }

    fn mark_cell_free(&self, slot: &mut [u8]) {
        slot[16..24].copy_from_slice(&Self::CELL_FREE.to_le_bytes());
    }

    fn is_cell_free(&self, slot: &[u8]) -> bool {
        i64::from_le_bytes(slot[16..24].try_into().expect("cell count")) == Self::CELL_FREE
    }

    fn write_cell(&self, slot: &mut [u8], state: &CellState) {
        slot[16..24].copy_from_slice(&state.count.to_le_bytes());
        for index in 0..self.measures {
            let sum_at = 24 + index * 8;
            slot[sum_at..sum_at + 8].copy_from_slice(&state.sums[index].to_le_bytes());
            let count_at = 24 + self.measures * 8 + index * 8;
            slot[count_at..count_at + 8].copy_from_slice(&state.sum_counts[index].to_le_bytes());
        }
    }

    fn read_cell(&self, slot: &[u8]) -> CellState {
        let mut state = CellState {
            count: i64::from_le_bytes(slot[16..24].try_into().expect("cell count")),
            sums: [0.0; MAX_MEASURES],
            sum_counts: [0; MAX_MEASURES],
        };
        for index in 0..self.measures {
            let sum_at = 24 + index * 8;
            state.sums[index] =
                f64::from_le_bytes(slot[sum_at..sum_at + 8].try_into().expect("cell sum"));
            let count_at = 24 + self.measures * 8 + index * 8;
            state.sum_counts[index] =
                i64::from_le_bytes(slot[count_at..count_at + 8].try_into().expect("cell n"));
        }
        state
    }

    fn write(&self, slot: &mut [u8], digest: u128, state: &InputState) {
        slot[Self::DIGEST_OFFSET..Self::DIGEST_OFFSET + 16].copy_from_slice(&digest.to_le_bytes());
        slot[Self::VERSION_OFFSET..Self::VERSION_OFFSET + 8]
            .copy_from_slice(&state.version.to_le_bytes());
        slot[Self::PRESENT_OFFSET] = state.present;
        for index in 0..self.dimensions {
            let at = Self::DIMENSIONS_OFFSET + index * 4;
            slot[at..at + 4].copy_from_slice(&state.key.0[index].to_le_bytes());
        }
        for index in 0..self.measures {
            let at = self.measures_offset() + index * 8;
            slot[at..at + 8].copy_from_slice(&state.measures[index].to_le_bytes());
        }
        for index in 0..self.sketches {
            let at = self.sketches_offset() + index * 8;
            slot[at..at + 8].copy_from_slice(&state.sketches[index].to_le_bytes());
        }
    }

    fn read_digest(&self, slot: &[u8]) -> u128 {
        u128::from_le_bytes(
            slot[Self::DIGEST_OFFSET..Self::DIGEST_OFFSET + 16]
                .try_into()
                .expect("digest field"),
        )
    }

    fn mark_free(&self, slot: &mut [u8]) {
        slot[Self::VERSION_OFFSET..Self::VERSION_OFFSET + 8]
            .copy_from_slice(&Self::FREE_VERSION.to_le_bytes());
    }

    fn is_free(&self, slot: &[u8]) -> bool {
        u64::from_le_bytes(
            slot[Self::VERSION_OFFSET..Self::VERSION_OFFSET + 8]
                .try_into()
                .expect("version field"),
        ) == Self::FREE_VERSION
    }

    fn read(&self, slot: &[u8]) -> InputState {
        let mut state = InputState {
            key: CompactKey::default(),
            measures: [0.0; MAX_MEASURES],
            sketches: [0; MAX_SKETCHES],
            present: slot[Self::PRESENT_OFFSET],
            version: u64::from_le_bytes(
                slot[Self::VERSION_OFFSET..Self::VERSION_OFFSET + 8]
                    .try_into()
                    .expect("version field"),
            ),
        };
        for index in 0..self.dimensions {
            let at = Self::DIMENSIONS_OFFSET + index * 4;
            state.key.0[index] =
                u32::from_le_bytes(slot[at..at + 4].try_into().expect("dimension field"));
        }
        for index in 0..self.measures {
            let at = self.measures_offset() + index * 8;
            state.measures[index] =
                f64::from_le_bytes(slot[at..at + 8].try_into().expect("measure field"));
        }
        for index in 0..self.sketches {
            let at = self.sketches_offset() + index * 8;
            state.sketches[index] =
                u64::from_le_bytes(slot[at..at + 8].try_into().expect("sketch field"));
        }
        state
    }
}

/// Exactly what one source record last contributed. A transient stack value
/// used by apply/retract; the durable form is the packed slab row above. Previously a `CellKey` plus a `Vec<Option<f64>>` (two allocations and
/// ~200 bytes); now the interned key, the measure values with a presence
/// bitmask, and the version.
#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
struct InputState {
    key: CompactKey,
    measures: [f64; MAX_MEASURES],
    /// Already-reduced sketch inputs: a stable hash for distinct-count, the
    /// f64 bits for quantiles. Storing the reduction rather than the original
    /// value is what lets a stale cell be rebuilt from the slab alone.
    sketches: [u64; MAX_SKETCHES],
    /// Bit i set = measure i had a value on this record. Distinguishes
    /// "measured zero" from "absent", which AVG's denominator depends on.
    present: u8,
    /// Source position of the event that produced this state; a later event
    /// with a lower position is stale and ignored.
    version: u64,
}

impl InputState {
    fn measure(&self, index: usize) -> Option<f64> {
        (self.present & (1 << index) != 0).then(|| self.measures[index])
    }

    fn sketch(&self, index: usize) -> Option<u64> {
        (self.present & (1 << (MAX_MEASURES + index)) != 0).then(|| self.sketches[index])
    }
}

/// What a projection costs to keep resident. `docs/olap-cubes.md` §12 makes
/// this reportable on purpose: the per-record input state is the price of
/// being able to retract without before-images, and a projection's grain has
/// to be justified against it rather than assumed free.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProjectionResidency {
    pub cells: usize,
    pub cell_bytes: u64,
    pub input_records: usize,
    /// Actual resident cost of the input state: the packed slab plus the
    /// digest->slot index and its hash-table slack.
    pub input_bytes: u64,
    /// The packed state slab alone, excluding the index and free list — the
    /// number that must not grow when vacated slots are reused.
    pub slab_bytes: u64,
    /// What the state *is*, before any container: `identity + dimensions +
    /// measures + version` per row. Reported separately from `input_bytes`
    /// because once the state is compact the container becomes the next
    /// enemy, and averaging the two would hide that.
    pub input_logical_bytes: u64,
}

impl ProjectionResidency {
    pub fn total_bytes(&self) -> u64 {
        self.cell_bytes.saturating_add(self.input_bytes)
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProjectionDrift {
    pub cells_compared: usize,
    pub cells_differing: usize,
    pub max_count_delta: i64,
    /// Cells present incrementally but absent from the recomputation, or the
    /// reverse. Either direction is a bug.
    pub cells_only_incremental: usize,
    pub cells_only_authoritative: usize,
}

impl ProjectionDrift {
    pub fn is_clean(&self) -> bool {
        self.cells_differing == 0
            && self.cells_only_incremental == 0
            && self.cells_only_authoritative == 0
    }
}

/// An incrementally maintained aggregate over one collection.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AggregateProjection {
    pub name: String,
    pub collection: String,
    /// Metadata field names forming the cell key, in order.
    dimension_paths: Vec<String>,
    /// Metadata field names summed (also feeding AVG).
    measure_paths: Vec<String>,
    /// One dictionary per dimension, in `dimension_paths` order.
    dictionaries: Vec<Dictionary>,
    layout: ProjectionLayout,
    /// Cell key -> slot in `cell_slots`. Cells are derived state, so this
    /// representation is versioned separately and may change freely.
    cells: FxHashMap<CompactKey, u32>,
    cell_slots: Vec<u8>,
    cell_free: Vec<u32>,
    /// Pages touched since the last checkpoint. Persistence cost tracks
    /// MUTATIONS, not total state — the whole point of G6.
    dirty_input_pages: BTreeSet<u32>,
    dirty_cell_pages: BTreeSet<u32>,
    /// What the last published manifest referenced, so an unchanged page is
    /// carried forward by reference instead of rewritten.
    persisted_input_pages: BTreeMap<u32, u64>,
    persisted_cell_pages: BTreeMap<u32, u64>,
    persisted_dictionaries: Vec<u64>,
    persisted_dictionary_lens: Vec<usize>,
    /// Record digest -> slot. Keyed by a digest of the persistent logical
    /// `Record.id`, never by the physical `RowId` (docs §2b).
    index: FxHashMap<u128, u32>,
    /// Flat, layout-strided state. One allocation that grows, rather than a
    /// per-record struct — which is also the shape G5 persists.
    slots: Vec<u8>,
    /// Slots vacated by deletes, reused before the slab grows.
    free_slots: Vec<u32>,
    /// Highest source position durably applied (docs §5). Reads reflect
    /// committed base changes through exactly this point.
    projection_position: u64,
    /// Incremented on every successful publish.
    generation: u64,
    /// Declared non-retractable measures. Part of the projection DEFINITION,
    /// so it is persisted; the sketch state itself is not.
    sketch_specs: Vec<SketchSpec>,
    /// Per-cell sketch state. **Derived**, never persisted: it is recomputed
    /// at load by the same page scan that rebuilds the maps, so adding
    /// sketches costs the G6 manifest nothing and adds no corruption surface.
    cell_sketches: FxHashMap<u32, Vec<SketchState>>,
    /// Doubly-linked list threading every input row of a cell, so a stale
    /// cell is rebuilt in O(cell). Derived; rebuilt by the load scan.
    chain_next: Vec<u32>,
    chain_prev: Vec<u32>,
    cell_head: FxHashMap<u32, u32>,
    /// Cells whose sketches a retract invalidated, pending recomputation.
    stale_cells: FxHashSet<u32>,
}

/// Chain terminator. `u32::MAX` slots cannot exist: the slab would be 128 GiB.
const NIL: u32 = u32::MAX;

impl AggregateProjection {
    pub fn new(
        name: impl Into<String>,
        collection: impl Into<String>,
        dimension_paths: Vec<String>,
        measure_paths: Vec<String>,
    ) -> Result<Self> {
        let name = name.into();
        // Reject at construction as well as at save/load: a projection that
        // cannot be named cannot be built.
        validate_projection_name(&name)?;
        if measure_paths.len() > MAX_MEASURES {
            return Err(BicDbError::ProjectionError(format!(
                "an aggregate projection supports at most {MAX_MEASURES} measures"
            )));
        }
        if dimension_paths.is_empty() {
            return Err(BicDbError::ProjectionError(
                "an aggregate projection needs at least one dimension".to_string(),
            ));
        }
        let measure_paths_len = measure_paths.len();
        let dimension_paths_len = dimension_paths.len();
        if dimension_paths_len > MAX_DIMENSIONS {
            return Err(BicDbError::ProjectionError(format!(
                "an aggregate projection supports at most {MAX_DIMENSIONS} dimensions"
            )));
        }
        Ok(Self {
            layout: ProjectionLayout::new(dimension_paths_len, measure_paths_len, 0),
            name,
            collection: collection.into(),
            dimension_paths,
            measure_paths,
            dictionaries: (0..dimension_paths_len)
                .map(|_| Dictionary::new())
                .collect(),
            cells: FxHashMap::default(),
            cell_slots: Vec::new(),
            cell_free: Vec::new(),
            dirty_input_pages: BTreeSet::new(),
            dirty_cell_pages: BTreeSet::new(),
            persisted_input_pages: BTreeMap::new(),
            persisted_cell_pages: BTreeMap::new(),
            persisted_dictionaries: Vec::new(),
            persisted_dictionary_lens: Vec::new(),
            index: FxHashMap::default(),
            slots: Vec::new(),
            free_slots: Vec::new(),
            projection_position: 0,
            generation: 0,
            sketch_specs: Vec::new(),
            cell_sketches: FxHashMap::default(),
            chain_next: Vec::new(),
            chain_prev: Vec::new(),
            cell_head: FxHashMap::default(),
            stale_cells: FxHashSet::default(),
        })
    }

    /// Declare non-retractable measures on a projection.
    ///
    /// Kept separate from `new` because it changes the maintenance contract,
    /// not just the width: a projection with sketches pays a per-cell row
    /// chain and rebuilds a cell's sketches whenever a row leaves it.
    pub fn with_sketches(mut self, sketches: Vec<SketchSpec>) -> Result<Self> {
        if sketches.len() > MAX_SKETCHES {
            return Err(BicDbError::ProjectionError(format!(
                "an aggregate projection supports at most {MAX_SKETCHES} sketch measures"
            )));
        }
        if !self.index.is_empty() {
            return Err(BicDbError::ProjectionError(
                "sketches must be declared before a projection is populated".to_string(),
            ));
        }
        self.layout = ProjectionLayout::new(
            self.dimension_paths.len(),
            self.measure_paths.len(),
            sketches.len(),
        );
        self.sketch_specs = sketches;
        Ok(self)
    }

    pub fn sketch_specs(&self) -> &[SketchSpec] {
        &self.sketch_specs
    }

    fn has_sketches(&self) -> bool {
        !self.sketch_specs.is_empty()
    }

    /// Link an input slot onto the front of its cell's row list.
    fn link_input(&mut self, slot: u32, cell_slot: u32) {
        let slot_index = slot as usize;
        if self.chain_next.len() <= slot_index {
            self.chain_next.resize(slot_index + 1, NIL);
            self.chain_prev.resize(slot_index + 1, NIL);
        }
        let head = self.cell_head.get(&cell_slot).copied().unwrap_or(NIL);
        self.chain_prev[slot_index] = NIL;
        self.chain_next[slot_index] = head;
        if head != NIL {
            self.chain_prev[head as usize] = slot;
        }
        self.cell_head.insert(cell_slot, slot);
    }

    /// Unlink a stored record from whatever cell it currently belongs to.
    ///
    /// Must run BEFORE the retract that may empty (and free) that cell,
    /// otherwise the cell is gone and its chain still points at a live row.
    fn unlink_stored(&mut self, key: u128) {
        if !self.has_sketches() {
            return;
        }
        let Some(slot) = self.index.get(&key).copied() else {
            return;
        };
        let state = self.read_slot(slot);
        let Some(cell_slot) = self.cells.get(&state.key).copied() else {
            return;
        };
        let slot_index = slot as usize;
        if slot_index >= self.chain_next.len() {
            return;
        }
        let previous = self.chain_prev[slot_index];
        let next = self.chain_next[slot_index];
        if previous != NIL {
            self.chain_next[previous as usize] = next;
        } else if next != NIL {
            self.cell_head.insert(cell_slot, next);
        } else {
            self.cell_head.remove(&cell_slot);
        }
        if next != NIL {
            self.chain_prev[next as usize] = previous;
        }
        self.chain_prev[slot_index] = NIL;
        self.chain_next[slot_index] = NIL;
    }

    /// Recompute every cell a retract invalidated, by walking that cell's own
    /// rows. Idempotent, and a no-op when nothing went stale.
    pub fn resolve_sketches(&mut self) {
        if self.stale_cells.is_empty() {
            return;
        }
        for cell_slot in std::mem::take(&mut self.stale_cells) {
            if !self.cell_head.contains_key(&cell_slot) {
                // The cell emptied out entirely; its state went with it.
                self.cell_sketches.remove(&cell_slot);
                continue;
            }
            let mut fresh: Vec<SketchState> = self
                .sketch_specs
                .iter()
                .map(|spec| SketchState::empty(spec.kind))
                .collect();
            let mut cursor = self.cell_head.get(&cell_slot).copied().unwrap_or(NIL);
            while cursor != NIL {
                let state = self.read_slot(cursor);
                let identity = self
                    .layout
                    .read_digest(&self.slots[self.slot_range(cursor)])
                    as u64;
                for (index, sketch) in fresh.iter_mut().enumerate() {
                    if let Some(word) = state.sketch(index) {
                        sketch.observe(word, identity);
                    }
                }
                cursor = self.chain_next[cursor as usize];
            }
            self.cell_sketches.insert(cell_slot, fresh);
        }
    }

    /// Sketch state for a cell. Resolves pending staleness first, which is
    /// why this takes `&mut self` — reading an approximate measure is allowed
    /// to do work, but it is never allowed to return a stale answer.
    pub fn cell_sketch(&mut self, key: &CellKey, index: usize) -> Option<SketchState> {
        self.resolve_sketches();
        let compact = self.lookup_key(key)?;
        let cell_slot = self.cells.get(&compact).copied()?;
        self.cell_sketches.get(&cell_slot)?.get(index).cloned()
    }

    /// The physical layout this projection uses.
    pub fn layout(&self) -> ProjectionLayout {
        self.layout
    }

    fn slot_range(&self, slot: u32) -> std::ops::Range<usize> {
        let width = self.layout.state_width();
        let start = slot as usize * width;
        start..start + width
    }

    fn read_slot(&self, slot: u32) -> InputState {
        self.layout.read(&self.slots[self.slot_range(slot)])
    }

    fn write_slot(&mut self, slot: u32, digest: u128, state: &InputState) {
        let range = self.slot_range(slot);
        Self::mark_dirty(&mut self.dirty_input_pages, &range);
        let layout = self.layout;
        layout.write(&mut self.slots[range], digest, state);
    }

    /// Mark every page a byte range touches. A row is not page-aligned, so
    /// it can straddle a boundary; marking only the starting page would leave
    /// the tail of a straddling row unwritten and silently corrupt on reload.
    fn mark_dirty(pages: &mut BTreeSet<u32>, range: &std::ops::Range<usize>) {
        if range.is_empty() {
            return;
        }
        let first = (range.start / PAGE_BYTES) as u32;
        let last = ((range.end - 1) / PAGE_BYTES) as u32;
        for page in first..=last {
            pages.insert(page);
        }
    }

    fn allocate_slot(&mut self) -> u32 {
        if let Some(slot) = self.free_slots.pop() {
            return slot;
        }
        let slot = (self.slots.len() / self.layout.state_width()) as u32;
        self.slots
            .resize(self.slots.len() + self.layout.state_width(), 0);
        slot
    }

    /// Look up a record's stored state, if any.
    fn stored_input(&self, key: u128) -> Option<InputState> {
        self.index.get(&key).map(|slot| self.read_slot(*slot))
    }

    fn store_input(&mut self, key: u128, state: &InputState) {
        let slot = match self.index.get(&key) {
            Some(slot) => *slot,
            None => {
                let slot = self.allocate_slot();
                self.index.insert(key, slot);
                slot
            }
        };
        self.write_slot(slot, key, state);
        if self.has_sketches() {
            if let Some(cell_slot) = self.cells.get(&state.key).copied() {
                self.link_input(slot, cell_slot);
            }
        }
    }

    fn remove_input(&mut self, key: u128) -> Option<InputState> {
        // Unlink while the cell still exists — the retract that follows may
        // free it.
        self.unlink_stored(key);
        let slot = self.index.remove(&key)?;
        let state = self.read_slot(slot);
        // Tombstone the row so a page scan can rebuild the free list.
        let range = self.slot_range(slot);
        let layout = self.layout;
        layout.mark_free(&mut self.slots[range.clone()]);
        Self::mark_dirty(&mut self.dirty_input_pages, &range);
        self.free_slots.push(slot);
        Some(state)
    }

    /// Publish generation of the loaded snapshot; increments on every save.
    pub fn generation(&self) -> u64 {
        self.generation
    }

    /// Full durability oracle: everything that must hold after a recovery.
    /// Reusable by design — G6 will replace the persistence mechanism
    /// underneath it without changing what this checks.
    pub fn verify_durable_invariants(&self, db: &BicDb) -> Result<ProjectionDrift> {
        let drift = self.reconcile(db)?;
        if !drift.is_clean() {
            return Ok(drift);
        }
        // The watermark may lag the source but must never lead it.
        let source = Self::source_position(db);
        if self.projection_position > source {
            return Err(BicDbError::ProjectionError(format!(
                "watermark {} is ahead of source position {source}",
                self.projection_position
            )));
        }
        for (_, cell) in self.cells() {
            if cell.count < 0 {
                return Err(BicDbError::ProjectionError(
                    "a durable cell has a negative count".to_string(),
                ));
            }
        }
        // Every live input row must appear on exactly one cell chain, and on
        // the chain of the cell it actually belongs to. A chain bug would
        // otherwise show up only as a quietly wrong distinct-count.
        if self.has_sketches() {
            let mut chained = 0usize;
            for (compact, cell_slot) in &self.cells {
                let mut cursor = self.cell_head.get(cell_slot).copied().unwrap_or(NIL);
                let mut guard = 0usize;
                while cursor != NIL {
                    let state = self.read_slot(cursor);
                    if state.key != *compact {
                        return Err(BicDbError::ProjectionError(
                            "an input row is chained to a cell it does not belong to".to_string(),
                        ));
                    }
                    chained += 1;
                    guard += 1;
                    if guard > self.index.len() + 1 {
                        return Err(BicDbError::ProjectionError(
                            "a cell row chain contains a cycle".to_string(),
                        ));
                    }
                    cursor = self.chain_next[cursor as usize];
                }
            }
            if chained != self.index.len() {
                return Err(BicDbError::ProjectionError(format!(
                    "{chained} rows are chained to cells but {} input rows are live",
                    self.index.len()
                )));
            }
        }
        // No slab row may reference a dictionary id that was never published:
        // the exact corruption a non-atomic dictionary write would cause.
        for slot in self.index.values() {
            let state = self.read_slot(*slot);
            for (dimension, dictionary) in self.dictionaries.iter().enumerate() {
                let id = state.key.0[dimension] as usize;
                if id >= dictionary.values.len() {
                    return Err(BicDbError::ProjectionError(format!(
                        "input state references dictionary id {id} in dimension {dimension}, but only {} are published",
                        dictionary.values.len()
                    )));
                }
            }
        }
        Ok(drift)
    }

    pub fn projection_position(&self) -> u64 {
        self.projection_position
    }

    /// Events behind the source: the lag an operator should see rather than
    /// a projection that silently pretends to be current (docs §5).
    pub fn lag_events(&self, source_position: u64) -> u64 {
        source_position.saturating_sub(self.projection_position)
    }

    /// Returned by value: `CellState` is `Copy` and the durable form is a
    /// packed slab row, not a struct in a map.
    pub fn cell(&self, key: &CellKey) -> Option<CellState> {
        let compact = self.lookup_key(key)?;
        self.cells.get(&compact).map(|slot| self.read_cell(*slot))
    }

    /// Cell keys are materialized on iteration; the stored form is interned.
    pub fn cells(&self) -> impl Iterator<Item = (CellKey, CellState)> + '_ {
        self.cells
            .iter()
            .map(|(compact, slot)| (self.resolve_key(compact), self.read_cell(*slot)))
    }

    fn cell_slot_range(&self, slot: u32) -> std::ops::Range<usize> {
        let width = self.layout.cell_width();
        let start = slot as usize * width;
        start..start + width
    }

    fn read_cell(&self, slot: u32) -> CellState {
        self.layout
            .read_cell(&self.cell_slots[self.cell_slot_range(slot)])
    }

    fn write_cell(&mut self, slot: u32, state: &CellState) {
        let range = self.cell_slot_range(slot);
        let layout = self.layout;
        layout.write_cell(&mut self.cell_slots[range.clone()], state);
        Self::mark_dirty(&mut self.dirty_cell_pages, &range);
    }

    fn write_cell_key(&mut self, slot: u32, key: &CompactKey) {
        let range = self.cell_slot_range(slot);
        let layout = self.layout;
        layout.write_cell_key(&mut self.cell_slots[range.clone()], key);
        Self::mark_dirty(&mut self.dirty_cell_pages, &range);
    }

    fn free_cell(&mut self, slot: u32) {
        let range = self.cell_slot_range(slot);
        let layout = self.layout;
        layout.mark_cell_free(&mut self.cell_slots[range.clone()]);
        Self::mark_dirty(&mut self.dirty_cell_pages, &range);
    }

    pub fn cell_count(&self) -> usize {
        self.cells.len()
    }

    /// Approximate resident cost of the compact representation.
    ///
    /// Counts what each structure actually holds: fixed-width cell and input
    /// records, plus the projection-local dictionaries (which are the only
    /// place dimension strings are stored now — once each, rather than once
    /// per row).
    pub fn residency(&self) -> ProjectionResidency {
        // BTreeMap node/pointer share per entry; HashMap slack at ~0.875 load.
        const BTREE_ENTRY_OVERHEAD: u64 = 24;
        const HASH_SLACK_NUMERATOR: u64 = 8;
        const HASH_SLACK_DENOMINATOR: u64 = 7;

        // Key + slot id in a hash map, plus the packed slab row — no longer a
        // 72-byte fixed struct inside a BTree node.
        let cell_entry = (std::mem::size_of::<CompactKey>() as u64
            + std::mem::size_of::<u32>() as u64)
            * HASH_SLACK_NUMERATOR
            / HASH_SLACK_DENOMINATOR;
        // Index entry: digest key + slot id, plus hash-table slack. The state
        // itself lives in the slab and is counted at its exact width.
        let index_entry = (std::mem::size_of::<u128>() as u64 + std::mem::size_of::<u32>() as u64)
            * HASH_SLACK_NUMERATOR
            / HASH_SLACK_DENOMINATOR;
        let dictionary_bytes: u64 = self
            .dictionaries
            .iter()
            .map(Dictionary::resident_bytes)
            .sum();

        let records = self.index.len();
        ProjectionResidency {
            cells: self.cells.len(),
            cell_bytes: self.cells.len() as u64 * cell_entry
                + self.cell_slots.capacity() as u64
                + self.cell_free.capacity() as u64 * 4
                + dictionary_bytes,
            input_records: records,
            // Slab capacity, not live rows: vacated slots are retained for
            // reuse and an honest residency number must include them.
            slab_bytes: self.slots.capacity() as u64,
            input_bytes: self.slots.capacity() as u64
                + records as u64 * index_entry
                + self.free_slots.capacity() as u64 * 4,
            input_logical_bytes: records as u64 * self.layout.logical_bytes_per_row() as u64,
        }
    }

    /// Rebuild every cell and input state from the base collection, then set
    /// the watermark to the current head of the audit stream.
    ///
    /// This is how a projection is created over data that already exists (a
    /// bulk load emits no audit events), and it is the first half of the
    /// rebuild-and-swap lifecycle in docs §10: rebuild from base, then catch
    /// up with the stream.
    pub fn rebuild_from_base(&mut self, db: &BicDb) -> Result<usize> {
        self.cells.clear();
        self.cell_slots.clear();
        self.cell_free.clear();
        self.index.clear();
        self.slots.clear();
        self.free_slots.clear();
        self.cell_sketches.clear();
        self.cell_head.clear();
        self.chain_next.clear();
        self.chain_prev.clear();
        self.stale_cells.clear();
        // Generation rebuild is the one safe place to compact dictionaries
        // (docs §10): nothing persisted refers to the old ids any more.
        self.dictionaries = (0..self.dimension_paths.len())
            .map(|_| Dictionary::new())
            .collect();
        // Read the head BEFORE scanning: anything committed during the scan is
        // then re-applied by catch_up rather than missed. Re-applying is safe
        // (effectively-once); missing an event is not.
        let head = Self::source_position(db);
        let mut rows = 0usize;
        for record in db.scan_collection(&self.collection)? {
            let input = self.project(&record.metadata, head);
            let identity = record_key(&record.id);
            self.apply_input(&input, identity, true);
            self.store_input(identity, &input);
            rows += 1;
        }
        self.projection_position = head;
        self.resolve_sketches();
        Ok(rows)
    }

    /// Intern this record's dimension values (minting ids as needed) and pack
    /// its measures. Write path only.
    fn project(&mut self, metadata: &serde_json::Value, version: u64) -> InputState {
        let mut key = CompactKey::default();
        for (index, path) in self.dimension_paths.iter().enumerate() {
            let value = DimensionValue::from_json(metadata.get(path));
            key.0[index] = self.dictionaries[index].intern(&value);
        }
        let mut measures = [0.0f64; MAX_MEASURES];
        let mut present = 0u8;
        for (index, path) in self.measure_paths.iter().enumerate().take(MAX_MEASURES) {
            if let Some(value) = metadata.get(path).and_then(serde_json::Value::as_f64) {
                measures[index] = value;
                present |= 1 << index;
            }
        }
        let mut sketches = [0u64; MAX_SKETCHES];
        for (index, spec) in self.sketch_specs.iter().enumerate().take(MAX_SKETCHES) {
            let raw = metadata.get(&spec.path);
            match spec.kind {
                SketchKind::DistinctCount => {
                    // Reduce to a stable hash at write time: a rebuild then
                    // never needs the original value back.
                    let value = DimensionValue::from_json(raw);
                    if value != DimensionValue::Missing {
                        sketches[index] = hash_dimension(&value);
                        present |= 1 << (MAX_MEASURES + index);
                    }
                }
                SketchKind::Quantile => {
                    if let Some(number) = raw.and_then(serde_json::Value::as_f64) {
                        sketches[index] = number.to_bits();
                        present |= 1 << (MAX_MEASURES + index);
                    }
                }
            }
        }
        InputState {
            key,
            measures,
            sketches,
            present,
            version,
        }
    }

    /// Read-path twin of `project` that never mints ids. Returns `None` when a
    /// value has never been written, which simply means no such cell exists.
    fn lookup_key(&self, key: &CellKey) -> Option<CompactKey> {
        let mut compact = CompactKey::default();
        for index in 0..self.dimension_paths.len() {
            let value = key.get(index).cloned().unwrap_or(DimensionValue::Missing);
            compact.0[index] = self.dictionaries[index].get(&value)?;
        }
        Some(compact)
    }

    fn resolve_key(&self, compact: &CompactKey) -> CellKey {
        (0..self.dimension_paths.len())
            .map(|index| self.dictionaries[index].resolve(compact.0[index]))
            .collect()
    }

    fn retract_input(&mut self, input: &InputState, touch_sketch: bool) {
        let Some(slot) = self.cells.get(&input.key).copied() else {
            return;
        };
        if touch_sketch && self.has_sketches() {
            // Cannot subtract from a sketch; the cell must be recomputed.
            self.stale_cells.insert(slot);
        }
        let mut cell = self.read_cell(slot);
        cell.retract(input);
        if cell.is_empty() {
            // Do not keep zeroed cells: an empty cell and an absent cell must
            // read the same, or reconciliation reports phantom drift.
            self.cells.remove(&input.key);
            self.free_cell(slot);
            self.cell_free.push(slot);
            self.cell_sketches.remove(&slot);
            self.stale_cells.remove(&slot);
            self.cell_head.remove(&slot);
        } else {
            self.write_cell(slot, &cell);
        }
    }

    /// `identity` is the record digest, NOT the event position. The quantile
    /// sketch keys its sample priorities on it, so apply and rebuild must
    /// agree on it exactly or an incrementally-maintained sketch and a
    /// recomputed one will hold different samples of the same data.
    fn apply_input(&mut self, input: &InputState, identity: u128, touch_sketch: bool) {
        let slot = match self.cells.get(&input.key) {
            Some(slot) => *slot,
            None => {
                let slot = match self.cell_free.pop() {
                    Some(slot) => slot,
                    None => {
                        let width = self.layout.cell_width();
                        let slot = (self.cell_slots.len() / width) as u32;
                        self.cell_slots.resize(self.cell_slots.len() + width, 0);
                        slot
                    }
                };
                self.cells.insert(input.key, slot);
                self.write_cell_key(slot, &input.key);
                self.write_cell(slot, &CellState::default());
                slot
            }
        };
        let mut cell = self.read_cell(slot);
        cell.apply(input);
        self.write_cell(slot, &cell);
        if touch_sketch && self.has_sketches() && !self.stale_cells.contains(&slot) {
            // Adding to a sketch IS incremental; only removal is not.
            let identity = identity as u64;
            let specs = &self.sketch_specs;
            let entry = self.cell_sketches.entry(slot).or_insert_with(|| {
                specs
                    .iter()
                    .map(|spec| SketchState::empty(spec.kind))
                    .collect()
            });
            for (index, sketch) in entry.iter_mut().enumerate() {
                if let Some(word) = input.sketch(index) {
                    sketch.observe(word, identity);
                }
            }
        }
    }

    /// Apply one audit event. Returns whether it changed anything.
    ///
    /// Effectively-once (docs §4): an event at or below the watermark is
    /// ignored, and an event older than the record's recorded version is
    /// ignored, so replay, duplicate delivery and out-of-order arrival are all
    /// defined rather than merely unlikely.
    pub fn apply_event(&mut self, stored: &StoredEvent) -> Result<bool> {
        if stored.event.stream != RECORD_AUDIT_STREAM {
            return Ok(false);
        }
        let position = stored.offset;
        if position <= self.projection_position && self.projection_position != 0 {
            return Ok(false);
        }
        let payload = &stored.event.payload;
        if payload.get("collection").and_then(|value| value.as_str()) != Some(&self.collection) {
            self.projection_position = self.projection_position.max(position);
            return Ok(false);
        }
        let Some(record_id) = payload.get("record_id").and_then(|value| value.as_str()) else {
            self.projection_position = self.projection_position.max(position);
            return Ok(false);
        };

        let key = record_key(record_id);
        // Late/out-of-order: a lower-versioned event must not overwrite a
        // newer one already folded in.
        if let Some(existing) = self.stored_input(key).as_ref() {
            if position < existing.version {
                self.projection_position = self.projection_position.max(position);
                return Ok(false);
            }
        }

        let deleted = stored.event.event_type == "RecordDeleted";
        let changed = if deleted {
            match self.remove_input(key) {
                Some(previous) => {
                    self.retract_input(&previous, true);
                    true
                }
                None => false,
            }
        } else {
            let metadata = payload
                .get("record")
                .and_then(|record| record.get("metadata"))
                .cloned()
                .unwrap_or(serde_json::Value::Null);
            let next = self.project(&metadata, position);
            // Retract the OLD contribution before applying the new one: this
            // is what makes a dimension change move a row between cells
            // instead of double-counting it.
            // The retract uses the STORED state, never a re-resolution of the
            // new row: the id recorded at apply time is authoritative, so a
            // dimension change retracts the cell the row actually joined.
            let previous = self.stored_input(key);
            // A sketch only goes stale if the row actually LEAVES its cell or
            // its sketch inputs change. An update that touches neither — the
            // common case in a recrawl, where prices move but hosts do not —
            // leaves the underlying set identical, so the sketch is still
            // exactly right and no rebuild is owed.
            let sketch_unchanged = previous.as_ref().is_some_and(|previous| {
                previous.key == next.key
                    && previous.sketches == next.sketches
                    && (previous.present >> MAX_MEASURES) == (next.present >> MAX_MEASURES)
            });
            if let Some(previous) = previous {
                self.unlink_stored(key);
                self.retract_input(&previous, !sketch_unchanged);
            }
            self.apply_input(&next, key, !sketch_unchanged);
            self.store_input(key, &next);
            true
        };

        // Advance last, together with the mutations above, so a crash between
        // them cannot double-apply on restart.
        self.projection_position = self.projection_position.max(position);
        Ok(changed)
    }

    /// Fold every audit event at or beyond the current watermark.
    ///
    /// Reads the record-audit stream directly rather than
    /// `export_events_since`, which is the MESH export path and allow-lists
    /// collections via `mesh_sync_enabled` — a projection must see its own
    /// collection's history regardless of whether that collection is
    /// replicated.
    pub fn catch_up(&mut self, db: &BicDb) -> Result<usize> {
        // Read only from the watermark forward. `read(stream)` clones EVERY
        // event in the log, so following a stream with it costs O(total
        // events) per call — invisible at test scale, and the dominant cost
        // at a million events, where each clone also carries a full record
        // copy. The scale harness found this; the 400-row tests could not.
        let events = db
            .events()
            .read_stream_since(RECORD_AUDIT_STREAM, self.projection_position);
        let mut applied = 0usize;
        for stored in events {
            if self.apply_event(&stored)? {
                applied += 1;
            }
        }
        self.resolve_sketches();
        Ok(applied)
    }

    /// Highest audit-stream position currently durable — the `source_position`
    /// half of the lag reading (docs §5).
    pub fn source_position(db: &BicDb) -> u64 {
        db.events().stream_head_offset(RECORD_AUDIT_STREAM)
    }

    /// Recompute authoritatively from the base collection and diff against the
    /// incrementally maintained cells (docs §9). This is the acceptance test
    /// for the whole subsystem, and it is meant to be run on a schedule, not
    /// only when something looks wrong.
    pub fn reconcile(&self, db: &BicDb) -> Result<ProjectionDrift> {
        // Recompute in RESOLVED value space, not interned space: comparing
        // dictionary ids to themselves would pass even if the dictionary were
        // wrong. This checks the values an operator would actually see.
        let mut authoritative: BTreeMap<CellKey, CellState> = BTreeMap::new();
        for record in db.scan_collection(&self.collection)? {
            let key: CellKey = self
                .dimension_paths
                .iter()
                .map(|path| DimensionValue::from_json(record.metadata.get(path)))
                .collect();
            let mut measures = [0.0f64; MAX_MEASURES];
            let mut present = 0u8;
            for (index, path) in self.measure_paths.iter().enumerate().take(MAX_MEASURES) {
                if let Some(value) = record
                    .metadata
                    .get(path)
                    .and_then(serde_json::Value::as_f64)
                {
                    measures[index] = value;
                    present |= 1 << index;
                }
            }
            let input = InputState {
                key: CompactKey::default(),
                measures,
                sketches: [0; MAX_SKETCHES],
                present,
                version: 0,
            };
            authoritative.entry(key).or_default().apply(&input);
        }

        let actual: BTreeMap<CellKey, CellState> = self
            .cells
            .iter()
            .map(|(compact, slot)| (self.resolve_key(compact), self.read_cell(*slot)))
            .collect();

        let mut drift = ProjectionDrift::default();
        for (key, expected) in &authoritative {
            match actual.get(key) {
                Some(actual) => {
                    drift.cells_compared += 1;
                    if actual != expected {
                        drift.cells_differing += 1;
                        drift.max_count_delta = drift
                            .max_count_delta
                            .max((actual.count - expected.count).abs());
                    }
                }
                None => drift.cells_only_authoritative += 1,
            }
        }
        for key in actual.keys() {
            if !authoritative.contains_key(key) {
                drift.cells_only_incremental += 1;
            }
        }
        Ok(drift)
    }
}

// ---------------------------------------------------------------------------
// G5: durable projections
// ---------------------------------------------------------------------------

/// Input-state byte layout version. **Frozen at G4.6a.** Changing it is a
/// generation upgrade (rebuild as v2 → catch up → atomic swap), never a
/// struct edit, because persisted slab bytes are interpreted by it.
pub const INPUT_STATE_FORMAT: u32 = 3;

/// Cell-state layout version, versioned **separately and deliberately**: cell
/// state is derived and can be rebuilt from the base table, so it may change
/// without a migration. (Input state cannot — it is the retract source of
/// truth.)
pub const CELL_STATE_FORMAT: u32 = 1;

const SNAPSHOT_MAGIC: &str = "bicdb.aggregate-projection";
/// Envelope version — the framing around the payload, independent of the
/// input-state and cell-state layout versions inside it.
const SNAPSHOT_ENVELOPE_VERSION: u32 = 1;

/// The published checkpoint: a tiny document naming the pages that
/// constitute the current state.
///
/// **This is the atomicity boundary.** Pages are immutable and
/// generation-stamped, so they can be written freely before publication; the
/// manifest rename is what makes a checkpoint real. A crash before the rename
/// leaves orphan pages that nothing references — inert, and reclaimable.
#[derive(Clone, Serialize, Deserialize)]
struct ProjectionManifest {
    magic: String,
    envelope_version: u32,
    input_state_format: u32,
    cell_state_format: u32,
    generation: u64,
    name: String,
    collection: String,
    dimension_paths: Vec<String>,
    measure_paths: Vec<String>,
    /// Part of the DEFINITION, so it must survive a restart. The sketch
    /// STATE is derived and deliberately absent.
    #[serde(default)]
    sketch_specs: Vec<SketchSpec>,
    /// Per dimension: the generation whose file holds that dictionary.
    dictionary_generations: Vec<u64>,
    dictionary_lens: Vec<usize>,
    /// page index -> generation of the file holding it.
    input_pages: Vec<(u32, u64)>,
    cell_pages: Vec<(u32, u64)>,
    input_slab_len: usize,
    cell_slab_len: usize,
    projection_position: u64,
    checksum: String,
}

/// Digest over every field except the checksum itself. JSON catches
/// structural damage but not a flipped digit inside a number, which would
/// yield a plausible, wrong manifest.
fn manifest_digest(manifest: &ProjectionManifest) -> Result<String> {
    let mut bare = manifest.clone();
    bare.checksum = String::new();
    Ok(hex::encode(Sha256::digest(serde_json::to_vec(&bare)?)))
}

fn page_slice(slab: &[u8], page: u32) -> &[u8] {
    let start = page as usize * PAGE_BYTES;
    let end = (start + PAGE_BYTES).min(slab.len());
    if start >= slab.len() {
        return &[];
    }
    &slab[start..end]
}

/// Page files carry their own checksum so a torn or truncated page is
/// refused rather than interpreted as zeros.
fn write_page(path: &Path, bytes: &[u8], fsync: bool) -> Result<()> {
    let mut framed = Vec::with_capacity(bytes.len() + 32);
    framed.extend_from_slice(&Sha256::digest(bytes));
    framed.extend_from_slice(bytes);
    crate::storage::write_atomic(path, &framed, fsync)
}

fn read_page(path: &Path) -> Result<Vec<u8>> {
    let framed = std::fs::read(path)?;
    if framed.len() < 32 {
        return Err(BicDbError::ProjectionError(format!(
            "projection page {} is truncated",
            path.display()
        )));
    }
    let (digest, bytes) = framed.split_at(32);
    if Sha256::digest(bytes).as_slice() != digest {
        return Err(BicDbError::ProjectionError(format!(
            "projection page {} failed its checksum",
            path.display()
        )));
    }
    Ok(bytes.to_vec())
}

/// Deterministic abort points for crash testing./// Deterministic abort points for crash testing. Each names a real durability
/// boundary in [`AggregateProjection::save`]; a test aborts there, reopens,
/// catches up and reconciles.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SaveFailpoint {
    /// Before anything is written — the previous snapshot must survive intact.
    BeforeWrite,
    /// After the new bytes are serialized but before they are published.
    BeforePublish,
    /// After publication; the new snapshot must be the one that survives.
    AfterPublish,
}

/// Everything that must be mutually consistent for the persisted input-state
/// bytes to mean anything.
///
/// **Dictionaries are durable state, not derived.** A slab byte reading
/// `host_id = 174` is meaningless without the exact dictionary that minted
/// 174; regenerating dictionaries independently would silently reinterpret
/// every persisted row. They therefore travel inside the same atomic unit as
/// the state that references them, which also satisfies the rule that a
/// dictionary mapping must be durable before anything references its id.
///
/// Cells are included for restart speed only — they are derived, and
/// `reconcile` can rebuild them.
impl AggregateProjection {
    /// Publish a checkpoint: write only the pages mutated since the last one,
    /// then publish a tiny manifest atomically.
    ///
    /// The contract is unchanged from the whole-state version — a crash
    /// leaves either the previous complete checkpoint or the new one — but
    /// the cost is now proportional to MUTATIONS rather than to total state.
    /// Atomicity moves from "rewrite everything and rename" to "write pages,
    /// fsync, then rename one small manifest": pages are immutable once
    /// written (each is named by the generation that produced it), so a
    /// half-written page can never be referenced by a published manifest.
    /// Publish a checkpoint.
    ///
    /// Takes `&mut self` because a checkpoint is not a read: it advances the
    /// generation, clears the dirty set and records what is now durable. When
    /// this took `&self` none of that could happen, so a second checkpoint
    /// from a live projection reused generation 1, **rewrote the very page
    /// files the published manifest still pointed at**, and re-wrote every
    /// page dirtied since the projection was built rather than since the last
    /// checkpoint.
    pub fn save(&mut self, directory: impl AsRef<Path>, fsync: bool) -> Result<()> {
        self.save_with_failpoint(directory, fsync, None)
    }

    pub fn save_with_failpoint(
        &mut self,
        directory: impl AsRef<Path>,
        fsync: bool,
        failpoint: Option<SaveFailpoint>,
    ) -> Result<()> {
        if failpoint == Some(SaveFailpoint::BeforeWrite) {
            return Err(BicDbError::ProjectionError(
                "failpoint: before write".into(),
            ));
        }
        validate_projection_name(&self.name)?;
        let root = directory.as_ref().join(format!("{}.projection", self.name));
        let pages_dir = root.join("pages");
        std::fs::create_dir_all(&pages_dir)?;
        let generation = self.generation.saturating_add(1);

        // Page files are immutable and generation-stamped, so writing them
        // before the manifest can never corrupt the current checkpoint.
        let mut input_pages = self.persisted_input_pages.clone();
        for page in &self.dirty_input_pages {
            let bytes = page_slice(&self.slots, *page);
            let name = format!("input-{page}-{generation}");
            write_page(&pages_dir.join(&name), bytes, fsync)?;
            input_pages.insert(*page, generation);
        }
        let mut cell_pages = self.persisted_cell_pages.clone();
        for page in &self.dirty_cell_pages {
            let bytes = page_slice(&self.cell_slots, *page);
            let name = format!("cell-{page}-{generation}");
            write_page(&pages_dir.join(&name), bytes, fsync)?;
            cell_pages.insert(*page, generation);
        }
        // Dictionaries are append-only; rewrite one only when it grew.
        let mut dictionary_generations = self.persisted_dictionaries.clone();
        for (index, dictionary) in self.dictionaries.iter().enumerate() {
            let persisted = self
                .persisted_dictionary_lens
                .get(index)
                .copied()
                .unwrap_or(0);
            if dictionary.values.len() != persisted || dictionary_generations.len() <= index {
                let bytes = serde_json::to_vec(&dictionary.values)?;
                let name = format!("dictionary-{index}-{generation}");
                write_page(&pages_dir.join(&name), &bytes, fsync)?;
                while dictionary_generations.len() <= index {
                    dictionary_generations.push(0);
                }
                dictionary_generations[index] = generation;
            }
        }

        if failpoint == Some(SaveFailpoint::BeforePublish) {
            // Pages exist but no manifest references them: the previous
            // checkpoint is still authoritative, and the orphans are inert.
            return Err(BicDbError::ProjectionError(
                "failpoint: before publish".into(),
            ));
        }

        let manifest = ProjectionManifest {
            magic: SNAPSHOT_MAGIC.to_string(),
            envelope_version: SNAPSHOT_ENVELOPE_VERSION,
            input_state_format: INPUT_STATE_FORMAT,
            cell_state_format: CELL_STATE_FORMAT,
            generation,
            name: self.name.clone(),
            collection: self.collection.clone(),
            dimension_paths: self.dimension_paths.clone(),
            measure_paths: self.measure_paths.clone(),
            sketch_specs: self.sketch_specs.clone(),
            dictionary_generations,
            dictionary_lens: self
                .dictionaries
                .iter()
                .map(|dictionary| dictionary.values.len())
                .collect(),
            input_pages: input_pages.into_iter().collect(),
            cell_pages: cell_pages.into_iter().collect(),
            input_slab_len: self.slots.len(),
            cell_slab_len: self.cell_slots.len(),
            projection_position: self.projection_position,
            checksum: String::new(),
        };
        let mut manifest = manifest;
        manifest.checksum = manifest_digest(&manifest)?;
        let bytes = serde_json::to_vec(&manifest)?;
        crate::storage::write_atomic(&root.join("manifest.json"), &bytes, fsync)?;

        // Published. Everything below reflects a checkpoint that is already
        // durable, so a failure here cannot un-publish it.
        let superseded = self.adopt_published(&manifest);
        prune_superseded_pages(&pages_dir, &superseded);
        // Also reclaim pages no manifest ever referenced: a publish that
        // failed after writing its pages leaves them behind, and until now
        // nothing removed them, so every failed checkpoint leaked a full set.
        prune_orphan_pages(&pages_dir, &manifest);

        if failpoint == Some(SaveFailpoint::AfterPublish) {
            return Err(BicDbError::ProjectionError(
                "failpoint: after publish".into(),
            ));
        }
        Ok(())
    }

    /// Record what the just-published manifest made durable, and return the
    /// page files the previous checkpoint referenced that this one does not.
    fn adopt_published(&mut self, manifest: &ProjectionManifest) -> Vec<String> {
        let mut superseded = Vec::new();
        let retained: FxHashSet<String> = manifest
            .input_pages
            .iter()
            .map(|(page, generation)| format!("input-{page}-{generation}"))
            .chain(
                manifest
                    .cell_pages
                    .iter()
                    .map(|(page, generation)| format!("cell-{page}-{generation}")),
            )
            .chain(
                manifest
                    .dictionary_generations
                    .iter()
                    .enumerate()
                    .map(|(index, generation)| format!("dictionary-{index}-{generation}")),
            )
            .collect();
        let mut consider = |name: String| {
            if !retained.contains(&name) {
                superseded.push(name);
            }
        };
        for (page, generation) in &self.persisted_input_pages {
            consider(format!("input-{page}-{generation}"));
        }
        for (page, generation) in &self.persisted_cell_pages {
            consider(format!("cell-{page}-{generation}"));
        }
        for (index, generation) in self.persisted_dictionaries.iter().enumerate() {
            consider(format!("dictionary-{index}-{generation}"));
        }

        self.generation = manifest.generation;
        self.dirty_input_pages.clear();
        self.dirty_cell_pages.clear();
        self.persisted_input_pages = manifest.input_pages.iter().copied().collect();
        self.persisted_cell_pages = manifest.cell_pages.iter().copied().collect();
        self.persisted_dictionaries = manifest.dictionary_generations.clone();
        self.persisted_dictionary_lens = manifest.dictionary_lens.clone();
        superseded
    }

    /// Load the published checkpoint, or `None` if none exists.
    ///
    /// The digest->slot index, the cell key->slot map and both free lists are
    /// **rebuilt by scanning the pages** rather than being serialized: rows
    /// carry their own identity and a tombstone, so the manifest stays tiny
    /// and a checkpoint costs mutations rather than state.
    pub fn load(directory: impl AsRef<Path>, name: &str) -> Result<Option<Self>> {
        validate_projection_name(name)?;
        let root = directory.as_ref().join(format!("{name}.projection"));
        let manifest_path = root.join("manifest.json");
        let bytes = match std::fs::read(&manifest_path) {
            Ok(bytes) => bytes,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(error.into()),
        };
        // Fail closed on ANY damage rather than interpreting it.
        let manifest: ProjectionManifest = serde_json::from_slice(&bytes).map_err(|error| {
            BicDbError::ProjectionError(format!(
                "projection manifest {} is unreadable: {error}",
                manifest_path.display()
            ))
        })?;
        if manifest.magic != SNAPSHOT_MAGIC {
            return Err(BicDbError::ProjectionError(format!(
                "{} is not an aggregate-projection manifest",
                manifest_path.display()
            )));
        }
        if manifest.checksum != manifest_digest(&manifest)? {
            return Err(BicDbError::ProjectionError(format!(
                "projection manifest {} failed its checksum; refusing possibly-corrupt aggregates",
                manifest_path.display()
            )));
        }
        // v2 rows are byte-identical to v3 rows when no sketches are declared
        // — the sketch block is appended, and an empty block is zero bytes —
        // so a projection built before sketches existed still loads.
        let input_format_ok = manifest.input_state_format == INPUT_STATE_FORMAT
            || (manifest.input_state_format == 2 && manifest.sketch_specs.is_empty());
        if manifest.envelope_version != SNAPSHOT_ENVELOPE_VERSION
            || !input_format_ok
            || manifest.cell_state_format != CELL_STATE_FORMAT
        {
            return Err(BicDbError::ProjectionError(format!(
                "projection `{name}` uses formats (envelope {}, input {}, cell {}) this build does not implement — rebuild the projection",
                manifest.envelope_version, manifest.input_state_format, manifest.cell_state_format
            )));
        }

        let projection = Self::new(
            manifest.name,
            manifest.collection,
            manifest.dimension_paths,
            manifest.measure_paths,
        )?;
        let mut projection = projection.with_sketches(manifest.sketch_specs)?;
        let pages_dir = root.join("pages");

        for (index, generation) in manifest.dictionary_generations.iter().enumerate() {
            let values: Vec<DimensionValue> = serde_json::from_slice(&read_page(
                &pages_dir.join(format!("dictionary-{index}-{generation}")),
            )?)?;
            let mut lookup = FxHashMap::default();
            for (id, value) in values.iter().enumerate() {
                lookup.insert(value.clone(), id as u32);
            }
            if index < projection.dictionaries.len() {
                projection.dictionaries[index] = Dictionary { values, lookup };
            }
        }

        projection.slots = vec![0u8; manifest.input_slab_len];
        for (page, generation) in &manifest.input_pages {
            let bytes = read_page(&pages_dir.join(format!("input-{page}-{generation}")))?;
            let start = *page as usize * PAGE_BYTES;
            let end = (start + bytes.len()).min(projection.slots.len());
            projection.slots[start..end].copy_from_slice(&bytes[..end - start]);
        }
        projection.cell_slots = vec![0u8; manifest.cell_slab_len];
        for (page, generation) in &manifest.cell_pages {
            let bytes = read_page(&pages_dir.join(format!("cell-{page}-{generation}")))?;
            let start = *page as usize * PAGE_BYTES;
            let end = (start + bytes.len()).min(projection.cell_slots.len());
            projection.cell_slots[start..end].copy_from_slice(&bytes[..end - start]);
        }

        // Rebuild the maps and free lists from the pages themselves.
        let width = projection.layout.state_width();
        if width > 0 {
            for slot in 0..(projection.slots.len() / width) as u32 {
                let range = projection.slot_range(slot);
                let row = &projection.slots[range];
                if projection.layout.is_free(row) {
                    projection.free_slots.push(slot);
                } else {
                    projection
                        .index
                        .insert(projection.layout.read_digest(row), slot);
                }
            }
        }
        let cell_width = projection.layout.cell_width();
        if cell_width > 0 {
            for slot in 0..(projection.cell_slots.len() / cell_width) as u32 {
                let range = projection.cell_slot_range(slot);
                let row = &projection.cell_slots[range];
                if projection.layout.is_cell_free(row) {
                    projection.cell_free.push(slot);
                } else {
                    projection
                        .cells
                        .insert(projection.layout.read_cell_key(row), slot);
                }
            }
        }

        // Sketches and row chains are DERIVED: rebuild them in the scan the
        // load already performs, rather than persisting them. That is why
        // sketch measures cost the manifest nothing and add no new way for a
        // checkpoint to be corrupt.
        if projection.has_sketches() {
            let slot_count = projection.slots.len() / width.max(1);
            projection.chain_next = vec![NIL; slot_count];
            projection.chain_prev = vec![NIL; slot_count];
            for slot in 0..slot_count as u32 {
                let range = projection.slot_range(slot);
                if projection.layout.is_free(&projection.slots[range]) {
                    continue;
                }
                let state = projection.read_slot(slot);
                if let Some(cell_slot) = projection.cells.get(&state.key).copied() {
                    projection.link_input(slot, cell_slot);
                }
            }
            projection.stale_cells = projection.cells.values().copied().collect();
            projection.resolve_sketches();
        }

        projection.projection_position = manifest.projection_position;
        projection.generation = manifest.generation;
        projection.persisted_input_pages = manifest.input_pages.into_iter().collect();
        projection.persisted_cell_pages = manifest.cell_pages.into_iter().collect();
        projection.persisted_dictionaries = manifest.dictionary_generations;
        projection.persisted_dictionary_lens = manifest.dictionary_lens;
        Ok(Some(projection))
    }

    /// The ordinary startup path: restore the snapshot if there is one and
    /// follow the stream from its watermark, otherwise build from the base
    /// table. Duplicate events replayed after a restart are inert (docs §4),
    /// so resuming from a slightly stale watermark is always safe.
    pub fn open(
        directory: impl AsRef<Path>,
        name: &str,
        db: &BicDb,
        build: impl FnOnce() -> Result<Self>,
    ) -> Result<Self> {
        let mut projection = match Self::load(directory, name)? {
            Some(projection) => projection,
            None => {
                let mut fresh = build()?;
                fresh.rebuild_from_base(db)?;
                fresh
            }
        };
        projection.catch_up(db)?;
        Ok(projection)
    }
}

/// Default directory for a database's durable projections.
/// How one dimension is coarsened by a rollup.
///
/// A rollup that merely DROPS dimensions is already free: `GROUP BY` over the
/// projection relation does it. What needs engine support is coarsening a
/// dimension's VALUE — h3 r8 to r6, `2026-08-16` to `2026-08`,
/// `food/pizza/napoli` to `food/pizza` — and, far more importantly, doing so
/// for measures that cannot simply be summed.
///
/// `SUM` and `COUNT` roll up by addition, so a naive `GROUP BY` gets them
/// right by accident. `COUNT(DISTINCT)` and percentiles do not: adding two
/// distinct-counts double-counts everything the two cells share. Those need
/// the sketches merged, which is what this path does and a `GROUP BY` cannot.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RollupLevel {
    /// Leave the dimension at its existing grain.
    Keep,
    /// Drop the dimension from the output key entirely.
    Whole,
    /// First `n` characters of the text form.
    Prefix(usize),
    /// First `depth` separator-delimited segments.
    Segment { separator: char, depth: usize },
    /// Floor an integer dimension into fixed-width buckets.
    Bucket(i64),
}

impl RollupLevel {
    /// Public form, for callers assembling their own coarsening functions.
    pub fn coarsen_public(&self, value: &DimensionValue) -> Option<DimensionValue> {
        self.coarsen(value)
    }

    fn coarsen(&self, value: &DimensionValue) -> Option<DimensionValue> {
        match self {
            Self::Whole => None,
            Self::Keep => Some(value.clone()),
            Self::Prefix(n) => Some(match value {
                DimensionValue::Text(text) => DimensionValue::Text(text.chars().take(*n).collect()),
                other => other.clone(),
            }),
            Self::Segment { separator, depth } => Some(match value {
                DimensionValue::Text(text) => {
                    let taken: Vec<&str> = text.split(*separator).take(*depth).collect();
                    DimensionValue::Text(taken.join(&separator.to_string()))
                }
                other => other.clone(),
            }),
            Self::Bucket(width) => Some(match value {
                DimensionValue::Int(number) if *width > 0 => {
                    // Floor toward negative infinity so negative values bucket
                    // consistently with positive ones.
                    DimensionValue::Int(number.div_euclid(*width) * width)
                }
                other => other.clone(),
            }),
        }
    }
}

/// One rolled-up cell: the additive state plus the merged sketches.
#[derive(Clone, Debug)]
pub struct RolledCell {
    pub state: CellState,
    pub sketches: Vec<SketchState>,
}

impl AggregateProjection {
    /// Coarsen this projection to a higher level of a hierarchy.
    ///
    /// `levels` is parallel to the projection's dimensions; a short slice
    /// leaves the remaining dimensions at their existing grain.
    pub fn rollup(&self, levels: &[RollupLevel]) -> BTreeMap<CellKey, RolledCell> {
        self.rollup_with(|index, value| {
            levels
                .get(index)
                .unwrap_or(&RollupLevel::Keep)
                .coarsen(value)
        })
    }

    /// Rollup with a caller-supplied coarsening function, so vocabularies that
    /// need dependencies this crate does not carry — H3 parents live in the
    /// SQL layer — can be plugged in without inverting the layering.
    ///
    /// Returning `None` drops the dimension from the output key.
    pub fn rollup_with(
        &self,
        coarsen: impl Fn(usize, &DimensionValue) -> Option<DimensionValue>,
    ) -> BTreeMap<CellKey, RolledCell> {
        debug_assert!(
            self.stale_cells.is_empty(),
            "rollup read sketches with staleness pending"
        );
        let mut rolled: BTreeMap<CellKey, RolledCell> = BTreeMap::new();
        for (compact, cell_slot) in &self.cells {
            let fine = self.resolve_key(compact);
            let mut coarse = CellKey::with_capacity(fine.len());
            for (index, value) in fine.iter().enumerate() {
                if let Some(value) = coarse_or_keep(&coarsen, index, value) {
                    coarse.push(value);
                }
            }
            let state = self.read_cell(*cell_slot);
            let sketches = self
                .cell_sketches
                .get(cell_slot)
                .cloned()
                .unwrap_or_default();
            match rolled.get_mut(&coarse) {
                Some(existing) => {
                    existing.state.merge(&state);
                    // The reason this is not a GROUP BY: sketches MERGE, they
                    // do not add.
                    for (index, sketch) in existing.sketches.iter_mut().enumerate() {
                        if let Some(other) = sketches.get(index) {
                            sketch.merge(other);
                        }
                    }
                }
                None => {
                    rolled.insert(coarse, RolledCell { state, sketches });
                }
            }
        }
        rolled
    }
}

fn coarse_or_keep(
    coarsen: &impl Fn(usize, &DimensionValue) -> Option<DimensionValue>,
    index: usize,
    value: &DimensionValue,
) -> Option<DimensionValue> {
    coarsen(index, value)
}

/// One shard's contribution to a distributed aggregate.
///
/// **In resolved value space, never in interned ids.** Each shard mints
/// dictionary ids independently, so shard A's id 7 and shard B's id 7 are
/// almost certainly different strings. Shipping compact keys between shards
/// would produce a merge that is silently, plausibly wrong — every cell
/// present, every number a lie. The compactness that matters is the RESIDENT
/// state; a partial is a one-time export, so it pays for its own safety.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PartialAggregate {
    pub collection: String,
    pub dimensions: Vec<String>,
    pub measures: Vec<String>,
    pub sketches: Vec<SketchSpec>,
    /// The shard's watermark. A merge is only as fresh as its LAGGIEST shard,
    /// so the merged position is the minimum, not the maximum.
    pub position: u64,
    pub cells: Vec<(CellKey, CellState, Vec<SketchState>)>,
}

impl PartialAggregate {
    /// Whether two partials describe the same grain over the same data. A
    /// merge across different grains is not a degraded answer, it is a
    /// meaningless one, so it is refused rather than approximated.
    fn compatible_with(&self, other: &Self) -> Option<String> {
        // Collection NAMES are deliberately not compared. Shards live in
        // separate databases under the same name in one topology, and in one
        // database under different names in another; neither is more correct,
        // and the name is a poor proxy for the real precondition anyway (that
        // the partials partition the data, which nothing here can verify).
        // What must match is the GRAIN.
        if self.dimensions != other.dimensions {
            return Some(format!(
                "dimensions differ: [{}] and [{}]",
                self.dimensions.join(", "),
                other.dimensions.join(", ")
            ));
        }
        if self.measures != other.measures {
            return Some(format!(
                "measures differ: [{}] and [{}]",
                self.measures.join(", "),
                other.measures.join(", ")
            ));
        }
        if self.sketches != other.sketches {
            return Some("sketch measures differ".to_string());
        }
        None
    }
}

impl AggregateProjection {
    /// Export this shard's cells for a coordinator to combine.
    pub fn export_partial(&self) -> PartialAggregate {
        debug_assert!(
            self.stale_cells.is_empty(),
            "export_partial read sketches with staleness pending"
        );
        let mut cells = Vec::with_capacity(self.cells.len());
        for (compact, cell_slot) in &self.cells {
            cells.push((
                self.resolve_key(compact),
                self.read_cell(*cell_slot),
                self.cell_sketches
                    .get(cell_slot)
                    .cloned()
                    .unwrap_or_default(),
            ));
        }
        cells.sort_by(|left, right| left.0.cmp(&right.0));
        PartialAggregate {
            collection: self.collection.clone(),
            dimensions: self.dimension_paths.clone(),
            measures: self.measure_paths.clone(),
            sketches: self.sketch_specs.clone(),
            position: self.projection_position,
            cells,
        }
    }

    /// Combine shard partials into one.
    ///
    /// **Precondition: the shards partition the data.** A record counted by
    /// two shards is counted twice here, and nothing in a partial carries the
    /// record identity that would let this be detected — that identity is
    /// exactly what aggregation discarded. Overlapping shards are a
    /// deployment error, not a merge-time condition.
    ///
    /// Additive measures add. Sketches MERGE — `COUNT(DISTINCT)` across shards
    /// that saw the same value must count it once, so adding would be wrong.
    pub fn merge_partials(partials: &[PartialAggregate]) -> Result<PartialAggregate> {
        let Some(first) = partials.first() else {
            return Err(BicDbError::ProjectionError(
                "merging requires at least one partial aggregate".to_string(),
            ));
        };
        for other in &partials[1..] {
            if let Some(reason) = first.compatible_with(other) {
                return Err(BicDbError::ProjectionError(format!(
                    "cannot merge partial aggregates: {reason}"
                )));
            }
        }

        let mut merged: BTreeMap<CellKey, (CellState, Vec<SketchState>)> = BTreeMap::new();
        for partial in partials {
            for (key, state, sketches) in &partial.cells {
                match merged.get_mut(key) {
                    Some((existing_state, existing_sketches)) => {
                        existing_state.merge(state);
                        for (index, sketch) in existing_sketches.iter_mut().enumerate() {
                            if let Some(other) = sketches.get(index) {
                                sketch.merge(other);
                            }
                        }
                    }
                    None => {
                        merged.insert(key.clone(), (*state, sketches.clone()));
                    }
                }
            }
        }

        Ok(PartialAggregate {
            // Keep every contributing source visible rather than silently
            // adopting the first shard's name.
            collection: {
                let mut sources: Vec<&str> = partials
                    .iter()
                    .map(|partial| partial.collection.as_str())
                    .collect();
                sources.sort_unstable();
                sources.dedup();
                sources.join("+")
            },
            dimensions: first.dimensions.clone(),
            measures: first.measures.clone(),
            sketches: first.sketches.clone(),
            // A merged answer is complete only through the point EVERY shard
            // has reached. Taking the max would report a freshness the result
            // does not have.
            position: partials
                .iter()
                .map(|partial| partial.position)
                .min()
                .unwrap_or(0),
            cells: merged
                .into_iter()
                .map(|(key, (state, sketches))| (key, state, sketches))
                .collect(),
        })
    }
}

/// Delete page files the newly-published manifest no longer references.
///
/// Only files the PREVIOUS manifest referenced are considered, so a page
/// written by a failed publish is left alone rather than raced against — and
/// a reader that is midway through the previous checkpoint keeps the files it
/// is reading until this publish supersedes them.
///
/// Removal failures are deliberately not fatal: the checkpoint is already
/// durable, and refusing to return success because cleanup failed would turn
/// a full disk into a lost checkpoint. The next checkpoint retries.
fn prune_superseded_pages(pages_dir: &Path, superseded: &[String]) {
    for name in superseded {
        let _ = std::fs::remove_file(pages_dir.join(name));
    }
}

/// Trailing `-<generation>` of a page filename.
fn page_file_generation(name: &str) -> Option<u64> {
    name.rsplit_once('-')
        .and_then(|(_, generation)| generation.parse::<u64>().ok())
}

/// Delete page files that no manifest references.
///
/// A publish that failed after writing its pages left them on disk forever.
/// They were inert — nothing referenced them — but a projection that fails to
/// publish repeatedly grew without bound.
///
/// **Only pages from a generation STRICTLY OLDER than the published one are
/// considered.** A save in flight writes at `published + 1` and has not
/// published yet, so its pages are unreferenced too; bounding the sweep below
/// the published generation makes it impossible to delete work another writer
/// is still doing. Failures are ignored: the checkpoint is already durable,
/// and a cleanup that cannot run is a leak, not a corruption.
fn prune_orphan_pages(pages_dir: &Path, manifest: &ProjectionManifest) {
    let referenced: std::collections::HashSet<String> = manifest
        .input_pages
        .iter()
        .map(|(page, generation)| format!("input-{page}-{generation}"))
        .chain(
            manifest
                .cell_pages
                .iter()
                .map(|(page, generation)| format!("cell-{page}-{generation}")),
        )
        .chain(
            manifest
                .dictionary_generations
                .iter()
                .enumerate()
                .map(|(index, generation)| format!("dictionary-{index}-{generation}")),
        )
        .collect();

    let Ok(entries) = std::fs::read_dir(pages_dir) else {
        return;
    };
    for entry in entries.flatten() {
        let name = entry.file_name();
        let Some(name) = name.to_str() else {
            continue;
        };
        if referenced.contains(name) {
            continue;
        }
        let Some(generation) = page_file_generation(name) else {
            continue;
        };
        if generation < manifest.generation {
            let _ = std::fs::remove_file(entry.path());
        }
    }
}

pub const PROJECTIONS_DIR: &str = "projections";

/// Longest projection/cube identifier accepted.
const MAX_PROJECTION_NAME_BYTES: usize = 64;

/// Validate a projection identifier before it can become a path component.
///
/// **No filesystem name may be derived from a SQL identifier without
/// passing through here.** A projection's on-disk directory is
/// `<data>/projections/<name>.projection`, so an unvalidated name is a
/// path-traversal primitive: `DROP CUBE "../../<other-db>/projections/x"`
/// resolved outside the data directory and reached another database's
/// files, and `remove_dir_all` on the result destroyed them. Because
/// every tenant's cubes share the `.projection` suffix, the traversal
/// landed on real, valuable directories rather than on nothing.
///
/// The accepted alphabet cannot express traversal at all: no separators,
/// no dots, no NUL, no control characters, nothing non-ASCII. That is what
/// makes using the validated identifier as the path component safe — the
/// check is the encoding step, and it is enforced here in the core rather
/// than only at the SQL layer so that every present and future caller is
/// covered.
pub fn validate_projection_name(name: &str) -> Result<()> {
    if name.is_empty() {
        return Err(BicDbError::ProjectionError(
            "projection name must not be empty".to_string(),
        ));
    }
    if name.len() > MAX_PROJECTION_NAME_BYTES {
        return Err(BicDbError::ProjectionError(format!(
            "projection name `{name}` exceeds {MAX_PROJECTION_NAME_BYTES} bytes"
        )));
    }
    if !name
        .bytes()
        .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_' || byte == b'-')
    {
        return Err(BicDbError::ProjectionError(format!(
            "invalid projection name `{name}`: use ASCII letters, digits, '-' or '_'"
        )));
    }
    Ok(())
}

/// Names of every projection with a snapshot under `<db>/projections/`.
pub fn list_projections(db_path: &Path) -> Vec<String> {
    let mut names = Vec::new();
    let Ok(entries) = std::fs::read_dir(db_path.join(PROJECTIONS_DIR)) else {
        return names;
    };
    for entry in entries.flatten() {
        // A projection is a DIRECTORY since G6 (`<name>.projection/` holding a
        // manifest and its pages). This matched `.projection.json` — the
        // pre-G6 single-file name — for one release, so `bicdb_projections`
        // silently listed nothing while every projection still worked.
        if !entry.file_type().map(|kind| kind.is_dir()).unwrap_or(false) {
            continue;
        }
        if let Some(name) = entry
            .file_name()
            .to_str()
            .and_then(|file| file.strip_suffix(".projection"))
        {
            names.push(name.to_string());
        }
    }
    names.sort();
    names
}

impl AggregateProjection {
    /// Column names this projection exposes as a relation: one per dimension,
    /// then `count`, then `sum_<measure>` / `avg_<measure>` per measure.
    pub fn relation_columns(&self) -> Vec<String> {
        let mut columns = self.dimension_paths.clone();
        columns.push("count".to_string());
        for measure in &self.measure_paths {
            columns.push(format!("sum_{measure}"));
            columns.push(format!("avg_{measure}"));
        }
        for spec in &self.sketch_specs {
            match spec.kind {
                SketchKind::DistinctCount => columns.push(format!("distinct_{}", spec.name)),
                SketchKind::Quantile => {
                    columns.push(format!("p50_{}", spec.name));
                    columns.push(format!("p95_{}", spec.name));
                }
            }
        }
        columns
    }

    /// Every cell as a row of `(column, value)` pairs — the relational view.
    pub fn relation_rows(&self) -> Vec<Vec<(String, Option<f64>, Option<String>)>> {
        let columns = self.relation_columns();
        debug_assert!(
            self.stale_cells.is_empty(),
            "relation_rows read sketches with staleness pending; \
             catch_up/rebuild/load all resolve before returning"
        );
        let mut rows = Vec::with_capacity(self.cells.len());
        for (compact, cell_slot) in &self.cells {
            let key = self.resolve_key(compact);
            let cell = self.read_cell(*cell_slot);
            let mut row = Vec::with_capacity(columns.len());
            for (index, path) in self.dimension_paths.iter().enumerate() {
                let text = match key.get(index) {
                    Some(DimensionValue::Text(text)) => Some(text.clone()),
                    Some(DimensionValue::Int(value)) => Some(value.to_string()),
                    Some(DimensionValue::Bool(value)) => Some(value.to_string()),
                    _ => None,
                };
                row.push((path.clone(), None, text));
            }
            row.push(("count".to_string(), Some(cell.count as f64), None));
            for (index, measure) in self.measure_paths.iter().enumerate() {
                row.push((format!("sum_{measure}"), Some(cell.sum(index)), None));
                row.push((format!("avg_{measure}"), cell.avg(index), None));
            }
            let sketches = self.cell_sketches.get(cell_slot);
            for (index, spec) in self.sketch_specs.iter().enumerate() {
                let sketch = sketches.and_then(|states| states.get(index));
                match spec.kind {
                    // Rounded, like every other surface. `COUNT(DISTINCT)`
                    // means a count; reporting 15.0068 advertises a precision
                    // the estimate does not have and reads as a bug.
                    SketchKind::DistinctCount => row.push((
                        format!("distinct_{}", spec.name),
                        sketch
                            .and_then(SketchState::distinct_estimate)
                            .map(f64::round),
                        None,
                    )),
                    SketchKind::Quantile => {
                        row.push((
                            format!("p50_{}", spec.name),
                            sketch.and_then(|state| state.percentile(0.5)),
                            None,
                        ));
                        row.push((
                            format!("p95_{}", spec.name),
                            sketch.and_then(|state| state.percentile(0.95)),
                            None,
                        ));
                    }
                }
            }
            rows.push(row);
        }
        rows
    }

    pub fn collection_name(&self) -> &str {
        &self.collection
    }

    pub fn dimension_names(&self) -> &[String] {
        &self.dimension_paths
    }

    pub fn measure_names(&self) -> &[String] {
        &self.measure_paths
    }
}

/// What a proposed grain would cost, measured against the real base table.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ProjectionEstimate {
    pub source_rows: u64,
    pub estimated_cells: u64,
    /// Per-dimension distinct values, in declared order — the input that
    /// explains a bad reduction ratio.
    pub dimension_cardinality: Vec<u64>,
    pub input_state_bytes: u64,
    pub cell_bytes: u64,
    /// `source_rows / estimated_cells`. Below ~2x a projection is barely an
    /// aggregate; the measured extremes on one million rows were 7,700x
    /// (`cms x ecommerce x score_band`) and 1.18x (`h3 x category`).
    pub reduction: f64,
    pub warnings: Vec<String>,
}

impl AggregateProjection {
    /// Estimate a proposed grain WITHOUT materializing it.
    ///
    /// Exact rather than sampled: it scans the base table once, which is a
    /// one-off planning cost and avoids reporting a confident-looking guess.
    /// (Sampling for very large tables is a later refinement — it must be
    /// labelled estimated when it lands.)
    pub fn estimate(&self, db: &BicDb) -> Result<ProjectionEstimate> {
        let mut distinct_tuples: std::collections::HashSet<Vec<String>> =
            std::collections::HashSet::new();
        let mut per_dimension: Vec<std::collections::HashSet<String>> =
            vec![std::collections::HashSet::new(); self.dimension_paths.len()];
        let mut rows = 0u64;
        for record in db.scan_collection(&self.collection)? {
            rows += 1;
            let tuple: Vec<String> = self
                .dimension_paths
                .iter()
                .enumerate()
                .map(|(index, path)| {
                    let value = match record.metadata.get(path) {
                        Some(serde_json::Value::String(text)) => text.clone(),
                        Some(serde_json::Value::Bool(flag)) => flag.to_string(),
                        Some(serde_json::Value::Number(number)) => number.to_string(),
                        _ => "\u{0}missing".to_string(),
                    };
                    per_dimension[index].insert(value.clone());
                    value
                })
                .collect();
            distinct_tuples.insert(tuple);
        }

        let cells = distinct_tuples.len() as u64;
        let reduction = if cells == 0 {
            0.0
        } else {
            rows as f64 / cells as f64
        };
        let mut warnings = Vec::new();
        if reduction < 2.0 && rows > 0 {
            warnings.push(format!(
                "high-cardinality grain: {cells} cells for {rows} source rows ({reduction:.2}x reduction). \
                 This projection is close to a second copy of the table; consider dropping the \
                 highest-cardinality dimension or rolling it up."
            ));
        }
        if let Some((index, count)) = per_dimension
            .iter()
            .enumerate()
            .map(|(index, values)| (index, values.len() as u64))
            .max_by_key(|(_, count)| *count)
        {
            if rows > 0 && count * 4 > rows {
                warnings.push(format!(
                    "dimension `{}` has {count} distinct values across {rows} rows and dominates the grain",
                    self.dimension_paths[index]
                ));
            }
        }

        Ok(ProjectionEstimate {
            source_rows: rows,
            estimated_cells: cells,
            dimension_cardinality: per_dimension
                .iter()
                .map(|values| values.len() as u64)
                .collect(),
            input_state_bytes: rows * self.layout.logical_bytes_per_row() as u64,
            cell_bytes: cells * self.layout.cell_width() as u64,
            reduction,
            warnings,
        })
    }
}
