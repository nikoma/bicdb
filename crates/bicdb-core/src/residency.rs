//! Resident-memory accounting for the `embedded_memory` engine.
//!
//! Phase 0 of `docs/server-paged-storage-todo.md` requires separate resident
//! costs for rows, version chains, primary-key maps, secondary indexes, exact
//! vectors, HNSW, and graphs, exposed machine-readably. Every later gate in that
//! roadmap is stated as a bound on memory ("steady RSS stays inside the declared
//! envelope", "RSS must not grow with total database size"), and none of them can
//! be evaluated without first being able to attribute the bytes.
//!
//! # What these numbers are
//!
//! An **estimate of live heap bytes owned by BicDB's resident structures**,
//! computed by walking them. Specifically:
//!
//! - Hash-table costs use *capacity*, not length, because that is what is
//!   actually held. The bucket-array formula approximates hashbrown's layout
//!   (one control byte per bucket plus the entry array); it is not exact.
//! - Allocator overhead, size-class rounding, fragmentation, thread caches, and
//!   arenas that have been freed to BicDB but not returned to the OS are **not**
//!   counted. Neither is anything outside the walked structures — stacks,
//!   buffers in flight, the segment page cache the OS keeps for us.
//! - Consequently `accounted_bytes` is a lower bound on RSS, and the difference
//!   ([`ResidencyReport::unaccounted_bytes`]) is itself the interesting figure:
//!   it is the part of the memory envelope that this accounting cannot yet
//!   explain, and Phase 0's job is to make that gap small and understood.
//!
//! # Shared records are counted once
//!
//! A committed record is allocated once and shared by `Arc` between the live
//! record map and its MVCC version chain. Naively summing both would double-count
//! every live row. Each distinct allocation is therefore attributed to exactly
//! one category, live rows first, so that `version_chain_bytes` answers the
//! question an operator actually has: *how much would dropping retained history
//! give back?* — the chain spine plus the versions no longer reachable as live
//! rows.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

/// Categories the roadmap names but that are not yet instrumented. Reported
/// explicitly rather than as zero: a budget that silently reads `0` for
/// "unmeasured" is worse than one that admits the hole.
pub const NOT_INSTRUMENTED: &[&str] = &["broker_projections", "query_intermediates"];

/// Heap bytes owned by a value, *excluding* the inline `size_of` the value
/// itself (which is already counted by whatever contains it).
pub(crate) trait ResidentBytes {
    fn heap_bytes(&self) -> u64;
}

/// Approximate bytes held by a hashbrown table with `capacity` usable slots.
///
/// hashbrown rounds up to a power-of-two bucket count and keeps one control byte
/// per bucket alongside the entry array. `capacity()` reports usable slots (7/8
/// of buckets), so this understates slightly; the error is a few percent and
/// consistent, which is what matters for tracking a budget over time.
pub(crate) fn table_bytes<K, V>(capacity: usize) -> u64 {
    let entry = std::mem::size_of::<(K, V)>() as u64;
    capacity as u64 * (entry + 1)
}

/// Approximate bytes held by a hashbrown set with `capacity` usable slots.
pub(crate) fn set_table_bytes<T>(capacity: usize) -> u64 {
    let entry = std::mem::size_of::<T>() as u64;
    capacity as u64 * (entry + 1)
}

/// Bytes held by a `Vec`'s buffer (capacity, not length).
pub(crate) fn vec_bytes<T>(capacity: usize) -> u64 {
    capacity as u64 * std::mem::size_of::<T>() as u64
}

/// Bytes held by a `String`'s buffer.
pub(crate) fn string_bytes(value: &str) -> u64 {
    value.len() as u64
}

/// Per-collection resident breakdown.
#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct CollectionResidency {
    pub name: String,
    pub record_count: u64,
    pub version_count: u64,
    /// Live rows: the `Arc<StoredRecord>` allocations reachable as current
    /// records, plus the record map's table.
    pub rows_bytes: u64,
    /// Retained MVCC history: chain spines, `VersionedRecord` entries, and the
    /// record allocations reachable *only* from history. This is what version
    /// reclamation would return.
    pub version_chains_bytes: u64,
    /// Primary-key maps and per-record commit bookkeeping: `pk_to_rowid`,
    /// primary-key/rowid maps, and the dirty-since-checkpoint set.
    pub primary_key_maps_bytes: u64,
    /// The denormalized exact-search vector store (ids, vectors, norms).
    pub exact_vectors_bytes: u64,
}

impl CollectionResidency {
    pub fn total_bytes(&self) -> u64 {
        self.rows_bytes
            + self.version_chains_bytes
            + self.primary_key_maps_bytes
            + self.exact_vectors_bytes
    }
}

/// Per-index resident cost. Kept separate from collections because an index's
/// memory is reclaimable independently (drop and rebuild) and because Phase 4
/// exists to move exactly these bytes to disk.
#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct IndexResidency {
    pub name: String,
    pub collection: String,
    pub entry_count: u64,
    /// The ordered byte-keyed store behind the index.
    pub store_bytes: u64,
    /// R-tree payload for spatial indexes.
    pub spatial_bytes: u64,
}

impl IndexResidency {
    pub fn total_bytes(&self) -> u64 {
        self.store_bytes + self.spatial_bytes
    }
}

/// HNSW graph residency, per indexed collection.
#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct HnswResidency {
    pub collection: String,
    pub node_count: u64,
    /// Vectors copied into graph nodes — duplicated with the row's own vector
    /// and with the exact-search store. Phase 6 exists to stop paying this
    /// three times.
    pub vectors_bytes: u64,
    /// Graph topology: per-level neighbour lists and their spines.
    pub graph_bytes: u64,
    /// Record-id strings held by nodes and the id lookup map.
    pub identity_bytes: u64,
}

impl HnswResidency {
    pub fn total_bytes(&self) -> u64 {
        self.vectors_bytes + self.graph_bytes + self.identity_bytes
    }
}

/// Machine-readable resident-memory report for a database.
#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct ResidencyReport {
    pub generated_at: i64,
    /// Sum of everything below. A lower bound on RSS — see the module docs.
    pub accounted_bytes: u64,
    /// Process resident set size, when the platform can report it.
    pub process_resident_bytes: Option<u64>,
    /// `process_resident_bytes - accounted_bytes`. The share of the memory
    /// envelope this accounting cannot yet explain.
    pub unaccounted_bytes: Option<i64>,
    pub rows_bytes: u64,
    pub version_chains_bytes: u64,
    pub primary_key_maps_bytes: u64,
    pub secondary_indexes_bytes: u64,
    pub exact_vectors_bytes: u64,
    pub hnsw_bytes: u64,
    pub graphs_bytes: u64,
    pub record_count: u64,
    pub version_count: u64,
    pub collections: Vec<CollectionResidency>,
    pub indexes: Vec<IndexResidency>,
    pub hnsw: Vec<HnswResidency>,
    /// Roadmap categories with no instrumentation behind them yet.
    pub not_instrumented: Vec<String>,
}

impl ResidencyReport {
    /// Aggregate category totals, for metric emission.
    ///
    /// Deliberately excludes per-collection and per-index rows: the roadmap
    /// requires bounded-cardinality labels, and collection/index names are
    /// user-controlled. Those breakdowns stay in the JSON report, which is
    /// fetched on demand rather than scraped into a time series.
    pub fn category_totals(&self) -> BTreeMap<&'static str, u64> {
        BTreeMap::from([
            ("rows_bytes", self.rows_bytes),
            ("version_chains_bytes", self.version_chains_bytes),
            ("primary_key_maps_bytes", self.primary_key_maps_bytes),
            ("secondary_indexes_bytes", self.secondary_indexes_bytes),
            ("exact_vectors_bytes", self.exact_vectors_bytes),
            ("hnsw_bytes", self.hnsw_bytes),
            ("graphs_bytes", self.graphs_bytes),
            ("accounted_bytes", self.accounted_bytes),
            ("record_count", self.record_count),
            ("version_count", self.version_count),
        ])
    }

    /// Recompute totals from the per-object breakdowns and stamp derived fields.
    pub(crate) fn finalize(mut self, generated_at: i64) -> Self {
        self.generated_at = generated_at;
        self.record_count = self.collections.iter().map(|c| c.record_count).sum();
        self.version_count = self.collections.iter().map(|c| c.version_count).sum();
        self.rows_bytes = self.collections.iter().map(|c| c.rows_bytes).sum();
        self.version_chains_bytes = self
            .collections
            .iter()
            .map(|c| c.version_chains_bytes)
            .sum();
        self.primary_key_maps_bytes = self
            .collections
            .iter()
            .map(|c| c.primary_key_maps_bytes)
            .sum();
        self.exact_vectors_bytes = self.collections.iter().map(|c| c.exact_vectors_bytes).sum();
        self.secondary_indexes_bytes = self.indexes.iter().map(|i| i.total_bytes()).sum();
        self.hnsw_bytes = self.hnsw.iter().map(|h| h.total_bytes()).sum();

        self.accounted_bytes = self.rows_bytes
            + self.version_chains_bytes
            + self.primary_key_maps_bytes
            + self.secondary_indexes_bytes
            + self.exact_vectors_bytes
            + self.hnsw_bytes
            + self.graphs_bytes;

        self.process_resident_bytes = process_resident_bytes();
        self.unaccounted_bytes = self
            .process_resident_bytes
            .map(|rss| rss as i64 - self.accounted_bytes as i64);
        self.not_instrumented = NOT_INSTRUMENTED.iter().map(|s| s.to_string()).collect();
        self
    }
}

/// Process resident set size in bytes, or `None` where it cannot be read.
///
/// Read from `/proc/self/statm` (field 2, resident pages) rather than parsed out
/// of `/proc/self/status`, because `statm` is a single short line of integers and
/// is cheap enough to sample alongside a report.
pub fn process_resident_bytes() -> Option<u64> {
    #[cfg(target_os = "linux")]
    {
        let statm = std::fs::read_to_string("/proc/self/statm").ok()?;
        let resident_pages: u64 = statm.split_whitespace().nth(1)?.parse().ok()?;
        // `sysconf(_SC_PAGESIZE)` is 4 KiB on every platform BicDB targets for
        // server deployment; hardcoded to keep this dependency-free.
        Some(resident_pages * 4096)
    }
    #[cfg(not(target_os = "linux"))]
    {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn table_bytes_scales_with_capacity_not_length() {
        let small = table_bytes::<u64, u64>(16);
        let large = table_bytes::<u64, u64>(1024);
        assert!(large > small);
        assert_eq!(large, 1024 * (std::mem::size_of::<(u64, u64)>() as u64 + 1));
    }

    #[test]
    fn finalize_sums_categories_and_derives_the_unaccounted_gap() {
        let report = ResidencyReport {
            collections: vec![CollectionResidency {
                name: "c".to_string(),
                record_count: 2,
                version_count: 3,
                rows_bytes: 100,
                version_chains_bytes: 20,
                primary_key_maps_bytes: 10,
                exact_vectors_bytes: 5,
            }],
            indexes: vec![IndexResidency {
                name: "i".to_string(),
                collection: "c".to_string(),
                entry_count: 2,
                store_bytes: 40,
                spatial_bytes: 0,
            }],
            hnsw: vec![HnswResidency {
                collection: "c".to_string(),
                node_count: 1,
                vectors_bytes: 8,
                graph_bytes: 4,
                identity_bytes: 2,
            }],
            graphs_bytes: 7,
            ..Default::default()
        }
        .finalize(1);

        assert_eq!(report.rows_bytes, 100);
        assert_eq!(report.secondary_indexes_bytes, 40);
        assert_eq!(report.hnsw_bytes, 14);
        assert_eq!(report.accounted_bytes, 100 + 20 + 10 + 40 + 5 + 14 + 7);
        assert_eq!(report.record_count, 2);
        assert_eq!(report.version_count, 3);

        if let (Some(rss), Some(gap)) = (report.process_resident_bytes, report.unaccounted_bytes) {
            assert_eq!(gap, rss as i64 - report.accounted_bytes as i64);
        }
    }

    #[test]
    fn category_totals_have_fixed_bounded_cardinality() {
        // The metric surface must not grow with the number of collections or
        // indexes; per-object detail stays in the JSON report.
        let one = ResidencyReport {
            collections: vec![CollectionResidency::default()],
            ..Default::default()
        }
        .finalize(0);
        let many = ResidencyReport {
            collections: (0..50)
                .map(|i| CollectionResidency {
                    name: format!("c{i}"),
                    ..Default::default()
                })
                .collect(),
            indexes: (0..50)
                .map(|i| IndexResidency {
                    name: format!("i{i}"),
                    ..Default::default()
                })
                .collect(),
            ..Default::default()
        }
        .finalize(0);

        assert_eq!(
            one.category_totals().keys().collect::<Vec<_>>(),
            many.category_totals().keys().collect::<Vec<_>>()
        );
    }

    #[test]
    fn unmeasured_categories_are_named_rather_than_reported_as_zero() {
        let report = ResidencyReport::default().finalize(0);
        assert!(report
            .not_instrumented
            .contains(&"query_intermediates".to_string()));
        assert!(report
            .not_instrumented
            .contains(&"broker_projections".to_string()));
    }
}
