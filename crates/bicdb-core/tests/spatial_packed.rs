//! Packed spatial index: immutable Hilbert/STR-packed durable base + delta.
//!
//! Pins the contract of `pack_spatial_index`: bulk-packed node tree in the
//! durable keyspace, atomic generation swap, durable delta tail for writes
//! after the pack, reopen from meta+delta (never the corpus), re-pack folds
//! the delta, DROP INDEX purges the namespace.

use bicdb_core::{BicDb, DbConfig, Geometry, Record, SpatialPackStrategy, StorageMode};
use serde_json::json;

const INDEX: &str = "idx_places_geometry_spatial";

fn config() -> DbConfig {
    DbConfig::default()
        .with_fsync(false)
        .with_storage_mode(StorageMode::ServerPaged)
        .with_sync_outbox(false)
}

fn reopened_with_collection(dir: &std::path::Path, collection: &str) -> BicDb {
    let mut db = BicDb::open_with_config(dir, config()).unwrap();
    db.create_collection(collection).unwrap();
    db.close().unwrap();
    BicDb::open_with_config(dir, config()).unwrap()
}

fn place(id: &str, lon: f64, lat: f64) -> Record {
    Record::new(id.to_string())
        .with_metadata(json!({ "name": id }))
        .with_geometry(Geometry::point(lon, lat).unwrap())
}

/// 300 points near the origin, 200 in a far cluster.
fn seeded(dir: &std::path::Path) -> BicDb {
    let mut db = reopened_with_collection(dir, "places");
    let mut records = Vec::new();
    for index in 0..300u32 {
        records.push(place(
            &format!("near-{index:04}"),
            0.001 * f64::from(index % 10),
            0.001 * f64::from(index / 10),
        ));
    }
    for index in 0..200u32 {
        records.push(place(
            &format!("far-{index:04}"),
            50.0 + 0.001 * f64::from(index),
            50.0,
        ));
    }
    db.bulk_load_insert("places", records).unwrap();
    db.create_spatial_index("places", "geometry").unwrap();
    db
}

fn radius_ids(db: &BicDb, lon: f64, lat: f64, meters: f64) -> Vec<String> {
    let mut ids = db
        .within_radius("places", "geometry", lon, lat, meters)
        .unwrap()
        .into_iter()
        .map(|hit| hit.record.id)
        .collect::<Vec<_>>();
    ids.sort();
    ids
}

#[test]
fn pack_builds_immutable_base_and_answers_match_the_dynamic_tree() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = seeded(dir.path());

    let before_near = radius_ids(&db, 0.0, 0.0, 5_000.0);
    let before_far = radius_ids(&db, 50.0, 50.0, 5_000.0);
    let before_nearest = db.nearest("places", "geometry", 0.0, 0.0, 7).unwrap();

    let report = db.pack_spatial_index(INDEX).unwrap();
    assert_eq!(report.index_name, INDEX);
    assert_eq!(report.strategy, SpatialPackStrategy::Hilbert);
    assert_eq!(report.generation, 1);
    assert_eq!(report.entry_count, 500);
    assert!(report.node_count >= 3, "500 entries should span nodes");
    assert!(report.height >= 2, "500 entries should stack levels");

    assert_eq!(radius_ids(&db, 0.0, 0.0, 5_000.0), before_near);
    assert_eq!(radius_ids(&db, 50.0, 50.0, 5_000.0), before_far);
    let after_nearest = db.nearest("places", "geometry", 0.0, 0.0, 7).unwrap();
    assert_eq!(
        after_nearest
            .iter()
            .map(|hit| hit.record.id.as_str())
            .collect::<Vec<_>>(),
        before_nearest
            .iter()
            .map(|hit| hit.record.id.as_str())
            .collect::<Vec<_>>(),
        "nearest order changed after packing"
    );

    let verification = db.verify_index(INDEX).unwrap();
    assert!(
        verification.valid,
        "packed index failed verification: {verification:?}"
    );
    assert_eq!(verification.indexed_records, 500);
}

#[test]
fn str_strategy_packs_and_answers_identically() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = seeded(dir.path());
    let before = radius_ids(&db, 0.0, 0.0, 5_000.0);
    let report = db
        .pack_spatial_index_with_strategy(INDEX, SpatialPackStrategy::Str)
        .unwrap();
    assert_eq!(report.strategy, SpatialPackStrategy::Str);
    assert_eq!(report.entry_count, 500);
    assert_eq!(radius_ids(&db, 0.0, 0.0, 5_000.0), before);
    assert!(db.verify_index(INDEX).unwrap().valid);
}

#[test]
fn packed_index_reopens_from_meta_without_scanning_the_corpus() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = seeded(dir.path());
    db.pack_spatial_index(INDEX).unwrap();
    let before = radius_ids(&db, 0.0, 0.0, 5_000.0);
    db.close().unwrap();

    let db = BicDb::open_with_config(dir.path(), config()).unwrap();
    let residency = db.residency_report().unwrap();
    assert_eq!(
        residency.record_count, 0,
        "reopen of a packed spatial index materialized {} rows",
        residency.record_count
    );
    assert_eq!(radius_ids(&db, 0.0, 0.0, 5_000.0), before);
    let verification = db.verify_index(INDEX).unwrap();
    assert!(
        verification.valid,
        "reopened packed index invalid: {verification:?}"
    );
}

#[test]
fn mutations_after_pack_mask_the_base_and_survive_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = seeded(dir.path());
    db.pack_spatial_index(INDEX).unwrap();

    // Move one row out of the near cluster, delete another, insert a fresh
    // one — all against the immutable base, so all must ride the delta.
    db.batch_insert("places", vec![place("near-0000", 50.0, 50.0)])
        .unwrap();
    db.delete("places", "near-0001").unwrap();
    db.batch_insert("places", vec![place("fresh-1", 0.0005, 0.0005)])
        .unwrap();

    let check = |db: &BicDb, phase: &str| {
        let near = radius_ids(db, 0.0, 0.0, 5_000.0);
        assert!(
            !near.contains(&"near-0000".to_string()),
            "{phase}: moved row still at old position"
        );
        assert!(
            !near.contains(&"near-0001".to_string()),
            "{phase}: deleted row still returned"
        );
        assert!(
            near.contains(&"fresh-1".to_string()),
            "{phase}: post-pack insert missing"
        );
        assert_eq!(near.len(), 299, "{phase}: unexpected near-cluster size");
        let moved = radius_ids(db, 50.0, 50.0, 1_000.0);
        assert!(
            moved.contains(&"near-0000".to_string()),
            "{phase}: moved row absent at new position"
        );
    };
    check(&db, "live");
    let verification = db.verify_index(INDEX).unwrap();
    assert!(
        verification.valid,
        "live delta state invalid: {verification:?}"
    );

    // The delta is durable: reopen must reconstruct tombstones + delta tree
    // from the delta tail, not lose the post-pack writes.
    db.close().unwrap();
    let db = BicDb::open_with_config(dir.path(), config()).unwrap();
    check(&db, "reopened");
    let verification = db.verify_index(INDEX).unwrap();
    assert!(
        verification.valid,
        "reopened delta state invalid: {verification:?}"
    );
}

#[test]
fn repack_folds_the_delta_and_retires_the_old_generation() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = seeded(dir.path());
    db.pack_spatial_index(INDEX).unwrap();
    db.batch_insert("places", vec![place("near-0000", 50.0, 50.0)])
        .unwrap();
    db.delete("places", "near-0001").unwrap();

    // Re-pack (switching strategy, which must be legal) folds the delta into
    // a fresh base and bumps the generation.
    let report = db
        .pack_spatial_index_with_strategy(INDEX, SpatialPackStrategy::Str)
        .unwrap();
    assert_eq!(report.generation, 2);
    assert_eq!(report.entry_count, 499, "fold lost or resurrected rows");

    let near = radius_ids(&db, 0.0, 0.0, 5_000.0);
    assert!(!near.contains(&"near-0000".to_string()));
    assert!(!near.contains(&"near-0001".to_string()));
    assert!(db.verify_index(INDEX).unwrap().valid);

    // And the folded state survives reopen.
    db.close().unwrap();
    let db = BicDb::open_with_config(dir.path(), config()).unwrap();
    let near = radius_ids(&db, 0.0, 0.0, 5_000.0);
    assert_eq!(near.len(), 298);
    assert!(db.verify_index(INDEX).unwrap().valid);
}

#[test]
fn rebuild_of_a_packed_index_repacks() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = seeded(dir.path());
    db.pack_spatial_index(INDEX).unwrap();
    db.delete("places", "near-0001").unwrap();

    let report = db.rebuild_index(INDEX).unwrap();
    assert!(report.valid, "rebuild-as-repack invalid: {report:?}");
    assert_eq!(report.indexed_records, 499);
    assert_eq!(radius_ids(&db, 0.0, 0.0, 5_000.0).len(), 299);
}

#[test]
fn drop_index_purges_the_packed_namespace() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = seeded(dir.path());
    db.pack_spatial_index(INDEX).unwrap();
    assert!(db.drop_index(INDEX).unwrap());

    // A fresh same-name index must start clean: no leaked meta resurrecting
    // a dropped packed base, no stale nodes behind a new pack.
    db.create_spatial_index("places", "geometry").unwrap();
    let report = db.pack_spatial_index(INDEX).unwrap();
    assert_eq!(
        report.generation, 1,
        "dropped index left packed meta behind"
    );
    assert_eq!(radius_ids(&db, 0.0, 0.0, 5_000.0).len(), 300);
    assert!(db.verify_index(INDEX).unwrap().valid);
}

#[test]
fn packing_requires_paged_storage_and_a_spatial_index() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = seeded(dir.path());
    assert!(db.pack_spatial_index("no_such_index").is_err());

    let embedded_dir = tempfile::tempdir().unwrap();
    let mut embedded = BicDb::open_with_config(
        embedded_dir.path(),
        DbConfig::default()
            .with_fsync(false)
            .with_sync_outbox(false),
    )
    .unwrap();
    embedded.create_collection("places").unwrap();
    embedded.insert("places", place("p-1", 0.0, 0.0)).unwrap();
    embedded.create_spatial_index("places", "geometry").unwrap();
    let error = embedded.pack_spatial_index(INDEX).unwrap_err();
    assert!(
        error.to_string().contains("paged storage"),
        "unexpected error: {error}"
    );
}

/// An interrupted pack (OOM, kill -9) leaves committed node batches under an
/// unpublished generation — invisible to readers and potentially many GiB.
/// Open must sweep them. Simulated by injecting a bogus generation through
/// the raw paged store, exactly the bytes a dying pack leaves behind.
#[test]
fn open_sweeps_crash_orphaned_pack_generations() {
    use bicdb_core::{PagedRecords, PagedRecordsOptions};

    let dir = tempfile::tempdir().unwrap();
    let mut db = seeded(dir.path());
    db.pack_spatial_index(INDEX).unwrap();
    db.close().unwrap();

    // Node key layout (paged_collection.rs): [0,0,13] ++ u16be(len) ++ index
    // ++ [1] ++ u64be(generation) ++ u64be(node).
    let node_key = |generation: u64, node: u64| -> Vec<u8> {
        let mut key = vec![0, 0, 13];
        key.extend_from_slice(&(INDEX.len() as u16).to_be_bytes());
        key.extend_from_slice(INDEX.as_bytes());
        key.push(1);
        key.extend_from_slice(&generation.to_be_bytes());
        key.extend_from_slice(&node.to_be_bytes());
        key
    };
    let orphan_generation = 99u64;
    {
        let paged = PagedRecords::open(
            dir.path().join("paged"),
            PagedRecordsOptions {
                fsync: false,
                ..PagedRecordsOptions::default()
            },
        )
        .unwrap();
        let store = paged.store();
        let (xid, _) = store.begin_transaction();
        for node in 1..=5u64 {
            store
                .put(xid, &node_key(orphan_generation, node), b"orphan")
                .unwrap();
        }
        store.commit(xid).unwrap();
    }

    // Reopen: the sweep must delete generation 99 and keep the published one.
    let db = BicDb::open_with_config(dir.path(), config()).unwrap();
    assert_eq!(radius_ids(&db, 0.0, 0.0, 5_000.0).len(), 300);
    drop(db);

    let paged = PagedRecords::open(
        dir.path().join("paged"),
        PagedRecordsOptions {
            fsync: false,
            ..PagedRecordsOptions::default()
        },
    )
    .unwrap();
    let store = paged.store();
    let snapshot = store.latest_snapshot();
    let orphan_prefix = &node_key(orphan_generation, 0)[..node_key(orphan_generation, 0).len() - 8];
    let mut orphan_keys = 0usize;
    let mut published_keys = 0usize;
    // Value scan, not a key scan: deleted keys stay visible as dead
    // historical entries until vacuum — liveness is what the sweep changes.
    for entry in store.scan_from(&snapshot, &[0, 0, 13]).unwrap() {
        let (key, _value) = entry.unwrap();
        if !key.starts_with(&[0, 0, 13]) {
            break;
        }
        if key.starts_with(orphan_prefix) {
            orphan_keys += 1;
        } else {
            published_keys += 1;
        }
    }
    assert_eq!(orphan_keys, 0, "orphaned generation survived reopen");
    assert!(
        published_keys > 0,
        "sweep must not touch the published generation"
    );
}

/// The ONLINE rebuild path must re-pack a packed index exactly like the
/// synchronous path — replacing only the resident state would silently stop
/// delta maintenance while the stale durable base stays published, losing
/// every later spatial mutation on reopen (confirmed review finding).
#[test]
fn online_rebuild_of_a_packed_index_repacks() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = seeded(dir.path());
    db.pack_spatial_index(INDEX).unwrap();

    let report = db.rebuild_index_online(INDEX).unwrap();
    assert_eq!(
        report.status, "complete",
        "online rebuild failed: {report:?}"
    );

    // Mutations AFTER the online rebuild must still reach the durable delta.
    db.batch_insert("places", vec![place("near-0000", 50.0, 50.0)])
        .unwrap();
    db.delete("places", "near-0001").unwrap();
    db.close().unwrap();

    let db = BicDb::open_with_config(dir.path(), config()).unwrap();
    let near = radius_ids(&db, 0.0, 0.0, 5_000.0);
    assert!(
        !near.contains(&"near-0000".to_string()),
        "post-online-rebuild move lost on reopen (stale packed base won)"
    );
    assert!(
        !near.contains(&"near-0001".to_string()),
        "post-online-rebuild delete lost on reopen"
    );
    assert!(db.verify_index(INDEX).unwrap().valid);
}

/// Renaming a packed spatial index would strand its durable keyspace under
/// the old name (queries error, DROP leaks) — refused until an alias layer
/// exists. Unpacked spatial indexes rename as before.
#[test]
fn rename_of_a_packed_index_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = seeded(dir.path());

    db.pack_spatial_index(INDEX).unwrap();
    let error = db.rename_index(INDEX, "idx_renamed").unwrap_err();
    assert!(
        error.to_string().contains("cannot be renamed"),
        "unexpected error: {error}"
    );
    // Still fully functional under its original name.
    assert_eq!(radius_ids(&db, 0.0, 0.0, 5_000.0).len(), 300);

    let unpacked_dir = tempfile::tempdir().unwrap();
    let mut unpacked = seeded(unpacked_dir.path());
    assert!(unpacked.rename_index(INDEX, "idx_renamed").unwrap());
}

/// create_packed_spatial_index registers the definition and packs without
/// ever building the corpus-sized resident tree.
#[test]
fn create_packed_spatial_index_goes_straight_to_the_packed_base() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = reopened_with_collection(dir.path(), "places");
    db.bulk_load_insert(
        "places",
        (0..400u32)
            .map(|index| place(&format!("p-{index:04}"), 0.001 * f64::from(index), 0.0))
            .collect::<Vec<_>>(),
    )
    .unwrap();

    let report = db
        .create_packed_spatial_index("places", "geometry", SpatialPackStrategy::Hilbert)
        .unwrap();
    assert_eq!(report.index_name, INDEX);
    assert_eq!(report.generation, 1);
    assert_eq!(report.entry_count, 400);
    assert!(!report.resumed);
    assert_eq!(db.residency_report().unwrap().record_count, 0);
    assert_eq!(radius_ids(&db, 0.0, 0.0, 50_000.0).len(), 400);
    assert!(db.verify_index(INDEX).unwrap().valid);

    // Calling it again on an existing index just re-packs.
    let report = db
        .create_packed_spatial_index("places", "geometry", SpatialPackStrategy::Hilbert)
        .unwrap();
    assert_eq!(report.generation, 2);

    db.close().unwrap();
    let db = BicDb::open_with_config(dir.path(), config()).unwrap();
    assert_eq!(radius_ids(&db, 0.0, 0.0, 50_000.0).len(), 400);
}

/// Brute-force haversine oracle over the LIVE rows (post-churn), the ground
/// truth `nearest` must reproduce through packed base + delta + tombstones.
fn brute_force_nearest(db: &BicDb, lon: f64, lat: f64, k: usize) -> Vec<String> {
    let haversine = |a_lon: f64, a_lat: f64, b_lon: f64, b_lat: f64| {
        let radius = 6_371_000.0_f64;
        let d_lat = (b_lat - a_lat).to_radians();
        let d_lon = (b_lon - a_lon).to_radians();
        let a = (d_lat / 2.0).sin().powi(2)
            + a_lat.to_radians().cos() * b_lat.to_radians().cos() * (d_lon / 2.0).sin().powi(2);
        2.0 * radius * a.sqrt().asin()
    };
    let mut scored: Vec<(f64, String)> = db
        .scan_collection("places")
        .unwrap()
        .into_iter()
        .filter_map(|record| {
            let Some(Geometry::Point(point)) = record.geometry.as_ref() else {
                return None;
            };
            Some((haversine(lon, lat, point.x(), point.y()), record.id))
        })
        .collect();
    scored.sort_by(|left, right| {
        left.0
            .total_cmp(&right.0)
            .then_with(|| left.1.cmp(&right.1))
    });
    scored.truncate(k);
    scored.into_iter().map(|(_, id)| id).collect()
}

#[test]
fn nearest_matches_brute_force_through_pack_delta_and_tombstones() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = seeded(dir.path());
    db.pack_spatial_index(INDEX).unwrap();

    // Churn against the immutable base: moves, deletes, and inserts that all
    // ride the delta and tombstone masking.
    db.batch_insert("places", vec![place("near-0000", 50.0, 50.0)])
        .unwrap();
    db.batch_insert("places", vec![place("far-0000", 0.0004, 0.0004)])
        .unwrap();
    db.delete("places", "near-0001").unwrap();
    db.delete("places", "far-0001").unwrap();
    db.batch_insert("places", vec![place("fresh-1", 0.0005, 0.0005)])
        .unwrap();
    db.batch_insert("places", vec![place("fresh-2", 49.9995, 49.9995)])
        .unwrap();

    let queries = [
        (0.0, 0.0),
        (0.0045, 0.0145),
        (50.0, 50.0),
        (25.0, 25.0),   // between the clusters
        (-120.0, 80.0), // far from everything
    ];
    let check = |db: &BicDb, phase: &str| {
        for (lon, lat) in queries {
            for k in [1usize, 3, 10, 400, 600] {
                let actual: Vec<String> = db
                    .nearest("places", "geometry", lon, lat, k)
                    .unwrap()
                    .into_iter()
                    .map(|hit| hit.record.id)
                    .collect();
                let expected = brute_force_nearest(db, lon, lat, k);
                assert_eq!(
                    actual, expected,
                    "{phase}: nearest mismatch at ({lon}, {lat}) k={k}"
                );
            }
        }
    };
    check(&db, "live");

    // The bounded walk must read from the durable delta + base after reopen
    // exactly as it did live.
    db.close().unwrap();
    let db = BicDb::open_with_config(dir.path(), config()).unwrap();
    check(&db, "reopened");
}
