use bicdb_core::{BicDb, DbConfig, Geometry, Record, StorageMode};
use serde_json::json;

fn config() -> DbConfig {
    DbConfig::default()
        .with_fsync(false)
        .with_storage_mode(StorageMode::ServerPaged)
        .with_sync_outbox(false)
}

/// Open, create the collection, reopen — lazy/paged-primary, the state a bulk
/// importer leaves behind.
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

#[test]
fn spatial_index_on_lazy_collection_streams_without_materializing() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = reopened_with_collection(dir.path(), "places");

    // A cluster near the origin and a far-away cluster.
    let mut records = Vec::new();
    for index in 0..300u32 {
        records.push(place(
            &format!("near-{index:04}"),
            0.001 * f64::from(index % 10),
            0.001 * f64::from(index / 10),
        ));
    }
    for index in 0..200u32 {
        records.push(place(&format!("far-{index:04}"), 50.0, 50.0));
    }
    db.bulk_load_insert("places", records).unwrap();

    db.create_spatial_index("places", "geometry").unwrap();

    // The build must NOT have materialized the corpus resident: this is the
    // whole point of the streamed path (a 74M-row collection measured 50+ GiB
    // materialized).
    let residency = db.residency_report().unwrap();
    assert_eq!(
        residency.record_count, 0,
        "spatial build materialized {} rows resident instead of streaming",
        residency.record_count
    );

    // Radius hit resolves rows through the paged fallback.
    let results = db
        .within_radius("places", "geometry", 0.0, 0.0, 5_000.0)
        .unwrap();
    assert_eq!(results.len(), 300, "expected only the near cluster");
    assert!(results.iter().all(|hit| hit.record.id.starts_with("near-")));
    assert!(results
        .iter()
        .any(|hit| hit.record.metadata["name"] == json!("near-0000")));

    let verification = db.verify_index("idx_places_geometry_spatial").unwrap();
    assert!(
        verification.valid,
        "streamed spatial index failed verification: {verification:?}"
    );
    assert_eq!(verification.indexed_records, 500);
}

#[test]
fn streamed_spatial_index_rebuilds_lazily_across_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = reopened_with_collection(dir.path(), "places");
    let records = (0..250u32)
        .map(|index| {
            place(
                &format!("p-{index:04}"),
                10.0 + 0.001 * f64::from(index),
                20.0,
            )
        })
        .collect::<Vec<_>>();
    db.bulk_load_insert("places", records).unwrap();
    db.create_spatial_index("places", "geometry").unwrap();
    db.close().unwrap();

    // Reopen: the collection must stay lazy (no O(n) residency at open), and
    // the R-tree must rebuild from the identity scan, not from (empty)
    // resident shards.
    let db = BicDb::open_with_config(dir.path(), config()).unwrap();
    let residency = db.residency_report().unwrap();
    assert_eq!(
        residency.record_count, 0,
        "reopen materialized {} rows resident for the spatial index",
        residency.record_count
    );
    let results = db
        .within_radius("places", "geometry", 10.1, 20.0, 20_000.0)
        .unwrap();
    assert!(
        !results.is_empty(),
        "reopened streamed spatial index returned no hits"
    );
    for hit in &results {
        assert!(hit.record.id.starts_with("p-"));
        assert!(hit.distance_meters <= 20_000.0);
    }
}

#[test]
fn spatial_mutations_are_incremental_and_exact() {
    // Update-in-place, move, and delete against a live spatial index — the
    // paths that previously cloned and rebuilt the whole tree per row.
    let dir = tempfile::tempdir().unwrap();
    let mut db = reopened_with_collection(dir.path(), "places");
    db.bulk_load_insert(
        "places",
        (0..50u32)
            .map(|index| place(&format!("p-{index:02}"), 0.001 * f64::from(index), 0.0))
            .collect::<Vec<_>>(),
    )
    .unwrap();
    db.create_spatial_index("places", "geometry").unwrap();

    // Move p-00 far away: it must leave the near cluster and appear there.
    db.batch_insert("places", vec![place("p-00", 50.0, 50.0)])
        .unwrap();
    let near = db
        .within_radius("places", "geometry", 0.0, 0.0, 20_000.0)
        .unwrap();
    assert!(
        near.iter().all(|hit| hit.record.id != "p-00"),
        "moved row still returned at its old position"
    );
    let far = db
        .within_radius("places", "geometry", 50.0, 50.0, 1_000.0)
        .unwrap();
    assert_eq!(far.len(), 1);
    assert_eq!(far[0].record.id, "p-00");

    // Re-upsert at the same position must not duplicate the entry.
    db.batch_insert("places", vec![place("p-01", 0.001, 0.0)])
        .unwrap();
    db.batch_insert("places", vec![place("p-01", 0.001, 0.0)])
        .unwrap();
    let hits = db
        .within_radius("places", "geometry", 0.001, 0.0, 50.0)
        .unwrap();
    assert_eq!(
        hits.iter().filter(|hit| hit.record.id == "p-01").count(),
        1,
        "re-upsert duplicated the spatial entry"
    );

    // Delete removes the entry.
    db.delete("places", "p-02").unwrap();
    let after_delete = db
        .within_radius("places", "geometry", 0.0, 0.0, 20_000.0)
        .unwrap();
    assert!(after_delete.iter().all(|hit| hit.record.id != "p-02"));

    let verification = db.verify_index("idx_places_geometry_spatial").unwrap();
    assert!(
        verification.valid,
        "index invalid after incremental mutations: {verification:?}"
    );
}

#[test]
fn writes_after_streamed_spatial_index_stay_queryable() {
    // Rows inserted after the index exists take the checked path (the
    // bulk-load bypass refuses indexed collections) and must land in the
    // R-tree transactionally.
    let dir = tempfile::tempdir().unwrap();
    let mut db = reopened_with_collection(dir.path(), "places");
    db.bulk_load_insert("places", vec![place("seed-1", 0.0, 0.0)])
        .unwrap();
    db.create_spatial_index("places", "geometry").unwrap();

    db.batch_insert("places", vec![place("late-1", 0.0005, 0.0005)])
        .unwrap();

    let results = db
        .within_radius("places", "geometry", 0.0, 0.0, 5_000.0)
        .unwrap();
    let ids = results
        .iter()
        .map(|hit| hit.record.id.as_str())
        .collect::<Vec<_>>();
    assert!(ids.contains(&"seed-1"), "seed row missing: {ids:?}");
    assert!(ids.contains(&"late-1"), "post-index row missing: {ids:?}");
}
