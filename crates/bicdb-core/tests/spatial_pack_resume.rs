//! Resumable external-sort spatial pack: crash mid-scan, resume from the
//! checkpoint, and stay correct when writes land between crash and resume.
//!
//! Lives in its own test binary because it drives the pack through
//! process-global env knobs (`BICDB_SPATIAL_PACK_RUN_ENTRIES`,
//! `BICDB_SPATIAL_PACK_CRASH_AFTER_RUNS`); tests here serialize on ENV_LOCK.

use bicdb_core::{BicDb, DbConfig, Geometry, Record, SpatialPackStrategy, StorageMode};
use serde_json::json;

static ENV_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

const INDEX: &str = "idx_places_geometry_spatial";

fn config() -> DbConfig {
    DbConfig::default()
        .with_fsync(false)
        .with_storage_mode(StorageMode::ServerPaged)
        .with_sync_outbox(false)
}

fn place(id: &str, lon: f64, lat: f64) -> Record {
    Record::new(id.to_string())
        .with_metadata(json!({ "name": id }))
        .with_geometry(Geometry::point(lon, lat).unwrap())
}

fn seeded(dir: &std::path::Path, rows: u32) -> BicDb {
    let mut db = BicDb::open_with_config(dir, config()).unwrap();
    db.create_collection("places").unwrap();
    db.close().unwrap();
    let mut db = BicDb::open_with_config(dir, config()).unwrap();
    db.bulk_load_insert(
        "places",
        (0..rows)
            .map(|index| {
                place(
                    &format!("p-{index:05}"),
                    -10.0 + 0.0001 * f64::from(index),
                    0.5,
                )
            })
            .collect::<Vec<_>>(),
    )
    .unwrap();
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

struct EnvGuard(&'static str);
impl EnvGuard {
    fn set(key: &'static str, value: &str) -> Self {
        std::env::set_var(key, value);
        Self(key)
    }
}
impl Drop for EnvGuard {
    fn drop(&mut self) {
        std::env::remove_var(self.0);
    }
}

#[test]
fn crashed_scan_resumes_from_its_checkpoint_and_packs_exactly() {
    let _lock = ENV_LOCK.lock().unwrap();
    let _budget = EnvGuard::set("BICDB_SPATIAL_PACK_RUN_ENTRIES", "100");
    let dir = tempfile::tempdir().unwrap();
    let mut db = seeded(dir.path(), 500);

    // Crash after two sorted runs (200 of 500 entries scanned).
    let error = {
        let _crash = EnvGuard::set("BICDB_SPATIAL_PACK_CRASH_AFTER_RUNS", "2");
        db.create_packed_spatial_index("places", "geometry", SpatialPackStrategy::Hilbert)
            .unwrap_err()
    };
    assert!(
        error.to_string().contains("crash injection"),
        "unexpected error: {error}"
    );
    let workspace_root = dir.path().join("spatial_pack");
    let workspaces = std::fs::read_dir(&workspace_root)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .collect::<Vec<_>>();
    assert_eq!(workspaces.len(), 1);
    assert!(
        workspaces[0].join("checkpoint.json").is_file(),
        "crash left no checkpoint to resume from"
    );

    // The definition survived; re-running the pack RESUMES the scan.
    let report = db.pack_spatial_index(INDEX).unwrap();
    assert!(report.resumed, "pack restarted instead of resuming");
    assert_eq!(report.entry_count, 500);
    assert_eq!(report.generation, 1);
    assert_eq!(radius_ids(&db, -10.0, 0.5, 2_000_000.0).len(), 500);
    assert!(db.verify_index(INDEX).unwrap().valid);
    assert_eq!(
        std::fs::read_dir(workspace_root).unwrap().count(),
        0,
        "workspace not discarded after publish"
    );
}

#[test]
fn crashed_scan_survives_reopen_without_streaming_the_corpus_and_resumes() {
    let _lock = ENV_LOCK.lock().unwrap();
    let _budget = EnvGuard::set("BICDB_SPATIAL_PACK_RUN_ENTRIES", "100");
    let dir = tempfile::tempdir().unwrap();
    let mut db = seeded(dir.path(), 400);
    {
        let _crash = EnvGuard::set("BICDB_SPATIAL_PACK_CRASH_AFTER_RUNS", "1");
        db.create_packed_spatial_index("places", "geometry", SpatialPackStrategy::Hilbert)
            .unwrap_err();
    }
    db.close().unwrap();

    // Reopen mid-build: the index loads EMPTY (not the corpus-sized resident
    // tree — that build is the OOM this pipeline avoids). Queries are
    // incomplete until the pack completes: that's the in-flight contract.
    let mut db = BicDb::open_with_config(dir.path(), config()).unwrap();
    assert_eq!(db.residency_report().unwrap().record_count, 0);

    // Writes DURING the interrupted build must survive the eventual publish:
    // they ride the durable delta and mask whatever the runs captured.
    db.batch_insert("places", vec![place("late-1", 20.0, 20.0)])
        .unwrap();
    db.batch_insert("places", vec![place("p-00000", 21.0, 20.0)])
        .unwrap(); // moves a row the crashed scan already captured

    let report = db.pack_spatial_index(INDEX).unwrap();
    assert!(report.resumed, "pack restarted instead of resuming");

    let near_new = radius_ids(&db, 20.5, 20.0, 100_000.0);
    assert!(
        near_new.contains(&"late-1".to_string()),
        "mid-build insert lost"
    );
    assert!(
        near_new.contains(&"p-00000".to_string()),
        "mid-build move lost (stale packed position won)"
    );
    let old_cluster = radius_ids(&db, -10.0, 0.5, 2_000_000.0);
    assert!(
        !old_cluster.contains(&"p-00000".to_string()),
        "moved row still served from its stale packed position"
    );
    assert!(db.verify_index(INDEX).unwrap().valid);

    // And the folded state is durable.
    db.close().unwrap();
    let db = BicDb::open_with_config(dir.path(), config()).unwrap();
    assert!(radius_ids(&db, 20.5, 20.0, 100_000.0).contains(&"late-1".to_string()));
    assert!(db.verify_index(INDEX).unwrap().valid);
}

#[test]
fn run_budget_spills_and_packs_identically_to_in_memory() {
    let _lock = ENV_LOCK.lock().unwrap();
    let dir_spill = tempfile::tempdir().unwrap();
    let dir_memory = tempfile::tempdir().unwrap();

    let ids_spill = {
        let _budget = EnvGuard::set("BICDB_SPATIAL_PACK_RUN_ENTRIES", "37");
        let mut db = seeded(dir_spill.path(), 300);
        db.create_packed_spatial_index("places", "geometry", SpatialPackStrategy::Hilbert)
            .unwrap();
        radius_ids(&db, -10.0, 0.5, 2_000_000.0)
    };
    let ids_memory = {
        let mut db = seeded(dir_memory.path(), 300);
        db.create_packed_spatial_index("places", "geometry", SpatialPackStrategy::Hilbert)
            .unwrap();
        radius_ids(&db, -10.0, 0.5, 2_000_000.0)
    };
    assert_eq!(ids_spill, ids_memory);
    assert_eq!(ids_spill.len(), 300);
}
