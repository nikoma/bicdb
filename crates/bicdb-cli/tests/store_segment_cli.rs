//! `bicdb store segment`: offline relayout of a paged store between the
//! monolithic and extent-segmented layouts, data-preserving both ways.

use std::process::Command;

use bicdb_core::{BicDb, DbConfig, Record, StorageMode};
use serde_json::json;

fn bicdb_bin() -> &'static str {
    env!("CARGO_BIN_EXE_bicdb")
}

fn config() -> DbConfig {
    DbConfig::default()
        .with_fsync(false)
        .with_storage_mode(StorageMode::ServerPaged)
        .with_sync_outbox(false)
        // Start monolithic so the conversion has something to do.
        .with_paged_extent_bytes(0)
}

#[test]
fn store_segment_round_trips_a_paged_database() {
    let dir = tempfile::tempdir().unwrap();
    {
        let mut db = BicDb::open_with_config(dir.path(), config()).unwrap();
        db.create_collection("rows").unwrap();
        let records = (0..2_000)
            .map(|index| {
                Record::new(format!("row-{index:05}"))
                    .with_metadata(json!({ "body": "x".repeat(512), "n": index }))
            })
            .collect::<Vec<_>>();
        db.bulk_load_insert("rows", records).unwrap();
        db.close().unwrap();
    }
    assert!(dir.path().join("paged/store.pages").is_file());

    // Monolithic → 1 MiB extents.
    let output = Command::new(bicdb_bin())
        .args([
            "store",
            "segment",
            dir.path().to_str().unwrap(),
            "--extent-bytes",
            "1048576",
        ])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "segment failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        dir.path().join("paged/store.pages.1").is_file(),
        "expected multiple segment files"
    );

    {
        let db = BicDb::open_with_config(dir.path(), config()).unwrap();
        assert!(db.get("rows", "row-01999").unwrap().is_some());
        assert!(db.get("rows", "row-00000").unwrap().is_some());
    }

    // Back to monolithic.
    let output = Command::new(bicdb_bin())
        .args([
            "store",
            "segment",
            dir.path().to_str().unwrap(),
            "--extent-bytes",
            "0",
        ])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "de-segment failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(!dir.path().join("paged/store.pages.1").exists());
    let db = BicDb::open_with_config(dir.path(), config()).unwrap();
    assert!(db.get("rows", "row-01234").unwrap().is_some());
}
