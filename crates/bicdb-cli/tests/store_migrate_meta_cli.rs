//! `bicdb store migrate-meta`: transform a pre-durable-abort-exceptions meta
//! page so old paged stores open on current engines. Dry-run reports, and
//! `--confirm` replays + rewrites, mirroring the stranded-production-store
//! recovery path end to end.

use std::process::Command;

use bicdb_core::{BicDb, DbConfig, Record, StorageMode};
use bicdb_page::page::PAGE_HEADER_BYTES;
use bicdb_page::{PageStore, PageStoreOptions};
use serde_json::json;

fn bicdb_bin() -> &'static str {
    env!("CARGO_BIN_EXE_bicdb")
}

fn config() -> DbConfig {
    DbConfig::default()
        .with_fsync(false)
        .with_storage_mode(StorageMode::ServerPaged)
        .with_paged_extent_bytes(0)
        .with_paged_accept_legacy_meta(false)
}

#[test]
fn migrate_meta_transforms_a_legacy_store() {
    let dir = tempfile::tempdir().unwrap();
    {
        let mut db = BicDb::open_with_config(dir.path(), config()).unwrap();
        db.create_collection("rows").unwrap();
        let records = (0..200)
            .map(|index| {
                Record::new(format!("row-{index:05}")).with_metadata(json!({ "n": index }))
            })
            .collect::<Vec<_>>();
        db.bulk_load_insert("rows", records).unwrap();
        db.close().unwrap();
    }

    // Forge the pre-durable-abort-exceptions signature: an extension region
    // no current engine would write.
    let paged_dir = dir.path().join("paged");
    {
        let store = PageStore::open(
            paged_dir.join("store.pages"),
            PageStoreOptions {
                page_size: 8192,
                fsync: false,
                create: false,
                extent_bytes: 0,
            },
        )
        .unwrap();
        let meta_page = store.root_page();
        assert_ne!(meta_page, 0);
        let mut buffer = vec![0u8; 8192];
        store.read_page(meta_page, &mut buffer).unwrap();
        buffer[PAGE_HEADER_BYTES + 40..PAGE_HEADER_BYTES + 48]
            .copy_from_slice(&100_000u64.to_le_bytes());
        store.write_page(meta_page, &mut buffer).unwrap();
        store.flush().unwrap();
    }

    // The store is now stranded on a strict engine.
    let stranded = BicDb::open_with_config(dir.path(), config());
    assert!(
        stranded.is_err(),
        "strict open must reject the forged region"
    );

    // Dry run: reports, changes nothing.
    let output = Command::new(bicdb_bin())
        .args(["store", "migrate-meta", dir.path().to_str().unwrap()])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "dry run failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("INVALID"), "dry run must report: {stdout}");
    assert!(
        stdout.contains("dry run only"),
        "must say it changed nothing: {stdout}"
    );
    assert!(
        BicDb::open_with_config(dir.path(), config()).is_err(),
        "dry run must not transform"
    );

    // Confirmed run: replays, rewrites, verifies.
    let output = Command::new(bicdb_bin())
        .args([
            "store",
            "migrate-meta",
            dir.path().to_str().unwrap(),
            "--confirm",
        ])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "migrate-meta --confirm failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("Transform complete"), "{stdout}");

    // The store opens strictly again and every row survived.
    let db = BicDb::open_with_config(dir.path(), config()).unwrap();
    assert!(db.get("rows", "row-00000").unwrap().is_some());
    assert!(db.get("rows", "row-00199").unwrap().is_some());
}

#[test]
fn migrate_meta_is_a_no_op_on_a_current_store() {
    let dir = tempfile::tempdir().unwrap();
    {
        let mut db = BicDb::open_with_config(dir.path(), config()).unwrap();
        db.create_collection("rows").unwrap();
        db.bulk_load_insert("rows", vec![Record::new("row-1")])
            .unwrap();
        db.close().unwrap();
    }
    let output = Command::new(bicdb_bin())
        .args([
            "store",
            "migrate-meta",
            dir.path().to_str().unwrap(),
            "--confirm",
        ])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("nothing to transform"), "{stdout}");
    let db = BicDb::open_with_config(dir.path(), config()).unwrap();
    assert!(db.get("rows", "row-1").unwrap().is_some());
}
