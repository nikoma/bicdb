//! `bicdb backup online` + chain restore: the CLI surface over
//! `BicDb::create_online_backup` (#443).

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
}

#[test]
fn backup_online_produces_a_chain_the_cli_restores() {
    let source = tempfile::tempdir().unwrap();
    let out = tempfile::tempdir().unwrap();
    let target = out.path().join("restored");
    let base = out.path().join("base.bicbackup");
    let tail = out.path().join("wal-tail.bicbackup");
    {
        let mut db = BicDb::open_with_config(source.path(), config()).unwrap();
        db.create_collection("rows").unwrap();
        db.bulk_load_insert(
            "rows",
            (0..1_500).map(|index| {
                Record::new(format!("row-{index:05}"))
                    .with_metadata(json!({ "body": "x".repeat(300) }))
            }),
        )
        .unwrap();
        db.close().unwrap();
    }

    let output = Command::new(bicdb_bin())
        .env("BICDB_BACKUP_KEY", "cli-online-backup-key")
        .args([
            "backup",
            "online",
            source.path().to_str().unwrap(),
            base.to_str().unwrap(),
            tail.to_str().unwrap(),
        ])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "backup online failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(base.is_file() && tail.is_file());

    // Chain verify via the CLI.
    let output = Command::new(bicdb_bin())
        .env("BICDB_BACKUP_KEY", "cli-online-backup-key")
        .args([
            "backup",
            "verify",
            base.to_str().unwrap(),
            tail.to_str().unwrap(),
        ])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "chain verify failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    // Chain restore: both artifacts in one invocation, base first.
    let output = Command::new(bicdb_bin())
        .env("BICDB_BACKUP_KEY", "cli-online-backup-key")
        .args([
            "backup",
            "restore",
            base.to_str().unwrap(),
            tail.to_str().unwrap(),
            target.to_str().unwrap(),
        ])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "chain restore failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let restored = BicDb::open_with_config(&target, config()).unwrap();
    assert!(restored.get("rows", "row-00000").unwrap().is_some());
    assert!(restored.get("rows", "row-01499").unwrap().is_some());
}
