//! Full logical verification and operator commands after a storage-mode copy.
use bicdb_core::{BicDb, Record};
use std::process::Command;

#[test]
fn offline_sql_follows_the_persisted_mode_for_reads_and_writes() {
    for mode in [
        bicdb_core::StorageMode::EmbeddedMemory,
        bicdb_core::StorageMode::ServerPaged,
    ] {
        let root = tempfile::tempdir().unwrap();
        let mut db = BicDb::open_with_config(
            root.path(),
            bicdb_core::DbConfig::default()
                .with_storage_mode(mode.clone())
                .with_fsync(false),
        )
        .unwrap();
        {
            let mut session = bicdb_sql::SqlSession::new(&mut db);
            session
                .execute("CREATE TABLE retained_sql(id INT PRIMARY KEY,label TEXT)")
                .unwrap();
            session
                .execute("INSERT INTO retained_sql VALUES(1,'retained')")
                .unwrap();
        }
        db.close().unwrap();
        for (query, expected) in [
            ("SELECT label FROM retained_sql WHERE id=1", "retained"),
            (
                "UPDATE retained_sql SET label='updated' WHERE id=1 RETURNING label",
                "updated",
            ),
            ("SELECT label FROM retained_sql WHERE id=1", "updated"),
        ] {
            let output = Command::new(env!("CARGO_BIN_EXE_bicdb"))
                .arg("sql")
                .arg(root.path())
                .args(["--json", query])
                .output()
                .unwrap();
            assert!(
                output.status.success(),
                "{mode} {query}: {}",
                String::from_utf8_lossy(&output.stderr)
            );
            let body = String::from_utf8(output.stdout).unwrap();
            assert!(body.contains(expected), "{body}");
        }
    }
}

#[test]
fn migrate_verify_and_inspect_follow_the_persisted_mode() {
    let root = tempfile::tempdir().unwrap();
    let source = root.path().join("source");
    let paged = root.path().join("paged");
    let returned = root.path().join("returned");
    let mut db = BicDb::open(&source).unwrap();
    db.create_collection("items").unwrap();
    db.insert(
        "items",
        Record::new("id").with_metadata(serde_json::json!({"value": "retained"})),
    )
    .unwrap();
    db.close().unwrap();
    for (from, to, mode) in [
        (&source, &paged, "server_paged"),
        (&paged, &returned, "embedded_memory"),
    ] {
        let output = Command::new(env!("CARGO_BIN_EXE_bicdb"))
            .args(["store", "migrate-mode"])
            .arg(from)
            .arg(to)
            .args(["--mode", mode])
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(
            String::from_utf8_lossy(&output.stdout).contains("Verified 1 records after reopening")
        );
        for command in ["verify", "inspect"] {
            let output = Command::new(env!("CARGO_BIN_EXE_bicdb"))
                .arg(command)
                .arg(to)
                .output()
                .unwrap();
            assert!(
                output.status.success(),
                "{command}: {}",
                String::from_utf8_lossy(&output.stderr)
            );
        }
    }
}
