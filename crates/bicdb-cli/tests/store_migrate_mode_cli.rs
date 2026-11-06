//! Full logical verification and operator commands after a storage-mode copy.
use bicdb_core::{BicDb, Record};
use std::process::Command;

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
