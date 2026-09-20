use bicdb_core::{BicDb, DbConfig, NodeId, Record};
use bicdb_sync::{FileSyncEndpoint, SyncCoordinator};
use serde_json::json;
use uuid::Uuid;

fn node(value: u128) -> NodeId {
    NodeId(Uuid::from_u128(value))
}

fn sync_config() -> DbConfig {
    DbConfig::default()
        .with_fsync(false)
        .with_audit_events(true)
        .with_mesh_signing(false)
        .with_require_signed_imports(false)
        .with_unsafe_legacy_mesh_collections(true)
}

#[test]
fn offline_clients_converge_through_filesystem_endpoint() {
    let left_temp = tempfile::tempdir().unwrap();
    let right_temp = tempfile::tempdir().unwrap();
    let endpoint_temp = tempfile::tempdir().unwrap();

    let mut left = BicDb::open_with_node_id(left_temp.path(), sync_config(), node(1)).unwrap();
    let mut right = BicDb::open_with_node_id(right_temp.path(), sync_config(), node(2)).unwrap();

    left.create_collection("patients").unwrap();
    right.create_collection("patients").unwrap();

    left.insert(
        "patients",
        Record::new("left-patient").with_metadata(json!({"clinic": "rural-1"})),
    )
    .unwrap();
    right
        .insert(
            "patients",
            Record::new("right-patient").with_metadata(json!({"clinic": "rural-2"})),
        )
        .unwrap();

    let mut endpoint = FileSyncEndpoint::open(endpoint_temp.path()).unwrap();
    let mut coordinator = SyncCoordinator::default();

    let left_first = coordinator.sync_once(&mut left, &mut endpoint).unwrap();
    assert_eq!(left_first.pushed_events, 1);
    assert_eq!(left_first.imported_events, 0);

    let right_first = coordinator.sync_once(&mut right, &mut endpoint).unwrap();
    assert_eq!(right_first.pushed_events, 1);
    assert_eq!(right_first.imported_events, 1);

    let left_second = coordinator.sync_once(&mut left, &mut endpoint).unwrap();
    assert_eq!(left_second.pushed_events, 0);
    assert_eq!(left_second.imported_events, 1);

    assert!(left.get("patients", "right-patient").unwrap().is_some());
    assert!(right.get("patients", "left-patient").unwrap().is_some());

    let left_third = coordinator.sync_once(&mut left, &mut endpoint).unwrap();
    assert_eq!(left_third.pushed_events, 0);
    assert_eq!(left_third.imported_events, 0);
    assert_eq!(left_third.duplicate_events, 0);
}

/// Sync bundles carry full record payloads, and this endpoint writes them
/// to a shared folder, a courier USB stick, or a hub dropbox. Every bundle
/// was pretty-printed JSON in the clear: anyone with file access read the
/// records — protected data in the reference field-clinic workload. `SyncBundle` has
/// supported authenticated encryption since the mesh landed; nothing ever
/// called it.
#[test]
fn encrypted_endpoints_seal_bundles_and_still_converge() {
    let left_temp = tempfile::tempdir().unwrap();
    let right_temp = tempfile::tempdir().unwrap();
    let endpoint_temp = tempfile::tempdir().unwrap();

    let mut left = BicDb::open_with_config(left_temp.path(), sync_config()).unwrap();
    let mut right = BicDb::open_with_config(right_temp.path(), sync_config()).unwrap();
    left.create_collection("patients").unwrap();
    right.create_collection("patients").unwrap();
    left.insert(
        "patients",
        Record::new("left-patient").with_metadata(json!({"diagnosis": "PLAINTEXTCANARY"})),
    )
    .unwrap();
    right
        .insert(
            "patients",
            Record::new("right-patient").with_metadata(json!({"clinic": "rural-2"})),
        )
        .unwrap();

    let encryption = bicdb_core::EncryptionConfig::with_passphrase("mesh-courier-passphrase");
    let mut endpoint =
        FileSyncEndpoint::open_encrypted(endpoint_temp.path(), false, encryption.clone()).unwrap();
    let mut coordinator = SyncCoordinator::default();

    coordinator.sync_once(&mut left, &mut endpoint).unwrap();
    coordinator.sync_once(&mut right, &mut endpoint).unwrap();
    coordinator.sync_once(&mut left, &mut endpoint).unwrap();

    // Convergence is unaffected: both sides hold both records.
    assert!(left.get("patients", "right-patient").unwrap().is_some());
    assert!(right.get("patients", "left-patient").unwrap().is_some());

    // Nothing on the transport reveals the record contents.
    fn files(root: &std::path::Path, out: &mut Vec<std::path::PathBuf>) {
        for entry in std::fs::read_dir(root).unwrap().flatten() {
            let path = entry.path();
            if path.is_dir() {
                files(&path, out);
            } else {
                out.push(path);
            }
        }
    }
    let mut paths = Vec::new();
    files(endpoint_temp.path(), &mut paths);
    let mut inspected = 0usize;
    for path in paths {
        let bytes = std::fs::read(&path).unwrap();
        let text = String::from_utf8_lossy(&bytes);
        assert!(
            !text.contains("PLAINTEXTCANARY"),
            "bundle {} exposed record data in the clear",
            path.display()
        );
        assert!(
            !text.contains("left-patient"),
            "bundle {} exposed a record id in the clear",
            path.display()
        );
        inspected += 1;
    }
    assert!(
        inspected > 0,
        "no bundles were written, so nothing was proven"
    );
}
