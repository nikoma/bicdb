//! Actual search-index rename and snapshot unlink against a live root EX.
use super::*;
use std::os::unix::fs::MetadataExt;
use std::sync::mpsc;

fn populated_snapshot() -> (tempfile::TempDir, BicDb, Record) {
    let directory = tempfile::tempdir().unwrap();
    let mut db = BicDb::open_with_config(
        directory.path(),
        DbConfig::default()
            .with_storage_mode(StorageMode::EmbeddedMemory)
            .with_snapshot(true)
            .with_fsync(true),
    )
    .unwrap();
    db.create_collection("before").unwrap();
    let record = Record::new("full-vector-row")
        .with_vector(vec![1.0, 0.0])
        .with_metadata(serde_json::json!({"full":true,"nullable":null}))
        .with_payload(b"complete payload across physical cut".to_vec());
    db.insert("before", record.clone()).unwrap();
    db.create_vector_index("before", crate::HnswIndexConfig::default())
        .unwrap();
    db.compact().unwrap();
    assert!(db.hnsw_index_path("before").is_file());
    assert!(
        directory.path().join("snapshot/manifest.json").is_file(),
        "required genuine compact snapshot producer did not publish"
    );
    (directory, db, record)
}

fn exact_exclusion(root: &Path, paths: &[PathBuf], operation: impl FnOnce() -> Result<()> + Send) {
    let root = root.canonicalize().unwrap();
    let read = || {
        paths
            .iter()
            .map(|path| match fs::read(path) {
                Ok(bytes) => Some(bytes),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
                Err(error) => panic!("required actual file read failed: {error}"),
            })
            .collect::<Vec<_>>()
    };
    let original = read();
    let metadata = fs::metadata(root.join(storage::BACKUP_WRITE_GATE)).unwrap();
    let identity = (metadata.dev(), metadata.ino());
    let (cut_tx, cut_rx) = mpsc::channel();
    let worker_root = root.clone();
    let observed = std::thread::scope(|scope| {
        let _observer =
            storage::backup_gate_observation::arm(&root, true, Box::new(|_| {})).unwrap();
        let exclusive = storage::acquire_database_backup_exclusive_guard(&root).unwrap();
        let snapshots = storage::backup_gate_observation::live_exclusive_guards(&root);
        let worker = scope.spawn(move || -> Result<()> {
            let _observer = storage::backup_gate_observation::arm(
                &worker_root,
                false,
                Box::new(move |event| {
                    if !event.acquired && !event.released {
                        let _ = cut_tx.send(event);
                    }
                }),
            )?;
            operation()
        });
        let cut = cut_rx.recv_timeout(Duration::from_secs(5));
        let live_before = snapshots.len() == 1 && snapshots[0].is_live();
        let during = read();
        let live_after = snapshots.len() == 1 && snapshots[0].is_live();
        // A failed cut/assertion cannot leave the worker behind our own EX.
        drop(exclusive);
        let result = worker.join();
        (cut, snapshots, live_before, during, live_after, result)
    });
    let (cut, snapshots, live_before, during, live_after, result) = observed;
    eprintln!("ACTUAL_RENAME_OR_SNAPSHOT_ROOT_EX {{identity:{identity:?}, snapshots:{snapshots:?}, cut:{cut:?}, live_before:{live_before}, live_after:{live_after}, result:{result:?}}}");
    assert!(
        live_before && live_after,
        "INCONCLUSIVE: actual EX lifetime lost across reads"
    );
    let cut = cut.expect("INCONCLUSIVE: actual operation did not reach root SH contention");
    assert_eq!(cut.gate, root.join(storage::BACKUP_WRITE_GATE));
    assert_eq!(cut.file_identity, Some(identity));
    assert!(!cut.exclusive && !cut.acquired && !cut.released);
    assert_eq!(during, original);
    result.expect("actual operation worker panic").unwrap();
}

#[test]
fn actual_collection_rename_preserves_search_and_snapshot_files_while_root_ex_is_live() {
    let (directory, mut db, record) = populated_snapshot();
    let old_index = db.hnsw_index_path("before");
    let new_index = db.hnsw_index_path("after");
    let manifest = directory.path().join("snapshot/manifest.json");
    let expected = record.clone();
    exact_exclusion(
        directory.path(),
        &[old_index.clone(), new_index.clone(), manifest.clone()],
        move || {
            db.rename_collection("before", "after")?;
            assert_eq!(db.get("after", &expected.id)?.unwrap().as_ref(), &expected);
            assert_eq!(
                db.search_vector_ann("after", &[1.0, 0.0], 1, 16)?[0].record,
                expected
            );
            db.close()
        },
    );
    assert!(!old_index.exists());
    assert!(new_index.is_file());
    assert!(!manifest.exists());
    let db = BicDb::open(directory.path()).unwrap();
    assert_eq!(
        db.get("after", &record.id).unwrap().unwrap().as_ref(),
        &record
    );
    assert_eq!(
        db.search_vector_ann("after", &[1.0, 0.0], 1, 16).unwrap()[0].record,
        record
    );
}

#[test]
fn actual_snapshot_manifest_unlink_waits_for_same_root_ex_and_keeps_full_rows() {
    let (directory, db, record) = populated_snapshot();
    let manifest = directory.path().join("snapshot/manifest.json");
    let records = directory.path().join("snapshot/records.snap");
    let records_before = fs::read(&records).unwrap();
    let index = db.hnsw_index_path("before");
    let worker_root = directory.path().to_path_buf();
    exact_exclusion(
        directory.path(),
        &[manifest.clone(), records.clone(), index],
        move || crate::snapshot::invalidate(&worker_root),
    );
    assert!(!manifest.exists());
    assert_eq!(fs::read(&records).unwrap(), records_before);
    assert_eq!(
        db.get("before", &record.id).unwrap().unwrap().as_ref(),
        &record
    );
    assert_eq!(
        db.search_vector_ann("before", &[1.0, 0.0], 1, 16).unwrap()[0].record,
        record
    );
    db.close().unwrap();
    let db = BicDb::open(directory.path()).unwrap();
    assert_eq!(
        db.get("before", &record.id).unwrap().unwrap().as_ref(),
        &record
    );
    assert_eq!(
        db.search_vector_ann("before", &[1.0, 0.0], 1, 16).unwrap()[0].record,
        record
    );
}
