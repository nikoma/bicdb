//! Retain the actual old preimage through repeated interrupted install cuts.
//! This does not prove cross-call logical association or target ACK inclusion.
use super::*;

fn payloads(root: &Path) -> BTreeMap<PathBuf, Vec<u8>> {
    fn visit(root: &Path, here: &Path, out: &mut BTreeMap<PathBuf, Vec<u8>>) {
        for entry in fs::read_dir(here).unwrap() {
            let entry = entry.unwrap();
            let path = entry.path();
            if entry.file_type().unwrap().is_dir() {
                visit(root, &path, out);
            } else if entry.file_name() != "store.lock"
                && entry.file_name() != storage::BACKUP_ONLINE_LOCK
                && entry.file_name() != storage::BACKUP_WRITE_GATE
            {
                out.insert(
                    path.strip_prefix(root).unwrap().to_path_buf(),
                    fs::read(path).unwrap(),
                );
            }
        }
    }
    let mut out = BTreeMap::new();
    visit(root, root, &mut out);
    out
}
fn config() -> DbConfig {
    DbConfig::default()
        .with_storage_mode(StorageMode::EmbeddedMemory)
        .with_fsync(true)
        .with_sync_outbox(false)
}
fn seed(root: &Path, id: &str) -> (BicDb, Record) {
    let mut db = BicDb::open_with_config(root, config()).unwrap();
    db.create_collection("items").unwrap();
    let record = Record::new(id)
        .with_metadata(serde_json::json!({"full":true,"nullable":null}))
        .with_payload(format!("complete original {id} payload").into_bytes());
    db.insert("items", record.clone()).unwrap();
    (db, record)
}
#[test]
fn actual_interrupted_old_preimage_survives_repeated_stage_rename_and_parent_sync_failures() {
    let directory = tempfile::tempdir().unwrap();
    let target = directory.path().join("target");
    let replaced = sibling_path(&target, "replaced");
    let staging = standby_bootstrap_staging(&target);
    let (source, source_record) = seed(&directory.path().join("source"), "source-full");
    let (old, old_record) = seed(&target, "old-full");
    old.close().unwrap();
    let archive = directory.path().join("source.bicbackup");
    let key = "synthetic-bootstrap-preimage-retry-key";
    source.create_standby_snapshot(&archive, key).unwrap();
    stage_standby_bootstrap(&target, &archive, key, true).unwrap();
    let old_before = payloads(&target);
    let staged_before = payloads(&staging);
    let mut original_errors = Vec::new();
    for attempt in 0..2 {
        let fault =
            bicdb_page::test_faults::arm_io_failure("bootstrap.stage_rename", &target, 1, false);
        let error = finish_standby_bootstrap(&target).unwrap_err();
        eprintln!("ACTUAL_BOOTSTRAP_PREIMAGE_STAGE_RENAME_ERROR {{attempt:{attempt},error:{error:?},fired:{}}}",fault.fired());
        assert_eq!(fault.fired(), 1, "required actual second-rename cut");
        assert!(matches!(&error,BicDbError::WriteFailed { site,.. } if site=="bootstrap.rename"));
        original_errors.push(error.to_string());
        drop(fault);
        assert!(
            !target.exists(),
            "old really moved before the second-rename fault"
        );
        assert!(replaced.is_dir());
        assert!(staging.join(BOOTSTRAP_READY).is_file());
        assert_eq!(
            payloads(&replaced),
            old_before,
            "actual original preimage survives retry{attempt}"
        );
        assert_eq!(
            payloads(&staging),
            staged_before,
            "actual complete staged source unchanged"
        );
    }
    // Read the actual retained old database, not just a marker or directory.
    let retained = BicDb::open_with_config(&replaced, config()).unwrap();
    assert_eq!(
        retained
            .get("items", &old_record.id)
            .unwrap()
            .unwrap()
            .as_ref(),
        &old_record
    );
    retained.close().unwrap();
    let retained_before = payloads(&replaced);
    let fault = bicdb_page::test_faults::arm_io_failure("bootstrap.dir_sync", &target, 1, false);
    let error = finish_standby_bootstrap(&target).unwrap_err();
    eprintln!(
        "ACTUAL_BOOTSTRAP_PREIMAGE_PARENT_SYNC_ERROR {{error:{error:?},fired:{}}}",
        fault.fired()
    );
    assert_eq!(
        fault.fired(),
        1,
        "required original actual post-rename parent-sync cut"
    );
    assert!(matches!(&error,BicDbError::WriteFailed {site,..} if site=="bootstrap.dir_sync"));
    original_errors.push(error.to_string());
    drop(fault);
    assert!(!staging.exists());
    assert!(target.join(BOOTSTRAP_READY).is_file());
    assert_eq!(payloads(&target), staged_before);
    assert_eq!(
        payloads(&replaced),
        retained_before,
        "old preimage cannot retire before successful parent sync"
    );
    let fresh = finish_standby_bootstrap(&target);
    eprintln!("ACTUAL_BOOTSTRAP_PREIMAGE_FRESH_RECOVERY {{original_errors:{original_errors:?},fresh:{fresh:?}}}");
    assert!(fresh.unwrap());
    assert!(!replaced.exists());
    assert!(!target.join(BOOTSTRAP_READY).exists());
    let actual = BicDb::open_with_config(&target, config()).unwrap();
    assert_eq!(
        actual
            .get("items", &source_record.id)
            .unwrap()
            .unwrap()
            .as_ref(),
        &source_record
    );
    assert!(actual.get("items", &old_record.id).unwrap().is_none());
    actual.close().unwrap();
    source.close().unwrap();
}
