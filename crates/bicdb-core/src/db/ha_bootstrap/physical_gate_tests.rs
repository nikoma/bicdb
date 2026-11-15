//! Actual bootstrap owners/backup gates and READY-before-restore completion.
//! These controls preserve the original bootstrap fault integration tests.
use super::*;
use std::os::unix::fs::MetadataExt;
use std::sync::mpsc;

const KEY: &str = "synthetic-bootstrap-owned-tail-key";
struct Scenario {
    directory: tempfile::TempDir,
    primary: BicDb,
    target: PathBuf,
    source: Record,
    old: Record,
}
impl Scenario {
    fn new(mode: StorageMode) -> Self {
        let directory = tempfile::tempdir().unwrap();
        let target = directory.path().join("standby");
        let config = DbConfig::default()
            .with_storage_mode(mode)
            .with_fsync(true)
            .with_paged_buffer_pool_bytes(1024 * 1024);
        let mut primary =
            BicDb::open_with_config(directory.path().join("primary"), config.clone()).unwrap();
        primary.create_collection("items").unwrap();
        let source = Record::new("source-full")
            .with_metadata(serde_json::json!({"from":"source","nullable":null}))
            .with_payload(b"complete source payload".to_vec());
        primary.insert("items", source.clone()).unwrap();
        let mut old_db = BicDb::open_with_config(&target, config).unwrap();
        old_db.create_collection("items").unwrap();
        let old = Record::new("old-full")
            .with_metadata(serde_json::json!({"from":"old","nullable":null}))
            .with_payload(b"complete original payload".to_vec());
        old_db.insert("items", old.clone()).unwrap();
        old_db.close().unwrap();
        Self {
            directory,
            primary,
            target,
            source,
            old,
        }
    }
    fn archive(&self, repository: bool) -> PathBuf {
        if repository {
            let repo = self.directory.path().join("repository");
            fs::create_dir_all(&repo).unwrap();
            let path = repo.join("full.bicbackup");
            self.primary
                .capture_online_backup(crate::OnlineBackupLimits::default())
                .unwrap()
                .write_backup(
                    &path,
                    crate::backup::BackupCreateOptions {
                        passphrase: KEY.into(),
                        base_backup: None,
                    },
                    crate::OnlineBackupControl::default(),
                )
                .unwrap();
            assert!(crate::backup::is_repository_backup(&path).unwrap());
            path
        } else {
            let path = self.directory.path().join("encrypted.bicbackup");
            self.primary.create_standby_snapshot(&path, KEY).unwrap();
            assert!(!crate::backup::is_repository_backup(&path).unwrap());
            path
        }
    }
    fn assert_installed(&self, mode: StorageMode) {
        let db = BicDb::open_with_config(
            &self.target,
            DbConfig::default()
                .with_storage_mode(mode)
                .with_paged_buffer_pool_bytes(1024 * 1024),
        )
        .unwrap();
        assert_eq!(
            db.get("items", &self.source.id).unwrap().unwrap().as_ref(),
            &self.source
        );
        assert!(db.get("items", &self.old.id).unwrap().is_none());
    }
}

fn payloads(root: &Path) -> BTreeMap<PathBuf, Vec<u8>> {
    fn collect(root: &Path, here: &Path, out: &mut BTreeMap<PathBuf, Vec<u8>>) {
        for entry in fs::read_dir(here).unwrap() {
            let entry = entry.unwrap();
            let path = entry.path();
            if entry.file_type().unwrap().is_dir() {
                collect(root, &path, out);
            } else if entry.file_name() != "store.lock"
                && entry.file_name() != storage::BACKUP_WRITE_GATE
                && entry.file_name() != storage::BACKUP_ONLINE_LOCK
            {
                out.insert(
                    path.strip_prefix(root).unwrap().to_path_buf(),
                    fs::read(path).unwrap(),
                );
            }
        }
    }
    let mut out = BTreeMap::new();
    collect(root, root, &mut out);
    out
}

#[test]
fn actual_bootstrap_install_waits_for_original_root_ex_before_any_rename() {
    let scenario = Scenario::new(StorageMode::EmbeddedMemory);
    let archive = scenario.archive(false);
    stage_standby_bootstrap(&scenario.target, &archive, KEY, true).unwrap();
    let staging = standby_bootstrap_staging(&scenario.target);
    let old_before = payloads(&scenario.target);
    let staged_before = payloads(&staging);
    let root = scenario.target.canonicalize().unwrap();
    let metadata = fs::metadata(root.join(storage::BACKUP_WRITE_GATE)).unwrap();
    let identity = (metadata.dev(), metadata.ino());
    let (cut_tx, cut_rx) = mpsc::channel();
    let worker_root = root.clone();
    let observed = std::thread::scope(|scope| {
        let _observer =
            storage::backup_gate_observation::arm(&root, true, Box::new(|_| {})).unwrap();
        let exclusive = storage::acquire_database_backup_exclusive_guard(&root).unwrap();
        let snapshots = storage::backup_gate_observation::live_exclusive_guards(&root);
        let worker = scope.spawn(move || -> Result<bool> {
            let _observer = storage::backup_gate_observation::arm(
                &worker_root,
                false,
                Box::new(move |event| {
                    if !event.acquired && !event.released {
                        let _ = cut_tx.send(event);
                    }
                }),
            )?;
            finish_standby_bootstrap(&worker_root)
        });
        let cut = cut_rx.recv_timeout(Duration::from_secs(5));
        let live_before = snapshots.len() == 1 && snapshots[0].is_live();
        let old_during = payloads(&root);
        let staged_during = payloads(&staging);
        let replaced_exists = sibling_path(&root, "replaced").exists();
        let live_after = snapshots.len() == 1 && snapshots[0].is_live();
        drop(exclusive);
        let result = worker.join();
        (
            cut,
            snapshots,
            live_before,
            old_during,
            staged_during,
            replaced_exists,
            live_after,
            result,
        )
    });
    let (
        cut,
        snapshots,
        live_before,
        old_during,
        staged_during,
        replaced_exists,
        live_after,
        result,
    ) = observed;
    eprintln!("ACTUAL_BOOTSTRAP_ROOT_EX {{identity:{identity:?}, snapshots:{snapshots:?}, cut:{cut:?}, live_before:{live_before}, live_after:{live_after}, result:{result:?}}}");
    assert!(
        live_before && live_after,
        "INCONCLUSIVE: same actual EX lost across read cut"
    );
    let cut = cut.expect("INCONCLUSIVE: bootstrap did not reach actual old root SH contention");
    assert_eq!(cut.gate, root.join(storage::BACKUP_WRITE_GATE));
    assert_eq!(cut.file_identity, Some(identity));
    assert!(!cut.exclusive && !cut.acquired && !cut.released);
    assert_eq!(old_during, old_before);
    assert_eq!(staged_during, staged_before);
    assert!(!replaced_exists);
    assert!(result.expect("actual installer panic").unwrap());
    scenario.assert_installed(StorageMode::EmbeddedMemory);
}

#[test]
fn actual_ready_tail_failure_keeps_pending_and_cannot_install_either_restore_codec() {
    for repository in [false, true] {
        let scenario = Scenario::new(StorageMode::EmbeddedMemory);
        let archive = scenario.archive(repository);
        // Existing PG stages before restart: the old owner really is live.
        let old = BicDb::open(&scenario.target).unwrap();
        let before = payloads(&scenario.target);
        let staging = standby_bootstrap_staging(&scenario.target);
        let fault = bicdb_page::test_faults::arm_io_failure(
            "bootstrap.ready.before_restore_complete",
            &staging,
            1,
            false,
        );
        let error = stage_standby_bootstrap(&scenario.target, &archive, KEY, true).unwrap_err();
        eprintln!("ACTUAL_READY_RESTORE_PENDING_ERROR {{repository:{repository}, error:{error:?}, fired:{}}}",fault.fired());
        assert!(matches!(error, BicDbError::Io(_)));
        assert_eq!(
            fault.fired(),
            1,
            "required actual post-READY/pre-pending-removal cut"
        );
        drop(fault);
        assert!(staging.join(BOOTSTRAP_READY).is_file());
        assert!(staging.join("backup-restore.pending").is_file());
        assert_eq!(payloads(&scenario.target), before);
        assert_eq!(
            old.get("items", &scenario.old.id)
                .unwrap()
                .unwrap()
                .as_ref(),
            &scenario.old
        );
        old.close().unwrap();
        let before_install = payloads(&scenario.target);
        let error = finish_standby_bootstrap(&scenario.target).unwrap_err();
        eprintln!("ACTUAL_READY_WITH_PENDING_INSTALL_REFUSAL {error:?}");
        assert!(
            matches!(error, BicDbError::Replication(ref detail) if detail.contains("READY cannot authorize installation while backup-restore.pending remains"))
        );
        assert_eq!(payloads(&scenario.target), before_install);
        assert!(staging.join(BOOTSTRAP_READY).is_file());
        assert!(staging.join("backup-restore.pending").is_file());
        assert!(!sibling_path(&scenario.target, "replaced").exists());
        stage_standby_bootstrap(&scenario.target, &archive, KEY, true).unwrap();
        assert!(!staging.join("backup-restore.pending").exists());
        assert!(finish_standby_bootstrap(&scenario.target).unwrap());
        scenario.assert_installed(StorageMode::EmbeddedMemory);
    }
}

#[test]
fn actual_post_pending_unlink_error_needs_a_distinct_successful_fresh_ready_barrier() {
    for repository in [false, true] {
        let scenario = Scenario::new(StorageMode::EmbeddedMemory);
        let archive = scenario.archive(repository);
        let staging = standby_bootstrap_staging(&scenario.target);
        let fault = bicdb_page::test_faults::arm_io_failure(
            "backup.restore.pending.dir_sync",
            &staging,
            1,
            false,
        );
        let original_error =
            stage_standby_bootstrap(&scenario.target, &archive, KEY, true).unwrap_err();
        eprintln!("ACTUAL_POST_UNLINK_RESTORE_UNKNOWN {{repository:{repository}, original_error:{original_error:?}, fired:{}}}",fault.fired());
        assert!(matches!(original_error, BicDbError::Io(_)));
        assert_eq!(
            fault.fired(),
            1,
            "required actual post-unlink/pre-directory-sync cut"
        );
        drop(fault);
        assert!(staging.join(BOOTSTRAP_READY).is_file());
        assert!(!staging.join("backup-restore.pending").exists());
        let old_before = payloads(&scenario.target);
        let staged_before = payloads(&staging);
        let barrier_fault =
            bicdb_page::test_faults::arm_io_failure("bootstrap.ready.dir_sync", &staging, 1, false);
        let new_error = finish_standby_bootstrap(&scenario.target).unwrap_err();
        eprintln!("ACTUAL_NEW_READY_BARRIER_REFUSAL {{original_restore_unknown:true,new_error:{new_error:?},fired:{}}}",barrier_fault.fired());
        assert!(
            matches!(new_error,BicDbError::WriteFailed { ref site, .. } if site=="bootstrap.ready.durability")
        );
        assert_eq!(
            barrier_fault.fired(),
            1,
            "required distinct fresh directory barrier cut"
        );
        drop(barrier_fault);
        assert_eq!(payloads(&scenario.target), old_before);
        assert_eq!(payloads(&staging), staged_before);
        assert!(!sibling_path(&scenario.target, "replaced").exists());
        let fresh_result = finish_standby_bootstrap(&scenario.target);
        eprintln!("ACTUAL_NEW_READY_BARRIER_AND_INSTALL_RESULT {{original_restore_unknown:true,fresh_result:{fresh_result:?}}}");
        assert!(fresh_result.unwrap());
        scenario.assert_installed(StorageMode::EmbeddedMemory);
    }
}

#[test]
fn actual_page_owner_surviving_core_drop_refuses_bootstrap_without_losing_its_payload() {
    let scenario = Scenario::new(StorageMode::ServerPaged);
    let archive = scenario.archive(false);
    let old = BicDb::open_with_config(
        &scenario.target,
        DbConfig::default()
            .with_storage_mode(StorageMode::ServerPaged)
            .with_paged_buffer_pool_bytes(1024 * 1024),
    )
    .unwrap();
    assert_eq!(
        old.get("items", &scenario.old.id)
            .unwrap()
            .unwrap()
            .as_ref(),
        &scenario.old
    );
    let page = Arc::clone(old.paged_records.as_ref().unwrap().store());
    drop(old);
    stage_standby_bootstrap(&scenario.target, &archive, KEY, true).unwrap();
    let before = payloads(&scenario.target);
    let error = finish_standby_bootstrap(&scenario.target).unwrap_err();
    eprintln!("ACTUAL_RETAINED_CHILD_BOOTSTRAP_REFUSAL {error:?}");
    assert!(
        matches!(error, BicDbError::Replication(ref detail) if detail.contains("bootstrap Page owner remains live"))
    );
    assert_eq!(payloads(&scenario.target), before);
    let xid = page.begin();
    page.put(
        xid,
        b"actual-child-owner",
        b"full Page payload after refusal",
    )
    .unwrap();
    page.commit(xid).unwrap();
    assert_eq!(
        page.get(b"actual-child-owner").unwrap(),
        Some(b"full Page payload after refusal".to_vec())
    );
    drop(page);
    assert!(finish_standby_bootstrap(&scenario.target).unwrap());
    scenario.assert_installed(StorageMode::ServerPaged);
}

#[test]
fn incomplete_and_replaced_cleanup_refuse_real_live_owners_before_removal() {
    let scenario = Scenario::new(StorageMode::EmbeddedMemory);
    let staging = standby_bootstrap_staging(&scenario.target);
    fs::create_dir_all(staging.join("paged")).unwrap();
    let (page, _) = bicdb_page::PagedStore::open(
        staging.join("paged"),
        bicdb_page::PagedStoreOptions::default().with_buffer_pool_bytes(1024 * 1024),
    )
    .unwrap();
    let xid = page.begin();
    page.put(
        xid,
        b"incomplete-owned",
        b"full payload retained by actual child",
    )
    .unwrap();
    page.commit(xid).unwrap();
    let old_before = payloads(&scenario.target);
    let staged_before = payloads(&staging);
    let error = finish_standby_bootstrap(&scenario.target).unwrap_err();
    assert!(
        matches!(error,BicDbError::Replication(ref detail) if detail.contains("bootstrap Page owner remains live"))
    );
    assert_eq!(payloads(&scenario.target), old_before);
    assert_eq!(payloads(&staging), staged_before);
    assert_eq!(
        page.get(b"incomplete-owned").unwrap(),
        Some(b"full payload retained by actual child".to_vec())
    );
    drop(page);
    assert!(!finish_standby_bootstrap(&scenario.target).unwrap());
    assert!(!staging.exists());

    let archive = scenario.archive(false);
    stage_standby_bootstrap(&scenario.target, &archive, KEY, true).unwrap();
    let replaced = sibling_path(&scenario.target, "replaced");
    let mut retained = BicDb::open(&replaced).unwrap();
    retained.create_collection("retained").unwrap();
    let record = Record::new("real-old-cleanup-owner")
        .with_payload(b"complete retained cleanup payload".to_vec());
    retained.insert("retained", record.clone()).unwrap();
    let old_before = payloads(&scenario.target);
    let staged_before = payloads(&staging);
    let replaced_before = payloads(&replaced);
    let error = finish_standby_bootstrap(&scenario.target).unwrap_err();
    eprintln!("ACTUAL_REPLACED_OWNER_CLEANUP_REFUSAL {error:?}");
    assert!(
        matches!(error,BicDbError::Replication(ref detail) if detail.contains("cannot mutate bootstrap tree over an open database"))
    );
    assert_eq!(payloads(&scenario.target), old_before);
    assert_eq!(payloads(&staging), staged_before);
    assert_eq!(payloads(&replaced), replaced_before);
    assert_eq!(
        retained
            .get("retained", &record.id)
            .unwrap()
            .unwrap()
            .as_ref(),
        &record
    );
    retained.close().unwrap();
    assert!(finish_standby_bootstrap(&scenario.target).unwrap());
    scenario.assert_installed(StorageMode::EmbeddedMemory);
}
