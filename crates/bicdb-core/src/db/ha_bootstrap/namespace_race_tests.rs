//! Genuine normal Core/Page opens at the actual namespace custody cuts.
//! No fabricated claim, readiness, native permit, or metadata setter.
use super::*;
use std::os::unix::fs::MetadataExt;
use std::sync::mpsc;

const KEY: &str = "synthetic-bootstrap-namespace-key";
struct Setup {
    primary: BicDb,
    target: PathBuf,
    source: Record,
    old: Record,
    // Last: actual database owners drop before their temporary directory.
    directory: tempfile::TempDir,
}
fn config() -> DbConfig {
    DbConfig::default()
        .with_storage_mode(StorageMode::EmbeddedMemory)
        .with_fsync(true)
        .with_sync_outbox(false)
}
fn populate(path: &Path, name: &str) -> (BicDb, Record) {
    let mut db = BicDb::open_with_config(path, config()).unwrap();
    db.create_collection("items").unwrap();
    let record = Record::new(name)
        .with_metadata(serde_json::json!({"full":true,"nullable":null}))
        .with_payload(format!("complete original {name} payload").into_bytes());
    db.insert("items", record.clone()).unwrap();
    (db, record)
}
impl Setup {
    fn new(old: bool) -> Self {
        let directory = tempfile::tempdir().unwrap();
        let target = directory.path().join("standby");
        let (primary, source) = populate(&directory.path().join("primary"), "source");
        let old_record = if old {
            let (db, record) = populate(&target, "old");
            db.close().unwrap();
            record
        } else {
            Record::new("not-created-old")
        };
        Self {
            primary,
            target,
            source,
            old: old_record,
            directory,
        }
    }
    fn stage(&self) {
        let archive = self.directory.path().join("snapshot.bicbackup");
        self.primary.create_standby_snapshot(&archive, KEY).unwrap();
        stage_standby_bootstrap(&self.target, &archive, KEY, true).unwrap();
    }
    fn installed(&self) {
        let db = BicDb::open_with_config(&self.target, config()).unwrap();
        assert_eq!(
            db.get("items", &self.source.id).unwrap().unwrap().as_ref(),
            &self.source
        );
        assert!(db.get("items", &self.old.id).unwrap().is_none());
    }
}
struct Releases(Vec<Option<mpsc::Sender<()>>>);
impl Releases {
    fn release(&mut self, index: usize) {
        if let Some(sender) = self.0[index].take() {
            let _ = sender.send(());
        }
    }
}
impl Drop for Releases {
    fn drop(&mut self) {
        for index in 0..self.0.len() {
            self.release(index);
        }
    }
}
fn selected(
    root: &Path,
    phase: &'static str,
    event: mpsc::Sender<serde_json::Value>,
    release: mpsc::Receiver<()>,
) -> impl Drop {
    namespace_cut::arm(
        root,
        phase,
        Box::new(move |data| {
            event
                .send(data)
                .expect("actual selected boundary observer disappeared");
            release
                .recv_timeout(Duration::from_secs(5))
                .expect("INCONCLUSIVE: actual boundary was not released");
        }),
    )
}
fn identity(root: &Path) -> (u64, u64) {
    let metadata = fs::metadata(root).unwrap();
    (metadata.dev(), metadata.ino())
}
fn full(db: &BicDb, record: &Record) {
    assert_eq!(
        db.get("items", &record.id).unwrap().unwrap().as_ref(),
        record
    );
}
fn payloads(root: &Path) -> BTreeMap<PathBuf, Vec<u8>> {
    fn collect(root: &Path, path: &Path, out: &mut BTreeMap<PathBuf, Vec<u8>>) {
        for entry in fs::read_dir(path).unwrap() {
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
fn normal_core_creation_after_original_absent_old_capture_is_never_moved_or_overwritten() {
    let setup = Setup::new(false);
    let root = setup.target.clone();
    let (open_event, open_seen) = mpsc::channel();
    let (open_release, open_wait) = mpsc::channel();
    let (install_event, install_seen) = mpsc::channel();
    let (install_release, install_wait) = mpsc::channel();
    std::thread::scope(|scope| {
        // Inside scope: release barriers before automatic joins on any panic.
        let mut releases = Releases(vec![Some(open_release), Some(install_release)]);
        let opener_root = root.clone();
        let opener = scope.spawn(move || {
            let _selected = selected(
                &opener_root,
                "after_core_bootstrap_check",
                open_event,
                open_wait,
            );
            populate(&opener_root, "actual-normal-opener")
        });
        let checked = open_seen.recv_timeout(Duration::from_secs(5)).unwrap();
        assert_eq!(checked["actual_initial_bootstrap_installed"], false);
        assert!(
            !root.exists(),
            "required actual opener cut must precede root creation"
        );
        setup.stage();
        let staging = standby_bootstrap_staging(&root);
        let staged_before = payloads(&staging);
        let installer_root = root.clone();
        let installer = scope.spawn(move || {
            let _selected = selected(
                &installer_root,
                "after_owner_capture",
                install_event,
                install_wait,
            );
            finish_standby_bootstrap(&installer_root)
        });
        let captured = install_seen.recv_timeout(Duration::from_secs(5)).unwrap();
        assert_eq!(captured["original_old_claimed"], false);
        assert_eq!(captured["original_replaced_claimed"], false);
        releases.release(0);
        let (mut actual, record) = opener.join().unwrap();
        full(&actual, &record);
        let actual_identity = identity(&root);
        let before = payloads(&root);
        releases.release(1);
        let original_error = installer.join().unwrap().unwrap_err();
        let original_text = original_error.to_string();
        eprintln!("ACTUAL_BOOTSTRAP_UNOWNED_NEW_ROOT {{checked:{checked},captured:{captured},identity:{actual_identity:?},error:{original_error:?}}}");
        assert!(
            matches!(&original_error,BicDbError::WriteFailed {site,..} if site=="bootstrap.rename")
        );
        assert_eq!(identity(&root), actual_identity);
        assert_eq!(payloads(&root), before);
        assert_eq!(payloads(&staging), staged_before);
        assert!(!sibling_path(&root, "replaced").exists());
        full(&actual, &record);
        let after = Record::new("same-owner-after-refusal")
            .with_payload(b"full new normal owner payload".to_vec());
        actual.insert("items", after.clone()).unwrap();
        full(&actual, &after);
        assert_eq!(original_error.to_string(), original_text);
        actual.close().unwrap();
    });
}

#[test]
fn normal_core_creation_after_absent_replaced_capture_prevents_old_rename_without_deletion() {
    let setup = Setup::new(true);
    setup.stage();
    let root = setup.target.clone();
    let replaced = sibling_path(&root, "replaced");
    let old_before = payloads(&root);
    let staged_before = payloads(&standby_bootstrap_staging(&root));
    let (event, seen) = mpsc::channel();
    let (release, wait) = mpsc::channel();
    std::thread::scope(|scope| {
        let mut releases = Releases(vec![Some(release)]);
        let worker_root = root.clone();
        let worker = scope.spawn(move || {
            let _selected = selected(&worker_root, "after_owner_capture", event, wait);
            finish_standby_bootstrap(&worker_root)
        });
        let captured = seen.recv_timeout(Duration::from_secs(5)).unwrap();
        assert_eq!(captured["original_old_claimed"], true);
        assert_eq!(captured["original_replaced_claimed"], false);
        let (mut actual, record) = populate(&replaced, "actual-new-replaced-owner");
        let actual_identity = identity(&replaced);
        let before = payloads(&replaced);
        releases.release(0);
        let original_error = worker.join().unwrap().unwrap_err();
        let original_text = original_error.to_string();
        eprintln!("ACTUAL_BOOTSTRAP_UNOWNED_REPLACED {{captured:{captured},identity:{actual_identity:?},error:{original_error:?}}}");
        assert!(
            matches!(&original_error,BicDbError::WriteFailed {site,..} if site=="bootstrap.rename")
        );
        assert_eq!(payloads(&root), old_before);
        assert_eq!(payloads(&standby_bootstrap_staging(&root)), staged_before);
        assert_eq!(identity(&replaced), actual_identity);
        assert_eq!(payloads(&replaced), before);
        full(&actual, &record);
        let after = Record::new("replaced-owner-after-refusal")
            .with_payload(b"full surviving replaced payload".to_vec());
        actual.insert("items", after.clone()).unwrap();
        full(&actual, &after);
        assert_eq!(original_error.to_string(), original_text);
        actual.close().unwrap();
    });
}

#[test]
fn completion_leaves_a_new_normal_replaced_owner_untouched_when_no_old_owner_moved() {
    let setup = Setup::new(false);
    setup.stage();
    let root = setup.target.clone();
    let replaced = sibling_path(&root, "replaced");
    let (event, seen) = mpsc::channel();
    let (release, wait) = mpsc::channel();
    std::thread::scope(|scope| {
        let mut releases = Releases(vec![Some(release)]);
        let worker_root = root.clone();
        let worker = scope.spawn(move || {
            let _selected = selected(&worker_root, "after_install_rename", event, wait);
            finish_standby_bootstrap(&worker_root)
        });
        let captured = seen.recv_timeout(Duration::from_secs(5)).unwrap();
        assert_eq!(captured["original_old_claimed"], false);
        assert_eq!(captured["original_replaced_claimed"], false);
        assert_eq!(captured["actual_old_moved_to_replaced"], false);
        let (mut actual, record) = populate(&replaced, "actual-completion-tail-owner");
        let actual_identity = identity(&replaced);
        let before = payloads(&replaced);
        releases.release(0);
        let result = worker.join().unwrap();
        eprintln!("ACTUAL_BOOTSTRAP_COMPLETION_UNOWNED_REPLACED {{captured:{captured},result:{result:?},identity:{actual_identity:?}}}");
        assert!(result.unwrap());
        assert_eq!(identity(&replaced), actual_identity);
        assert_eq!(payloads(&replaced), before);
        full(&actual, &record);
        let after = Record::new("completion-owner-continues")
            .with_payload(b"full completion owner payload".to_vec());
        actual.insert("items", after.clone()).unwrap();
        full(&actual, &after);
        setup.installed();
        actual.close().unwrap();
    });
}

#[test]
fn actual_page_creation_before_missing_child_claim_refuses_cleanup_and_preserves_live_owner() {
    let setup = Setup::new(true);
    setup.stage();
    let root = setup.target.clone();
    let paged = root.join(DEFAULT_PAGED_DIR);
    assert!(
        !paged.exists(),
        "required genuine Memory root has no conventional Page child"
    );
    let old_before = payloads(&root);
    let staged_before = payloads(&standby_bootstrap_staging(&root));
    let (event, seen) = mpsc::channel();
    let (release, wait) = mpsc::channel();
    std::thread::scope(|scope| {
        let mut releases = Releases(vec![Some(release)]);
        let worker_root = root.clone();
        let worker = scope.spawn(move || {
            let _selected = selected(&worker_root, "before_page_claim", event, wait);
            finish_standby_bootstrap(&worker_root)
        });
        let captured = seen.recv_timeout(Duration::from_secs(5)).unwrap();
        assert_eq!(captured["actual_root_claimed"], true);
        assert_eq!(captured["actual_root_SH_acquired"], true);
        let (page, _) = bicdb_page::PagedStore::open(
            &paged,
            bicdb_page::PagedStoreOptions::default().with_buffer_pool_bytes(1024 * 1024),
        )
        .unwrap();
        let xid = page.begin();
        page.put(
            xid,
            b"actual-page-owner",
            b"full actual Page creation payload",
        )
        .unwrap();
        page.commit(xid).unwrap();
        let page_before = payloads(&paged);
        releases.release(0);
        let original_error = worker.join().unwrap().unwrap_err();
        eprintln!("ACTUAL_BOOTSTRAP_PAGE_CREATION_REFUSAL {{captured:{captured},error:{original_error:?}}}");
        assert!(
            matches!(original_error,BicDbError::Replication(ref detail) if detail.contains("bootstrap Page owner remains live"))
        );
        assert_eq!(payloads(&paged), page_before);
        let mut without_child = payloads(&root);
        without_child.retain(|p, _| !p.starts_with(DEFAULT_PAGED_DIR));
        assert_eq!(without_child, old_before);
        assert_eq!(payloads(&standby_bootstrap_staging(&root)), staged_before);
        let xid = page.begin();
        page.put(
            xid,
            b"after-refusal",
            b"same full original Page owner continues",
        )
        .unwrap();
        page.commit(xid).unwrap();
        assert_eq!(
            page.get(b"actual-page-owner").unwrap(),
            Some(b"full actual Page creation payload".to_vec())
        );
        assert_eq!(
            page.get(b"after-refusal").unwrap(),
            Some(b"same full original Page owner continues".to_vec())
        );
        drop(page);
    });
}

#[test]
fn actual_missing_child_claim_prevents_later_standalone_page_open_until_install_finishes() {
    let setup = Setup::new(true);
    setup.stage();
    let root = setup.target.clone();
    let paged = root.join(DEFAULT_PAGED_DIR);
    assert!(!paged.exists());
    let (event, seen) = mpsc::channel();
    let (release, wait) = mpsc::channel();
    std::thread::scope(|scope| {
        let mut releases = Releases(vec![Some(release)]);
        let worker_root = root.clone();
        let worker = scope.spawn(move || {
            let _selected = selected(&worker_root, "after_owner_capture", event, wait);
            finish_standby_bootstrap(&worker_root)
        });
        let captured = seen.recv_timeout(Duration::from_secs(5)).unwrap();
        assert_eq!(captured["original_old_claimed"], true);
        let refusal = bicdb_page::PagedStore::open(
            &paged,
            bicdb_page::PagedStoreOptions::default().with_buffer_pool_bytes(1024 * 1024),
        )
        .unwrap_err();
        eprintln!("ACTUAL_BOOTSTRAP_CAPTURED_CHILD_OPEN_REFUSAL {refusal:?}");
        assert!(matches!(refusal, bicdb_page::PageError::AlreadyOpen { .. }));
        assert!(!paged.join("store.pages").exists());
        releases.release(0);
        assert!(worker.join().unwrap().unwrap());
        setup.installed();
    });
}
