//! Raw-cache resource/semantic controls, not an isolation or performance proof.
use super::*;

fn database() -> (tempfile::TempDir, BicDb) {
    let root = tempfile::tempdir().unwrap();
    let mut db =
        BicDb::open_with_config(root.path(), DbConfig::default().with_fsync(false)).unwrap();
    db.create_collection("roots").unwrap();
    for (id, tenant) in [("untouched", "old-tenant"), ("current", "new-tenant")] {
        db.insert(
            "roots",
            Record::new(id).with_metadata(serde_json::json!({"tenant":tenant})),
        )
        .unwrap();
    }
    (root, db)
}

#[test]
fn raw_invariant_memo_preserves_all_roots_and_original_transaction_overlay() {
    let (_root, db) = database();
    let mut tx = db.begin_transaction().unwrap();
    tx.delete("roots", "current").unwrap();
    tx.insert(
        "roots",
        Record::new("new").with_metadata(serde_json::json!({"tenant":"third"})),
    )
    .unwrap();
    // Exercise the cache under an actual stable admission. These readonly
    // tests do not attach this raw helper as a standalone writer capability.
    tx.isolation = TransactionIsolation::Serializable;
    let _exclusive = db.serializable_commit_admission.write();
    let snapshot = NativeInvariantRelationSnapshot::new(&db, &tx, Some(&_exclusive));
    let first = snapshot.records("roots").unwrap();
    let second = snapshot.records("roots").unwrap();
    assert!(Arc::ptr_eq(&first, &second));
    assert_eq!(
        first.iter().map(|r| r.id.as_str()).collect::<Vec<_>>(),
        ["new", "untouched"]
    );
    assert_eq!(first[1].metadata["tenant"], "old-tenant");
}

#[test]
fn raw_invariant_memo_cap_falls_back_without_truncation_or_new_memory_refusal() {
    let (_root, db) = database();
    let mut tx = db.begin_transaction().unwrap();
    tx.isolation = TransactionIsolation::Serializable;
    let _exclusive = db.serializable_commit_admission.write();
    let _memory = crate::query_budget::QueryMemoryScope::new(1024 * 1024);
    let mut snapshot = NativeInvariantRelationSnapshot::new(&db, &tx, Some(&_exclusive));
    snapshot.cache_limit = 1;
    let first = snapshot.records("roots").unwrap();
    let second = snapshot.records("roots").unwrap();
    assert_eq!(first.len(), 2);
    assert_eq!(first.as_ref(), second.as_ref());
    assert!(!Arc::ptr_eq(&first, &second));
    assert!(snapshot.relations.borrow().is_empty());
}

#[test]
fn raw_invariant_memo_combined_cap_falls_back_and_counts_each_arc_body_once() {
    let (_root, mut db) = database();
    db.create_collection("children").unwrap();
    db.insert("children", Record::new("child")).unwrap();
    let mut tx = db.begin_transaction().unwrap();
    tx.isolation = TransactionIsolation::Serializable;
    let _exclusive = db.serializable_commit_admission.write();
    let mut snapshot = NativeInvariantRelationSnapshot::new(&db, &tx, Some(&_exclusive));
    let root_bytes = resulting_records_for_relation(&db, &tx, "roots")
        .unwrap()
        .iter()
        .map(|r| crate::query_budget::materialized_record_bytes(r).unwrap())
        .sum::<usize>();
    let child_bytes = resulting_records_for_relation(&db, &tx, "children")
        .unwrap()
        .iter()
        .map(|r| crate::query_budget::materialized_record_bytes(r).unwrap())
        .sum::<usize>();
    snapshot.cache_limit = root_bytes + child_bytes - 1;
    let roots = snapshot.records("roots").unwrap();
    let same_roots = snapshot.records("roots").unwrap();
    assert!(Arc::ptr_eq(&roots, &same_roots));
    let child = snapshot.records("children").unwrap();
    let uncached_child = snapshot.records("children").unwrap();
    assert_eq!(child.as_ref(), uncached_child.as_ref());
    assert!(!Arc::ptr_eq(&child, &uncached_child));
    assert_eq!(snapshot.relations.borrow().len(), 1);
    // Releasing the callers allows the cache-only roots to be evicted before
    // another scan; no live borrowed root record can outlast its owning Arc.
    drop(roots);
    drop(same_roots);
    let cached_child = snapshot.records("children").unwrap();
    assert!(Arc::ptr_eq(
        &cached_child,
        &snapshot.records("children").unwrap()
    ));
    assert!(!snapshot.relations.borrow().contains_key("roots"));
}

#[test]
fn raw_invariant_memo_without_active_budget_still_has_bounded_retention() {
    let (_root, mut db) = database();
    let large = "x".repeat(2 * 1024 * 1024 + 1024);
    db.insert(
        "roots",
        Record::new("large").with_metadata(serde_json::json!({"value":large})),
    )
    .unwrap();
    let mut tx = db.begin_transaction().unwrap();
    tx.isolation = TransactionIsolation::Serializable;
    let _exclusive = db.serializable_commit_admission.write();
    assert!(!crate::query_budget::active());
    let snapshot = NativeInvariantRelationSnapshot::new(&db, &tx, Some(&_exclusive));
    assert_eq!(snapshot.cache_limit, 16 * 1024 * 1024);
    let all = snapshot.records("roots").unwrap();
    assert_eq!(all.len(), 3);
    assert!(snapshot.relations.borrow().is_empty());
    // The same full relation fits the caller's existing larger budget; the
    // smaller memo cap still falls back rather than raising a memory refusal.
    let _memory = crate::query_budget::QueryMemoryScope::new(64 * 1024 * 1024);
    let budgeted = NativeInvariantRelationSnapshot::new(&db, &tx, Some(&_exclusive));
    assert_eq!(budgeted.records("roots").unwrap().as_ref(), all.as_ref());
    assert!(budgeted.relations.borrow().is_empty());
}

#[test]
fn raw_invariant_memo_hit_preserves_expired_scope_and_existing_memory_failure() {
    let (_root, db) = database();
    let mut tx = db.begin_transaction().unwrap();
    tx.isolation = TransactionIsolation::Serializable;
    let _exclusive = db.serializable_commit_admission.write();
    let snapshot = NativeInvariantRelationSnapshot::new(&db, &tx, Some(&_exclusive));
    let _first = snapshot.records("roots").unwrap();
    {
        let _expired = crate::query_budget::QueryMemoryScope::with_deadline(
            1024 * 1024,
            std::time::Instant::now(),
        );
        assert!(matches!(
            snapshot.records("roots"),
            Err(BicDbError::QueryTimedOut)
        ));
    }
    {
        let _small = crate::query_budget::QueryMemoryScope::new(1);
        crate::query_budget::observe(2);
        assert!(matches!(
            snapshot.records("roots"),
            Err(BicDbError::QueryMemoryLimit(_))
        ));
    }
}

fn paged_database() -> (tempfile::TempDir, BicDb) {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open_with_config(
        root.path(),
        DbConfig::default()
            .with_fsync(false)
            .with_storage_mode(StorageMode::ServerPaged),
    )
    .unwrap();
    db.create_collection("roots").unwrap();
    db.insert("roots", Record::new("seed")).unwrap();
    (root, db)
}

#[test]
fn raw_invariant_escaped_writer_forces_fresh_complete_scans_and_never_resets() {
    let (_root, db) = paged_database();
    let raw = db.paged_records_handle().unwrap();
    let mut tx = db.begin_transaction().unwrap();
    tx.isolation = TransactionIsolation::Serializable;
    let exclusive = db.serializable_commit_admission.write();
    let snapshot = NativeInvariantRelationSnapshot::new(&db, &tx, Some(&exclusive));
    assert!(!snapshot.share_relations);
    let before = snapshot.records("roots").unwrap();
    {
        // The actual escaped low-level writer is not excluded by core
        // Serializable admission. It must make memo reuse ineligible.
        let _write = raw.write_guard();
        let (xid, _) = raw.begin();
        raw.put(xid, "roots", &Record::new("raw-added")).unwrap();
        raw.commit(xid).unwrap();
    }
    let after = snapshot.records("roots").unwrap();
    assert_eq!(
        before.iter().map(|r| r.id.as_str()).collect::<Vec<_>>(),
        ["seed"]
    );
    assert_eq!(
        after.iter().map(|r| r.id.as_str()).collect::<Vec<_>>(),
        ["raw-added", "seed"]
    );
    assert!(!Arc::ptr_eq(&before, &after));
    assert!(snapshot.relations.borrow().is_empty());
    drop(snapshot);
    drop(raw);
    let later = NativeInvariantRelationSnapshot::new(&db, &tx, Some(&exclusive));
    assert!(
        !later.share_relations,
        "dropping a raw Arc cannot revoke its clones"
    );
}

#[test]
fn raw_invariant_active_memo_refuses_late_raw_issuance_without_waiting() {
    let (_root, db) = paged_database();
    let mut tx = db.begin_transaction().unwrap();
    tx.isolation = TransactionIsolation::Serializable;
    std::thread::scope(|scope| {
        // Construct the guard and its borrower in this lexical scope. Moving
        // a guard into a closure while capturing its outside borrower is
        // invalid; the production lifetime relationship stays unchanged.
        let exclusive = db.serializable_commit_admission.write();
        let snapshot = NativeInvariantRelationSnapshot::new(&db, &tx, Some(&exclusive));
        assert!(snapshot.share_relations);
        let first = snapshot.records("roots").unwrap();
        assert!(Arc::ptr_eq(&first, &snapshot.records("roots").unwrap()));
        let (send, receive) = std::sync::mpsc::channel();
        let database = &db;
        let issuer = scope.spawn(move || {
            send.send(database.paged_records_handle().is_some())
                .unwrap();
        });
        let observed = receive.recv_timeout(std::time::Duration::from_secs(1));
        // Always release before joining/asserting: a blocking regression must
        // fail this control rather than hang its test process indefinitely.
        drop(snapshot);
        drop(exclusive);
        issuer.join().unwrap();
        assert_eq!(observed, Ok(false));
    });
    assert!(
        db.paged_records_handle().is_some(),
        "issuance works after the pass"
    );
}

#[test]
fn raw_invariant_memo_requires_same_database_exclusive_and_serializable() {
    let (_root, db) = database();
    let (_other_root, other) = database();
    let mut tx = db.begin_transaction().unwrap();
    tx.isolation = TransactionIsolation::Serializable;
    assert!(!NativeInvariantRelationSnapshot::new(&db, &tx, None).share_relations);
    let foreign = other.serializable_commit_admission.write();
    assert!(!NativeInvariantRelationSnapshot::new(&db, &tx, Some(&foreign)).share_relations);
    drop(foreign);
    tx.isolation = TransactionIsolation::ReadCommitted;
    let exclusive = db.serializable_commit_admission.write();
    let snapshot = NativeInvariantRelationSnapshot::new(&db, &tx, Some(&exclusive));
    assert!(
        !snapshot.share_relations,
        "RC remains fresh even with a borrowed gate"
    );
    assert!(!Arc::ptr_eq(
        &snapshot.records("roots").unwrap(),
        &snapshot.records("roots").unwrap()
    ));
}

#[test]
fn raw_invariant_escaped_store_arc_blocks_fresh_open_until_last_writer_drops() {
    let (root, db) = paged_database();
    let raw = db.paged_records_handle().unwrap();
    let escaped_store = Arc::clone(raw.store());
    drop(raw);
    db.close().unwrap();
    let config = DbConfig::default()
        .with_fsync(false)
        .with_storage_mode(StorageMode::ServerPaged);
    let error = BicDb::open_with_config(root.path(), config.clone())
        .expect_err("an escaped low-level Store Arc still owns the actual writer directory lock");
    assert!(
        error.to_string().to_lowercase().contains("already open"),
        "{error}"
    );
    drop(escaped_store);
    let db = BicDb::open_with_config(root.path(), config).unwrap();
    let mut tx = db.begin_transaction().unwrap();
    tx.isolation = TransactionIsolation::Serializable;
    let exclusive = db.serializable_commit_admission.write();
    assert!(NativeInvariantRelationSnapshot::new(&db, &tx, Some(&exclusive)).share_relations);
}

#[derive(Debug)]
struct ReentrantRawGetter {
    database: std::sync::Weak<BicDb>,
    calls: std::sync::atomic::AtomicUsize,
    escaped: std::sync::atomic::AtomicBool,
}
impl CommitAdmission for ReentrantRawGetter {
    fn admit(&self, _: &CommitAdmissionIntent) -> Result<CommitAdmissionTicket> {
        // The ordinary actual admission path is invoked while the same
        // Serializable commit owns exclusive admission. No native birth flag
        // or fake enrolled writer is installed to force another callback.
        let db = self.database.upgrade().unwrap();
        self.calls.fetch_add(1, AtomicOrdering::SeqCst);
        self.escaped
            .store(db.paged_records_handle().is_some(), AtomicOrdering::SeqCst);
        Ok(CommitAdmissionTicket {
            authority: "raw-getter-control".into(),
            command_id: "same-commit".into(),
        })
    }
    fn local_applied(&self, _: &CommitAdmissionTicket, _: u64) -> Result<()> {
        Ok(())
    }
}

#[test]
fn raw_invariant_reentrant_admission_getter_refuses_without_deadlocking() {
    const CHILD: &str = "BICDB_RAW_GETTER_REENTRANT_CONTROL";
    if std::env::var_os(CHILD).is_some() {
        let (_root, db) = paged_database();
        let db = Arc::new(db);
        let hook = Arc::new(ReentrantRawGetter {
            database: Arc::downgrade(&db),
            calls: std::sync::atomic::AtomicUsize::new(0),
            escaped: std::sync::atomic::AtomicBool::new(false),
        });
        db.install_commit_admission(hook.clone());
        let mut tx = db.begin_transaction().unwrap();
        tx.isolation = TransactionIsolation::Serializable;
        tx.insert("roots", Record::new("hooked")).unwrap();
        tx.commit().unwrap();
        assert_eq!(hook.calls.load(AtomicOrdering::SeqCst), 1);
        assert!(!hook.escaped.load(AtomicOrdering::SeqCst));
        assert!(!db
            .paged_records
            .as_ref()
            .unwrap()
            .raw_row_capability_issued());
        return;
    }
    // Bound the actual reentrant commit in an owned child. A regression which
    // blocks on its own exclusive admission must not hang the parent suite.
    let logs = tempfile::tempdir().unwrap();
    let output = logs.path().join("child.log");
    let file = std::fs::File::create(&output).unwrap();
    let mut child = std::process::Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "db::invariant_relation_snapshot_tests::raw_invariant_reentrant_admission_getter_refuses_without_deadlocking", "--nocapture"])
        .env(CHILD, "1")
        .stdout(std::process::Stdio::from(file.try_clone().unwrap()))
        .stderr(std::process::Stdio::from(file))
        .spawn().unwrap();
    let started = std::time::Instant::now();
    loop {
        if let Some(status) = child.try_wait().unwrap() {
            let log = std::fs::read_to_string(&output).unwrap();
            assert!(status.success(), "actual reentrant child {status}: {log}");
            assert!(
                log.contains("1 passed; 0 failed"),
                "no actual child control ran: {log}"
            );
            break;
        }
        if started.elapsed() > std::time::Duration::from_secs(5) {
            let _ = child.kill();
            let _ = child.wait();
            panic!(
                "reentrant child exceeded 5s; owned child stopped: {}",
                std::fs::read_to_string(&output).unwrap()
            );
        }
        std::thread::sleep(std::time::Duration::from_millis(10));
    }
}
