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
    let snapshot = NativeInvariantRelationSnapshot::new(&db, &tx);
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
    let mut snapshot = NativeInvariantRelationSnapshot::new(&db, &tx);
    snapshot.cache_limit = 1;
    let first = snapshot.records("roots").unwrap();
    let second = snapshot.records("roots").unwrap();
    assert_eq!(first.len(), 2);
    assert_eq!(first.as_ref(), second.as_ref());
    assert!(!Arc::ptr_eq(&first, &second));
    assert!(snapshot.relations.borrow().is_empty());
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
    let snapshot = NativeInvariantRelationSnapshot::new(&db, &tx);
    assert_eq!(snapshot.cache_limit, 16 * 1024 * 1024);
    let all = snapshot.records("roots").unwrap();
    assert_eq!(all.len(), 3);
    assert!(snapshot.relations.borrow().is_empty());
    // The same full relation fits the caller's existing larger budget; the
    // smaller memo cap still falls back rather than raising a memory refusal.
    let _memory = crate::query_budget::QueryMemoryScope::new(64 * 1024 * 1024);
    let budgeted = NativeInvariantRelationSnapshot::new(&db, &tx);
    assert_eq!(budgeted.records("roots").unwrap().as_ref(), all.as_ref());
    assert!(budgeted.relations.borrow().is_empty());
}

#[test]
fn raw_invariant_memo_hit_preserves_expired_scope_and_existing_memory_failure() {
    let (_root, db) = database();
    let mut tx = db.begin_transaction().unwrap();
    tx.isolation = TransactionIsolation::Serializable;
    let _exclusive = db.serializable_commit_admission.write();
    let snapshot = NativeInvariantRelationSnapshot::new(&db, &tx);
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
