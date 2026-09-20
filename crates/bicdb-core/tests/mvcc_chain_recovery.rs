use bicdb_core::{BicDb, BicDbError, DbConfig, Record, StorageMode, TupleLocator};
use serde_json::json;

fn paged_config() -> DbConfig {
    DbConfig::default()
        .with_fsync(false)
        .with_storage_mode(StorageMode::ServerPaged)
}

#[test]
fn public_chain_diagnostics_verify_healthy_paged_records_and_refuse_rewrites() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = BicDb::open_with_config(dir.path(), paged_config()).unwrap();
    db.create_collection("pubmed_state").unwrap();
    db.insert(
        "pubmed_state",
        Record::new("current").with_metadata(json!({
            "last_published_update": 1336,
        })),
    )
    .unwrap();

    let inspection = db
        .inspect_paged_record_chain("pubmed_state", "current")
        .unwrap();
    assert!(inspection.healthy());
    let head = inspection.head.expect("inserted record has no chain head");
    assert_eq!(inspection.versions_examined, 1);

    let integrity = db.verify_paged_storage_integrity(16).unwrap();
    assert!(integrity.valid);
    assert_eq!(integrity.version_chains.cycles, 0);
    assert_eq!(integrity.version_chains.limit_exceeded, 0);
    let standard_integrity = db.verify_integrity().unwrap();
    assert!(
        standard_integrity
            .paged_storage
            .as_ref()
            .is_some_and(|paged| paged.valid),
        "the standard integrity check omitted paged MVCC verification"
    );

    let replacement = Record::new("current").with_metadata(json!({
        "last_published_update": 9999,
    }));
    let error = db
        .repair_paged_record_chain("pubmed_state", "current", head, &replacement)
        .unwrap_err();
    assert!(matches!(error, BicDbError::VersionChain(_)));
    assert!(
        error
            .to_string()
            .contains("not a cycle or safety-limit fault"),
        "healthy-chain refusal was not actionable: {error}"
    );

    let changed = db
        .repair_paged_record_chain(
            "pubmed_state",
            "current",
            TupleLocator::new(head.page_id, head.slot, head.generation.saturating_add(1)),
            &replacement,
        )
        .unwrap_err();
    assert!(matches!(changed, BicDbError::VersionChain(_)));
    assert!(
        changed.to_string().contains("expected head"),
        "head compare-and-swap failure was not actionable: {changed}"
    );

    let current = db.get("pubmed_state", "current").unwrap().unwrap();
    assert_eq!(current.metadata["last_published_update"], json!(1336));
}
