//! Full-prototype union: protected Pending15 is the ONE catalogue owner.
//! JSON reads below are diagnostic assertions; only the actual DB/journal
//! initializer can consume the original protected Pending capability.
use super::*;

const IDENTITY: &str = "abort-range-catalogue:identity";
fn pending(db: &BicDb) -> serde_json::Value {
    db.read_native_range_image_record::<serde_json::Value>(IDENTITY)
        .unwrap()
        .unwrap()
}

#[test]
fn native_catalogue_custody_abort15_pending_all_owner_continuation_keeps_original_uuid() {
    let (_old_root, old, _, spec) = native_origin_fixture();
    drop(old);
    for cut in [
        FreshCatalogueTestCut::BeforeWrite,
        FreshCatalogueTestCut::BeforeFileSync,
        FreshCatalogueTestCut::AfterDurable,
    ] {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("pending");
        assert!(RangeWriteStore::test_with_fresh_catalogue_cut(cut, || {
            BicDb::create_new_enrolled_range_image_database(&path, config(), spec.clone())
        })
        .is_err());
        let (mut db, _) = BicDb::open_range_image_database(&path, config(), &spec).unwrap();
        let original = pending(&db);
        assert_eq!(original["registered"], false);
        let original_id: Uuid = serde_json::from_value(original["store_id"].clone()).unwrap();
        assert!(!original_id.is_nil());
        assert!(db.native_abort_catalogue_store_id().is_err());
        assert!(db
            .read_native_range_image_record::<NativeWriterPolicyRecord>(POLICY_POINTER)
            .unwrap()
            .is_none());
        let birth = db.read_native_birth().unwrap().unwrap();
        // This is original initial schema creation, before any READY or policy;
        // no existing namespace or business data is reset/imported.
        db.create_collection("items").unwrap();
        let uid = db.local_namespace_identity("items").unwrap();
        drop(db); // ALL actual DB/initializer owners end before strict reopen.
        let (db, factory) = BicDb::open_range_image_database(&path, config(), &spec).unwrap();
        assert_eq!(db.read_native_birth().unwrap().unwrap(), birth);
        assert_eq!(db.local_namespace_identity("items").unwrap(), uid);
        assert_eq!(
            pending(&db),
            original,
            "strict reopen never relabels Pending as Registered"
        );
        db.schema_compatibility_fingerprint().unwrap();
        let db = Arc::new(RwLock::new(db));
        for range in &spec.initial_ranges {
            let gate = db.read().range_mutation_gate();
            let frozen = gate.try_freeze(range.id, Uuid::now_v7()).unwrap();
            BicDb::initialize_range_image_genesis(
                &db,
                &factory,
                range,
                &frozen,
                Duration::from_secs(3),
                &CancellationToken::uncancelable(),
            )
            .unwrap();
        }
        assert_eq!(
            db.read().native_abort_catalogue_store_id().unwrap(),
            Some(original_id)
        );
        let registered = pending(&db.read());
        assert_eq!(registered["registered"], true);
        assert_eq!(registered["store_id"], original["store_id"]);
        let bytes = std::fs::read(path.join(DEFAULT_RANGE_WRITE_LOG)).unwrap();
        let raw: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(raw["catalog"]["format_version"], 15);
        assert_eq!(
            raw["catalog"]["native_abort_protocol"]["store_id"],
            original["store_id"]
        );
        assert!(raw["catalog"]["ranges"].as_object().unwrap().is_empty());
        drop(db);
        let (db, _) = BicDb::open_range_image_database(&path, config(), &spec).unwrap();
        assert_eq!(
            db.native_abort_catalogue_store_id().unwrap(),
            Some(original_id)
        );
        assert_eq!(db.local_namespace_identity("items").unwrap(), uid);
        assert_eq!(
            std::fs::read(path.join(DEFAULT_RANGE_WRITE_LOG)).unwrap(),
            bytes
        );
    }
}

#[test]
fn native_catalogue_custody_abort15_pending_never_adopts_protocol_none_empty_catalogue() {
    let (_old_root, old, _, spec) = native_origin_fixture();
    drop(old);
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("pending");
    assert!(RangeWriteStore::test_with_fresh_catalogue_cut(
        FreshCatalogueTestCut::BeforeWrite,
        || BicDb::create_new_enrolled_range_image_database(&path, config(), spec.clone())
    )
    .is_err());
    let (db, _) = BicDb::open_range_image_database(&path, config(), &spec).unwrap();
    let original = pending(&db);
    let birth = db.read_native_birth().unwrap().unwrap();
    let other = root.path().join("actual-read-profile");
    let (read_profile, _) = BicDb::create_new_range_image_database_with_mode(
        &other,
        config(),
        spec.clone(),
        true,
        true,
        false,
        |_| Ok(()),
    )
    .unwrap();
    let empty = std::fs::read(other.join(DEFAULT_RANGE_WRITE_LOG)).unwrap();
    let raw: serde_json::Value = serde_json::from_slice(&empty).unwrap();
    assert_eq!(raw["catalog"]["format_version"], 12);
    assert!(raw["catalog"].get("native_abort_protocol").is_none());
    drop(read_profile);
    // An actual old/read-profile empty envelope is DATA, not Pending authority.
    crate::storage::write_atomic(&path.join(DEFAULT_RANGE_WRITE_LOG), &empty, true).unwrap();
    assert!(db.initialize_native_abort_catalogue(&birth).is_err());
    assert_eq!(
        std::fs::read(path.join(DEFAULT_RANGE_WRITE_LOG)).unwrap(),
        empty
    );
    assert_eq!(pending(&db), original);
    assert!(db
        .read_native_range_image_record::<NativeInitializationComplete>(READY_KEY)
        .unwrap()
        .is_none());
    assert!(db
        .read_native_range_image_record::<NativeWriterPolicyRecord>(POLICY_POINTER)
        .unwrap()
        .is_none());
}
