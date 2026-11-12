//! Actual NEW-factory ordering and strict existing-catalogue custody. These
//! controls do not synthesize a policy, native history or fresh MQ grant.
use super::*;
use crate::distribution_range_consensus::FreshCatalogueTestCut;

#[test]
fn native_catalogue_custody_fresh_bootstrap_and_all_owner_normal_cold_preserve_bytes() {
    let (_root, db, spec, _) = fresh();
    let path = db.read().data_path().to_path_buf();
    let catalogue = path.join(DEFAULT_RANGE_WRITE_LOG);
    let before = std::fs::read(&catalogue).unwrap();
    let store =
        RangeWriteStore::open(&path, spec.cluster_id.clone(), spec.node_id.clone(), true).unwrap();
    {
        let actual = db.read();
        actual
            .require_native_writer_catalogue_custody(&store)
            .unwrap();
        assert!(actual
            .read_native_range_image_record::<NativeWriterPolicyRecord>(POLICY_POINTER)
            .unwrap()
            .is_none());
    }
    for range in &spec.initial_ranges {
        assert_eq!(store.progress(range.id).last_index, 0);
        assert_eq!(store.progress(range.id).resolved_through, 0);
    }
    let (birth, uid, schema) = {
        let actual = db.read();
        (
            actual.read_native_birth().unwrap().unwrap(),
            actual.local_namespace_identity("items").unwrap(),
            actual.schema_compatibility_fingerprint().unwrap().sha256,
        )
    };
    drop(store);
    drop(db);
    let (actual, _) = BicDb::open_range_image_database(&path, config(), &spec).unwrap();
    assert_eq!(actual.read_native_birth().unwrap().unwrap(), birth);
    assert_eq!(actual.local_namespace_identity("items").unwrap(), uid);
    assert_eq!(
        actual.schema_compatibility_fingerprint().unwrap().sha256,
        schema
    );
    let store = RangeWriteStore::open(&path, spec.cluster_id, spec.node_id, true).unwrap();
    actual
        .require_native_writer_catalogue_custody(&store)
        .unwrap();
    assert_eq!(std::fs::read(catalogue).unwrap(), before);
}

#[test]
fn native_catalogue_custody_missing_original_read_repeats_without_creating_blank() {
    let (_root, db, spec, _) = fresh();
    let path = db.read().data_path().to_path_buf();
    let catalogue = path.join(DEFAULT_RANGE_WRITE_LOG);
    let store =
        RangeWriteStore::open(&path, spec.cluster_id.clone(), spec.node_id.clone(), true).unwrap();
    // Even an actually verified live open must not reseal after custody loss.
    std::fs::remove_file(&catalogue).unwrap();
    assert!(matches!(
        db.read().require_native_writer_catalogue_custody(&store),
        Err(BicDbError::NativeRangeCatalogueCustodyMissing)
    ));
    for _ in 0..3 {
        assert!(matches!(
            RangeWriteStore::open(&path, spec.cluster_id.clone(), spec.node_id.clone(), true),
            Err(BicDbError::NativeRangeCatalogueCustodyMissing)
        ));
        assert!(!catalogue.exists());
        assert!(db
            .read()
            .read_native_range_image_record::<NativeWriterPolicyRecord>(POLICY_POINTER)
            .unwrap()
            .is_none());
    }
    // Existing no-policy/zero-index roots cannot obtain the private NEW token.
    assert!(BicDb::create_new_enrolled_range_image_database(&path, config(), spec).is_err());
    assert!(!catalogue.exists());
}

#[test]
fn native_catalogue_custody_creation_and_sync_cuts_never_publish_policy_or_reinitialize() {
    let (_old_root, old, _, spec) = native_origin_fixture();
    drop(old);
    for cut in [
        FreshCatalogueTestCut::BeforeWrite,
        FreshCatalogueTestCut::BeforeFileSync,
        FreshCatalogueTestCut::AfterDurable,
    ] {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("new");
        let result = RangeWriteStore::test_with_fresh_catalogue_cut(cut, || {
            BicDb::create_new_enrolled_range_image_database(&path, config(), spec.clone())
        });
        assert!(result.is_err());
        assert!(format::native_catalogue_custody_active(&path).unwrap());
        let catalogue = path.join(DEFAULT_RANGE_WRITE_LOG);
        if cut == FreshCatalogueTestCut::BeforeWrite {
            assert!(!catalogue.exists());
            for _ in 0..2 {
                assert!(matches!(
                    RangeWriteStore::open(
                        &path,
                        spec.cluster_id.clone(),
                        spec.node_id.clone(),
                        true
                    ),
                    Err(BicDbError::NativeRangeCatalogueCustodyMissing)
                ));
                assert!(!catalogue.exists());
            }
        } else {
            let before = std::fs::read(&catalogue).unwrap();
            let store =
                RangeWriteStore::open(&path, spec.cluster_id.clone(), spec.node_id.clone(), true)
                    .unwrap();
            assert!(store.require_native_writer_host_base().is_err());
            assert_eq!(std::fs::read(&catalogue).unwrap(), before);
        }
        // Inspection is not a completed genesis, metadata/Host grant or writer.
        let actual = BicDb::open_with_config(&path, config()).unwrap();
        assert!(actual
            .read_native_range_image_record::<NativeWriterPolicyRecord>(POLICY_POINTER)
            .unwrap()
            .is_none());
        assert!(actual
            .native_recovery_admission(
                &spec.cluster_id,
                spec.bootstrap_id,
                &spec.node_id,
                spec.member_incarnation
            )
            .is_err());
        drop(actual);
        assert!(
            BicDb::create_new_enrolled_range_image_database(&path, config(), spec.clone()).is_err()
        );
    }
}

#[test]
fn native_catalogue_custody_actual_reader_fence_is_local_and_old_known_flags_refuse() {
    let (_root, db, _, _) = fresh();
    let actual = db.read();
    let path = actual.data_path();
    let bytes = std::fs::read(path.join(format::FORMAT_METADATA_FILE)).unwrap();
    assert!(format::native_catalogue_custody_active(path).unwrap());
    let error = format::test_old_reader_without_catalogue_custody(path).unwrap_err();
    assert!(matches!(error, BicDbError::FormatCompatibility(_)));
    assert!(error
        .to_string()
        .contains(format::NATIVE_CATALOGUE_CUSTODY_FEATURE_FLAG));
    assert_eq!(
        std::fs::read(path.join(format::FORMAT_METADATA_FILE)).unwrap(),
        bytes
    );
    assert!(!format::required_reader_features(path)
        .unwrap()
        .iter()
        .any(|f| f == format::NATIVE_CATALOGUE_CUSTODY_FEATURE_FLAG));
}
