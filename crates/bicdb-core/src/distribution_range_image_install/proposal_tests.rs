//! Actual producer cut before catalogue publication. No synthetic source cut,
//! supplied voter set or raw counter establishes this orphan control.
use super::current_repair_tests::CurrentRepairFixture;
use super::*;
use std::time::Duration;
#[test]
fn actual_protected_proposal_orphan_preserves_native_point_and_reopens_without_intent() {
    let mut fixture = CurrentRepairFixture::new();
    fixture.origin.root.disable_cleanup(true);
    let target = crate::distribution::ClusterNodeId::new("n1").unwrap();
    let db = Arc::clone(&fixture.origin.databases[&target]);
    let before = db
        .read()
        .current_native_range_source_lineage(fixture.range.id)
        .unwrap();
    assert_eq!(before.range_index(), 0);
    let before_h = before.logical_history_sha256().to_string();
    let uid = db.read().local_namespace_identity("items").unwrap();
    let role = fixture.role();
    let source = fixture
        .capture
        .as_ref()
        .unwrap()
        .authorize_target_install()
        .unwrap();
    let attempt = Uuid::now_v7();
    let lease = fixture.runtimes[&target]
        .transfer_image_install(
            &fixture.scope,
            &source,
            &NativeImageInstallRole::CurrentVoterRepair(&role),
            ImageInstallPurpose::CurrentVoterRepair,
            attempt,
            &CancellationToken::uncancelable(),
        )
        .unwrap();
    let failure = fixture
        .service
        .lock()
        .test_current_image_catalogue_boundary(
            &source,
            &NativeImageInstallRole::CurrentVoterRepair(&role),
            lease.as_frozen(),
            attempt,
            true,
        )
        .unwrap_err();
    assert!(failure
        .to_string()
        .contains("before ANY catalogue intent publication"));
    let native = db
        .read()
        .native_recovery_admission(
            &fixture.origin.specs[&target].cluster_id,
            fixture.origin.specs[&target].bootstrap_id,
            &target,
            fixture.origin.specs[&target].member_incarnation,
        )
        .unwrap();
    let proposal = db
        .read()
        .read_native_current_image_proposal_by_id(&native, attempt)
        .unwrap()
        .unwrap();
    let pointer = proposal.pointer();
    assert_eq!(
        proposal
            .preimage()
            .native_point
            .as_ref()
            .unwrap()
            .range_index,
        0
    );
    assert_eq!(proposal.historical_source().source_range_index, 1);
    let after = db
        .read()
        .current_native_range_source_lineage(fixture.range.id)
        .unwrap();
    assert_eq!(after.range_index(), 0);
    assert_eq!(after.logical_history_sha256(), before_h);
    assert_eq!(db.read().local_namespace_identity("items").unwrap(), uid);
    assert!(crate::distribution_range_consensus::RangeWriteStore::require_no_native_image_install_on_disk(db.read().data_path(),fixture.range.id).is_ok());
    assert!(
        fixture
            .service
            .lock()
            .require_persisted_recovery_scope(&fixture.scope)
            .is_err(),
        "failed BEGIN retains conservative LIVE memory refusal"
    );
    let root = fixture.origin.root.path().to_path_buf();
    let spec = fixture.origin.specs[&target].clone();
    let range = fixture.range.id;
    let weak = fixture
        .origin
        .databases
        .values()
        .map(Arc::downgrade)
        .collect::<Vec<_>>();
    drop(after);
    drop(before);
    drop(native);
    drop(proposal);
    drop(source);
    drop(role);
    drop(lease);
    drop(db);
    drop(fixture);
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while weak.iter().any(|db| db.upgrade().is_some()) && std::time::Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(1));
    }
    assert!(weak.iter().all(|db| db.upgrade().is_none()));
    let mut copy = crate::db::ActualNativeRf3Fixture::copy_stopped_recovery_tree(&root).unwrap();
    copy.disable_cleanup(true);
    let path = copy.path().join(target.as_str());
    let (reopened, _) = BicDb::open_range_image_database(
        &path,
        crate::DbConfig::default()
            .with_fsync(true)
            .with_storage_mode(crate::StorageMode::ServerPaged)
            .with_paged_buffer_pool_bytes(8 * 1024 * 1024),
        &spec,
    )
    .unwrap();
    let native = reopened
        .native_recovery_admission(
            &spec.cluster_id,
            spec.bootstrap_id,
            &target,
            spec.member_incarnation,
        )
        .unwrap();
    let actual = reopened.current_native_range_source_lineage(range).unwrap();
    assert_eq!(actual.range_index(), 0);
    assert_eq!(actual.logical_history_sha256(), before_h);
    assert_eq!(reopened.local_namespace_identity("items").unwrap(), uid);
    let retained = reopened
        .read_native_current_image_proposal(&native, &pointer)
        .unwrap();
    assert_eq!(retained.pointer(), pointer);
    let store = RangeWriteStore::open(&path, spec.cluster_id.clone(), target, true).unwrap();
    store.require_no_image_install(range).unwrap();
    let report = serde_json::json!({"result":"ORPHAN_HISTORICAL_PROPOSAL_ONLY_NATIVE_POINT_PRESERVED",
        "original_root":root,"copied_root":copy.path(),"native_range_index":0,"native_history_sha256":before_h,
        "target_uid":uid,"protected_proposal":pointer,"published_intent_present":false,
        "fresh_authority_from_orphan":false,"native_ready_proven":false,"physical_qualification":false});
    std::fs::write(
        copy.path().join("orphan-proposal-result.json"),
        serde_json::to_vec_pretty(&report).unwrap(),
    )
    .unwrap();
    eprintln!("ORPHAN_PROPOSAL_RESULT {report}");
}
