//! Actual native floor versus deliberately rolled-back logical metadata.
//! The copied partial restore is a NEGATIVE corruption fixture, not a legal
//! restore/election protocol or physical durability qualification.
use super::*;
use crate::distribution_consensus::{MetadataReadParticipant, MetadataRecoveryRolePurpose};
use crate::distribution_metadata_read_provider::MetadataReadProvider;
use crate::distribution_range_consensus::{
    native_origin_fixture, seed_actual_origin, DEFAULT_RANGE_WRITE_LOG,
};
use crate::distribution_range_recovery::{
    test_recovery_metadata_cluster, ClosedNativeReadyQuorumRound, NativeReadyObservation,
    NativeReadyRequest,
};
use crate::{CancellationToken, ClusterTopology, StorageMode};
use std::time::{Duration, Instant};

fn role(
    providers: &BTreeMap<ClusterNodeId, Arc<MetadataReadProvider>>,
    actual: &MetadataReadParticipant,
    topology: &ClusterTopology,
    range_id: RangeId,
    nonce: Uuid,
) -> RecoveryRoleAdmission {
    let range = topology.range_by_id(range_id).unwrap();
    let business = Uuid::now_v7();
    let challenge = Uuid::now_v7();
    let purpose = MetadataRecoveryRolePurpose::NormalRecovery;
    let parent = CancellationToken::uncancelable()
        .child_with_deadline(Instant::now() + Duration::from_secs(5));
    let witness = providers[actual.node_id()]
        .witness_recovery_role(
            actual,
            business,
            range.id,
            range.epoch,
            nonce,
            challenge,
            purpose.clone(),
            &parent,
        )
        .unwrap();
    RecoveryRoleAdmission::from_witness(
        witness,
        actual,
        actual.node_id(),
        business,
        range.id,
        range.epoch,
        nonce,
        challenge,
        &purpose,
    )
    .unwrap()
}

fn copy_tree(source: &Path, destination: &Path) {
    std::fs::create_dir_all(destination).unwrap();
    for entry in std::fs::read_dir(source).unwrap() {
        let entry = entry.unwrap();
        let kind = entry.file_type().unwrap();
        assert!(
            !kind.is_symlink(),
            "owned native fixture has no external aliases"
        );
        let target = destination.join(entry.file_name());
        if kind.is_dir() {
            copy_tree(&entry.path(), &target);
        } else {
            std::fs::copy(entry.path(), &target).unwrap();
        }
    }
}

#[test]
fn actual_native_floor_vetoes_lower_and_same_epoch_drift_after_copied_logical_rollback() {
    let (root, db, topology, spec) = native_origin_fixture();
    let range = topology.ranges.values().next().unwrap().clone();
    seed_actual_origin(&db, topology.clone(), &spec, range.id, 1);
    let acknowledged_key = (0..)
        .map(|index| format!("stage-actual-origin-{index}"))
        .find(|key| topology.range_for_key("items", key).unwrap().id == range.id)
        .unwrap();
    let acknowledged_record = db.read().get("items", &acknowledged_key).unwrap().unwrap();
    let path = db.read().data_path().to_path_buf();
    let authentic_old_catalogue = std::fs::read(path.join(DEFAULT_RANGE_WRITE_LOG)).unwrap();
    let mut higher = topology.clone();
    higher
        .ranges
        .values_mut()
        .find(|r| r.id == range.id)
        .unwrap()
        .epoch = range.epoch + 1;
    higher.validate().unwrap();
    // Declared higher-metadata topology precondition, with REAL metadata
    // consensus/probes and REAL fsynced data transitions. Not a claim that the
    // online topology/election operation was exercised by this RF1 control.
    let providers = test_recovery_metadata_cluster(
        &root.path().join("higher-metadata"),
        &higher,
        &spec.node_id,
    )
    .unwrap();
    let peer = providers[&spec.node_id].local_participant();
    let nonce = Uuid::now_v7();
    let admitted = role(&providers, &peer, &higher, range.id, nonce);
    let scope = RecoveryFenceScope::from_role(&admitted).unwrap();
    let frozen = Arc::new(
        db.read()
            .range_mutation_gate()
            .try_freeze(range.id, nonce)
            .unwrap(),
    );
    let native = db
        .read()
        .native_recovery_admission(
            &spec.cluster_id,
            spec.bootstrap_id,
            &spec.node_id,
            spec.member_incarnation,
        )
        .unwrap();
    let mut store =
        RangeWriteStore::open(&path, spec.cluster_id.clone(), spec.node_id.clone(), true).unwrap();
    store
        .fence_recovery_epoch(&admitted, &native, &scope)
        .unwrap();
    let observed = store
        .observe_recovery_resolved_prefix(&admitted, &native, &scope)
        .unwrap();
    let local = db
        .read()
        .attest_local_native_recovery_ready(
            native,
            admitted,
            observed,
            &store,
            Arc::clone(&frozen),
            &CancellationToken::uncancelable(),
        )
        .unwrap();
    let prepared = store
        .prepare_native_recovery_ready(&db.read(), local)
        .unwrap();
    let fresh = role(&providers, &peer, &higher, range.id, nonce);
    let request = NativeReadyRequest {
        scope: RecoveryFenceScope::from_role(&fresh).unwrap(),
        challenge: Uuid::now_v7(),
        budget_ms: 5000,
    };
    let actual = store
        .observe_prepared_native_for_role(&db.read(), &prepared, &fresh, &request)
        .unwrap();
    let reply = NativeReadyObservation::from_local(
        actual,
        providers[&spec.node_id].local_participant(),
        &request,
        fresh.deadline(),
    )
    .unwrap();
    let mut round =
        ClosedNativeReadyQuorumRound::begin(fresh, &CancellationToken::uncancelable()).unwrap();
    round.record(reply).unwrap();
    let closed = round.finish().unwrap();
    let fresh = role(&providers, &peer, &higher, range.id, nonce);
    let receipt = db
        .read()
        .persist_native_recovered_serving_configuration(&prepared, &store, &closed, &fresh)
        .unwrap();
    let serving = db
        .read()
        .test_native_serving_record_json(range.id)
        .unwrap()
        .unwrap();
    assert_eq!(serving["range"]["epoch"], range.epoch + 1);
    let uid = db.read().local_namespace_identity("items").unwrap();
    drop(receipt);
    drop(closed);
    drop(prepared);
    drop(frozen);
    drop(store);
    let old_database = Arc::downgrade(&db);
    drop(db);
    assert_eq!(
        old_database.strong_count(),
        0,
        "no live native owner before copied rollback"
    );
    let diagnostic = root.path().join("deliberate-partial-restore-copy");
    copy_tree(&path, &diagnostic);
    // Restore the stopped store through the authorized producer, so the
    // copy opens and the serving floor, not directory custody, must veto.
    assert_eq!(
        crate::authorize_native_cold_restore(&path, &diagnostic).unwrap(),
        crate::NativeColdRestoreOutcome::Rebound {
            custody_generation: 1
        }
    );
    // Restore authentic EARLIER logical bytes in a COPY only. Native rows,
    // birth, native WAL, history and the genuine higher serving floor remain.
    std::fs::write(
        diagnostic.join(DEFAULT_RANGE_WRITE_LOG),
        &authentic_old_catalogue,
    )
    .unwrap();
    let (copied, _) = BicDb::open_range_image_database(
        &diagnostic,
        crate::DbConfig::default()
            .with_fsync(true)
            .with_storage_mode(StorageMode::ServerPaged)
            .with_paged_buffer_pool_bytes(8 * 1024 * 1024),
        &spec,
    )
    .unwrap();
    copied.schema_compatibility_fingerprint().unwrap();
    assert_eq!(
        copied.test_native_serving_record_json(range.id).unwrap(),
        Some(serving.clone())
    );
    assert_eq!(copied.local_namespace_identity("items").unwrap(), uid);
    assert_eq!(
        copied.get("items", &acknowledged_key).unwrap(),
        Some(acknowledged_record.clone())
    );
    let lower_providers = test_recovery_metadata_cluster(
        &root.path().join("older-metadata-negative"),
        &topology,
        &spec.node_id,
    )
    .unwrap();
    let lower_peer = lower_providers[&spec.node_id].local_participant();
    let lower = role(
        &lower_providers,
        &lower_peer,
        &topology,
        range.id,
        Uuid::now_v7(),
    );
    let scope_low = RecoveryFenceScope::from_role(&lower).unwrap();
    let frozen_low = Arc::new(
        copied
            .range_mutation_gate()
            .try_freeze(range.id, scope_low.recovery_nonce)
            .unwrap(),
    );
    let native_low = copied
        .native_recovery_admission(
            &spec.cluster_id,
            spec.bootstrap_id,
            &spec.node_id,
            spec.member_incarnation,
        )
        .unwrap();
    let mut rolled = RangeWriteStore::open(
        &diagnostic,
        spec.cluster_id.clone(),
        spec.node_id.clone(),
        true,
    )
    .unwrap();
    rolled
        .fence_recovery_epoch(&lower, &native_low, &scope_low)
        .unwrap();
    let observed_low = rolled
        .observe_recovery_resolved_prefix(&lower, &native_low, &scope_low)
        .unwrap();
    let refused = copied
        .attest_local_native_recovery_ready(
            native_low,
            lower,
            observed_low,
            &rolled,
            Arc::clone(&frozen_low),
            &CancellationToken::uncancelable(),
        )
        .unwrap_err();
    assert!(
        refused
            .to_string()
            .contains("below the durable serving floor"),
        "{refused}"
    );
    drop(frozen_low);

    // A separate valid metadata projection at the SAME higher epoch changes
    // placement policy. It is deliberately an unsupported semantic drift;
    // local native history cannot turn that projection into serving authority.
    let mut drift = higher.clone();
    drift
        .nodes
        .get_mut(&spec.node_id)
        .unwrap()
        .labels
        .insert("native-floor-test-policy".into(), "changed".into());
    drift
        .ranges
        .values_mut()
        .find(|r| r.id == range.id)
        .unwrap()
        .placement
        .required_node_labels
        .insert("native-floor-test-policy".into(), "changed".into());
    drift.validate().unwrap();
    let drift_providers = test_recovery_metadata_cluster(
        &root.path().join("same-epoch-drift-negative"),
        &drift,
        &spec.node_id,
    )
    .unwrap();
    let drift_peer = drift_providers[&spec.node_id].local_participant();
    let changed = role(
        &drift_providers,
        &drift_peer,
        &drift,
        range.id,
        Uuid::now_v7(),
    );
    let scope_changed = RecoveryFenceScope::from_role(&changed).unwrap();
    let frozen_changed = Arc::new(
        copied
            .range_mutation_gate()
            .try_freeze(range.id, scope_changed.recovery_nonce)
            .unwrap(),
    );
    let native_changed = copied
        .native_recovery_admission(
            &spec.cluster_id,
            spec.bootstrap_id,
            &spec.node_id,
            spec.member_incarnation,
        )
        .unwrap();
    rolled
        .fence_recovery_epoch(&changed, &native_changed, &scope_changed)
        .unwrap();
    let observed_changed = rolled
        .observe_recovery_resolved_prefix(&changed, &native_changed, &scope_changed)
        .unwrap();
    let refused_changed = copied
        .attest_local_native_recovery_ready(
            native_changed,
            changed,
            observed_changed,
            &rolled,
            frozen_changed,
            &CancellationToken::uncancelable(),
        )
        .unwrap_err();
    assert!(
        refused_changed
            .to_string()
            .contains("same-epoch semantic domain"),
        "{refused_changed}"
    );
    assert_eq!(
        copied.test_native_serving_record_json(range.id).unwrap(),
        Some(serving)
    );
    assert_eq!(copied.local_namespace_identity("items").unwrap(), uid);
    assert_eq!(
        copied.get("items", &acknowledged_key).unwrap(),
        Some(acknowledged_record)
    );
    eprintln!(
        "NATIVE_SERVING_FLOOR_NEGATIVES {}",
        serde_json::json!({"actual_floor_epoch":range.epoch+1,"lower_metadata_epoch":range.epoch,"native_range_index":1,"copied_authentic_logical_rollback":true,"lower_role_refused":true,"same_epoch_semantic_drift_refused":true,"physical_qualification":false})
    );
}
