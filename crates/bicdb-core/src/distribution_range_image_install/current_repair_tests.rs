//! Nonempty current-voter image repair using real RF3 origin acknowledgments,
//! real durable metadata witnesses, current-data-quorum fences and the same
//! live service-owned catalogues. In-process trusted peer identities are an
//! explicit test precondition; this is not TCP/TLS or physical qualification.
use super::*;
use crate::db::ActualNativeRf3Fixture;
use crate::distribution::{ClusterNodeId, RangeDescriptor};
use crate::distribution_consensus::{MetadataReadParticipant, MetadataRecoveryRolePurpose};
use crate::distribution_data::ClusterDataNodeService;
use crate::distribution_metadata_read_provider::MetadataReadProvider;
use crate::distribution_range_consensus::{RecoveryFenceScope, DEFAULT_RANGE_WRITE_LOG};
use crate::distribution_range_image::RangeImageCapture;
use crate::distribution_range_recovery::{
    test_recovery_metadata_cluster, RangeRecoveryRuntime, RecoveryEpochFenceRequest,
    RecoveryFenceQuorumRound, RecoveryRoleAdmission,
};
use parking_lot::Mutex;
use std::collections::BTreeMap;
use std::time::{Duration, Instant};

fn node(id: &str) -> ClusterNodeId {
    ClusterNodeId::new(id).unwrap()
}

struct CurrentRepairFixture {
    origin: ActualNativeRf3Fixture,
    providers: BTreeMap<ClusterNodeId, Arc<MetadataReadProvider>>,
    runtimes: BTreeMap<ClusterNodeId, Arc<RangeRecoveryRuntime>>,
    initiator: MetadataReadParticipant,
    range: RangeDescriptor,
    scope: RecoveryFenceScope,
    capture: Option<RangeImageCapture>,
    service: Arc<Mutex<ClusterDataNodeService>>,
    selected_key: String,
    other_keys: Vec<String>,
    source_b: u64,
    target_b_before: u64,
    original_target_uid: Uuid,
}
impl CurrentRepairFixture {
    fn fresh_role_for(
        providers: &BTreeMap<ClusterNodeId, Arc<MetadataReadProvider>>,
        initiator: &MetadataReadParticipant,
        recipient: &ClusterNodeId,
        range: &RangeDescriptor,
        nonce: Uuid,
    ) -> RecoveryRoleAdmission {
        let business = Uuid::now_v7();
        let challenge = Uuid::now_v7();
        let parent = CancellationToken::uncancelable()
            .child_with_deadline(Instant::now() + Duration::from_secs(5));
        let purpose = MetadataRecoveryRolePurpose::NormalRecovery;
        let witness = providers[recipient]
            .witness_recovery_role(
                initiator,
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
            initiator,
            recipient,
            business,
            range.id,
            range.epoch,
            nonce,
            challenge,
            &purpose,
        )
        .unwrap()
    }
    fn new() -> Self {
        let origin = ActualNativeRf3Fixture::new_with_initial_ranges(2);
        let topology = origin.topology.read().clone();
        let range = topology.ranges.values().next().unwrap().clone();
        let other = topology.ranges.values().find(|r| r.id != range.id).unwrap();
        let key = |prefix: &str, which: &RangeDescriptor| {
            (0..)
                .map(|i| format!("{prefix}-{i}"))
                .find(|key| which.contains_token(distribution_key_token("items", key)))
                .unwrap()
        };
        // Real RF3 origin ACKs on another range separate global DB sequence
        // from selected-range R=1. No counters or receipts are assigned.
        for i in 0..4 {
            origin
                .append(&key(&format!("source-other-{i}"), other))
                .unwrap();
        }
        let selected_key = key("current-repair-selected", &range);
        origin.append(&selected_key).unwrap();
        let source_b = origin.databases[&node("n0")].read().current_commit_seq();
        assert!(source_b > 1);
        let source = origin.databases[&node("n0")].read();
        assert_eq!(
            source
                .current_native_range_source_lineage(range.id)
                .unwrap()
                .range_index(),
            1
        );
        drop(source);
        let target = &origin.databases[&node("n1")];
        let original_target_uid = target
            .read()
            .local_namespace_identity("items")
            .unwrap()
            .unwrap();
        // The actual RF3 origin sent prepare/commit/certify to this recipient;
        // bare replica Apply is intentionally refused. It is genuinely behind.
        let probe = origin.services[&node("n1")]
            .lock()
            .probe_range_write_for_peer(&range, &node("n0"), 1)
            .unwrap();
        assert_eq!(
            probe.entry.unwrap().state,
            crate::distribution_range_consensus::RangeWriteState::QuorumCommitted
        );
        assert_eq!(probe.progress.resolved_through, 0);
        let original_target_lineage = target
            .read()
            .current_native_range_source_lineage(range.id)
            .unwrap();
        let original_target_receipt = original_target_lineage.native_receipt_sha256().to_string();
        let original_target_history = original_target_lineage.logical_history_sha256().to_string();
        assert_eq!(
            target
                .read()
                .current_native_range_source_lineage(range.id)
                .unwrap()
                .range_index(),
            0
        );
        assert!(target.read().get("items", &selected_key).unwrap().is_none());
        // A born current voter correctly refuses an ordinary user transaction
        // before range commit authority exists. Preserve this negative control.
        {
            let db = target.write();
            let mut tx = db.begin_transaction().unwrap();
            tx.insert(
                "items",
                crate::Record::new(key("refused-target-user", other)),
            )
            .unwrap();
            let error = tx.commit().unwrap_err();
            assert!(error
                .to_string()
                .contains("range-quorum commit authority is not ready"));
        }
        // Deliberately UNACKNOWLEDGED diagnostic physical imports use the
        // existing native range importer, as in the prior MVCC-bound control.
        // They are NOT authorized user/RF3 commits. Each real local import
        // transaction advances target B and creates other-range sentinels.
        let other_keys = (0..17)
            .map(|i| key(&format!("target-local-other-{i}"), other))
            .collect::<Vec<_>>();
        for key in &other_keys {
            let row = crate::Record::new(key);
            target
                .read()
                .apply_range_snapshot_batch_preinstalled(
                    other,
                    &CollectionMeta::standard("items"),
                    &crate::RangeSnapshotBatch {
                        collection: "items".into(),
                        range_id: other.id,
                        range_epoch: other.epoch,
                        snapshot_commit_sequence: 0,
                        resume_after_key: key.clone(),
                        serialized_record_bytes: serde_json::to_vec(&row).unwrap().len(),
                        records: vec![row],
                    },
                )
                .unwrap();
        }
        let target_b_before = target.read().current_commit_seq();
        assert!(target_b_before > source_b);
        let unchanged = target
            .read()
            .current_native_range_source_lineage(range.id)
            .unwrap();
        assert_eq!(unchanged.range_index(), 0);
        assert_eq!(unchanged.native_receipt_sha256(), original_target_receipt);
        assert_eq!(unchanged.logical_history_sha256(), original_target_history);
        let providers = test_recovery_metadata_cluster(
            &origin.root.path().join("repair-metadata"),
            &topology,
            &node("n2"),
        )
        .unwrap(); // Deliberately distinct metadata and data leaders.
        let initiator = providers[&node("n0")].local_participant();
        let runtimes = origin
            .services
            .iter()
            .map(|(node, service)| {
                (
                    node.clone(),
                    Arc::new(RangeRecoveryRuntime::new(
                        Arc::clone(service),
                        Arc::clone(&providers[node]),
                    )),
                )
            })
            .collect::<BTreeMap<_, _>>();
        let nonce = Uuid::now_v7();
        let parent = CancellationToken::uncancelable();
        let mut quorum = RecoveryFenceQuorumRound::begin(
            Self::fresh_role_for(&providers, &initiator, &node("n0"), &range, nonce),
            &parent,
        )
        .unwrap();
        let scope = quorum.scope().clone();
        for id in [node("n0"), node("n1"), node("n2")] {
            let observation = runtimes[&id]
                .fence_epoch(
                    &initiator,
                    &RecoveryEpochFenceRequest {
                        scope: scope.clone(),
                        budget_ms: 5000,
                    },
                    &parent,
                )
                .unwrap();
            if id != node("n2") {
                quorum.record(observation).unwrap();
            }
        }
        let authority = Arc::new(quorum.finish().unwrap());
        assert_eq!(authority.observations().len(), 2);
        let frozen = runtimes[&node("n0")].source_capture_freeze(&scope).unwrap();
        let source_service = &origin.services[&node("n0")];
        let ready = source_service
            .lock()
            .attest_native_recovery_ready(
                Self::fresh_role_for(&providers, &initiator, &node("n0"), &range, nonce),
                Arc::clone(&frozen),
                &parent,
            )
            .unwrap();
        assert_eq!(ready.range_index(), 1);
        let mut round = source_service
            .lock()
            .begin_native_recovery_source_history(authority, &ready, node("n1"))
            .unwrap();
        let mut done = false;
        for _ in 0..16 {
            if source_service
                .lock()
                .advance_native_recovery_source_history(&mut round, &ready, &parent)
                .unwrap()
            {
                done = true;
                break;
            }
        }
        assert!(done, "bounded actual source history must verify full R1");
        let cut = source_service
            .lock()
            .finish_native_recovery_source_history(round, &ready)
            .unwrap();
        let source_db = Arc::clone(&origin.databases[&node("n0")]);
        let lineage = source_db
            .read()
            .current_native_range_source_lineage(range.id)
            .unwrap();
        drop(ready); // Capture atomically reacquires the actual source read pin.
        let capture = RangeImageCapture::begin(
            source_db,
            cut,
            frozen,
            lineage,
            Duration::from_secs(30),
            &parent,
        )
        .unwrap();
        assert_eq!(capture.range_index().get(), 1);
        let service = Arc::clone(&origin.services[&node("n1")]);
        Self {
            origin,
            providers,
            runtimes,
            initiator,
            range,
            scope,
            capture: Some(capture),
            service,
            selected_key,
            other_keys,
            source_b,
            target_b_before,
            original_target_uid,
        }
    }
    fn role(&self) -> RecoveryRoleAdmission {
        Self::fresh_role_for(
            &self.providers,
            &self.initiator,
            &node("n1"),
            &self.range,
            self.scope.recovery_nonce,
        )
    }
    fn begin(&self, attempt: Uuid) -> NativeRangeImageInstaller {
        let role = self.role();
        let source = self
            .capture
            .as_ref()
            .unwrap()
            .authorize_target_install()
            .unwrap();
        let lease = self.runtimes[&node("n1")]
            .transfer_image_install(
                &self.scope,
                &source,
                &NativeImageInstallRole::CurrentVoterRepair(&role),
                ImageInstallPurpose::CurrentVoterRepair,
                attempt,
                &CancellationToken::uncancelable(),
            )
            .unwrap();
        self.service
            .lock()
            .begin_native_range_image_install(
                &source,
                &NativeImageInstallRole::CurrentVoterRepair(&role),
                lease,
                ImageInstallPurpose::CurrentVoterRepair,
                attempt,
                &CancellationToken::uncancelable(),
            )
            .unwrap()
    }
    fn materialize(
        &mut self,
        mut install: NativeRangeImageInstaller,
    ) -> VerifiedNativeRangeImageInstall {
        loop {
            let step = {
                let capture = self.capture.as_mut().unwrap();
                let cursor = capture.history_cursor().clone();
                capture
                    .export_history_step(&cursor, 256, &CancellationToken::uncancelable())
                    .unwrap()
            };
            let role = self.role();
            let source = self
                .capture
                .as_ref()
                .unwrap()
                .authorize_target_install()
                .unwrap();
            let stage_id = install.history_stage_id();
            let progress = self
                .service
                .lock()
                .stage_native_range_image_history(
                    &mut install,
                    &NativeImageInstallRole::CurrentVoterRepair(&role),
                    &source,
                    stage_id,
                    &step,
                    &CancellationToken::uncancelable(),
                )
                .unwrap();
            if progress.cursor.complete() {
                break;
            }
        }
        loop {
            let role = self.role();
            let source = self
                .capture
                .as_ref()
                .unwrap()
                .authorize_target_install()
                .unwrap();
            if self
                .service
                .lock()
                .audit_native_range_image_history(
                    &mut install,
                    &NativeImageInstallRole::CurrentVoterRepair(&role),
                    &source,
                    256,
                    &CancellationToken::uncancelable(),
                )
                .unwrap()
                .audit_complete
            {
                break;
            }
        }
        loop {
            let role = self.role();
            let source = self
                .capture
                .as_ref()
                .unwrap()
                .authorize_target_install()
                .unwrap();
            if self
                .service
                .lock()
                .clear_native_range_image_step(
                    &mut install,
                    &NativeImageInstallRole::CurrentVoterRepair(&role),
                    &source,
                    &CancellationToken::uncancelable(),
                )
                .unwrap()
            {
                break;
            }
        }
        loop {
            let step = {
                let capture = self.capture.as_mut().unwrap();
                let cursor = capture.cursor().clone();
                capture
                    .export_step(&cursor, &options(), &CancellationToken::uncancelable())
                    .unwrap()
            };
            let role = self.role();
            let source = self
                .capture
                .as_ref()
                .unwrap()
                .authorize_target_install()
                .unwrap();
            let attempt = install.fence().attempt_id();
            self.service
                .lock()
                .copy_native_range_image_step(
                    &mut install,
                    &NativeImageInstallRole::CurrentVoterRepair(&role),
                    &source,
                    attempt,
                    &step,
                    &CancellationToken::uncancelable(),
                )
                .unwrap();
            if step.complete {
                break;
            }
        }
        let source = self.capture.take().unwrap().complete().unwrap();
        let role = self.role();
        let mut verification = self
            .service
            .lock()
            .begin_native_range_image_verification(
                install,
                &NativeImageInstallRole::CurrentVoterRepair(&role),
                source,
                &CancellationToken::uncancelable(),
            )
            .unwrap();
        let mut completed = false;
        for _ in 0..128 {
            let role = self.role();
            if self
                .service
                .lock()
                .advance_native_range_image_verification(
                    &mut verification,
                    &NativeImageInstallRole::CurrentVoterRepair(&role),
                    &CancellationToken::uncancelable(),
                )
                .unwrap()
                == NativeTargetImageVerificationProgress::Complete
            {
                completed = true;
                break;
            }
        }
        assert!(
            completed,
            "bounded actual lease target verification must finish"
        );
        let role = self.role();
        self.service
            .lock()
            .finish_native_range_image_verification(
                verification,
                &NativeImageInstallRole::CurrentVoterRepair(&role),
                &CancellationToken::uncancelable(),
            )
            .unwrap()
    }
}

#[test]
fn actual_rf3_nonempty_current_voter_repair_uses_same_catalogue_and_keeps_recovery_closed() {
    let mut fixture = CurrentRepairFixture::new();
    let expected = fixture.origin.databases[&node("n0")]
        .read()
        .get("items", &fixture.selected_key)
        .unwrap()
        .unwrap();
    let source_history = fixture
        .capture
        .as_ref()
        .unwrap()
        .authorize_target_install()
        .unwrap()
        .lineage()
        .logical_history_sha256()
        .to_string();
    let attempt = Uuid::now_v7();
    let install = fixture.begin(attempt);
    let verified = fixture.materialize(install);
    let installed_b = verified.base().target_snapshot_bound().get();
    assert_eq!(verified.base().range_index(), 1);
    assert_eq!(verified.base().logical_history_sha256(), source_history);
    assert!(verified.base().target_snapshot_bound().get() > fixture.target_b_before);
    assert_ne!(
        verified.base().target_snapshot_bound().get(),
        fixture.source_b
    );
    let role = fixture.role();
    let adopted = fixture
        .service
        .lock()
        .adopt_native_range_image_catalogue(
            verified,
            &NativeImageInstallRole::CurrentVoterRepair(&role),
            &CancellationToken::uncancelable(),
        )
        .unwrap();
    let role = fixture.role();
    let (freeze, finalization) = fixture
        .service
        .lock()
        .finalize_native_range_image_catalogue(
            adopted,
            &NativeImageInstallRole::CurrentVoterRepair(&role),
            &CancellationToken::uncancelable(),
        )
        .unwrap();
    let NativeImageInstallFreeze::Recovery(lease) = freeze else {
        panic!("actual current-repair Runtime must retain owned lease");
    };
    let role = fixture.role();
    lease
        .return_after_catalogue_adoption(
            finalization,
            &NativeImageInstallRole::CurrentVoterRepair(&role),
            &CancellationToken::uncancelable(),
        )
        .unwrap();
    let target = Arc::clone(&fixture.origin.databases[&node("n1")]);
    assert_eq!(
        target.read().get("items", &fixture.selected_key).unwrap(),
        Some(expected.clone())
    );
    assert_eq!(
        target.read().local_namespace_identity("items").unwrap(),
        Some(fixture.original_target_uid)
    );
    for key in &fixture.other_keys {
        assert!(target.read().get("items", key).unwrap().is_some());
    }
    assert!(target
        .read()
        .range_mutation_gate()
        .try_enter(fixture.range.id)
        .is_err());
    assert!(
        fixture
            .service
            .lock()
            .require_persisted_recovery_scope(&fixture.scope)
            .is_err(),
        "retained image intent must refuse generic recovery readiness"
    );
    let frozen = fixture.runtimes[&node("n1")]
        .source_capture_freeze(&fixture.scope)
        .unwrap();
    let role = fixture.role();
    assert!(
        fixture
            .service
            .lock()
            .attest_native_recovery_ready(role, frozen, &CancellationToken::uncancelable(),)
            .is_err(),
        "an adopted physical image cannot bypass the retained intent into NativeReady"
    );
    let persisted: serde_json::Value = serde_json::from_slice(
        &std::fs::read(target.read().data_path().join(DEFAULT_RANGE_WRITE_LOG)).unwrap(),
    )
    .unwrap();
    let key = fixture.range.id.get().to_string();
    assert_eq!(persisted["catalog"]["ranges"][&key]["resolved_through"], 1);
    assert_eq!(persisted["catalog"]["ranges"][&key]["last_index"], 1);
    assert_eq!(persisted["catalog"]["ranges"][&key]["compacted_through"], 1);
    assert!(persisted["catalog"]["ranges"][&key]["entries"]
        .as_object()
        .unwrap()
        .is_empty());
    assert_eq!(
        persisted["catalog"]["native_image_bases"][&key]["attempt_id"],
        attempt.to_string()
    );
    assert_eq!(
        persisted["catalog"]["image_install_intents"][&key]["attempt_id"],
        attempt.to_string()
    );
    assert_eq!(
        persisted["catalog"]["recovery_fences"][&key]["phase"],
        "Active"
    );
    let root = std::mem::replace(&mut fixture.origin.root, tempfile::tempdir().unwrap()).keep();
    let path = target.read().data_path().to_path_buf();
    let config = crate::DbConfig::default()
        .with_fsync(true)
        .with_storage_mode(crate::StorageMode::ServerPaged)
        .with_paged_buffer_pool_bytes(8 * 1024 * 1024);
    let spec = fixture.origin.specs[&node("n1")].clone();
    let selected = fixture.selected_key.clone();
    let other_keys = fixture.other_keys.clone();
    let range_id = fixture.range.id;
    let target_uid = fixture.original_target_uid;
    let source_b = fixture.source_b;
    let target_b_before = fixture.target_b_before;
    let old_target = Arc::downgrade(&target);
    drop(target);
    drop(fixture); // ALL real Runtime/service/coordinator/DB owners end first.
    assert_eq!(
        old_target.strong_count(),
        0,
        "cold reopen requires no old DB owner"
    );
    let (reopened, _) = BicDb::open_range_image_database(&path, config, &spec).unwrap();
    assert_eq!(reopened.get("items", &selected).unwrap(), Some(expected));
    assert_eq!(
        reopened.local_namespace_identity("items").unwrap(),
        Some(target_uid)
    );
    for key in other_keys {
        assert!(reopened.get("items", &key).unwrap().is_some());
    }
    let native = reopened
        .native_recovery_admission(
            &spec.cluster_id,
            spec.bootstrap_id,
            &spec.node_id,
            spec.member_incarnation,
        )
        .unwrap();
    let floor = reopened
        .native_installed_range_image_integrity(&native, range_id)
        .unwrap()
        .unwrap();
    assert_eq!(floor.range_index(), 1);
    assert_eq!(floor.logical_history_sha256(), source_history);
    // Cold receipt/rows/floor survive, but intent remains closed. This does not
    // prove cold resume, Prepared/Complete, serving, StrongGet or a new write.
    let store =
        RangeWriteStore::open(&path, spec.cluster_id.clone(), spec.node_id.clone(), true).unwrap();
    assert!(store.require_no_image_install(range_id).is_err());
    assert!(store.require_no_active_recovery(range_id).is_err());
    eprintln!(
        "RF3_CURRENT_REPAIR_EVIDENCE {}",
        serde_json::json!({
            "source_range_index": 1,
            "source_db_sequence": source_b,
            "target_before_db_sequence": target_b_before,
            "installed_target_local_db_sequence": installed_b,
            "cold_target_db_sequence": reopened.current_commit_seq(),
            "logical_history_sha256": source_history,
            "target_local_namespace_preserved": true,
            "selected_native_r0_unchanged_by_diagnostic_other_range_imports": true,
            "other_range_diagnostic_imports": 17,
            "other_range_imports_are_rf3_user_acks": false,
            "current_data_quorum_members": ["n0", "n1"],
            "source_data_leader": "n0",
            "metadata_leader": "n2",
            "native_ready_after_adoption": "REFUSED_RETAINED_IMAGE_INTENT",
            "cold_recovery_and_image_refusals_retained": true,
            "physical_qualification": false,
        })
    );
    drop(store);
    drop(floor);
    drop(native);
    drop(reopened);
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn actual_rf3_nonempty_finalized_image_prepares_without_an_unfenced_window() {
    let mut fixture = CurrentRepairFixture::new();
    let target = Arc::clone(&fixture.origin.databases[&node("n1")]);
    let expected = fixture.origin.databases[&node("n0")]
        .read()
        .get("items", &fixture.selected_key)
        .unwrap()
        .unwrap();
    let original_command = fixture.origin.services[&node("n0")]
        .lock()
        .probe_range_write_for_peer(&fixture.range, &node("n0"), 1)
        .unwrap()
        .entry
        .unwrap()
        .command;
    let attempt = Uuid::now_v7();
    let install = fixture.begin(attempt);
    let verified = fixture.materialize(install);
    let history = verified.base().logical_history_sha256().to_string();
    let native_receipt = verified.base().native_receipt_sha256().to_string();
    let target_b = verified.base().target_snapshot_bound().get();
    assert!(target_b > fixture.target_b_before && target_b > fixture.source_b);
    let role = fixture.role();
    let adopted = fixture
        .service
        .lock()
        .adopt_native_range_image_catalogue(
            verified,
            &NativeImageInstallRole::CurrentVoterRepair(&role),
            &CancellationToken::uncancelable(),
        )
        .unwrap();
    let role = fixture.role();
    let (freeze, finalization) = fixture
        .service
        .lock()
        .finalize_native_range_image_catalogue(
            adopted,
            &NativeImageInstallRole::CurrentVoterRepair(&role),
            &CancellationToken::uncancelable(),
        )
        .unwrap();
    let NativeImageInstallFreeze::Recovery(lease) = freeze else {
        panic!("actual Normal repair must retain its owned Runtime lease")
    };
    let role = fixture.role();
    let image = lease
        .return_after_catalogue_adoption(
            finalization,
            &NativeImageInstallRole::CurrentVoterRepair(&role),
            &CancellationToken::uncancelable(),
        )
        .unwrap();
    let frozen = fixture.runtimes[&node("n1")]
        .source_capture_freeze(&fixture.scope)
        .unwrap();
    assert_eq!(frozen.owner(), fixture.scope.recovery_nonce);
    assert_eq!(image.intent_fence().attempt_id(), attempt);
    assert_eq!(image.base().range_index(), 1);
    assert_eq!(image.base().target_snapshot_bound().get(), target_b);
    // The ordinary producer STILL refuses the actual image intent. No clear
    // or permissive intermediate catalogue state is used to obtain physical
    // attestation. Only the typed real owner-return receipt enters that path.
    let ordinary_role = fixture.role();
    assert!(fixture
        .service
        .lock()
        .attest_native_recovery_ready(
            ordinary_role,
            Arc::clone(&frozen),
            &CancellationToken::uncancelable(),
        )
        .is_err());
    assert!(fixture
        .service
        .lock()
        .require_persisted_recovery_scope(&fixture.scope)
        .is_err());
    let role = fixture.role();
    let prepared = fixture
        .service
        .lock()
        .prepare_finalized_native_recovery_image(
            image,
            role,
            Arc::clone(&frozen),
            &CancellationToken::uncancelable(),
        )
        .unwrap();
    assert_eq!(prepared.range_index(), 1);
    assert_eq!(prepared.logical_history_sha256(), history);
    assert_eq!(prepared.native_receipt_sha256(), native_receipt);
    assert_eq!(prepared.local_visibility_sequence(), target_b);
    assert!(prepared.scope().same_session(&fixture.scope));
    assert_eq!(
        target.read().get("items", &fixture.selected_key).unwrap(),
        Some(expected.clone())
    );
    assert_eq!(
        target.read().local_namespace_identity("items").unwrap(),
        Some(fixture.original_target_uid)
    );
    for key in &fixture.other_keys {
        assert!(target.read().get("items", key).unwrap().is_some());
    }
    assert!(target
        .read()
        .range_mutation_gate()
        .try_controlled_mutation(&frozen)
        .is_err());
    assert!(fixture
        .service
        .lock()
        .require_persisted_recovery_scope(&fixture.scope)
        .is_err());
    assert!(fixture
        .service
        .lock()
        .prepare_range_write(&fixture.range, &node("n0"), original_command.clone(),)
        .is_err());
    let persisted: serde_json::Value = serde_json::from_slice(
        &std::fs::read(target.read().data_path().join(DEFAULT_RANGE_WRITE_LOG)).unwrap(),
    )
    .unwrap();
    let key = fixture.range.id.get().to_string();
    assert!(persisted["catalog"]["image_install_intents"]
        .get(&key)
        .is_none());
    assert_eq!(
        persisted["catalog"]["native_image_bases"][&key]["attempt_id"],
        attempt.to_string()
    );
    let point = &persisted["catalog"]["recovery_fences"][&key]["phase"]["NativePrepared"];
    assert_eq!(point["range_index"], 1);
    assert_eq!(point["logical_history_sha256"], history);
    assert_eq!(point["native_receipt_sha256"], native_receipt);
    assert_eq!(point["local_visibility_sequence"], target_b);
    assert_eq!(
        persisted["catalog"]["recovery_fences"][&key]["scope"]["recovery_nonce"],
        fixture.scope.recovery_nonce.to_string()
    );
    let root = std::mem::replace(&mut fixture.origin.root, tempfile::tempdir().unwrap()).keep();
    let path = target.read().data_path().to_path_buf();
    let spec = fixture.origin.specs[&node("n1")].clone();
    let selected = fixture.selected_key.clone();
    let other_keys = fixture.other_keys.clone();
    let target_uid = fixture.original_target_uid;
    let scope = fixture.scope.clone();
    let old_target = Arc::downgrade(&target);
    drop(prepared);
    drop(frozen);
    drop(target);
    drop(fixture);
    assert_eq!(
        old_target.strong_count(),
        0,
        "all physical owners end before reopen"
    );
    let (reopened, _) = BicDb::open_range_image_database(
        &path,
        crate::DbConfig::default()
            .with_fsync(true)
            .with_storage_mode(crate::StorageMode::ServerPaged)
            .with_paged_buffer_pool_bytes(8 * 1024 * 1024),
        &spec,
    )
    .unwrap();
    assert_eq!(reopened.get("items", &selected).unwrap(), Some(expected));
    assert_eq!(
        reopened.local_namespace_identity("items").unwrap(),
        Some(target_uid)
    );
    for key in other_keys {
        assert!(reopened.get("items", &key).unwrap().is_some());
    }
    let store =
        RangeWriteStore::open(&path, spec.cluster_id.clone(), spec.node_id.clone(), true).unwrap();
    store.require_no_image_install(scope.range_id).unwrap();
    assert_eq!(
        store.recovery_observation_phase(&scope).unwrap(),
        crate::distribution_range_recovery::RecoveryFenceObservationPhase::NativePrepared
    );
    assert!(store.require_persisted_recovery_scope(&scope).is_err());
    assert!(store
        .normal_native_write_allowed(&original_command)
        .is_err());
    eprintln!(
        "RF3_IMAGE_PREPARED_EVIDENCE {}",
        serde_json::json!({
            "range_index": 1, "local_visibility_sequence": target_b,
            "logical_history_sha256": history, "native_receipt_sha256": native_receipt,
            "target_namespace_preserved": true, "exact_image_intent_replaced": true,
            "native_image_catalogue_base_retained": true, "cold_refusal_retained": true,
            "normal_writes_admitted": false, "activation_proven": false,
            "physical_qualification": false,
        })
    );
    drop(store);
    drop(reopened);
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn actual_adopted_image_compacted_marker_conflict_refuses_native_preparation() {
    let mut fixture = CurrentRepairFixture::new();
    let target = Arc::clone(&fixture.origin.databases[&node("n1")]);
    let attempt = Uuid::now_v7();
    let install = fixture.begin(attempt);
    let verified = fixture.materialize(install);
    let role = fixture.role();
    let adopted = fixture
        .service
        .lock()
        .adopt_native_range_image_catalogue(
            verified,
            &NativeImageInstallRole::CurrentVoterRepair(&role),
            &CancellationToken::uncancelable(),
        )
        .unwrap();
    // Deliberately corrupt REAL durable catalogue metadata after its actual
    // image adoption. This is not a legal quorum/authority operation and is
    // not attributed to Azure or an observed production failure.
    fixture
        .service
        .lock()
        .test_corrupt_adopted_image_marker(&fixture.scope, 1, "0".repeat(64))
        .unwrap();
    let role = fixture.role();
    let (freeze, finalization) = fixture
        .service
        .lock()
        .finalize_native_range_image_catalogue(
            adopted,
            &NativeImageInstallRole::CurrentVoterRepair(&role),
            &CancellationToken::uncancelable(),
        )
        .unwrap();
    let NativeImageInstallFreeze::Recovery(lease) = freeze else {
        panic!("actual Runtime lease required");
    };
    let role = fixture.role();
    let image = lease
        .return_after_catalogue_adoption(
            finalization,
            &NativeImageInstallRole::CurrentVoterRepair(&role),
            &CancellationToken::uncancelable(),
        )
        .unwrap();
    // The opaque receipt binds the newly observed map, including the injected
    // bad marker. Hashing that map alone cannot prove agreement with native H.
    let frozen = fixture.runtimes[&node("n1")]
        .source_capture_freeze(&fixture.scope)
        .unwrap();
    let role = fixture.role();
    let error = fixture
        .service
        .lock()
        .prepare_finalized_native_recovery_image(
            image,
            role,
            Arc::clone(&frozen),
            &CancellationToken::uncancelable(),
        )
        .unwrap_err();
    assert!(
        error.to_string().contains(
            "finalized image historical certification marker conflicts with actual native history"
        ),
        "{error}"
    );
    let persisted: serde_json::Value = serde_json::from_slice(
        &std::fs::read(target.read().data_path().join(DEFAULT_RANGE_WRITE_LOG)).unwrap(),
    )
    .unwrap();
    let key = fixture.range.id.get().to_string();
    assert_eq!(
        persisted["catalog"]["recovery_fences"][&key]["phase"],
        "Active"
    );
    assert_eq!(
        persisted["catalog"]["image_install_intents"][&key]["attempt_id"],
        attempt.to_string()
    );
    assert_eq!(
        persisted["catalog"]["native_image_bases"][&key]["attempt_id"],
        attempt.to_string()
    );
    assert!(target
        .read()
        .get("items", &fixture.selected_key)
        .unwrap()
        .is_some());
    assert!(fixture
        .service
        .lock()
        .require_persisted_recovery_scope(&fixture.scope)
        .is_err());
    assert!(target
        .read()
        .range_mutation_gate()
        .try_enter(fixture.range.id)
        .is_err());
    eprintln!("RF3_IMAGE_MARKER_CORRUPTION_REFUSAL {error}");
}

impl CurrentRepairFixture {
    fn prepare_actual_current_image(
        &mut self,
        attempt: Uuid,
    ) -> crate::distribution_range_consensus::PreparedNativeRecoveryReady {
        let install = self.begin(attempt);
        let verified = self.materialize(install);
        let role = self.role();
        let adopted = self
            .service
            .lock()
            .adopt_native_range_image_catalogue(
                verified,
                &NativeImageInstallRole::CurrentVoterRepair(&role),
                &CancellationToken::uncancelable(),
            )
            .unwrap();
        let role = self.role();
        let (freeze, finalization) = self
            .service
            .lock()
            .finalize_native_range_image_catalogue(
                adopted,
                &NativeImageInstallRole::CurrentVoterRepair(&role),
                &CancellationToken::uncancelable(),
            )
            .unwrap();
        let NativeImageInstallFreeze::Recovery(lease) = freeze else {
            panic!("actual Runtime lease required");
        };
        let role = self.role();
        let image = lease
            .return_after_catalogue_adoption(
                finalization,
                &NativeImageInstallRole::CurrentVoterRepair(&role),
                &CancellationToken::uncancelable(),
            )
            .unwrap();
        let frozen = self.runtimes[&node("n1")]
            .source_capture_freeze(&self.scope)
            .unwrap();
        let role = self.role();
        self.service
            .lock()
            .prepare_finalized_native_recovery_image(
                image,
                role,
                frozen,
                &CancellationToken::uncancelable(),
            )
            .unwrap()
    }

    fn recapture_actual_source_under_new_nonce(&mut self, nonce: Uuid) {
        let parent = CancellationToken::uncancelable();
        let role = Self::fresh_role_for(
            &self.providers,
            &self.initiator,
            &node("n0"),
            &self.range,
            nonce,
        );
        let mut round = RecoveryFenceQuorumRound::begin(role, &parent).unwrap();
        let scope = round.scope().clone();
        assert_ne!(scope.recovery_nonce, self.scope.recovery_nonce);
        assert_eq!(scope.range_epoch, self.scope.range_epoch);
        for id in [node("n0"), node("n1"), node("n2")] {
            let observed = self.runtimes[&id]
                .fence_epoch(
                    &self.initiator,
                    &RecoveryEpochFenceRequest {
                        scope: scope.clone(),
                        budget_ms: 5000,
                    },
                    &parent,
                )
                .unwrap();
            if id != node("n2") {
                round.record(observed).unwrap();
            }
        }
        let authority = Arc::new(round.finish().unwrap());
        let frozen = self.runtimes[&node("n0")]
            .source_capture_freeze(&scope)
            .unwrap();
        let source_service = &self.origin.services[&node("n0")];
        let ready = source_service
            .lock()
            .attest_native_recovery_ready(
                Self::fresh_role_for(
                    &self.providers,
                    &self.initiator,
                    &node("n0"),
                    &self.range,
                    nonce,
                ),
                Arc::clone(&frozen),
                &parent,
            )
            .unwrap();
        assert_eq!(ready.range_index(), 1);
        let mut history = source_service
            .lock()
            .begin_native_recovery_source_history(authority, &ready, node("n1"))
            .unwrap();
        let mut done = false;
        for _ in 0..16 {
            if source_service
                .lock()
                .advance_native_recovery_source_history(&mut history, &ready, &parent)
                .unwrap()
            {
                done = true;
                break;
            }
        }
        assert!(done);
        let cut = source_service
            .lock()
            .finish_native_recovery_source_history(history, &ready)
            .unwrap();
        let source = Arc::clone(&self.origin.databases[&node("n0")]);
        let lineage = source
            .read()
            .current_native_range_source_lineage(self.range.id)
            .unwrap();
        drop(ready);
        self.capture = Some(
            RangeImageCapture::begin(
                source,
                cut,
                frozen,
                lineage,
                Duration::from_secs(30),
                &parent,
            )
            .unwrap(),
        );
        self.scope = scope;
    }
}

#[test]
fn actual_prepared_serving_record_survives_a_second_current_voter_image_attempt() {
    use crate::distribution_range_recovery::{
        ClosedNativeReadyQuorumRound, NativeReadyObservation, NativeReadyRequest,
    };
    let mut fixture = CurrentRepairFixture::new();
    // Keep all actual native/catalogue roots even if a healthy precondition
    // fails before the second-attempt boundary. The source methods own their
    // existing concrete paths; replacing this cleanup owner changes no DB.
    let retained = std::mem::replace(&mut fixture.origin.root, tempfile::tempdir().unwrap()).keep();
    eprintln!(
        "ACTUAL_SERVING_IMAGE_WITNESS_RETAINED_ROOT {}",
        retained.display()
    );
    std::fs::write(
        retained.join("serving-image-witness-start.json"),
        serde_json::to_vec_pretty(&serde_json::json!({
            "scope":fixture.scope,"target_birth_spec":fixture.origin.specs[&node("n1")],
            "target_uid":fixture.original_target_uid,"source_b":fixture.source_b,
            "target_b_before":fixture.target_b_before,"physical_qualification":false,
        }))
        .unwrap(),
    )
    .unwrap();
    let target = Arc::clone(&fixture.origin.databases[&node("n1")]);
    let attempt_a = Uuid::now_v7();
    let target_prepared = fixture.prepare_actual_current_image(attempt_a);
    let source_frozen = fixture.runtimes[&node("n0")]
        .source_capture_freeze(&fixture.scope)
        .unwrap();
    let source_role = CurrentRepairFixture::fresh_role_for(
        &fixture.providers,
        &fixture.initiator,
        &node("n0"),
        &fixture.range,
        fixture.scope.recovery_nonce,
    );
    let source_prepared = fixture.origin.services[&node("n0")]
        .lock()
        .test_prepare_existing_native_source(
            source_role,
            Arc::clone(&source_frozen),
            &CancellationToken::uncancelable(),
        )
        .unwrap();
    assert_eq!(source_prepared.range_index(), 1);
    assert_eq!(
        source_prepared.logical_history_sha256(),
        target_prepared.logical_history_sha256()
    );
    // This closed quorum is for the ACTUAL target recipient. The initiating
    // data leader remains n0; acquiring target-local metadata evidence does
    // not substitute a caller-selected subject or manufacture voter replies.
    let fresh = CurrentRepairFixture::fresh_role_for(
        &fixture.providers,
        &fixture.initiator,
        &node("n1"),
        &fixture.range,
        fixture.scope.recovery_nonce,
    );
    let request = NativeReadyRequest {
        scope: RecoveryFenceScope::from_role(&fresh).unwrap(),
        challenge: Uuid::now_v7(),
        budget_ms: 5000,
    };
    let mut round =
        ClosedNativeReadyQuorumRound::begin(fresh, &CancellationToken::uncancelable()).unwrap();
    for (id, prepared) in [
        (node("n0"), &source_prepared),
        (node("n1"), &target_prepared),
    ] {
        let role = CurrentRepairFixture::fresh_role_for(
            &fixture.providers,
            &fixture.initiator,
            &id,
            &fixture.range,
            fixture.scope.recovery_nonce,
        );
        let actual = fixture.origin.services[&id]
            .lock()
            .test_project_actual_prepared_native(prepared, &role, &request)
            .unwrap();
        let observation = NativeReadyObservation::from_local(
            actual,
            fixture.providers[&id].local_participant(),
            &request,
            role.deadline(),
        )
        .unwrap();
        round.record(observation).unwrap();
    }
    let closed = round.finish().unwrap();
    assert_eq!(closed.range_index(), 1);
    let fresh = fixture.role();
    let serving = fixture
        .service
        .lock()
        .test_persist_actual_prepared_serving(&target_prepared, &closed, &fresh)
        .unwrap();
    serving.require_current(&target.read()).unwrap();
    let serving_a = target
        .read()
        .test_native_serving_record_json(fixture.range.id)
        .unwrap()
        .unwrap();
    assert_eq!(serving_a["checksum_sha256"], serving.checksum_sha256());
    assert_eq!(serving_a["activation_index"], 1);
    assert_eq!(serving_a["range"]["epoch"], fixture.range.epoch);
    let native_a = target_prepared.native_receipt_sha256().to_string();
    let history = target_prepared.logical_history_sha256().to_string();
    let target_uid = fixture.original_target_uid;
    // The first record is genuine durable native Serving metadata while the
    // catalogue is STILL NativePrepared/fenced. No activation or post-recovery
    // ACK is invented. End every old pin before a new actual recovery nonce.
    drop(serving);
    drop(closed);
    drop(source_prepared);
    drop(target_prepared);
    drop(source_frozen);
    let old_nonce = fixture.scope.recovery_nonce;
    fixture.recapture_actual_source_under_new_nonce(Uuid::now_v7());
    assert_ne!(fixture.scope.recovery_nonce, old_nonce);
    assert_eq!(
        target
            .read()
            .test_native_serving_record_json(fixture.range.id)
            .unwrap()
            .unwrap(),
        serving_a
    );
    let attempt_b = Uuid::now_v7();
    // On a baseline failure retain the actual stopped native/catalogue roots
    // after unwinding. On success remove them explicitly. This is local test
    // custody, not a crash/power-loss or physical qualification claim.
    let before: serde_json::Value = serde_json::from_slice(
        &std::fs::read(target.read().data_path().join(DEFAULT_RANGE_WRITE_LOG)).unwrap(),
    )
    .unwrap();
    let stage = serde_json::json!({"scope":"ACTUAL_LOCAL_INTERRUPTED_COMPLETION_THEN_SECOND_REPAIR","attempt_a":attempt_a,"attempt_b":attempt_b,"old_nonce":old_nonce,"new_scope":fixture.scope,"serving_a":serving_a,"native_receipt_a":native_a,"logical_history_sha256":history,"target_uid":target_uid,"catalogue_before_b":before,"physical_qualification":false});
    {
        use std::io::Write;
        let mut file =
            std::fs::File::create(retained.join("serving-image-witness-before-b.json")).unwrap();
        file.write_all(&serde_json::to_vec_pretty(&stage).unwrap())
            .unwrap();
        file.sync_all().unwrap();
        std::fs::File::open(&retained).unwrap().sync_all().unwrap();
    }
    eprintln!(
        "ACTUAL_SERVING_IMAGE_WITNESS_RETAINED_ROOT {}",
        retained.display()
    );
    let install = fixture.begin(attempt_b);
    let verified = fixture.materialize(install);
    assert_eq!(verified.base().range_index(), 1);
    assert_eq!(verified.base().logical_history_sha256(), history);
    assert_ne!(verified.base().native_receipt_sha256(), native_a);
    assert_eq!(
        target
            .read()
            .test_native_serving_record_json(fixture.range.id)
            .unwrap()
            .unwrap(),
        serving_a
    );
    let role = fixture.role();
    let adopted = fixture
        .service
        .lock()
        .adopt_native_range_image_catalogue(
            verified,
            &NativeImageInstallRole::CurrentVoterRepair(&role),
            &CancellationToken::uncancelable(),
        )
        .unwrap();
    let role = fixture.role();
    let (freeze, finalization) = fixture
        .service
        .lock()
        .finalize_native_range_image_catalogue(
            adopted,
            &NativeImageInstallRole::CurrentVoterRepair(&role),
            &CancellationToken::uncancelable(),
        )
        .unwrap();
    let NativeImageInstallFreeze::Recovery(lease) = freeze else {
        panic!("actual Runtime lease required")
    };
    let role = fixture.role();
    let image = lease
        .return_after_catalogue_adoption(
            finalization,
            &NativeImageInstallRole::CurrentVoterRepair(&role),
            &CancellationToken::uncancelable(),
        )
        .unwrap();
    let frozen = fixture.runtimes[&node("n1")]
        .source_capture_freeze(&fixture.scope)
        .unwrap();
    let role = fixture.role();
    let second = fixture
        .service
        .lock()
        .prepare_finalized_native_recovery_image(
            image,
            role,
            frozen,
            &CancellationToken::uncancelable(),
        );
    let after: serde_json::Value = serde_json::from_slice(
        &std::fs::read(target.read().data_path().join(DEFAULT_RANGE_WRITE_LOG)).unwrap(),
    )
    .unwrap();
    std::fs::write(retained.join("serving-image-witness-after-b.json"),serde_json::to_vec_pretty(&serde_json::json!({"catalogue":after,"serving_record":target.read().test_native_serving_record_json(fixture.range.id).unwrap(),"result":second.as_ref().map(|prepared|prepared.range_index()).map_err(|error|error.to_string())})).unwrap()).unwrap();
    std::fs::File::open(retained.join("serving-image-witness-after-b.json"))
        .unwrap()
        .sync_all()
        .unwrap();
    std::fs::File::open(&retained).unwrap().sync_all().unwrap();
    assert_eq!(
        target
            .read()
            .test_native_serving_record_json(fixture.range.id)
            .unwrap()
            .unwrap(),
        serving_a,
        "the prior durable native serving record/floor must remain unchanged",
    );
    assert!(second.is_ok(),"a new actual compatible image must prepare without deleting the prior durable serving epoch fence: {:?}",second.as_ref().err());
    let prepared = second.unwrap();
    assert_eq!(prepared.range_index(), 1);
    assert_eq!(prepared.logical_history_sha256(), history);
    assert_eq!(
        target.read().local_namespace_identity("items").unwrap(),
        Some(target_uid)
    );
    let retained_serving = target
        .read()
        .test_native_serving_record_json(fixture.range.id)
        .unwrap()
        .unwrap();
    assert_eq!(
        retained_serving, serving_a,
        "physical repair may not delete/relabel the historical epoch fence"
    );
    drop(prepared);
    drop(target);
    drop(fixture);
    std::fs::remove_dir_all(retained).unwrap();
}
