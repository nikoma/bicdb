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
        assert_eq!(
            target
                .read()
                .current_native_range_source_lineage(range.id)
                .unwrap()
                .range_index(),
            0
        );
        assert!(target.read().get("items", &selected_key).unwrap().is_none());
        // These are real target-local, deliberately UNACKNOWLEDGED diagnostic
        // writes in the other range. They create a higher target-local B and
        // sentinels that selected-range replacement must leave untouched.
        let other_keys = (0..17)
            .map(|i| key(&format!("target-local-other-{i}"), other))
            .collect::<Vec<_>>();
        for key in &other_keys {
            let db = target.write();
            let mut tx = db.begin_transaction().unwrap();
            tx.insert("items", crate::Record::new(key)).unwrap();
            tx.commit().unwrap();
        }
        let target_b_before = target.read().current_commit_seq();
        assert!(target_b_before > source_b);
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
    assert_eq!(persisted["catalog"]["ranges"][&key]["compacted_through"], 1);
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
    drop(store);
    drop(floor);
    drop(native);
    drop(reopened);
    std::fs::remove_dir_all(root).unwrap();
}
