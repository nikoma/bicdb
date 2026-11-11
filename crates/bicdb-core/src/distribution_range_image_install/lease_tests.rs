//! Actual local Runtime/DataService choreography. Metadata uses real durable
//! stores/probes with registered in-process participants, not authenticated TCP.
//! RF1 source handoff is deliberate; these cases do not qualify RF3 failover.
use super::*;
use crate::distribution_consensus::{MetadataReadParticipant, MetadataRecoveryRolePurpose};
use crate::distribution_data::ClusterDataNodeService;
use crate::distribution_metadata_read_provider::MetadataReadProvider;
use crate::distribution_range_consensus::{RecoveryFenceScope, DEFAULT_RANGE_WRITE_LOG};
use crate::distribution_range_recovery::{
    test_recovery_metadata_cluster, HandoffRoleAdmission, RangeRecoveryRuntime,
    RecoveryEpochFenceRequest, RecoveryRoleAdmission,
};
use parking_lot::Mutex;
use std::collections::BTreeMap;
use std::time::{Duration, Instant};

fn node(id: &str) -> crate::ClusterNodeId {
    crate::ClusterNodeId::new(id).unwrap()
}

struct LeaseFixture {
    runtime: Arc<RangeRecoveryRuntime>,
    service: Arc<Mutex<ClusterDataNodeService>>,
    providers: BTreeMap<crate::ClusterNodeId, Arc<MetadataReadProvider>>,
    initiator: MetadataReadParticipant,
    scope: RecoveryFenceScope,
    base: super::tests::Fixture,
}
impl LeaseFixture {
    fn new() -> Self {
        let base = super::tests::Fixture::new();
        let providers = test_recovery_metadata_cluster(
            &base.root.path().join("lease-metadata"),
            &base.topology,
            &node("n1"),
        )
        .unwrap();
        let initiator = providers[&node("n1")].local_participant();
        let target = base.target();
        let service = Arc::new(Mutex::new(
            ClusterDataNodeService::open(
                base.topology.cluster_id.clone(),
                node("n2"),
                target.read().data_path(),
                Arc::clone(&target),
                true,
            )
            .unwrap(),
        ));
        let runtime = Arc::new(RangeRecoveryRuntime::new(
            Arc::clone(&service),
            Arc::clone(&providers[&node("n2")]),
        ));
        let role = Self::fresh_role_for(&base, &providers, &initiator);
        let scope = RecoveryFenceScope::from_role(role.role()).unwrap();
        let request = RecoveryEpochFenceRequest {
            scope: scope.clone(),
            budget_ms: 5_000,
        };
        runtime
            .fence_epoch(&initiator, &request, &CancellationToken::uncancelable())
            .unwrap();
        assert_ne!(
            scope.freeze_owner().unwrap(),
            scope.recovery_nonce,
            "actual committed Handoff S differs from nonce N"
        );
        Self {
            runtime,
            service,
            providers,
            initiator,
            scope,
            base,
        }
    }
    fn fresh_role_for(
        base: &super::tests::Fixture,
        providers: &BTreeMap<crate::ClusterNodeId, Arc<MetadataReadProvider>>,
        initiator: &MetadataReadParticipant,
    ) -> HandoffRoleAdmission {
        let plan = base.topology.relocations.values().next().unwrap();
        let session = plan.handoff_session.as_ref().unwrap();
        let purpose = MetadataRecoveryRolePurpose::CommittedHandoff {
            relocation_id: plan.id,
            plan_id: plan.plan_id.clone(),
            session_id: session.session_id,
            nonce: session.nonce,
        };
        let business = Uuid::now_v7();
        let challenge = Uuid::now_v7();
        let parent = CancellationToken::uncancelable()
            .child_with_deadline(Instant::now() + Duration::from_secs(5));
        let observation = providers[&node("n2")]
            .witness_recovery_role(
                initiator,
                business,
                plan.range_id,
                plan.learner_epoch,
                session.nonce,
                challenge,
                purpose.clone(),
                &parent,
            )
            .unwrap();
        RecoveryRoleAdmission::from_witness(
            observation,
            initiator,
            &node("n2"),
            business,
            plan.range_id,
            plan.learner_epoch,
            session.nonce,
            challenge,
            &purpose,
        )
        .unwrap()
        .into_handoff()
        .unwrap()
    }
    fn role(&self) -> HandoffRoleAdmission {
        Self::fresh_role_for(&self.base, &self.providers, &self.initiator)
    }
    fn transfer(
        &self,
        attempt: Uuid,
        parent: &CancellationToken,
    ) -> crate::distribution_range_recovery::RecoveryImageInstallFreeze {
        let role = self.role();
        let source = self
            .base
            .capture
            .as_ref()
            .unwrap()
            .authorize_target_install()
            .unwrap();
        self.runtime
            .transfer_image_install(
                &self.scope,
                &source,
                &NativeImageInstallRole::CommittedHandoff(&role),
                ImageInstallPurpose::RelocationLearner,
                attempt,
                parent,
            )
            .unwrap()
    }
    fn persisted_catalog(&self) -> serde_json::Value {
        serde_json::from_slice(
            &std::fs::read(
                self.base
                    .root
                    .path()
                    .join("target")
                    .join(DEFAULT_RANGE_WRITE_LOG),
            )
            .unwrap(),
        )
        .unwrap()
    }
    fn assert_fenced(&self, expected_owner: Uuid) {
        let frozen = self.runtime.source_capture_freeze(&self.scope).unwrap();
        assert_eq!(frozen.owner(), expected_owner);
        assert!(self
            .base
            .target()
            .read()
            .range_mutation_gate()
            .try_enter(self.scope.range_id)
            .is_err());
        self.service
            .lock()
            .require_persisted_recovery_scope(&self.scope)
            .unwrap();
    }
    fn begin(
        &self,
        lease: crate::distribution_range_recovery::RecoveryImageInstallFreeze,
        attempt: Uuid,
    ) -> NativeRangeImageInstaller {
        let role = self.role();
        let source = self
            .base
            .capture
            .as_ref()
            .unwrap()
            .authorize_target_install()
            .unwrap();
        self.service
            .lock()
            .begin_native_range_image_install(
                &source,
                &NativeImageInstallRole::CommittedHandoff(&role),
                lease,
                ImageInstallPurpose::RelocationLearner,
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
                let capture = self.base.capture.as_mut().unwrap();
                let cursor = capture.history_cursor().clone();
                capture
                    .export_history_step(&cursor, 256, &CancellationToken::uncancelable())
                    .unwrap()
            };
            let role = self.role();
            let source = self
                .base
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
                    &NativeImageInstallRole::CommittedHandoff(&role),
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
                .base
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
                    &NativeImageInstallRole::CommittedHandoff(&role),
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
                .base
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
                    &NativeImageInstallRole::CommittedHandoff(&role),
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
                let capture = self.base.capture.as_mut().unwrap();
                let cursor = capture.cursor().clone();
                capture
                    .export_step(&cursor, &options(), &CancellationToken::uncancelable())
                    .unwrap()
            };
            let role = self.role();
            let source = self
                .base
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
                    &NativeImageInstallRole::CommittedHandoff(&role),
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
        let source = self.base.capture.take().unwrap().complete().unwrap();
        let role = self.role();
        let mut verification = self
            .service
            .lock()
            .begin_native_range_image_verification(
                install,
                &NativeImageInstallRole::CommittedHandoff(&role),
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
                    &NativeImageInstallRole::CommittedHandoff(&role),
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
                &NativeImageInstallRole::CommittedHandoff(&role),
                &CancellationToken::uncancelable(),
            )
            .unwrap()
    }
}

#[test]
fn actual_runtime_lease_untouched_cancel_drop_requires_fresh_authority_and_keeps_recovery_fenced() {
    let mut fixture = LeaseFixture::new();
    let attempt = Uuid::now_v7();
    let parent = CancellationToken::uncancelable();
    let lease = fixture.transfer(attempt, &parent);
    assert_eq!(lease.as_frozen().owner(), attempt);
    assert!(fixture
        .runtime
        .source_capture_freeze(&fixture.scope)
        .is_err());
    parent.cancel();
    drop(lease);
    drop(fixture.base.capture.take());
    assert!(
        fixture
            .runtime
            .source_capture_freeze(&fixture.scope)
            .is_err(),
        "Drop cannot implicitly return A to S"
    );
    let fresh = fixture.role();
    fixture
        .runtime
        .return_untouched_image_transfer(
            &fixture.scope,
            &NativeImageInstallRole::CommittedHandoff(&fresh),
            &CancellationToken::uncancelable(),
        )
        .unwrap();
    fixture.assert_fenced(fixture.scope.freeze_owner().unwrap());
    let catalog = fixture.persisted_catalog();
    assert!(catalog["catalog"]
        .get("image_install_intents")
        .is_none_or(|value| value.as_object().unwrap().is_empty()));
    assert_eq!(
        catalog["catalog"]["ranges"][fixture.scope.range_id.get().to_string()]["resolved_through"],
        0
    );
}

#[test]
fn actual_runtime_lease_post_intent_drop_refuses_untouched_return() {
    let fixture = LeaseFixture::new();
    let attempt = Uuid::now_v7();
    let lease = fixture.transfer(attempt, &CancellationToken::uncancelable());
    let install = fixture.begin(lease, attempt);
    drop(install);
    let fresh = fixture.role();
    assert!(fixture
        .runtime
        .return_untouched_image_transfer(
            &fixture.scope,
            &NativeImageInstallRole::CommittedHandoff(&fresh),
            &CancellationToken::uncancelable()
        )
        .is_err());
    assert!(
        fixture
            .runtime
            .source_capture_freeze(&fixture.scope)
            .is_err(),
        "modified attempt remains A-owned"
    );
    assert_eq!(
        fixture.persisted_catalog()["catalog"]["image_install_intents"]
            [fixture.scope.range_id.get().to_string()]["attempt_id"],
        attempt.to_string()
    );
    assert!(fixture
        .base
        .target()
        .read()
        .range_mutation_gate()
        .try_enter(fixture.scope.range_id)
        .is_err());
}

#[test]
fn actual_runtime_lease_full_image_adoption_returns_only_control_and_preserves_all_refusals() {
    let mut fixture = LeaseFixture::new();
    let attempt = Uuid::now_v7();
    let source_log = fixture
        .base
        .source
        .read()
        .data_path()
        .join(DEFAULT_RANGE_WRITE_LOG);
    let original_source = std::fs::read(&source_log).unwrap();
    let lease = fixture.transfer(attempt, &CancellationToken::uncancelable());
    let install = fixture.begin(lease, attempt);
    let verified = fixture.materialize(install);
    let role = fixture.role();
    let adopted = fixture
        .service
        .lock()
        .adopt_native_range_image_catalogue(
            verified,
            &NativeImageInstallRole::CommittedHandoff(&role),
            &CancellationToken::uncancelable(),
        )
        .unwrap();
    let role = fixture.role();
    let (freeze, finalization) = fixture
        .service
        .lock()
        .finalize_native_range_image_catalogue(
            adopted,
            &NativeImageInstallRole::CommittedHandoff(&role),
            &CancellationToken::uncancelable(),
        )
        .unwrap();
    let NativeImageInstallFreeze::Recovery(lease) = freeze else {
        panic!("actual Runtime lease must remain owned through native/catalog finalization")
    };
    let role = fixture.role();
    lease
        .return_after_catalogue_adoption(
            finalization,
            &NativeImageInstallRole::CommittedHandoff(&role),
            &CancellationToken::uncancelable(),
        )
        .unwrap();
    fixture.assert_fenced(fixture.scope.freeze_owner().unwrap());
    let catalog = fixture.persisted_catalog();
    let key = fixture.scope.range_id.get().to_string();
    assert_eq!(
        catalog["catalog"]["image_install_intents"][&key]["attempt_id"],
        attempt.to_string()
    );
    assert_eq!(
        catalog["catalog"]["native_image_bases"][&key]["attempt_id"],
        attempt.to_string()
    );
    assert_eq!(
        catalog["catalog"]["recovery_fences"][&key]["scope"]["recovery_nonce"],
        fixture.scope.recovery_nonce.to_string()
    );
    assert_eq!(
        std::fs::read(source_log).unwrap(),
        original_source,
        "source permanent handoff promises remain untouched"
    );
}
