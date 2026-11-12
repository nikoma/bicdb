//! Born-v2 image row capability positive. Actual TLS host-MQ and all three
//! policies precede every ACK. Prepare/Commit/Certify and healthy Apply use TLS;
//! one target's NativeApply delivery is deliberately dropped. Its post-WAL cut
//! is an embedded trusted-caller replay of a captured actual origin instruction.
//! Image transfer is local opaque-object integration, not a wire/operator test.
use super::*;
use crate::distribution_consensus::MetadataRecoveryRoleRequest;
use crate::distribution_native_apply::{NativeOriginApplyRequest, NativeReplicaApplyObservation};
use crate::distribution_range_consensus::{
    ImageInstallPurpose, NativeImageInstallRole, NativeRangeCommitCertificate,
};
use crate::distribution_range_image::RangeImageCapture;
use crate::distribution_range_image_install::{
    materialize_actual_current_repair_image, NativeImageInstallFreeze,
};
use sha2::Digest;

#[derive(Debug)]
struct ImageLossTransport {
    inner: Arc<TcpClusterRelocationTransport>,
    captured: Mutex<BTreeMap<u64, NativeOriginApplyRequest>>,
}
impl RangeWriteTransport for ImageLossTransport {
    fn apply_range_write_repair(
        &self,
        n: &ClusterNodeId,
        b: &RangeWriteRepairBatch,
        limits: &RangeWriteRepairLimits,
    ) -> Result<RangeWriteProgress> {
        self.inner.apply_range_write_repair(n, b, limits)
    }
    fn fetch_range_write_repair(
        &self,
        n: &ClusterNodeId,
        r: RangeId,
        e: u64,
        after: u64,
        limits: &RangeWriteRepairLimits,
    ) -> Result<RangeWriteRepairBatch> {
        self.inner.fetch_range_write_repair(n, r, e, after, limits)
    }
    fn apply_range_write(&self, n: &ClusterNodeId, c: &RangeWriteCommand) -> Result<RangeWriteAck> {
        self.inner.apply_range_write(n, c)
    }
    fn apply_native_origin(
        &self,
        n: &ClusterNodeId,
        c: &RangeWriteCommand,
        q: &NativeRangeCommitCertificate,
        parent: &CancellationToken,
    ) -> Result<NativeReplicaApplyObservation> {
        let wire = NativeOriginApplyRequest::from_certificate(c, q, parent)?;
        self.captured.lock().insert(c.index, wire);
        if n == &node("n2") {
            return Err(crate::BicDbError::Cluster(
                "test drops native instruction delivery to n2".into(),
            ));
        }
        self.inner.apply_native_origin(n, c, q, parent)
    }
    fn observe_native_origin(
        &self,
        n: &ClusterNodeId,
        request: &crate::distribution_native_apply::NativeOriginStatusRequest,
        parent: &CancellationToken,
    ) -> Result<NativeReplicaApplyObservation> {
        self.inner.observe_native_origin(n, request, parent)
    }
    fn recertify_native_origin(
        &self,
        n: &ClusterNodeId,
        c: &RangeWriteCommand,
        parent: &CancellationToken,
    ) -> Result<RangeWriteAck> {
        self.inner.recertify_native_origin(n, c, parent)
    }
    fn validate_native_command_pre_admission(
        &self,
        voters: &[ClusterNodeId],
        c: &RangeWriteCommand,
    ) -> Result<()> {
        self.inner.validate_native_command_pre_admission(voters, c)
    }
    fn validate_native_origin_pre_admission(
        &self,
        voters: &[ClusterNodeId],
        c: &RangeWriteCommand,
        intent: &crate::distribution_range_consensus::NativeRangeCommandIntent,
    ) -> Result<()> {
        self.inner
            .validate_native_origin_pre_admission(voters, c, intent)
    }
    fn range_write_progress(
        &self,
        n: &ClusterNodeId,
        r: RangeId,
        e: u64,
    ) -> Result<RangeWriteProgress> {
        self.inner.range_write_progress(n, r, e)
    }
    fn probe_range_write(
        &self,
        n: &ClusterNodeId,
        r: RangeId,
        e: u64,
        i: u64,
    ) -> Result<RangeWriteProbe> {
        self.inner.probe_range_write(n, r, e, i)
    }
    fn prepare_range_write(
        &self,
        n: &ClusterNodeId,
        c: &RangeWriteCommand,
    ) -> Result<RangeWriteAck> {
        self.inner.prepare_range_write(n, c)
    }
    fn commit_range_write(
        &self,
        n: &ClusterNodeId,
        c: &RangeWriteCommand,
    ) -> Result<RangeWriteAck> {
        self.inner.commit_range_write(n, c)
    }
    fn certify_range_write(
        &self,
        n: &ClusterNodeId,
        c: &RangeWriteCommand,
    ) -> Result<RangeWriteAck> {
        self.inner.certify_range_write(n, c)
    }
    fn abort_range_write(&self, n: &ClusterNodeId, c: &RangeWriteCommand) -> Result<RangeWriteAck> {
        self.inner.abort_range_write(n, c)
    }
}
fn fresh_image_role(
    fixture: &EnforcedNetwork,
    range: &RangeDescriptor,
    nonce: Uuid,
    recipient: &ClusterNodeId,
) -> RecoveryRoleAdmission {
    let actor = fixture.clients[&node("n0")].local_metadata_read_participant();
    let local = fixture.clients[recipient].local_metadata_read_participant();
    let business = Uuid::now_v7();
    let challenge = Uuid::now_v7();
    let purpose = MetadataRecoveryRolePurpose::NormalRecovery;
    let request = MetadataRecoveryRoleRequest {
        protocol_version: crate::METADATA_READ_ACQUISITION_PROTOCOL_VERSION,
        acquisition_id: Uuid::now_v7(),
        read_request_id: business,
        initiator_node_id: actor.node_id().clone(),
        initiator_incarnation: actor.member_incarnation(),
        initiator_certificate_sha256: actor.certificate_sha256().map(str::to_owned),
        initiator_plaintext_loopback: actor.plaintext_loopback(),
        recipient_node_id: recipient.clone(),
        recipient_incarnation: local.member_incarnation(),
        range_id: range.id,
        range_epoch: range.epoch,
        recovery_nonce: nonce,
        recipient_challenge: challenge,
        purpose: purpose.clone(),
        budget_ms: 5000,
    };
    let witness = fixture.clients[recipient]
        .acquire_metadata_recovery_role_observation(
            &node("n1"),
            request,
            &CancellationToken::uncancelable(),
        )
        .unwrap();
    RecoveryRoleAdmission::from_witness(
        witness,
        &actor,
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

#[test]
fn native_born_v2_actual_policy_image_clear_copy_history_and_prepared_with_distinct_local_b() {
    let mut fixture = EnforcedNetwork::new();
    let source = node("n0");
    let target = node("n2");
    let range = fixture.ranges()[0].clone();
    let parent = CancellationToken::uncancelable();
    // Replace only the originating transport before the first application ACK.
    // Its policy is again minted from a genuine fresh HostMQ and installed;
    // the other two actual policies already exist at genesis R0.
    let transport = Arc::new(ImageLossTransport {
        inner: fixture.clients[&source].clone(),
        captured: Mutex::new(BTreeMap::new()),
    });
    let coordinator = Arc::new(
        RangeWriteCoordinator::new_schema_fenced(
            fixture.native.topology.read().cluster_id.clone(),
            source.clone(),
            fixture.native.topology.clone(),
            fixture.native.services[&source].clone(),
            transport.clone(),
        )
        .unwrap(),
    );
    let business = Uuid::now_v7();
    let host_mq = fixture.clients[&source]
        .acquire_host_metadata_observation(&node("n1"), business, Duration::from_secs(5), &parent)
        .unwrap();
    let policy = {
        let service = fixture.native.services[&source].lock();
        let db = fixture.native.databases[&source].read();
        service
            .prepare_native_host_writer_enrollment(&db, host_mq, business)
            .unwrap()
    };
    let host = fixture.native.databases[&source]
        .read()
        .install_enrolled_native_coordinator(coordinator.clone(), policy)
        .unwrap();
    coordinator.recover_local_leadership(8).unwrap();
    for id in fixture.native.databases.keys() {
        let service = fixture.native.services[id].lock();
        let db = fixture.native.databases[id].read();
        assert_eq!(
            db.current_commit_seq(),
            0,
            "all policy installation precedes first ACK"
        );
        service
            .require_native_writer_host_for_range(&db, &range)
            .unwrap();
    }
    let keys = [
        fixture.key(range.id, "born2-image-ack-a"),
        fixture.key(range.id, "born2-image-ack-b"),
    ];
    let ledger_path = fixture
        .native
        .root
        .path()
        .join("born2-image-ack-ledger.jsonl");
    let mut ledger = std::fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&ledger_path)
        .unwrap();
    let mut expected = Vec::new();
    for (offset, key) in keys.iter().enumerate() {
        fixture.native.append(key).unwrap();
        let record = fixture.native.databases[&source]
            .read()
            .get("items", key)
            .unwrap()
            .unwrap();
        let entry = fixture.native.services[&source]
            .lock()
            .probe_range_write_for_peer(&range, &source, (offset + 1) as u64)
            .unwrap()
            .entry
            .unwrap();
        let row = serde_json::to_vec(record.as_ref()).unwrap();
        let evidence = serde_json::json!({"logical_index":offset+1,"key":key,"record":record.as_ref(),"record_sha256":hex::encode(sha2::Sha256::digest(&row)),"command_id":entry.command.command_id,"command_sha256":entry.command.checksum_sha256,"application_commit_returned_ok":true});
        use std::io::Write;
        ledger
            .write_all(&serde_json::to_vec(&evidence).unwrap())
            .unwrap();
        ledger.write_all(b"\n").unwrap();
        ledger.sync_all().unwrap();
        expected.push(record);
    }
    drop(ledger);
    let source_b = fixture.native.databases[&source]
        .read()
        .current_commit_seq();
    assert_eq!(source_b, 2);
    let target_db = fixture.native.databases[&target].clone();
    let uid = target_db
        .read()
        .local_namespace_identity("items")
        .unwrap()
        .unwrap();
    assert_eq!(
        target_db
            .read()
            .current_native_range_source_lineage(range.id)
            .unwrap()
            .range_index(),
        0
    );
    assert!(keys
        .iter()
        .all(|key| target_db.read().get("items", key).unwrap().is_none()));
    // At least one target-local failed actual materialization leaves a physical
    // row to clear. This is NOT a new logical/user ACK or a fabricated import.
    let instruction = transport.captured.lock().get(&1).unwrap().clone();
    assert_eq!(instruction.command.index, 1);
    let actor = fixture.clients[&source].local_metadata_read_participant();
    // If the first origin cert loop stopped at its actual majority, deliver
    // exact command1 through real service operations before native replay.
    {
        let mut service = fixture.native.services[&target].lock();
        service
            .prepare_range_write(&range, &source, instruction.command.clone())
            .unwrap();
        service
            .commit_range_write_decision(&range, &source, &instruction.command)
            .unwrap();
        service
            .certify_range_write(&range, &source, &instruction.command)
            .unwrap();
    }
    BicDb::test_reject_next_native_after_local_wal();
    let error = fixture.runtimes[&target]
        .apply_native_ordinary(&actor, instruction, &parent)
        .unwrap_err();
    assert!(
        error.to_string().contains("post-WAL/pre-native-prefix"),
        "actual materialization cut: {error}"
    );
    assert_eq!(
        target_db.read().get("items", &keys[0]).unwrap(),
        Some(expected[0].clone())
    );
    assert_eq!(target_db.read().current_commit_seq(), 1);
    assert!(
        target_db
            .read()
            .current_native_range_source_lineage(range.id)
            .is_err(),
        "pending data is no certified floor"
    );
    // Public/generic projection remains forbidden even with installed policy.
    let before = target_db.read().current_commit_seq();
    let gate = target_db.read().range_mutation_gate();
    let mutation = gate.try_enter(range.id).unwrap();
    let raw = crate::CommitAdmissionMutation {
        collection: "items".into(),
        record_id: keys[0].clone(),
        record: Some((*expected[0]).clone()),
    };
    assert!(target_db
        .read()
        .apply_admitted_mutations_with_mutation(
            range.id,
            &[raw],
            &crate::distribution_range_mutation::RangeMutationAuthority::Mutation(&mutation)
        )
        .unwrap_err()
        .to_string()
        .contains("generic import"));
    drop(mutation);
    assert_eq!(target_db.read().current_commit_seq(), before);
    let nonce = Uuid::now_v7();
    let mut quorum = RecoveryFenceQuorumRound::begin(
        fresh_image_role(&fixture, &range, nonce, &source),
        &parent,
    )
    .unwrap();
    let scope = quorum.scope().clone();
    for id in [source.clone(), node("n1")] {
        let observation = fixture.clients[&source]
            .recovery_epoch_fence_observation(
                &id,
                &RecoveryEpochFenceRequest {
                    scope: scope.clone(),
                    budget_ms: 5000,
                },
                &parent,
            )
            .unwrap();
        quorum.record(Arc::new(observation)).unwrap();
    }
    // Direct embedded call uses the actual configured initiator identity; its
    // own fresh recipient role still comes from the real metadata TLS client.
    fixture.runtimes[&target]
        .fence_epoch(
            &actor,
            &RecoveryEpochFenceRequest {
                scope: scope.clone(),
                budget_ms: 5000,
            },
            &parent,
        )
        .unwrap();
    let authority = Arc::new(quorum.finish().unwrap());
    let frozen = fixture.runtimes[&source]
        .source_capture_freeze(&scope)
        .unwrap();
    let source_role = fresh_image_role(&fixture, &range, nonce, &source);
    let ready = fixture.native.services[&source]
        .lock()
        .attest_native_recovery_ready(source_role, frozen.clone(), &parent)
        .unwrap();
    assert_eq!(ready.range_index(), 2);
    let mut history = fixture.native.services[&source]
        .lock()
        .begin_native_recovery_source_history(authority, &ready, target.clone())
        .unwrap();
    let mut done = false;
    for _ in 0..16 {
        if fixture.native.services[&source]
            .lock()
            .advance_native_recovery_source_history(&mut history, &ready, &parent)
            .unwrap()
        {
            done = true;
            break;
        }
    }
    assert!(done);
    let cut = fixture.native.services[&source]
        .lock()
        .finish_native_recovery_source_history(history, &ready)
        .unwrap();
    let lineage = fixture.native.databases[&source]
        .read()
        .current_native_range_source_lineage(range.id)
        .unwrap();
    let source_h = lineage.logical_history_sha256().to_owned();
    drop(ready);
    let mut capture = Some(
        RangeImageCapture::begin(
            fixture.native.databases[&source].clone(),
            cut,
            frozen,
            lineage,
            Duration::from_secs(30),
            &parent,
        )
        .unwrap(),
    );
    let attempt = Uuid::now_v7();
    let role = fresh_image_role(&fixture, &range, nonce, &target);
    let authorization = capture
        .as_ref()
        .unwrap()
        .authorize_target_install()
        .unwrap();
    let lease = fixture.runtimes[&target]
        .transfer_image_install(
            &scope,
            &authorization,
            &NativeImageInstallRole::CurrentVoterRepair(&role),
            ImageInstallPurpose::CurrentVoterRepair,
            attempt,
            &parent,
        )
        .unwrap();
    let install = fixture.native.services[&target]
        .lock()
        .begin_native_range_image_install(
            &authorization,
            &NativeImageInstallRole::CurrentVoterRepair(&role),
            lease,
            ImageInstallPurpose::CurrentVoterRepair,
            attempt,
            &parent,
        )
        .unwrap();
    drop(authorization);
    drop(role);
    let service = fixture.native.services[&target].clone();
    let verified = materialize_actual_current_repair_image(
        &mut capture,
        &service,
        || fresh_image_role(&fixture, &range, nonce, &target),
        || {},
        install,
    );
    assert_eq!(verified.base().range_index(), 2);
    assert_eq!(verified.base().logical_history_sha256(), source_h);
    let target_b = verified.base().target_snapshot_bound().get();
    assert_eq!(
        target_b, 3,
        "actual clear B2 and batch copy B3, distinct from R2/sourceB2"
    );
    assert_ne!(target_b, verified.base().range_index());
    let receipt = verified.base().native_receipt_sha256().to_owned();
    let role = fresh_image_role(&fixture, &range, nonce, &target);
    let adopted = service
        .lock()
        .adopt_native_range_image_catalogue(
            verified,
            &NativeImageInstallRole::CurrentVoterRepair(&role),
            &parent,
        )
        .unwrap();
    drop(role);
    let role = fresh_image_role(&fixture, &range, nonce, &target);
    let (freeze, finalization) = service
        .lock()
        .finalize_native_range_image_catalogue(
            adopted,
            &NativeImageInstallRole::CurrentVoterRepair(&role),
            &parent,
        )
        .unwrap();
    let NativeImageInstallFreeze::Recovery(lease) = freeze else {
        panic!("actual Runtime-owned image lease required")
    };
    drop(role);
    let role = fresh_image_role(&fixture, &range, nonce, &target);
    let image = lease
        .return_after_catalogue_adoption(
            finalization,
            &NativeImageInstallRole::CurrentVoterRepair(&role),
            &parent,
        )
        .unwrap();
    drop(role);
    let frozen = fixture.runtimes[&target]
        .source_capture_freeze(&scope)
        .unwrap();
    let role = fresh_image_role(&fixture, &range, nonce, &target);
    let prepared = service
        .lock()
        .prepare_finalized_native_recovery_image(image, role, frozen.clone(), &parent)
        .unwrap();
    assert_eq!(prepared.range_index(), 2);
    assert_eq!(prepared.local_visibility_sequence(), target_b);
    assert_eq!(prepared.logical_history_sha256(), source_h);
    assert_eq!(prepared.native_receipt_sha256(), receipt);
    for (key, record) in keys.iter().zip(expected.iter()) {
        assert_eq!(
            target_db.read().get("items", key).unwrap(),
            Some(record.clone())
        );
    }
    assert_eq!(
        target_db.read().local_namespace_identity("items").unwrap(),
        Some(uid)
    );
    assert!(
        gate.try_enter(range.id).is_err(),
        "Prepared image stays frozen"
    );
    assert!(
        gate.try_controlled_mutation(&frozen).is_err(),
        "physical preparation pins exclude writers"
    );
    host.require_for_coordinator(
        &fixture.native.databases[&source].read(),
        &range,
        &coordinator,
    )
    .unwrap();
    let report = serde_json::json!({"source_acknowledged_commands":2,"target_failed_replay_is_user_ack":false,"source_R":2,"source_B":source_b,"target_before_image_B":before,"target_installed_B":target_b,"target_R":prepared.range_index(),"logical_history_sha256":source_h,"target_namespace_uid":uid,"native_receipt_sha256":receipt,"actual_clear_and_copy":true,"full_history_entries":3,"target_stage":"PreparedFrozen","all_policies_before_first_ack":true,"host_mq_tls":true,"image_transfer_tls":false,"physical_qualification":false,"activation":false,"ack_ledger_sha256":hex::encode(sha2::Sha256::digest(std::fs::read(&ledger_path).unwrap())),"target_materialization_error":error.to_string()});
    let report_path = fixture.native.root.path().join("born2-image-result.json");
    {
        use std::io::Write;
        let mut output = std::fs::File::create(&report_path).unwrap();
        output
            .write_all(&serde_json::to_vec_pretty(&report).unwrap())
            .unwrap();
        output.sync_all().unwrap();
    }
    fixture.native.root.disable_cleanup(true);
    eprintln!(
        "BORN2_IMAGE_STOPPED_ROOT {}",
        fixture.native.root.path().display()
    );
    eprintln!("BORN2_IMAGE_TYPED_GRANT_RESULT {report}");
    drop(prepared);
    fixture.idle();
}
