//! REQUIRED baseline: the real fifth has no INITIAL ranges or writer policy.
//! Passing only a refusal assertion must never count as successful movement.
use super::*;
use crate::distribution::{
    RangeHandoffSession, RangeRelocation, RangeReplica, RelocationId, RelocationPhase, ReplicaId,
    ReplicaMoveReason,
};
use crate::distribution_metadata_read_provider::MetadataReadProvider;
use crate::distribution_native_host::NativeMetadataControlHost;
use crate::distribution_range_consensus::RecoveryFenceScope;
use crate::distribution_range_recovery::{RecoveryEpochFenceRequest, RecoveryRoleAdmission};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;

mod closed_fence_controls;
mod custody;
mod placement_cases;
mod remote_source_tests;
use closed_fence_controls::Case;
use placement_cases::AssignmentMode;

fn budget(ms: u64) -> CancellationToken {
    CancellationToken::uncancelable()
        .child_with_deadline(Instant::now() + Duration::from_millis(ms))
}
fn state(
    database: &Arc<RwLock<BicDb>>,
    topology: &ClusterTopology,
    local: &ClusterNodeId,
    ranges: &[crate::RangeDescriptor],
) -> Result<serde_json::Value> {
    let db = database
        .try_read()
        .ok_or_else(|| BicDbError::Cluster("baseline actual DB busy during snapshot".into()))?;
    let admission = db.native_recovery_admission(
        &topology.cluster_id,
        topology.range_image_bootstrap_id.unwrap(),
        local,
        topology.nodes[local].incarnation,
    )?;
    let mut rows = BTreeMap::new();
    for collection in db.collections() {
        let mut actual = db.scan_collection(&collection.name)?;
        actual.sort_by(|a, b| a.id.cmp(&b.id));
        rows.insert(collection.name, actual);
    }
    let points = ranges
        .iter()
        .map(|range| Ok((range.id, db.native_range_prefix_observation(range.id)?)))
        .collect::<Result<BTreeMap<_, _>>>()?;
    let raw = std::fs::read(
        db.data_path()
            .join(crate::distribution_range_consensus::DEFAULT_RANGE_WRITE_LOG),
    )?;
    Ok(serde_json::json!({
        "open":db.incarnation_id(),"B":db.current_commit_seq(),
        "birth":crate::distribution::NativeStoreBirthV1::from_actual_native(&admission)?,
        "schema":db.schema_compatibility_fingerprint()?.sha256,
        "namespace_ids":db.capture_range_image_bindings()?.namespace_ids(),
        "protected_catalogue_uuid":db.native_abort_catalogue_store_id()?,
        "points":points,"slots":db.native_abort_slot_inventory()?,
        "rows":rows,"raw":raw,"writer_installed":db.commit_admission_ready(),
    }))
}
// Intent persistence is mandatory before an API starts. Once an actual API
// has returned, evidence IO failures remain separate from its primary result.
fn evidence(path: &std::path::Path, value: &impl serde::Serialize) -> Result<()> {
    crate::storage::write_atomic(path, &serde_json::to_vec_pretty(value)?, true)
}
fn evidence_after(path: &std::path::Path, value: &impl serde::Serialize, errors: &mut Vec<String>) {
    if let Err(error) = evidence(path, value) {
        errors.push(format!("{}: {error}", path.display()));
    }
}
fn configuration(
    template: &crate::DistributionConfig,
    topology: &ClusterTopology,
    local: &ClusterNodeId,
    network: &ClusterNetworkTransportConfig,
) -> crate::DistributionConfig {
    let member = &topology.nodes[local];
    let mut result = template.clone();
    result.node_id = local.clone();
    result.node_address = member.address.clone();
    result.node_incarnation = member.incarnation;
    result.node_capacity_bytes = member.capacity_bytes;
    result.node_tls_certificate_sha256 = member.tls_certificate_sha256.clone();
    result.transport = network.clone();
    result
}
fn assign_two_ranges(
    current: &ClusterTopology,
    ranges: &[crate::RangeDescriptor],
) -> ClusterTopology {
    let mut proposed = current.clone();
    assert_eq!(ranges.len(), 2);
    assert_eq!(
        proposed.nodes[&node(4)].metadata_role,
        MetadataMemberRole::Voter
    );
    assert!(proposed.nodes[&node(4)].native_store_birth_v1.is_some());
    assert!(proposed.ranges.values().all(|range| range
        .replicas
        .iter()
        .all(|replica| replica.node_id != node(4))));
    for original in ranges {
        let id = RelocationId::new(proposed.next_relocation_id).unwrap();
        proposed.next_relocation_id += 1;
        let target_replica = ReplicaId::new(proposed.next_replica_id).unwrap();
        proposed.next_replica_id += 1;
        let range = proposed
            .ranges
            .values_mut()
            .find(|r| r.id == original.id)
            .unwrap();
        assert_eq!(&*range, original);
        range.epoch += 1;
        range.replicas.push(RangeReplica {
            id: target_replica,
            node_id: node(4),
            role: RangeReplicaRole::Learner,
        });
        proposed.relocations.insert(
            id,
            RangeRelocation {
                id,
                plan_id: format!("actual-registered-fifth-closed-image-{}", original.id),
                handoff_session: Some(RangeHandoffSession {
                    session_id: Uuid::now_v7(),
                    nonce: Uuid::now_v7(),
                }),
                native_joint_handoff_v1: None,
                range_id: original.id,
                expected_epoch: original.epoch,
                learner_epoch: original.epoch + 1,
                promoted_epoch: None,
                source: Some(original.leader.clone()),
                target: node(4),
                target_replica_id: target_replica,
                reason: ReplicaMoveReason::LoadBalance,
                phase: RelocationPhase::LearnerAllocated,
                resume_phase: None,
                snapshot_id: None,
                snapshot_sha256: None,
                snapshot_bytes_copied: 0,
                snapshot_resume_after_key: None,
                snapshot_commit_sequence: None,
                destination_durable_commit_sequence: None,
                source_commit_sequence: None,
                cleanup_resume_after_key: None,
                cleanup_records_deleted: 0,
                created_at_ms: 100,
                updated_at_ms: 100,
                last_error: None,
            },
        );
    }
    proposed.generation += 1;
    proposed.validate().unwrap();
    proposed
}
fn key_in(topology: &ClusterTopology, range: crate::RangeId) -> String {
    (0..100_000)
        .map(|i| format!("registered-fifth-ack-{}-{i}", range))
        .find(|key| {
            let id = crate::cluster_event_record_id("items", key).unwrap();
            topology
                .range_for_key(crate::CLUSTER_EVENTS_COLLECTION, &id)
                .unwrap()
                .id
                == range
        })
        .unwrap()
}

#[test]
fn native_registered_fifth_actual_closed_handoff_fence_without_initial_writer_baseline() {
    run_registered_image_baseline(Case::Required);
}
fn run_registered_image_baseline(case: Case) {
    run_registered_image_baseline_with_placement(case, AssignmentMode::OriginalLeader);
}
fn run_registered_image_baseline_with_placement(case: Case, assignment: AssignmentMode) {
    run_registered_image_baseline_with_placement_and_source(case, assignment, None);
}
fn run_registered_image_baseline_with_source(
    case: Case,
    source_case: Option<remote_source_tests::SourceCase>,
) {
    run_registered_image_baseline_with_placement_and_source(
        case,
        AssignmentMode::OriginalLeader,
        source_case,
    );
}
fn run_registered_image_baseline_with_placement_and_source(
    case: Case,
    assignment: AssignmentMode,
    source_case: Option<remote_source_tests::SourceCase>,
) {
    let mut f = EnrollmentFixture::new();
    let (factory, mut host) = f.factory("actual-registered-fifth-image-target");
    assert!(factory.spec.initial_ranges.is_empty());
    let token = CancellationToken::uncancelable();
    let template = config(&f);
    let protected_metadata_uuid = start_owned(&f, &mut host);
    let readiness = ready(&f, &token);
    let (post, business) = f.current();
    let entry = f.launches[0]
        .metadata
        .lock()
        .propose_native_learner_promotion(&readiness, &post, business, &template, 71, &token)
        .unwrap()
        .unwrap();
    f.launches[0].heartbeat_initial_metadata(&token).unwrap();
    let request = f.launches[0]
        .metadata
        .lock()
        .append_request_from(entry.index, 16)
        .unwrap();
    let ack = f.launches[0]
        .transport
        .append_metadata(&node(4), request)
        .unwrap();
    assert!(ack.success && ack.native_provenance.is_some() && ack.match_index >= entry.index);
    assert_eq!(
        ack.native_receiver.as_ref().unwrap().metadata_store_id(),
        protected_metadata_uuid
    );
    f.launches[0]
        .metadata
        .lock()
        .record_append_response(&node(4), &ack)
        .unwrap();
    let own_business = Uuid::now_v7();
    let own = f
        .learner_transport
        .acquire_host_metadata_observation(&node(0), own_business, Duration::from_secs(5), &token)
        .unwrap();
    let promoted = own.committed_topology(own_business).unwrap().clone();
    let own_config = configuration(&template, &promoted, &node(4), &f.networks[4]);
    host.enable_metadata_control(&own, own_business, own_config, &token)
        .unwrap();
    // This existing producer is still metadata-only/no-assignment. It is
    // invoked BEFORE Pending placement; no factory predicate is loosened.
    host.open_registered_replica_receiver(&own, own_business, &token)
        .unwrap();
    let receiver = host.replica_receiver_status(&token).unwrap().unwrap();
    assert!(receiver.receiver_ready && !receiver.data_serving);
    assert_eq!(receiver.metadata_store_id, protected_metadata_uuid);
    let target_service = host.test_service();
    let target_metadata = host.test_owned_metadata().unwrap();
    let target_slot = target_service
        .lock()
        .native_replica_listener_store_open_guard();
    let original_birth = factory.actual_birth().clone();
    let original_factory_path = factory.database.read().data_path().to_path_buf();
    assert_eq!(receiver.birth_id, original_birth.birth_id());
    assert!(!factory.database.read().commit_admission_ready());
    let original_namespaces = factory
        .database
        .read()
        .capture_range_image_bindings()
        .unwrap()
        .namespace_ids();

    let databases = f
        .seeds
        .iter()
        .map(|seed| seed.database.clone())
        .chain(std::iter::once(factory.database.clone()))
        .collect::<Vec<_>>();
    let metadata = f
        .launches
        .iter()
        .map(|launch| launch.metadata.clone())
        .chain(std::iter::once(target_metadata.clone()))
        .collect::<Vec<_>>();
    let services = f
        .launches
        .iter()
        .map(|launch| launch.service.clone())
        .chain(std::iter::once(target_service.clone()))
        .collect::<Vec<_>>();
    let clients = f
        .launches
        .iter()
        .map(|launch| launch.transport.clone())
        .collect::<Vec<_>>();
    let configs = (0..4)
        .map(|i| configuration(&template, &promoted, &node(i), &f.networks[i]))
        .collect::<Vec<_>>();
    let launches = std::mem::take(&mut f.launches);
    let listeners = std::mem::take(&mut f.listeners);
    let mut controls = launches
        .into_iter()
        .zip(listeners)
        .enumerate()
        .map(|(i, (launch, listener))| {
            NativeMetadataControlHost::new(
                launch,
                listener,
                f.networks[i].clone(),
                configs[i].clone(),
            )
        })
        .collect::<Vec<_>>();
    let output = std::env::var_os("BICDB_TEST_EVIDENCE_ROOT")
        .map(PathBuf::from)
        .unwrap_or_else(|| f.root.path().join("registered-image-diagnostics"))
        .join(format!("native-registered-fifth-image-{}", Uuid::now_v7()));
    std::fs::create_dir_all(&output).unwrap();
    let mut phase = "four_initial_control";
    let mut reports = Vec::new();
    let mut ledger = Vec::new();
    let mut evidence_errors = Vec::new();
    let clock_origin = Instant::now();
    let mut metadata_only_refusal = None;
    // Keep the actual original guards outside catch_unwind too: diagnostic
    // capture must not drop them on a partial setup/operation failure.
    let mut original_writers = Vec::new();
    let mut automatic_plan = None;
    let mut ranges = promoted
        .ranges
        .values()
        .take(2)
        .cloned()
        .collect::<Vec<_>>();
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        for control in &mut controls {
            control.advance(&budget(5000)).unwrap();
        }
        phase = "four_initial_data_open";
        let until = Instant::now() + Duration::from_secs(20);
        let mut opened = [false; 4];
        while !opened.iter().all(|ok| *ok) {
            for (i, control) in controls.iter_mut().enumerate() {
                if !opened[i] {
                    opened[i] = control.open_initial_data(&budget(1500)).is_ok();
                }
            }
            assert!(
                Instant::now() < until,
                "four actual INITIAL DATA receivers not ready: {opened:?}"
            );
        }
        original_writers = controls
            .iter()
            .map(|control| control.test_retained_initial_data_writer_guard().unwrap())
            .collect::<Vec<_>>();
        let current = metadata[0].lock().committed_topology().clone();
        let selected = placement_cases::select(&current, &configs[0], assignment);
        ranges = selected.0;
        automatic_plan = selected.1;
        if let Some(plan) = &automatic_plan {
            evidence(&output.join("automatic-placement-plan.json"), &serde_json::json!({
                "input_topology":current,"input_config":configs[0],"plan":plan,
                "planner_now_ms":current.nodes.values().map(|n|n.last_heartbeat_ms).max().unwrap(),
                "liveness_clock_is_fixture_topology_clock_not_physical_measurement":true,
                "leader_transfers_not_applied_before_real_joint_proofs":true,
                "test_only_consensus_proposal_adapter_not_automatic_controller":true
            })).unwrap();
        }
        let gateway = crate::ClusterEventClient::new(&configs[0], current.clone())
            .unwrap()
            .with_request_timeout(Duration::from_secs(2))
            .unwrap();
        phase = "two_real_source_acks";
        for range in &ranges {
            let key = key_in(&current, range.id);
            let value = serde_json::json!({"registered_fifth_source":range.id.get(),"object":{},"nested":[null,17,{"full":"payload"}]});
            let record_id = crate::cluster_event_record_id("items", &key).unwrap();
            let logical_id = Uuid::now_v7();
            ledger.push(serde_json::json!({
                "logical_id":logical_id,"phase":"intent","operation":"ClusterEventClient.append",
                "range":range.id,"range_epoch":range.epoch,"topology_generation":current.generation,
                "actual_api_caller":node(0),"actual_api_caller_address":current.nodes[&node(0)].address,
                "expected_route_leader":range.leader,"expected_route_address":current.nodes[&range.leader].address,
                "configured_request_timeout_ms":2000,"configured_network":configs[0].transport,
                "collection":"items","key":key,"record_id":record_id,"value":value,
                "payload_json_sha256":hex::encode(Sha256::digest(serde_json::to_vec(&value).unwrap())),
                "configuration":current,"logical_ledger_not_raw_wire":true,
                "outcome":"not_started","effect_unknown_on_error":false}));
            evidence(&output.join("ack-ledger.json"), &ledger).unwrap();
            let append = gateway.append("items", &key, value.clone());
            // Preserve the actual API error first; failed evidence IO cannot
            // replace it. Err means unknown effect, never a no-Commit claim.
            let outcome = ledger.last_mut().unwrap();
            outcome["phase"] = "reply".into();
            outcome["outcome"] = if append.is_ok() {
                "returned"
            } else {
                "unknown_effect"
            }
            .into();
            outcome["actual_api_inserted"] = serde_json::to_value(append.as_ref().ok()).unwrap();
            outcome["actual_api_error"] =
                serde_json::to_value(append.as_ref().err().map(|e| e.to_string())).unwrap();
            outcome["effect_unknown_on_error"] = append.is_err().into();
            evidence_after(
                &output.join("ack-ledger.json"),
                &ledger,
                &mut evidence_errors,
            );
            assert!(append.unwrap());
            let leader = (0..4).find(|i| node(*i) == range.leader).unwrap();
            let record = databases[leader]
                .read()
                .get(crate::CLUSTER_EVENTS_COLLECTION, &record_id)
                .unwrap()
                .unwrap();
            assert_eq!(record.metadata, value);
            ledger.last_mut().unwrap()["actual_full_record"] =
                serde_json::to_value(record).unwrap();
            evidence_after(
                &output.join("ack-ledger.json"),
                &ledger,
                &mut evidence_errors,
            );
        }
        phase = "commit_two_data_learner_assignments";
        let pending = match &automatic_plan {
            Some(plan) => placement_cases::pending(&current, &ranges, plan),
            None => assign_two_ranges(&current, &ranges),
        };
        let entry = metadata[0]
            .lock()
            .propose_topology(pending.clone())
            .unwrap();
        controls[0].heartbeat(&budget(5000)).unwrap();
        // A second genuine heartbeat advertises the committed prefix; it does
        // not create another index, role, vote or promotion declaration.
        controls[0].heartbeat(&budget(5000)).unwrap();
        for actual in &metadata {
            let store = actual.lock();
            assert!(store.status().commit_index >= entry.index);
            assert_eq!(store.committed_topology(), &pending);
        }
        for client in &clients {
            client.install_topology(pending.clone()).unwrap();
        }
        let target_transport = Arc::new(
            TcpClusterRelocationTransport::new(
                pending.cluster_id.clone(),
                node(4),
                Arc::new(RwLock::new(pending.clone())),
                f.networks[4].clone(),
            )
            .unwrap(),
        );
        let target_provider =
            MetadataReadProvider::new(target_metadata.clone(), target_transport.clone()).unwrap();
        phase = "current_registered_identity_is_not_assigned_image_admission";
        let own_parent = budget(5000);
        let business = Uuid::now_v7();
        let actual_own = target_transport
            .acquire_host_metadata_observation(
                &node(0),
                business,
                Duration::from_secs(5),
                &own_parent,
            )
            .unwrap();
        let actual_member = {
            let store = target_metadata.lock();
            store
                .observe_owned_native_metadata_member(&actual_own, business, &own_parent)
                .unwrap()
        };
        // Actual protected identity and DB verification are not inferred from
        // getters. No metadata guard spans this DB validation.
        actual_member
            .require_database(&factory.database.read())
            .unwrap();
        actual_member
            .require_current(&target_metadata.lock())
            .unwrap();
        let no_assignment_error = factory
            .require_owned_metadata_member(&actual_member, &actual_own, business, &f.networks[4])
            .unwrap_err();
        assert!(
            no_assignment_error.to_string().contains("data placement"),
            "{no_assignment_error}"
        );
        own_parent.check().unwrap();
        actual_own.require_live_request(business).unwrap();
        metadata_only_refusal = Some(no_assignment_error.to_string());
        phase = "actual_registered_fifth_fence";
        let initial = state(&factory.database, &pending, &node(4), &ranges).unwrap();
        assert_eq!(initial["writer_installed"], false);
        for point in initial["points"].as_object().unwrap().values() {
            assert!(point["native_applied_index"].is_null());
        }
        let mut succeeded = 0;
        for original in &ranges {
            for (i, guard) in original_writers.iter().enumerate() {
                assert!(Arc::ptr_eq(
                    guard,
                    &controls[i]
                        .test_retained_initial_data_writer_guard()
                        .unwrap()
                ));
                guard
                    .require_current(&databases[i].read(), original)
                    .unwrap();
            }
            let relocation = pending
                .relocations
                .values()
                .find(|plan| plan.range_id == original.id)
                .unwrap();
            let session = relocation.handoff_session.as_ref().unwrap();
            let initiator_index = if assignment == AssignmentMode::MetadataLeader {
                0
            } else {
                (0..4).find(|i| node(*i) == original.leader).unwrap()
            };
            let actor = clients[initiator_index].local_metadata_read_participant();
            let business = Uuid::now_v7();
            let challenge = Uuid::now_v7();
            let purpose =
                crate::distribution_consensus::MetadataRecoveryRolePurpose::CommittedHandoff {
                    relocation_id: relocation.id,
                    plan_id: relocation.plan_id.clone(),
                    session_id: session.session_id,
                    nonce: session.nonce,
                };
            let range = pending.range_by_id(original.id).unwrap();
            // This attempt's prestate includes any previous legitimate fence.
            // Capture it and durable intent BEFORE the original five-second
            // acquisition begins; no replacement token is created later.
            let before = state(&factory.database, &pending, &node(4), &ranges).unwrap();
            let report = reports.len();
            reports.push(serde_json::json!({
                "logical_id":Uuid::now_v7(),"operation":"recovery_epoch_fence_observation","phase":"intent",
                "range":range.id,"range_epoch":range.epoch,"topology_generation":pending.generation,
                "actual_source":{"node_id":actor.node_id(),"incarnation":actor.member_incarnation(),
                    "certificate_sha256":actor.certificate_sha256(),"plaintext_loopback":actor.plaintext_loopback()},"source_address":pending.nodes[actor.node_id()].address,
                "retiring_replica":relocation.source,"current_range_leader":range.leader,"assignment_mode":format!("{assignment:?}"),
                "actual_target":node(4),"target_address":pending.nodes[&node(4)].address,
                "business_id":business,"recipient_challenge":challenge,"purpose":purpose,
                "configured_parent_budget_ms":5000,"configured_request_budget_ms":4000,
                "configured_source_network":f.networks[initiator_index],
                "configured_target_network":f.networks[4],"configuration":pending,
                "logical_ledger_not_raw_wire":true,"before":before,
                "rows_copied":false,"serving_granted":false,"outcome":"not_started"}));
            evidence(&output.join("fence-ledger.json"), &reports).unwrap();
            let parent = budget(5000);
            reports[report]["original_parent_deadline_from_clock_origin_us"] =
                serde_json::to_value(
                    parent
                        .deadline()
                        .map(|d| d.saturating_duration_since(clock_origin).as_micros()),
                )
                .unwrap();
            let observed = target_provider.witness_recovery_role(
                &actor,
                business,
                range.id,
                range.epoch,
                session.nonce,
                challenge,
                purpose.clone(),
                &parent,
            );
            reports[report]["role_acquisition_error"] =
                serde_json::to_value(observed.as_ref().err().map(|e| e.to_string())).unwrap();
            evidence_after(
                &output.join("fence-ledger.json"),
                &reports,
                &mut evidence_errors,
            );
            let role = RecoveryRoleAdmission::from_witness(
                observed.unwrap(),
                &actor,
                &node(4),
                business,
                range.id,
                range.epoch,
                session.nonce,
                challenge,
                &purpose,
            );
            reports[report]["role_admission_error"] =
                serde_json::to_value(role.as_ref().err().map(|e| e.to_string())).unwrap();
            evidence_after(
                &output.join("fence-ledger.json"),
                &reports,
                &mut evidence_errors,
            );
            let role = role.unwrap();
            let scope = RecoveryFenceScope::from_role(&role).unwrap();
            reports[report]["scope"] = serde_json::to_value(&scope).unwrap();
            reports[report]["original_role_deadline_from_clock_origin_us"] = serde_json::to_value(
                role.deadline()
                    .saturating_duration_since(clock_origin)
                    .as_micros(),
            )
            .unwrap();
            reports[report]["phase"] = "dispatch".into();
            evidence(&output.join("fence-ledger.json"), &reports).unwrap();
            parent.check().unwrap();
            role.require_live_request().unwrap();
            let sent = Instant::now();
            let response = clients[initiator_index].recovery_epoch_fence_observation(
                &node(4),
                &RecoveryEpochFenceRequest {
                    scope: scope.clone(),
                    budget_ms: 4000,
                },
                &parent,
            );
            let elapsed = sent.elapsed();
            let caller_live = parent.check();
            let role_live = role.require_live_request();
            reports[report]["phase"] = "reply".into();
            reports[report]["outcome"] = if response.is_ok() {
                "returned"
            } else {
                "error_reply_unknown_effect"
            }
            .into();
            reports[report]["receipt_elapsed_us"] =
                serde_json::to_value(elapsed.as_micros()).unwrap();
            reports[report]["original_parent_remaining_ms"] = serde_json::to_value(
                parent
                    .deadline()
                    .map(|d| d.saturating_duration_since(Instant::now()).as_millis()),
            )
            .unwrap();
            reports[report]["original_role_remaining_ms"] = serde_json::to_value(
                role.deadline()
                    .saturating_duration_since(Instant::now())
                    .as_millis(),
            )
            .unwrap();
            reports[report]["actual_reply_ok"] = response.is_ok().into();
            reports[report]["actual_success_observation"] = response.as_ref().ok().map(|actual|serde_json::json!({
                "scope":actual.scope(),"node_id":actual.node_id(),"member_incarnation":actual.member_incarnation(),
                "store_open_id":actual.store_open_id(),"native_database_incarnation":actual.native_database_incarnation(),
                "progress":actual.progress(),"native":actual.native(),"image_history":actual.image_history(),
                "is_active_recovery":actual.is_active_recovery(),
                "original_observation_deadline_from_clock_origin_us":actual.deadline().saturating_duration_since(clock_origin).as_micros()
            })).unwrap_or(serde_json::Value::Null);
            reports[report]["actual_error"] =
                serde_json::to_value(response.as_ref().err().map(|e| e.to_string())).unwrap();
            reports[report]["caller_error_at_reply"] =
                serde_json::to_value(caller_live.as_ref().err().map(|e| e.to_string())).unwrap();
            reports[report]["role_error_at_reply"] =
                serde_json::to_value(role_live.as_ref().err().map(|e| e.to_string())).unwrap();
            evidence_after(
                &output.join("fence-ledger.json"),
                &reports,
                &mut evidence_errors,
            );
            caller_live.unwrap();
            role_live.unwrap();
            let after = state(&factory.database, &pending, &node(4), &ranges).unwrap();
            if response.is_err() {
                assert_eq!(
                    after, before,
                    "failed fence changed actual target native/raw state"
                );
            }
            let status = host.replica_receiver_status(&token).unwrap().unwrap();
            assert_eq!(status.range_store_open_id, target_slot.id());
            assert_eq!(status.metadata_store_id, protected_metadata_uuid);
            assert_eq!(status.database_incarnation, receiver.database_incarnation);
            assert_eq!(status.birth_id, original_birth.birth_id());
            target_slot.require_live().unwrap();
            assert!(!factory.database.read().commit_admission_ready());
            assert_eq!(
                factory
                    .database
                    .read()
                    .capture_range_image_bindings()
                    .unwrap()
                    .namespace_ids(),
                original_namespaces
            );
            assert_eq!(
                target_metadata
                    .lock()
                    .registered_native_metadata_member_store_id()
                    .unwrap(),
                protected_metadata_uuid
            );
            succeeded += usize::from(response.is_ok());
            reports[report]["original_role_and_caller_live_at_reply"] = true.into();
            reports[report]["after"] = after;
            reports[report]["receiver"] = serde_json::to_value(status).unwrap();
            evidence_after(
                &output.join("fence-ledger.json"),
                &reports,
                &mut evidence_errors,
            );
        }
        for (original, acknowledged) in ranges.iter().zip(&ledger) {
            for (i, guard) in original_writers.iter().enumerate() {
                assert!(Arc::ptr_eq(
                    guard,
                    &controls[i]
                        .test_retained_initial_data_writer_guard()
                        .unwrap()
                ));
                guard
                    .require_current(&databases[i].read(), original)
                    .unwrap();
            }
            let key = acknowledged["key"].as_str().unwrap();
            let id = crate::cluster_event_record_id("items", key).unwrap();
            let leader = (0..4).find(|i| node(*i) == original.leader).unwrap();
            let actual = databases[leader]
                .read()
                .get(crate::CLUSTER_EVENTS_COLLECTION, &id)
                .unwrap()
                .unwrap();
            assert_eq!(
                serde_json::to_value(actual).unwrap(),
                acknowledged["actual_full_record"]
            );
        }
        phase = "required_fence_acceptance";
        assert_eq!(succeeded,2,"REQUIRED genuine registered fifth CommittedHandoff fences unavailable; no INITIAL writer substitute");
        if let Some(source_case) = source_case {
            phase = "actual_local_old_quorum_seeded_remote_source_frames";
            remote_source_tests::exercise(
                source_case,
                &controls,
                &clients,
                &services,
                &metadata,
                &target_transport,
                host.test_registered_image_control().unwrap(),
                &host.test_registered_receiver_parts().unwrap().1,
                &target_service,
                &target_metadata,
                &factory,
                &pending,
                &ranges,
                &ledger,
                &output,
                &original_writers,
                &token,
            );
        } else if case != Case::Required {
            phase = "captured_valid_registered_closed_fence_control";
            closed_fence_controls::run(
                case,
                &mut host,
                &factory,
                &target_service,
                &services[0],
                &target_provider,
                &clients,
                &pending,
                &ranges,
                &output,
                &mut reports,
                &f.networks[4],
            );
        }
    }));
    let capture = custody::capture(&output, &databases, &services, &metadata);
    let (captured, capture_error) = match capture {
        Ok(value) => (Some(value), None),
        Err(error) => (None, Some(error.to_string())),
    };
    let capture_ok = capture_error.is_none();
    let primary_error = result.as_ref().err().map(|panic| {
        panic
            .downcast_ref::<String>()
            .cloned()
            .or_else(|| panic.downcast_ref::<&str>().map(|s| s.to_string()))
            .unwrap_or_else(|| "non-string actual baseline panic".into())
    });
    let terminal = serde_json::json!({"source":std::env::var("BICDB_TEST_SOURCE_SHA").ok(),"package_version":env!("CARGO_PKG_VERSION"),
        "phase":phase,"required_baseline_succeeded":result.is_ok(),"reports":reports,"ack_ledger":ledger,"assignment_mode":format!("{assignment:?}"),"actual_automatic_plan":automatic_plan,
        "primary_error":primary_error,"existing_metadata_only_validator_refusal":metadata_only_refusal,
        "original_root":f.root.path(),"factory_root":original_factory_path,
        "initial_ranges":factory.spec.initial_ranges,"original_birth":original_birth,
        "protected_metadata_uuid":protected_metadata_uuid,"store_slot":target_slot.id(),
        "actual_original_writer_guards_retained":original_writers.len(),
        "custody":captured,"custody_error":capture_error,"evidence_errors_so_far":evidence_errors,
        "old_closed_data_quorum_exercised":false,"source_image_capture_exercised":false,
        "local_image_source_session_wire_not_exercised":true,"no_registered_row_or_new_data_quorum_claim":true});
    evidence_after(
        &output.join("terminal.json"),
        &terminal,
        &mut evidence_errors,
    );
    if !capture_ok {
        evidence_after(
            &output.join("custody-partial.json"),
            &serde_json::json!({
            "status":"incomplete","error":capture_error,"partial_destination":output.join("custody"),
            "manifest_verified":false,"engine_reopened":false}),
            &mut evidence_errors,
        );
    }
    use std::io::Write as _;
    let _ = writeln!(
        std::io::stderr(),
        "ACTUAL_REGISTERED_FIFTH_IMAGE_BASELINE {} {}",
        output.display(),
        terminal
    );
    let mut cleanup = Vec::new();
    for control in &mut controls {
        if let Err(error) = control.stop(&budget(5000)) {
            cleanup.push(error.to_string());
        }
    }
    if let Err(error) = host.stop_owned_metadata(&budget(5000)) {
        cleanup.push(error.to_string());
    }
    if let Err(error) = host.shutdown() {
        cleanup.push(error.to_string());
    }
    f.shutdown();
    evidence_after(
        &output.join("cleanup.json"),
        &serde_json::json!({"errors":cleanup,
        "primary_failure_preserved":primary_error,"original_roots_retained":true}),
        &mut evidence_errors,
    );
    // IO failures are reported separately; the required baseline's original
    // panic takes precedence even if the evidence destination has failed.
    let _ = writeln!(
        std::io::stderr(),
        "REGISTERED_FIFTH_EVIDENCE_ERRORS {:?}",
        evidence_errors
    );
    let evidence_errors_value = serde_json::json!({"errors":evidence_errors});
    if let Err(error) = evidence(&output.join("evidence-errors.json"), &evidence_errors_value) {
        evidence_errors.push(format!("evidence-errors.json: {error}"));
        let _ = writeln!(
            std::io::stderr(),
            "REGISTERED_FIFTH_FINAL_EVIDENCE_ERROR {error}"
        );
    }
    if let Err(panic) = result {
        std::panic::resume_unwind(panic);
    }
    assert!(
        evidence_errors.is_empty(),
        "actual evidence IO failures: {evidence_errors:?}"
    );
    assert!(
        cleanup.is_empty(),
        "actual baseline cleanup errors: {cleanup:?}"
    );
    assert!(
        capture_ok,
        "all-five actual custody unavailable; see terminal custody_error"
    );
}
