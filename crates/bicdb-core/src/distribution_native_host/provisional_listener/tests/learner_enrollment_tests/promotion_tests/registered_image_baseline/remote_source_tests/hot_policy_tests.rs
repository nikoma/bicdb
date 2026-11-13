//! Actual remote-OLD/original-Registered-A continuation through native policy.
//! Policy component cases stop there; distinct DATA cases use the actual Joined
//! adapter and unchanged client. Neither gate is physical qualification.
use super::*;
use crate::distribution_data::{
    ActualServiceNativeRegisteredJointNewEvidence, ClusterDataNodeService,
};
use crate::distribution_range_handoff::{
    NativeJointDurableDecision, NativeJointNewObservation, NativeJointNewRound,
    NativeJointPreparedServingObservation, NativeJointPreparedServingRound,
};

pub(in super::super) fn is_hot_policy(case: SourceCase) -> bool {
    matches!(
        case,
        SourceCase::TargetHotPolicy
            | SourceCase::TargetHotEarlyPolicy
            | SourceCase::TargetHotStoreReplacement
            | SourceCase::TargetHotDecisionReplyLost
            | SourceCase::TargetHotData
            | SourceCase::TargetHotDataReplyLost
            | SourceCase::TargetHotDataFirstCallerExpired
            | SourceCase::TargetHotDataFollower
    )
}
#[test]
fn native_registered_fifth_remote_hot_policy_actual_original_a_through_first_policy() {
    run_registered_image_baseline_with_source(Case::Required, Some(SourceCase::TargetHotPolicy));
}
#[test]
fn native_registered_fifth_remote_hot_policy_early_all_released_admission_refuses() {
    run_registered_image_baseline_with_source(
        Case::Required,
        Some(SourceCase::TargetHotEarlyPolicy),
    );
}
#[test]
fn native_registered_fifth_remote_hot_policy_actual_store_slot_replacement_refuses() {
    run_registered_image_baseline_with_source(
        Case::Required,
        Some(SourceCase::TargetHotStoreReplacement),
    );
}
#[test]
fn native_registered_fifth_remote_hot_policy_lost_committed_decision_reply_same_native_attempt() {
    run_registered_image_baseline_with_source(
        Case::Required,
        Some(SourceCase::TargetHotDecisionReplyLost),
    );
}

/// Intent is durable BEFORE each real mutator. Preserve its exact reply or
/// error BEFORE the assertion; an API error is never a rollback declaration.
fn operation<T>(
    output: &std::path::Path,
    ledger: &mut Vec<serde_json::Value>,
    name: &str,
    call: impl FnOnce() -> Result<T>,
) -> Result<T> {
    let at = ledger.len();
    ledger.push(serde_json::json!({"operation":name,"outcome":"not_started","effect_unknown_on_error":false}));
    evidence(&output.join("registered-hot-policy-ledger.json"), ledger)?;
    let result = call();
    ledger[at]["outcome"] = if result.is_ok() {
        "returned"
    } else {
        "error_unknown_effect"
    }
    .into();
    ledger[at]["exact_error"] =
        serde_json::to_value(result.as_ref().err().map(ToString::to_string))?;
    ledger[at]["effect_unknown_on_error"] = result.is_err().into();
    ledger[at]["error_debug"] =
        serde_json::to_value(result.as_ref().err().map(|error| format!("{error:?}")))?;
    let saved = evidence(&output.join("registered-hot-policy-ledger.json"), ledger);
    match result {
        Err(primary) => {
            let _ = saved;
            Err(primary)
        }
        Ok(value) => {
            saved?;
            Ok(value)
        }
    }
}
fn own(
    client: &TcpClusterRelocationTransport,
    parent: &CancellationToken,
) -> (
    crate::distribution_consensus::HostMetadataReadObservation,
    Uuid,
) {
    let business = Uuid::now_v7();
    (
        client
            .acquire_host_metadata_observation(&node(0), business, Duration::from_secs(5), parent)
            .unwrap(),
        business,
    )
}
fn commit_candidate(
    controls: &[NativeMetadataControlHost],
    metadata: &[Arc<Mutex<crate::MetadataConsensusStore>>],
    clients: &[Arc<TcpClusterRelocationTransport>],
    candidate: ClusterTopology,
    parent: &CancellationToken,
) {
    let entry = metadata[0]
        .lock()
        .propose_topology(candidate.clone())
        .unwrap();
    controls[0].heartbeat(parent).unwrap();
    controls[0].heartbeat(parent).unwrap();
    for actual in metadata {
        let actual = actual.lock();
        assert!(actual.status().commit_index >= entry.index);
        assert_eq!(actual.committed_topology(), &candidate);
    }
    for client in clients {
        client.install_topology(candidate.clone()).unwrap();
    }
}
fn joint_stage(
    controls: &[NativeMetadataControlHost],
    metadata: &[Arc<Mutex<crate::MetadataConsensusStore>>],
    clients: &[Arc<TcpClusterRelocationTransport>],
    template: &crate::DistributionConfig,
    decision: &NativeJointDurableDecision,
    stage: u8,
    parent: &CancellationToken,
) {
    let (fresh, business) = own(&clients[0], parent);
    let current = fresh.committed_topology(business).unwrap().clone();
    let snapshot = crate::ClusterBootstrapSnapshot {
        topology: current.clone(),
        config_template: template.clone(),
        metadata_leader_id: Some(node(0)),
    };
    let root = tempfile::tempdir().unwrap();
    let config = crate::provision_cluster_member_directory(
        root.path(),
        &snapshot,
        &node(0),
        template.transport.clone(),
        true,
    )
    .unwrap();
    let mut view = crate::DistributionStore::open(root.path(), config, true)
        .unwrap()
        .fork_ephemeral();
    match stage {
        0 => assert!(view
            .certify_native_joint_handoff(decision, &fresh, business, &node(0), 201)
            .unwrap()),
        1 => {
            view.promote_native_joint_handoff_frozen(decision, &fresh, business, &node(0), 202)
                .unwrap();
        }
        2 => {
            view.finalize_native_joint_membership_frozen(decision, &fresh, business, &node(0), 203)
                .unwrap();
        }
        _ => unreachable!(),
    }
    commit_candidate(controls, metadata, clients, view.topology().clone(), parent);
}

#[allow(clippy::too_many_arguments)]
pub(in super::super) fn exercise(
    case: SourceCase,
    controls: &mut [NativeMetadataControlHost],
    clients: &[Arc<TcpClusterRelocationTransport>],
    services: &[Arc<Mutex<crate::ClusterDataNodeService>>],
    metadata: &[Arc<Mutex<crate::MetadataConsensusStore>>],
    target: &Arc<TcpClusterRelocationTransport>,
    target_control: &mut NativeMetadataControlHost,
    factory: &Arc<crate::distribution_native_host::NativeUnassignedLearnerFactory>,
    pending: &ClusterTopology,
    ranges: &[crate::RangeDescriptor],
    acknowledged: &[serde_json::Value],
    output: &std::path::Path,
    template: &crate::DistributionConfig,
    original_gateway: &crate::ClusterEventClient,
    process: &CancellationToken,
) {
    assert!(is_hot_policy(case));
    assert_eq!(ranges.len(), 2);
    assert_eq!(acknowledged.len(), 2);
    let selected = ranges[0].id;
    let untouched = ranges[1].clone();
    assert_eq!(pending.range_by_id(untouched.id), Some(&untouched));
    assert_eq!(
        pending
            .relocations
            .values()
            .filter(|p| p.range_id == selected)
            .count(),
        1
    );
    for source in controls.iter() {
        source.bind_initial_image_source_process(process).unwrap();
    }
    let range = pending.range_by_id(selected).unwrap();
    let relocation = pending
        .relocations
        .values()
        .find(|p| p.range_id == selected)
        .unwrap();
    let source = (0..4).find(|i| node(*i) == range.leader).unwrap();
    let operation_parent = budget(5000);
    let business = Uuid::now_v7();
    let read = clients[source]
        .acquire_metadata_read_observation(
            &node(0),
            business,
            selected,
            range.epoch,
            Duration::from_secs(5),
            &operation_parent,
        )
        .unwrap();
    let round = NativeJointOldRound::begin(read, business, relocation.id).unwrap();
    let plan = round.plan();
    drop(round); // No local OLD quorum; only the remote collector below counts.
    let mut journal = Vec::new();
    let (donor, retained, _) = remote_old_seed(
        SourceCase::TargetCatalogueFloor,
        controls,
        clients,
        services,
        metadata,
        &plan,
        process,
        output,
        &mut journal,
    )
    .unwrap();
    let publish = budget(5000);
    let (mq, business) = own(&clients[donor], &publish);
    let manifest = retained.publish(&mq, business, &publish).unwrap();
    let source_fact = Arc::new(
        NativeRemoteImageFact::from_actual_manifest(
            frame(target, &manifest, Op::Manifest, output, &mut journal).unwrap(),
        )
        .unwrap(),
    );
    let actor = clients[donor].local_metadata_read_participant();
    let provider = MetadataReadProvider::new(metadata[4].clone(), target.clone()).unwrap();
    target_final_image_tests::exercise(
        SourceCase::TargetCatalogueFloor,
        target_control,
        target,
        source_fact.clone(),
        &plan,
        &provider,
        &actor,
        factory,
        pending,
        ranges,
        &acknowledged[0],
        output,
        &mut journal,
    );
    let request = budget(5000);
    let role = actual_role(&provider, &actor, &plan, &node(4), &request)
        .into_handoff()
        .unwrap();
    let (mq, business) = own(target, &request);
    let finished = source_fact
        .bind_step(frame(target, &manifest, Op::Finish, output, &mut journal).unwrap())
        .unwrap();
    let mut ledger = Vec::new();
    let original = operation(
        output,
        &mut ledger,
        "take_original_registered_joint_a",
        || {
            target_control.take_registered_remote_joint_target(
                &source_fact,
                &finished,
                role,
                mq,
                business,
                &request,
            )
        },
    )
    .unwrap();
    if matches!(case, SourceCase::TargetHotStoreReplacement) {
        let request = budget(5000);
        let role = actual_role(&provider, &actor, &plan, &node(4), &request)
            .into_handoff()
            .unwrap();
        let (mq, business) = own(target, &request);
        let before = state(&factory.database, pending, &node(4), ranges).unwrap();
        let replacement = crate::RangeWriteStore::open(
            output.join("hot-replacement-store"),
            pending.cluster_id.clone(),
            node(4),
            true,
        )
        .unwrap();
        let old = services[4]
            .lock()
            .test_replace_native_range_write_store(replacement);
        let removed = services[4]
            .lock()
            .test_replace_native_range_write_store(old);
        drop(removed); // Original Store returns, but actual slot stays revoked.
        request.check().unwrap();
        role.role().require_live_request().unwrap();
        mq.require_live_request(business).unwrap();
        let result = operation(
            output,
            &mut ledger,
            "registered_new_after_actual_slot_aba",
            || {
                ActualServiceNativeRegisteredJointNewEvidence::observe(
                    retained.actual_remote_old_certificate()?,
                    role,
                    mq,
                    business,
                    original.clone(),
                    &request,
                )
            },
        );
        evidence(&output.join("registered-hot-policy-negative.json"),&serde_json::json!({
            "case":format!("{case:?}"),"before":before,"after":state(&factory.database,pending,&node(4),ranges).unwrap(),
            "exact_error":result.as_ref().err().map(ToString::to_string),"application_tested":false})).unwrap();
        assert!(result.is_err());
        assert!(!factory.database.read().commit_admission_ready());
        return;
    }
    let certificate = retained.actual_remote_old_certificate().unwrap();
    let mut recipient_owners = BTreeMap::new();
    let mut runtimes = BTreeMap::new();
    for local in plan.old_members().keys() {
        let i = (0..4).find(|i| node(*i) == *local).unwrap();
        let (actual, runtime) = controls[i]
            .retained_initial_old_joint_owners(selected)
            .unwrap();
        recipient_owners.insert(local.clone(), actual);
        if plan.new_members().contains_key(local) {
            runtimes.insert(local.clone(), runtime);
        }
    }
    let request = budget(5000);
    let role = actual_role(&provider, &actor, &plan, &node(4), &request)
        .into_handoff()
        .unwrap();
    let (mq, business) = own(target, &request);
    let actual_new = operation(
        output,
        &mut ledger,
        "actual_registered_new_materialization",
        || {
            ActualServiceNativeRegisteredJointNewEvidence::observe(
                certificate.clone(),
                role,
                mq,
                business,
                original.clone(),
                &request,
            )
        },
    )
    .unwrap();
    retained
        .retire_export_after_actual_registered_new(&actual_new, process)
        .unwrap();
    let mut new = NativeJointNewRound::begin_remote_actual(certificate, recipient_owners).unwrap();
    new.record(NativeJointNewObservation::from_registered_actual_target(actual_new).unwrap())
        .unwrap();
    for local in plan.new_members().keys().filter(|n| **n != node(4)) {
        let i = (0..4).find(|i| node(*i) == *local).unwrap();
        let request = budget(5000);
        let provider = MetadataReadProvider::new(metadata[i].clone(), clients[i].clone()).unwrap();
        new.record_actual_survivor(
            local,
            actual_role(&provider, &actor, &plan, local, &request),
        )
        .unwrap();
    }
    let mut decision = new
        .finish()
        .unwrap()
        .prepare_decision()
        .unwrap()
        .publication_round();
    let mut publishers = plan.old_members().keys().cloned().collect::<Vec<_>>();
    publishers.push(node(4));
    for local in &publishers {
        let i = (0..5).find(|i| node(*i) == *local).unwrap();
        let client = if i == 4 { target } else { &clients[i] };
        let provider = MetadataReadProvider::new(metadata[i].clone(), client.clone()).unwrap();
        let request = budget(5000);
        let role = actual_role(&provider, &actor, &plan, local, &request);
        let (mq, business) = own(client, &request);
        if i == 4
            && matches!(
                case,
                SourceCase::TargetHotDecisionReplyLost | SourceCase::TargetHotDataReplyLost
            )
        {
            let token = request.clone();
            let cut = BicDb::test_registered_joint_decision_after_commit(Box::new(move || {
                token.cancel()
            }));
            let result = operation(
                output,
                &mut ledger,
                "registered_decision_actual_committed_reply_lost",
                || decision.publish_actual_owned_member(role, mq, business, &request),
            );
            drop(cut);
            let before_retry = original.test_decision_attempt_evidence().unwrap();
            evidence(&output.join("registered-hot-policy-decision-lost.json"),&serde_json::json!({
                "original_result":result.as_ref().err().map(ToString::to_string),"before_retry":before_retry})).unwrap();
            assert!(result.is_err());
            let request = budget(5000);
            let role = actual_role(&provider, &actor, &plan, local, &request);
            let (mq, business) = own(client, &request);
            operation(
                output,
                &mut ledger,
                "registered_decision_fresh_same_attempt_retry",
                || decision.publish_actual_owned_member(role, mq, business, &request),
            )
            .unwrap();
            assert_eq!(
                before_retry,
                original.test_decision_attempt_evidence().unwrap()
            );
        } else {
            operation(output, &mut ledger, "actual_owned_native_decision", || {
                decision.publish_actual_owned_member(role, mq, business, &request)
            })
            .unwrap();
        }
    }
    let decision = decision.finish().unwrap();
    let data = decision.data().clone();
    let mut all_clients = clients.to_vec();
    all_clients.push(target.clone());
    for stage in 0..3 {
        joint_stage(
            controls,
            metadata,
            &all_clients,
            template,
            &decision,
            stage,
            &budget(5000),
        );
    }
    let current = metadata[0].lock().committed_topology().clone();
    assert_eq!(current.range_by_id(untouched.id), Some(&untouched));
    let mut prepared: Option<NativeJointPreparedServingRound> = None;
    for (local, (service, lease)) in decision.into_new_serving_owners().unwrap() {
        let i = (0..5).find(|i| node(*i) == local).unwrap();
        let client = &all_clients[i];
        let closed = if i != 4 {
            controls[i]
                .begin_native_writer_data_domain(selected, lease.clone(), process)
                .unwrap();
            controls[i]
                .advance_native_writer_closed_domain(selected, &budget(5000))
                .unwrap();
            Some(
                controls[i]
                    .native_closed_writer_domain(selected, process)
                    .unwrap(),
            )
        } else {
            None
        };
        let lease = if i != 4 {
            controls[i]
                .take_native_closed_joint_lease(selected, process)
                .unwrap()
        } else {
            lease
        };
        let request = budget(5000);
        let (mq, business) = own(client, &request);
        let point = ClusterDataNodeService::capture_actual_joint_metadata_point(
            &service, &mq, business, &request,
        )
        .unwrap();
        let actual = {
            let actual = service.lock();
            let domain = match &closed {
                Some(closed) => crate::db::NativeJointWriterDomain::Closed(closed.clone()),
                None => crate::db::NativeJointWriterDomain::Registered(Arc::downgrade(&original)),
            };
            let committed = actual
                .observe_joint_decision_with_writer_domain_and_point(
                    &domain, mq, business, selected, point,
                )
                .unwrap();
            match closed {
                Some(closed) => actual.prepare_local_joint_serving_with_closed_writer(
                    closed, committed, lease, process,
                ),
                None => actual.prepare_local_registered_joint_serving(committed, lease, process),
            }
            .unwrap()
        };
        let observation =
            NativeJointPreparedServingObservation::from_actual_service(actual, service).unwrap();
        match &mut prepared {
            Some(round) => round.record(observation).unwrap(),
            None => {
                prepared =
                    Some(NativeJointPreparedServingRound::begin(observation, process).unwrap())
            }
        }
    }
    let prepared = Arc::new(prepared.unwrap().finish().unwrap());
    let mut publication = prepared.resume_native_publication().unwrap();
    for local in data.new_members().keys() {
        let mut phase = prepared
            .begin_fresh_preparation_phase(Duration::from_secs(4))
            .unwrap();
        for member in data.new_members().keys() {
            let i = (0..5).find(|i| node(*i) == *member).unwrap();
            phase
                .acquire_actual_member(&all_clients[i], &node(0))
                .unwrap();
        }
        let phase = Arc::new(phase.finish().unwrap());
        let i = (0..5).find(|i| node(*i) == *local).unwrap();
        let admission = phase
            .acquire_local_activation(&all_clients[i], &node(0))
            .unwrap();
        operation(output, &mut ledger, "fresh_actual_native_bridge", || {
            publication.publish_actual_local_member(&admission, process)
        })
        .unwrap();
    }
    let published = Arc::new(publication.finish().unwrap());
    let mut activations = BTreeMap::new();
    let mut release_owner = None;
    for local in data.new_members().keys() {
        let mut phase = published
            .begin_fresh_open_phase(Duration::from_secs(4))
            .unwrap();
        for member in data.new_members().keys() {
            let i = (0..5).find(|i| node(*i) == *member).unwrap();
            phase
                .acquire_actual_member(&all_clients[i], &node(0))
                .unwrap();
        }
        let phase = Arc::new(phase.finish().unwrap());
        let i = (0..5).find(|i| node(*i) == *local).unwrap();
        let admission = phase.acquire_local_open(&all_clients[i], &node(0)).unwrap();
        let receipt = Arc::new(
            operation(output, &mut ledger, "actual_catalogue_open", || {
                admission.persist_local_catalogue_activation(process)
            })
            .unwrap(),
        );
        if i == 4 {
            release_owner = Some(
                admission
                    .retain_registered_release_owner(receipt.clone())
                    .unwrap(),
            );
        }
        activations.insert(local.clone(), receipt);
    }
    let continuation = Arc::new(
        published
            .retain_registered_release_continuation(activations, runtimes, release_owner.unwrap())
            .unwrap(),
    );
    drop(published);
    drop(prepared);
    let mut phase = continuation
        .begin_fresh_release_phase(Duration::from_secs(4))
        .unwrap();
    for member in data.new_members().keys() {
        let i = (0..5).find(|i| node(*i) == *member).unwrap();
        phase
            .acquire_actual_member(&all_clients[i], &node(0))
            .unwrap();
    }
    let phase = Arc::new(phase.finish().unwrap());
    if matches!(case, SourceCase::TargetHotEarlyPolicy) {
        let error = phase
            .acquire_local_writer_policy(target, &node(0))
            .err()
            .expect("actual Frozen member cannot claim ALL released");
        evidence(&output.join("registered-hot-policy-early.json"),&serde_json::json!({"exact_error":error.to_string(),
            "writer_installed":factory.database.read().commit_admission_ready(),"original_a_retained":true,"application_tested":false})).unwrap();
        assert!(!factory.database.read().commit_admission_ready());
    }
    retained
        .retire_old_export_custody_after_joint_open(&phase)
        .unwrap();
    for member in data.new_members().keys().filter(|n| **n != node(4)) {
        let i = (0..4).find(|i| node(*i) == *member).unwrap();
        controls[i]
            .retire_initial_old_observation_after_joint_open(selected, &phase)
            .unwrap();
    }
    drop(phase);
    drop(retained);
    for local in data.new_members().keys() {
        let mut phase = continuation
            .begin_fresh_release_phase(Duration::from_secs(4))
            .unwrap();
        for member in data.new_members().keys() {
            let i = (0..5).find(|i| node(*i) == *member).unwrap();
            phase
                .acquire_actual_member(&all_clients[i], &node(0))
                .unwrap();
        }
        let phase = Arc::new(phase.finish().unwrap());
        let i = (0..5).find(|i| node(*i) == *local).unwrap();
        let admission = phase
            .acquire_local_release(&all_clients[i], &node(0))
            .unwrap();
        operation(output, &mut ledger, "actual_original_owner_release", || {
            admission.release_actual_local()
        })
        .unwrap();
    }
    for local in data.new_members().keys() {
        let mut phase = continuation
            .begin_fresh_release_phase(Duration::from_secs(4))
            .unwrap();
        for member in data.new_members().keys() {
            let i = (0..5).find(|i| node(*i) == *member).unwrap();
            phase
                .acquire_actual_member(&all_clients[i], &node(0))
                .unwrap();
        }
        let phase = Arc::new(phase.finish().unwrap());
        let i = (0..5).find(|i| node(*i) == *local).unwrap();
        let admission = Arc::new(
            phase
                .acquire_local_writer_policy(&all_clients[i], &node(0))
                .unwrap(),
        );
        if i == 4 {
            data_tests::install_first_policy(
                case,
                target_control,
                selected,
                admission,
                &continuation,
                &all_clients,
                &data,
                output,
                &mut ledger,
            );
        } else {
            operation(
                output,
                &mut ledger,
                "actual_same_state_survivor_serving_policy",
                || {
                    controls[i].publish_native_writer_serving_domain(
                        selected,
                        &admission,
                        &budget(5000),
                    )
                },
            )
            .unwrap();
        }
    }
    let after = state(&factory.database, &current, &node(4), ranges).unwrap();
    let selected_record = acknowledged[0]["record_id"].as_str().unwrap();
    let actual_selected = factory
        .database
        .read()
        .get(crate::CLUSTER_EVENTS_COLLECTION, selected_record)
        .unwrap();
    let target_attempt = original.test_decision_attempt_evidence().unwrap();
    let old_payloads=acknowledged.iter().map(|entry| {
        let record=entry["record_id"].as_str().unwrap();
        let actual=services.iter().take(4).map(|service| {
            let database=service.lock().database();
            let row=database.read().get(crate::CLUSTER_EVENTS_COLLECTION,record);
            serde_json::json!({"row":row.as_ref().ok(),"exact_error":row.as_ref().err().map(ToString::to_string)})
        }).collect::<Vec<_>>();
        serde_json::json!({"record_id":record,"expected":entry["actual_full_record"],"original_four":actual})
    }).collect::<Vec<_>>();
    evidence(&output.join("registered-hot-policy-component-result.json"),&serde_json::json!({"case":format!("{case:?}"),
        "actual_decision":data,"target_after":after,"original_app_ack_ledger":acknowledged,
        "target_exact_native_decision_attempt":target_attempt,"selected_target_physical_record":actual_selected,
        "original_four_readonly_reconciliation":old_payloads,
        "unchanged_gateway_retained":true,"manual_gateway_routing":false,"second_range":untouched,
        "through_policy_only":true,"application_after_movement_tested":false,"strong_after_movement_tested":false,
        "joined_lifecycle_exercised":false,"physical_or_capacity_qualification":false})).unwrap();
    std::hint::black_box(original_gateway);
    assert_eq!(
        serde_json::to_value(actual_selected.as_ref().unwrap()).unwrap(),
        acknowledged[0]["actual_full_record"]
    );
    assert!(factory.database.read().commit_admission_ready());
    assert_eq!(current.range_by_id(untouched.id), Some(&untouched));
    let other = &after["points"][untouched.id.get().to_string()];
    assert!(
        other["native_applied_index"].is_null(),
        "no second-range native grant"
    );
    if data_tests::is_data_case(case) {
        data_tests::exercise(
            case,
            target_control,
            original_gateway,
            factory,
            &all_clients,
            services,
            &current,
            &data,
            acknowledged,
            output,
        );
    }
}

mod data_tests;
