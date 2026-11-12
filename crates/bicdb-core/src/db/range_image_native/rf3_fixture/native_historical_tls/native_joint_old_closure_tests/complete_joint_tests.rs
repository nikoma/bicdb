//! Genuine same-open joint prerequisite. Initial four-member/ RF3 placement
//! is declared before birth; every subsequent proof comes from actual native
//! stores/services, TLS metadata, the completed image and distinct data sets.
//! Local opaque-object orchestration is NOT an operator/wire handoff test.
use super::*;
#[path = "complete_joint_tests/current_joint_authority_tests.rs"]
mod current_joint_authority_tests;
#[path = "complete_joint_tests/open_adversarial_tests.rs"]
mod open_adversarial_tests;
#[path = "complete_joint_tests/original_release_writer_tests.rs"]
mod original_release_writer_tests;
#[path = "complete_joint_tests/post_joint_no_data_tests.rs"]
mod post_joint_no_data_tests;
use crate::distribution_range_consensus::{
    ImageInstallPurpose, NativeImageInstallRole, RecoveryFenceScope,
};
use crate::distribution_range_handoff::{
    NativeJointDurableDecision, NativeJointNewObservation, NativeJointNewRound,
    NativeJointPreparedServingObservation, NativeJointPreparedServingQuorum,
    NativeJointPreparedServingRound,
};
use crate::distribution_range_image::RangeImageCapture;
use crate::distribution_range_image_install::{
    NativeImageCatalogueFinalization, NativeImageInstallFreeze, NativeRangeImageInstaller,
};
use crate::distribution_range_recovery::RecoveryEpochFenceRequest;
use crate::RangeSnapshotOptions;

fn refresh_hosts(fixture: &mut EnforcedNetwork) {
    for local in [node("n0"), node("n1"), node("n2"), node("n3")] {
        let business = Uuid::now_v7();
        let mq = fixture.clients[&local]
            .acquire_host_metadata_observation(
                &node("n1"),
                business,
                Duration::from_secs(5),
                &CancellationToken::uncancelable(),
            )
            .unwrap();
        let policy = {
            let service = fixture.native.services[&local].lock();
            let db = fixture.native.databases[&local].read();
            service
                .prepare_native_host_writer_enrollment(&db, mq, business)
                .unwrap()
        };
        let guard = fixture.native.databases[&local]
            .read()
            .install_enrolled_native_coordinator(fixture.coordinators[&local].clone(), policy)
            .unwrap();
        fixture.hosts.insert(local, Arc::new(guard));
    }
}

fn commit_actual_topology(fixture: &mut EnforcedNetwork, candidate: crate::ClusterTopology) {
    let mut leader = fixture.metadata[&node("n1")].lock();
    let entry = leader.propose_topology(candidate).unwrap();
    for local in [node("n0"), node("n2"), node("n3")] {
        let reply = fixture.metadata[&local]
            .lock()
            .handle_append_request(leader.append_request_from(entry.index, 16).unwrap())
            .unwrap();
        leader.record_append_response(&local, &reply).unwrap();
    }
    assert!(leader.status().commit_index >= entry.index);
    for local in [node("n0"), node("n2"), node("n3")] {
        let reply = fixture.metadata[&local]
            .lock()
            .handle_append_request(leader.append_request_from(entry.index + 1, 16).unwrap())
            .unwrap();
        leader.record_append_response(&local, &reply).unwrap();
    }
    let committed = leader.committed_topology().clone();
    drop(leader);
    for store in fixture.metadata.values() {
        assert_eq!(store.lock().committed_topology(), &committed);
    }
    *fixture.native.topology.write() = committed;
}

fn commit_joint_stage(
    fixture: &mut EnforcedNetwork,
    decision: &NativeJointDurableDecision,
    stage: usize,
) {
    let business = Uuid::now_v7();
    let fresh = fixture.clients[&node("n1")]
        .acquire_host_metadata_observation(
            &node("n1"),
            business,
            Duration::from_secs(5),
            &CancellationToken::uncancelable(),
        )
        .unwrap();
    let current = fresh.committed_topology(business).unwrap().clone();
    let member = &current.nodes[&node("n1")];
    // A planning view only. The actual opaque decision + independent HostMQ
    // validate each builder; only real MStore majority ACKs commit its output.
    let root = tempfile::tempdir().unwrap();
    let snapshot = crate::ClusterBootstrapSnapshot {
        topology: current.clone(),
        config_template: crate::DistributionConfig {
            enabled: true,
            cluster_id: current.cluster_id.clone(),
            node_id: member.id.clone(),
            node_address: member.address.clone(),
            node_incarnation: member.incarnation,
            node_tls_certificate_sha256: member.tls_certificate_sha256.clone(),
            range_image_bootstrap_id: current.range_image_bootstrap_id,
            initial_ranges: 2,
            replication_factor: 3,
            ..Default::default()
        },
        metadata_leader_id: Some(node("n1")),
    };
    // Populate only an EMPTY planning cache from the actual fresh MQ C. Do
    // not invent a preexisting unbound voter and then try to enroll it through
    // snapshot installation. This creates no native owner or consensus store.
    let config = crate::provision_cluster_member_directory(
        root.path(),
        &snapshot,
        &node("n1"),
        snapshot.config_template.transport.clone(),
        true,
    )
    .unwrap();
    let mut view = crate::DistributionStore::open(root.path(), config, true)
        .unwrap()
        .fork_ephemeral();
    assert_eq!(view.topology(), fresh.committed_topology(business).unwrap());
    match stage {
        0 => {
            assert!(view
                .certify_native_joint_handoff(decision, &fresh, business, &node("n1"), 10)
                .unwrap());
        }
        1 => {
            view.promote_native_joint_handoff_frozen(decision, &fresh, business, &node("n1"), 11)
                .unwrap();
        }
        2 => {
            view.finalize_native_joint_membership_frozen(
                decision,
                &fresh,
                business,
                &node("n1"),
                12,
            )
            .unwrap();
        }
        _ => panic!("unknown actual joint stage"),
    }
    commit_actual_topology(fixture, view.topology().clone());
}

fn install_actual_target(
    fixture: &EnforcedNetwork,
    plan: &crate::distribution_range_handoff::NativeJointHandoffPlan,
    mut capture: RangeImageCapture,
) -> (NativeImageInstallFreeze, NativeImageCatalogueFinalization) {
    let target = node("n3");
    let parent = CancellationToken::uncancelable();
    let actor = fixture.clients[&node("n0")].local_metadata_read_participant();
    let initial = role(fixture, plan, &target);
    let scope = RecoveryFenceScope::from_role(&initial).unwrap();
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
    let handoff = role(fixture, plan, &target).into_handoff().unwrap();
    let image_role = NativeImageInstallRole::CommittedHandoff(&handoff);
    let attempt = Uuid::now_v7();
    let source = capture.authorize_target_install().unwrap();
    let lease = fixture.runtimes[&target]
        .transfer_image_install(
            &scope,
            &source,
            &image_role,
            ImageInstallPurpose::RelocationLearner,
            attempt,
            &parent,
        )
        .unwrap();
    let mut install: NativeRangeImageInstaller = fixture.native.services[&target]
        .lock()
        .begin_native_range_image_install(
            &source,
            &image_role,
            lease,
            ImageInstallPurpose::RelocationLearner,
            attempt,
            &parent,
        )
        .unwrap();
    drop(source);
    loop {
        let cursor = capture.history_cursor().clone();
        let step = capture.export_history_step(&cursor, 256, &parent).unwrap();
        let handoff = role(fixture, plan, &target).into_handoff().unwrap();
        let source = capture.authorize_target_install().unwrap();
        let stage_id = install.history_stage_id();
        let progress = fixture.native.services[&target]
            .lock()
            .stage_native_range_image_history(
                &mut install,
                &NativeImageInstallRole::CommittedHandoff(&handoff),
                &source,
                stage_id,
                &step,
                &parent,
            )
            .unwrap();
        if progress.cursor.complete() {
            break;
        }
    }
    loop {
        let handoff = role(fixture, plan, &target).into_handoff().unwrap();
        let source = capture.authorize_target_install().unwrap();
        if fixture.native.services[&target]
            .lock()
            .audit_native_range_image_history(
                &mut install,
                &NativeImageInstallRole::CommittedHandoff(&handoff),
                &source,
                256,
                &parent,
            )
            .unwrap()
            .audit_complete
        {
            break;
        }
    }
    loop {
        let handoff = role(fixture, plan, &target).into_handoff().unwrap();
        let source = capture.authorize_target_install().unwrap();
        if fixture.native.services[&target]
            .lock()
            .clear_native_range_image_step(
                &mut install,
                &NativeImageInstallRole::CommittedHandoff(&handoff),
                &source,
                &parent,
            )
            .unwrap()
        {
            break;
        }
    }
    let options = RangeSnapshotOptions {
        max_records_per_batch: 16,
        max_bytes_per_batch: 1024 * 1024,
        max_record_bytes: 1024 * 1024,
    };
    loop {
        let cursor = capture.cursor().clone();
        let step = capture.export_step(&cursor, &options, &parent).unwrap();
        let handoff = role(fixture, plan, &target).into_handoff().unwrap();
        let source = capture.authorize_target_install().unwrap();
        fixture.native.services[&target]
            .lock()
            .copy_native_range_image_step(
                &mut install,
                &NativeImageInstallRole::CommittedHandoff(&handoff),
                &source,
                attempt,
                &step,
                &parent,
            )
            .unwrap();
        if step.complete {
            break;
        }
    }
    let completed = capture.complete().unwrap();
    let handoff = role(fixture, plan, &target).into_handoff().unwrap();
    let mut verification = fixture.native.services[&target]
        .lock()
        .begin_native_range_image_verification(
            install,
            &NativeImageInstallRole::CommittedHandoff(&handoff),
            completed,
            &parent,
        )
        .unwrap();
    let mut complete = false;
    for _ in 0..128 {
        let handoff = role(fixture, plan, &target).into_handoff().unwrap();
        if fixture.native.services[&target]
            .lock()
            .advance_native_range_image_verification(
                &mut verification,
                &NativeImageInstallRole::CommittedHandoff(&handoff),
                &parent,
            )
            .unwrap()
            == crate::db::NativeTargetImageVerificationProgress::Complete
        {
            complete = true;
            break;
        }
    }
    assert!(complete, "actual bounded target verification must finish");
    let handoff = role(fixture, plan, &target).into_handoff().unwrap();
    let verified = fixture.native.services[&target]
        .lock()
        .finish_native_range_image_verification(
            verification,
            &NativeImageInstallRole::CommittedHandoff(&handoff),
            &parent,
        )
        .unwrap();
    assert_eq!(verified.base().range_index(), 1);
    let handoff = role(fixture, plan, &target).into_handoff().unwrap();
    let adopted = fixture.native.services[&target]
        .lock()
        .adopt_native_range_image_catalogue(
            verified,
            &NativeImageInstallRole::CommittedHandoff(&handoff),
            &parent,
        )
        .unwrap();
    let handoff = role(fixture, plan, &target).into_handoff().unwrap();
    fixture.native.services[&target]
        .lock()
        .finalize_native_range_image_catalogue(
            adopted,
            &NativeImageInstallRole::CommittedHandoff(&handoff),
            &parent,
        )
        .unwrap()
}

#[derive(Debug, PartialEq, Eq)]
enum JointTestOutcome {
    FullHandoffAckAndStrong,
    PublicationOnly,
    OriginalCallerRejected,
    FrozenOpenOnly,
    AdversarialOnly,
    OriginalReleaseWriterRejected,
    FullHandoffThenPostJointNoDataAck4Strong,
    FullHandoffThenCurrentJointAuthorityRefusals,
}

#[test]
fn native_joint_actual_complete_image_target_becomes_leader_ack_and_strong_read() {
    // Full contract uses DISTINCT bounded phases after original MQ expiry;
    // every original row/history/UID/final replica assertion remains below.
    assert_eq!(
        run_complete_joint(0),
        JointTestOutcome::FullHandoffAckAndStrong,
        "a prerequisite/adversarial early return must not qualify the full handoff"
    );
}
#[test]
fn native_joint_fresh_preparation_phase_publishes_all_final_members_after_original_expiry() {
    run_complete_joint(1);
}
#[test]
fn native_joint_fresh_phase_refuses_expired_original_caller_with_later_long_budget() {
    run_complete_joint(2);
}

#[test]
fn native_joint_fresh_open_phase_reconciles_exact_target_epoch_and_lost_catalogue_reply_while_frozen(
) {
    run_complete_joint(3);
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum JointFullContinuation {
    OriginalTerminalControls,
    PostJointNoDataRecovery,
    CurrentJointAuthorityRefusals,
}

fn run_complete_joint(mode: u8) -> JointTestOutcome {
    run_complete_joint_with_original_release_control(mode, None)
}

fn run_complete_joint_with_continuation(
    mode: u8,
    continuation: JointFullContinuation,
) -> JointTestOutcome {
    run_complete_joint_with_controls(mode, None, continuation)
}

fn run_complete_joint_with_original_release_control(
    mode: u8,
    control: Option<original_release_writer_tests::OriginalReleaseControl>,
) -> JointTestOutcome {
    run_complete_joint_with_controls(
        mode,
        control,
        JointFullContinuation::OriginalTerminalControls,
    )
}

fn run_complete_joint_with_controls(
    mode: u8,
    control: Option<original_release_writer_tests::OriginalReleaseControl>,
    continuation: JointFullContinuation,
) -> JointTestOutcome {
    assert!(control.is_none() || mode == 0);
    assert!(
        continuation == JointFullContinuation::OriginalTerminalControls
            || (mode == 0 && control.is_none())
    );
    let parent = CancellationToken::uncancelable();
    let mut fixture = EnforcedNetwork::new_with_metadata_members(4);
    fixture.native.root.disable_cleanup(true);
    fixture._keys.disable_cleanup(true);
    eprintln!(
        "RETAINED_NATIVE_JOINT_ROOT={}",
        fixture.native.root.path().display()
    );
    eprintln!(
        "RETAINED_NATIVE_JOINT_METADATA_ROOT={}",
        fixture._keys.path().display()
    );
    let (key, old) = fixture.seed();
    let acknowledged = fixture.native.databases[&node("n0")]
        .read()
        .get("items", &key)
        .unwrap()
        .unwrap();
    crate::storage::write_atomic(
        &fixture
            .native
            .root
            .path()
            .join("actual-joint-selected-range-ack-ledger.json"),
        &serde_json::to_vec(&vec![acknowledged.clone()]).unwrap(),
        true,
    )
    .unwrap();
    let original_uid = fixture.native.databases[&node("n3")]
        .read()
        .local_namespace_identity("items")
        .unwrap();
    let relocation = commit_pending(&mut fixture, &old);
    let current = fixture
        .native
        .topology
        .read()
        .range_by_id(old.id)
        .unwrap()
        .clone();
    let business = Uuid::now_v7();
    let mq = fixture.clients[&node("n0")]
        .acquire_metadata_read_observation(
            &node("n1"),
            business,
            current.id,
            current.epoch,
            Duration::from_secs(5),
            &parent,
        )
        .unwrap();
    let mut round = NativeJointOldRound::begin(mq, business, relocation).unwrap();
    let plan = round.plan();
    assert_eq!(plan.activation_range().leader, node("n3"));
    let runtimes = [node("n0"), node("n1"), node("n2")]
        .into_iter()
        .map(|id| {
            (
                id.clone(),
                Arc::new(
                    NativeJointHandoffRuntime::new(fixture.native.services[&id].clone()).unwrap(),
                ),
            )
        })
        .collect::<BTreeMap<_, _>>();
    for local in runtimes.keys() {
        let actual = runtimes[local]
            .observe_constraints(plan.clone(), role(&fixture, &plan, local), &parent)
            .unwrap();
        round
            .record_constraints(actual, &fixture.native.services[local])
            .unwrap();
    }
    for local in runtimes.keys() {
        round
            .record_closed(
                runtimes[local]
                    .close_old(plan.clone(), role(&fixture, &plan, local), &parent)
                    .unwrap(),
            )
            .unwrap();
    }
    let mut closed = round.finish().unwrap();
    let capture = closed
        .begin_capture(Duration::from_secs(2), &parent)
        .unwrap();
    let closed = Arc::new(closed);
    let (frozen, image) = install_actual_target(&fixture, &plan, capture);
    assert!(matches!(&frozen, NativeImageInstallFreeze::Recovery(_)));
    assert_eq!(
        image.base().logical_history_sha256(),
        closed.logical_history_sha256()
    );
    let handoff = role(&fixture, &plan, &node("n3")).into_handoff().unwrap();
    let target = fixture.native.services[&node("n3")]
        .lock()
        .observe_native_joint_new_target(&closed, handoff, image, frozen, &parent)
        .unwrap();
    let mut new = NativeJointNewRound::begin(closed.clone()).unwrap();
    for local in [node("n1"), node("n2")] {
        new.record_actual_survivor(&local, role(&fixture, &plan, &local))
            .unwrap();
    }
    new.record(
        NativeJointNewObservation::from_finalized_actual_target(
            target,
            fixture.native.services[&node("n3")].clone(),
        )
        .unwrap(),
    )
    .unwrap();
    let mut publication = new
        .finish()
        .unwrap()
        .prepare_decision()
        .unwrap()
        .publication_round();
    for local in [node("n0"), node("n1"), node("n2"), node("n3")] {
        publication
            .publish_actual_member(role(&fixture, &plan, &local), &parent)
            .unwrap();
    }
    let decision = publication.finish().unwrap();
    let old_h = decision.data().logical_history_sha256().to_owned();
    for stage in 0..3 {
        commit_joint_stage(&mut fixture, &decision, stage);
    }
    refresh_hosts(&mut fixture);
    let activation_range = fixture
        .native
        .topology
        .read()
        .range_by_id(old.id)
        .unwrap()
        .clone();
    assert_eq!(&activation_range, plan.activation_range());
    let preparation_started = std::time::Instant::now();
    let mut prepared_round: Option<NativeJointPreparedServingRound> = None;
    let round_parent = if mode == 2 {
        parent.child_with_deadline(std::time::Instant::now() + Duration::from_secs(2))
    } else {
        parent.child()
    };
    for (local, (service, lease)) in decision.into_new_serving_owners().unwrap() {
        eprintln!(
            "ACTUAL_JOINT_PHASE prepare-begin node={local} elapsed_ms={}",
            preparation_started.elapsed().as_millis()
        );
        let business = Uuid::now_v7();
        let mq = fixture.clients[&local]
            .acquire_host_metadata_observation(
                &node("n1"),
                business,
                Duration::from_secs(5),
                &parent,
            )
            .unwrap();
        let actual = {
            let actual = service.lock();
            let committed = actual
                .observe_committed_native_joint_decision(mq, business, old.id)
                .unwrap();
            actual
                .prepare_local_native_joint_serving(committed, lease, &parent)
                .unwrap()
        };
        let observation =
            NativeJointPreparedServingObservation::from_actual_service(actual, service).unwrap();
        eprintln!(
            "ACTUAL_JOINT_PHASE prepare-complete node={local} elapsed_ms={}",
            preparation_started.elapsed().as_millis()
        );
        if let Some(round) = &mut prepared_round {
            round.record(observation).unwrap();
        } else {
            prepared_round =
                Some(NativeJointPreparedServingRound::begin(observation, &round_parent).unwrap());
        }
    }
    let prepared = Arc::new(prepared_round.unwrap().finish().unwrap());
    if mode == 2 {
        std::thread::sleep(
            round_parent
                .deadline()
                .unwrap()
                .saturating_duration_since(std::time::Instant::now())
                + Duration::from_millis(10),
        );
        assert!(!round_parent.is_cancel_requested());
        let later = parent.child_with_deadline(std::time::Instant::now() + Duration::from_secs(30));
        later.check().unwrap();
        assert!(matches!(
            prepared.begin_fresh_preparation_phase(Duration::from_secs(4)),
            Err(crate::BicDbError::QueryTimedOut)
        ));
        assert!(prepared.require_current().is_err());
        for id in [node("n1"), node("n2"), node("n3")] {
            let db = fixture.native.databases[&id].read();
            assert!(db.range_mutation_gate().try_enter(old.id).is_err());
        }
        return JointTestOutcome::OriginalCallerRejected;
    }
    if mode == 1 || mode >= 3 {
        let published =
            exercise_fresh_native_publication(&fixture, &prepared, &parent, old.id, mode == 1);
        if mode == 3 {
            exercise_fresh_open_while_frozen(&fixture, &prepared, published, &parent, old.id);
            return JointTestOutcome::FrozenOpenOnly;
        } else if mode >= 4 {
            open_adversarial_tests::exercise(&fixture, &prepared, published, &parent, old.id, mode);
            return JointTestOutcome::AdversarialOnly;
        }
        return JointTestOutcome::PublicationOnly;
    }

    if let Some(control) = control {
        let published =
            exercise_fresh_native_publication(&fixture, &prepared, &parent, old.id, false);
        let release =
            original_release_writer_tests::prepare(&fixture, &published, &parent, control);
        // Drain genuine duplicate owners before the attempted ORIGINAL release.
        // No Frozen/pin or old receipt is recreated for this negative.
        drop(published);
        drop(prepared);
        drop(closed);
        original_release_writer_tests::reject_after_reenrollment(
            &fixture,
            &runtimes,
            &parent,
            old.id,
            &key,
            &acknowledged,
            release,
        );
        return JointTestOutcome::OriginalReleaseWriterRejected;
    }

    let release_continuation = if mode == 0 {
        let published =
            exercise_fresh_native_publication(&fixture, &prepared, &parent, old.id, false);
        let (activations, image_owner) = exercise_fresh_open_boundary(
            &fixture,
            &prepared,
            published.clone(),
            &parent,
            old.id,
            false,
        );
        let survivors = [node("n1"), node("n2")]
            .into_iter()
            .map(|id| (id.clone(), runtimes[&id].clone()))
            .collect();
        let continuation = Arc::new(
            published
                .retain_release_continuation(activations, survivors, image_owner.unwrap())
                .unwrap(),
        );
        // Real freeze owners must drain; retaining old Prepared/Published
        // quorum objects across this transition would correctly block release.
        drop(published);
        drop(prepared);
        drop(closed);
        exercise_fresh_release(&fixture, &continuation, &parent, old.id);
        Some(continuation)
    } else {
        let mut publication = prepared.serving_publication_round().unwrap();
        for local in [node("n1"), node("n2"), node("n3")] {
            let current = prepared.require_current();
            eprintln!(
            "ACTUAL_JOINT_PHASE activation-preflight node={local} elapsed_ms={} remaining_original_ms={:?} result={current:?}",
            preparation_started.elapsed().as_millis(),
            prepared.test_original_budget_remaining_ms()
        );
            current.unwrap();
            let acquisition_started = std::time::Instant::now();
            let admission =
                prepared.acquire_local_activation(&fixture.clients[&local], &node("n1"), &parent);
            eprintln!(
            "ACTUAL_JOINT_PHASE activation-acquired node={local} elapsed_ms={} acquisition_ms={} remaining_original_ms={:?} error={:?}",
            preparation_started.elapsed().as_millis(),
            acquisition_started.elapsed().as_millis(),
            prepared.test_original_budget_remaining_ms(),
            admission.as_ref().err()
        );
            if admission.is_err() {
                for (id, store) in &fixture.metadata {
                    if let Some(store) = store.try_lock() {
                        let status = store.status();
                        eprintln!(
                        "ACTUAL_JOINT_PHASE activation-failure-metadata node={id} term={} commit={} last={} generation={}",
                        status.current_term,
                        status.commit_index,
                        status.last_log_index,
                        status.topology_generation
                    );
                    } else {
                        eprintln!(
                            "ACTUAL_JOINT_PHASE activation-failure-metadata node={id} busy=true"
                        );
                    }
                }
            }
            let admission = admission.unwrap();
            publication
                .publish_actual_local_member(&admission, &parent)
                .unwrap();
        }
        let published = Arc::new(publication.finish().unwrap());
        let mut activations: BTreeMap<
            ClusterNodeId,
            Arc<crate::distribution_range_consensus::NativeJointCatalogueActivationReceipt>,
        > = BTreeMap::new();
        let mut image_release = None;
        for local in [node("n1"), node("n2"), node("n3")] {
            let admission = published
                .acquire_local_open(&fixture.clients[&local], &node("n1"), &parent)
                .unwrap();
            let activation = Arc::new(
                admission
                    .persist_local_catalogue_activation(&parent)
                    .unwrap(),
            );
            if local == node("n3") {
                assert!(
                    admission
                        .retain_image_release_owner(activations[&node("n1")].clone())
                        .is_err(),
                    "another actual member receipt must not release this image"
                );
                image_release = Some(
                    admission
                        .retain_image_release_owner(activation.clone())
                        .unwrap(),
                );
            }
            activations.insert(local, activation);
        }
        let mut image_release = image_release.unwrap();
        assert!(
            image_release.release(&parent).is_err(),
            "actual quorum owners still hold the target freeze"
        );
        drop(published);
        drop(prepared);
        drop(closed);
        let canceled = CancellationToken::uncancelable()
            .child_with_deadline(Instant::now() - Duration::from_millis(1));
        assert!(image_release.release(&canceled).is_err());
        {
            let held = fixture.native.services[&node("n3")].lock();
            assert!(image_release.release(&parent).is_err());
            drop(held);
        }
        assert!(fixture.native.databases[&node("n3")]
            .read()
            .range_mutation_gate()
            .try_enter(old.id)
            .is_err());
        assert!(image_release
            .test_release_after_actual_drop_error(&parent)
            .is_err());
        // Actual owner has drained, but the protected joint guard remains CLOSED
        // until this SAME retained receipt arms it. A retry cannot mint a new TTL.
        let before_arm = fixture.native.services[&node("n3")]
            .lock()
            .range_write_progress(old.id);
        let refused_key = fixture.key(old.id, "must-refuse-before-image-arm");
        {
            let db = fixture.native.databases[&node("n3")].write();
            let mut tx = db.begin_transaction().unwrap();
            tx.insert("items", crate::Record::new(&refused_key))
                .unwrap();
            assert!(tx.commit().is_err());
            assert!(db.get("items", &refused_key).unwrap().is_none());
        }
        assert_eq!(
            fixture.native.services[&node("n3")]
                .lock()
                .range_write_progress(old.id),
            before_arm
        );
        image_release.release(&parent).unwrap();
        image_release.release(&parent).unwrap();
        for local in [node("n1"), node("n2")] {
            runtimes[&local]
                .release_activated_new_member(activations[&local].clone())
                .unwrap();
        }
        None
    };
    assert!(fixture.native.databases[&node("n0")]
        .read()
        .range_mutation_gate()
        .try_enter(old.id)
        .is_err());
    assert_eq!(
        fixture.native.databases[&node("n3")]
            .read()
            .local_namespace_identity("items")
            .unwrap(),
        original_uid
    );
    for local in [node("n1"), node("n2"), node("n3")] {
        let db = fixture.native.databases[&local].read();
        assert_eq!(db.actual_native_writer_enforcement_version().unwrap(), 1);
        let native = db.current_native_range_source_lineage(old.id).unwrap();
        assert_eq!(native.range_index(), 1);
        assert_eq!(native.logical_history_sha256(), old_h);
        assert_eq!(native.native_write_range().unwrap(), &activation_range);
        assert_eq!(db.get("items", &key).unwrap(), Some(acknowledged.clone()));
    }
    assert!(
        fixture.coordinators[&node("n3")]
            .recover_local_leadership(8)
            .unwrap()
            >= 1
    );
    let next_key = fixture.key(old.id, "actual-new-target-next-ack");
    let next = crate::Record::new(&next_key);
    {
        let db = fixture.native.databases[&node("n3")].write();
        let mut tx = db.begin_transaction().unwrap();
        tx.insert("items", next.clone()).unwrap();
        tx.commit().unwrap();
    }
    crate::storage::write_atomic(
        &fixture
            .native
            .root
            .path()
            .join("actual-joint-selected-range-ack-ledger.json"),
        &serde_json::to_vec(&vec![acknowledged.as_ref(), &next]).unwrap(),
        true,
    )
    .unwrap();
    let mut actual_h = None;
    for local in [node("n1"), node("n2"), node("n3")] {
        let db = fixture.native.databases[&local].read();
        let native = db.current_native_range_source_lineage(old.id).unwrap();
        assert_eq!(native.range_index(), 2);
        if let Some(h) = &actual_h {
            assert_eq!(native.logical_history_sha256(), h);
        } else {
            actual_h = Some(native.logical_history_sha256().to_owned());
        }
        assert_eq!(db.get("items", &key).unwrap(), Some(acknowledged.clone()));
        assert_eq!(db.get("items", &next_key).unwrap().as_deref(), Some(&next));
    }
    if let Some(continuation) = &release_continuation {
        // All members are now ACTUAL R2. A fresh retry of the historical R1
        // release must preserve that complete newer history, not reset a floor.
        exercise_one_fresh_release(&fixture, continuation, &node("n3"), false);
        for id in [node("n1"), node("n2"), node("n3")] {
            let db = fixture.native.databases[&id].read();
            assert_eq!(
                db.current_native_range_source_lineage(old.id)
                    .unwrap()
                    .range_index(),
                2
            );
            assert_eq!(db.get("items", &next_key).unwrap().as_deref(), Some(&next));
        }
    }
    {
        let db = fixture.native.databases[&node("n0")].write();
        let mut tx = db.begin_transaction().unwrap();
        tx.insert(
            "items",
            crate::Record::new(fixture.key(old.id, "stale-old-must-refuse")),
        )
        .unwrap();
        assert!(tx.commit().is_err());
    }
    let request = fixture.request(&next_key, &activation_range);
    let first = fixture.clients[&node("n3")]
        .acquire_metadata_read_observation(
            &node("n1"),
            request.business_request_id,
            old.id,
            activation_range.epoch,
            Duration::from_secs(5),
            &parent,
        )
        .unwrap();
    let observed = observed(fixture.coordinators[&node("n3")].strong_point_read(
        &fixture.runtimes[&node("n3")],
        Some(fixture.hosts[&node("n3")].clone()),
        fixture.clients[&node("n3")].clone(),
        first,
        request,
        &parent,
    ));
    assert_eq!(observed.record().map(|record| record.as_ref()), Some(&next));
    assert_eq!(observed.audit().range_index, 2);
    fixture.idle();
    if continuation == JointFullContinuation::CurrentJointAuthorityRefusals {
        assert!(
            release_continuation.is_some(),
            "authority controls require real all-member release"
        );
        current_joint_authority_tests::exercise(
            &fixture,
            &activation_range,
            acknowledged.as_ref(),
            &next,
            decision.data().promise().nonce,
        );
        return JointTestOutcome::FullHandoffThenCurrentJointAuthorityRefusals;
    }
    if continuation == JointFullContinuation::PostJointNoDataRecovery {
        assert!(
            release_continuation.is_some(),
            "post-joint prerequisite must use real all-member release continuation"
        );
        eprintln!("ACTUAL_POST_JOINT_NO_DATA_PREREQUISITE all3release=true targetACK2=true StrongR2=true no-ABA-invalidation-before-recovery=true");
        post_joint_no_data_tests::exercise_actual_new_domain_prepare_only_recovery(
            &fixture,
            &activation_range,
            acknowledged.as_ref(),
            &next,
        );
        return JointTestOutcome::FullHandoffThenPostJointNoDataAck4Strong;
    }
    if let Some(continuation) = &release_continuation {
        let stale = fresh_release_admission(&fixture, continuation, &node("n3")).unwrap();
        let gate = fixture.native.databases[&node("n3")]
            .read()
            .range_mutation_gate();
        let new_owner = gate.try_freeze(old.id, Uuid::now_v7()).unwrap();
        drop(new_owner);
        // Actual new-owner/drop ABA invalidates serving generation even though
        // the gate is now unfrozen and immutable R2/H2/ACK rows are unchanged.
        assert!(stale.release_actual_local().is_err());
        let mut replacement = continuation
            .begin_fresh_release_phase(Duration::from_secs(4))
            .unwrap();
        assert!(replacement
            .acquire_actual_member(&fixture.clients[&node("n3")], &node("n1"))
            .is_err());
        assert!(replacement.finish().is_err());
        parent.cancel();
        assert!(continuation
            .begin_fresh_release_phase(Duration::from_secs(4))
            .is_err());
        let db = fixture.native.databases[&node("n3")].read();
        assert_eq!(
            db.current_native_range_source_lineage(old.id)
                .unwrap()
                .range_index(),
            2
        );
        assert_eq!(db.get("items", &key).unwrap(), Some(acknowledged.clone()));
        assert_eq!(db.get("items", &next_key).unwrap().as_deref(), Some(&next));
    }
    assert!(
        release_continuation.is_some(),
        "full acceptance must exercise fresh all-member release continuation"
    );
    eprintln!("ACTUAL_JOINT_FULL_ACCEPTANCE all-three-release-checked=true target-ACK2-checked=true strong-R2-checked=true idempotent-R2-checked=true ABA-and-original-caller-refusal-checked=true");
    JointTestOutcome::FullHandoffAckAndStrong
}

/// Complete ONLY the bounded Prepared→protected-native-publication phase.
/// No catalogue/open/release grant is inferred from renewed native facts.
fn exercise_fresh_native_publication(
    fixture: &EnforcedNetwork,
    prepared: &Arc<NativeJointPreparedServingQuorum>,
    parent: &CancellationToken,
    range: crate::RangeId,
    test_cancellation: bool,
) -> Arc<crate::distribution_range_handoff::NativeJointPublishedServingQuorum> {
    let remaining = prepared.test_original_budget_remaining_ms().0.unwrap();
    std::thread::sleep(Duration::from_millis(
        u64::try_from(remaining).unwrap() + 10,
    ));
    assert!(
        prepared.require_current().is_err(),
        "the ORIGINAL metadata window really expired"
    );
    let mut wrong = prepared
        .begin_fresh_preparation_phase(Duration::from_secs(4))
        .unwrap();
    assert!(
        wrong
            .acquire_actual_member(&fixture.clients[&node("n0")], &node("n1"))
            .is_err(),
        "old member0 is not a final NEW participant"
    );
    assert!(
        wrong
            .acquire_actual_member(&fixture.clients[&node("n1")], &node("n1"))
            .is_err(),
        "a poisoned phase cannot ignore an earlier error"
    );
    assert!(wrong.finish().is_err());
    let mut partial = prepared
        .begin_fresh_preparation_phase(Duration::from_secs(4))
        .unwrap();
    for id in [node("n1"), node("n2")] {
        partial
            .acquire_actual_member(&fixture.clients[&id], &node("n1"))
            .unwrap();
    }
    match partial.finish() {
        Ok(_) => panic!("missing actual target/leader3 must not shrink the full participant set"),
        Err(error) => assert!(
            error.to_string().contains("omit/replace"),
            "missing-member check must not pass merely by timeout: {error}"
        ),
    }
    let mut publication = prepared.resume_native_publication().unwrap();
    let mut lost_reply_sha = None;
    for local in [node("n1"), node("n2"), node("n3")] {
        let mut completed = false;
        for attempt in 0..3 {
            let attempt_started = std::time::Instant::now();
            eprintln!(
                "ACTUAL_JOINT_PHASE fresh-publication-attempt node={local} attempt={attempt}"
            );
            let result = (|| -> crate::Result<()> {
                let mut fresh = prepared.begin_fresh_preparation_phase(Duration::from_secs(4))?;
                for id in [node("n1"), node("n2"), node("n3")] {
                    fresh.acquire_actual_member(&fixture.clients[&id], &node("n1"))?;
                }
                let fresh = Arc::new(fresh.finish()?);
                assert!(
                    prepared.require_current().is_err(),
                    "fresh authority must NOT revive old objects"
                );
                let admission =
                    fresh.acquire_local_activation(&fixture.clients[&local], &node("n1"))?;
                assert!(
                    fresh
                        .acquire_local_activation(&fixture.clients[&local], &node("n1"))
                        .is_err(),
                    "one phase cannot select a second local publication"
                );
                if local == node("n1") && attempt == 0 {
                    lost_reply_sha =
                        Some(publication.test_lose_actual_publication_reply(&admission, parent)?);
                    eprintln!("ACTUAL_JOINT_PHASE test-lost-reply-after-real-native-producer node={local}");
                    return Err(crate::BicDbError::QueryTimedOut);
                }
                publication.publish_actual_local_member(&admission, parent)?;
                assert!(
                    publication
                        .publish_actual_local_member(&admission, parent)
                        .is_err(),
                    "one admission cannot republish/recover twice"
                );
                Ok(())
            })();
            eprintln!("ACTUAL_JOINT_PHASE fresh-publication-result node={local} attempt={attempt} elapsed_ms={} error={:?}", attempt_started.elapsed().as_millis(), result.as_ref().err());
            match result {
                Ok(())=>{completed=true;break;},
                Err(crate::BicDbError::QueryTimedOut) if attempt<2=>eprintln!("ACTUAL_JOINT_PHASE bounded-fresh-publication-retry node={local} original-still-expired=true"),
                Err(error)=>panic!("fresh native publication failed for {local}: {error}"),
            }
        }
        assert!(completed);
    }
    // Aggregation is historical fact validation, not another native write.
    // Recollect actual current per-node authority; preserve all native audits.
    let mut current = prepared
        .begin_fresh_preparation_phase(Duration::from_secs(4))
        .unwrap();
    for id in [node("n1"), node("n2"), node("n3")] {
        current
            .acquire_actual_member(&fixture.clients[&id], &node("n1"))
            .unwrap();
    }
    let current = Arc::new(current.finish().unwrap());
    let published = Arc::new(publication.finish_with_fresh_phase(current).unwrap());
    assert_eq!(
        published.test_actual_receipt_checksums().unwrap()[&node("n1")],
        lost_reply_sha.expect("genuine lost reply must have reached protected WAL producer"),
        "recover exact original immutable quorum audit, never rewrite under new MQ"
    );
    assert_eq!(
        published
            .test_actual_receipt_checksums()
            .unwrap()
            .keys()
            .cloned()
            .collect::<Vec<_>>(),
        vec![node("n1"), node("n2"), node("n3")]
    );
    assert!(
        published
            .acquire_local_open(&fixture.clients[&node("n3")], &node("n1"), parent)
            .is_err(),
        "native publication phase is NOT a renewed catalogue/open/release grant"
    );
    for id in [node("n1"), node("n2"), node("n3")] {
        let db = fixture.native.databases[&id].read();
        assert!(db.range_mutation_gate().try_enter(range).is_err());
        assert_eq!(
            db.current_native_range_source_lineage(range)
                .unwrap()
                .range_index(),
            prepared.data().range_index()
        );
    }
    assert!(prepared.require_current().is_err());
    if test_cancellation {
        prepared.test_cancel_original_quorum();
        assert!(matches!(
            prepared.begin_fresh_preparation_phase(Duration::from_secs(4)),
            Err(crate::BicDbError::QueryCanceled)
        ));
        assert!(prepared.resume_native_publication().is_err());
        parent.cancel();
        assert!(
            prepared
                .begin_fresh_preparation_phase(Duration::from_secs(4))
                .is_err(),
            "ORIGINAL caller cancellation must stop new phases too"
        );
    }
    eprintln!("ACTUAL_JOINT_FRESH_PUBLICATION_ONLY all-final-members=3 old-quorum-expired=true original-caller-cancellation-preserved=true no-catalogue-open-release-grant=true");
    published
}

/// Every fresh round checks the SAME original cut. This stage deliberately
/// keeps all real owners Frozen; release needs its own current lifecycle proof.
fn exercise_fresh_open_while_frozen(
    fixture: &EnforcedNetwork,
    prepared: &Arc<NativeJointPreparedServingQuorum>,
    published: Arc<crate::distribution_range_handoff::NativeJointPublishedServingQuorum>,
    parent: &CancellationToken,
    range: crate::RangeId,
) {
    exercise_fresh_open_boundary(fixture, prepared, published, parent, range, true);
}

fn exercise_fresh_open_boundary(
    fixture: &EnforcedNetwork,
    prepared: &Arc<NativeJointPreparedServingQuorum>,
    published: Arc<crate::distribution_range_handoff::NativeJointPublishedServingQuorum>,
    parent: &CancellationToken,
    range: crate::RangeId,
    terminal_controls: bool,
) -> (
    BTreeMap<
        ClusterNodeId,
        Arc<crate::distribution_range_consensus::NativeJointCatalogueActivationReceipt>,
    >,
    Option<crate::distribution_data::NativeJointImageReleaseOwner>,
) {
    let mut image_owner = None;
    let mut activations = BTreeMap::new();
    let mut lost_target = None;
    for local in [node("n3"), node("n1"), node("n2")] {
        let mut complete = false;
        // Preserve three pre-receipt attempts; reserve ONE additional phase
        // solely to recover the deliberate first-actual-receipt reply loss.
        let limit = if local == node("n3") { 4 } else { 3 };
        for attempt in 0..limit {
            if attempt == 3 && lost_target.is_none() {
                break;
            }
            let attempt_started = Instant::now();
            let mut stage = "begin-fresh-open".to_owned();
            let result = (|| -> crate::Result<()> {
                let mut round = published.begin_fresh_open_phase(Duration::from_secs(4))?;
                for id in [node("n1"), node("n2"), node("n3")] {
                    stage = format!("collect-{id}");
                    round.acquire_actual_member(&fixture.clients[&id], &node("n1"))?;
                }
                stage = "finish-full-open-round".into();
                let round = Arc::new(round.finish()?);
                stage = "local-post-QQ-MQ-and-binding".into();
                let open = round.acquire_local_open(&fixture.clients[&local], &node("n1"))?;
                assert!(round
                    .acquire_local_open(&fixture.clients[&local], &node("n1"))
                    .is_err());
                stage = "actual-activate-or-reconcile-catalogue".into();
                let receipt = Arc::new(open.persist_local_catalogue_activation(parent)?);
                assert!(open.persist_local_catalogue_activation(parent).is_err());
                if !terminal_controls && local == node("n3") && image_owner.is_none() {
                    image_owner = Some(open.retain_image_release_owner(receipt.clone())?);
                }
                if local == node("n3") && lost_target.is_none() {
                    let sha = receipt.activation_sha256()?;
                    let before = fixture.native.services[&local]
                        .lock()
                        .range_write_progress(range);
                    assert_eq!(
                        before.current_epoch,
                        published.data().activation_range().epoch
                    );
                    lost_target = Some((sha, receipt));
                    // The old response expires. A new controller/phase must
                    // read the SAME protected activation, without rewriting it.
                    drop(open);
                    drop(round);
                    std::thread::sleep(Duration::from_secs(4) + Duration::from_millis(10));
                    assert!(lost_target.as_ref().unwrap().1.require_live().is_err());
                    eprintln!("ACTUAL_JOINT_OPEN test-lost-reply-after-real-catalogue-fsync target={local}");
                    return Err(crate::BicDbError::QueryTimedOut);
                }
                if local == node("n3") {
                    assert_eq!(
                        receipt.activation_sha256()?,
                        lost_target
                            .as_ref()
                            .expect("actual target lost reply reached producer")
                            .0
                    );
                    assert!(
                        lost_target.as_ref().unwrap().1.require_live().is_err(),
                        "new phase must not revive original activation receipt"
                    );
                }
                activations.insert(local.clone(), receipt);
                Ok(())
            })();
            eprintln!("ACTUAL_JOINT_OPEN_RESULT node={local} attempt={attempt} stage={stage} elapsed_ms={} error={:?}",attempt_started.elapsed().as_millis(),result.as_ref().err());
            match result {
                Ok(()) => {
                    complete = true;
                    break;
                }
                Err(crate::BicDbError::QueryTimedOut) if attempt + 1 < limit => eprintln!(
                    "ACTUAL_JOINT_OPEN fresh-current-retry node={local} attempt={attempt}"
                ),
                Err(e) => panic!("genuine fresh open phase failed for {local}: {e}"),
            }
        }
        assert!(complete);
    }
    assert_eq!(
        activations.keys().cloned().collect::<Vec<_>>(),
        vec![node("n1"), node("n2"), node("n3")]
    );
    assert!(prepared.require_current().is_err());
    for local in [node("n1"), node("n2"), node("n3")] {
        let db = fixture.native.databases[&local].read();
        assert!(db.range_mutation_gate().try_enter(range).is_err());
        assert_eq!(
            db.current_native_range_source_lineage(range)
                .unwrap()
                .range_index(),
            published.data().range_index()
        );
    }
    if !terminal_controls {
        return (activations, image_owner);
    }
    // A real new serving-generation invalidation cannot be hidden by another
    // fresh metadata request, even though all recorded hashes are unchanged.
    fixture.native.databases[&node("n3")]
        .read()
        .range_mutation_gate()
        .invalidate_serving_generation(range);
    let mut stale = published
        .begin_fresh_open_phase(Duration::from_secs(4))
        .unwrap();
    assert!(stale
        .acquire_actual_member(&fixture.clients[&node("n3")], &node("n1"))
        .is_err());
    assert!(stale.finish().is_err());
    parent.cancel();
    assert!(published
        .begin_fresh_open_phase(Duration::from_secs(4))
        .is_err());
    eprintln!("ACTUAL_JOINT_FRESH_OPEN_ONLY exact-target-epoch-transition=true all-catalogue-activations=3 lost-reply-original-expired=true native-open-generation-ABA-refused=true still-frozen=true no-release-or-newACK-claim=true");
    (activations, image_owner)
}

fn fresh_release_admission(
    fixture: &EnforcedNetwork,
    continuation: &Arc<crate::distribution_range_handoff::NativeJointReleaseContinuation>,
    local: &ClusterNodeId,
) -> crate::Result<crate::distribution_range_handoff::NativeJointReleaseAdmission> {
    let mut round = continuation.begin_fresh_release_phase(Duration::from_secs(4))?;
    for id in [node("n1"), node("n2"), node("n3")] {
        round.acquire_actual_member(&fixture.clients[&id], &node("n1"))?;
    }
    Arc::new(round.finish()?).acquire_local_release(&fixture.clients[local], &node("n1"))
}
fn exercise_one_fresh_release(
    fixture: &EnforcedNetwork,
    continuation: &Arc<crate::distribution_range_handoff::NativeJointReleaseContinuation>,
    local: &ClusterNodeId,
    drop_error: bool,
) {
    let mut complete = false;
    for attempt in 0..3 {
        let start = Instant::now();
        let result = (|| -> crate::Result<()> {
            let admission = fresh_release_admission(fixture, continuation, local)?;
            if drop_error {
                let error = admission
                    .test_release_after_actual_drop_error()
                    .unwrap_err();
                if !error.to_string().contains("AFTER actual drop BEFORE arm") {
                    return Err(error);
                }
                // Receipt is now a real dropped/unarmed fact. The old bounded
                // phase expires; the original caller stays live independently.
                std::thread::sleep(Duration::from_secs(4) + Duration::from_millis(10));
                assert!(admission.require_live().is_err());
                assert!(admission.release_actual_local().is_err());
            } else {
                admission.release_actual_local()?;
                assert!(admission.release_actual_local().is_err());
            }
            Ok(())
        })();
        eprintln!("ACTUAL_JOINT_RELEASE_RESULT node={local} attempt={attempt} drop_error={drop_error} elapsed_ms={} error={:?}",start.elapsed().as_millis(),result.as_ref().err());
        match result {
            Ok(()) => {
                complete = true;
                break;
            }
            Err(crate::BicDbError::QueryTimedOut) if attempt < 2 => {}
            Err(e) => panic!("actual fresh release failed for {local}: {e}"),
        }
    }
    assert!(
        complete,
        "finite genuine same-open release attempts exhausted"
    );
}
fn exercise_fresh_release(
    fixture: &EnforcedNetwork,
    continuation: &Arc<crate::distribution_range_handoff::NativeJointReleaseContinuation>,
    parent: &CancellationToken,
    range: crate::RangeId,
) {
    let mut partial = continuation
        .begin_fresh_release_phase(Duration::from_secs(4))
        .unwrap();
    for id in [node("n1"), node("n2")] {
        partial
            .acquire_actual_member(&fixture.clients[&id], &node("n1"))
            .unwrap();
    }
    assert!(
        partial.finish().is_err(),
        "missing actual new leader/third cannot shrink release set"
    );
    let mut wrong = continuation
        .begin_fresh_release_phase(Duration::from_secs(4))
        .unwrap();
    assert!(wrong
        .acquire_actual_member(&fixture.clients[&node("n0")], &node("n1"))
        .is_err());
    assert!(wrong.finish().is_err());
    // Original activation receipt has actually expired; the old API is still
    // rejected even though this new purpose can inspect the stored fact.
    assert!(continuation.test_original_image_release().is_err());
    exercise_one_fresh_release(fixture, continuation, &node("n3"), true);
    let before = fixture.native.services[&node("n3")]
        .lock()
        .range_write_progress(range);
    let refused_key = fixture.key(range, "fresh-release-dropped-unarmed-refuses");
    {
        let db = fixture.native.databases[&node("n3")].write();
        let mut tx = db.begin_transaction().unwrap();
        tx.insert("items", crate::Record::new(&refused_key))
            .unwrap();
        assert!(tx.commit().is_err());
        assert!(db.get("items", &refused_key).unwrap().is_none());
    }
    assert_eq!(
        fixture.native.services[&node("n3")]
            .lock()
            .range_write_progress(range),
        before
    );
    exercise_one_fresh_release(fixture, continuation, &node("n3"), false);
    for id in [node("n1"), node("n2")] {
        exercise_one_fresh_release(fixture, continuation, &id, false);
    }
    parent.check().unwrap();
    eprintln!("ACTUAL_JOINT_RELEASE all-final-members=3 target-leader-released=true old-receipt-expired=true actual-drop-before-arm-recovered=true caller-original=true still-no-ACK2-at-this-marker=true");
}
