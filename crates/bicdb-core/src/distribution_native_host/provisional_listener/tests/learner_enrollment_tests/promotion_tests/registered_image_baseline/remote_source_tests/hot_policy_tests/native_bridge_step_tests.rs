//! After the decision is durable and the membership is final, every NEW
//! member is driven ONLY through bounded bridge step RPCs. The driver hands
//! no member a lease, role, own observation, quorum or phase object and never
//! calls a member's in-process preparation; host handles are used for
//! memory-only observation of what each member itself retained.
use super::*;
use crate::distribution_range_handoff::{
    test_arm_bridge_second_window_fault, NativeBridgeSecondWindowFault,
    NativeJointBridgePhaseKindV1 as PhaseKind, NativeJointDecisionData,
    NativeJointHandoffStepOperationV1 as StepOperation, NativeJointHandoffStepOutcomeV1,
    NativeJointHandoffStepRequestV1, NativeJointHandoffStepStageV1 as StepStage,
    NativeRemoteBridgePublicationCertificateRound,
};
use crate::distribution_transport::AuthenticatedNativeJointBridgeStepFrame;

pub(super) fn is_case(case: SourceCase) -> bool {
    matches!(
        case,
        SourceCase::NativeBridgeStepPrepareObserve
            | SourceCase::NativeBridgeStepRefusals
            | SourceCase::NativeBridgeStepPublish
            | SourceCase::NativeBridgeStepPublishFaults
            | SourceCase::NativeBridgeStepOpen
    )
}
#[test]
fn native_bridge_step_open_every_member_activates_own_catalogue_and_recovers_unconfirmed() {
    run_registered_image_baseline_with_source(
        Case::Required,
        Some(SourceCase::NativeBridgeStepOpen),
    );
}
#[test]
fn native_bridge_step_publish_every_member_publishes_own_bridge_and_certificate_counts_all() {
    run_registered_image_baseline_with_source(
        Case::Required,
        Some(SourceCase::NativeBridgeStepPublish),
    );
}
#[test]
fn native_bridge_step_publish_refuses_unprepared_and_recovers_unconfirmed_second_window() {
    run_registered_image_baseline_with_source(
        Case::Required,
        Some(SourceCase::NativeBridgeStepPublishFaults),
    );
}
#[test]
fn native_bridge_step_prepare_retains_member_custody_and_phase_observations_agree() {
    run_registered_image_baseline_with_source(
        Case::Required,
        Some(SourceCase::NativeBridgeStepPrepareObserve),
    );
}
#[test]
fn native_bridge_step_refuses_non_initiator_outsider_and_foreign_decision_or_epoch() {
    run_registered_image_baseline_with_source(
        Case::Required,
        Some(SourceCase::NativeBridgeStepRefusals),
    );
}

fn index(local: &ClusterNodeId) -> usize {
    (0..5).find(|i| node(*i) == *local).unwrap()
}
fn prepare_operation(data: &NativeJointDecisionData) -> StepOperation {
    StepOperation::BridgePrepare {
        decision_sha256: data.checksum_sha256().into(),
    }
}
fn publish_operation(data: &NativeJointDecisionData) -> StepOperation {
    StepOperation::BridgePublish {
        decision_sha256: data.checksum_sha256().into(),
    }
}
fn open_operation(data: &NativeJointDecisionData) -> StepOperation {
    StepOperation::BridgeOpen {
        decision_sha256: data.checksum_sha256().into(),
    }
}
fn observe_operation(data: &NativeJointDecisionData, kind: PhaseKind) -> StepOperation {
    StepOperation::BridgePhaseObserve {
        decision_sha256: data.checksum_sha256().into(),
        kind,
    }
}
struct Ledger<'a> {
    entries: Vec<serde_json::Value>,
    output: &'a std::path::Path,
}
impl Ledger<'_> {
    fn record(&mut self, entry: serde_json::Value) {
        self.entries.push(entry);
        evidence(
            &self.output.join("native-bridge-step-ledger.json"),
            &self.entries,
        )
        .unwrap();
    }
}
/// One genuine pinned exchange from `client`. The request identity is built
/// from decision DATA; the member proves everything else itself.
fn exchange(
    client: &TcpClusterRelocationTransport,
    member: &ClusterNodeId,
    data: &NativeJointDecisionData,
    operation: StepOperation,
    ledger: &mut Ledger<'_>,
) -> Result<AuthenticatedNativeJointBridgeStepFrame> {
    let parent = budget(5000);
    let business = Uuid::now_v7();
    let own = client
        .acquire_host_metadata_observation(&node(0), business, Duration::from_secs(5), &parent)
        .unwrap();
    let request =
        NativeJointHandoffStepRequestV1::from_decision(data, operation.clone(), business, 5000)
            .unwrap();
    let started = Instant::now();
    let result = match &operation {
        StepOperation::BridgePrepare { .. } => {
            client.request_native_joint_bridge_prepare(member, data, &request, &own, &parent)
        }
        StepOperation::BridgePublish { .. } => {
            client.request_native_joint_bridge_publish(member, data, &request, &own, &parent)
        }
        StepOperation::BridgeOpen { .. } => {
            client.request_native_joint_bridge_open(member, data, &request, &own, &parent)
        }
        StepOperation::BridgePhaseObserve { kind, .. } => client
            .request_native_joint_bridge_phase_observation(
                member, data, *kind, &request, &own, &parent,
            ),
        StepOperation::TargetInstall { .. } => unreachable!(),
    };
    ledger.record(serde_json::json!({
        "operation":"native_joint_bridge_step","request":request,"recipient":member,
        "actual_initiator":client.local_metadata_read_participant().node_id(),
        "elapsed_ms":started.elapsed().as_millis() as u64,
        "outcome":result.as_ref().ok().map(|actual|actual.outcome()),
        "metadata_anchor":result.as_ref().ok().map(|actual|actual.metadata_anchor()),
        "exact_error":result.as_ref().err().map(ToString::to_string),
        "reply_is_checksummed_DATA_not_authority":true,
    }));
    let frame = result?;
    frame.require_live()?;
    assert_eq!(&frame.reply().node_id, member);
    assert_eq!(frame.actual_peer(), member);
    assert_eq!(
        frame.actual_peer_incarnation(),
        data.new_members()[member].incarnation
    );
    assert_eq!(&frame.reply().birth, frame.actual_peer_birth());
    assert_eq!(
        frame.actual_observer(),
        client.local_metadata_read_participant().node_id()
    );
    Ok(frame)
}
/// Repeats the same logical step until the member reports its retained
/// preparation. Every call is one bounded request.
fn prepare(
    client: &TcpClusterRelocationTransport,
    member: &ClusterNodeId,
    data: &NativeJointDecisionData,
    ledger: &mut Ledger<'_>,
) -> AuthenticatedNativeJointBridgeStepFrame {
    for _ in 0..6 {
        let frame = exchange(client, member, data, prepare_operation(data), ledger).unwrap();
        if frame.outcome().complete {
            return frame;
        }
    }
    panic!("member {member} did not retain its preparation in six bounded steps");
}
/// Repeats the same logical publish step until the member reports its own
/// confirmed durable bridge. Every call is one bounded request.
fn publish(
    client: &TcpClusterRelocationTransport,
    member: &ClusterNodeId,
    data: &NativeJointDecisionData,
    ledger: &mut Ledger<'_>,
) -> AuthenticatedNativeJointBridgeStepFrame {
    for _ in 0..4 {
        let frame = exchange(client, member, data, publish_operation(data), ledger).unwrap();
        if frame.outcome().complete {
            return frame;
        }
        assert_eq!(frame.outcome().stage, StepStage::BridgePublication);
        assert!(frame.outcome().bridge_receipt_sha256.is_none());
    }
    panic!("member {member} did not confirm its bridge publication in four bounded steps");
}
/// Repeats the same logical open step until the member reports its own
/// confirmed durable catalogue activation.
fn open(
    client: &TcpClusterRelocationTransport,
    member: &ClusterNodeId,
    data: &NativeJointDecisionData,
    ledger: &mut Ledger<'_>,
) -> AuthenticatedNativeJointBridgeStepFrame {
    for _ in 0..4 {
        let frame = exchange(client, member, data, open_operation(data), ledger).unwrap();
        if frame.outcome().complete {
            return frame;
        }
        assert_eq!(frame.outcome().stage, StepStage::BridgeActivation);
        assert!(frame.outcome().activation_sha256.is_none());
    }
    panic!("member {member} did not confirm its catalogue activation in four bounded steps");
}
/// What the member itself retained and durably holds of its publication.
fn publication(
    controls: &[NativeMetadataControlHost],
    target_control: &NativeMetadataControlHost,
    member: &ClusterNodeId,
    range: crate::RangeId,
) -> Option<serde_json::Value> {
    let i = index(member);
    if i == 4 {
        target_control
            .test_registered_bridge_publication(range)
            .unwrap()
    } else {
        controls[i].test_initial_bridge_publication(range).unwrap()
    }
}
/// The member's durable bridge is its own, for this decision, with pointer
/// and archive equal, and its durable votes name EVERY NEW member with the
/// preparation identity that member itself reported.
fn assert_durable_bridge(
    evidence: &serde_json::Value,
    member: &ClusterNodeId,
    data: &NativeJointDecisionData,
    prepared: &BTreeMap<ClusterNodeId, NativeJointHandoffStepOutcomeV1>,
) -> String {
    let durable = &evidence["durable_bridge"];
    assert!(durable.is_object(), "{member} holds no durable bridge");
    assert_eq!(durable["decision_sha256"], data.checksum_sha256());
    assert_eq!(durable["local_node_id"], serde_json::json!(member));
    assert_eq!(durable["archive_equals_pointer"], true);
    let votes = durable["new_prepared"].as_object().unwrap();
    assert_eq!(
        votes.keys().cloned().collect::<BTreeSet<_>>(),
        data.new_members()
            .keys()
            .map(|node| node.as_str().to_owned())
            .collect::<BTreeSet<_>>()
    );
    let mut contexts = BTreeSet::new();
    for (node, original) in prepared {
        let vote = &votes[node.as_str()];
        assert_eq!(
            vote["preparation_sha256"],
            serde_json::json!(original.preparation_sha256)
        );
        assert_eq!(
            vote["native_database_incarnation"],
            serde_json::json!(original.native_database_incarnation)
        );
        assert_eq!(
            vote["actual_local_visibility_sequence"],
            serde_json::json!(original.local_visibility_sequence)
        );
        assert!(contexts.insert(vote["metadata_context_id"].to_string()));
        assert_ne!(
            vote["metadata_context_id"],
            durable["post_quorum_metadata_context_id"]
        );
        assert!(vote["metadata_term"].as_u64() <= durable["post_quorum_metadata_term"].as_u64());
        assert!(
            vote["metadata_read_index"].as_u64()
                <= durable["post_quorum_metadata_read_index"].as_u64()
        );
    }
    durable["checksum_sha256"].as_str().unwrap().to_owned()
}
/// Fault-control exchange: decoded DATA or the member's exact refusal.
fn raw(
    client: &TcpClusterRelocationTransport,
    member: &ClusterNodeId,
    request: &NativeJointHandoffStepRequestV1,
    name: &str,
    ledger: &mut Ledger<'_>,
) -> String {
    let result = client.test_send_joint_bridge_step_request(member, request, &budget(5000));
    ledger.record(serde_json::json!({
        "operation":name,"request":request,"recipient":member,
        "actual_initiator":client.local_metadata_read_participant().node_id(),
        "returned_success":result.is_ok(),
        "exact_error":result.as_ref().err().map(ToString::to_string),
    }));
    result
        .err()
        .unwrap_or_else(|| panic!("{name} must be refused by the member itself"))
        .to_string()
}
fn retained(
    controls: &[NativeMetadataControlHost],
    target_control: &NativeMetadataControlHost,
    member: &ClusterNodeId,
    range: crate::RangeId,
) -> Option<serde_json::Value> {
    let i = index(member);
    if i == 4 {
        target_control
            .test_registered_bridge_preparation(range)
            .unwrap()
    } else {
        controls[i].test_initial_bridge_preparation(range).unwrap()
    }
}

#[allow(clippy::too_many_arguments)]
pub(super) fn exercise(
    case: SourceCase,
    decision: NativeJointDurableDecision,
    controls: &[NativeMetadataControlHost],
    all_clients: &[Arc<TcpClusterRelocationTransport>],
    target_control: &NativeMetadataControlHost,
    current: &ClusterTopology,
    selected: crate::RangeId,
    output: &std::path::Path,
) {
    assert!(is_case(case));
    let data = decision.data().clone();
    // The in-process driver's decision custody ends here, exactly where the
    // in-process route hands it over. Each member keeps its own original
    // owners (retained OLD closure / Registered joint target) in its own host.
    drop(decision.into_new_serving_owners().unwrap());
    let mut ledger = Ledger {
        entries: Vec::new(),
        output,
    };
    let new = data.new_members().keys().cloned().collect::<Vec<_>>();
    let committed_leader = current.range_by_id(selected).unwrap().leader.clone();
    assert!(new.contains(&node(4)) && new.contains(&committed_leader));
    // The coordinator is the metadata leader. It cannot send itself an RPC,
    // so its own preparation is triggered by the committed range leader.
    let initiator = |member: &ClusterNodeId| -> usize {
        if *member != node(0) {
            0
        } else {
            assert_ne!(committed_leader, node(0));
            index(&committed_leader)
        }
    };
    let outsider = data
        .old_members()
        .keys()
        .find(|old| !new.contains(old))
        .cloned()
        .expect("one original member retires");
    let bystander = new
        .iter()
        .find(|n| **n != node(0) && **n != committed_leader)
        .cloned()
        .expect("a NEW member that is neither metadata nor range leader");
    ledger.record(
        serde_json::json!({"operation":"fixture_identities","new_members":new,
        "metadata_leader":node(0),"committed_range_leader":committed_leader,
        "retired_original_member":outsider,"non_initiator_new_member":bystander,
        "decision_sha256":data.checksum_sha256()}),
    );
    for member in &new {
        assert!(retained(controls, target_control, member, selected).is_none());
    }
    if matches!(
        case,
        SourceCase::NativeBridgeStepPublish
            | SourceCase::NativeBridgeStepPublishFaults
            | SourceCase::NativeBridgeStepOpen
    ) {
        let clients = new
            .iter()
            .map(|member| (member.clone(), all_clients[initiator(member)].clone()))
            .collect::<BTreeMap<_, _>>();
        exercise_publish(
            case,
            &data,
            &new,
            &clients,
            all_clients,
            controls,
            target_control,
            &committed_leader,
            &bystander,
            selected,
            &mut ledger,
        );
        return;
    }
    if matches!(case, SourceCase::NativeBridgeStepRefusals) {
        let foreign = |operation: StepOperation| {
            NativeJointHandoffStepRequestV1::from_decision(&data, operation, Uuid::now_v7(), 5000)
                .unwrap()
        };
        // 1. A mutation from a NEW member that is not the coordinator.
        for victim in new.iter().filter(|n| **n != bystander) {
            let error = raw(
                &all_clients[index(&bystander)],
                victim,
                &foreign(prepare_operation(&data)),
                "bridge_prepare_from_non_initiator_new_member",
                &mut ledger,
            );
            assert!(error.contains("not an admitted initiator"), "{error}");
            assert!(retained(controls, target_control, victim, selected).is_none());
        }
        // 2. A read-only observation from a peer outside the NEW member set.
        let outsider_is_coordinator = outsider == node(0);
        if !outsider_is_coordinator {
            for victim in &new {
                let error = raw(
                    &all_clients[index(&outsider)],
                    victim,
                    &foreign(observe_operation(&data, PhaseKind::Preparation)),
                    "bridge_phase_observe_from_retired_original_member",
                    &mut ledger,
                );
                assert!(error.contains("not an admitted initiator"), "{error}");
            }
        }
        // 3. The coordinator naming another decision, then another epoch.
        for victim in new.iter().filter(|n| **n != node(0)) {
            let mut other = foreign(prepare_operation(&data));
            let digest = if data.checksum_sha256().starts_with('0') {
                "1".repeat(64)
            } else {
                "0".repeat(64)
            };
            other.operation = StepOperation::BridgePrepare {
                decision_sha256: digest,
            };
            let error = raw(
                &all_clients[0],
                victim,
                &other,
                "bridge_prepare_names_another_decision",
                &mut ledger,
            );
            assert!(
                error.contains("differs from the decision committed in own metadata"),
                "{error}"
            );
            let mut other = foreign(prepare_operation(&data));
            other.range_epoch += 1;
            let error = raw(
                &all_clients[0],
                victim,
                &other,
                "bridge_prepare_names_another_epoch",
                &mut ledger,
            );
            assert!(
                error.contains("differs from the decision committed in own metadata"),
                "{error}"
            );
            // 4. Nothing retained: a phase observation has nothing to attest.
            let error = raw(
                &all_clients[0],
                victim,
                &foreign(observe_operation(&data, PhaseKind::Preparation)),
                "bridge_phase_observe_before_any_preparation",
                &mut ledger,
            );
            assert!(error.contains("retains no preparation"), "{error}");
            assert!(retained(controls, target_control, victim, selected).is_none());
        }
        // The strict client itself refuses to bind a request to a decision
        // the caller did not observe.
        let mut other = foreign(prepare_operation(&data));
        other.range_epoch += 1;
        assert!(other.require_decision(&data).is_err());
        ledger.record(serde_json::json!({"operation":"refusal_summary",
            "outsider_observation_refusal_exercised":!outsider_is_coordinator,
            "no_member_retained_a_preparation":true}));
        assert!(
            !outsider_is_coordinator,
            "fixture's retired member is the metadata leader; outsider refusal not exercised"
        );
        return;
    }
    // (a) Every NEW member prepares ITSELF and retains its own custody.
    let mut prepared = BTreeMap::<ClusterNodeId, NativeJointHandoffStepOutcomeV1>::new();
    let mut contexts = BTreeSet::new();
    for member in &new {
        let client = &all_clients[initiator(member)];
        let frame = prepare(client, member, &data, &mut ledger);
        let outcome = frame.outcome().clone();
        assert_eq!(
            outcome.decision_sha256.as_deref(),
            Some(data.checksum_sha256())
        );
        assert!(outcome.phase_kind.is_none() && outcome.window_remaining_ms.is_none());
        let custody = retained(controls, target_control, member, selected)
            .expect("the member itself retained its preparation");
        assert_eq!(custody["node"], serde_json::json!(member));
        assert_eq!(
            custody["native_preparation_sha256"],
            serde_json::json!(outcome.preparation_sha256)
        );
        assert_eq!(
            custody["actual_preparation_incarnation"],
            serde_json::json!(outcome.native_database_incarnation)
        );
        assert!(custody["original_session_error"].is_null());
        assert!(contexts.insert(frame.metadata_anchor().2));
        // An identical retry returns the SAME retained preparation.
        let again = prepare(client, member, &data, &mut ledger);
        assert_eq!(again.outcome(), &outcome);
        assert!(contexts.insert(again.metadata_anchor().2));
        let after = retained(controls, target_control, member, selected).unwrap();
        assert_eq!(
            after["native_preparation_sha256"],
            custody["native_preparation_sha256"]
        );
        assert_eq!(
            after["original_metadata_context"],
            custody["original_metadata_context"]
        );
        ledger.record(
            serde_json::json!({"operation":"member_retained_preparation_custody",
            "member":member,"custody":custody,"identical_retry":true}),
        );
        prepared.insert(member.clone(), outcome);
    }
    // (b) One NEW member collects read-only PREPARATION observations from
    // every other NEW member over its own transport inside one window. A
    // second observer covers the first one as recipient.
    let second = new.iter().find(|n| **n != node(4)).unwrap().clone();
    for observer in [node(4), second] {
        let client = &all_clients[index(&observer)];
        let window = Instant::now();
        let mut frames = Vec::new();
        for member in new.iter().filter(|n| **n != observer) {
            let frame = exchange(
                client,
                member,
                &data,
                observe_operation(&data, PhaseKind::Preparation),
                &mut ledger,
            )
            .unwrap();
            let outcome = frame.outcome();
            let original = &prepared[member];
            assert_eq!(outcome.phase_kind, Some(PhaseKind::Preparation));
            assert_eq!(outcome.decision_sha256, original.decision_sha256);
            assert_eq!(outcome.preparation_sha256, original.preparation_sha256);
            assert_eq!(
                outcome.native_database_incarnation,
                original.native_database_incarnation
            );
            assert_eq!(
                outcome.local_visibility_sequence,
                original.local_visibility_sequence
            );
            assert_eq!(
                outcome.decision_receipt_sha256,
                original.decision_receipt_sha256
            );
            assert_eq!(outcome.store_open_id, original.store_open_id);
            // The exact writer registration the member proved under its lock.
            assert!(outcome.writer_identity_sha256.is_some());
            assert_eq!(
                outcome.writer_identity_sha256,
                original.writer_identity_sha256
            );
            // The member's own DISTINCT fresh metadata observation.
            assert!(contexts.insert(frame.metadata_anchor().2));
            assert!(frame.window_deadline() > Instant::now());
            assert!(frame.window_deadline() <= window + Duration::from_secs(5) * 2);
            frames.push(frame);
        }
        assert_eq!(frames.len(), new.len() - 1);
        // Every frame is still inside its own window when the last arrives,
        // and the whole collection fitted one four-second phase.
        for frame in &frames {
            frame.require_live().unwrap();
        }
        assert!(window.elapsed() <= Duration::from_secs(4));
        let terms = frames
            .iter()
            .map(|frame| frame.metadata_anchor().0)
            .collect::<BTreeSet<_>>();
        assert_eq!(terms.len(), 1, "one metadata term across the window");
        ledger.record(
            serde_json::json!({"operation":"preparation_phase_observations_agree",
            "observer":observer,"frames":frames.len(),
            "collection_elapsed_ms":window.elapsed().as_millis() as u64}),
        );
    }
    // The later phase kinds have no retained member state in this slice.
    let error = exchange(
        &all_clients[0],
        new.iter().find(|n| **n != node(0)).unwrap(),
        &data,
        observe_operation(&data, PhaseKind::Open),
        &mut ledger,
    )
    .err()
    .expect("no member can attest an Open phase before it publishes")
    .to_string();
    assert!(error.contains("no retained member state yet"), "{error}");
}

/// After every NEW member prepared itself, each one publishes its OWN bridge
/// through one step RPC from the coordinator. The driver never touches a
/// member's service, quorum or phase object.
#[allow(clippy::too_many_arguments)]
fn exercise_publish(
    case: SourceCase,
    data: &NativeJointDecisionData,
    new: &[ClusterNodeId],
    clients: &BTreeMap<ClusterNodeId, Arc<TcpClusterRelocationTransport>>,
    all_clients: &[Arc<TcpClusterRelocationTransport>],
    controls: &[NativeMetadataControlHost],
    target_control: &NativeMetadataControlHost,
    committed_leader: &ClusterNodeId,
    bystander: &ClusterNodeId,
    selected: crate::RangeId,
    ledger: &mut Ledger<'_>,
) {
    let evidence_of =
        |member: &ClusterNodeId| publication(controls, target_control, member, selected);
    let foreign = |operation: StepOperation| {
        NativeJointHandoffStepRequestV1::from_decision(data, operation, Uuid::now_v7(), 5000)
            .unwrap()
    };
    if matches!(case, SourceCase::NativeBridgeStepOpen) {
        return exercise_open(
            data,
            new,
            clients,
            all_clients,
            controls,
            target_control,
            committed_leader,
            bystander,
            selected,
            ledger,
        );
    }
    let mut prepared = BTreeMap::<ClusterNodeId, NativeJointHandoffStepOutcomeV1>::new();
    if matches!(case, SourceCase::NativeBridgeStepPublishFaults) {
        let victim = bystander.clone();
        let late = new
            .iter()
            .rev()
            .find(|n| **n != victim && **n != node(0) && *n != committed_leader)
            .cloned()
            .expect("a second NEW member that is neither leader");
        // 1. Nothing prepared: a publish has no retained preparation.
        let error = raw(
            &clients[&victim],
            &victim,
            &foreign(publish_operation(data)),
            "bridge_publish_before_any_preparation",
            ledger,
        );
        assert!(error.contains("retains no preparation"), "{error}");
        assert!(evidence_of(&victim).is_none());
        for member in new.iter().filter(|n| **n != late) {
            let frame = prepare(&clients[member], member, data, ledger);
            prepared.insert(member.clone(), frame.outcome().clone());
        }
        // 2. One NEW member is not prepared: the publisher's own first
        // window cannot be completed, so nothing is written and its own
        // read pin stays held.
        let error = exchange(
            &clients[&victim],
            &victim,
            data,
            publish_operation(data),
            ledger,
        )
        .err()
        .expect("publish before every NEW member is prepared must fail")
        .to_string();
        eprintln!("NATIVE_BRIDGE_PUBLISH_BEFORE_ALL_PREPARED exact_error={error}");
        let after = evidence_of(&victim).unwrap();
        assert!(after["durable_bridge"].is_null(), "{after}");
        assert_eq!(after["read_pin_released"], false);
        assert_eq!(after["bridge_confirmed"], false);
        assert!(after["retained_bridge_sha256"].is_null());
        // 3. A publish from a NEW member that is not the coordinator.
        let other = new
            .iter()
            .find(|n| **n != victim && **n != late)
            .cloned()
            .unwrap();
        let error = raw(
            &all_clients[index(&victim)],
            &other,
            &foreign(publish_operation(data)),
            "bridge_publish_from_non_initiator_new_member",
            ledger,
        );
        assert!(error.contains("not an admitted initiator"), "{error}");
        assert!(evidence_of(&other).unwrap()["durable_bridge"].is_null());
        assert_eq!(evidence_of(&other).unwrap()["read_pin_released"], false);
        let frame = prepare(&clients[&late], &late, data, ledger);
        prepared.insert(late.clone(), frame.outcome().clone());
        // 4/5. The durable write happens, then the second window does not
        // confirm an unchanged member set: fail closed, keep the marker,
        // report nothing; the next call recovers the SAME bridge.
        for (member, fault) in [
            (
                victim.clone(),
                NativeBridgeSecondWindowFault::ChangedIdentity,
            ),
            (late.clone(), NativeBridgeSecondWindowFault::LostReply),
        ] {
            test_arm_bridge_second_window_fault(&member, data.checksum_sha256(), fault);
            let frame = exchange(
                &clients[&member],
                &member,
                data,
                publish_operation(data),
                ledger,
            )
            .unwrap();
            let outcome = frame.outcome();
            assert!(!outcome.complete, "{fault:?} must not confirm");
            assert_eq!(outcome.stage, StepStage::BridgePublication);
            assert!(outcome.bridge_receipt_sha256.is_none());
            let unconfirmed = evidence_of(&member).unwrap();
            assert_eq!(unconfirmed["publication_pending"], true);
            assert_eq!(unconfirmed["bridge_confirmed"], false);
            assert_eq!(unconfirmed["read_pin_released"], true);
            let checksum = assert_durable_bridge(&unconfirmed, &member, data, &prepared);
            assert_eq!(unconfirmed["retained_bridge_sha256"], checksum);
            // Recovery under a distinct fresh phase: exact original audit.
            let frame = publish(&clients[&member], &member, data, ledger);
            assert_eq!(
                frame.outcome().bridge_receipt_sha256.as_deref(),
                Some(checksum.as_str())
            );
            let recovered = evidence_of(&member).unwrap();
            assert_eq!(recovered["durable_bridge"], unconfirmed["durable_bridge"]);
            assert_eq!(recovered["publication_pending"], false);
            assert_eq!(recovered["bridge_confirmed"], true);
            ledger.record(
                serde_json::json!({"operation":"unconfirmed_second_window_recovered",
                "member":member,"fault":format!("{fault:?}"),"bridge_sha256":checksum,
                "durable_audit_unchanged":true}),
            );
        }
        // The remaining members publish normally.
        for member in new {
            let frame = publish(&clients[member], member, data, ledger);
            let after = evidence_of(member).unwrap();
            let checksum = assert_durable_bridge(&after, member, data, &prepared);
            assert_eq!(
                frame.outcome().bridge_receipt_sha256.as_deref(),
                Some(checksum.as_str())
            );
        }
        return;
    }
    for member in new {
        let frame = prepare(&clients[member], member, data, ledger);
        assert!(frame.outcome().writer_identity_sha256.is_some());
        prepared.insert(member.clone(), frame.outcome().clone());
    }
    let identities = prepared
        .values()
        .map(|outcome| outcome.writer_identity_sha256.clone().unwrap())
        .collect::<BTreeSet<_>>();
    assert_eq!(identities.len(), new.len(), "one registration per member");
    for member in new {
        let before = evidence_of(member).unwrap();
        assert!(before["durable_bridge"].is_null());
        assert_eq!(before["read_pin_released"], false);
    }
    let mut receipts = BTreeMap::new();
    for (position, member) in new.iter().enumerate() {
        let started = Instant::now();
        let frame = publish(&clients[member], member, data, ledger);
        let outcome = frame.outcome().clone();
        let original = &prepared[member];
        assert_eq!(outcome.preparation_sha256, original.preparation_sha256);
        assert_eq!(outcome.store_open_id, original.store_open_id);
        assert_eq!(
            outcome.decision_receipt_sha256,
            original.decision_receipt_sha256
        );
        assert_eq!(
            outcome.writer_identity_sha256,
            original.writer_identity_sha256
        );
        let after = evidence_of(member).unwrap();
        assert_eq!(after["publication_pending"], false);
        assert_eq!(after["bridge_confirmed"], true);
        assert_eq!(after["read_pin_released"], true);
        let checksum = assert_durable_bridge(&after, member, data, &prepared);
        assert_eq!(
            outcome.bridge_receipt_sha256.as_deref(),
            Some(checksum.as_str())
        );
        assert_eq!(after["retained_bridge_sha256"], checksum);
        // Only this member's own pin went with its own publication.
        for later in &new[position + 1..] {
            let untouched = evidence_of(later).unwrap();
            assert!(untouched["durable_bridge"].is_null());
            assert_eq!(untouched["read_pin_released"], false);
        }
        // An identical retry returns the SAME outcome and rewrites nothing.
        let again = publish(&clients[member], member, data, ledger);
        assert_eq!(again.outcome(), &outcome);
        assert_eq!(
            evidence_of(member).unwrap()["durable_bridge"],
            after["durable_bridge"]
        );
        ledger.record(
            serde_json::json!({"operation":"member_published_own_bridge",
            "member":member,"bridge_sha256":checksum,
            "elapsed_ms":started.elapsed().as_millis() as u64,
            "durable_bridge":after["durable_bridge"],"identical_retry":true}),
        );
        receipts.insert(member.clone(), checksum);
    }
    // Coordinator-side aggregation from authenticated frames only.
    let coordinator = node(0);
    let begin = || {
        NativeRemoteBridgePublicationCertificateRound::begin(
            data.clone(),
            coordinator.clone(),
            &budget(5000),
        )
        .unwrap()
    };
    let frame_of = |member: &ClusterNodeId, ledger: &mut Ledger<'_>| {
        publish(&clients[member], member, data, ledger)
    };
    // (i) Omitting one NEW member.
    let mut round = begin();
    for member in &new[1..] {
        round.record(frame_of(member, ledger)).unwrap();
    }
    let error = round.finish().err().unwrap().to_string();
    assert!(error.contains("cannot omit or replace"), "{error}");
    // (ii) Counting one member twice, (iii) a frame of another operation,
    // (iv) a frame another initiator observed.
    let plain = new
        .iter()
        .find(|n| **n != coordinator && *n != committed_leader)
        .unwrap();
    let mut round = begin();
    round.record(frame_of(plain, ledger)).unwrap();
    let error = round
        .record(frame_of(plain, ledger))
        .err()
        .unwrap()
        .to_string();
    assert!(error.contains("already counted"), "{error}");
    let mut round = begin();
    let error = round
        .record(prepare(&clients[plain], plain, data, ledger))
        .err()
        .unwrap()
        .to_string();
    assert!(error.contains("not a confirmed publication"), "{error}");
    let foreign_frame = publish(&all_clients[index(committed_leader)], plain, data, ledger);
    let error = round.record(foreign_frame).err().unwrap().to_string();
    assert!(error.contains("observed by another initiator"), "{error}");
    // (v) Every NEW member exactly once.
    let mut round = begin();
    for member in new {
        round.record(frame_of(member, ledger)).unwrap();
    }
    let certificate = round.finish().unwrap();
    certificate.require_current().unwrap();
    assert_eq!(certificate.data(), data);
    assert_eq!(certificate.receipts().unwrap(), receipts);
    let checksum = certificate.checksum_sha256().unwrap();
    assert_eq!(checksum, certificate.checksum_sha256().unwrap());
    ledger.record(
        serde_json::json!({"operation":"bridge_publication_certificate",
        "coordinator":coordinator,"receipts":receipts,"certificate_sha256":checksum,
        "built_only_from_authenticated_frames":true}),
    );
    // Every member now attests its own durable bridge in an OPEN observation;
    // the release kind still has no retained member state.
    for member in new {
        let frame = exchange(
            &clients[member],
            member,
            data,
            observe_operation(data, PhaseKind::Open),
            ledger,
        )
        .unwrap();
        assert_eq!(
            frame.outcome().bridge_receipt_sha256.as_ref(),
            Some(&receipts[member])
        );
        assert!(frame.outcome().bridge_anchor.is_some());
        assert_eq!(
            frame.outcome().writer_identity_sha256,
            prepared[member].writer_identity_sha256
        );
    }
    let error = exchange(
        &all_clients[0],
        new.iter().find(|n| **n != node(0)).unwrap(),
        data,
        observe_operation(data, PhaseKind::Release),
        ledger,
    )
    .err()
    .expect("no member can attest a Release phase before the release step exists")
    .to_string();
    assert!(error.contains("no retained member state yet"), "{error}");
}

/// After prepare and publish, each member persists its OWN catalogue
/// activation through one step RPC. Covers the refusals, one unconfirmed
/// second window and its recovery, and the durable state of every member.
#[allow(clippy::too_many_arguments)]
fn exercise_open(
    data: &NativeJointDecisionData,
    new: &[ClusterNodeId],
    clients: &BTreeMap<ClusterNodeId, Arc<TcpClusterRelocationTransport>>,
    all_clients: &[Arc<TcpClusterRelocationTransport>],
    controls: &[NativeMetadataControlHost],
    target_control: &NativeMetadataControlHost,
    committed_leader: &ClusterNodeId,
    bystander: &ClusterNodeId,
    selected: crate::RangeId,
    ledger: &mut Ledger<'_>,
) {
    let evidence_of =
        |member: &ClusterNodeId| publication(controls, target_control, member, selected);
    let foreign = |operation: StepOperation| {
        NativeJointHandoffStepRequestV1::from_decision(data, operation, Uuid::now_v7(), 5000)
            .unwrap()
    };
    let mut prepared = BTreeMap::<ClusterNodeId, NativeJointHandoffStepOutcomeV1>::new();
    for member in new {
        let frame = prepare(&clients[member], member, data, ledger);
        prepared.insert(member.clone(), frame.outcome().clone());
    }
    let victim = bystander.clone();
    let late = new
        .iter()
        .rev()
        .find(|n| **n != victim && **n != node(0) && *n != committed_leader)
        .cloned()
        .expect("a second NEW member that is neither leader");
    // 1. Prepared but not published: the member holds no confirmed bridge.
    let error = raw(
        &clients[&victim],
        &victim,
        &foreign(open_operation(data)),
        "bridge_open_before_own_publication",
        ledger,
    );
    assert!(
        error.contains("holds no confirmed bridge publication"),
        "{error}"
    );
    let mut bridges = BTreeMap::new();
    for member in new.iter().filter(|n| **n != late) {
        let frame = publish(&clients[member], member, data, ledger);
        bridges.insert(
            member.clone(),
            frame.outcome().bridge_receipt_sha256.clone().unwrap(),
        );
    }
    // 2. One NEW member has not published: the opener's own first window
    // cannot be completed, so no catalogue activation is written.
    let error = exchange(
        &clients[&victim],
        &victim,
        data,
        open_operation(data),
        ledger,
    )
    .err()
    .expect("open before every NEW member published must fail")
    .to_string();
    eprintln!("NATIVE_BRIDGE_OPEN_BEFORE_ALL_PUBLISHED exact_error={error}");
    let after = evidence_of(&victim).unwrap();
    assert!(after["durable_activation_sha256"].is_null(), "{after}");
    assert_eq!(after["activation_confirmed"], false);
    assert!(after["retained_activation_sha256"].is_null());
    // 3. An open from a NEW member that is not the coordinator.
    let other = new
        .iter()
        .find(|n| **n != victim && **n != late)
        .cloned()
        .unwrap();
    let error = raw(
        &all_clients[index(&victim)],
        &other,
        &foreign(open_operation(data)),
        "bridge_open_from_non_initiator_new_member",
        ledger,
    );
    assert!(error.contains("not an admitted initiator"), "{error}");
    assert!(evidence_of(&other).unwrap()["durable_activation_sha256"].is_null());
    let frame = publish(&clients[&late], &late, data, ledger);
    bridges.insert(
        late.clone(),
        frame.outcome().bridge_receipt_sha256.clone().unwrap(),
    );
    // 4. The durable activation happens, then the second window does not
    // confirm an unchanged member set: fail closed, report nothing, and the
    // next call recovers the SAME activation.
    test_arm_bridge_second_window_fault(
        &victim,
        data.checksum_sha256(),
        NativeBridgeSecondWindowFault::ChangedIdentity,
    );
    let frame = exchange(
        &clients[&victim],
        &victim,
        data,
        open_operation(data),
        ledger,
    )
    .unwrap();
    assert!(!frame.outcome().complete);
    assert_eq!(frame.outcome().stage, StepStage::BridgeActivation);
    assert!(frame.outcome().activation_sha256.is_none());
    let unconfirmed = evidence_of(&victim).unwrap();
    assert_eq!(unconfirmed["activation_pending"], true);
    assert_eq!(unconfirmed["activation_confirmed"], false);
    assert!(unconfirmed["durable_activation_sha256"].is_string());
    assert_eq!(
        unconfirmed["retained_activation_sha256"],
        unconfirmed["durable_activation_sha256"]
    );
    let mut activations = BTreeMap::new();
    for member in new {
        let started = Instant::now();
        let frame = open(&clients[member], member, data, ledger);
        let outcome = frame.outcome().clone();
        assert_eq!(
            outcome.bridge_receipt_sha256.as_ref(),
            Some(&bridges[member])
        );
        assert_eq!(
            outcome.preparation_sha256,
            prepared[member].preparation_sha256
        );
        assert_eq!(
            outcome.writer_identity_sha256,
            prepared[member].writer_identity_sha256
        );
        let after = evidence_of(member).unwrap();
        assert_eq!(after["activation_pending"], false);
        assert_eq!(after["activation_confirmed"], true);
        // The actual protected catalogue fact, read from the member's store.
        assert_eq!(
            after["durable_activation_sha256"],
            serde_json::json!(outcome.activation_sha256)
        );
        assert_eq!(
            after["retained_activation_sha256"],
            after["durable_activation_sha256"]
        );
        assert_eq!(
            after["retained_activation_bridge_sha256"],
            serde_json::json!(bridges[member])
        );
        // The bridge under the activation is the one published before.
        assert_eq!(
            assert_durable_bridge(&after, member, data, &prepared),
            bridges[member]
        );
        // Only the original Registered receiver retains a release owner.
        assert_eq!(after["release_owner_retained"], *member == node(4));
        if *member == victim {
            assert_eq!(
                after["durable_activation_sha256"],
                unconfirmed["durable_activation_sha256"]
            );
        }
        // An identical retry returns the SAME outcome and rewrites nothing.
        let again = open(&clients[member], member, data, ledger);
        assert_eq!(again.outcome(), &outcome);
        assert_eq!(
            evidence_of(member).unwrap()["durable_activation_sha256"],
            after["durable_activation_sha256"]
        );
        ledger.record(
            serde_json::json!({"operation":"member_activated_own_catalogue",
            "member":member,"activation_sha256":outcome.activation_sha256,
            "bridge_sha256":bridges[member],
            "elapsed_ms":started.elapsed().as_millis() as u64,
            "release_owner_retained":after["release_owner_retained"],"identical_retry":true}),
        );
        activations.insert(member.clone(), outcome.activation_sha256.clone().unwrap());
    }
    assert_eq!(activations.len(), new.len());
    // Every activated member still attests its own durable bridge.
    for member in new {
        let frame = exchange(
            &clients[member],
            member,
            data,
            observe_operation(data, PhaseKind::Open),
            ledger,
        )
        .unwrap();
        assert_eq!(
            frame.outcome().bridge_receipt_sha256.as_ref(),
            Some(&bridges[member])
        );
    }
}
