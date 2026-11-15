//! After the decision is durable and the membership is final, every NEW
//! member is driven ONLY through bounded bridge step RPCs. The driver hands
//! no member a lease, role, own observation, quorum or phase object and never
//! calls a member's in-process preparation; host handles are used for
//! memory-only observation of what each member itself retained.
use super::*;
use crate::distribution_range_handoff::{
    NativeJointBridgePhaseKindV1 as PhaseKind, NativeJointDecisionData,
    NativeJointHandoffStepOperationV1 as StepOperation, NativeJointHandoffStepOutcomeV1,
    NativeJointHandoffStepRequestV1,
};
use crate::distribution_transport::AuthenticatedNativeJointBridgeStepFrame;

pub(super) fn is_case(case: SourceCase) -> bool {
    matches!(
        case,
        SourceCase::NativeBridgeStepPrepareObserve | SourceCase::NativeBridgeStepRefusals
    )
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
