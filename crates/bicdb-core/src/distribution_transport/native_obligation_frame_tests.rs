//! Structural serialization bounds only. The DTOs here are never admitted as
//! actual observations, authenticated votes, image proof or row-read authority.
use super::*;
use crate::distribution_range_consensus::{
    native_origin_fixture, native_ready_test_command, validate_native_recovery_entry_size,
    RangeWriteLogEntry, RangeWriteState, MAX_NATIVE_RECOVERY_ENTRY_BYTES,
    MIN_NATIVE_RECOVERY_FRAME_BYTES,
};
use crate::distribution_range_recovery::{
    test_only_structural_fence_dtos, RecoveryEpochFenceReply, RecoveryFenceObservationPhase,
    RecoveryFencedProbeReply, MAX_FENCED_IRREVERSIBLE_COMMANDS,
};
use crate::{CommitAdmissionIntent, CommitAdmissionMutation, Record};

#[test]
fn complete_fenced_obligation_envelope_and_maximum_entry_fit_native_frame_floor() {
    let (_root, _db, topology, _spec) = native_origin_fixture();
    let range = topology.range_for_key("items", "boundary").unwrap();
    let make_entry = |bytes| {
        let intent = CommitAdmissionIntent {
            transaction_id: u64::MAX,
            mutations: vec![CommitAdmissionMutation {
                collection: "items".into(),
                record_id: "boundary".into(),
                record: Some(Record::new("boundary").with_metadata(
                    serde_json::json!({"v": "x".repeat(bytes)}),
                )),
            }],
        };
        RangeWriteLogEntry {
            command: native_ready_test_command(
                topology.cluster_id.clone(), range, u64::MAX, range.leader.clone(), &intent,
            ).unwrap(),
            state: RangeWriteState::QuorumCommitted,
        }
    };
    let overhead = serde_json::to_vec(&make_entry(0)).unwrap().len();
    let entry = make_entry(MAX_NATIVE_RECOVERY_ENTRY_BYTES - overhead);
    assert_eq!(serde_json::to_vec(&entry).unwrap().len(), MAX_NATIVE_RECOVERY_ENTRY_BYTES);
    validate_native_recovery_entry_size(&entry).unwrap();
    let (scope, progress, history) = test_only_structural_fence_dtos(MAX_FENCED_IRREVERSIBLE_COMMANDS);
    history.require_shape(&scope, &progress).unwrap();
    let expected = history.irreversible_command_sha256.clone();
    let native = crate::db::NativeRangePrefixObservation {
        range_id: scope.range_id,
        native_base_index: Some(0),
        native_applied_index: Some(u64::MAX - 1),
        pending_command_index: Some(u64::MAX),
        materialized_pending_index: None,
        pending_command_id: Some(entry.command.command_id.clone()),
        native_receipt_sha256: Some("e".repeat(64)),
        factory_initialization_complete: true,
        invalidated: false,
        diagnostic: None,
    };
    let fence = RecoveryEpochFenceReply {
        phase: RecoveryFenceObservationPhase::ActiveRecovery,
        image_history: history,
        node_id: progress.node_id.clone(),
        member_incarnation: 1,
        store_open_id: Uuid::new_v4(),
        native_database_incarnation: "structural-wire-bound-only".into(),
        birth_initialization_complete: true,
        progress,
        native,
        scope,
    };
    let response = ClusterDataResponseEnvelope {
        protocol_version: CLUSTER_DATA_PROTOCOL_VERSION,
        cluster_id: topology.cluster_id.clone(),
        source_node_id: range.leader.clone(),
        request_id: u64::MAX,
        response: Ok(ClusterDataResponse::RecoveryFencedProbe(RecoveryFencedProbeReply {
            fence,
            index: Some(u64::MAX),
            entry: Some(entry),
            current_certification: None,
        })),
    };
    let bytes = serde_json::to_vec(&response).unwrap().len();
    assert!(bytes > MAX_NATIVE_RECOVERY_ENTRY_BYTES);
    assert!(bytes < MIN_NATIVE_RECOVERY_FRAME_BYTES, "full envelope: {bytes}");
    let mut wire = Vec::new();
    send_json_frame(&mut wire, &response, MIN_NATIVE_RECOVERY_FRAME_BYTES).unwrap();
    assert_eq!(wire.len(), bytes + 4);
    let decoded: ClusterDataResponseEnvelope = recv_json_frame(
        &mut std::io::Cursor::new(wire), MIN_NATIVE_RECOVERY_FRAME_BYTES,
    ).unwrap();
    let Ok(ClusterDataResponse::RecoveryFencedProbe(reply)) = decoded.response else {
        panic!("wrong structural response after frame roundtrip");
    };
    assert_eq!(reply.fence.image_history.irreversible_command_sha256, expected);
    assert_eq!(serde_json::to_vec(reply.entry.as_ref().unwrap()).unwrap().len(), MAX_NATIVE_RECOVERY_ENTRY_BYTES);
    println!("structural_full_native_obligation_envelope_bytes={bytes} frame_floor={MIN_NATIVE_RECOVERY_FRAME_BYTES} complete_checksums={}", expected.len());
}
