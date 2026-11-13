//! Test-only opt-in publication observations before the original assertions.
//! These DATA never substitute for original Role/OWN/source liveness proofs.
use super::*;

#[allow(clippy::too_many_arguments)]
pub(super) fn advance_publication(
    control: &mut NativeMetadataControlHost,
    client: &TcpClusterRelocationTransport,
    source: &Arc<NativeRemoteImageFact>,
    plan: &NativeJointHandoffPlan,
    provider: &MetadataReadProvider,
    actor: &crate::distribution_consensus::MetadataReadParticipant,
    factory: &Arc<crate::distribution_native_host::NativeUnassignedLearnerFactory>,
    pending: &ClusterTopology,
    ranges: &[crate::RangeDescriptor],
    acknowledged: &serde_json::Value,
    output: &std::path::Path,
    journal: &mut Vec<serde_json::Value>,
    operation: Operation,
    cancel: bool,
) -> Result<Progress> {
    let operation_name = match &operation {
        Operation::PublishFinalImage => "PublishFinalImage",
        Operation::PublishNativeBase => "PublishNativeBase",
        _ => panic!("critical publication diagnostics only"),
    };
    let range = plan.old_range().id;
    let attempt = source.manifest().target_attempt;
    // Outside the newly created request's unchanged five-second budget.
    let before = catalogue_tests::prerequisite_snapshot(
        control,
        factory,
        pending,
        ranges,
        range,
        attempt,
        acknowledged,
    );
    let mut windows = Vec::new();
    let started = Instant::now();
    let result = advance_with_diagnostics(
        control,
        client,
        source,
        plan,
        provider,
        actor,
        output,
        journal,
        operation,
        cancel,
        Some(&mut windows),
        None,
    );
    let elapsed_us = started.elapsed().as_micros();
    let after = catalogue_tests::prerequisite_snapshot(
        control,
        factory,
        pending,
        ranges,
        range,
        attempt,
        acknowledged,
    );
    let path = output.join("registered-native-image-critical-outcome.json");
    // One test owns this output directory. Preserve earlier publication/cancel
    // observations rather than replacing a failed call with a later success.
    let mut attempts: Vec<serde_json::Value> = if path.exists() {
        serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap()
    } else {
        Vec::new()
    };
    assert!(attempts.len() < 16, "bounded critical diagnostic history");
    attempts.push(serde_json::json!({
        "version":1,"operation":operation_name,"call_index":attempts.len(),
        "before":before,"after":after,"request_windows":windows,
        "request_elapsed_us":elapsed_us,
        "exact_primary_error":result.as_ref().err().map(|e|e.to_string()),
        "primary_error_debug":result.as_ref().err().map(|e|format!("{e:?}")),
        "progress":result.as_ref().ok(),"expected_cancel_control":cancel,
        "actual_manifest_sha256":source.manifest().checksum_sha256,
        "actual_target_attempt":attempt,"source_session":source.manifest().source_session,
        "request_order_unchanged":["Role","OWN","Finished","actual_host"],
        "parent_budget_ms_unchanged":5000,"retry_or_deadline_change":false,
        "local_attempt_observations_not_source_frames":true,
        "native_read_diagnostics_not_durability_or_cold_proof":true
    }));
    // Retain the actual outcome before callers perform their original unwrap
    // or acceptance assertions. This does not retry the operation.
    evidence(&path, &serde_json::to_value(attempts).unwrap()).unwrap();
    result
}
