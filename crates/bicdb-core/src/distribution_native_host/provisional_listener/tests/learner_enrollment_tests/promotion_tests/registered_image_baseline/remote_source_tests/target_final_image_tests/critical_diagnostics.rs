//! Test-only opt-in publication observations before the original assertions.
//! These DATA never substitute for original Role/OWN/source liveness proofs.
use super::*;

const MAX_CRITICAL_REPORT_BYTES: u64 = 32 * 1024 * 1024;
const MAX_CRITICAL_REPORTS: usize = 16;

fn prior_reports(path: &std::path::Path) -> std::result::Result<Vec<serde_json::Value>, String> {
    use std::io::Read;
    let file = match std::fs::File::open(path) {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(error) => return Err(format!("diagnostic history open: {error}")),
    };
    if file
        .metadata()
        .map_err(|e| format!("diagnostic history metadata: {e}"))?
        .len()
        > MAX_CRITICAL_REPORT_BYTES
    {
        return Err("diagnostic history byte limit exceeded".into());
    }
    // A concurrent growth cannot bypass the metadata length check or cause
    // an unbounded read allocation. The fixture never retries this read.
    let mut bytes = Vec::new();
    file.take(MAX_CRITICAL_REPORT_BYTES + 1)
        .read_to_end(&mut bytes)
        .map_err(|e| format!("diagnostic history read: {e}"))?;
    if bytes.len() as u64 > MAX_CRITICAL_REPORT_BYTES {
        return Err("diagnostic history grew beyond byte limit".into());
    }
    struct BoundedReports;
    impl<'de> serde::de::Visitor<'de> for BoundedReports {
        type Value = Vec<serde_json::Value>;
        fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("a bounded critical diagnostic history")
        }
        fn visit_seq<A: serde::de::SeqAccess<'de>>(
            self,
            mut seq: A,
        ) -> std::result::Result<Self::Value, A::Error> {
            let mut reports = Vec::new();
            while reports.len() < MAX_CRITICAL_REPORTS - 1 {
                let Some(value) = seq.next_element::<serde_json::Value>()? else {
                    return Ok(reports);
                };
                reports.push(value);
            }
            // Reserve one slot for this actual outcome. Never deserialize an
            // oversized history into a growing Vec before checking its cap.
            if seq.next_element::<serde::de::IgnoredAny>()?.is_some() {
                return Err(serde::de::Error::custom(
                    "critical diagnostic history entry limit exceeded",
                ));
            }
            Ok(reports)
        }
    }
    let mut decoder = serde_json::Deserializer::from_slice(&bytes);
    let reports = serde::de::Deserializer::deserialize_seq(&mut decoder, BoundedReports)
        .map_err(|e| format!("diagnostic history parse/bound: {e}"))?;
    decoder
        .end()
        .map_err(|e| format!("diagnostic history trailing data: {e}"))?;
    Ok(reports)
}

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
    let diagnostic = (|| -> std::result::Result<(), String> {
        let mut attempts = prior_reports(&path)?;
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
        evidence(&path, &attempts).map_err(|error| format!("diagnostic history persist: {error}"))
    })();
    if let Err(error) = diagnostic {
        eprintln!("NATIVE_CRITICAL_EVIDENCE_FAILURE attempt={attempt} operation={operation_name} primary_error={:?} diagnostic={error}", result.as_ref().err());
        // Reporting failure must never replace an already failed API result.
        // Conversely an otherwise successful API cannot hide missing evidence.
        if result.is_ok() {
            return Err(BicDbError::Cluster(format!(
                "critical publication evidence failed for attempt {attempt}: {error}"
            )));
        }
    }
    result
}
