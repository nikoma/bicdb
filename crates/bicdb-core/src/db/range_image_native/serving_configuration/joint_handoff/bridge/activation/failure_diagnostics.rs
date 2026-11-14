//! Bounded, thread-local failure DATA only. No owner, guard, error, raw record
//! bytes or authority survives the synchronous failed publication call.
use super::*;
use sha2::{Digest, Sha256};
use std::cell::RefCell;

thread_local! {
    static LAST: RefCell<Option<serde_json::Value>> = const { RefCell::new(None) };
}

pub(super) fn clear() {
    LAST.with(|last| *last.borrow_mut() = None);
}

pub(super) fn take() -> Option<serde_json::Value> {
    LAST.with(|last| last.borrow_mut().take())
}

pub(super) fn record(
    scope: &NativeJointPreparedPublicationScope<'_>,
    expected: &NativeJointServingBridgeRecord,
    admission: &NativeJointActivationAdmission,
    recovered: bool,
    stage: &'static str,
) {
    // Capture clocks BEFORE forensic reads. These observations cannot renew
    // the failed request or establish an API return/old fsync outcome.
    let started = std::time::Instant::now();
    let token = admission.test_phase_cancellation();
    let phase_remaining_us = token
        .deadline()
        .map(|d| d.saturating_duration_since(started).as_micros());
    let phase_cancel_requested = token.is_cancel_requested();
    let phase_own = admission.committed().test_request_memory();
    let db = scope.database;
    let range = expected.prepared.range_id;
    let gate = db
        .range_mutation_gate()
        .test_source_capture_pin_state(range);
    let storage = db.paged_records.as_ref().map(|paged| {
        let snapshot = paged.latest_snapshot();
        let observe = |name: &str| {
            match paged.store().get_as_of(&snapshot, &native_key(name)) {
                Ok(Some(bytes)) if bytes.len() <= MAX_NATIVE_RECEIPT_BYTES => {
                    let hash: [u8; 32] = Sha256::digest(&bytes).into();
                    let parsed = serde_json::from_slice::<serde_json::Value>(&bytes);
                    serde_json::json!({
                        "present":true, "raw_bytes":bytes.len(), "raw_sha256_bytes":hash,
                        "decoded_native_page_xid":parsed.as_ref().ok().and_then(|v|v.get("native_page_xid")).and_then(|v|v.as_u64()),
                        "decoded_checksum_sha256":parsed.as_ref().ok().and_then(|v|v.get("checksum_sha256")).and_then(|v|v.as_str()).filter(|s|s.len()<=128),
                        "decode_error":parsed.is_err(),
                        "read_error":false,"oversize":false
                    })
                }
                Ok(Some(bytes)) => serde_json::json!({
                    "present":true,"raw_bytes":bytes.len(),"oversize":true,
                    "read_error":false,"hash_omitted":true
                }),
                Ok(None) => serde_json::json!({"present":false,"read_error":false}),
                Err(_) => serde_json::json!({"present":null,"read_error":true}),
            }
        };
        serde_json::json!({
            "native_next_xid":paged.store().transactions().next_xid(),
            "native_next_lsn":paged.store().wal().next_lsn(),
            "native_durable_lsn":paged.store().wal().durable_lsn(),
            "current":observe(&key(range)),
            "archive":observe(&archive_key(range,&expected.checksum_sha256))
        })
    });
    let value = serde_json::json!({
        "stage":stage,"new_native_commit_returned_this_call":!recovered,
        "existing_committed_record_reconciliation":recovered,
        "expected_native_page_xid":expected.native_page_xid,
        "expected_checksum_sha256":expected.checksum_sha256,
        "actual_frozen_owner":scope.frozen.owner(),
        "phase_remaining_us_at_observation_entry":phase_remaining_us,
        "phase_cancel_requested_at_observation_entry":phase_cancel_requested,
        "phase_own_at_observation_entry":phase_own,
        "clocks_precede_forensic_reads":true,
        "gate":gate.as_ref().ok(),"gate_diagnostic_error":gate.is_err(),
        "storage":storage,"diagnostic_elapsed_us":started.elapsed().as_micros(),
        "scope":"historical native readback DATA; not API success, authority, rollback or old-fsync reconciliation"
    });
    // One bounded metadata-only observation per synchronous call. Raw native
    // values/parse errors and all borrowed guards have already been dropped.
    LAST.with(|last| *last.borrow_mut() = Some(value));
}
