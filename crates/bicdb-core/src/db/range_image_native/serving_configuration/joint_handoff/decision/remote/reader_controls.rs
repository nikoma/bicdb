//! Scoped denial-only read-observation faults in the ACTUAL current reader.
//! No on-disk write, fake receipt constructor, source/Role grant or cold claim.
use super::*;
use std::cell::Cell;
#[derive(Clone, Copy, Debug)]
enum Fault {
    ErasedProfilePointer,
    MissingCurrent,
    MissingArchive,
    MismatchedArchive,
    MismatchedXid,
    MissingConstraints,
}
thread_local! { static FAULT:Cell<Option<Fault>>=const { Cell::new(None) }; }
struct Guard(Option<Fault>);
impl Drop for Guard {
    fn drop(&mut self) {
        FAULT.with(|s| s.set(self.0));
    }
}
fn install(fault: Fault) -> Guard {
    Guard(FAULT.with(|s| s.replace(Some(fault))))
}
pub(super) fn profile_record(
    record: &NativeJointDecisionRecord,
) -> Option<NativeJointDecisionRecord> {
    FAULT
        .with(|f| matches!(f.get(), Some(Fault::ErasedProfilePointer)))
        .then(|| {
            let mut observed = record.clone();
            observed.remote_provenance_sha256 = None;
            observed
        })
}
pub(super) fn current_observation(value: &mut Option<RemoteDecisionProvenance>) -> Result<()> {
    let fault = FAULT.with(Cell::get);
    if matches!(fault, Some(Fault::MissingCurrent)) {
        *value = None;
        return Ok(());
    }
    if let Some(value) = value {
        match fault {
            Some(Fault::MismatchedXid) => {
                value.native_page_xid = value
                    .native_page_xid
                    .checked_add(1)
                    .ok_or_else(|| invalid("reader control xid overflow"))?
            }
            Some(Fault::MissingConstraints) => {
                value.correlation.source_proof.constraints.pop();
                value.correlation.source_proof.checksum_sha256 =
                    value.correlation.source_proof.checksum()?;
                value.correlation.source_evidence_sha256 =
                    value.correlation.source_proof.checksum_sha256.clone();
            }
            _ => {}
        }
    }
    Ok(())
}
pub(super) fn archive_observation(value: &mut Option<RemoteDecisionProvenance>) {
    match FAULT.with(Cell::get) {
        Some(Fault::MissingArchive) => *value = None,
        Some(Fault::MismatchedArchive) => {
            if let Some(value) = value {
                value.checksum_sha256 = "e".repeat(64);
            }
        }
        _ => {}
    }
}
impl BicDb {
    /// Starts from an actual committed protected receipt, performs the SAME
    /// actual reader for every cut, and exports ONLY diagnostics. No grant or
    /// receipt escapes, and the original native bytes are never modified.
    pub(crate) fn test_remote_native_decision_current_reader_refusals(
        &self,
        native: &NativeRecoveryAdmission,
        range: RangeId,
    ) -> Result<serde_json::Value> {
        let actual = self.read_current_native_joint_writer_decision(native, range)?;
        let record = actual.record.clone();
        if record.remote_provenance_sha256.is_none() {
            return Err(invalid("reader control needs genuine remote profile"));
        }
        let sidecar = self.require_remote_native_decision_provenance(&record)?;
        let original_body = bounded_json(&record, MAX_NATIVE_RECEIPT_BYTES)?;
        let original_sidecar = bounded_json(&sidecar, MAX_NATIVE_RECEIPT_BYTES)?;
        let actual_status = self
            .paged_records
            .as_ref()
            .ok_or_else(|| invalid("reader control native store absent"))?
            .store()
            .transactions()
            .status(record.native_page_xid);
        if actual_status != bicdb_page::TxStatus::Committed {
            return Err(invalid("reader control requires actual committed xid"));
        }
        let mut observations = Vec::new();
        for fault in [
            Fault::ErasedProfilePointer,
            Fault::MissingCurrent,
            Fault::MissingArchive,
            Fault::MismatchedArchive,
            Fault::MismatchedXid,
            Fault::MissingConstraints,
        ] {
            let result = {
                let _guard = install(fault);
                self.read_native_joint_decision(native, range, record.decision.checksum_sha256())
            };
            let expected_refusal = match fault {
                Fault::ErasedProfilePointer => "provenance pointer erased from common CURRENT",
                Fault::MissingCurrent => "protected provenance CURRENT absent",
                Fault::MissingArchive => "immutable provenance archive absent",
                Fault::MismatchedArchive => "provenance CURRENT/archive mismatch",
                Fault::MismatchedXid => "provenance/body/xid mismatch",
                Fault::MissingConstraints => "source obligations/plan changed",
            };
            observations.push(
                serde_json::json!({"injected_read_observation":format!("{fault:?}"),"expected_refusal":expected_refusal,
                "actual_current_reader_error":result.as_ref().err().map(ToString::to_string),
                "receipt_returned":result.is_ok(),"not_on_disk_corruption":true}),
            );
            // Every fault guard has ended. Re-read the actual original profile
            // through its normal reader before advancing to another fault.
            actual.require_current(self)?;
        }
        let after = self.read_current_native_joint_writer_decision(native, range)?;
        let sidecar_after = self.require_remote_native_decision_provenance(&after.record)?;
        if bounded_json(&after.record, MAX_NATIVE_RECEIPT_BYTES)? != original_body
            || bounded_json(&sidecar_after, MAX_NATIVE_RECEIPT_BYTES)? != original_sidecar
        {
            return Err(invalid(
                "reader controls changed actual protected native bytes",
            ));
        }
        Ok(
            serde_json::json!({"actual_native_xid":record.native_page_xid,"actual_native_status":format!("{actual_status:?}"),
            "actual_body_canonical_bytes_sha256":hex::encode(sha2::Sha256::digest(&original_body)),
            "actual_provenance_canonical_bytes_sha256":hex::encode(sha2::Sha256::digest(&original_sidecar)),
            "original_native_bytes_unchanged":true,"observations":observations,"grants_returned":0,
            "not_physical_or_cold_qualification":true}),
        )
    }
}
