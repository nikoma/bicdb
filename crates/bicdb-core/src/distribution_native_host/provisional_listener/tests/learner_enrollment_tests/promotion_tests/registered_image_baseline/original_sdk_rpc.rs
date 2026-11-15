//! Exact original gateway transport diagnostics; test-only and no reply hook.
//! Existing transport quotas fence evidence failure rather than hide/truncate
//! attempts. Native/source/target traffic uses its independent existing logs.
use super::*;

pub(super) const LIMITS: crate::ClusterRpcEvidenceLimits = crate::ClusterRpcEvidenceLimits {
    max_records: 4096,
    max_bytes: 64 * 1024 * 1024,
};
pub(super) struct Journal {
    directory: PathBuf,
    attached: std::sync::atomic::AtomicBool,
}
impl Journal {
    pub(super) fn create(directory: PathBuf) -> Result<Arc<Self>> {
        std::fs::create_dir(&directory)?;
        Ok(Arc::new(Self {
            directory,
            attached: std::sync::atomic::AtomicBool::new(false),
        }))
    }
    pub(super) fn mark_attached_before_first_append(&self) {
        self.attached
            .store(true, std::sync::atomic::Ordering::Release);
    }
    /// Called after all original SDK calls return/unwind. These files retain
    /// exact error chains and complete primary/refresh request/response bytes.
    /// A malformed/partial file is reported, never treated as a missing call.
    pub(super) fn snapshot(&self) -> Result<serde_json::Value> {
        let mut entries =
            std::fs::read_dir(&self.directory)?.collect::<std::io::Result<Vec<_>>>()?;
        if entries.len() > LIMITS.max_records as usize {
            return Err(BicDbError::Cluster(
                "original SDK RPC journal record bound exceeded".into(),
            ));
        }
        entries.sort_by_key(|entry| entry.file_name());
        let mut total = 0u64;
        let mut records = Vec::with_capacity(entries.len());
        let mut complete = true;
        let mut caller_ended_reads = 0u64;
        for entry in entries {
            let name = entry.file_name().to_string_lossy().into_owned();
            if !entry.file_type()?.is_file()
                || !name.starts_with("rpc-")
                || !name.ends_with(".json")
            {
                return Err(BicDbError::Cluster(
                    "unexpected entry in original SDK RPC journal".into(),
                ));
            }
            let length = entry.metadata()?.len();
            total = total
                .checked_add(length)
                .ok_or_else(|| BicDbError::Cluster("SDK RPC journal size overflow".into()))?;
            if total > LIMITS.max_bytes {
                return Err(BicDbError::Cluster(
                    "original SDK RPC journal byte bound exceeded".into(),
                ));
            }
            let bytes = std::fs::read(entry.path())?;
            let length_after = entry.metadata()?.len();
            let stable = length == bytes.len() as u64 && length_after == length;
            let decoded = serde_json::from_slice::<serde_json::Value>(&bytes);
            let (frame, frame_error) = match decoded.as_ref().ok().map(|v| &v["raw_frame"]) {
                None | Some(serde_json::Value::Null) => (None, None),
                Some(raw) => match serde_json::from_value::<Vec<u8>>(raw.clone()) {
                    Err(error) => (None, Some(error.to_string())),
                    Ok(raw) => {
                        let header = raw.get(..4).map(|h| <[u8; 4]>::try_from(h).unwrap());
                        let declared = header.map(u32::from_be_bytes);
                        let exact =
                            declared.is_some_and(|n| raw.len().checked_sub(4) == Some(n as usize));
                        (Some(serde_json::json!({"actual_frame_bytes":raw.len(),
                            "sha256":hex::encode(Sha256::digest(&raw)),
                            "actual_length_header":header,"declared_payload_bytes":declared,
                            "complete_length_framing":exact})),
                            (!exact).then(||"actual recorded frame is partial or has mismatched length header".into()))
                    }
                },
            };
            // A read that this client itself ended (it hung up on an attempt
            // it no longer waits for, or its own request deadline passed) is
            // retained by the transport as the bytes received so far. That
            // record is complete; only the response it describes is not. Any
            // other short or mismatched frame (a peer that closed early, a
            // rejected length, a request frame) stays an evidence error.
            let caller_ended_read = frame_error.is_some()
                && decoded
                    .as_ref()
                    .is_ok_and(|record| caller_ended_response_read(record));
            let frame_error = frame_error.filter(|_| !caller_ended_read);
            caller_ended_reads += u64::from(caller_ended_read);
            complete &= stable && frame_error.is_none();
            complete &= decoded.as_ref().is_ok_and(|record| {
                record["journal_sequence"].as_u64() == Some(records.len() as u64 + 1)
                    && name
                        == format!(
                            "rpc-{:06}.json",
                            record["journal_sequence"].as_u64().unwrap_or(0)
                        )
            });
            records.push(serde_json::json!({"file":name,"bytes":bytes.len(),
                "sha256":hex::encode(Sha256::digest(&bytes)),
                "file_bytes_before_read":length,"file_bytes_after_read":length_after,
                "file_byte_count_stable":stable,"raw_frame_header_and_hash":frame,
                "raw_frame_evidence_error":frame_error,
                "response_read_ended_by_this_client_before_complete_frame":caller_ended_read,
                "attempt_id":decoded.as_ref().ok().map(|v|&v["attempt_id"]),
                "journal_sequence":decoded.as_ref().ok().map(|v|&v["journal_sequence"]),
                "phase":decoded.as_ref().ok().map(|v|&v["phase"]),
                "operation":decoded.as_ref().ok().map(|v|&v["operation"]),
                "caller":decoded.as_ref().ok().map(|v|&v["caller"]),
                "destination":decoded.as_ref().ok().map(|v|&v["destination"]),
                "topology_generation":decoded.as_ref().ok().map(|v|&v["topology_generation"]),
                "address":decoded.as_ref().ok().map(|v|&v["address"]),
                "may_have_dispatched":decoded.as_ref().ok().map(|v|&v["may_have_dispatched"]),
                "error_chain":decoded.as_ref().ok().map(|v|&v["error_chain"]),
                "decode_error":decoded.as_ref().err().map(ToString::to_string)}));
        }
        Ok(serde_json::json!({"configured_for_original_sdk":true,
            "attached_before_first_source_append":self.attached.load(std::sync::atomic::Ordering::Acquire),
            "complete_record_decode_and_contiguous_sequence":complete,
            "evidence_error":(!complete).then_some("malformed, gapped, partial or unstable actual journal records"),
            "response_reads_ended_by_this_client_before_complete_frame":caller_ended_reads,
            "zero_records_do_not_prove_no_attempt":true,
            "configured_original_sdk_request_timeout_ms":2000,"max_records":LIMITS.max_records,
            "max_bytes":LIMITS.max_bytes,"actual_record_count":records.len(),"actual_bytes":total,
            "records":records,"raw_frame_bytes_retained":true,"native_phase_rpcs_in_this_observer":false,
            "sdk_retry_or_error_selection_modified":false,"is_qualification":false}))
    }
}
/// The transport's record of a response read that the observed client ended
/// itself: its cancellation of the attempt or its own deadline, with fewer
/// bytes retained than a whole frame. Nothing a peer sends can produce the
/// two causes named here; an early close by the peer reports an end of file.
fn caller_ended_response_read(record: &serde_json::Value) -> bool {
    let ended_here = record["error_chain"].as_array().is_some_and(|chain| {
        chain
            .iter()
            .filter_map(|cause| cause.as_str())
            .any(|cause| {
                cause.starts_with("cluster RPC interrupted: ")
                    || cause == "cluster RPC deadline expired"
            })
    });
    let short = serde_json::from_value::<Vec<u8>>(record["raw_frame"].clone()).is_ok_and(|raw| {
        match raw.get(..4) {
            None => true,
            Some(header) => {
                let declared = u32::from_be_bytes(header.try_into().unwrap()) as usize;
                raw.len() - 4 < declared
            }
        }
    });
    record["phase"] == "partial_or_rejected_response_frame"
        && record["may_have_dispatched"] == true
        && ended_here
        && short
}
impl crate::ClusterRpcEvidenceObserver for Journal {
    fn record(&self, record: &crate::ClusterRpcEvidence) -> std::io::Result<()> {
        // Reservation order and quota are assigned by the existing actual
        // transport. Unique filenames allow legitimate concurrent callbacks;
        // no global sink lock or callback reentry is introduced.
        if record.journal_sequence == 0 || record.journal_sequence > LIMITS.max_records {
            return Err(std::io::Error::other(
                "actual SDK RPC reservation outside test bound",
            ));
        }
        let path = self
            .directory
            .join(format!("rpc-{:06}.json", record.journal_sequence));
        let bytes = serde_json::to_vec(record)?;
        if bytes.len() as u64 > LIMITS.max_bytes {
            return Err(std::io::Error::other(
                "actual SDK RPC record exceeds test byte bound",
            ));
        }
        crate::storage::write_atomic(&path, &bytes, true)
            .map_err(|error| std::io::Error::other(error.to_string()))
    }
}

#[cfg(test)]
mod journal_tests {
    use super::*;
    use crate::ClusterRpcEvidenceObserver as _;

    fn record(
        sequence: u64,
        phase: &'static str,
        frame: Option<Vec<u8>>,
        chain: &[&str],
    ) -> crate::ClusterRpcEvidence {
        crate::ClusterRpcEvidence {
            attempt_id: Uuid::now_v7(),
            journal_sequence: sequence,
            phase_unix_ns: 1,
            elapsed_ns: 1,
            phase,
            operation: "native_event_get",
            caller: "n0".into(),
            destination: "n1".into(),
            topology_generation: Some(5),
            address: Some("127.0.0.1:1".into()),
            request_id: Some(7),
            raw_frame: frame,
            bytes_written: 9,
            may_have_dispatched: true,
            error_chain: chain.iter().map(|cause| cause.to_string()).collect(),
        }
    }
    fn complete(records: &[crate::ClusterRpcEvidence]) -> (bool, u64) {
        let root = tempfile::tempdir().unwrap();
        let journal = Journal::create(root.path().join("journal")).unwrap();
        for record in records {
            journal.record(record).unwrap();
        }
        let value = journal.snapshot().unwrap();
        (
            value["complete_record_decode_and_contiguous_sequence"] == true,
            value["response_reads_ended_by_this_client_before_complete_frame"]
                .as_u64()
                .unwrap(),
        )
    }
    const CANCELED: [&str; 2] = [
        "io error: cluster RPC interrupted: canceling statement due to user request",
        "cluster RPC interrupted: canceling statement due to user request",
    ];
    const EXPIRED: [&str; 2] = [
        "io error: cluster RPC deadline expired",
        "cluster RPC deadline expired",
    ];
    const CLOSED: [&str; 2] = [
        "io error: failed to fill whole buffer",
        "failed to fill whole buffer",
    ];
    const PARTIAL: &str = "partial_or_rejected_response_frame";

    /// The record the transport writes when the client hangs up on its own
    /// read is a complete record and is counted, not an evidence error.
    #[test]
    fn original_sdk_rpc_journal_accepts_a_response_read_ended_by_the_client_itself() {
        let whole = vec![0, 0, 0, 2, b'{', b'}'];
        for (chain, frame) in [
            (CANCELED, vec![]),
            (CANCELED, vec![0, 0]),
            (CANCELED, vec![0, 0, 0, 9, b'{']),
            (EXPIRED, vec![]),
        ] {
            assert_eq!(
                complete(&[
                    record(1, "request_frame_before_dispatch", Some(whole.clone()), &[]),
                    record(2, PARTIAL, Some(frame.clone()), &chain),
                    record(3, "validated_transport_result_error", None, &chain[1..]),
                ]),
                (true, 1),
                "{chain:?} {frame:?}"
            );
        }
    }
    /// Everything else that is short, mismatched or missing still fails.
    #[test]
    fn original_sdk_rpc_journal_still_refuses_other_incomplete_frames_and_gaps() {
        let whole = vec![0, 0, 0, 2, b'{', b'}'];
        // The peer closed early, with and without the client's wording
        // somewhere other than a cause of the chain.
        assert_eq!(
            complete(&[record(1, PARTIAL, Some(vec![]), &CLOSED)]),
            (false, 0)
        );
        assert_eq!(
            complete(&[record(1, PARTIAL, Some(vec![0, 0, 0, 9, b'{']), &CLOSED)]),
            (false, 0)
        );
        assert_eq!(
            complete(&[record(1, PARTIAL, Some(vec![]), &[])]),
            (false, 0)
        );
        // A frame at least as long as it declares is not a short read.
        assert_eq!(
            complete(&[record(
                1,
                PARTIAL,
                Some(vec![0, 0, 0, 1, b'{', b'}']),
                &CANCELED
            )]),
            (false, 0)
        );
        // Only a response read may be short: never a request frame or a
        // response the transport went on to decode.
        for phase in [
            "request_frame_before_dispatch",
            "response_frame_before_decode",
        ] {
            assert_eq!(
                complete(&[record(1, phase, Some(vec![]), &CANCELED)]),
                (false, 0),
                "{phase}"
            );
        }
        // A missing record is a gap whatever the records around it say.
        assert_eq!(
            complete(&[
                record(1, "request_frame_before_dispatch", Some(whole.clone()), &[]),
                record(3, PARTIAL, Some(vec![]), &CANCELED),
            ]),
            (false, 1)
        );
        assert_eq!(
            complete(&[record(1, "request_frame_before_dispatch", Some(whole), &[])]),
            (true, 0)
        );
    }
}
