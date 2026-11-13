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
            let decoded = serde_json::from_slice::<serde_json::Value>(&bytes);
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
        Ok(
            serde_json::json!({"enabled_before_first_source_append":true,
            "configured_original_sdk_request_timeout_ms":2000,"max_records":LIMITS.max_records,
            "max_bytes":LIMITS.max_bytes,"actual_record_count":records.len(),"actual_bytes":total,
            "records":records,"raw_frame_bytes_retained":true,"native_phase_rpcs_in_this_observer":false,
            "sdk_retry_or_error_selection_modified":false,"is_qualification":false}),
        )
    }
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
