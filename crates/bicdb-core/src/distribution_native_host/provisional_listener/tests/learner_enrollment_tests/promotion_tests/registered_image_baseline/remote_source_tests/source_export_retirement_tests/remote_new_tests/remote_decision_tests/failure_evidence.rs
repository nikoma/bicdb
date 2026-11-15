//! Test-only, bounded, lossless export of already-written diagnostics on panic.
//! No native owner, authorization, request or lifetime is retained here.
use sha2::{Digest, Sha256};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

const MAX_FILES: usize = 16;
const MAX_DIRECTORY_ENTRIES: usize = 128;
const MAX_FILE_BYTES: usize = 1024 * 1024;
const MAX_TOTAL_BYTES: usize = 4 * 1024 * 1024;
const MAX_API_ERRORS: usize = 2;
const MAX_API_ERROR_BYTES: usize = 64 * 1024;
const COMPACT_FILES: [&str; 3] = [
    "remote-decision-compact-first.json",
    "remote-decision-compact-retry.json",
    "remote-decision-compact-retry-with-gate-after.json",
];
const MAX_COMPACT_WRITES: usize = 3;
const MAX_SCALAR_BYTES: usize = 256;
const MAX_COMPACT_BYTES: usize = 128 * 1024;

pub(super) struct OnPanic {
    directory: PathBuf,
    api_results: Vec<serde_json::Value>,
    omitted_api_results: usize,
    compact_writes: Vec<serde_json::Value>,
}
impl OnPanic {
    pub(super) fn new(directory: &Path) -> Self {
        Self {
            directory: directory.to_owned(),
            api_results: Vec::new(),
            omitted_api_results: 0,
            compact_writes: Vec::new(),
        }
    }
    /// Fixed DATA projection of existing post-API observations. No native read,
    /// authority or error owner is retained. Missing/busy reads stay unavailable.
    pub(super) fn record_compact(
        &mut self,
        retry: bool,
        native: &Result<serde_json::Value, crate::BicDbError>,
        health: Option<&serde_json::Value>,
        gate_before: &serde_json::Value,
        gate_after: Option<&serde_json::Value>,
    ) {
        let fields = [
            "/original_phase",
            "/actual_test_cut/code",
            "/actual_test_cut/fired",
            "/retained_request_contexts",
            "/native/actual_issued_xid",
            "/native/native_attempt_limit",
            "/native/actual_commit_sync_fault_count",
            "/native/actual_native_attempt/native_xid",
            "/native/actual_native_attempt/outcome/state",
            "/native/actual_native_attempt/outcome/actual_xid",
            "/native/actual_native_attempt/outcome/error",
            "/native/actual_native_attempt/bytes_sha256",
            "/native/actual_native_attempt/provenance_bytes_sha256",
            "/native/actual_native_attempt/record/checksum_sha256",
        ];
        let native_fields: serde_json::Map<String, serde_json::Value> = fields
            .into_iter()
            .map(|path| {
                (
                    path.into(),
                    scalar(native.as_ref().ok().and_then(|v| v.pointer(path))),
                )
            })
            .collect();
        let health_fields: serde_json::Map<String, serde_json::Value> = [
            "database_open",
            "core_health",
            "page_health",
            "wal_health",
            "requested_own_xid",
            "native_status_without_health_admission",
        ]
        .into_iter()
        .map(|key| (key.into(), scalar(health.and_then(|h| h.get(key)))))
        .collect();
        let native_error = native
            .as_ref()
            .err()
            .map(|e| serde_json::Value::String(e.to_string()));
        let value = serde_json::json!({
            "stage":if retry {"retry"} else {"first"},
            "native_observation_available":native.is_ok(), "native_observation_error":scalar(native_error.as_ref()),
            "native_fields":native_fields, "health_observation_available":health.is_some(),
            "health_unavailable_reason":if health.is_none() {Some("actual nonblocking DB borrow unavailable")} else {None},
            "health_fields":health_fields, "gate_before_API":gate_summary(Some(gate_before)), "gate_after_API":gate_summary(gate_after),
            "scope":"bounded post-API DATA only; missing observations do not prove absence, rollback, native outcome or qualification",
        });
        // Enforce a separate tighter bound; do not raise the existing exporter
        // cap. If this projection ever grows, make its omission explicit.
        let value = match serde_json::to_vec_pretty(&value) {
            Ok(bytes) if bytes.len() > MAX_COMPACT_BYTES => serde_json::json!({
                "compact_projection_omitted":true,"reason":"explicit compact byte bound",
                "bytes":bytes.len(),"sha256":hex::encode(Sha256::digest(&bytes))}),
            Err(error) => serde_json::json!({"compact_projection_omitted":true,
                "reason":"compact serialization unavailable",
                "error":scalar(Some(&serde_json::Value::String(error.to_string())))}),
            _ => value,
        };
        // Preserve the first retry observation even if later state/gate
        // diagnostics panic or the later evidence write fails.
        let index = if retry && gate_after.is_some() {
            2
        } else {
            usize::from(retry)
        };
        let name = COMPACT_FILES[index];
        // Best effort: a failed write cannot replace the primary API result.
        let error = super::evidence(&self.directory.join(name), &value)
            .err()
            .map(|e| serde_json::Value::String(e.to_string()));
        if self.compact_writes.len() < MAX_COMPACT_WRITES {
            self.compact_writes.push(serde_json::json!({"file":name, "write_returned_ok":error.is_none(), "write_error":scalar(error.as_ref())}));
        }
    }
    /// DATA only, captured immediately after the API returns. It remains
    /// available even if later diagnostics fail or exceed their file bounds.
    pub(super) fn record_api(&mut self, stage: &'static str, error: Option<&str>) {
        if self.api_results.len() == MAX_API_ERRORS {
            self.omitted_api_results += 1;
            return;
        }
        let value = match error {
            Some(error) if error.len() > MAX_API_ERROR_BYTES => {
                self.omitted_api_results += 1;
                serde_json::json!({"stage":stage,"returned_error":true,
                    "error_omitted":true,"bytes":error.len(),
                    "sha256":hex::encode(Sha256::digest(error.as_bytes()))})
            }
            _ => serde_json::json!({"stage":stage,"returned_error":error.is_some(),
                "exact_debug_error":error}),
        };
        self.api_results.push(value);
    }
}
// Only bounded primitives (and the gate's tiny tuples) are copied. No frames,
// rows, full provenance, authority or original Error is retained by this guard.
fn scalar(value: Option<&serde_json::Value>) -> serde_json::Value {
    use serde_json::Value;
    match value {
        None => serde_json::json!({"available":false,"reason":"field not observed"}),
        Some(Value::String(text)) if text.len() > MAX_SCALAR_BYTES => {
            let mut end = MAX_SCALAR_BYTES;
            while !text.is_char_boundary(end) {
                end -= 1;
            }
            serde_json::json!({"prefix":&text[..end],"truncated":true,"original_bytes":text.len(),"sha256":hex::encode(Sha256::digest(text.as_bytes()))})
        }
        Some(v @ (Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_))) => v.clone(),
        Some(Value::Array(values))
            if values.len() <= 4 && values.iter().all(|v| !v.is_array() && !v.is_object()) =>
        {
            Value::Array(values.iter().map(|v| scalar(Some(v))).collect())
        }
        Some(_) => serde_json::json!({"available":false,"reason":"not bounded primitive DATA"}),
    }
}
fn gate_summary(value: Option<&serde_json::Value>) -> serde_json::Value {
    let Some(nodes) = value.and_then(serde_json::Value::as_object) else {
        return serde_json::json!({"available":false,"reason":"gate observation not reached"});
    };
    let observed: Vec<_> = nodes.iter().take(8).map(|(node, state)| {
        let fields: serde_json::Map<String, serde_json::Value> = ["capture_pin", "frozen", "frozen_owner_dropped", "poisoned", "active", "controlled"]
            .into_iter().map(|key| (key.into(), scalar(state.get(key)))).collect();
        serde_json::json!({"node":scalar(Some(&serde_json::Value::String(node.clone()))),"state":fields})
    }).collect();
    serde_json::json!({"available":true,"nodes":observed,"omitted_nodes":nodes.len().saturating_sub(8)})
}
fn emit(value: serde_json::Value) {
    // A diagnostic write failure must never replace the original panic.
    let _ = writeln!(
        std::io::stderr().lock(),
        "NATIVE_REMOTE_DECISION_FAILURE_EVIDENCE {value}"
    );
}
impl Drop for OnPanic {
    fn drop(&mut self) {
        if !std::thread::panicking() {
            return;
        }
        emit(serde_json::json!({"api_results":self.api_results,
            "omitted_api_results":self.omitted_api_results,
            "recorded_api_results_untruncated":self.omitted_api_results==0,
            "compact_diagnostic_writes":self.compact_writes,
            "scope":"returned API results recorded before panic; not complete RPC coverage"}));
        let result = (|| -> std::io::Result<()> {
            let mut paths = Vec::new();
            let mut enumeration_truncated = false;
            for (index, entry) in std::fs::read_dir(&self.directory)?.enumerate() {
                if index >= MAX_DIRECTORY_ENTRIES {
                    enumeration_truncated = true;
                    break;
                }
                let entry = entry?;
                let name = entry.file_name();
                let name = name.to_string_lossy();
                if name.starts_with("remote-decision-") && name.ends_with(".json") {
                    paths.push(entry.path());
                }
            }
            // Explicit compact files precede oversized full reports; existing
            // file and aggregate caps remain unchanged.
            paths.sort_by_key(|path| {
                let compact = path
                    .file_name()
                    .and_then(|n| n.to_str())
                    .is_some_and(|n| COMPACT_FILES.contains(&n));
                (!compact, path.clone())
            });
            let total_files = paths.len();
            let mut total_bytes = 0;
            let mut omitted = total_files.saturating_sub(MAX_FILES);
            for path in paths.into_iter().take(MAX_FILES) {
                let name = path
                    .file_name()
                    .map(|name| name.to_string_lossy().into_owned());
                let metadata = std::fs::symlink_metadata(&path)?;
                if !metadata.is_file()
                    || metadata.file_type().is_symlink()
                    || metadata.len() > MAX_FILE_BYTES as u64
                {
                    omitted += 1;
                    emit(serde_json::json!({"file":name,"omitted":true,
                        "reason":"not regular nonsymlink file or explicit file byte bound",
                        "reported_bytes":metadata.len()}));
                    continue;
                }
                let mut bytes = Vec::new();
                std::fs::File::open(&path)?
                    .take((MAX_FILE_BYTES + 1) as u64)
                    .read_to_end(&mut bytes)?;
                if bytes.len() > MAX_FILE_BYTES || bytes.len() > MAX_TOTAL_BYTES - total_bytes {
                    omitted += 1;
                    emit(serde_json::json!({"file":name,"omitted":true,
                        "reason":"explicit aggregate/file byte bound"}));
                    continue;
                }
                total_bytes += bytes.len();
                let sha = hex::encode(Sha256::digest(&bytes));
                match String::from_utf8(bytes) {
                    Ok(text) => emit(serde_json::json!({"file":name,
                        "bytes":text.len(),"sha256":sha,"raw_utf8":text})),
                    Err(error) => emit(serde_json::json!({"file":name,
                        "bytes":error.as_bytes().len(),"sha256":sha,
                        "raw_hex":hex::encode(error.as_bytes())})),
                }
            }
            emit(
                serde_json::json!({"summary":true,"matching_files_enumerated":total_files,
                "directory_entry_limit":MAX_DIRECTORY_ENTRIES,
                "directory_enumeration_truncated":enumeration_truncated,
                "bytes_exported":total_bytes,"omitted_files":omitted,
                "complete":omitted==0 && !enumeration_truncated,
                "reason":"original test panic; no runtime authority"}),
            );
            Ok(())
        })();
        if let Err(error) = result {
            emit(serde_json::json!({"summary":true,"complete":false,
                "diagnostic_error":error.to_string(),"original_panic_preserved":true}));
        }
    }
}
