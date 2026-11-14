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

pub(super) struct OnPanic {
    directory: PathBuf,
    api_results: Vec<serde_json::Value>,
    omitted_api_results: usize,
}
impl OnPanic {
    pub(super) fn new(directory: &Path) -> Self {
        Self {
            directory: directory.to_owned(),
            api_results: Vec::new(),
            omitted_api_results: 0,
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
            paths.sort();
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
