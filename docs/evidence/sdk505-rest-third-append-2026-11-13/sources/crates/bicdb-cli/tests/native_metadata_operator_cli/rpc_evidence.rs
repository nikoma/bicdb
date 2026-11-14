//! Test-only actual public-client wire journal. Never admission/read authority.
use super::*;
use bicdb_core::{ClusterRpcEvidence, ClusterRpcEvidenceObserver};
use sha2::{Digest, Sha256};
use std::{
    fs,
    io::{self, Write},
    sync::{Arc, Mutex},
    time::{SystemTime, UNIX_EPOCH},
};
use uuid::Uuid;
const MAX_RECORDS: u64 = 4096;
const MAX_BYTES: u64 = 64 * 1024 * 1024;

struct Context {
    id: Uuid,
    request: Value,
    may_have_dispatched: bool,
}
struct State {
    file: fs::File,
    records: u64,
    bytes: u64,
    active: Option<Context>,
    failed: bool,
    closed: bool,
}
pub(super) struct DurableRpcJournal {
    state: Mutex<State>,
    pub(super) path: PathBuf,
}
pub(super) struct LogicalCall {
    journal: Arc<DurableRpcJournal>,
    pub(super) id: Uuid,
    finished: bool,
}
struct BoundObserver {
    journal: Arc<DurableRpcJournal>,
    id: Uuid,
    name: &'static str,
}
fn invalid(message: &str) -> io::Error {
    io::Error::other(message)
}
fn now() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos()
}
fn error_chain(error: &(dyn std::error::Error + 'static)) -> Vec<String> {
    let mut errors = vec![error.to_string()];
    let mut source = error.source();
    while let Some(error) = source {
        errors.push(error.to_string());
        source = error.source();
    }
    errors
}
fn append(state: &mut State, mut value: Value) -> io::Result<()> {
    if state.failed || state.closed {
        return Err(invalid(
            "durable CLI RPC journal already failed; no trace continuity claim",
        ));
    }
    value["sink_sequence"] = (state.records + 1).into();
    value["sink_unix_ns"] = serde_json::json!(now());
    let mut bytes = serde_json::to_vec(&value)?;
    bytes.push(b'\n');
    if state.records >= MAX_RECORDS
        || state
            .bytes
            .checked_add(bytes.len() as u64)
            .is_none_or(|n| n > MAX_BYTES)
    {
        state.failed = true;
        return Err(invalid(
            "durable CLI RPC journal cap; no truncation or trace continuity claim",
        ));
    }
    let result = state
        .file
        .write_all(&bytes)
        .and_then(|_| state.file.sync_all());
    if let Err(error) = result {
        state.failed = true;
        return Err(error);
    }
    state.records += 1;
    state.bytes += bytes.len() as u64;
    Ok(())
}
// Persist the pathname as well as the record bytes. Workflow-created parent
// directories may also be new: syncing only the leaf loses the whole journal
// after a controller crash. Walk ONLY journal ancestors, never a DB tree/file.
fn prepare_journal_directory(directory: &Path, fixture: &Path) -> io::Result<()> {
    if !directory.is_absolute()
        || directory
            .components()
            .any(|part| matches!(part, std::path::Component::ParentDir))
    {
        return Err(invalid(
            "journal destination must be absolute without parent traversal",
        ));
    }
    let fixture = fixture.canonicalize()?;
    if directory
        .strip_prefix(&fixture)
        .ok()
        .and_then(|relative| relative.components().next())
        .and_then(|part| part.as_os_str().to_str())
        .is_some_and(|part| {
            part.strip_prefix("db").is_some_and(|number| {
                !number.is_empty() && number.bytes().all(|byte| byte.is_ascii_digit())
            })
        })
    {
        return Err(invalid(
            "journal must not create or fsync inside an original database root",
        ));
    }
    for ancestor in directory.ancestors() {
        match fs::symlink_metadata(ancestor) {
            Ok(metadata) if metadata.file_type().is_symlink() || !metadata.is_dir() => {
                return Err(invalid(
                    "journal ancestor must be an actual directory, not a symlink",
                ))
            }
            Ok(_) => {}
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(error),
        }
    }
    fs::create_dir_all(directory)
}
fn sync_journal_directory_chain(directory: &Path) -> io::Result<()> {
    // Leaf contains journal.jsonl; each following parent contains the previous
    // directory entry. This also covers unsynced workflow-created ancestors.
    for ancestor in directory.ancestors() {
        fs::File::open(ancestor)?.sync_all()?;
    }
    Ok(())
}
impl DurableRpcJournal {
    pub(super) fn open(root: &Path) -> io::Result<Arc<Self>> {
        let directory = match std::env::var_os("BICDB_TEST_CLI_RPC_EVIDENCE_ROOT") {
            Some(parent) => PathBuf::from(parent).join(format!("client-{}", Uuid::now_v7())),
            None => root.join("client-rpc-evidence"),
        };
        prepare_journal_directory(&directory, root)?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(&directory, fs::Permissions::from_mode(0o700))?;
        }
        let path = directory.join("journal.jsonl");
        let file = fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            file.set_permissions(fs::Permissions::from_mode(0o600))?;
        }
        let journal = Arc::new(Self {
            path,
            state: Mutex::new(State {
                file,
                records: 0,
                bytes: 0,
                active: None,
                failed: false,
                closed: false,
            }),
        });
        {
            let mut state = journal
                .state
                .lock()
                .map_err(|_| invalid("CLI RPC journal poisoned"))?;
            append(
                &mut state,
                serde_json::json!({"kind":"journal_started","source":std::env::var("BICDB_TEST_SOURCE_SHA").ok(),"test":std::thread::current().name(),"fixture_root":root,"max_records":MAX_RECORDS,"max_bytes":MAX_BYTES,"scope":"actual public EventClient and explicit replica transport only; no global host/peer-wire coverage","bootstrap_raw_frame_covered":false,"no_protocol_or_serving_authority":true}),
            )?;
        }
        sync_journal_directory_chain(&directory)?;
        eprintln!(
            "NATIVE_CLI_RPC_JOURNAL path={} source={:?} scope=actual_client_only",
            journal.path.display(),
            std::env::var("BICDB_TEST_SOURCE_SHA").ok()
        );
        Ok(journal)
    }
    pub(super) fn finish_case(&self, scope: &str) -> io::Result<()> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| invalid("CLI RPC journal poisoned"))?;
        if state.active.is_some() {
            return Err(invalid(
                "cannot finish journal with active logical operation",
            ));
        }
        append(
            &mut state,
            serde_json::json!({"kind":"fixture_case_finished","scope":scope,"trace_fenced":false,"bootstrap_raw_frame_covered":false,"no_global_wire_or_healthy_postjoin_claim":true}),
        )?;
        state.closed = true;
        eprintln!(
            "NATIVE_CLI_RPC_JOURNAL_COMPLETE path={} records={} bytes={} scope={scope}",
            self.path.display(),
            state.records,
            state.bytes
        );
        Ok(())
    }
    pub(super) fn observer(
        self: &Arc<Self>,
        name: &'static str,
    ) -> Arc<dyn ClusterRpcEvidenceObserver> {
        Arc::new(BoundObserver {
            journal: self.clone(),
            id: Uuid::now_v7(),
            name,
        })
    }
    pub(super) fn begin(self: &Arc<Self>, request: Value) -> io::Result<LogicalCall> {
        let id = Uuid::now_v7();
        let mut state = self
            .state
            .lock()
            .map_err(|_| invalid("CLI RPC journal poisoned"))?;
        if state.active.is_some() {
            return Err(invalid(
                "overlapping fixture logical calls cannot relabel RPCs",
            ));
        }
        append(
            &mut state,
            serde_json::json!({"kind":"logical_request_intent","diagnostic_operation_id":id,"request":request,"context_is_protocol_business_id":false}),
        )?;
        state.active = Some(Context {
            id,
            request,
            may_have_dispatched: false,
        });
        Ok(LogicalCall {
            journal: self.clone(),
            id,
            finished: false,
        })
    }
}
impl ClusterRpcEvidenceObserver for BoundObserver {
    fn record(&self, evidence: &ClusterRpcEvidence) -> io::Result<()> {
        let mut state = self
            .journal
            .state
            .lock()
            .map_err(|_| invalid("CLI RPC journal poisoned"))?;
        let context = state.active.as_mut().map(|active| {
            active.may_have_dispatched |= evidence.may_have_dispatched;
            serde_json::json!({"diagnostic_operation_id":active.id,"request":active.request})
        });
        append(
            &mut state,
            serde_json::json!({"kind":"actual_rpc_phase","observer_instance_id":self.id,"observer_name":self.name,"logical_context":context,"actual":evidence}),
        )
    }
}
impl LogicalCall {
    pub(super) fn finish(mut self, outcome: Value) -> io::Result<()> {
        let mut state = self
            .journal
            .state
            .lock()
            .map_err(|_| invalid("CLI RPC journal poisoned"))?;
        let active = state
            .active
            .as_ref()
            .ok_or_else(|| invalid("logical context ended before synchronous retries completed"))?;
        if active.id != self.id {
            return Err(invalid("logical context changed"));
        }
        let dispatched = active.may_have_dispatched;
        append(
            &mut state,
            serde_json::json!({"kind":"logical_public_reply","diagnostic_operation_id":self.id,"any_actual_rpc_may_have_dispatched":dispatched,"outcome":outcome,"durable_outcome_inferred_from_error":false}),
        )?;
        state.active = None;
        self.finished = true;
        Ok(())
    }
    pub(super) fn append(self, result: &bicdb_core::Result<bool>) -> io::Result<()> {
        self.finish(match result { Ok(inserted)=>serde_json::json!({"classification":"USER_ACK","inserted":inserted}),Err(error)=>serde_json::json!({"classification":"NO_USER_ACK_DURABLE_OUTCOME_UNKNOWN","error_chain":error_chain(error)}) })
    }
    pub(super) fn strong(self, result: &bicdb_core::Result<Option<Value>>) -> io::Result<()> {
        self.finish(match result { Ok(Some(value))=>serde_json::json!({"classification":"STRONG_SUCCESS","presence":"present","value":value}),Ok(None)=>serde_json::json!({"classification":"STRONG_SUCCESS","presence":"absent"}),Err(error)=>serde_json::json!({"classification":"NO_SUCCESSFUL_READ_NO_ABSENCE","error_chain":error_chain(error)}) })
    }
    pub(super) fn bootstrap(
        self,
        result: &bicdb_core::Result<bicdb_core::ClusterBootstrapSnapshot>,
    ) -> io::Result<()> {
        self.finish(match result { Ok(snapshot)=>serde_json::json!({"classification":"ACTUAL_AUTHENTICATED_BOOTSTRAP_DATA_ONLY","bootstrap_raw_frame_covered":false,"snapshot":snapshot}),Err(error)=>serde_json::json!({"classification":"BOOTSTRAP_ERROR","bootstrap_raw_frame_covered":false,"error_chain":error_chain(error)}) })
    }
    pub(super) fn receiver(
        self,
        result: &bicdb_core::Result<bicdb_core::ClusterEventResponse>,
    ) -> io::Result<()> {
        self.finish(match result { Ok(response)=>serde_json::json!({"classification":"ACTUAL_RECEIVER_RESPONSE","response":response}),Err(error)=>serde_json::json!({"classification":"ACTUAL_RECEIVER_REFUSAL","error_chain":error_chain(error)}) })
    }
}
impl Drop for LogicalCall {
    fn drop(&mut self) {
        if self.finished {
            return;
        }
        if let Ok(mut state) = self.journal.state.lock() {
            if state
                .active
                .as_ref()
                .is_some_and(|active| active.id == self.id)
            {
                let result = append(
                    &mut state,
                    serde_json::json!({"kind":"logical_call_interrupted","diagnostic_operation_id":self.id,"classification":"NO_COMPLETE_SUPPLEMENTAL_REPLY_RECORD","unknown_not_rollback":true,"primary_external_ack_ledger_not_reclassified":true}),
                );
                state.active = None;
                if let Err(error) = result {
                    eprintln!(
                        "NATIVE_CLI_RPC_EVIDENCE_PARTIAL error={error} path={}",
                        self.journal.path.display()
                    );
                }
            }
        }
    }
}
pub(super) fn payload_sha256(value: &Value) -> String {
    hex::encode(Sha256::digest(serde_json::to_vec(value).unwrap()))
}
