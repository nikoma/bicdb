use super::external::External;
use bicdb_core::{ClusterRpcEvidence, ClusterRpcEvidenceObserver};
use serde::Serialize;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::fs::{File, OpenOptions};
use std::io::{self, Write};
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::sync::{MutexGuard, OnceLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use uuid::Uuid;

const MAX_JOURNAL_BYTES: u64 = 2 * 1024 * 1024 * 1024;

#[derive(Serialize)]
struct Row<'a, T> {
    format: u32,
    run_id: Uuid,
    sequence: u64,
    kind: &'a str,
    previous_record_sha256: &'a Option<String>,
    unix_ns: u128,
    monotonic_elapsed_ns: u128,
    data: T,
}

struct State {
    file: File,
    sequence: u64,
    bytes: u64,
    previous_sha256: Option<String>,
    failed: bool,
    external: Option<External>,
}

pub struct Journal {
    state: Mutex<State>,
    started: Instant,
    run: Uuid,
    external_timeout: OnceLock<Duration>,
    fenced: AtomicBool,
}

fn lock_before<T>(mutex: &Mutex<T>, end: Option<Instant>) -> io::Result<MutexGuard<'_, T>> {
    if let Some(end) = end {
        loop {
            check_end(Some(end))?;
            match mutex.try_lock() {
                Ok(guard) => return Ok(guard),
                Err(std::sync::TryLockError::Poisoned(_)) => {
                    return Err(io::Error::other("ledger mutex poisoned"))
                }
                Err(std::sync::TryLockError::WouldBlock) => {
                    std::thread::sleep(Duration::from_millis(1))
                }
            }
        }
    }
    mutex
        .lock()
        .map_err(|_| io::Error::other("ledger mutex poisoned"))
}
fn check_end(end: Option<Instant>) -> io::Result<()> {
    if end.is_some_and(|end| Instant::now() >= end) {
        return Err(io::Error::new(
            io::ErrorKind::TimedOut,
            "original external evidence deadline expired",
        ));
    }
    Ok(())
}

impl Journal {
    pub fn create(directory: &Path, run: Uuid) -> io::Result<Arc<Self>> {
        let mut directory_builder = std::fs::DirBuilder::new();
        #[cfg(unix)]
        {
            use std::os::unix::fs::DirBuilderExt;
            directory_builder.mode(0o700);
        }
        directory_builder.create(directory)?;
        let parent = directory
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
            .unwrap_or_else(|| Path::new("."));
        File::open(parent)?.sync_all()?;
        let mut options = OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let file = options.open(directory.join("journal.jsonl"))?;
        file.sync_all()?;
        File::open(directory)?.sync_all()?;
        Ok(Arc::new(Self {
            state: Mutex::new(State {
                file,
                sequence: 0,
                bytes: 0,
                previous_sha256: None,
                failed: false,
                external: None,
            }),
            started: Instant::now(),
            run,
            external_timeout: OnceLock::new(),
            fenced: AtomicBool::new(false),
        }))
    }

    pub fn attach_external(&self, external: External) -> io::Result<()> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| io::Error::other("journal mutex poisoned"))?;
        if state.sequence != 0 || state.external.is_some() {
            return Err(io::Error::other(
                "external sink must attach once before first row",
            ));
        }
        self.external_timeout
            .set(external.timeout())
            .map_err(|_| io::Error::other("external sink already attached"))?;
        state.external = Some(external);
        Ok(())
    }
    fn deadline(&self) -> Option<Instant> {
        self.external_timeout
            .get()
            .map(|timeout| Instant::now() + *timeout)
    }
    pub fn external_description(&self) -> io::Result<Value> {
        let state = lock_before(&self.state, self.deadline())?;
        Ok(state
            .external
            .as_ref()
            .map(External::description)
            .unwrap_or(json!({"mode":"local_only","physical_use_blocked":true})))
    }

    /// A receipt is returned only AFTER the complete line's fsync returns.
    /// A visible trailing line alone cannot prove that this method returned.
    pub fn append(&self, kind: &str, data: impl Serialize) -> io::Result<Value> {
        self.append_before(kind, data, self.deadline())
    }
    fn append_before(
        &self,
        kind: &str,
        data: impl Serialize,
        end: Option<Instant>,
    ) -> io::Result<Value> {
        let result = self.append_inner(kind, data, end);
        if result.is_err() {
            self.fenced.store(true, Ordering::Release);
        }
        result
    }
    fn append_inner(
        &self,
        kind: &str,
        data: impl Serialize,
        end: Option<Instant>,
    ) -> io::Result<Value> {
        let mut state = lock_before(&self.state, end)?;
        if state.failed || self.fenced.load(Ordering::Acquire) {
            return Err(io::Error::other("journal fenced after persistence failure"));
        }
        let result = (|| {
            let sequence = state.sequence + 1;
            let row = Row {
                format: 1,
                run_id: self.run,
                sequence,
                kind,
                previous_record_sha256: &state.previous_sha256,
                unix_ns: SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .map_err(io::Error::other)?
                    .as_nanos(),
                monotonic_elapsed_ns: self.started.elapsed().as_nanos(),
                data,
            };
            let bytes = serde_json::to_vec(&row).map_err(io::Error::other)?;
            drop(row);
            let length = u64::try_from(bytes.len()).map_err(io::Error::other)? + 1;
            if length > MAX_JOURNAL_BYTES.saturating_sub(state.bytes) {
                return Err(io::Error::other("journal quota exhausted; no truncation"));
            }
            let hash = hex::encode(Sha256::digest(&bytes));
            check_end(end)?;
            state.file.write_all(&bytes)?;
            state.file.write_all(b"\n")?;
            state.file.sync_all()?;
            check_end(end)?;
            let previous = state.previous_sha256.clone();
            if let Some(sink) = state.external.as_mut() {
                let mut line = bytes;
                line.push(b'\n');
                sink.row(
                    &line,
                    sequence,
                    &previous,
                    end.expect("attached external timeout"),
                )?;
            }
            check_end(end)?;
            if self.fenced.load(Ordering::Acquire) {
                return Err(io::Error::other(
                    "another evidence callback fenced producer",
                ));
            }
            state.sequence = sequence;
            state.bytes += length;
            state.previous_sha256 = Some(hash.clone());
            Ok(json!({"sequence":sequence,"record_sha256":hash}))
        })();
        if result.is_err() {
            state.failed = true;
        }
        result
    }
}

#[derive(Default)]
struct Attribution {
    active: Option<String>,
    rpc_owners: BTreeMap<Uuid, String>,
}

pub struct Observer {
    journal: Arc<Journal>,
    attribution: Mutex<Attribution>,
}

impl Observer {
    pub fn new(journal: Arc<Journal>) -> Arc<Self> {
        Arc::new(Self {
            journal,
            attribution: Mutex::new(Attribution::default()),
        })
    }

    pub fn set_active(&self, operation: Option<&str>) -> io::Result<()> {
        let mut state = lock_before(&self.attribution, self.journal.deadline())?;
        state.active = operation.map(str::to_owned);
        Ok(())
    }
}

impl ClusterRpcEvidenceObserver for Observer {
    fn record(&self, evidence: &ClusterRpcEvidence) -> io::Result<()> {
        let end = self.journal.deadline();
        let owner = {
            let mut state = lock_before(&self.attribution, end)?;
            if evidence.phase == "invoked" {
                let owner = state
                    .active
                    .clone()
                    .ok_or_else(|| io::Error::other("RPC invoked outside logical operation"))?;
                if state.rpc_owners.contains_key(&evidence.attempt_id) {
                    return Err(io::Error::other("duplicate RPC invocation identity"));
                }
                if state.rpc_owners.len() >= 65_536 {
                    return Err(io::Error::other("RPC attribution quota; no eviction"));
                }
                state.rpc_owners.insert(evidence.attempt_id, owner);
            }
            state
                .rpc_owners
                .get(&evidence.attempt_id)
                .cloned()
                .ok_or_else(|| io::Error::other("RPC phase lacks original invocation"))?
        };
        // Preserve every SDK field and raw frame. No routing, reply production,
        // client reentry, eviction, truncation or failed-observer replacement.
        #[derive(Serialize)]
        struct RpcData<'a> {
            logical_id: &'a str,
            actual_sdk_evidence: &'a ClusterRpcEvidence,
        }
        self.journal.append_before(
            "sdk_rpc",
            RpcData {
                logical_id: &owner,
                actual_sdk_evidence: evidence,
            },
            end,
        )?;
        Ok(())
    }
}
