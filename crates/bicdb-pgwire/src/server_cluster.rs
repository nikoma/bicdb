//! Split out of the parent module to keep files digestible; behavior
//! unchanged. Items are re-exported from the parent via `pub(crate) use`.
use super::*;
#[allow(unused_imports)]
use crate::*;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct ClusterDatabaseEntry {
    pub(crate) directory: String,
    pub(crate) owner: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct ClusterManifest {
    pub(crate) version: u32,
    pub(crate) default_database: String,
    pub(crate) databases: BTreeMap<String, ClusterDatabaseEntry>,
}

#[derive(Debug)]
pub struct PgWireCluster {
    pub(crate) root: PathBuf,
    pub(crate) config: PgWireConfig,
    /// One admission state for every database hosted by this node. Per-
    /// database governors would multiply the declared node envelope whenever
    /// another database is opened.
    pub(crate) resource_governor: ResourceGovernor,
    pub(crate) manifest: Mutex<ClusterManifest>,
    pub(crate) servers: Mutex<HashMap<String, Arc<PgWireServer>>>,
    pub(crate) database_background_enabled: AtomicBool,
}

/// Which connection cap refused an admission, and the value it was set to.
#[derive(Clone, Copy, Debug)]
pub(crate) enum ConnectionLimit {
    /// `--max-connections-per-ip`: one client host has too many connections.
    PerSourceIp { limit: usize },
    /// `--max-connections`: the server as a whole is full.
    Server { limit: usize },
}

impl ConnectionLimit {
    /// Operator-facing sentence naming the knob that actually applies.
    pub(crate) fn operator_hint(&self, rejected: u64) -> String {
        match self {
            Self::PerSourceIp { limit } => format!(
                "bicdb server: rejecting connections at --max-connections-per-ip={limit} \
                 ({rejected} rejected since start). One client host has reached its own \
                 cap while the server as a whole may be idle; raise \
                 --max-connections-per-ip or spread clients across hosts. Clients see \
                 FATAL 53300."
            ),
            Self::Server { limit } => format!(
                "bicdb server: rejecting connections at --max-connections={limit} \
                 ({rejected} rejected since start). Raise --max-connections or pool \
                 client connections; clients see FATAL 53300."
            ),
        }
    }

    pub(crate) fn reason(&self) -> &'static str {
        match self {
            Self::PerSourceIp { .. } => "max_connections_per_ip",
            Self::Server { .. } => "max_connections",
        }
    }
}

/// RAII guard for an in-flight shared-path write execution; releases the slot
/// back to the server's bound on drop.
pub(crate) struct SharedWriteSlot {
    pub(crate) server: Arc<PgWireServer>,
}

impl Drop for SharedWriteSlot {
    fn drop(&mut self) {
        self.server
            .active_shared_writes
            .fetch_sub(1, Ordering::Relaxed);
    }
}

/// Server-wide LISTEN/NOTIFY dispatch. Channels registered per connection;
/// pending notifications queue per connection and are flushed to the socket
/// by the connection loop (asynchronously while idle, and before each
/// ReadyForQuery). Broker queue publishes are fanned in automatically on the
/// `bicdb_broker__<queue>` channel via an event-stream prefix subscription.
#[derive(Debug, Default)]
pub(crate) struct NotificationBus {
    pub(crate) channels: HashMap<String, HashSet<u64>>,
    pub(crate) listening: HashMap<u64, HashSet<String>>,
    pub(crate) pending: HashMap<u64, std::collections::VecDeque<(String, String, i32)>>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub(crate) enum AdvisoryLockKey {
    BigInt(i64),
    IntPair(i32, i32),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum AdvisoryLockMode {
    Exclusive,
    Shared,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum AdvisoryLockScope {
    Session,
    Transaction,
}

#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct AdvisoryHoldCounts {
    pub(crate) session: u32,
    pub(crate) transaction: u32,
}

impl AdvisoryHoldCounts {
    pub(crate) fn increment(&mut self, scope: AdvisoryLockScope) {
        match scope {
            AdvisoryLockScope::Session => self.session = self.session.saturating_add(1),
            AdvisoryLockScope::Transaction => self.transaction = self.transaction.saturating_add(1),
        }
    }

    pub(crate) fn is_empty(self) -> bool {
        self.session == 0 && self.transaction == 0
    }
}

#[derive(Debug, Default)]
pub(crate) struct AdvisoryLockEntry {
    pub(crate) exclusive: HashMap<u64, AdvisoryHoldCounts>,
    pub(crate) shared: HashMap<u64, AdvisoryHoldCounts>,
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct AdvisoryWait {
    pub(crate) key: AdvisoryLockKey,
    pub(crate) mode: AdvisoryLockMode,
    pub(crate) started_at: i64,
}

#[derive(Debug, Default)]
pub(crate) struct AdvisoryLockState {
    pub(crate) locks: HashMap<AdvisoryLockKey, AdvisoryLockEntry>,
    pub(crate) waiting: HashMap<u64, AdvisoryWait>,
}

#[derive(Debug, Default)]
pub(crate) struct AdvisoryLockManager {
    pub(crate) state: Mutex<AdvisoryLockState>,
    pub(crate) changed: Condvar,
}

impl AdvisoryLockManager {
    pub(crate) fn acquire(
        &self,
        connection_id: u64,
        key: AdvisoryLockKey,
        mode: AdvisoryLockMode,
        scope: AdvisoryLockScope,
        try_only: bool,
        cancellation: &CancellationToken,
    ) -> bicdb_sql::Result<bool> {
        let mut state = self.state.lock().unwrap();
        loop {
            if Self::can_acquire(&state, connection_id, key, mode) {
                state.waiting.remove(&connection_id);
                let entry = state.locks.entry(key).or_default();
                let holders = match mode {
                    AdvisoryLockMode::Exclusive => &mut entry.exclusive,
                    AdvisoryLockMode::Shared => &mut entry.shared,
                };
                holders.entry(connection_id).or_default().increment(scope);
                return Ok(true);
            }
            if try_only {
                return Ok(false);
            }
            state.waiting.entry(connection_id).or_insert(AdvisoryWait {
                key,
                mode,
                started_at: unix_timestamp(),
            });
            if Self::wait_cycle_from(&state, connection_id, connection_id, &mut HashSet::new()) {
                state.waiting.remove(&connection_id);
                return Err(SqlError::ConstraintViolation {
                    sqlstate: "40P01",
                    message: "deadlock detected while waiting for advisory lock".to_string(),
                    table: None,
                    column: None,
                    constraint: None,
                });
            }
            if let Err(error) = cancellation.check() {
                state.waiting.remove(&connection_id);
                return Err(SqlError::from(error));
            }
            let (next, _) = self
                .changed
                .wait_timeout(state, Duration::from_millis(25))
                .unwrap();
            state = next;
        }
    }

    pub(crate) fn unlock(
        &self,
        connection_id: u64,
        key: AdvisoryLockKey,
        mode: AdvisoryLockMode,
    ) -> bool {
        let mut state = self.state.lock().unwrap();
        let released = state.locks.get_mut(&key).is_some_and(|entry| {
            let holders = match mode {
                AdvisoryLockMode::Exclusive => &mut entry.exclusive,
                AdvisoryLockMode::Shared => &mut entry.shared,
            };
            let Some(counts) = holders.get_mut(&connection_id) else {
                return false;
            };
            if counts.session == 0 {
                return false;
            }
            counts.session -= 1;
            if counts.is_empty() {
                holders.remove(&connection_id);
            }
            true
        });
        Self::prune_key(&mut state, key);
        if released {
            self.changed.notify_all();
        }
        released
    }

    pub(crate) fn release_transaction(&self, connection_id: u64) {
        self.release_matching(connection_id, false);
    }

    pub(crate) fn unlock_all_session(&self, connection_id: u64) {
        self.release_matching(connection_id, true);
    }

    pub(crate) fn disconnect(&self, connection_id: u64) {
        let mut state = self.state.lock().unwrap();
        state.waiting.remove(&connection_id);
        state.locks.retain(|_, entry| {
            entry.exclusive.remove(&connection_id);
            entry.shared.remove(&connection_id);
            !entry.exclusive.is_empty() || !entry.shared.is_empty()
        });
        self.changed.notify_all();
    }

    pub(crate) fn release_matching(&self, connection_id: u64, session: bool) {
        let mut state = self.state.lock().unwrap();
        state.locks.retain(|_, entry| {
            for holders in [&mut entry.exclusive, &mut entry.shared] {
                if let Some(counts) = holders.get_mut(&connection_id) {
                    if session {
                        counts.session = 0;
                    } else {
                        counts.transaction = 0;
                    }
                    if counts.is_empty() {
                        holders.remove(&connection_id);
                    }
                }
            }
            !entry.exclusive.is_empty() || !entry.shared.is_empty()
        });
        self.changed.notify_all();
    }

    pub(crate) fn can_acquire(
        state: &AdvisoryLockState,
        connection_id: u64,
        key: AdvisoryLockKey,
        mode: AdvisoryLockMode,
    ) -> bool {
        let Some(entry) = state.locks.get(&key) else {
            return true;
        };
        let no_other_exclusive = entry
            .exclusive
            .keys()
            .all(|holder| *holder == connection_id);
        match mode {
            AdvisoryLockMode::Shared => no_other_exclusive,
            AdvisoryLockMode::Exclusive => {
                no_other_exclusive && entry.shared.keys().all(|holder| *holder == connection_id)
            }
        }
    }

    pub(crate) fn blockers(
        state: &AdvisoryLockState,
        connection_id: u64,
        wait: AdvisoryWait,
    ) -> Vec<u64> {
        let Some(entry) = state.locks.get(&wait.key) else {
            return Vec::new();
        };
        let mut blockers = entry
            .exclusive
            .keys()
            .filter(|holder| **holder != connection_id)
            .copied()
            .collect::<Vec<_>>();
        if wait.mode == AdvisoryLockMode::Exclusive {
            blockers.extend(
                entry
                    .shared
                    .keys()
                    .filter(|holder| **holder != connection_id)
                    .copied(),
            );
        }
        blockers
    }

    pub(crate) fn wait_cycle_from(
        state: &AdvisoryLockState,
        current: u64,
        target: u64,
        visited: &mut HashSet<u64>,
    ) -> bool {
        if !visited.insert(current) {
            return false;
        }
        let Some(wait) = state.waiting.get(&current).copied() else {
            return false;
        };
        Self::blockers(state, current, wait)
            .into_iter()
            .any(|blocker| {
                blocker == target || Self::wait_cycle_from(state, blocker, target, visited)
            })
    }

    pub(crate) fn prune_key(state: &mut AdvisoryLockState, key: AdvisoryLockKey) {
        if state
            .locks
            .get(&key)
            .is_some_and(|entry| entry.exclusive.is_empty() && entry.shared.is_empty())
        {
            state.locks.remove(&key);
        }
    }

    pub(crate) fn snapshots(&self) -> Vec<AdvisoryLockSnapshot> {
        let state = self.state.lock().unwrap();
        let mut snapshots = Vec::new();
        for (key, entry) in &state.locks {
            for (mode, holders) in [
                (AdvisoryLockMode::Exclusive, &entry.exclusive),
                (AdvisoryLockMode::Shared, &entry.shared),
            ] {
                snapshots.extend(holders.keys().map(|connection_id| AdvisoryLockSnapshot {
                    connection_id: *connection_id,
                    key: *key,
                    mode,
                    granted: true,
                    wait_started_at: None,
                }));
            }
        }
        snapshots.extend(
            state
                .waiting
                .iter()
                .map(|(connection_id, wait)| AdvisoryLockSnapshot {
                    connection_id: *connection_id,
                    key: wait.key,
                    mode: wait.mode,
                    granted: false,
                    wait_started_at: Some(wait.started_at),
                }),
        );
        snapshots.sort_by_key(|snapshot| {
            (
                snapshot.connection_id,
                snapshot.key,
                snapshot.mode as u8,
                !snapshot.granted,
            )
        });
        snapshots
    }
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct AdvisoryLockSnapshot {
    pub(crate) connection_id: u64,
    pub(crate) key: AdvisoryLockKey,
    pub(crate) mode: AdvisoryLockMode,
    pub(crate) granted: bool,
    pub(crate) wait_started_at: Option<i64>,
}

#[derive(Debug)]
pub(crate) struct PgWireSqlRuntime {
    pub(crate) connection_id: u64,
    pub(crate) in_transaction: bool,
    pub(crate) advisory_locks: Arc<AdvisoryLockManager>,
}

impl SqlSessionRuntime for PgWireSqlRuntime {
    fn backend_pid(&self) -> i32 {
        i32::try_from(self.connection_id).unwrap_or(i32::MAX)
    }

    fn execute_advisory_lock(
        &self,
        name: &str,
        args: &[SqlValue],
        cancellation: &CancellationToken,
    ) -> bicdb_sql::Result<Option<SqlValue>> {
        let operation = match name {
            "pg_advisory_lock" => AdvisoryLockOperation::Acquire {
                mode: AdvisoryLockMode::Exclusive,
                scope: AdvisoryLockScope::Session,
                try_only: false,
            },
            "pg_advisory_lock_shared" => AdvisoryLockOperation::Acquire {
                mode: AdvisoryLockMode::Shared,
                scope: AdvisoryLockScope::Session,
                try_only: false,
            },
            "pg_try_advisory_lock" => AdvisoryLockOperation::Acquire {
                mode: AdvisoryLockMode::Exclusive,
                scope: AdvisoryLockScope::Session,
                try_only: true,
            },
            "pg_try_advisory_lock_shared" => AdvisoryLockOperation::Acquire {
                mode: AdvisoryLockMode::Shared,
                scope: AdvisoryLockScope::Session,
                try_only: true,
            },
            "pg_advisory_xact_lock" => AdvisoryLockOperation::Acquire {
                mode: AdvisoryLockMode::Exclusive,
                scope: AdvisoryLockScope::Transaction,
                try_only: false,
            },
            "pg_advisory_xact_lock_shared" => AdvisoryLockOperation::Acquire {
                mode: AdvisoryLockMode::Shared,
                scope: AdvisoryLockScope::Transaction,
                try_only: false,
            },
            "pg_try_advisory_xact_lock" => AdvisoryLockOperation::Acquire {
                mode: AdvisoryLockMode::Exclusive,
                scope: AdvisoryLockScope::Transaction,
                try_only: true,
            },
            "pg_try_advisory_xact_lock_shared" => AdvisoryLockOperation::Acquire {
                mode: AdvisoryLockMode::Shared,
                scope: AdvisoryLockScope::Transaction,
                try_only: true,
            },
            "pg_advisory_unlock" => AdvisoryLockOperation::Unlock {
                mode: AdvisoryLockMode::Exclusive,
            },
            "pg_advisory_unlock_shared" => AdvisoryLockOperation::Unlock {
                mode: AdvisoryLockMode::Shared,
            },
            "pg_advisory_unlock_all" => AdvisoryLockOperation::UnlockAll,
            _ => return Ok(None),
        };
        let key = advisory_lock_key_from_values(name, operation, args)?;
        let value = match (operation, key) {
            (AdvisoryLockOperation::UnlockAll, _) => {
                self.advisory_locks.unlock_all_session(self.connection_id);
                SqlValue::Null
            }
            (_, None) => SqlValue::Null,
            (
                AdvisoryLockOperation::Acquire {
                    mode,
                    scope,
                    try_only,
                },
                Some(key),
            ) => {
                let acquired = self.advisory_locks.acquire(
                    self.connection_id,
                    key,
                    mode,
                    scope,
                    try_only,
                    cancellation,
                )?;
                if scope == AdvisoryLockScope::Transaction && !self.in_transaction {
                    self.advisory_locks.release_transaction(self.connection_id);
                }
                if try_only {
                    SqlValue::Bool(acquired)
                } else {
                    SqlValue::Null
                }
            }
            (AdvisoryLockOperation::Unlock { mode }, Some(key)) => {
                SqlValue::Bool(self.advisory_locks.unlock(self.connection_id, key, mode))
            }
        };
        Ok(Some(value))
    }

    fn advisory_lock_rows(&self, database_oid: i64) -> Vec<BTreeMap<String, SqlValue>> {
        self.advisory_locks
            .snapshots()
            .into_iter()
            .map(|snapshot| {
                let (classid, objid, objsubid) = match snapshot.key {
                    AdvisoryLockKey::BigInt(key) => {
                        let bits = key as u64;
                        ((bits >> 32) as u32, bits as u32, 1_i64)
                    }
                    AdvisoryLockKey::IntPair(classid, objid) => {
                        (classid as u32, objid as u32, 2_i64)
                    }
                };
                [
                    (
                        "locktype".to_string(),
                        SqlValue::String("advisory".to_string()),
                    ),
                    ("database".to_string(), SqlValue::Int(database_oid)),
                    ("relation".to_string(), SqlValue::Null),
                    ("page".to_string(), SqlValue::Null),
                    ("tuple".to_string(), SqlValue::Null),
                    ("virtualxid".to_string(), SqlValue::Null),
                    ("transactionid".to_string(), SqlValue::Null),
                    ("classid".to_string(), SqlValue::Int(i64::from(classid))),
                    ("objid".to_string(), SqlValue::Int(i64::from(objid))),
                    ("objsubid".to_string(), SqlValue::Int(objsubid)),
                    (
                        "virtualtransaction".to_string(),
                        SqlValue::String(format!("0/{}", snapshot.connection_id + 1)),
                    ),
                    (
                        "pid".to_string(),
                        SqlValue::Int(i64::from(
                            i32::try_from(snapshot.connection_id).unwrap_or(i32::MAX),
                        )),
                    ),
                    (
                        "mode".to_string(),
                        SqlValue::String(
                            match snapshot.mode {
                                AdvisoryLockMode::Exclusive => "ExclusiveLock",
                                AdvisoryLockMode::Shared => "ShareLock",
                            }
                            .to_string(),
                        ),
                    ),
                    ("granted".to_string(), SqlValue::Bool(snapshot.granted)),
                    ("fastpath".to_string(), SqlValue::Bool(false)),
                    (
                        "waitstart".to_string(),
                        snapshot
                            .wait_started_at
                            .map(|seconds| {
                                PgTimestamp::Finite(
                                    (seconds - 946_684_800).saturating_mul(1_000_000),
                                )
                                .to_iso_text(true)
                            })
                            .map(SqlValue::String)
                            .unwrap_or(SqlValue::Null),
                    ),
                ]
                .into_iter()
                .collect()
            })
            .collect()
    }
}

pub(crate) fn advisory_lock_key_from_values(
    name: &str,
    operation: AdvisoryLockOperation,
    args: &[SqlValue],
) -> bicdb_sql::Result<Option<AdvisoryLockKey>> {
    if matches!(operation, AdvisoryLockOperation::UnlockAll) {
        if args.is_empty() {
            return Ok(None);
        }
        return Err(SqlError::InvalidSql(format!("{name} expects no arguments")));
    }
    let integer = |value: &SqlValue| match value {
        SqlValue::Null => Ok(None),
        SqlValue::Int(value) => Ok(Some(*value)),
        other => Err(SqlError::InvalidSql(format!(
            "{name} advisory lock key must be an integer, got {}",
            other.to_cell()
        ))),
    };
    match args {
        [value] => Ok(integer(value)?.map(AdvisoryLockKey::BigInt)),
        [left, right] => match (integer(left)?, integer(right)?) {
            (Some(left), Some(right)) => Ok(Some(AdvisoryLockKey::IntPair(
                i32::try_from(left).map_err(|_| {
                    SqlError::InvalidSql(format!("{name} integer key is out of range"))
                })?,
                i32::try_from(right).map_err(|_| {
                    SqlError::InvalidSql(format!("{name} integer key is out of range"))
                })?,
            ))),
            _ => Ok(None),
        },
        _ => Err(SqlError::InvalidSql(format!(
            "{name} expects one bigint or two integer arguments"
        ))),
    }
}

/// Per-connection pending-notification cap; the oldest entries drop first
/// (a LISTENing client that falls this far behind must re-poll anyway).
pub(crate) const MAX_PENDING_NOTIFICATIONS: usize = 4096;

impl NotificationBus {
    pub(crate) fn listen(&mut self, connection_id: u64, channel: &str) {
        self.channels
            .entry(channel.to_string())
            .or_default()
            .insert(connection_id);
        self.listening
            .entry(connection_id)
            .or_default()
            .insert(channel.to_string());
    }

    pub(crate) fn unlisten(&mut self, connection_id: u64, channel: Option<&str>) {
        match channel {
            Some(channel) => {
                if let Some(listeners) = self.channels.get_mut(channel) {
                    listeners.remove(&connection_id);
                    if listeners.is_empty() {
                        self.channels.remove(channel);
                    }
                }
                if let Some(channels) = self.listening.get_mut(&connection_id) {
                    channels.remove(channel);
                }
            }
            None => {
                if let Some(channels) = self.listening.remove(&connection_id) {
                    for channel in channels {
                        if let Some(listeners) = self.channels.get_mut(&channel) {
                            listeners.remove(&connection_id);
                            if listeners.is_empty() {
                                self.channels.remove(&channel);
                            }
                        }
                    }
                }
            }
        }
    }

    pub(crate) fn notify(&mut self, channel: &str, payload: &str, sender_pid: i32) {
        let Some(listeners) = self.channels.get(channel) else {
            return;
        };
        for connection_id in listeners.clone() {
            let queue = self.pending.entry(connection_id).or_default();
            if queue.len() >= MAX_PENDING_NOTIFICATIONS {
                queue.pop_front();
            }
            queue.push_back((channel.to_string(), payload.to_string(), sender_pid));
        }
    }

    pub(crate) fn drain(&mut self, connection_id: u64) -> Vec<(String, String, i32)> {
        self.pending
            .get_mut(&connection_id)
            .map(|queue| queue.drain(..).collect())
            .unwrap_or_default()
    }

    pub(crate) fn disconnect(&mut self, connection_id: u64) {
        self.unlisten(connection_id, None);
        self.pending.remove(&connection_id);
    }
}

impl PgWireServer {
    pub fn open(path: impl AsRef<Path>, config: PgWireConfig) -> Result<Arc<Self>> {
        let db = BicDb::open_with_config(
            path.as_ref(),
            database_config_for_server(path.as_ref(), &config)?,
        )?;
        Self::open_with_context(
            path.as_ref(),
            path.as_ref(),
            config,
            Arc::new(RwLock::new(db)),
            None,
            None,
            None,
        )
    }

    /// Serves an already-open database shared with an embedding process
    /// (e.g. a broker exposing the same data over multiple protocols). The
    /// caller keeps using its clone of the `Arc` for its own operations.
    pub fn open_with_shared(
        path: impl AsRef<Path>,
        config: PgWireConfig,
        db: Arc<RwLock<BicDb>>,
    ) -> Result<Arc<Self>> {
        Self::open_with_context(path.as_ref(), path.as_ref(), config, db, None, None, None)
    }

    /// Open a pgwire gateway with a generation-cached distributed point
    /// router. This does not change SQL semantics by itself; point-aware
    /// session/executor paths call [`Self::route_cluster_point`] and remote
    /// owners call [`Self::validate_cluster_point`] before data access.
    pub fn open_with_cluster_router(
        path: impl AsRef<Path>,
        config: PgWireConfig,
        router: ClusterRequestRouter,
    ) -> Result<Arc<Self>> {
        let db = BicDb::open_with_config(
            path.as_ref(),
            database_config_for_server(path.as_ref(), &config)?,
        )?;
        Self::open_with_context(
            path.as_ref(),
            path.as_ref(),
            config,
            Arc::new(RwLock::new(db)),
            None,
            Some(router),
            None,
        )
    }

    pub(crate) fn open_with_context(
        path: &Path,
        auth_path: &Path,
        config: PgWireConfig,
        db: Arc<RwLock<BicDb>>,
        cluster: Option<Weak<PgWireCluster>>,
        distribution_router: Option<ClusterRequestRouter>,
        resource_governor: Option<ResourceGovernor>,
    ) -> Result<Arc<Self>> {
        validate_server_config(&config)?;
        if config.automatic_paged_read_ahead {
            if let Some(snapshot) = db.read().paged_storage_snapshot()? {
                config
                    .automatic_paged_read_ahead_limits
                    .validate(snapshot.page_size)
                    .map_err(|error| {
                        PgWireError::Server(format!(
                            "automatic paged read-ahead limits are invalid for the active page size: {error}"
                        ))
                    })?;
            }
        }
        let tls_config = load_tls_config(&config)?;
        if config.require_auth && config.auth_method == AuthMethod::ScramSha256 {
            if let Some(tls) = &tls_config {
                if let Err(error) = &tls.channel_binding {
                    match config.effective_channel_binding_policy() {
                        ChannelBindingPolicy::Require => return Err(PgWireError::Server(error.clone())),
                        ChannelBindingPolicy::Prefer => eprintln!(
                            "pgwire: SCRAM-PLUS unavailable: {error}; channel_binding=prefer permits plain SCRAM over TLS"
                        ),
                        ChannelBindingPolicy::Disable => {},
                    }
                }
            }
        }
        let distribution_router = match distribution_router {
            Some(router) => Some(router),
            None => load_local_distribution_router(path, config.fsync)?,
        };
        let resource_governor = match resource_governor {
            Some(governor) => governor,
            None => ResourceGovernor::new(config.resource_governor.clone(), cluster_now_ms())?,
        };
        let rejection_drains = Arc::new(tokio::sync::Semaphore::new(config.max_pending_accepts));
        let path = path.to_path_buf();
        // Fan broker queue publishes into the LISTEN/NOTIFY bus so clients
        // listening on `bicdb_broker__<queue>` wake without polling. The
        // handler runs inline on the appending thread: one mutex push.
        let notifications = Arc::new(Mutex::new(NotificationBus::default()));
        let tx_log = {
            let mut guard = db.write();
            ensure_primary_key_indexes(&mut guard)?;
            ensure_unique_constraint_indexes(&mut guard)?;
            let bus = Arc::clone(&notifications);
            guard
                .events_mut()
                .subscribe_prefix("queue:", move |stored| {
                    if stored.event.event_type != "QueueMessage" {
                        return;
                    }
                    let Some(queue) = stored.event.stream.strip_prefix("queue:") else {
                        return;
                    };
                    let payload = json!({
                        "queue": queue,
                        "message_id": stored.event.id.to_string(),
                    })
                    .to_string();
                    if let Ok(mut bus) = bus.lock() {
                        bus.notify(&format!("bicdb_broker__{queue}"), &payload, 0);
                    }
                });
            guard.tx_log_handle()
        };
        Ok(Arc::new(Self {
            path,
            auth_path: auth_path.to_path_buf(),
            config,
            db,
            distribution_router,
            resource_governor,
            tx_log,
            started_at: Instant::now(),
            active_connections: AtomicUsize::new(0),
            active_connections_by_ip: Mutex::new(HashMap::new()),
            rejection_drains,
            peak_active_connections: AtomicUsize::new(0),
            shared_write_credit: AtomicI64::new(32),
            shared_write_probe: AtomicU64::new(0),
            active_shared_writes: AtomicUsize::new(0),
            total_connections: AtomicU64::new(0),
            rejected_connections: AtomicU64::new(0),
            saturation_hint_millis: AtomicU64::new(0),
            active_queries: AtomicUsize::new(0),
            peak_active_queries: AtomicUsize::new(0),
            queued_queries: AtomicUsize::new(0),
            queued_queries_max: AtomicUsize::new(0),
            rejected_queries: AtomicU64::new(0),
            active_reads: AtomicUsize::new(0),
            peak_active_reads: AtomicUsize::new(0),
            queued_reads: AtomicUsize::new(0),
            queued_reads_max: AtomicUsize::new(0),
            active_writes: AtomicUsize::new(0),
            peak_active_writes: AtomicUsize::new(0),
            queued_writes: AtomicUsize::new(0),
            queued_writes_max: AtomicUsize::new(0),
            query_queue_wait_samples_ns: Mutex::new(Vec::new()),
            next_connection_id: AtomicU64::new(1),
            queries_executed: AtomicU64::new(0),
            failed_queries: AtomicU64::new(0),
            canceled_queries: AtomicU64::new(0),
            timed_out_queries: AtomicU64::new(0),
            writes_executed: AtomicU64::new(0),
            proc_neword: AtomicU64::new(0),
            proc_payment: AtomicU64::new(0),
            proc_delivery: AtomicU64::new(0),
            proc_orderstatus: AtomicU64::new(0),
            proc_stocklevel: AtomicU64::new(0),
            write_queue_depth: AtomicUsize::new(0),
            write_queue_depth_max: AtomicUsize::new(0),
            write_wait_total_ns: AtomicU64::new(0),
            write_wait_max_ns: AtomicU64::new(0),
            write_execution_total_ns: AtomicU64::new(0),
            write_execution_max_ns: AtomicU64::new(0),
            write_rejected_count: AtomicU64::new(0),
            write_timed_out_count: AtomicU64::new(0),
            db_lock_acquisitions: AtomicU64::new(0),
            db_lock_wait_total_ns: AtomicU64::new(0),
            db_lock_wait_max_ns: AtomicU64::new(0),
            db_lock_hold_total_ns: AtomicU64::new(0),
            db_lock_hold_max_ns: AtomicU64::new(0),
            db_read_lock_acquisitions: AtomicU64::new(0),
            db_read_lock_wait_total_ns: AtomicU64::new(0),
            db_read_lock_wait_max_ns: AtomicU64::new(0),
            db_read_lock_hold_total_ns: AtomicU64::new(0),
            db_read_lock_hold_max_ns: AtomicU64::new(0),
            db_write_lock_acquisitions: AtomicU64::new(0),
            db_write_lock_wait_total_ns: AtomicU64::new(0),
            db_write_lock_wait_max_ns: AtomicU64::new(0),
            db_write_lock_hold_total_ns: AtomicU64::new(0),
            db_write_lock_hold_max_ns: AtomicU64::new(0),
            rows_streamed: AtomicU64::new(0),
            bytes_streamed: AtomicU64::new(0),
            active_cursors: AtomicUsize::new(0),
            cursor_memory_bytes: AtomicUsize::new(0),
            spilled_to_disk_bytes: AtomicU64::new(0),
            last_checkpoint: Mutex::new(None),
            connections: Mutex::new(HashMap::new()),
            cancel_tokens: Mutex::new(HashMap::new()),
            last_cancel: Mutex::new(None),
            shutdown: AtomicBool::new(false),
            database_background_started: AtomicBool::new(false),
            host_background_started: AtomicBool::new(false),
            host_services: Mutex::new(Vec::new()),
            background_workers: std::sync::atomic::AtomicUsize::new(0),
            tls_config,
            notifications,
            advisory_locks: Arc::new(AdvisoryLockManager::default()),
            cluster,
        }))
    }

    pub fn config(&self) -> &PgWireConfig {
        &self.config
    }

    pub fn distribution_router(&self) -> Option<&ClusterRequestRouter> {
        self.distribution_router.as_ref()
    }

    /// Install one optional host service before server background work begins.
    pub fn install_host_service(&self, service: Arc<dyn PgWireHostService>) -> Result<()> {
        let mut services = self.host_services.lock().map_err(|_| {
            PgWireError::Server("pgwire host service registry lock poisoned".to_string())
        })?;
        if self.host_background_started.load(Ordering::SeqCst) {
            return Err(PgWireError::Server(format!(
                "cannot install host service `{}` after server startup",
                service.name()
            )));
        }
        if services
            .iter()
            .any(|existing| existing.name() == service.name())
        {
            return Err(PgWireError::Server(format!(
                "host service `{}` is already installed",
                service.name()
            )));
        }
        services.push(service);
        Ok(())
    }

    /// Install optional host services before server background work begins.
    pub fn install_host_services(
        &self,
        services: impl IntoIterator<Item = Arc<dyn PgWireHostService>>,
    ) -> Result<()> {
        for service in services {
            self.install_host_service(service)?;
        }
        Ok(())
    }

    pub fn route_cluster_point(
        &self,
        namespace: &str,
        key: &str,
        operation: PointOperation,
        requester: Option<&ClusterNodeId>,
        now_ms: u64,
    ) -> Result<RouteDecision> {
        self.distribution_router
            .as_ref()
            .ok_or_else(|| {
                PgWireError::Server("distributed routing is not configured".to_string())
            })?
            .route_point(namespace, key, operation, requester, now_ms)
            .map_err(PgWireError::from)
    }

    /// Build the single-range execution plan used by a shard-key SQL fast
    /// path. The caller can forward the statement to `targets[0]` without
    /// constructing scatter work.
    pub fn plan_cluster_point_query(
        &self,
        namespace: &str,
        key: &str,
    ) -> Result<DistributedQueryPlan> {
        let topology = self
            .distribution_router
            .as_ref()
            .ok_or_else(|| {
                PgWireError::Server("distributed routing is not configured".to_string())
            })?
            .topology()?;
        plan_point_query(&topology, namespace, key).map_err(PgWireError::from)
    }

    /// Build a bounded all-range SQL/FTS plan before any remote request is
    /// dispatched.
    pub fn plan_cluster_scatter_query(
        &self,
        limits: &DistributedQueryLimits,
    ) -> Result<DistributedQueryPlan> {
        let topology = self
            .distribution_router
            .as_ref()
            .ok_or_else(|| {
                PgWireError::Server("distributed routing is not configured".to_string())
            })?
            .topology()?;
        plan_scatter_query(&topology, limits).map_err(PgWireError::from)
    }

    /// Validate and group a SQL write before execution. Cross-range writes are
    /// rejected here unless the caller deliberately enters the separate
    /// distributed-commit protocol.
    pub fn plan_cluster_shard_local_write<Write>(
        &self,
        writes: Vec<DistributedWriteIntent<Write>>,
    ) -> Result<ShardLocalWritePlan<Write>> {
        let topology = self
            .distribution_router
            .as_ref()
            .ok_or_else(|| {
                PgWireError::Server("distributed routing is not configured".to_string())
            })?
            .topology()?;
        plan_shard_local_write(&topology, writes).map_err(PgWireError::from)
    }

    pub fn validate_cluster_point(
        &self,
        local_node: &ClusterNodeId,
        header: &RoutedRequestHeader,
        now_ms: u64,
    ) -> Result<RouteValidation> {
        self.distribution_router
            .as_ref()
            .ok_or_else(|| {
                PgWireError::Server("distributed routing is not configured".to_string())
            })?
            .validate_at_node(local_node, header, now_ms)
            .map_err(PgWireError::from)
    }

    pub fn request_shutdown(&self) {
        if let Some(cluster) = self.cluster.as_ref().and_then(Weak::upgrade) {
            cluster.request_shutdown();
        } else {
            self.shutdown.store(true, Ordering::SeqCst);
        }
    }

    pub fn is_shutdown_requested(&self) -> bool {
        self.shutdown.load(Ordering::SeqCst)
    }

    #[cfg(debug_assertions)]
    #[doc(hidden)]
    pub fn hold_write_admission_for_test(self: &Arc<Self>, duration: Duration) -> Result<()> {
        let _admission = self.try_admit_write()?;
        thread::sleep(duration);
        Ok(())
    }

    #[cfg(debug_assertions)]
    #[doc(hidden)]
    pub fn request_cancel_for_test(&self, process_id: i32, secret_key: i32) {
        self.request_cancel(process_id, secret_key);
    }

    pub fn stats_snapshot(&self) -> ServerStatsSnapshot {
        let last_cancel = self.last_cancel.lock().unwrap().clone();
        let wal_stats = self.tx_log.stats();
        let routine_exceptions = routine_exception_counts_snapshot();
        let (query_queue_wait_p50_ns, query_queue_wait_p95_ns, query_queue_wait_p99_ns) = {
            let samples = self.query_queue_wait_samples_ns.lock().unwrap();
            queue_wait_percentiles(&samples)
        };
        let db_size_bytes = database_directory_size(&self.path).unwrap_or_default();
        let ha_status = self.db.try_read().and_then(|db| db.ha_status().ok());
        let active_connections = self.active_connections.load(Ordering::SeqCst);
        ServerStatsSnapshot {
            max_connections: self.config.max_connections,
            active_connections,
            peak_active_connections: self.peak_active_connections.load(Ordering::SeqCst),
            total_connections: self.total_connections.load(Ordering::SeqCst),
            rejected_connections: self.rejected_connections.load(Ordering::SeqCst),
            max_pending_accepts: self.config.max_pending_accepts,
            max_active_queries: self.config.max_active_queries,
            max_queued_queries: self.config.max_queued_queries,
            active_queries: self.active_queries.load(Ordering::SeqCst),
            peak_active_queries: self.peak_active_queries.load(Ordering::SeqCst),
            queued_queries: self.queued_queries.load(Ordering::SeqCst),
            queued_queries_max: self.queued_queries_max.load(Ordering::SeqCst),
            rejected_queries: self.rejected_queries.load(Ordering::SeqCst),
            max_active_reads: self.config.max_active_reads,
            active_reads: self.active_reads.load(Ordering::SeqCst),
            peak_active_reads: self.peak_active_reads.load(Ordering::SeqCst),
            max_queued_reads: self.config.max_queued_reads,
            queued_reads: self.queued_reads.load(Ordering::SeqCst),
            queued_reads_max: self.queued_reads_max.load(Ordering::SeqCst),
            max_active_writes: self.config.max_active_writes,
            active_writes: self.active_writes.load(Ordering::SeqCst),
            peak_active_writes: self.peak_active_writes.load(Ordering::SeqCst),
            queued_writes: self.queued_writes.load(Ordering::SeqCst),
            queued_writes_max: self.queued_writes_max.load(Ordering::SeqCst),
            query_queue_wait_p50_ns,
            query_queue_wait_p95_ns,
            query_queue_wait_p99_ns,
            queries_executed: self.queries_executed.load(Ordering::SeqCst),
            failed_queries: self.failed_queries.load(Ordering::SeqCst),
            canceled_queries: self.canceled_queries.load(Ordering::SeqCst),
            timed_out_queries: self.timed_out_queries.load(Ordering::SeqCst),
            last_cancel_at: last_cancel.as_ref().map(|meta| meta.at),
            last_cancel_connection_id: last_cancel.as_ref().map(|meta| meta.connection_id),
            last_cancel_reason: last_cancel.as_ref().map(|meta| meta.reason.clone()),
            last_cancel_sqlstate: last_cancel.as_ref().map(|meta| meta.sqlstate.clone()),
            uptime_seconds: self.started_at.elapsed().as_secs() as i64,
            db_size_bytes,
            memory_estimate_bytes: db_size_bytes
                .saturating_add((active_connections as u64).saturating_mul(64 * 1024)),
            last_checkpoint: *self.last_checkpoint.lock().unwrap(),
            writes_executed: self.writes_executed.load(Ordering::SeqCst),
            proc_neword: self.proc_neword.load(Ordering::Relaxed),
            proc_payment: self.proc_payment.load(Ordering::Relaxed),
            proc_delivery: self.proc_delivery.load(Ordering::Relaxed),
            proc_orderstatus: self.proc_orderstatus.load(Ordering::Relaxed),
            proc_stocklevel: self.proc_stocklevel.load(Ordering::Relaxed),
            routine_serialization_failure: routine_exceptions.serialization_failure,
            routine_deadlock_detected: routine_exceptions.deadlock_detected,
            routine_no_data_found: routine_exceptions.no_data_found,
            routine_other: routine_exceptions.other,
            wal_written_seq: wal_stats.written_seq,
            wal_write_calls: wal_stats.write_calls,
            wal_sync_calls: wal_stats.sync_calls,
            wal_bytes_written: wal_stats.bytes_written,
            wal_commits_written: wal_stats.commits_written,
            wal_max_batch_commits: wal_stats.max_batch_commits,
            max_queued_writes: self.config.max_queued_writes,
            write_queue_depth: self.write_queue_depth.load(Ordering::SeqCst),
            write_queue_depth_max: self.write_queue_depth_max.load(Ordering::SeqCst),
            write_wait_total_ns: self.write_wait_total_ns.load(Ordering::SeqCst),
            write_wait_max_ns: self.write_wait_max_ns.load(Ordering::SeqCst),
            write_execution_total_ns: self.write_execution_total_ns.load(Ordering::SeqCst),
            write_execution_max_ns: self.write_execution_max_ns.load(Ordering::SeqCst),
            write_rejected_count: self.write_rejected_count.load(Ordering::SeqCst),
            write_timed_out_count: self.write_timed_out_count.load(Ordering::SeqCst),
            db_lock_acquisitions: self.db_lock_acquisitions.load(Ordering::SeqCst),
            db_lock_wait_total_ns: self.db_lock_wait_total_ns.load(Ordering::SeqCst),
            db_lock_wait_max_ns: self.db_lock_wait_max_ns.load(Ordering::SeqCst),
            db_lock_hold_total_ns: self.db_lock_hold_total_ns.load(Ordering::SeqCst),
            db_lock_hold_max_ns: self.db_lock_hold_max_ns.load(Ordering::SeqCst),
            db_read_lock_acquisitions: self.db_read_lock_acquisitions.load(Ordering::SeqCst),
            db_read_lock_wait_total_ns: self.db_read_lock_wait_total_ns.load(Ordering::SeqCst),
            db_read_lock_wait_max_ns: self.db_read_lock_wait_max_ns.load(Ordering::SeqCst),
            db_read_lock_hold_total_ns: self.db_read_lock_hold_total_ns.load(Ordering::SeqCst),
            db_read_lock_hold_max_ns: self.db_read_lock_hold_max_ns.load(Ordering::SeqCst),
            db_write_lock_acquisitions: self.db_write_lock_acquisitions.load(Ordering::SeqCst),
            db_write_lock_wait_total_ns: self.db_write_lock_wait_total_ns.load(Ordering::SeqCst),
            db_write_lock_wait_max_ns: self.db_write_lock_wait_max_ns.load(Ordering::SeqCst),
            db_write_lock_hold_total_ns: self.db_write_lock_hold_total_ns.load(Ordering::SeqCst),
            db_write_lock_hold_max_ns: self.db_write_lock_hold_max_ns.load(Ordering::SeqCst),
            rows_streamed: self.rows_streamed.load(Ordering::SeqCst),
            bytes_streamed: self.bytes_streamed.load(Ordering::SeqCst),
            cursor_count: self.active_cursors.load(Ordering::SeqCst),
            cursor_memory_bytes: self.cursor_memory_bytes.load(Ordering::SeqCst),
            spilled_to_disk_bytes: self.spilled_to_disk_bytes.load(Ordering::SeqCst),
            ha_status,
        }
    }

    pub(crate) fn record_streamed_rows(&self, rows: u64, bytes: u64) {
        self.rows_streamed.fetch_add(rows, Ordering::SeqCst);
        self.bytes_streamed.fetch_add(bytes, Ordering::SeqCst);
    }

    pub(crate) fn register_cursor_memory(&self, bytes: usize) {
        self.active_cursors.fetch_add(1, Ordering::SeqCst);
        self.cursor_memory_bytes.fetch_add(bytes, Ordering::SeqCst);
    }

    pub(crate) fn adjust_cursor_memory(&self, previous: usize, current: usize) {
        if current >= previous {
            self.cursor_memory_bytes
                .fetch_add(current - previous, Ordering::SeqCst);
        } else {
            self.cursor_memory_bytes
                .fetch_sub(previous - current, Ordering::SeqCst);
        }
    }

    pub(crate) fn unregister_cursor_memory(&self, bytes: usize) {
        self.active_cursors.fetch_sub(1, Ordering::SeqCst);
        self.cursor_memory_bytes.fetch_sub(bytes, Ordering::SeqCst);
    }

    /// Whether `user` administers this server, and may therefore see other
    /// sessions' peer addresses and in-flight SQL.
    pub(crate) fn user_is_admin(&self, user: &str) -> bool {
        bicdb_sql::pg_role_is_superuser(&self.db.read(), user).unwrap_or(false)
    }

    pub fn connection_snapshots(&self) -> Vec<ServerConnectionSnapshot> {
        let mut connections = self
            .connections
            .lock()
            .unwrap()
            .values()
            .cloned()
            .collect::<Vec<_>>();
        connections.sort_by_key(|connection| connection.connection_id);
        connections
    }

    /// Rate-limited, human-readable saturation hint that names the exact
    /// flag to raise. The structured operational log fires per event; this
    /// line exists because an unset host-sized default has already cost a
    /// deployment weeks of misdiagnosis — the number in the failure pattern
    /// must appear next to the flag that sets it. At most one hint per 10s
    /// across all admission gates.
    pub(crate) fn saturation_operator_hint(&self, message: impl FnOnce() -> String) {
        const HINT_INTERVAL_MILLIS: u64 = 10_000;
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|elapsed| elapsed.as_millis() as u64)
            .unwrap_or(0);
        let last = self.saturation_hint_millis.load(Ordering::Relaxed);
        if now.saturating_sub(last) < HINT_INTERVAL_MILLIS {
            return;
        }
        if self
            .saturation_hint_millis
            .compare_exchange(last, now, Ordering::Relaxed, Ordering::Relaxed)
            .is_ok()
        {
            eprintln!("{}", message());
        }
    }

    pub(crate) fn try_admit_connection(
        self: &Arc<Self>,
        source_ip: IpAddr,
    ) -> Option<ConnectionAdmission> {
        self.admit_connection(source_ip).ok()
    }

    /// Admits a connection, or reports **which** limit refused it.
    ///
    /// Two different caps can refuse here, and they are raised with different
    /// knobs. Reporting one while enforcing the other sends an operator to
    /// change a setting that cannot help, which is how a saturated server
    /// stays saturated through several restarts.
    pub(crate) fn admit_connection(
        self: &Arc<Self>,
        source_ip: IpAddr,
    ) -> std::result::Result<ConnectionAdmission, ConnectionLimit> {
        {
            let mut by_ip = self.active_connections_by_ip.lock().unwrap();
            let current = by_ip.get(&source_ip).copied().unwrap_or(0);
            if current >= self.config.max_connections_per_ip {
                self.rejected_connections.fetch_add(1, Ordering::SeqCst);
                return Err(ConnectionLimit::PerSourceIp {
                    limit: self.config.max_connections_per_ip,
                });
            }
            by_ip.insert(source_ip, current + 1);
        }
        let mut current = self.active_connections.load(Ordering::SeqCst);
        loop {
            if current >= self.config.max_connections {
                self.rejected_connections.fetch_add(1, Ordering::SeqCst);
                self.release_source_ip(source_ip);
                return Err(ConnectionLimit::Server {
                    limit: self.config.max_connections,
                });
            }
            match self.active_connections.compare_exchange(
                current,
                current + 1,
                Ordering::SeqCst,
                Ordering::SeqCst,
            ) {
                Ok(_) => {
                    atomic_max_usize(&self.peak_active_connections, current + 1);
                    self.total_connections.fetch_add(1, Ordering::SeqCst);
                    return Ok(ConnectionAdmission {
                        server: self.clone(),
                        source_ip,
                        released: false,
                    });
                }
                Err(observed) => current = observed,
            }
        }
    }

    pub(crate) fn release_source_ip(&self, source_ip: IpAddr) {
        let mut by_ip = self.active_connections_by_ip.lock().unwrap();
        if let Some(current) = by_ip.get_mut(&source_ip) {
            *current = current.saturating_sub(1);
            if *current == 0 {
                by_ip.remove(&source_ip);
            }
        }
    }

    pub(crate) fn acquire_query(self: &Arc<Self>, kind: QueryKind) -> Result<ActiveQueryPermit> {
        let started = Instant::now();
        if let Some(permit) = self.try_acquire_query_now(kind, started)? {
            return Ok(permit);
        }
        let queue = self.try_enter_query_queue(kind)?;
        loop {
            if let Some(permit) = self.try_acquire_query_now(kind, started)? {
                drop(queue);
                return Ok(permit);
            }
            if started.elapsed() >= self.config.overload_timeout {
                self.timed_out_queries.fetch_add(1, Ordering::SeqCst);
                return Err(PgWireError::QueryTimedOut);
            }
            thread::sleep(Duration::from_millis(1));
        }
    }

    pub(crate) fn try_acquire_query_now(
        self: &Arc<Self>,
        kind: QueryKind,
        started: Instant,
    ) -> Result<Option<ActiveQueryPermit>> {
        let active_by_kind = self.active_by_kind(kind).load(Ordering::SeqCst);
        if active_by_kind >= self.max_active_by_kind(kind) {
            return Ok(None);
        }
        let mut current = self.active_queries.load(Ordering::SeqCst);
        loop {
            if current >= self.config.max_active_queries {
                return Ok(None);
            }
            match self.active_queries.compare_exchange(
                current,
                current + 1,
                Ordering::SeqCst,
                Ordering::SeqCst,
            ) {
                Ok(_) => {
                    let by_kind = self.active_by_kind(kind).fetch_add(1, Ordering::SeqCst) + 1;
                    if by_kind > self.max_active_by_kind(kind) {
                        self.active_by_kind(kind).fetch_sub(1, Ordering::SeqCst);
                        self.active_queries.fetch_sub(1, Ordering::SeqCst);
                        return Ok(None);
                    }
                    atomic_max_usize(&self.peak_active_queries, current + 1);
                    atomic_max_usize(self.peak_active_by_kind(kind), by_kind);
                    self.record_query_queue_wait(duration_nanos_u64(started.elapsed()));
                    return Ok(Some(ActiveQueryPermit {
                        server: self.clone(),
                        kind,
                    }));
                }
                Err(observed) => current = observed,
            }
        }
    }

    pub(crate) fn try_enter_query_queue(
        self: &Arc<Self>,
        kind: QueryKind,
    ) -> Result<QueryQueueAdmission> {
        if !try_increment_bounded(
            &self.queued_queries,
            &self.queued_queries_max,
            self.config.max_queued_queries,
        ) {
            self.rejected_queries.fetch_add(1, Ordering::SeqCst);
            self.saturation_operator_hint(|| {
                format!(
                    "bicdb server: query rejected with the queue full at \
                     --max-queued-queries={} (actives saturated at \
                     --max-active-queries={}). Raise both for server workloads.",
                    self.config.max_queued_queries, self.config.max_active_queries
                )
            });
            return Err(PgWireError::QueryRejected(format!(
                "too many queued queries; max_queued_queries is {}",
                self.config.max_queued_queries
            )));
        }
        if !try_increment_bounded(
            self.queued_by_kind(kind),
            self.queued_max_by_kind(kind),
            self.max_queued_by_kind(kind),
        ) {
            self.queued_queries.fetch_sub(1, Ordering::SeqCst);
            self.rejected_queries.fetch_add(1, Ordering::SeqCst);
            self.saturation_operator_hint(|| {
                format!(
                    "bicdb server: {} query rejected with its queue full at {} \
                     (see --max-queued-reads/--max-queued-writes and \
                     --max-active-queries).",
                    kind.label(),
                    self.max_queued_by_kind(kind)
                )
            });
            return Err(PgWireError::QueryRejected(format!(
                "too many queued {} queries; max queued {} is {}",
                kind.label(),
                kind.label(),
                self.max_queued_by_kind(kind)
            )));
        }
        Ok(QueryQueueAdmission {
            server: self.clone(),
            kind,
            released: false,
        })
    }

    pub(crate) fn active_by_kind(&self, kind: QueryKind) -> &AtomicUsize {
        match kind {
            QueryKind::Read => &self.active_reads,
            QueryKind::Write => &self.active_writes,
        }
    }

    pub(crate) fn peak_active_by_kind(&self, kind: QueryKind) -> &AtomicUsize {
        match kind {
            QueryKind::Read => &self.peak_active_reads,
            QueryKind::Write => &self.peak_active_writes,
        }
    }

    pub(crate) fn queued_by_kind(&self, kind: QueryKind) -> &AtomicUsize {
        match kind {
            QueryKind::Read => &self.queued_reads,
            QueryKind::Write => &self.queued_writes,
        }
    }

    pub(crate) fn queued_max_by_kind(&self, kind: QueryKind) -> &AtomicUsize {
        match kind {
            QueryKind::Read => &self.queued_reads_max,
            QueryKind::Write => &self.queued_writes_max,
        }
    }

    pub(crate) fn max_active_by_kind(&self, kind: QueryKind) -> usize {
        match kind {
            QueryKind::Read => self.config.max_active_reads,
            QueryKind::Write => self.config.max_active_writes,
        }
    }

    pub(crate) fn max_queued_by_kind(&self, kind: QueryKind) -> usize {
        match kind {
            QueryKind::Read => self.config.max_queued_reads,
            QueryKind::Write => self.config.max_queued_writes,
        }
    }

    pub(crate) fn record_query_queue_wait(&self, wait_ns: u64) {
        if !pgwire_query_queue_wait_trace_enabled() {
            return;
        }
        let mut samples = self.query_queue_wait_samples_ns.lock().unwrap();
        if samples.len() >= 4096 {
            samples.remove(0);
        }
        samples.push(wait_ns);
    }

    pub(crate) fn flush(&self) -> Result<()> {
        self.db.read().flush()?;
        *self.last_checkpoint.lock().unwrap() = Some(unix_timestamp());
        Ok(())
    }

    /// Whether the concurrent (read-lock execute) write path should be tried,
    /// based on the conflict-credit gate. Returns true while there is positive
    /// credit, and otherwise only on an occasional probe so the server recovers
    /// if contention subsides.
    pub(crate) fn shared_write_gate_open(&self) -> bool {
        if self.shared_write_credit.load(Ordering::Relaxed) > 0 {
            return true;
        }
        self.shared_write_probe.fetch_add(1, Ordering::Relaxed) % 256 == 0
    }

    /// Tries to acquire a slot to run a write on the concurrent shared path.
    /// Returns `None` (caller runs exclusively) when the conflict gate is closed
    /// or the bound on in-flight shared executions is reached. The returned
    /// guard releases the slot on drop.
    pub(crate) fn try_acquire_shared_write(self: &Arc<Self>) -> Option<SharedWriteSlot> {
        // Bound chosen to overlap execution without starving the write lock.
        const MAX_INFLIGHT_SHARED_WRITES: usize = 48;
        if !self.shared_write_gate_open() {
            return None;
        }
        let mut current = self.active_shared_writes.load(Ordering::Relaxed);
        loop {
            if current >= MAX_INFLIGHT_SHARED_WRITES {
                return None;
            }
            match self.active_shared_writes.compare_exchange_weak(
                current,
                current + 1,
                Ordering::AcqRel,
                Ordering::Relaxed,
            ) {
                Ok(_) => {
                    return Some(SharedWriteSlot {
                        server: self.clone(),
                    });
                }
                Err(observed) => current = observed,
            }
        }
    }

    /// Records the outcome of a shared-path commit, adjusting the gate credit.
    pub(crate) fn note_shared_commit(&self, conflicted: bool) {
        if conflicted {
            if self.shared_write_credit.fetch_sub(8, Ordering::Relaxed) - 8 < -64 {
                self.shared_write_credit.store(-64, Ordering::Relaxed);
            }
        } else if self.shared_write_credit.fetch_add(1, Ordering::Relaxed) + 1 > 32 {
            self.shared_write_credit.store(32, Ordering::Relaxed);
        }
    }

    pub(crate) fn read_db(&self) -> Result<InstrumentedDbReadGuard<'_>> {
        let wait_started = Instant::now();
        let guard = self.db.read();
        let wait = wait_started.elapsed();
        let wait_ns = duration_nanos_u64(wait);
        self.record_db_lock_wait(DbLockKind::Read, wait_ns);
        Ok(InstrumentedDbReadGuard {
            server: self,
            guard,
            acquired_at: Instant::now(),
        })
    }

    /// Acquire shared database access for work that must let an already-open
    /// transaction make progress (statement execution, deferred triggers, or
    /// commit). `parking_lot::RwLock::read` is writer-fair: once a background
    /// DDL/checkpoint writer queues, it blocks new readers. That ordering can
    /// deadlock if an existing reader is waiting for a row owned by the
    /// transaction trying to acquire this guard. Recursive read acquisition
    /// bypasses queued writers, allowing the owner to commit and release the
    /// row; the waiting reader then exits and the queued writer proceeds.
    pub(crate) fn read_db_for_transaction_progress(&self) -> Result<InstrumentedDbReadGuard<'_>> {
        let wait_started = Instant::now();
        let guard = self.db.read_recursive();
        let wait = wait_started.elapsed();
        let wait_ns = duration_nanos_u64(wait);
        self.record_db_lock_wait(DbLockKind::Read, wait_ns);
        Ok(InstrumentedDbReadGuard {
            server: self,
            guard,
            acquired_at: Instant::now(),
        })
    }

    pub(crate) fn try_admit_write(self: &Arc<Self>) -> Result<WriteAdmission> {
        let mut current = self.write_queue_depth.load(Ordering::SeqCst);
        loop {
            if current >= self.config.max_queued_writes {
                self.write_rejected_count.fetch_add(1, Ordering::SeqCst);
                self.saturation_operator_hint(|| {
                    format!(
                        "bicdb server: write rejected with the write queue full at \
                         --max-queued-writes={}. Raise it for write-heavy workloads.",
                        self.config.max_queued_writes
                    )
                });
                return Err(PgWireError::Server(format!(
                    "too many queued writes; max_queued_writes is {}",
                    self.config.max_queued_writes
                )));
            }
            match self.write_queue_depth.compare_exchange(
                current,
                current + 1,
                Ordering::SeqCst,
                Ordering::SeqCst,
            ) {
                Ok(_) => {
                    atomic_max_usize(&self.write_queue_depth_max, current + 1);
                    return Ok(WriteAdmission {
                        server: self.clone(),
                        admitted_at: Instant::now(),
                        released: false,
                    });
                }
                Err(observed) => current = observed,
            }
        }
    }

    pub(crate) fn write_db_with_admission(
        &self,
        admission: &WriteAdmission,
    ) -> Result<InstrumentedDbWriteGuard<'_>> {
        let wait_started = Instant::now();
        let poll_budget = Duration::from_micros(50);
        let poll_until = wait_started + poll_budget;
        while Instant::now() < poll_until {
            if let Some(guard) = self.db.try_write() {
                let wait_ns = duration_nanos_u64(wait_started.elapsed());
                self.record_db_lock_wait(DbLockKind::Write, wait_ns);
                self.record_write_wait(duration_nanos_u64(admission.admitted_at.elapsed()));
                return Ok(InstrumentedDbWriteGuard {
                    server: self,
                    guard,
                    acquired_at: Instant::now(),
                });
            }
            if wait_started.elapsed() < Duration::from_micros(100) {
                std::hint::spin_loop();
            } else {
                thread::yield_now();
            }
        }
        let elapsed = wait_started.elapsed();
        let remaining = self.config.write_timeout.saturating_sub(elapsed);
        match self.db.try_write_for(remaining) {
            Some(guard) => {
                let wait_ns = duration_nanos_u64(wait_started.elapsed());
                self.record_db_lock_wait(DbLockKind::Write, wait_ns);
                self.record_write_wait(duration_nanos_u64(admission.admitted_at.elapsed()));
                Ok(InstrumentedDbWriteGuard {
                    server: self,
                    guard,
                    acquired_at: Instant::now(),
                })
            }
            None => {
                self.write_timed_out_count.fetch_add(1, Ordering::SeqCst);
                Err(PgWireError::Server(format!(
                    "write timed out waiting for database writer after {}ms",
                    self.config.write_timeout.as_millis()
                )))
            }
        }
    }

    pub(crate) fn record_write_wait(&self, wait_ns: u64) {
        self.write_wait_total_ns
            .fetch_add(wait_ns, Ordering::SeqCst);
        atomic_max(&self.write_wait_max_ns, wait_ns);
    }

    pub(crate) fn record_write_execution(&self, execution_ns: u64) {
        self.write_execution_total_ns
            .fetch_add(execution_ns, Ordering::SeqCst);
        atomic_max(&self.write_execution_max_ns, execution_ns);
    }

    pub(crate) fn record_db_lock_wait(&self, kind: DbLockKind, wait_ns: u64) {
        self.db_lock_acquisitions.fetch_add(1, Ordering::SeqCst);
        self.db_lock_wait_total_ns
            .fetch_add(wait_ns, Ordering::SeqCst);
        atomic_max(&self.db_lock_wait_max_ns, wait_ns);
        match kind {
            DbLockKind::Read => {
                self.db_read_lock_acquisitions
                    .fetch_add(1, Ordering::SeqCst);
                self.db_read_lock_wait_total_ns
                    .fetch_add(wait_ns, Ordering::SeqCst);
                atomic_max(&self.db_read_lock_wait_max_ns, wait_ns);
            }
            DbLockKind::Write => {
                self.db_write_lock_acquisitions
                    .fetch_add(1, Ordering::SeqCst);
                self.db_write_lock_wait_total_ns
                    .fetch_add(wait_ns, Ordering::SeqCst);
                atomic_max(&self.db_write_lock_wait_max_ns, wait_ns);
            }
        }
    }

    pub(crate) fn record_db_lock_hold(&self, kind: DbLockKind, hold_ns: u64) {
        self.db_lock_hold_total_ns
            .fetch_add(hold_ns, Ordering::SeqCst);
        atomic_max(&self.db_lock_hold_max_ns, hold_ns);
        match kind {
            DbLockKind::Read => {
                self.db_read_lock_hold_total_ns
                    .fetch_add(hold_ns, Ordering::SeqCst);
                atomic_max(&self.db_read_lock_hold_max_ns, hold_ns);
            }
            DbLockKind::Write => {
                self.db_write_lock_hold_total_ns
                    .fetch_add(hold_ns, Ordering::SeqCst);
                atomic_max(&self.db_write_lock_hold_max_ns, hold_ns);
            }
        }
    }

    pub(crate) fn close(&self) -> Result<()> {
        if let Some(cluster) = self.cluster.as_ref().and_then(Weak::upgrade) {
            cluster.close()
        } else {
            self.close_local()
        }
    }

    pub(crate) fn close_local(&self) -> Result<()> {
        // Background workers hold this server, and through it the database,
        // and the flush and checkpoint loops still write. Let them observe the
        // shutdown request and exit before claiming a clean shutdown: the
        // marker means "this database is closed", and writing it while a
        // five-second flush loop is still running makes that a lie.
        self.wait_for_background_quiesce(BACKGROUND_QUIESCE_TIMEOUT);
        self.flush()?;
        std::fs::write(
            self.path.join(CLEAN_SHUTDOWN_MARKER),
            unix_timestamp().to_string(),
        )?;
        Ok(())
    }

    /// Waits until no background worker is registered, or `timeout` elapses.
    ///
    /// Bounded rather than unbounded: a wedged worker must not turn shutdown
    /// into a hang, and the caller is about to exit regardless.
    pub(crate) fn wait_for_background_quiesce(&self, timeout: Duration) -> bool {
        let started = Instant::now();
        while self.background_workers.load(Ordering::SeqCst) > 0 {
            if started.elapsed() >= timeout {
                return false;
            }
            std::thread::sleep(Duration::from_millis(5));
        }
        true
    }

    pub(crate) fn register_cancel_token(
        &self,
        process_id: i32,
        secret_key: i32,
    ) -> Arc<AtomicBool> {
        let token = Arc::new(AtomicBool::new(false));
        self.cancel_tokens
            .lock()
            .unwrap()
            .insert((process_id, secret_key), token.clone());
        token
    }

    /// Signal every registered cancel token. Used when the shutdown grace
    /// period expires: a connection sitting in a long statement releases as
    /// soon as its query observes the flag, which is what lets teardown
    /// finish instead of parking on an uncancellable blocking task.
    pub(crate) fn cancel_all_active_queries(&self) {
        let tokens: Vec<Arc<AtomicBool>> = match self.cancel_tokens.lock() {
            Ok(tokens) => tokens.values().cloned().collect(),
            Err(poisoned) => poisoned.into_inner().values().cloned().collect(),
        };
        for token in tokens {
            token.store(true, Ordering::SeqCst);
        }
    }

    pub(crate) fn unregister_cancel_token(&self, process_id: i32, secret_key: i32) {
        self.cancel_tokens
            .lock()
            .unwrap()
            .remove(&(process_id, secret_key));
    }

    pub(crate) fn request_cancel(&self, process_id: i32, secret_key: i32) {
        if let Some(token) = self
            .cancel_tokens
            .lock()
            .unwrap()
            .get(&(process_id, secret_key))
        {
            token.store(true, Ordering::SeqCst);
        }
    }

    pub(crate) fn record_cancel_metadata(&self, connection_id: u64, reason: &str, sqlstate: &str) {
        if let Ok(mut last_cancel) = self.last_cancel.try_lock() {
            *last_cancel = Some(CancelMetadata {
                at: unix_timestamp(),
                connection_id,
                reason: reason.to_string(),
                sqlstate: sqlstate.to_string(),
            });
        }
    }
}

pub(crate) fn load_local_distribution_router(
    path: &Path,
    fsync: bool,
) -> Result<Option<ClusterRequestRouter>> {
    if !path.join(DEFAULT_DISTRIBUTION_CONFIG).is_file() {
        return Ok(None);
    }
    let distribution = load_distribution_config(path)?;
    let store = DistributionStore::open(path, distribution.clone(), fsync)?;
    Ok(Some(ClusterRequestRouter::new(
        distribution,
        store.topology().clone(),
    )?))
}

impl PgWireCluster {
    pub fn open(
        root: impl AsRef<Path>,
        default_database: impl Into<String>,
        config: PgWireConfig,
    ) -> Result<Arc<Self>> {
        validate_server_config(&config)?;
        let root = root.as_ref().to_path_buf();
        fs::create_dir_all(root.join("databases"))?;
        let default_database = normalize_cluster_database_name(&default_database.into())?;
        let manifest_path = root.join(CLUSTER_MANIFEST_FILE);
        let manifest = if manifest_path.exists() {
            let bytes = fs::read(&manifest_path)?;
            let manifest: ClusterManifest = serde_json::from_slice(&bytes).map_err(|error| {
                PgWireError::Server(format!(
                    "read cluster manifest {}: {error}",
                    manifest_path.display()
                ))
            })?;
            validate_cluster_manifest(&manifest)?;
            manifest
        } else {
            let directory = Uuid::new_v4().to_string();
            let mut databases = BTreeMap::new();
            databases.insert(
                default_database.clone(),
                ClusterDatabaseEntry {
                    directory: directory.clone(),
                    owner: "bicdb".to_string(),
                },
            );
            let manifest = ClusterManifest {
                version: 1,
                default_database,
                databases,
            };
            fs::create_dir_all(root.join("databases").join(directory))?;
            persist_cluster_manifest(&root, &manifest)?;
            manifest
        };

        let resource_governor =
            ResourceGovernor::new(config.resource_governor.clone(), cluster_now_ms())?;
        let cluster = Arc::new(Self {
            root,
            config,
            resource_governor,
            manifest: Mutex::new(manifest),
            servers: Mutex::new(HashMap::new()),
            database_background_enabled: AtomicBool::new(false),
        });
        let default = cluster.default_database();
        cluster.server_for_database(&default)?;
        cluster.sync_role_catalogs_from_default()?;
        cluster.sync_database_catalogs()?;
        eprintln!(
            "bicdb server: PgWireCluster databases share one process and failure domain. \
             Database separation is not a Cell boundary. For Cell isolation, launch one \
             bicdb-cell runtime per process with dedicated OS isolation and resource limits."
        );
        Ok(cluster)
    }

    pub fn default_database(&self) -> String {
        self.manifest.lock().unwrap().default_database.clone()
    }

    pub fn default_server(self: &Arc<Self>) -> Result<Arc<PgWireServer>> {
        self.server_for_database(&self.default_database())
    }

    pub(crate) fn server_for_database(
        self: &Arc<Self>,
        database: &str,
    ) -> Result<Arc<PgWireServer>> {
        let database = normalize_cluster_database_name(database)?;
        if let Some(server) = self.servers.lock().unwrap().get(&database).cloned() {
            return Ok(server);
        }
        let entry = self
            .manifest
            .lock()
            .unwrap()
            .databases
            .get(&database)
            .cloned()
            .ok_or_else(|| PgWireError::DatabaseNotFound(database.clone()))?;
        validate_cluster_directory(&entry.directory)?;
        let path = self.root.join("databases").join(&entry.directory);
        let db = BicDb::open_with_config(&path, database_config_for_server(&path, &self.config)?)?;
        let server = PgWireServer::open_with_context(
            &path,
            &self.root,
            self.config.clone(),
            Arc::new(RwLock::new(db)),
            Some(Arc::downgrade(self)),
            None,
            Some(self.resource_governor.clone()),
        )?;
        let mut servers = self.servers.lock().unwrap();
        let server = servers
            .entry(database)
            .or_insert_with(|| server.clone())
            .clone();
        drop(servers);
        if self.database_background_enabled.load(Ordering::SeqCst) {
            start_database_background_tasks(server.clone())?;
        }
        Ok(server)
    }

    pub(crate) fn create_database(
        self: &Arc<Self>,
        source: &Arc<PgWireServer>,
        state: &ConnectionState,
        sql: &str,
        name: &str,
        owner: Option<&str>,
    ) -> Result<SqlResult> {
        let name = normalize_cluster_database_name(name)?;
        let owner = owner
            .map(str::to_string)
            .unwrap_or_else(|| state.user.trim_matches('"').to_ascii_lowercase());
        let mut manifest = self.manifest.lock().unwrap();
        if manifest.databases.contains_key(&name) {
            return Err(PgWireError::DatabaseAlreadyExists(name));
        }

        let directory = Uuid::new_v4().to_string();
        let path = self.root.join("databases").join(&directory);
        fs::create_dir(&path)?;
        let opened = (|| {
            let db =
                BicDb::open_with_config(&path, database_config_for_server(&path, &self.config)?)?;
            PgWireServer::open_with_context(
                &path,
                &self.root,
                self.config.clone(),
                Arc::new(RwLock::new(db)),
                Some(Arc::downgrade(self)),
                None,
                Some(self.resource_governor.clone()),
            )
        })();
        let server = match opened {
            Ok(server) => server,
            Err(error) => {
                let _ = fs::remove_dir_all(&path);
                return Err(error);
            }
        };
        if let Err(error) = self.copy_role_catalog(source, &server) {
            drop(server);
            let _ = fs::remove_dir_all(&path);
            return Err(error);
        }

        let entry = ClusterDatabaseEntry {
            directory,
            owner: owner.clone(),
        };
        manifest.databases.insert(name.clone(), entry.clone());
        if let Err(error) = persist_cluster_manifest(&self.root, &manifest) {
            manifest.databases.remove(&name);
            drop(manifest);
            drop(server);
            let _ = fs::remove_dir_all(&path);
            return Err(error);
        }

        // The manifest is published before mutating pg_database so an SQL
        // rejection can remove the complete physical database atomically. The
        // manifest lock prevents startup routing from observing this tentative
        // entry while PostgreSQL ownership and CREATEDB checks run.
        let create_result = {
            let mut db = source.db.write();
            let mut session =
                with_connection_guc_state(sql_session_for_server(source, &mut db), source, state);
            session.execute(sql)
        };
        if let Err(error) = create_result {
            manifest.databases.remove(&name);
            if let Err(rollback_error) = persist_cluster_manifest(&self.root, &manifest) {
                // Keep memory consistent with the durable manifest if the
                // rollback itself cannot be persisted.
                manifest.databases.insert(name.clone(), entry);
                return Err(PgWireError::Server(format!(
                    "CREATE DATABASE failed ({error}); cluster manifest rollback failed: {rollback_error}"
                )));
            }
            drop(manifest);
            drop(server);
            fs::remove_dir_all(&path).map_err(|rollback_error| {
                PgWireError::Server(format!(
                    "CREATE DATABASE failed ({error}); remove physical database {}: {rollback_error}",
                    path.display()
                ))
            })?;
            return Err(PgWireError::Sql(error));
        }

        drop(manifest);
        self.servers.lock().unwrap().insert(name, server.clone());
        if self.database_background_enabled.load(Ordering::SeqCst) {
            start_database_background_tasks(server)?;
        }
        self.sync_database_catalogs()?;
        Ok(SqlResult::command("CREATE DATABASE"))
    }

    pub(crate) fn copy_role_catalog(
        &self,
        source: &Arc<PgWireServer>,
        target: &Arc<PgWireServer>,
    ) -> Result<()> {
        let source_db = source.db.read();
        let mut target_db = target.db.write();
        copy_cluster_role_catalog(&source_db, &mut target_db).map_err(PgWireError::from)
    }

    pub(crate) fn sync_database_catalogs(self: &Arc<Self>) -> Result<()> {
        let databases = self
            .manifest
            .lock()
            .unwrap()
            .databases
            .iter()
            .map(|(name, entry)| (name.clone(), entry.owner.clone()))
            .collect::<Vec<_>>();
        for target in databases.iter().map(|(name, _)| name) {
            let server = self.server_for_database(target)?;
            let mut db = server.db.write();
            let mut session = sql_session_for_server(&server, &mut db);
            let known = session
                .execute("SELECT datname FROM pg_catalog.pg_database")?
                .rows
                .into_iter()
                .filter_map(|row| row.into_iter().next())
                .map(|value| value.to_cell().to_ascii_lowercase())
                .collect::<HashSet<_>>();
            for (name, owner) in &databases {
                if known.contains(&name.to_ascii_lowercase()) {
                    continue;
                }
                let sql = format!(
                    "CREATE DATABASE {} OWNER {}",
                    quote_sql_identifier(name),
                    quote_sql_identifier(owner)
                );
                session.execute(&sql)?;
            }
        }
        Ok(())
    }

    pub(crate) fn sync_role_catalogs_from_default(self: &Arc<Self>) -> Result<()> {
        let default_name = self.default_database();
        let source = self.server_for_database(&default_name)?;
        let databases = self
            .manifest
            .lock()
            .unwrap()
            .databases
            .keys()
            .filter(|name| *name != &default_name)
            .cloned()
            .collect::<Vec<_>>();
        for database in databases {
            let target = self.server_for_database(&database)?;
            self.copy_role_catalog(&source, &target)?;
        }
        Ok(())
    }

    pub(crate) fn sync_role_ddl(
        self: &Arc<Self>,
        source: &Arc<PgWireServer>,
        sql: &str,
    ) -> Result<()> {
        let statements = shared_role_ddl_statements(sql);
        if statements.is_empty() {
            return Ok(());
        }
        let mut databases = self
            .manifest
            .lock()
            .unwrap()
            .databases
            .keys()
            .cloned()
            .collect::<Vec<_>>();
        let default = self.default_database();
        databases.sort_by_key(|database| database != &default);
        for database in databases {
            let target = self.server_for_database(&database)?;
            if Arc::ptr_eq(source, &target) {
                continue;
            }
            let mut db = target.db.write();
            let mut session = sql_session_for_server(&target, &mut db);
            for statement in &statements {
                session.execute(statement)?;
            }
        }
        Ok(())
    }

    pub(crate) fn request_shutdown(&self) {
        for server in self.servers.lock().unwrap().values() {
            server.shutdown.store(true, Ordering::SeqCst);
        }
    }

    pub(crate) fn enable_database_background_tasks(&self) {
        self.database_background_enabled
            .store(true, Ordering::SeqCst);
        let servers = self
            .servers
            .lock()
            .unwrap()
            .values()
            .cloned()
            .collect::<Vec<_>>();
        for server in servers {
            if let Err(error) = start_database_background_tasks(server.clone()) {
                eprintln!("bicdb database background startup error: {error}");
                server.request_shutdown();
            }
        }
    }

    pub(crate) fn close(&self) -> Result<()> {
        let servers = self
            .servers
            .lock()
            .unwrap()
            .values()
            .cloned()
            .collect::<Vec<_>>();
        for server in servers {
            server.close_local()?;
        }
        Ok(())
    }
}

pub(crate) fn normalize_cluster_database_name(name: &str) -> Result<String> {
    let name = name.trim().trim_matches('"');
    if name.is_empty() || name.len() > 63 || name.bytes().any(|byte| byte == 0) {
        return Err(PgWireError::Server(format!(
            "invalid database name {name:?}"
        )));
    }
    Ok(name.to_ascii_lowercase())
}

pub(crate) fn validate_cluster_directory(directory: &str) -> Result<()> {
    if Uuid::parse_str(directory)
        .ok()
        .filter(|uuid| uuid.to_string() == directory)
        .is_none()
    {
        return Err(PgWireError::Server(format!(
            "invalid database directory in cluster manifest: {directory:?}"
        )));
    }
    Ok(())
}

pub(crate) fn validate_cluster_manifest(manifest: &ClusterManifest) -> Result<()> {
    if manifest.version != 1 {
        return Err(PgWireError::Server(format!(
            "unsupported cluster manifest version {}",
            manifest.version
        )));
    }
    if !manifest.databases.contains_key(&manifest.default_database) {
        return Err(PgWireError::Server(
            "cluster manifest default database is missing".to_string(),
        ));
    }
    for (name, entry) in &manifest.databases {
        normalize_cluster_database_name(name)?;
        validate_cluster_directory(&entry.directory)?;
    }
    Ok(())
}

pub(crate) fn persist_cluster_manifest(root: &Path, manifest: &ClusterManifest) -> Result<()> {
    let path = root.join(CLUSTER_MANIFEST_FILE);
    let temporary = root.join(format!(".{CLUSTER_MANIFEST_FILE}.{}.tmp", Uuid::new_v4()));
    let bytes = serde_json::to_vec_pretty(manifest)
        .map_err(|error| PgWireError::Server(format!("serialize cluster manifest: {error}")))?;
    let mut file = File::create(&temporary)?;
    file.write_all(&bytes)?;
    file.sync_all()?;
    fs::rename(&temporary, &path)?;
    #[cfg(unix)]
    File::open(root)?.sync_all()?;
    Ok(())
}

pub(crate) fn quote_sql_identifier(value: &str) -> String {
    format!("\"{}\"", value.replace('"', "\"\""))
}

pub(crate) fn shared_role_ddl_statements(sql: &str) -> Vec<String> {
    split_sql_statements(sql)
        .into_iter()
        .filter(|statement| {
            let normalized = normalize_executable_sql(statement);
            [
                "create role ",
                "create user ",
                "alter role ",
                "alter user ",
                "drop role ",
                "drop user ",
            ]
            .iter()
            .any(|prefix| normalized.starts_with(prefix))
                || is_role_membership_ddl(statement)
        })
        .collect()
}

#[derive(Clone, Debug)]
pub(crate) struct CancelMetadata {
    pub(crate) at: i64,
    pub(crate) connection_id: u64,
    pub(crate) reason: String,
    pub(crate) sqlstate: String,
}

#[derive(Clone, Copy, Debug)]
pub(crate) enum DbLockKind {
    Read,
    Write,
}

#[derive(Clone, Copy, Debug)]
pub(crate) enum QueryKind {
    Read,
    Write,
}

impl QueryKind {
    pub(crate) fn label(self) -> &'static str {
        match self {
            QueryKind::Read => "read",
            QueryKind::Write => "write",
        }
    }
}

pub(crate) struct InstrumentedDbReadGuard<'a> {
    pub(crate) server: &'a PgWireServer,
    pub(crate) guard: RwLockReadGuard<'a, BicDb>,
    pub(crate) acquired_at: Instant,
}

pub(crate) struct InstrumentedDbWriteGuard<'a> {
    pub(crate) server: &'a PgWireServer,
    pub(crate) guard: RwLockWriteGuard<'a, BicDb>,
    pub(crate) acquired_at: Instant,
}

pub(crate) struct ConnectionAdmission {
    pub(crate) server: Arc<PgWireServer>,
    pub(crate) source_ip: IpAddr,
    pub(crate) released: bool,
}

impl Drop for ConnectionAdmission {
    fn drop(&mut self) {
        if !self.released {
            self.server
                .active_connections
                .fetch_sub(1, Ordering::SeqCst);
            self.server.release_source_ip(self.source_ip);
            self.released = true;
        }
    }
}

pub(crate) struct ActiveQueryPermit {
    pub(crate) server: Arc<PgWireServer>,
    pub(crate) kind: QueryKind,
}

pub(crate) struct QueryQueueAdmission {
    pub(crate) server: Arc<PgWireServer>,
    pub(crate) kind: QueryKind,
    pub(crate) released: bool,
}

pub(crate) struct WriteAdmission {
    pub(crate) server: Arc<PgWireServer>,
    pub(crate) admitted_at: Instant,
    pub(crate) released: bool,
}

impl Drop for ActiveQueryPermit {
    fn drop(&mut self) {
        self.server
            .active_by_kind(self.kind)
            .fetch_sub(1, Ordering::SeqCst);
        self.server.active_queries.fetch_sub(1, Ordering::SeqCst);
    }
}

impl Drop for QueryQueueAdmission {
    fn drop(&mut self) {
        if !self.released {
            self.server.queued_queries.fetch_sub(1, Ordering::SeqCst);
            self.server
                .queued_by_kind(self.kind)
                .fetch_sub(1, Ordering::SeqCst);
            self.released = true;
        }
    }
}

impl Drop for WriteAdmission {
    fn drop(&mut self) {
        if !self.released {
            self.server.write_queue_depth.fetch_sub(1, Ordering::SeqCst);
            self.released = true;
        }
    }
}

/// RAII unregistration for a registered connection.
///
/// Cleanup used to be plain statements after `handle_client_loop`, so any
/// path that skipped them leaked the registry entry, the advisory locks, the
/// LISTEN/NOTIFY subscriptions and the cancel token — while the connection
/// count (already RAII, via `ConnectionAdmission`) kept reporting the
/// connection as live. Two such paths existed: an early `?` between
/// registration and the handler (a flush failure when the peer vanished mid
/// startup), and any panic unwinding through the handler. Those are the
/// "phantom connections" that survived their clients and then blocked
/// graceful shutdown until SIGKILL.
pub(crate) struct ConnectionRegistration {
    pub(crate) server: Arc<PgWireServer>,
    pub(crate) control_server: Arc<PgWireServer>,
    pub(crate) connection_id: u64,
    pub(crate) process_id: i32,
    pub(crate) secret_key: i32,
}

impl Drop for ConnectionRegistration {
    fn drop(&mut self) {
        cleanup_client(
            &self.server,
            &self.control_server,
            self.connection_id,
            self.process_id,
            self.secret_key,
        );
    }
}

pub(crate) struct InitializedPlainClient {
    /// Dropped last: unregisters the connection on every exit path.
    pub(crate) _registration: ConnectionRegistration,
    pub(crate) stream: TcpStream,
    pub(crate) server: Arc<PgWireServer>,
    pub(crate) control_server: Arc<PgWireServer>,
    pub(crate) state: ConnectionState,
    pub(crate) process_id: i32,
    pub(crate) secret_key: i32,
    pub(crate) admission: ConnectionAdmission,
}

pub(crate) struct InitializedBlockingClient {
    /// Dropped last: unregisters the connection on every exit path.
    pub(crate) _registration: ConnectionRegistration,
    pub(crate) stream: ClientStream,
    pub(crate) server: Arc<PgWireServer>,
    pub(crate) control_server: Arc<PgWireServer>,
    pub(crate) state: ConnectionState,
    pub(crate) process_id: i32,
    pub(crate) secret_key: i32,
    pub(crate) admission: ConnectionAdmission,
}

pub(crate) enum InitializedClient {
    Plain(Box<InitializedPlainClient>),
    Blocking(Box<InitializedBlockingClient>),
    Done,
}

impl Deref for InstrumentedDbReadGuard<'_> {
    type Target = BicDb;

    fn deref(&self) -> &Self::Target {
        &self.guard
    }
}

impl Deref for InstrumentedDbWriteGuard<'_> {
    type Target = BicDb;

    fn deref(&self) -> &Self::Target {
        &self.guard
    }
}

impl DerefMut for InstrumentedDbWriteGuard<'_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.guard
    }
}

impl Drop for InstrumentedDbReadGuard<'_> {
    fn drop(&mut self) {
        let hold_ns = duration_nanos_u64(self.acquired_at.elapsed());
        self.server.record_db_lock_hold(DbLockKind::Read, hold_ns);
    }
}

impl Drop for InstrumentedDbWriteGuard<'_> {
    fn drop(&mut self) {
        let hold_ns = duration_nanos_u64(self.acquired_at.elapsed());
        self.server.record_db_lock_hold(DbLockKind::Write, hold_ns);
    }
}

pub(crate) fn duration_nanos_u64(duration: Duration) -> u64 {
    duration.as_nanos().min(u128::from(u64::MAX)) as u64
}

/// Diagnostic (env `BICDB_COMMIT_PHASE_TRACE=1`): aggregate the write path's
/// phase timings (cumulative from statement start: execute -> wal-prep ->
/// admission -> db-read-lock -> commit-apply -> durable) and dump avg/max per
/// phase every ~10s. Row write locks taken during execution are held until the
/// commit-apply phase ends, so (commit_done - exec_done) is the extra lock-hold
/// window the pgwire pipeline adds on top of procedure execution.
pub(crate) fn commit_phase_trace(
    exec: Duration,
    wal: Duration,
    admit: Duration,
    rdb: Duration,
    commit: Duration,
    durable: Duration,
) {
    use std::sync::OnceLock;
    static ENABLED: OnceLock<bool> = OnceLock::new();
    if !*ENABLED.get_or_init(|| {
        std::env::var("BICDB_COMMIT_PHASE_TRACE")
            .map(|value| matches!(value.as_str(), "1" | "on" | "true" | "yes"))
            .unwrap_or(false)
    }) {
        return;
    }
    // sum-ns / max-ns per phase (deltas, not cumulative), plus a txn count.
    static AGG: OnceLock<[(AtomicU64, AtomicU64); 6]> = OnceLock::new();
    static COUNT: AtomicU64 = AtomicU64::new(0);
    static LAST_DUMP: OnceLock<parking_lot::Mutex<Instant>> = OnceLock::new();
    let agg = AGG.get_or_init(|| std::array::from_fn(|_| (AtomicU64::new(0), AtomicU64::new(0))));
    let deltas = [
        duration_nanos_u64(exec),
        duration_nanos_u64(wal.saturating_sub(exec)),
        duration_nanos_u64(admit.saturating_sub(wal)),
        duration_nanos_u64(rdb.saturating_sub(admit)),
        duration_nanos_u64(commit.saturating_sub(rdb)),
        duration_nanos_u64(durable.saturating_sub(commit)),
    ];
    for (slot, delta) in agg.iter().zip(deltas) {
        slot.0.fetch_add(delta, Ordering::Relaxed);
        atomic_max(&slot.1, delta);
    }
    let count = COUNT.fetch_add(1, Ordering::Relaxed) + 1;
    let last_dump = LAST_DUMP.get_or_init(|| parking_lot::Mutex::new(Instant::now()));
    let Some(mut last) = last_dump.try_lock() else {
        return;
    };
    if last.elapsed() < Duration::from_secs(10) {
        return;
    }
    *last = Instant::now();
    let names = ["exec", "walprep", "admit", "rdblock", "apply", "durable"];
    let mut line = format!("COMMIT_PHASES n={count}");
    for (name, (sum, max)) in names.iter().zip(agg.iter()) {
        let avg_us = sum.load(Ordering::Relaxed) / count.max(1) / 1_000;
        let max_us = max.load(Ordering::Relaxed) / 1_000;
        line.push_str(&format!(" {name}=avg{avg_us}us/max{max_us}us"));
    }
    eprintln!("{line}");
}

pub(crate) fn atomic_max(target: &AtomicU64, value: u64) {
    let mut current = target.load(Ordering::SeqCst);
    while value > current {
        match target.compare_exchange(current, value, Ordering::SeqCst, Ordering::SeqCst) {
            Ok(_) => return,
            Err(observed) => current = observed,
        }
    }
}

pub(crate) fn atomic_max_usize(target: &AtomicUsize, value: usize) {
    let mut current = target.load(Ordering::SeqCst);
    while value > current {
        match target.compare_exchange(current, value, Ordering::SeqCst, Ordering::SeqCst) {
            Ok(_) => return,
            Err(observed) => current = observed,
        }
    }
}

pub(crate) fn try_increment_bounded(
    current: &AtomicUsize,
    peak: &AtomicUsize,
    limit: usize,
) -> bool {
    let mut observed = current.load(Ordering::SeqCst);
    loop {
        if observed >= limit {
            return false;
        }
        match current.compare_exchange(observed, observed + 1, Ordering::SeqCst, Ordering::SeqCst) {
            Ok(_) => {
                atomic_max_usize(peak, observed + 1);
                return true;
            }
            Err(next) => observed = next,
        }
    }
}

pub(crate) fn queue_wait_percentiles(samples: &[u64]) -> (u64, u64, u64) {
    if samples.is_empty() {
        return (0, 0, 0);
    }
    let mut sorted = samples.to_vec();
    sorted.sort_unstable();
    (
        percentile_u64(&sorted, 0.50),
        percentile_u64(&sorted, 0.95),
        percentile_u64(&sorted, 0.99),
    )
}

pub(crate) fn percentile_u64(sorted: &[u64], percentile: f64) -> u64 {
    if sorted.is_empty() {
        return 0;
    }
    let index = ((sorted.len() - 1) as f64 * percentile).round() as usize;
    sorted[index.min(sorted.len() - 1)]
}

pub(crate) fn database_directory_size(path: &Path) -> io::Result<u64> {
    let metadata = fs::metadata(path)?;
    if metadata.is_file() {
        return Ok(metadata.len());
    }
    if !metadata.is_dir() {
        return Ok(0);
    }

    let mut total = 0u64;
    let mut stack = vec![path.to_path_buf()];
    while let Some(dir) = stack.pop() {
        for entry in fs::read_dir(dir)? {
            let entry = entry?;
            let metadata = entry.metadata()?;
            if metadata.is_dir() {
                stack.push(entry.path());
            } else if metadata.is_file() {
                total = total.saturating_add(metadata.len());
            }
        }
    }
    Ok(total)
}
