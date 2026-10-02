//! Flashback: durable, SCN-ordered row history (Oracle Flashback Query /
//! Flashback Data Archive semantics).
//!
//! A collection with flashback enabled gets a companion history collection
//! (`__bicdb_fb_<name>`). Every committed change to a tracked row adds one
//! history row `<rid>@<scn>` holding the row image (or the deleted image) and
//! the operation. History rows are injected into the *same* transaction as the
//! data change, after commit-time conflict repair, so they are validated,
//! logged, applied, replicated, checkpointed and recovered exactly like the
//! data itself: there is no window in which data commits without its history.
//!
//! # SCN clock
//!
//! The engine's `commit_seq` rebases after checkpoints, so it cannot name a
//! point in time durably. Flashback owns a hybrid logical clock instead:
//! `scn = max(previous + 1, unix_micros)`. SCNs therefore increase across
//! restarts and *are* their commit time in Unix microseconds (PostgreSQL's
//! timestamp resolution): `AS OF TIMESTAMP t` is exactly `AS OF SCN t`. Only
//! when more than one tracked commit lands in the same microsecond does an SCN
//! run ahead of the wall clock, by at most the number of such commits.
//!
//! The clock mutex is held for the whole commit of any transaction that writes
//! a tracked collection. Tracked commits are therefore serialized, and their
//! SCN order equals their visibility order: every `AS OF` answer is a state the
//! tracked tables actually passed through. Transactions that touch no tracked
//! collection never take the mutex.
//!
//! When a reader asks for an SCN at or after the newest assigned one, the read
//! "fences" the clock (`last_scn = scn`), so no later commit can receive an SCN
//! at or below a point that has already been read: repeated `AS OF` queries at
//! the same point always return the same rows.
use super::*;

/// Name prefix of history collections. Collections with this prefix are never
/// tracked themselves.
pub const FLASHBACK_HISTORY_PREFIX: &str = "__bicdb_fb_";

const FLASHBACK_WRITE_BATCH: usize = 1_000;
const MAX_COLLECTION_NAME_BYTES: usize = 255;

/// Durable per-collection flashback configuration (part of the catalog).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct FlashbackConfig {
    /// History older than this is purged by [`BicDb::purge_expired_flashback`].
    /// Zero keeps history until it is purged explicitly.
    pub retention_secs: u64,
    /// Oldest SCN that can be queried. Raised by enabling and by purges.
    pub since_scn: u64,
    /// Wall-clock time flashback was (last) enabled, in Unix microseconds.
    pub enabled_at_micros: i64,
    /// False while the baseline copy of existing rows is being written. Reads
    /// are refused until it completes; an interrupted baseline is rebuilt at
    /// open with a new `since_scn`.
    #[serde(default)]
    pub baseline_complete: bool,
}

/// A point in history.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FlashbackPoint {
    Scn(u64),
    /// Unix microseconds.
    TimestampMicros(i64),
}

/// The operation that produced a row version.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum FlashbackOperation {
    /// The row already existed when flashback was enabled (or its earlier
    /// history was purged); its creation time is unknown.
    Baseline,
    Insert,
    Update,
    Delete,
}

impl FlashbackOperation {
    /// Oracle `VERSIONS_OPERATION` code: `I`, `U`, `D`, or `None` for baseline.
    pub fn code(self) -> Option<&'static str> {
        match self {
            Self::Baseline => None,
            Self::Insert => Some("I"),
            Self::Update => Some("U"),
            Self::Delete => Some("D"),
        }
    }

    fn tag(self) -> &'static str {
        match self {
            Self::Baseline => "B",
            Self::Insert => "I",
            Self::Update => "U",
            Self::Delete => "D",
        }
    }

    fn from_tag(tag: &str) -> Result<Self> {
        match tag {
            "B" => Ok(Self::Baseline),
            "I" => Ok(Self::Insert),
            "U" => Ok(Self::Update),
            "D" => Ok(Self::Delete),
            other => Err(BicDbError::Flashback(format!(
                "corrupt flashback history operation `{other}`"
            ))),
        }
    }
}

/// One row version returned by [`BicDb::flashback_versions_unchecked`]
/// (Oracle `VERSIONS BETWEEN`).
#[derive(Clone, Debug, PartialEq)]
pub struct FlashbackVersion {
    /// The row image of this version. For a delete, the row as it was deleted.
    pub record: Record,
    /// SCN that created this version; `None` when it was created before the
    /// lower bound or before history began (Oracle `VERSIONS_STARTSCN`).
    pub start_scn: Option<u64>,
    /// SCN that ended this version; `None` while it is still current and for
    /// delete versions (Oracle `VERSIONS_ENDSCN`).
    pub end_scn: Option<u64>,
    pub operation: FlashbackOperation,
    /// Engine transaction id of the change (`VERSIONS_XID`); zero for baseline.
    pub xid: u64,
}

#[derive(Debug, Default)]
pub(crate) struct FlashbackClock {
    last_scn: u64,
    initialized: bool,
}

struct HistoryEntry {
    rid: String,
    scn: u64,
    op: FlashbackOperation,
    xid: u64,
    row: Option<Record>,
}

/// PostgreSQL SQLSTATE for a [`BicDbError::Flashback`] message: `72000`
/// (snapshot_too_old) for purged or pre-archive points, `22023`
/// (invalid_parameter_value) for future points, `55000`
/// (object_not_in_prerequisite_state) otherwise (not archived, initializing).
pub fn flashback_error_sqlstate(message: &str) -> &'static str {
    if message.starts_with("snapshot too old") {
        "72000"
    } else if message.contains("is in the future")
        || message.contains("precedes the Unix epoch")
        || message.contains("lower bound")
    {
        "22023"
    } else {
        "55000"
    }
}

/// The history collection that stores `source`'s versions.
pub fn flashback_history_collection(source: &str) -> String {
    let name = format!("{FLASHBACK_HISTORY_PREFIX}{source}");
    if name.len() <= MAX_COLLECTION_NAME_BYTES {
        name
    } else {
        format!(
            "{FLASHBACK_HISTORY_PREFIX}h{}",
            hex::encode(sha2::Sha256::digest(source.as_bytes()))
        )
    }
}

pub fn is_flashback_history_collection(name: &str) -> bool {
    name.starts_with(FLASHBACK_HISTORY_PREFIX)
}

/// Commit time of an SCN, in Unix microseconds.
pub fn scn_to_unix_micros(scn: u64) -> i64 {
    i64::try_from(scn).unwrap_or(i64::MAX)
}

/// The SCN of a Unix microsecond: `AS OF TIMESTAMP t` == `AS OF SCN
/// unix_micros_to_scn(t)`.
pub fn unix_micros_to_scn(micros: i64) -> u64 {
    micros.max(0) as u64
}

/// `YYYY-MM-DD HH:MM:SS.ffffff UTC` for an SCN, for error messages.
fn scn_utc_text(scn: u64) -> String {
    let micros = scn_to_unix_micros(scn);
    let secs = micros.div_euclid(1_000_000);
    let fraction = micros.rem_euclid(1_000_000);
    let days = secs.div_euclid(86_400);
    let day_secs = secs.rem_euclid(86_400);
    // Howard Hinnant's days-to-civil.
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = yoe + era * 400 + i64::from(month <= 2);
    format!(
        "{year:04}-{month:02}-{day:02} {:02}:{:02}:{:02}.{fraction:06} UTC",
        day_secs / 3_600,
        (day_secs / 60) % 60,
        day_secs % 60
    )
}

fn unix_timestamp_micros() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| i64::try_from(duration.as_micros()).unwrap_or(i64::MAX))
        .unwrap_or_default()
}

fn wall_clock_scn() -> u64 {
    unix_micros_to_scn(unix_timestamp_micros())
}

fn history_record_id(rid: &str, scn: u64) -> String {
    format!("{rid}@{scn:020}")
}

fn history_record(
    rid: &str,
    scn: u64,
    op: FlashbackOperation,
    xid: u64,
    row: Option<&Record>,
) -> Result<Record> {
    let row = match row {
        Some(row) => serde_json::to_value(row)?,
        None => Value::Null,
    };
    Ok(Record {
        id: history_record_id(rid, scn),
        vector: None,
        metadata: json!({
            "rid": rid,
            "scn": scn,
            "op": op.tag(),
            "xid": xid,
            "row": row,
        }),
        geometry: None,
        timestamp: Some(scn_to_unix_micros(scn)),
        payload: None,
    })
}

fn decode_history(record: &Record) -> Result<HistoryEntry> {
    let corrupt =
        || BicDbError::Flashback(format!("corrupt flashback history row `{}`", record.id));
    let metadata = &record.metadata;
    let rid = metadata
        .get("rid")
        .and_then(Value::as_str)
        .ok_or_else(corrupt)?
        .to_string();
    let scn = metadata
        .get("scn")
        .and_then(Value::as_u64)
        .ok_or_else(corrupt)?;
    let op = FlashbackOperation::from_tag(
        metadata
            .get("op")
            .and_then(Value::as_str)
            .ok_or_else(corrupt)?,
    )?;
    let xid = metadata.get("xid").and_then(Value::as_u64).unwrap_or(0);
    let row = match metadata.get("row") {
        None | Some(Value::Null) => None,
        Some(value) => Some(serde_json::from_value(value.clone())?),
    };
    Ok(HistoryEntry {
        rid,
        scn,
        op,
        xid,
        row,
    })
}

impl BicDb {
    // ---------------------------------------------------------------- commit

    /// Commit entry point. Transactions that write a flashback-tracked
    /// collection take the flashback clock for their whole commit and carry
    /// their history rows; all others go straight to the commit body.
    pub(crate) fn commit_transaction(&self, tx: &mut Transaction) -> Result<u64> {
        if self.flashback_tracked.load(AtomicOrdering::Acquire) == 0
            || tx.bypass_commit_admission
            || tx.state != TxState::Pending
            || !self.flashback_tx_touches_tracked(tx)?
        {
            return self.commit_transaction_inner(tx, None);
        }
        let mut clock = self.flashback_clock.lock();
        self.flashback_ensure_clock(&mut clock)?;
        let scn = clock.last_scn.saturating_add(1).max(wall_clock_scn());
        let base_len = tx.writes.len();
        let result = self.commit_transaction_inner(tx, Some(scn));
        match &result {
            Ok(_) => clock.last_scn = scn,
            Err(_) => {
                // The transaction may be retried or rolled back by its owner;
                // it must not keep history rows it never committed.
                if tx.writes.len() > base_len {
                    tx.writes.truncate(base_len);
                    tx.rebuild_write_index();
                }
            }
        }
        result
    }

    fn flashback_tx_touches_tracked(&self, tx: &Transaction) -> Result<bool> {
        let mut seen = FxHashSet::default();
        for write in &tx.writes {
            if is_flashback_history_collection(&write.collection) {
                // Already carries history (an import or replay): never capture twice.
                return Ok(false);
            }
            if seen.insert(write.collection.as_str())
                && self.flashback_is_tracked(&write.collection)
            {
                return Ok(true);
            }
        }
        Ok(false)
    }

    fn flashback_is_tracked(&self, collection: &str) -> bool {
        self.collections
            .get(collection)
            .is_some_and(|state| state.read().meta.flashback.is_some())
    }

    /// Appends one history row per distinct tracked record written by `tx`
    /// (the final write wins), stamped with `scn`.
    pub(super) fn flashback_inject_history(&self, tx: &mut Transaction, scn: u64) -> Result<()> {
        let mut latest: BTreeMap<(String, String), usize> = BTreeMap::new();
        let mut tracked: FxHashMap<String, bool> = FxHashMap::default();
        for (idx, write) in tx.writes.iter().enumerate() {
            let is_tracked = *tracked
                .entry(write.collection.clone())
                .or_insert_with(|| self.flashback_is_tracked(&write.collection));
            if is_tracked {
                latest.insert((write.collection.clone(), write.record_id.clone()), idx);
            }
        }
        if latest.is_empty() {
            return Ok(());
        }
        let xid = tx.id.0;
        let timestamp = unix_timestamp();
        let mut history = Vec::with_capacity(latest.len());
        for ((collection, rid), idx) in latest {
            let write = &tx.writes[idx];
            let previous = self.get_unchecked(&collection, &rid)?;
            let record = match write.op {
                TxWriteOp::Upsert => {
                    let row = write.record()?.ok_or_else(|| {
                        BicDbError::Flashback(format!(
                            "upsert of {collection}/{rid} carries no record image"
                        ))
                    })?;
                    let op = if previous.is_some() {
                        FlashbackOperation::Update
                    } else {
                        FlashbackOperation::Insert
                    };
                    history_record(&rid, scn, op, xid, Some(row.as_ref()))?
                }
                TxWriteOp::Delete => match previous.as_deref() {
                    Some(previous) => {
                        history_record(&rid, scn, FlashbackOperation::Delete, xid, Some(previous))?
                    }
                    // Deleting a row that does not exist changes nothing.
                    None => continue,
                },
            };
            history.push(TxWrite {
                collection: flashback_history_collection(&collection),
                op: TxWriteOp::Upsert,
                record_id: record.id.clone(),
                record: OnceLock::from(Some(Arc::new(record))),
                timestamp,
                statement_snapshot: 0,
                repair: None,
                stored: None,
                previous: None,
                previous_stored: None,
                changed: None,
            });
        }
        tx.push_writes(history);
        Ok(())
    }

    // ------------------------------------------------------------------ clock

    /// Initializes the clock above every SCN already recorded, so a wall clock
    /// that moved backwards across a restart cannot reuse an SCN.
    fn flashback_ensure_clock(&self, clock: &mut FlashbackClock) -> Result<()> {
        if clock.initialized {
            return Ok(());
        }
        let mut high = clock.last_scn;
        for (name, state) in &self.collections {
            let Some(config) = state.read().meta.flashback.clone() else {
                continue;
            };
            high = high.max(config.since_scn);
            let history = flashback_history_collection(name);
            if self.collections.contains_key(&history) {
                for record in self.scan_collection_unchecked(&history)? {
                    if let Some(scn) = record.metadata.get("scn").and_then(Value::as_u64) {
                        high = high.max(scn);
                    }
                }
            }
        }
        clock.last_scn = high;
        clock.initialized = true;
        Ok(())
    }

    /// The current SCN. Every change committed so far has an SCN at or below
    /// it, and none committed later will, so `AS OF SCN current_scn()` is
    /// stable.
    pub fn current_scn(&self) -> Result<u64> {
        let mut clock = self.flashback_clock.lock();
        self.flashback_ensure_clock(&mut clock)?;
        let scn = clock.last_scn.max(wall_clock_scn());
        clock.last_scn = scn;
        Ok(scn)
    }

    /// Resolves a point to an SCN, rejecting the future and fencing the clock
    /// (see the module docs).
    fn flashback_resolve_point(&self, point: FlashbackPoint) -> Result<u64> {
        let scn = match point {
            FlashbackPoint::Scn(scn) => scn,
            FlashbackPoint::TimestampMicros(micros) => {
                if micros < 0 {
                    return Err(BicDbError::Flashback(
                        "flashback timestamp precedes the Unix epoch".to_string(),
                    ));
                }
                unix_micros_to_scn(micros)
            }
        };
        let mut clock = self.flashback_clock.lock();
        self.flashback_ensure_clock(&mut clock)?;
        let ceiling = clock.last_scn.max(wall_clock_scn());
        if scn > ceiling {
            let current = clock.last_scn.max(wall_clock_scn());
            return Err(BicDbError::Flashback(format!(
                "SCN {scn} ({}) is in the future (current SCN is {current}, {})",
                scn_utc_text(scn),
                scn_utc_text(current)
            )));
        }
        clock.last_scn = clock.last_scn.max(scn);
        Ok(scn)
    }

    // ------------------------------------------------------- administration

    /// Drops a collection. Dropping a flashback-tracked collection also drops
    /// its history.
    pub fn drop_collection(&mut self, name: &str) -> Result<bool> {
        let tracked = self
            .collections
            .get(name)
            .is_some_and(|state| state.read().meta.flashback.is_some());
        let dropped = self.drop_collection_inner(name)?;
        if tracked {
            self.flashback_refresh_tracked();
            let history = flashback_history_collection(name);
            if self.collections.contains_key(&history) {
                self.drop_collection_inner(&history)?;
            }
        }
        Ok(dropped)
    }

    /// Enables flashback history for `collection` (Oracle `ALTER TABLE ...
    /// FLASHBACK ARCHIVE`). Existing rows are copied into history as the
    /// baseline, so the table can be queried as of any SCN from now on. If
    /// flashback is already enabled only the retention changes.
    pub fn enable_flashback(
        &mut self,
        collection: &str,
        retention_secs: u64,
    ) -> Result<FlashbackConfig> {
        self.ensure_writable("enable flashback")?;
        validate_collection_name(collection)?;
        if is_flashback_history_collection(collection) {
            return Err(BicDbError::Flashback(format!(
                "`{collection}` is a flashback history collection"
            )));
        }
        let existing = self.collection_state(collection)?.meta.flashback.clone();
        if let Some(mut config) = existing {
            config.retention_secs = retention_secs;
            self.collection_state_mut(collection)?.meta.flashback = Some(config.clone());
            self.persist_catalog()?;
            return Ok(config);
        }
        let history = flashback_history_collection(collection);
        if self.collections.contains_key(&history) {
            // Left over from an earlier, disabled history: start clean.
            self.drop_collection_inner(&history)?;
        }
        self.create_collection(&history)?;
        let since_scn = {
            let mut clock = self.flashback_clock.lock();
            self.flashback_ensure_clock(&mut clock)?;
            let scn = clock.last_scn.saturating_add(1).max(wall_clock_scn());
            clock.last_scn = scn;
            scn
        };
        let config = FlashbackConfig {
            retention_secs,
            since_scn,
            enabled_at_micros: unix_timestamp_micros(),
            baseline_complete: false,
        };
        self.collection_state_mut(collection)?.meta.flashback = Some(config);
        self.persist_catalog()?;
        self.flashback_refresh_tracked();
        self.flashback_write_baseline(collection, since_scn)
    }

    /// `&mut self` excludes every concurrent commit, so the scan below is the
    /// table exactly as of `since_scn`; later commits carry larger SCNs.
    fn flashback_write_baseline(
        &mut self,
        collection: &str,
        since_scn: u64,
    ) -> Result<FlashbackConfig> {
        let history = flashback_history_collection(collection);
        let records = self.scan_collection_unchecked(collection)?;
        for chunk in records.chunks(FLASHBACK_WRITE_BATCH) {
            let rows = chunk
                .iter()
                .map(|record| {
                    history_record(
                        &record.id,
                        since_scn,
                        FlashbackOperation::Baseline,
                        0,
                        Some(record),
                    )
                })
                .collect::<Result<Vec<_>>>()?;
            let mut tx = self.begin_transaction()?;
            tx.write_upserts_unchecked(&history, rows)?;
            tx.commit()?;
        }
        let state = self.collection_state_mut(collection)?;
        let config = state.meta.flashback.as_mut().ok_or_else(|| {
            BicDbError::Flashback(format!("flashback was disabled on `{collection}`"))
        })?;
        config.since_scn = since_scn;
        config.baseline_complete = true;
        let config = config.clone();
        self.persist_catalog()?;
        Ok(config)
    }

    /// Disables flashback for `collection` and discards its history (Oracle
    /// `ALTER TABLE ... NO FLASHBACK ARCHIVE`). Returns false if it was not
    /// enabled.
    pub fn disable_flashback(&mut self, collection: &str) -> Result<bool> {
        self.ensure_writable("disable flashback")?;
        validate_collection_name(collection)?;
        let state = self.collection_state_mut(collection)?;
        if state.meta.flashback.take().is_none() {
            return Ok(false);
        }
        self.persist_catalog()?;
        self.flashback_refresh_tracked();
        let history = flashback_history_collection(collection);
        if self.collections.contains_key(&history) {
            self.drop_collection_inner(&history)?;
        }
        Ok(true)
    }

    /// Flashback configuration of `collection`, if enabled.
    pub fn flashback_config(&self, collection: &str) -> Result<Option<FlashbackConfig>> {
        Ok(self.collection_state(collection)?.meta.flashback.clone())
    }

    /// Every collection with flashback enabled, sorted by name.
    pub fn flashback_collections(&self) -> Vec<(String, FlashbackConfig)> {
        let mut tracked = self
            .collections
            .iter()
            .filter_map(|(name, state)| {
                state
                    .read()
                    .meta
                    .flashback
                    .clone()
                    .map(|config| (name.clone(), config))
            })
            .collect::<Vec<_>>();
        tracked.sort_by(|a, b| a.0.cmp(&b.0));
        tracked
    }

    fn flashback_refresh_tracked(&self) {
        let count = self
            .collections
            .values()
            .filter(|state| state.read().meta.flashback.is_some())
            .count();
        self.flashback_tracked.store(count, AtomicOrdering::Release);
    }

    /// Called once at the end of open: counts tracked collections and rebuilds
    /// any baseline a crash interrupted.
    pub(super) fn flashback_after_open(&mut self) -> Result<()> {
        self.flashback_refresh_tracked();
        let interrupted = self
            .flashback_collections()
            .into_iter()
            .filter(|(_, config)| !config.baseline_complete)
            .map(|(name, _)| name)
            .collect::<Vec<_>>();
        if interrupted.is_empty() || self.ensure_writable("rebuild flashback baseline").is_err() {
            return Ok(());
        }
        for collection in interrupted {
            let history = flashback_history_collection(&collection);
            if !self.collections.contains_key(&history) {
                self.create_collection(&history)?;
            }
            let since_scn = {
                let mut clock = self.flashback_clock.lock();
                self.flashback_ensure_clock(&mut clock)?;
                let scn = clock.last_scn.saturating_add(1).max(wall_clock_scn());
                clock.last_scn = scn;
                scn
            };
            if let Some(config) = self
                .collection_state_mut(&collection)?
                .meta
                .flashback
                .as_mut()
            {
                config.since_scn = since_scn;
            }
            self.persist_catalog()?;
            self.flashback_write_baseline(&collection, since_scn)?;
        }
        Ok(())
    }

    /// Discards history that only describes the table before `before` (default:
    /// now minus the configured retention). Afterwards the table can be queried
    /// as of `before` or later. Returns the number of history rows removed.
    pub fn purge_flashback(
        &mut self,
        collection: &str,
        before: Option<FlashbackPoint>,
    ) -> Result<usize> {
        if self.flashback_raise_floor(collection, before)?.is_none() {
            return Ok(0);
        }
        self.flashback_delete_obsolete(collection)
    }

    /// Purge phase 1 (catalog change, needs exclusive access): raises the
    /// queryable floor of `collection` to `before` (default: retention). From
    /// then on no reader may ask for a point phase 2 is about to make
    /// unanswerable. Returns the new floor, or `None` if it did not move.
    pub fn flashback_raise_floor(
        &mut self,
        collection: &str,
        before: Option<FlashbackPoint>,
    ) -> Result<Option<u64>> {
        self.ensure_writable("purge flashback history")?;
        let config = self.flashback_ready_config(collection)?;
        let horizon = match before {
            Some(point) => self.flashback_resolve_point(point)?,
            None if config.retention_secs == 0 => return Ok(None),
            None => {
                let retention_micros =
                    i64::try_from(config.retention_secs.saturating_mul(1_000_000))
                        .unwrap_or(i64::MAX);
                unix_micros_to_scn(unix_timestamp_micros().saturating_sub(retention_micros))
            }
        };
        if horizon <= config.since_scn {
            return Ok(None);
        }
        if let Some(config) = self
            .collection_state_mut(collection)?
            .meta
            .flashback
            .as_mut()
        {
            config.since_scn = horizon;
        }
        self.persist_catalog()?;
        Ok(Some(horizon))
    }

    /// Purge phase 2 (ordinary transactions, runs alongside other commits):
    /// deletes history rows that no query at or after the floor can need. For
    /// each row, only the newest version at or before the floor stays, and not
    /// even that one when it is a delete. Idempotent: a crash between the
    /// phases is healed by running this again. Returns rows removed.
    pub fn flashback_delete_obsolete(&self, collection: &str) -> Result<usize> {
        let config = self.flashback_ready_config(collection)?;
        let horizon = config.since_scn;
        let history = flashback_history_collection(collection);
        let mut per_row: FxHashMap<String, Vec<(u64, FlashbackOperation, String)>> =
            FxHashMap::default();
        for record in self.scan_collection_unchecked(&history)? {
            let entry = decode_history(&record)?;
            if entry.scn <= horizon {
                per_row
                    .entry(entry.rid)
                    .or_default()
                    .push((entry.scn, entry.op, record.id));
            }
        }
        let mut obsolete = Vec::new();
        for (_, mut entries) in per_row {
            entries.sort_by_key(|(scn, _, _)| *scn);
            let (_, last_op, last_id) = entries.pop().expect("grouped rows are non-empty");
            obsolete.extend(entries.into_iter().map(|(_, _, id)| id));
            // A row deleted before the floor has no state left to answer.
            if last_op == FlashbackOperation::Delete {
                obsolete.push(last_id);
            }
        }
        obsolete.sort();
        for chunk in obsolete.chunks(FLASHBACK_WRITE_BATCH) {
            let mut tx = self.begin_transaction()?;
            for id in chunk {
                tx.delete(&history, id)?;
            }
            tx.commit()?;
        }
        Ok(obsolete.len())
    }

    /// Applies every tracked collection's retention. Returns rows removed.
    pub fn purge_expired_flashback(&mut self) -> Result<usize> {
        let mut removed = 0;
        for collection in self.flashback_raise_expired_floors()? {
            removed += self.flashback_delete_obsolete(&collection)?;
        }
        Ok(removed)
    }

    /// Phase 1 of [`Self::purge_expired_flashback`] for every tracked
    /// collection with a retention. Returns the collections whose floor moved;
    /// run [`Self::flashback_delete_obsolete`] on each (no exclusive access
    /// needed).
    pub fn flashback_raise_expired_floors(&mut self) -> Result<Vec<String>> {
        let mut raised = Vec::new();
        for (collection, config) in self.flashback_collections() {
            if config.retention_secs > 0
                && config.baseline_complete
                && self.flashback_raise_floor(&collection, None)?.is_some()
            {
                raised.push(collection);
            }
        }
        Ok(raised)
    }

    // ------------------------------------------------------------------ reads

    fn flashback_ready_config(&self, collection: &str) -> Result<FlashbackConfig> {
        let config = self.flashback_config(collection)?.ok_or_else(|| {
            BicDbError::Flashback(format!(
                "flashback is not enabled for `{collection}` (ALTER TABLE ... FLASHBACK ARCHIVE)"
            ))
        })?;
        if !config.baseline_complete {
            return Err(BicDbError::Flashback(format!(
                "flashback history for `{collection}` is still being initialized"
            )));
        }
        Ok(config)
    }

    fn flashback_check_available(
        collection: &str,
        config: &FlashbackConfig,
        scn: u64,
    ) -> Result<()> {
        if scn < config.since_scn {
            return Err(BicDbError::Flashback(format!(
                "snapshot too old: `{collection}` has history from SCN {} ({}); requested SCN {scn} ({})",
                config.since_scn,
                scn_utc_text(config.since_scn),
                scn_utc_text(scn)
            )));
        }
        Ok(())
    }

    fn flashback_history_entries(&self, collection: &str) -> Result<Vec<HistoryEntry>> {
        self.scan_collection_unchecked(&flashback_history_collection(collection))?
            .iter()
            .map(decode_history)
            .collect()
    }

    /// `collection` as of `point`, with no authorization. Rows are sorted by id.
    pub fn flashback_rows_unchecked(
        &self,
        collection: &str,
        point: FlashbackPoint,
    ) -> Result<Vec<Record>> {
        let config = self.flashback_ready_config(collection)?;
        let scn = self.flashback_resolve_point(point)?;
        Self::flashback_check_available(collection, &config, scn)?;
        let mut best: FxHashMap<String, HistoryEntry> = FxHashMap::default();
        for entry in self.flashback_history_entries(collection)? {
            if entry.scn > scn {
                continue;
            }
            match best.get(&entry.rid) {
                Some(current) if current.scn >= entry.scn => {}
                _ => {
                    best.insert(entry.rid.clone(), entry);
                }
            }
        }
        let mut rows = best
            .into_values()
            .filter(|entry| entry.op != FlashbackOperation::Delete)
            .filter_map(|entry| entry.row)
            .collect::<Vec<_>>();
        rows.sort_by(|a, b| a.id.cmp(&b.id));
        Ok(rows)
    }

    /// [`Self::flashback_rows_unchecked`] behind the same legacy access gate as
    /// [`Self::scan_collection`].
    pub fn flashback_rows(&self, collection: &str, point: FlashbackPoint) -> Result<Vec<Record>> {
        self.ensure_unprotected_legacy_access(collection, SecureOperation::Read)?;
        self.flashback_rows_unchecked(collection, point)
    }

    /// Historical rows filtered and projected by `collection`'s *current*
    /// policy for `ctx`, exactly as [`Self::scan_collection_with_context`]
    /// filters current rows.
    pub fn flashback_rows_with_context(
        &self,
        ctx: &SecurityContext,
        collection: &str,
        point: FlashbackPoint,
    ) -> Result<Vec<Record>> {
        let records = self.flashback_rows_unchecked(collection, point)?;
        self.flashback_filter_with_context(ctx, collection, records)
    }

    fn flashback_filter_with_context(
        &self,
        ctx: &SecurityContext,
        collection: &str,
        records: Vec<Record>,
    ) -> Result<Vec<Record>> {
        let state = self.collection_state(collection)?;
        let Some(policy) = state.meta.policy.as_ref() else {
            return Ok(records);
        };
        Self::authorize_read_without_audit(collection, policy, ctx)?;
        let mut visible = Vec::new();
        for record in records {
            if Self::record_visible_to_policy(policy, ctx, &record)? {
                visible.push(protected_data::project_record(
                    &self.sync_state.node_id.to_string(),
                    &self.path,
                    collection,
                    policy,
                    &ctx.roles,
                    &record,
                    ctx.bypass_policy.is_some(),
                )?);
            }
        }
        Ok(visible)
    }

    /// Every version of every row that existed at some point in `[from, to]`
    /// (Oracle `VERSIONS BETWEEN`). `None` bounds mean the oldest retained
    /// history and the current SCN (`MINVALUE` / `MAXVALUE`). Versions are
    /// ordered by row id, then by start SCN.
    pub fn flashback_versions_unchecked(
        &self,
        collection: &str,
        from: Option<FlashbackPoint>,
        to: Option<FlashbackPoint>,
    ) -> Result<Vec<FlashbackVersion>> {
        let config = self.flashback_ready_config(collection)?;
        let low = match from {
            Some(point) => {
                let scn = self.flashback_resolve_point(point)?;
                Self::flashback_check_available(collection, &config, scn)?;
                scn
            }
            None => config.since_scn,
        };
        let high = match to {
            Some(point) => self.flashback_resolve_point(point)?,
            None => self.current_scn()?,
        };
        if low > high {
            return Err(BicDbError::Flashback(format!(
                "VERSIONS BETWEEN lower bound {low} is after upper bound {high}"
            )));
        }
        let mut per_row: BTreeMap<String, Vec<HistoryEntry>> = BTreeMap::new();
        for entry in self.flashback_history_entries(collection)? {
            per_row.entry(entry.rid.clone()).or_default().push(entry);
        }
        let mut versions = Vec::new();
        for (_, mut entries) in per_row {
            entries.sort_by_key(|entry| entry.scn);
            let ends = entries
                .iter()
                .skip(1)
                .map(|entry| Some(entry.scn))
                .chain(std::iter::once(None))
                .collect::<Vec<_>>();
            for (entry, end) in entries.into_iter().zip(ends) {
                let starts_in_range = entry.scn <= high;
                let alive_in_range = end.is_none_or(|end| end > low);
                if !starts_in_range || !alive_in_range {
                    continue;
                }
                let Some(record) = entry.row else {
                    continue;
                };
                let start_scn = (entry.op != FlashbackOperation::Baseline && entry.scn >= low)
                    .then_some(entry.scn);
                let end_scn = if entry.op == FlashbackOperation::Delete {
                    None
                } else {
                    end
                };
                versions.push(FlashbackVersion {
                    record,
                    start_scn,
                    end_scn,
                    operation: entry.op,
                    xid: entry.xid,
                });
            }
        }
        Ok(versions)
    }

    /// [`Self::flashback_versions_unchecked`] filtered and projected by the
    /// collection's current policy for `ctx`.
    pub fn flashback_versions_with_context(
        &self,
        ctx: &SecurityContext,
        collection: &str,
        from: Option<FlashbackPoint>,
        to: Option<FlashbackPoint>,
    ) -> Result<Vec<FlashbackVersion>> {
        let versions = self.flashback_versions_unchecked(collection, from, to)?;
        let state = self.collection_state(collection)?;
        let Some(policy) = state.meta.policy.as_ref() else {
            return Ok(versions);
        };
        Self::authorize_read_without_audit(collection, policy, ctx)?;
        let mut visible = Vec::new();
        for mut version in versions {
            if Self::record_visible_to_policy(policy, ctx, &version.record)? {
                version.record = protected_data::project_record(
                    &self.sync_state.node_id.to_string(),
                    &self.path,
                    collection,
                    policy,
                    &ctx.roles,
                    &version.record,
                    ctx.bypass_policy.is_some(),
                )?;
                visible.push(version);
            }
        }
        Ok(visible)
    }

    /// [`Self::flashback_versions_unchecked`] behind the legacy access gate.
    pub fn flashback_versions(
        &self,
        collection: &str,
        from: Option<FlashbackPoint>,
        to: Option<FlashbackPoint>,
    ) -> Result<Vec<FlashbackVersion>> {
        self.ensure_unprotected_legacy_access(collection, SecureOperation::Read)?;
        self.flashback_versions_unchecked(collection, from, to)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scn_utc_text_matches_the_civil_calendar() {
        for (micros, expected) in [
            (0, "1970-01-01 00:00:00.000000 UTC"),
            (1_790_943_628_123_456, "2026-10-02 12:20:28.123456 UTC"),
            (951_782_400_000_001, "2000-02-29 00:00:00.000001 UTC"),
            (4_102_444_799_999_999, "2099-12-31 23:59:59.999999 UTC"),
        ] {
            assert_eq!(scn_utc_text(unix_micros_to_scn(micros)), expected);
        }
    }

    #[test]
    fn history_ids_and_records_round_trip() {
        let row = Record::new("a@b").with_metadata(json!({ "v": 1 }));
        let record = history_record("a@b", 42, FlashbackOperation::Update, 7, Some(&row)).unwrap();
        assert_eq!(record.id, "a@b@00000000000000000042");
        let entry = decode_history(&record).unwrap();
        assert_eq!(entry.rid, "a@b");
        assert_eq!(entry.scn, 42);
        assert_eq!(entry.op, FlashbackOperation::Update);
        assert_eq!(entry.xid, 7);
        assert_eq!(entry.row, Some(row));
    }
}
