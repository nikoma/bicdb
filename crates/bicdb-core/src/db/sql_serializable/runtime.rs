use super::{
    Dependencies, KeyRange, PendingCommit, ReadKeys, ReadScope, SnapshotSafety, WriteFootprint,
};
use crate::db::{
    BicDb, BicDbError, IndexDefinition, IndexKind, IndexRecordMutation, IndexValue, Result, RowId,
    Transaction,
};
use crate::CancellationToken;
use parking_lot::{Condvar, Mutex, MutexGuard};
use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};
use std::ops::Bound;
use std::sync::Arc;
use std::time::Duration;

fn index_identity(definition: &IndexDefinition) -> Result<String> {
    Ok(serde_json::to_string(definition)?)
}

fn next_key_prefix(mut key: Vec<u8>) -> Bound<Vec<u8>> {
    while let Some(last) = key.pop() {
        if last < u8::MAX {
            key.push(last + 1);
            return Bound::Excluded(key);
        }
    }
    Bound::Unbounded
}

/// Keep the incarnation allocated until its last predicate or write is retired.
#[derive(Clone, Debug)]
struct CollectionIdentity(Arc<()>);

impl PartialEq for CollectionIdentity {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
}

impl Eq for CollectionIdentity {}

impl PartialOrd for CollectionIdentity {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for CollectionIdentity {
    fn cmp(&self, other: &Self) -> Ordering {
        Arc::as_ptr(&self.0).cmp(&Arc::as_ptr(&other.0))
    }
}

#[derive(Debug, Default)]
pub(in crate::db) struct SerializableTransactions {
    dependencies: Mutex<Dependencies<CollectionIdentity>>,
    changed: Condvar,
}

#[derive(Debug)]
pub(in crate::db) struct Registration {
    registry: Arc<SerializableTransactions>,
    id: u64,
    pub(in crate::db) read_only: bool,
    active: bool,
}

impl Registration {
    fn retire(&mut self) {
        if self.active {
            self.registry.dependencies.lock().abort(self.id);
            self.active = false;
            self.registry.changed.notify_all();
        }
    }
}

impl Drop for Registration {
    fn drop(&mut self) {
        self.retire();
    }
}

/// Own the graph lock until the durable decision succeeds or the tentative
/// dependency edges have been undone. The caller cannot reacquire the graph
/// while the write is being finalized.
pub(in crate::db) struct CommitRegistry<'a> {
    registry: &'a SerializableTransactions,
    guard: MutexGuard<'a, Dependencies<CollectionIdentity>>,
}

pub(in crate::db) struct SerializableCommit<'a> {
    pending: PendingCommit<'a, CollectionIdentity>,
    changed: &'a Condvar,
}

impl SerializableCommit<'_> {
    pub(in crate::db) fn committed(self, tx: &mut Transaction) {
        self.pending.committed();
        tx.postgres_serializable.as_mut().unwrap().active = false;
        self.changed.notify_all();
    }
}

impl CommitRegistry<'_> {
    pub(in crate::db) fn prepare(
        &mut self,
        db: &BicDb,
        tx: &Transaction,
        mutations: &BTreeMap<String, Vec<IndexRecordMutation>>,
    ) -> Result<SerializableCommit<'_>> {
        let writes = db.postgres_write_footprints(mutations)?;
        let pending = self
            .guard
            .prepare_write_commit(tx.id.0, writes)
            .ok_or_else(|| {
                BicDbError::TransactionConflict(
                    "could not serialize access due to read/write dependencies among transactions"
                        .into(),
                )
            })?;
        Ok(SerializableCommit {
            pending,
            changed: &self.registry.changed,
        })
    }
}

impl BicDb {
    pub(in crate::db) fn postgres_commit_registry(
        &self,
        tx: &Transaction,
    ) -> Option<CommitRegistry<'_>> {
        let participant = tx.postgres_serializable.as_ref().filter(|r| r.active)?;
        debug_assert!(Arc::ptr_eq(
            &self.postgres_serializable,
            &participant.registry
        ));
        Some(CommitRegistry {
            registry: &self.postgres_serializable,
            guard: self.postgres_serializable.dependencies.lock(),
        })
    }

    fn postgres_write_footprints(
        &self,
        mutations: &BTreeMap<String, Vec<IndexRecordMutation>>,
    ) -> Result<Vec<WriteFootprint<CollectionIdentity>>> {
        let mut writes = Vec::new();
        for (name, changes) in mutations {
            let collection =
                CollectionIdentity(Arc::clone(&self.collection_state(name)?.incarnation));
            let mut indexes = Vec::new();
            for index in self.indexes.values() {
                let state = index.read();
                if state.definition.collection == *name && state.definition.kind == IndexKind::BTree
                {
                    indexes.push((index, index_identity(&state.definition)?));
                }
            }
            for change in changes {
                let mut index_keys = BTreeMap::new();
                for (index, identity) in &indexes {
                    let state = index.read();
                    let definition = &state.definition;
                    let mut keys = BTreeSet::new();
                    for image in [&change.old_record, &change.new_record] {
                        if image.matches_predicate(definition)? == Some(true) {
                            if let Some(key) = image.encoded_index_key(&definition.fields)? {
                                keys.insert(key.to_vec());
                            }
                        }
                    }
                    index_keys.insert(identity.clone(), keys);
                }
                for record in change
                    .old_record
                    .id()
                    .into_iter()
                    .chain(change.new_record.id())
                {
                    writes.push(WriteFootprint {
                        collection: collection.clone(),
                        record_key: record.as_bytes().to_vec(),
                        index_keys: index_keys.clone(),
                    });
                    if change.old_record.id() == change.new_record.id() {
                        break;
                    }
                }
            }
        }
        Ok(writes)
    }
}

impl Transaction {
    pub fn is_postgres_serializable(&self) -> bool {
        self.postgres_serializable.is_some()
    }

    /// Pin and register a PostgreSQL Serializable snapshot at the first
    /// snapshot-requiring statement. A deferrable reader waits outside commit
    /// admission and retries before it can expose an unsafe snapshot.
    pub fn acquire_postgres_serializable_snapshot(
        &mut self,
        floor: u64,
        read_only: bool,
        deferrable: bool,
        cancellation: &CancellationToken,
    ) -> Result<()> {
        if self.statement_snapshot_acquired {
            return Ok(());
        }
        // As in commit_with_sequence, the transaction owns the database
        // pointer for its lifetime and the caller provides access admission.
        let db = unsafe { self.db.as_ref() };
        loop {
            cancellation.check()?;
            let registry = Arc::clone(&db.postgres_serializable);
            let candidate = {
                let _admission = db.serializable_commit_admission.write();
                db.check_recovery_state()?;
                if db.is_read_only_standby()
                    || db.config.require_commit_admission
                    || db.commit_admission.read().is_some()
                {
                    return Err(BicDbError::Unsupported(
                        "PostgreSQL Serializable requires a writable local commit authority".into(),
                    ));
                }
                self.set_snapshot_isolation(true, floor)?;
                // A preexisting session floor can equal the new watermark.
                // Refresh the physical page snapshot even when the registered
                // numeric visibility boundary does not need to advance.
                self.applied_snapshot = db
                    .last_committed_tx
                    .load(std::sync::atomic::Ordering::SeqCst);
                self.paged_snapshot = db
                    .paged_records
                    .as_ref()
                    .map(|paged| paged.latest_snapshot());
                let mut graph = registry.dependencies.lock();
                if deferrable && read_only {
                    Some(graph.begin_safe_snapshot(self.id.0))
                } else {
                    graph.begin(self.id.0, read_only);
                    None
                }
            };
            let mut registration = Registration {
                registry,
                id: self.id.0,
                read_only,
                active: true,
            };
            if let Some(candidate) = candidate {
                let safety = loop {
                    let registry = &registration.registry;
                    let graph = registry.dependencies.lock();
                    let safety = graph.snapshot_safety(&candidate);
                    if safety != SnapshotSafety::Waiting {
                        break safety;
                    }
                    // Do not try to reacquire host capacity while holding
                    // either the database commit admission or graph lock.
                    drop(graph);
                    cancellation.blocking_wait(|| {
                        let mut graph = registry.dependencies.lock();
                        if graph.snapshot_safety(&candidate) == SnapshotSafety::Waiting {
                            registry
                                .changed
                                .wait_for(&mut graph, Duration::from_millis(25));
                        }
                    })?;
                };
                if safety == SnapshotSafety::Retry {
                    continue;
                }
                db.check_recovery_state()?;
                registration.retire();
            }
            self.postgres_serializable = Some(registration);
            self.statement_snapshot_acquired = true;
            return Ok(());
        }
    }

    fn postgres_collection_identity(&self, collection: &str) -> CollectionIdentity {
        CollectionIdentity(Arc::clone(&self.collection_incarnations.lock()[collection]))
    }

    fn observe_postgres_read(&self, collection: &str, keys: impl FnOnce() -> ReadKeys) {
        if let Some(registration) = self.postgres_serializable.as_ref().filter(|r| r.active) {
            let scope = ReadScope {
                collection: self.postgres_collection_identity(collection),
                keys: keys(),
            };
            registration
                .registry
                .dependencies
                .lock()
                .observe_read(registration.id, scope);
        }
    }

    pub(in crate::db) fn observe_postgres_relation(&self, collection: &str) {
        self.observe_postgres_read(collection, || ReadKeys::Relation);
    }

    pub(in crate::db) fn observe_postgres_record(&self, collection: &str, key: &str) {
        self.observe_postgres_read(collection, || {
            ReadKeys::Record(KeyRange::Exact(key.as_bytes().to_vec()))
        });
    }

    pub(in crate::db) fn observe_postgres_prefix(&self, collection: &str, prefix: &str) {
        self.observe_postgres_read(collection, || {
            ReadKeys::Record(KeyRange::Prefix(prefix.as_bytes().to_vec()))
        });
    }

    pub(in crate::db) fn observe_postgres_range(
        &self,
        collection: &str,
        lower: Bound<&str>,
        upper: Bound<&str>,
    ) {
        self.observe_postgres_read(collection, || {
            let encode = |key: &str| key.as_bytes().to_vec();
            ReadKeys::Record(KeyRange::bounded(lower.map(encode), upper.map(encode)))
        });
    }

    /// Record an index prefix probe, even when it returns no row locator.
    pub fn observe_postgres_index_prefix(&self, name: &str, prefix: &[IndexValue]) -> Result<()> {
        self.observe_postgres_index(name, || {
            KeyRange::Prefix(crate::db::encode_index_key(prefix))
        })
    }

    /// Reconcile live index candidates against fixed-snapshot keys. The
    /// caller probes the live index first, so a key can only have moved
    /// after its replacement version entered the MVCC chain.
    pub fn postgres_snapshot_index_changes(
        &self,
        name: &str,
        prefix: &[IndexValue],
        bounds: Option<(Option<&IndexValue>, Option<&IndexValue>)>,
        filters: &[(usize, IndexValue)],
        cancellation: &CancellationToken,
    ) -> Result<Vec<(String, RowId, bool)>> {
        if self.postgres_serializable.is_none() {
            return Ok(Vec::new());
        }
        let db = self.db_ref();
        if db.commit_seq.load(std::sync::atomic::Ordering::Acquire) <= self.snapshot_tx.0 {
            return Ok(Vec::new());
        }
        let index = db
            .indexes
            .get(name)
            .ok_or_else(|| BicDbError::Index(format!("index {name} not found")))?
            .read()
            .definition
            .clone();
        let collection = &index.collection;
        self.bind_collection_incarnation(collection)?;
        let changed = {
            let state = db.collection_state(collection)?;
            let mut changed = Vec::new();
            for shard in state.read_all() {
                for (position, (rowid, versions)) in shard.versions.iter().enumerate() {
                    if position % 1024 == 0 {
                        cancellation.check()?;
                    }
                    if versions.iter().any(|version| {
                        version.created_tx.0 > self.snapshot_tx.0
                            || version
                                .deleted_tx
                                .is_some_and(|end| end.0 > self.snapshot_tx.0)
                    }) {
                        if let Some(version) =
                            crate::db::visible_version(versions, self.snapshot_tx)
                        {
                            changed.push((version.record.id.clone(), *rowid));
                        }
                    }
                }
            }
            changed
        };
        let mut matches = Vec::with_capacity(changed.len());
        for (id, rowid) in changed {
            cancellation.check()?;
            let (record, _, _) =
                crate::db::record_read_snapshot::read_record_at_snapshot_unchecked(
                    db,
                    collection,
                    &id,
                    self.snapshot_tx,
                    &self.paged_snapshot,
                )?;
            let visible = match record {
                Some(record) if crate::db::record_matches_index_predicate(&record, &index)? => {
                    let key = index.key_for_record(&record);
                    key.starts_with(prefix)
                        && bounds.is_none_or(|(lower, upper)| {
                            key.get(prefix.len()).is_some_and(|value| {
                                lower.is_none_or(|lower| value >= lower)
                                    && upper.is_none_or(|upper| value <= upper)
                            })
                        })
                        && filters
                            .iter()
                            .all(|(field, value)| key.get(*field) == Some(value))
                }
                _ => false,
            };
            matches.push((id, rowid, visible));
        }
        Ok(matches)
    }

    /// The index scans use inclusive first-field bounds inside the prefix.
    pub fn observe_postgres_index_range(
        &self,
        name: &str,
        prefix: &[IndexValue],
        lower: Option<&IndexValue>,
        upper: Option<&IndexValue>,
    ) -> Result<()> {
        self.observe_postgres_index(name, || {
            let prefix = crate::db::encode_index_key(prefix);
            let bound_key = |value: Option<&IndexValue>| {
                let mut key = prefix.clone();
                if let Some(value) = value {
                    crate::db::encode_index_value(&mut key, value);
                }
                key
            };
            KeyRange::bounded(
                Bound::Included(bound_key(lower)),
                next_key_prefix(bound_key(upper)),
            )
        })
    }

    fn observe_postgres_index(&self, name: &str, range: impl FnOnce() -> KeyRange) -> Result<()> {
        if self
            .postgres_serializable
            .as_ref()
            .is_none_or(|r| !r.active)
        {
            return Ok(());
        }
        let index = self
            .db_ref()
            .indexes
            .get(name)
            .ok_or_else(|| BicDbError::Index(format!("index {name} not found")))?;
        let (collection, identity) = {
            let state = index.read();
            (
                state.definition.collection.clone(),
                index_identity(&state.definition)?,
            )
        };
        self.bind_collection_incarnation(&collection)?;
        self.observe_postgres_read(&collection, || ReadKeys::Index {
            identity,
            range: range(),
        });
        Ok(())
    }
}
