//! Native transaction admission for a declared collection catalog operation.
//! The SQL host must separately reserve its complete dependency closure and
//! fence catalog/Describe readers; these runtime reservations are not recovery.
use super::*;
use std::cell::RefCell;
use std::sync::Weak;

pub(super) type Bindings = Mutex<FxHashMap<String, CatalogBinding>>;

const RECORD_OBSERVATIONS_PER_COLLECTION: usize = 128;

#[derive(Debug)]
pub(super) struct CatalogBinding {
    pub(super) incarnation: Arc<()>,
    /// Ordinary native row access observes MVCC state. SQL's explicit
    /// catalog check pins only metadata identity and does not acquire a data
    /// snapshot, even when it scans the whole metadata collection.
    pub(super) snapshot_observed: bool,
    // None is a whole-collection observation. Point catalog observations
    // escalate to that conservative representation at a fixed memory bound.
    records: Option<BTreeSet<String>>,
}

#[derive(Debug)]
struct RecordReservation {
    incarnation: Option<Arc<()>>,
    ids: BTreeSet<String>,
}

impl RecordReservation {
    fn matches(&self, name: &str, incarnation: Option<&Arc<()>>, collection: &str) -> bool {
        name == collection
            || self.incarnation.as_ref().is_some_and(|expected| {
                incarnation.is_some_and(|actual| Arc::ptr_eq(expected, actual))
            })
    }

    fn same_backing(&self, incarnation: Option<&Arc<()>>) -> bool {
        match (self.incarnation.as_ref(), incarnation) {
            (Some(expected), Some(actual)) => Arc::ptr_eq(expected, actual),
            (None, None) => true,
            _ => false,
        }
    }
}

#[derive(Clone, Debug)]
pub struct CatalogScopeToken {
    admission: Weak<CatalogAdmission>,
    owner: TransactionId,
    serial: u64,
}

struct AccessFrame {
    token: CatalogScopeToken,
    identity: Arc<()>,
}

#[derive(Clone)]
struct ReadFrame {
    admission: Weak<CatalogAdmission>,
    owner: TransactionId,
    bindings: Weak<Bindings>,
    actor: Option<u64>,
    identity: Arc<()>,
}

thread_local! {
    static HOST_ACCESS: RefCell<Vec<AccessFrame>> = const { RefCell::new(Vec::new()) };
    static READ_ACCESS: RefCell<Vec<ReadFrame>> = const { RefCell::new(Vec::new()) };
}

/// Read-only catalog identity for a live transaction. This does not turn
/// transactions begun on this thread into trusted internal catalog writers.
/// An expired owner refuses reads; dropping this guard uses owned handles only.
#[derive(Debug)]
pub struct CatalogReadAccess {
    identity: Arc<()>,
    _not_send: PhantomData<std::rc::Rc<()>>,
}

impl Drop for CatalogReadAccess {
    fn drop(&mut self) {
        let _ = READ_ACCESS.try_with(|frames| {
            frames
                .borrow_mut()
                .retain(|frame| !Arc::ptr_eq(&frame.identity, &self.identity));
        });
    }
}

/// Temporary authority for trusted internal catalog operations on this thread.
/// Internal row mutations stay within the token's declared collections and
/// backing identities. Reads retain ordinary native access checks. This does
/// not grant row-policy or mutation-grant authority or authorize raw catalog
/// changes. Drop before the outer business decision; transactions created
/// inside must finish first.
#[derive(Debug)]
pub struct CatalogScopeAccess {
    identity: Arc<()>,
    _not_send: PhantomData<std::rc::Rc<()>>,
}

impl Drop for CatalogScopeAccess {
    fn drop(&mut self) {
        let _ = HOST_ACCESS.try_with(|frames| {
            frames
                .borrow_mut()
                .retain(|frame| !Arc::ptr_eq(&frame.identity, &self.identity));
        });
    }
}

#[derive(Debug)]
struct Scope {
    owner: TransactionId,
    names: BTreeSet<String>,
    incarnations: Vec<Arc<()>>,
    records: BTreeMap<String, RecordReservation>,
    context: Option<String>,
    owner_live: bool,
    owner_outcome: Option<TxState>,
    closing: bool,
}

#[derive(Debug)]
struct Registration {
    bindings: Weak<Bindings>,
    actor_scope: Option<u64>,
}

impl Scope {
    fn overlaps(&self, other: &Scope) -> bool {
        !self.names.is_disjoint(&other.names)
            || self.incarnations.iter().any(|left| {
                other
                    .incarnations
                    .iter()
                    .any(|right| Arc::ptr_eq(left, right))
            })
            || self.records.iter().any(|(name, rows)| {
                other.covers(name, rows.incarnation.as_ref())
                    || other.records.iter().any(|(other_name, other_rows)| {
                        rows.matches(name, other_rows.incarnation.as_ref(), other_name)
                            && !rows.ids.is_disjoint(&other_rows.ids)
                    })
            })
            || other
                .records
                .iter()
                .any(|(name, rows)| self.covers(name, rows.incarnation.as_ref()))
    }

    fn covers(&self, name: &str, incarnation: Option<&Arc<()>>) -> bool {
        self.names.contains(name)
            || incarnation.is_some_and(|current| {
                self.incarnations
                    .iter()
                    .any(|expected| Arc::ptr_eq(current, expected))
            })
    }

    fn covers_record(&self, name: &str, incarnation: Option<&Arc<()>>, id: Option<&str>) -> bool {
        self.covers(name, incarnation)
            || self.records.iter().any(|(collection, rows)| {
                rows.matches(collection, incarnation, name)
                    && id.is_none_or(|id| rows.ids.contains(id))
            })
    }

    fn intersects_observation(&self, name: &str, binding: &CatalogBinding) -> bool {
        self.covers(name, Some(&binding.incarnation))
            || self.records.iter().any(|(collection, rows)| {
                rows.matches(collection, Some(&binding.incarnation), name)
                    && binding
                        .records
                        .as_ref()
                        .is_none_or(|observed| !rows.ids.is_disjoint(observed))
            })
    }

    fn allows_record(&self, name: &str, incarnation: Option<&Arc<()>>, id: Option<&str>) -> bool {
        self.covers(name, incarnation)
            || self.records.iter().any(|(collection, rows)| {
                rows.matches(collection, incarnation, name)
                    && rows.same_backing(incarnation)
                    && id.is_none_or(|id| rows.ids.contains(id))
            })
    }
}

#[derive(Debug, Default)]
struct State {
    scopes: BTreeMap<u64, Scope>,
    next_serial: u64,
}

impl State {
    fn check_access(
        &self,
        owner: TransactionId,
        actor: Option<u64>,
        collection: &str,
        incarnation: Option<&Arc<()>>,
    ) -> Result<()> {
        self.check_record_access(owner, actor, collection, incarnation, None)
    }

    fn check_record_access(
        &self,
        owner: TransactionId,
        actor: Option<u64>,
        collection: &str,
        incarnation: Option<&Arc<()>>,
        id: Option<&str>,
    ) -> Result<()> {
        self.check_actor(actor)?;
        for scope in self.scopes.values() {
            if scope.owner == owner || !scope.covers_record(collection, incarnation, id) {
                continue;
            }
            let delegated = actor
                .and_then(|serial| self.scopes.get(&serial))
                .is_some_and(|access| {
                    access.owner == scope.owner && access.covers_record(collection, incarnation, id)
                });
            if !delegated {
                return Err(BicDbError::TransactionConflict(format!(
                    "collection `{collection}` is reserved by another catalog operation"
                )));
            }
        }
        Ok(())
    }

    fn check_actor(&self, actor: Option<u64>) -> Result<()> {
        if let Some(serial) = actor {
            if !self.scopes.get(&serial).is_some_and(|scope| {
                !scope.closing && (scope.owner_live || scope.context.is_some())
            }) {
                return Err(BicDbError::TransactionConflict(
                    "internal catalog scope is closed or no longer active".into(),
                ));
            }
        }
        Ok(())
    }
}

#[derive(Debug, Default)]
pub(super) struct CatalogAdmission {
    state: RwLock<State>,
    registrations: Mutex<FxHashMap<u64, Registration>>,
    active: AtomicUsize,
}

impl CatalogAdmission {
    fn has_overlapping_children(&self, state: &State, serial: u64) -> bool {
        let Some(target) = state.scopes.get(&serial) else {
            return false;
        };
        self.registrations.lock().values().any(|entry| {
            entry
                .actor_scope
                .and_then(|actor| state.scopes.get(&actor))
                .is_some_and(|scope| scope.overlaps(target))
        })
    }
    pub(super) fn check_name_access(
        &self,
        owner: TransactionId,
        actor: Option<u64>,
        name: &str,
    ) -> Result<()> {
        if actor.is_some() || self.active.load(AtomicOrdering::Acquire) != 0 {
            self.state.read().check_access(owner, actor, name, None)?;
        }
        Ok(())
    }

    pub(super) fn ambient_actor(self: &Arc<Self>) -> Result<Option<u64>> {
        let identity = Arc::downgrade(self);
        let token = HOST_ACCESS.with(|frames| {
            frames
                .borrow()
                .iter()
                .rev()
                .find(|frame| Weak::ptr_eq(&frame.token.admission, &identity))
                .map(|frame| frame.token.clone())
        });
        let Some(token) = token else { return Ok(None) };
        let state = self.state.read();
        self.validate_token(&state, &token)?;
        state.check_actor(Some(token.serial))?;
        Ok(Some(token.serial))
    }

    pub(super) fn register(
        &self,
        owner: TransactionId,
        bindings: &Arc<Bindings>,
        actor: Option<u64>,
    ) -> Result<()> {
        let _state = actor.map(|_| self.state.read());
        if let Some(state) = _state.as_ref() {
            state.check_actor(actor)?;
        }
        self.registrations.lock().insert(
            owner.0,
            Registration {
                bindings: Arc::downgrade(bindings),
                actor_scope: actor,
            },
        );
        Ok(())
    }

    pub(super) fn finish(&self, owner: TransactionId, outcome: TxState) {
        // Uses only owned shared handles: never dereference a database during
        // Transaction::drop, including while another thread owns its write lock.
        self.registrations.lock().remove(&owner.0);
        if self.active.load(AtomicOrdering::Acquire) == 0 {
            return;
        }
        let mut state = self.state.write();
        for scope in state
            .scopes
            .values_mut()
            .filter(|scope| scope.owner == owner)
        {
            scope.owner_live = false;
            scope.owner_outcome = Some(outcome);
            if scope.context.is_none() {
                scope.closing = true;
            }
        }
        let registrations = self.registrations.lock();
        state.scopes.retain(|serial, scope| {
            scope.owner_live
                || scope.context.is_some()
                || registrations
                    .values()
                    .any(|entry| entry.actor_scope == Some(*serial))
        });
        self.active
            .store(state.scopes.len(), AtomicOrdering::Release);
    }

    pub(super) fn bind(
        &self,
        owner: TransactionId,
        actor: Option<u64>,
        bindings: &Bindings,
        collection: &str,
        record: Option<&str>,
        current: impl FnOnce() -> Result<Arc<()>>,
    ) -> Result<()> {
        // Already-published bindings make a conflicting reservation refuse.
        // Only a first observation needs the shared admission gate when there
        // are no active scopes, keeping repeated ordinary reads inexpensive.
        if actor.is_none()
            && self.active.load(AtomicOrdering::Acquire) == 0
            && bindings.lock().contains_key(collection)
        {
            return Self::remember(bindings, collection, current()?);
        }
        let state = self.state.read();
        // Check absent names too: a reserved rename destination/source must
        // not return NotFound while its owner has an unfinished catalog move.
        state.check_record_access(owner, actor, collection, None, record)?;
        let current = current()?;
        state.check_record_access(owner, actor, collection, Some(&current), record)?;
        Self::remember_binding(bindings, collection, current, record, true)
    }

    fn remember(bindings: &Bindings, collection: &str, incarnation: Arc<()>) -> Result<()> {
        Self::remember_binding(bindings, collection, incarnation, None, true)
    }

    fn remember_record(
        bindings: &Bindings,
        collection: &str,
        incarnation: Arc<()>,
        record: Option<&str>,
    ) -> Result<()> {
        Self::remember_binding(bindings, collection, incarnation, record, false)
    }

    fn remember_binding(
        bindings: &Bindings,
        collection: &str,
        incarnation: Arc<()>,
        record: Option<&str>,
        snapshot_observed: bool,
    ) -> Result<()> {
        let mut bindings = bindings.lock();
        match bindings.get_mut(collection) {
            Some(expected) if !Arc::ptr_eq(&expected.incarnation, &incarnation) => {
                Err(BicDbError::TransactionConflict(format!(
                    "collection `{collection}` was replaced after this transaction observed it"
                )))
            }
            Some(binding) => {
                binding.snapshot_observed |= snapshot_observed;
                match (binding.records.as_mut(), record) {
                    (Some(records), Some(id)) if records.contains(id) => {}
                    (Some(records), Some(id))
                        if records.len() < RECORD_OBSERVATIONS_PER_COLLECTION =>
                    {
                        records.insert(id.to_string());
                    }
                    (_, None) | (Some(_), Some(_)) => binding.records = None,
                    (None, Some(_)) => {}
                }
                Ok(())
            }
            None => {
                bindings.insert(
                    collection.to_string(),
                    CatalogBinding {
                        incarnation,
                        snapshot_observed,
                        records: record.map(|id| BTreeSet::from([id.to_string()])),
                    },
                );
                Ok(())
            }
        }
    }

    pub(super) fn owner_covers(
        &self,
        owner: TransactionId,
        actor: Option<u64>,
        old: &str,
        new: &str,
    ) -> bool {
        let state = self.state.read();
        if state.check_actor(actor).is_err() {
            return false;
        }
        state.scopes.iter().any(|(serial, scope)| {
            (scope.owner == owner || Some(*serial) == actor)
                && scope.names.contains(old)
                && scope.names.contains(new)
        })
    }

    fn validate_token(&self, state: &State, token: &CatalogScopeToken) -> Result<()> {
        let same = token
            .admission
            .upgrade()
            .is_some_and(|admission| std::ptr::eq(admission.as_ref(), self));
        if !same
            || !state
                .scopes
                .get(&token.serial)
                .is_some_and(|scope| scope.owner == token.owner)
        {
            return Err(BicDbError::TransactionConflict(
                "foreign or inactive catalog scope token".into(),
            ));
        }
        Ok(())
    }

    pub(super) fn prepare_commit(&self, owner: TransactionId, actor: Option<u64>) -> Result<()> {
        if actor.is_none() && self.active.load(AtomicOrdering::Acquire) == 0 {
            return Ok(());
        }
        let mut state = self.state.write();
        state.check_actor(actor)?;
        let owned = state
            .scopes
            .iter()
            .filter(|(_, scope)| scope.owner == owner)
            .map(|(serial, _)| *serial)
            .collect::<BTreeSet<_>>();
        if owned.is_empty() {
            return Ok(());
        }
        if self.registrations.lock().values().any(|entry| {
            entry
                .actor_scope
                .is_some_and(|serial| owned.contains(&serial))
        }) {
            return Err(BicDbError::TransactionConflict(
                "internal catalog work is still pending before the outer decision".into(),
            ));
        }
        for serial in owned {
            state.scopes.get_mut(&serial).unwrap().closing = true;
        }
        Ok(())
    }

    pub(super) fn reopen_after_failed_commit(&self, owner: TransactionId) {
        if self.active.load(AtomicOrdering::Acquire) == 0 {
            return;
        }
        for scope in self
            .state
            .write()
            .scopes
            .values_mut()
            .filter(|scope| scope.owner == owner)
        {
            scope.closing = false;
        }
    }
}

impl Transaction {
    /// Carry this owner's identity through SQL catalog readers without granting
    /// host mutation authority. The owner must still be pending at every read.
    pub fn enter_catalog_read_access(&self) -> Result<CatalogReadAccess> {
        self.db_ref().check_recovery_state()?;
        if self.state != TxState::Pending {
            return Err(BicDbError::TransactionNotPending);
        }
        let identity = Arc::new(());
        READ_ACCESS.with(|frames| {
            frames.borrow_mut().push(ReadFrame {
                admission: Arc::downgrade(&self.catalog_admission),
                owner: self.id,
                bindings: Arc::downgrade(&self.collection_incarnations),
                actor: self.catalog_actor_scope,
                identity: Arc::clone(&identity),
            })
        });
        Ok(CatalogReadAccess {
            identity,
            _not_send: PhantomData,
        })
    }

    pub(super) fn ensure_catalog_mutation_scope(&self, collection: &str) -> Result<()> {
        let Some(actor) = self.catalog_actor_scope else {
            return Ok(());
        };
        let db = self.db_ref();
        db.ensure_writable("buffer internal catalog mutation")?;
        // Resolve under the host's database execution guard before taking
        // admission, retaining only the immutable identity across the gate.
        let incarnation = db
            .collections
            .get(collection)
            .map(|current| Arc::clone(&current.read().incarnation));
        let state = self.catalog_admission.state.read();
        state.check_actor(Some(actor))?;
        if !state.scopes[&actor].allows_record(collection, incarnation.as_ref(), None) {
            return Err(BicDbError::TransactionConflict(format!(
                "internal catalog mutation of `{collection}` exceeds its reserved resources"
            )));
        }
        Ok(())
    }

    pub(super) fn ensure_catalog_record_mutation_scope(
        &self,
        collection: &str,
        id: &str,
    ) -> Result<()> {
        let Some(actor) = self.catalog_actor_scope else {
            return Ok(());
        };
        let db = self.db_ref();
        db.ensure_writable("buffer internal catalog record mutation")?;
        let incarnation = db
            .collections
            .get(collection)
            .map(|current| Arc::clone(&current.read().incarnation));
        let state = self.catalog_admission.state.read();
        state.check_actor(Some(actor))?;
        if !state.scopes[&actor].allows_record(collection, incarnation.as_ref(), Some(id)) {
            return Err(BicDbError::TransactionConflict(format!("internal catalog mutation of `{collection}` record `{id}` exceeds its reserved resources")));
        }
        state.check_record_access(
            self.id,
            Some(actor),
            collection,
            incarnation.as_ref(),
            Some(id),
        )
    }

    pub(super) fn prepare_catalog_record_mutations<'a>(
        &self,
        collection: &str,
        ids: impl Iterator<Item = &'a str> + Clone,
    ) -> Result<()> {
        if self.catalog_actor_scope.is_some() {
            for id in ids.clone() {
                self.ensure_catalog_record_mutation_scope(collection, id)?;
            }
            for id in ids {
                self.bind_collection_incarnation_for_record(collection, id)?;
            }
        }
        Ok(())
    }

    pub(super) fn ensure_unprotected_collection_access(
        &self,
        collection: &str,
        operation: SecureOperation,
    ) -> Result<()> {
        let result = self
            .db_ref()
            .ensure_unprotected_legacy_access(collection, operation);
        if matches!(result, Err(BicDbError::CollectionNotFound(_))) {
            // Existing relations keep the original policy/grant error order.
            // A moved-away reserved name must not leak an intermediate
            // NotFound result before the transaction reaches its bind gate.
            self.catalog_admission.check_name_access(
                self.id,
                self.catalog_actor_scope,
                collection,
            )?;
        }
        result
    }

    /// Reserve exact collection names and their current runtime identities.
    /// Existing observations on any protected identity refuse admission;
    /// later transactions refuse access until release, commit, rollback or
    /// drop. Absent destination names are protected too. No durable state is
    /// changed, and this grants no mutation authority or SQL catalog visibility.
    /// As with other Transaction operations, the host must hold its ordinary
    /// database execution guard; this is not a replacement for that guard.
    pub fn reserve_catalog_collections(&self, names: &[String]) -> Result<CatalogScopeToken> {
        self.reserve_catalog_resources(names, &[])
    }

    /// Reserve relation names/backings and exact catalog record identities in
    /// one admission step. A scan observes all records; bounded point-read sets
    /// may conservatively escalate to that representation. This is runtime
    /// admission only: durable host undo and SQL visibility remain separate.
    pub fn reserve_catalog_resources(
        &self,
        names: &[String],
        records: &[(String, String)],
    ) -> Result<CatalogScopeToken> {
        let db = self.db_ref();
        db.check_recovery_state()?;
        if self.state != TxState::Pending {
            return Err(BicDbError::TransactionNotPending);
        }
        if self.catalog_actor_scope.is_some() {
            return Err(BicDbError::Unsupported(
                "internal catalog transactions cannot reserve independent scopes".into(),
            ));
        }
        let names = names.iter().cloned().collect::<BTreeSet<_>>();
        if names.is_empty() && records.is_empty() {
            return Err(BicDbError::Unsupported("empty catalog scope".into()));
        }
        for name in &names {
            validate_collection_name(name)?;
        }
        let mut rows: BTreeMap<String, RecordReservation> = BTreeMap::new();
        for (collection, id) in records {
            validate_collection_name(collection)?;
            if id.is_empty() {
                return Err(BicDbError::Unsupported(
                    "empty catalog record identity".into(),
                ));
            }
            rows.entry(collection.clone())
                .or_insert_with(|| RecordReservation {
                    incarnation: db
                        .collections
                        .get(collection)
                        .map(|current| Arc::clone(&current.read().incarnation)),
                    ids: BTreeSet::new(),
                })
                .ids
                .insert(id.clone());
        }
        // Resolve the immutable identities under the caller's database guard
        // before entering admission. Never wait for a collection lock while
        // holding the global scope gate or the registration registry.
        let incarnations = names
            .iter()
            .filter_map(|name| db.collections.get(name))
            .map(|collection| Arc::clone(&collection.read().incarnation))
            .collect();
        let scope = Scope {
            owner: self.id,
            names,
            incarnations,
            records: rows,
            context: None,
            owner_live: true,
            owner_outcome: None,
            closing: false,
        };
        let mut state = self.catalog_admission.state.write();
        for name in &scope.names {
            state.check_access(self.id, None, name, None)?;
        }
        for (collection, rows) in &scope.records {
            for id in &rows.ids {
                state.check_record_access(
                    self.id,
                    None,
                    collection,
                    rows.incarnation.as_ref(),
                    Some(id),
                )?;
            }
        }
        for existing in state.scopes.values() {
            if existing.owner != self.id && scope.overlaps(existing) {
                return Err(BicDbError::TransactionConflict(
                    "catalog scope overlaps another owner's backing".into(),
                ));
            }
        }
        for (id, registration) in self.catalog_admission.registrations.lock().iter() {
            if *id == self.id.0 {
                continue;
            }
            if let Some(bindings) = registration.bindings.upgrade() {
                if bindings
                    .lock()
                    .iter()
                    .any(|(name, binding)| scope.intersects_observation(name, binding))
                {
                    return Err(BicDbError::TransactionConflict(
                        "catalog scope has an existing transaction observation".into(),
                    ));
                }
            }
        }
        let serial = state.next_serial;
        state.next_serial = serial
            .checked_add(1)
            .ok_or_else(|| BicDbError::Unsupported("catalog scope identity exhausted".into()))?;
        db.check_recovery_state()?;
        state.scopes.insert(serial, scope);
        self.catalog_admission
            .active
            .store(state.scopes.len(), AtomicOrdering::Release);
        Ok(CatalogScopeToken {
            admission: Arc::downgrade(&self.catalog_admission),
            owner: self.id,
            serial,
        })
    }

    /// Release only this owner's exact runtime scope. Tokens from a previous
    /// open or a different database/transaction cannot release another lease.
    pub fn release_catalog_scope(&self, token: &CatalogScopeToken) -> Result<()> {
        if !Weak::ptr_eq(&token.admission, &Arc::downgrade(&self.catalog_admission))
            || token.owner != self.id
        {
            return Err(BicDbError::Unsupported(
                "foreign catalog scope token".into(),
            ));
        }
        let mut state = self.catalog_admission.state.write();
        if !state
            .scopes
            .get(&token.serial)
            .is_some_and(|scope| scope.owner == self.id)
        {
            return Err(BicDbError::TransactionConflict(
                "catalog scope is no longer active".into(),
            ));
        }
        if state.scopes[&token.serial].context.is_some() {
            return Err(BicDbError::Unsupported(
                "host-retained catalog scope requires explicit host finalization".into(),
            ));
        }
        if self
            .catalog_admission
            .has_overlapping_children(&state, token.serial)
        {
            return Err(BicDbError::TransactionConflict(
                "internal catalog work is still pending".into(),
            ));
        }
        state.scopes.remove(&token.serial);
        self.catalog_admission
            .active
            .store(state.scopes.len(), AtomicOrdering::Release);
        Ok(())
    }

    /// Retain a reservation independently of this native transaction. The
    /// trusted SQL host must first have made its undo journal durable, and must
    /// explicitly finalize this scope after a durable decision or complete
    /// undo. Dropping the transaction or token deliberately leaves the fence.
    pub fn retain_catalog_scope(&self, token: &CatalogScopeToken, context: &str) -> Result<()> {
        self.db_ref().check_recovery_state()?;
        if self.state != TxState::Pending || self.catalog_actor_scope.is_some() {
            return Err(BicDbError::TransactionNotPending);
        }
        if context.is_empty() {
            return Err(BicDbError::Unsupported(
                "empty catalog recovery context".into(),
            ));
        }
        let mut state = self.catalog_admission.state.write();
        self.catalog_admission.validate_token(&state, token)?;
        if token.owner != self.id {
            return Err(BicDbError::Unsupported(
                "foreign catalog scope owner".into(),
            ));
        }
        if state.scopes.iter().any(|(serial, scope)| {
            *serial != token.serial && scope.context.as_deref() == Some(context)
        }) {
            return Err(BicDbError::TransactionConflict(
                "catalog recovery context already owns a different scope".into(),
            ));
        }
        let scope = state.scopes.get_mut(&token.serial).unwrap();
        if scope
            .context
            .as_deref()
            .is_some_and(|existing| existing != context)
        {
            return Err(BicDbError::TransactionConflict(
                "catalog scope recovery context is immutable".into(),
            ));
        }
        scope.context = Some(context.to_string());
        Ok(())
    }

    /// Validate the live business owner before a trusted host re-enters its
    /// durable undo context. Possession of another owner's journal identifier
    /// does not authorize that transaction to repair the journal.
    pub fn check_catalog_scope_owner(&self, token: &CatalogScopeToken) -> Result<()> {
        self.db_ref().check_recovery_state()?;
        let state = self.catalog_admission.state.read();
        self.catalog_admission.validate_token(&state, token)?;
        if self.state != TxState::Pending
            || token.owner != self.id
            || self.catalog_actor_scope.is_some()
        {
            return Err(BicDbError::TransactionConflict(
                "catalog scope has a different live business owner".into(),
            ));
        }
        state.check_actor(Some(token.serial))
    }
}

impl BicDb {
    /// Check an undo image against the already reserved runtime scope before
    /// applying any image. This validates coverage; it grants no authority.
    pub fn check_catalog_scope_resources(
        &self,
        token: &CatalogScopeToken,
        names: &[String],
        records: &[(String, String)],
    ) -> Result<()> {
        self.check_recovery_state()?;
        // Keep the same lock order as reservation: resolve collection state
        // before entering admission, never while holding the global scope gate.
        let incarnations = records
            .iter()
            .map(|(collection, _)| {
                self.collections
                    .get(collection)
                    .map(|current| Arc::clone(&current.read().incarnation))
            })
            .collect::<Vec<_>>();
        let state = self.catalog_admission.state.read();
        self.catalog_admission.validate_token(&state, token)?;
        state.check_actor(Some(token.serial))?;
        let scope = &state.scopes[&token.serial];
        if names.iter().any(|name| !scope.names.contains(name)) {
            return Err(BicDbError::TransactionConflict(
                "undo relation is outside its reserved catalog scope".into(),
            ));
        }
        for ((collection, id), incarnation) in records.iter().zip(&incarnations) {
            if !scope.covers_record(collection, incarnation.as_ref(), Some(id))
                || !scope.allows_record(collection, incarnation.as_ref(), Some(id))
            {
                return Err(BicDbError::TransactionConflict(
                    "undo record is outside its reserved catalog scope".into(),
                ));
            }
        }
        Ok(())
    }

    /// Select the actual statement owner, or explicitly mask an enclosing
    /// owner's identity after a statement ended its transaction. An unowned
    /// context grants no access to another owner's reserved catalog.
    pub fn enter_catalog_read_access(
        &self,
        owner: Option<&Transaction>,
    ) -> Result<Option<CatalogReadAccess>> {
        if let Some(owner) = owner {
            if !Arc::ptr_eq(&self.catalog_admission, &owner.catalog_admission) {
                return Err(BicDbError::Unsupported(
                    "foreign catalog read transaction".into(),
                ));
            }
            return owner.enter_catalog_read_access().map(Some);
        }
        let admission = Arc::downgrade(&self.catalog_admission);
        if !READ_ACCESS.with(|frames| {
            frames
                .borrow()
                .iter()
                .any(|frame| Weak::ptr_eq(&frame.admission, &admission))
        }) {
            return Ok(None);
        }
        self.check_recovery_state()?;
        let identity = Arc::new(());
        READ_ACCESS.with(|frames| {
            frames.borrow_mut().push(ReadFrame {
                admission,
                owner: TransactionId(0),
                bindings: Weak::new(),
                actor: None,
                identity: Arc::clone(&identity),
            })
        });
        Ok(Some(CatalogReadAccess {
            identity,
            _not_send: PhantomData,
        }))
    }

    /// Check a SQL catalog read before consulting any cache. `None` describes
    /// a collection scan. Whole collection reservations protect every record;
    /// record reservations protect their declared keys and backing identity.
    ///
    /// The host must use this for each metadata dependency; it is not a query
    /// parser, a durable journal, or authority to bypass ordinary data policy.
    pub fn check_catalog_read_access(&self, collection: &str, id: Option<&str>) -> Result<()> {
        if self.catalog_admission.active.load(AtomicOrdering::Acquire) == 0
            && HOST_ACCESS.with(|frames| frames.borrow().is_empty())
            && READ_ACCESS.with(|frames| frames.borrow().is_empty())
        {
            return Ok(());
        }
        self.check_recovery_state()?;
        let incarnation = self
            .collections
            .get(collection)
            .map(|current| Arc::clone(&current.read().incarnation));
        if let Some(actor) = self.catalog_admission.ambient_actor()? {
            let state = self.catalog_admission.state.read();
            if state.scopes[&actor].covers_record(collection, incarnation.as_ref(), id)
                && !state.scopes[&actor].allows_record(collection, incarnation.as_ref(), id)
            {
                return Err(BicDbError::TransactionConflict(
                    "catalog record backing was replaced after reservation".into(),
                ));
            }
            state.check_record_access(
                TransactionId(0),
                Some(actor),
                collection,
                incarnation.as_ref(),
                id,
            )?;
            return self.check_recovery_state();
        }
        let admission = Arc::downgrade(&self.catalog_admission);
        let frame = READ_ACCESS.with(|frames| {
            frames
                .borrow()
                .iter()
                .rev()
                .find(|frame| Weak::ptr_eq(&frame.admission, &admission))
                .cloned()
        });
        let state = self.catalog_admission.state.read();
        if let Some(frame) = frame {
            if frame.owner == TransactionId(0) {
                state.check_record_access(
                    TransactionId(0),
                    None,
                    collection,
                    incarnation.as_ref(),
                    id,
                )?;
                return self.check_recovery_state();
            }
            let registrations = self.catalog_admission.registrations.lock();
            let registration = registrations
                .get(&frame.owner.0)
                .filter(|entry| {
                    Weak::ptr_eq(&entry.bindings, &frame.bindings)
                        && entry.actor_scope == frame.actor
                })
                .ok_or_else(|| {
                    BicDbError::TransactionConflict("catalog read owner is no longer active".into())
                })?;
            let bindings = registration.bindings.upgrade().ok_or_else(|| {
                BicDbError::TransactionConflict("catalog read owner has expired".into())
            })?;
            if frame.actor.is_some_and(|actor| {
                state.scopes.get(&actor).is_some_and(|scope| {
                    scope.covers_record(collection, incarnation.as_ref(), id)
                        && !scope.allows_record(collection, incarnation.as_ref(), id)
                })
            }) {
                return Err(BicDbError::TransactionConflict(
                    "catalog record backing was replaced after reservation".into(),
                ));
            }
            state.check_record_access(
                frame.owner,
                frame.actor,
                collection,
                incarnation.as_ref(),
                id,
            )?;
            if state.scopes.values().any(|scope| {
                scope.owner == frame.owner
                    && scope.closing
                    && scope.covers_record(collection, incarnation.as_ref(), id)
            }) {
                return Err(BicDbError::TransactionConflict(
                    "catalog read owner is finalizing its decision".into(),
                ));
            }
            if state
                .scopes
                .values()
                .filter(|scope| scope.owner == frame.owner)
                .any(|scope| {
                    scope.records.iter().any(|(name, rows)| {
                        rows.matches(name, incarnation.as_ref(), collection)
                            && id.is_none_or(|id| rows.ids.contains(id))
                            && !rows.same_backing(incarnation.as_ref())
                    })
                })
            {
                return Err(BicDbError::TransactionConflict(
                    "catalog record backing was replaced after reservation".into(),
                ));
            }
            if let Some(incarnation) = incarnation {
                CatalogAdmission::remember_record(&bindings, collection, incarnation, id)?;
            }
        } else {
            state.check_record_access(
                TransactionId(0),
                None,
                collection,
                incarnation.as_ref(),
                id,
            )?;
        }
        self.check_recovery_state()
    }

    /// Enter this capability only around trusted internal catalog effects.
    /// Transactions begun on this thread capture it; ordinary transactions on
    /// other threads retain their own authority and remain fenced. This is
    /// not permission to execute arbitrary SQL or bypass native data policy.
    pub fn enter_catalog_scope(&self, token: &CatalogScopeToken) -> Result<CatalogScopeAccess> {
        self.check_recovery_state()?;
        let state = self.catalog_admission.state.read();
        self.catalog_admission.validate_token(&state, token)?;
        state.check_actor(Some(token.serial))?;
        let identity = Arc::new(());
        HOST_ACCESS.with(|frames| {
            frames.borrow_mut().push(AccessFrame {
                token: token.clone(),
                identity: Arc::clone(&identity),
            })
        });
        Ok(CatalogScopeAccess {
            identity,
            _not_send: PhantomData,
        })
    }

    /// Find an in-memory scope for a durable journal owned by this trusted
    /// host. Recovery must separately establish that it may repair the journal;
    /// this lookup neither steals ownership nor starts repair or a transaction.
    pub fn catalog_scope_for_context(&self, context: &str) -> Result<Option<CatalogScopeToken>> {
        self.check_recovery_state()?;
        Ok(self
            .catalog_admission
            .state
            .read()
            .scopes
            .iter()
            .find(|(_, scope)| scope.context.as_deref() == Some(context))
            .map(|(serial, scope)| CatalogScopeToken {
                admission: Arc::downgrade(&self.catalog_admission),
                owner: scope.owner,
                serial: *serial,
            }))
    }

    /// Re-enter host cleanup after a consumed owner's failed decision. A
    /// successful outer decision stays closed until explicit finalization.
    /// The host must verify that its durable journal remains undecided before
    /// calling this recovery API. Read-only replicas cannot perform recovery.
    pub fn resume_catalog_scope_for_recovery(&self, token: &CatalogScopeToken) -> Result<()> {
        self.ensure_writable("resume catalog recovery scope")?;
        let mut state = self.catalog_admission.state.write();
        self.catalog_admission.validate_token(&state, token)?;
        let registrations = self.catalog_admission.registrations.lock();
        if registrations.contains_key(&token.owner.0) {
            return Err(BicDbError::TransactionConflict(
                "catalog owner or internal work is still active".into(),
            ));
        }
        drop(registrations);
        if self
            .catalog_admission
            .has_overlapping_children(&state, token.serial)
        {
            return Err(BicDbError::TransactionConflict(
                "overlapping internal catalog work is still active".into(),
            ));
        }
        let scope = state.scopes.get_mut(&token.serial).unwrap();
        if scope.owner_outcome == Some(TxState::Committed) {
            return Err(BicDbError::TransactionConflict(
                "committed catalog scope cannot be resumed as undecided recovery".into(),
            ));
        }
        if scope.context.is_none() {
            return Err(BicDbError::Unsupported(
                "catalog scope has no durable recovery context".into(),
            ));
        }
        scope.closing = false;
        Ok(())
    }

    /// Finalize only after the journal's commit is durable or its complete undo
    /// has finished. The host owns that durability proof. Pending delegated
    /// work refuses release, so a late internal commit cannot pass the fence.
    pub fn finish_catalog_scope(&self, token: &CatalogScopeToken) -> Result<()> {
        self.check_recovery_state()?;
        let mut state = self.catalog_admission.state.write();
        self.catalog_admission.validate_token(&state, token)?;
        if self
            .catalog_admission
            .has_overlapping_children(&state, token.serial)
        {
            return Err(BicDbError::TransactionConflict(
                "internal catalog work is still pending at finalization".into(),
            ));
        }
        state.scopes.remove(&token.serial);
        self.catalog_admission
            .active
            .store(state.scopes.len(), AtomicOrdering::Release);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scope_and_registration_drop_use_only_owned_handles_after_database_drop() {
        let root = tempfile::tempdir().unwrap();
        let mut db = BicDb::open(root.path()).unwrap();
        db.create_collection("source").unwrap();
        let owner = db.begin_transaction().unwrap();
        let token = owner
            .reserve_catalog_collections(&["source".into(), "renamed".into()])
            .unwrap();
        let admission = Arc::downgrade(&db.catalog_admission);
        drop(db);
        assert!(admission.upgrade().is_some());
        drop(owner);
        assert!(admission.upgrade().is_none());
        assert!(token.admission.upgrade().is_none());
    }
}
