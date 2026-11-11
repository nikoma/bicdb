//! Native transaction admission for a declared collection catalog operation.
//! The SQL host must separately reserve its complete dependency closure and
//! fence catalog/Describe readers; these runtime reservations are not recovery.
use super::*;
use std::sync::Weak;

type Bindings = Mutex<FxHashMap<String, Arc<()>>>;

#[derive(Clone, Debug)]
pub struct CatalogScopeToken {
    admission: Weak<CatalogAdmission>,
    owner: TransactionId,
    serial: u64,
}

#[derive(Debug)]
struct Scope {
    owner: TransactionId,
    names: BTreeSet<String>,
    incarnations: Vec<Arc<()>>,
}

impl Scope {
    fn covers(&self, name: &str, incarnation: Option<&Arc<()>>) -> bool {
        self.names.contains(name)
            || incarnation.is_some_and(|current| {
                self.incarnations
                    .iter()
                    .any(|expected| Arc::ptr_eq(current, expected))
            })
    }
}

#[derive(Debug, Default)]
struct State {
    registrations: FxHashMap<u64, Weak<Bindings>>,
    scopes: BTreeMap<u64, Scope>,
    next_serial: u64,
}

impl State {
    fn check_access(
        &self,
        owner: TransactionId,
        collection: &str,
        incarnation: Option<&Arc<()>>,
    ) -> Result<()> {
        if self
            .scopes
            .values()
            .any(|scope| scope.owner != owner && scope.covers(collection, incarnation))
        {
            return Err(BicDbError::TransactionConflict(format!(
                "collection `{collection}` is reserved by another catalog operation"
            )));
        }
        Ok(())
    }
}

#[derive(Debug, Default)]
pub(super) struct CatalogAdmission {
    state: RwLock<State>,
    active: AtomicUsize,
}

impl CatalogAdmission {
    pub(super) fn register(&self, owner: TransactionId, bindings: &Arc<Bindings>) {
        self.state
            .write()
            .registrations
            .insert(owner.0, Arc::downgrade(bindings));
    }

    pub(super) fn finish(&self, owner: TransactionId) {
        // Uses only owned shared handles: never dereference a database during
        // Transaction::drop, including while another thread owns its write lock.
        let mut state = self.state.write();
        state.registrations.remove(&owner.0);
        state.scopes.retain(|_, scope| scope.owner != owner);
        self.active
            .store(state.scopes.len(), AtomicOrdering::Release);
    }

    pub(super) fn bind(
        &self,
        owner: TransactionId,
        bindings: &Bindings,
        collection: &str,
        current: impl FnOnce() -> Result<Arc<()>>,
    ) -> Result<()> {
        // Already-published bindings make a conflicting reservation refuse.
        // Only a first observation needs the shared admission gate when there
        // are no active scopes, keeping repeated ordinary reads inexpensive.
        if self.active.load(AtomicOrdering::Acquire) == 0
            && bindings.lock().contains_key(collection)
        {
            return Self::remember(bindings, collection, current()?);
        }
        let state = self.state.read();
        // Check absent names too: a reserved rename destination/source must
        // not return NotFound while its owner has an unfinished catalog move.
        state.check_access(owner, collection, None)?;
        let current = current()?;
        state.check_access(owner, collection, Some(&current))?;
        Self::remember(bindings, collection, current)
    }

    fn remember(bindings: &Bindings, collection: &str, incarnation: Arc<()>) -> Result<()> {
        let mut bindings = bindings.lock();
        match bindings.get(collection) {
            Some(expected) if !Arc::ptr_eq(expected, &incarnation) => {
                Err(BicDbError::TransactionConflict(format!(
                    "collection `{collection}` was replaced after this transaction observed it"
                )))
            }
            Some(_) => Ok(()),
            None => {
                bindings.insert(collection.to_string(), incarnation);
                Ok(())
            }
        }
    }

    pub(super) fn owner_covers(&self, owner: TransactionId, old: &str, new: &str) -> bool {
        self.state.read().scopes.values().any(|scope| {
            scope.owner == owner && scope.names.contains(old) && scope.names.contains(new)
        })
    }
}

impl Transaction {
    /// Reserve exact collection names and their current runtime identities.
    /// Existing observations on any protected identity refuse admission;
    /// later transactions refuse access until release, commit, rollback or
    /// drop. Absent destination names are protected too. No durable state is
    /// changed, and this grants no mutation authority or SQL catalog visibility.
    pub fn reserve_catalog_collections(&self, names: &[String]) -> Result<CatalogScopeToken> {
        let db = self.db_ref();
        db.check_recovery_state()?;
        if self.state != TxState::Pending {
            return Err(BicDbError::TransactionNotPending);
        }
        let names = names.iter().cloned().collect::<BTreeSet<_>>();
        if names.is_empty() {
            return Err(BicDbError::Unsupported("empty catalog scope".into()));
        }
        for name in &names {
            validate_collection_name(name)?;
        }
        let mut state = self.catalog_admission.state.write();
        let incarnations = names
            .iter()
            .filter_map(|name| db.collections.get(name))
            .map(|collection| Arc::clone(&collection.read().incarnation))
            .collect();
        let scope = Scope {
            owner: self.id,
            names,
            incarnations,
        };
        for name in &scope.names {
            state.check_access(self.id, name, None)?;
        }
        for existing in state.scopes.values() {
            if existing.owner != self.id
                && existing.incarnations.iter().any(|incarnation| {
                    scope
                        .incarnations
                        .iter()
                        .any(|other| Arc::ptr_eq(incarnation, other))
                })
            {
                return Err(BicDbError::TransactionConflict(
                    "catalog scope overlaps another owner's backing".into(),
                ));
            }
        }
        for (id, bindings) in &state.registrations {
            if *id == self.id.0 {
                continue;
            }
            if let Some(bindings) = bindings.upgrade() {
                if bindings
                    .lock()
                    .iter()
                    .any(|(name, incarnation)| scope.covers(name, Some(incarnation)))
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
        state.scopes.remove(&token.serial);
        self.catalog_admission
            .active
            .store(state.scopes.len(), AtomicOrdering::Release);
        Ok(())
    }
}
