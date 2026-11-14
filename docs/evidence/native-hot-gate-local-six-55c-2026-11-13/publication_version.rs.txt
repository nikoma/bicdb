//! ACTUAL-open local metadata publication version, not a distributed lease.
//! Metadata mutators invalidate BEFORE changing authority state. Native IO may
//! outlive a version: exact attempts then remain historical, never grants.
//! The final read guard spans ONLY bounded in-memory publication.
use super::*;
use parking_lot::{RwLock, RwLockReadGuard};
use std::sync::Arc;

#[derive(Debug)]
struct VersionState {
    generation: u64,
    ready: bool,
    failed: bool,
    initializing: bool,
    durable_generation: Option<u64>,
    operations: BTreeMap<Uuid, u64>,
}
#[derive(Debug)]
pub(super) struct MetadataPublicationVersion {
    actual_open: Uuid,
    state: RwLock<VersionState>,
}
/// This unique owner lives only in the ACTUAL MStore. Observations retain the
/// gate, never this owner. Drop closes the version BEFORE native-owner drop.
#[derive(Debug)]
pub(super) struct MetadataPublicationVersionOwner {
    version: Arc<MetadataPublicationVersion>,
}
impl std::ops::Deref for MetadataPublicationVersionOwner {
    type Target = Arc<MetadataPublicationVersion>;
    fn deref(&self) -> &Self::Target {
        &self.version
    }
}
impl Drop for MetadataPublicationVersionOwner {
    fn drop(&mut self) {
        self.version.persistence_failed();
    }
}
pub(super) struct AuthorityOperation {
    version: Arc<MetadataPublicationVersion>,
    id: Uuid,
    completed: bool,
}
pub(super) struct PersistenceVersion {
    actual_open: Uuid,
    generation: u64,
}
/// No Clone/Deserialize/path constructor. This points to the actual original
/// MStore gate and request. A generation number alone constructs nothing.
pub(crate) struct NativeMetadataVersionPoint {
    actual: Arc<MetadataPublicationVersion>,
    generation: u64,
    request: crate::CancellationToken,
}
pub(crate) struct NativeMetadataPublicationGuard<'a> {
    point: &'a NativeMetadataVersionPoint,
    state: RwLockReadGuard<'a, VersionState>,
}
impl MetadataPublicationVersion {
    pub(super) fn new() -> MetadataPublicationVersionOwner {
        MetadataPublicationVersionOwner {
            version: Arc::new(Self {
                actual_open: Uuid::now_v7(),
                state: RwLock::new(VersionState {
                    generation: 1,
                    ready: false,
                    failed: false,
                    initializing: true,
                    durable_generation: None,
                    operations: BTreeMap::new(),
                }),
            }),
        }
    }
    pub(super) fn begin(self: &Arc<Self>) -> Result<AuthorityOperation> {
        let mut state = self
            .state
            .try_write()
            .ok_or_else(|| metadata_error("metadata authority version publication busy"))?;
        if state.failed || state.initializing || state.operations.len() >= 64 {
            return Err(metadata_error(
                "metadata authority version poisoned/uninitialized or operation bound exceeded",
            ));
        }
        let id = Uuid::now_v7();
        let generation = state.generation;
        state.operations.insert(id, generation);
        Ok(AuthorityOperation {
            version: self.clone(),
            id,
            completed: false,
        })
    }
    /// Must be called immediately BEFORE the first assignment of each actual
    /// authority change; no-op heartbeat/timer/read bookkeeping skips this.
    pub(super) fn invalidate(&self) -> Result<()> {
        let mut state = self.state.try_write().ok_or_else(|| {
            metadata_error("metadata authority final publication active; retry mutation")
        })?;
        if state.failed || (!state.initializing && state.operations.is_empty()) {
            return Err(metadata_error(
                "metadata authority mutation lacks live original operation or is poisoned",
            ));
        }
        let Some(next) = state.generation.checked_add(1) else {
            state.failed = true;
            state.ready = false;
            return Err(metadata_error(
                "metadata authority version exhausted; no ABA reset",
            ));
        };
        state.generation = next;
        state.ready = false;
        Ok(())
    }
    pub(super) fn begin_persistence(&self) -> Result<PersistenceVersion> {
        let state = self
            .state
            .try_read()
            .ok_or_else(|| metadata_error("metadata persistence version busy"))?;
        if state.failed && !state.initializing {
            return Err(metadata_error(
                "metadata authority persistence remains poisoned; explicit recovery/open required",
            ));
        }
        Ok(PersistenceVersion {
            actual_open: self.actual_open,
            generation: state.generation,
        })
    }
    pub(super) fn persisted(&self, token: PersistenceVersion) -> Result<()> {
        let mut state = self.state.write();
        if token.actual_open != self.actual_open || token.generation != state.generation {
            state.failed = true;
            state.ready = false;
            return Err(metadata_error(
                "metadata persistence crossed actual authority version",
            ));
        }
        // No harmless persist clears dirty/failed or publishes ready. Only a
        // successful original outer mutation or initialization can do that.
        state.durable_generation = Some(token.generation);
        Ok(())
    }
    pub(super) fn persistence_failed(&self) {
        let mut state = self.state.write();
        if let Some(next) = state.generation.checked_add(1) {
            state.generation = next;
        }
        state.ready = false;
        state.failed = true;
    }
    /// Explicit original actual-open initialization completion only. This is
    /// not available to a later mutation/no-op persistence recovery path.
    pub(super) fn finish_initialization(&self) -> Result<()> {
        let mut state = self.state.write();
        if !state.initializing {
            if state.ready
                && !state.failed
                && state.operations.is_empty()
                && state.durable_generation == Some(state.generation)
            {
                return Ok(());
            }
            return Err(metadata_error(
                "running metadata authority cannot re-enter initialization to clear poison",
            ));
        }
        if !state.operations.is_empty() || state.durable_generation != Some(state.generation) {
            return Err(metadata_error(
                "metadata authority initialization has no exact durable original version",
            ));
        }
        state.initializing = false;
        state.failed = false;
        state.ready = true;
        Ok(())
    }
    pub(super) fn require_ready(&self) -> Result<()> {
        let state = self
            .state
            .try_read()
            .ok_or_else(|| metadata_error("metadata authority version busy"))?;
        if state.failed || !state.ready || state.initializing {
            return Err(metadata_error(
                "metadata authority version dirty/poisoned; cannot produce positive authority",
            ));
        }
        Ok(())
    }
    pub(super) fn seal(
        self: &Arc<Self>,
        request: crate::CancellationToken,
    ) -> Result<NativeMetadataVersionPoint> {
        request.check()?;
        let state = self
            .state
            .try_read()
            .ok_or_else(|| metadata_error("metadata authority version capture busy"))?;
        if state.failed || !state.ready || state.initializing || !state.operations.is_empty() {
            return Err(metadata_error(
                "cannot capture dirty/poisoned/in-flight actual metadata authority",
            ));
        }
        Ok(NativeMetadataVersionPoint {
            actual: self.clone(),
            generation: state.generation,
            request,
        })
    }
}
impl AuthorityOperation {
    pub(super) fn finish(mut self, success: bool) -> Result<()> {
        let result = self.finish_inner(success);
        self.completed = true;
        result
    }
    fn finish_inner(&self, success: bool) -> Result<()> {
        let mut state = self.version.state.write();
        let Some(start) = state.operations.remove(&self.id) else {
            state.failed = true;
            state.ready = false;
            return Err(metadata_error(
                "metadata authority operation custody changed",
            ));
        };
        let changed = state.generation != start;
        if !success && changed {
            state.failed = true;
            state.ready = false;
        }
        if state.operations.is_empty() && changed && success {
            if state.failed || state.durable_generation != Some(state.generation) {
                state.failed = true;
                state.ready = false;
            } else {
                state.ready = true;
            }
        }
        if success && state.failed {
            return Err(metadata_error(
                "metadata authority mutation left poisoned/undurable version",
            ));
        }
        Ok(())
    }
}
impl Drop for AuthorityOperation {
    fn drop(&mut self) {
        if !self.completed {
            let _ = self.finish_inner(false);
        }
    }
}
impl NativeMetadataVersionPoint {
    pub(crate) fn require_current_memory(&self) -> Result<()> {
        let guard = self.try_publication_guard()?;
        guard.require_current_memory()
    }
    /// Necessary LOCAL version guard only. Each concrete caller MUST retain
    /// its actual lexical post-IO DB/journal/hook/A proof through this guard.
    /// Never invoke full require_live/IO/callback helpers while holding it.
    pub(crate) fn try_publication_guard(&self) -> Result<NativeMetadataPublicationGuard<'_>> {
        self.request.check()?;
        let state = self
            .actual
            .state
            .try_read()
            .ok_or_else(|| metadata_error("metadata final version gate busy"))?;
        let guard = NativeMetadataPublicationGuard { point: self, state };
        guard.require_current_memory()?;
        Ok(guard)
    }
}
impl NativeMetadataPublicationGuard<'_> {
    /// Memory-only: request atomics/deadline and same actual gate generation.
    pub(crate) fn require_current_memory(&self) -> Result<()> {
        self.point.request.check()?;
        if self.state.failed
            || !self.state.ready
            || self.state.initializing
            || self.state.generation != self.point.generation
        {
            return Err(metadata_error("actual metadata authority version changed/dirty; retained native outcome is not a current grant"));
        }
        Ok(())
    }
}
impl MetadataConsensusStore {
    pub(super) fn authority_operation<R>(
        &mut self,
        operation: impl FnOnce(&mut Self) -> Result<R>,
    ) -> Result<R> {
        let guard = self.publication_version.begin()?;
        let result = operation(self);
        let post = guard.finish(result.is_ok());
        match result {
            Err(primary) => {
                let _ = post;
                Err(primary)
            }
            Ok(value) => {
                post?;
                Ok(value)
            }
        }
    }
    pub(super) fn invalidate_authority_before_change(&self) -> Result<()> {
        self.publication_version.invalidate()
    }
    pub(super) fn finish_publication_version_initialization(&self) -> Result<()> {
        self.publication_version.finish_initialization()
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    fn initialized() -> MetadataPublicationVersionOwner {
        let gate = MetadataPublicationVersion::new();
        gate.persisted(gate.begin_persistence().unwrap()).unwrap();
        gate.finish_initialization().unwrap();
        gate
    }
    #[test]
    fn metadata_publication_version_nested_outer_completion_and_noop_do_not_self_invalidate() {
        let gate = initialized();
        let original = gate.seal(crate::CancellationToken::uncancelable()).unwrap();
        let outer = gate.begin().unwrap();
        let inner = gate.begin().unwrap();
        gate.invalidate().unwrap();
        assert!(original.require_current_memory().is_err());
        gate.persisted(gate.begin_persistence().unwrap()).unwrap();
        inner.finish(true).unwrap();
        assert!(gate.require_ready().is_err());
        outer.finish(true).unwrap();
        let current = gate.seal(crate::CancellationToken::uncancelable()).unwrap();
        let noop = gate.begin().unwrap();
        gate.persisted(gate.begin_persistence().unwrap()).unwrap();
        noop.finish(true).unwrap();
        current.require_current_memory().unwrap();
    }
    #[test]
    fn metadata_publication_version_failed_persistence_stays_poisoned_after_harmless_persist() {
        let gate = initialized();
        let original = gate.seal(crate::CancellationToken::uncancelable()).unwrap();
        let operation = gate.begin().unwrap();
        gate.invalidate().unwrap();
        gate.persistence_failed();
        operation.finish(false).unwrap();
        assert!(original.require_current_memory().is_err());
        assert!(gate.begin_persistence().is_err());
        assert!(gate.begin().is_err());
        assert!(gate.require_ready().is_err());
    }
    #[test]
    fn metadata_publication_version_changed_error_without_persist_cannot_be_cleared() {
        let gate = initialized();
        let operation = gate.begin().unwrap();
        gate.invalidate().unwrap();
        operation.finish(false).unwrap();
        assert!(gate.begin_persistence().is_err());
        assert!(gate.finish_initialization().is_err());
    }
    #[test]
    fn metadata_publication_version_short_guard_refuses_authority_change_before_assignment() {
        let gate = initialized();
        let old = gate.seal(crate::CancellationToken::uncancelable()).unwrap();
        let operation = gate.begin().unwrap();
        let publication = old.try_publication_guard().unwrap();
        assert!(
            gate.invalidate().is_err(),
            "actual change cannot invalidate past held final guard"
        );
        publication.require_current_memory().unwrap();
        drop(publication);
        operation.finish(true).unwrap();
        old.require_current_memory().unwrap();
        let operation = gate.begin().unwrap();
        gate.invalidate().unwrap();
        assert!(
            old.try_publication_guard().is_err(),
            "old point rejected before changed state can become usable"
        );
        gate.persisted(gate.begin_persistence().unwrap()).unwrap();
        operation.finish(true).unwrap();
        assert!(
            old.try_publication_guard().is_err(),
            "durable newer generation never revives old point"
        );
    }
    #[test]
    fn metadata_publication_version_nested_failed_change_cannot_be_hidden_by_outer_persist() {
        let gate = initialized();
        let outer = gate.begin().unwrap();
        let inner = gate.begin().unwrap();
        gate.invalidate().unwrap();
        inner.finish(false).unwrap();
        assert!(gate.begin_persistence().is_err());
        assert!(outer.finish(true).is_err());
        assert!(gate.require_ready().is_err());
    }
    #[test]
    fn metadata_publication_version_original_open_owner_drop_revokes_retained_point() {
        let owner = initialized();
        let original = owner
            .seal(crate::CancellationToken::uncancelable())
            .unwrap();
        drop(owner);
        assert!(original.try_publication_guard().is_err());
    }
}
