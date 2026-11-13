//! Server-owned native invariant authority for one coherent active catalog.
use super::*;

#[derive(Clone)]
pub(crate) struct NativeInvariantBundle {
    owner: String,
    package_sha256: String,
    definition_sha256: String,
    validators: Arc<[bicdb_core::NativeCommitValidator]>,
}

pub(super) fn prepare_native_invariant_bundles(
    catalog: &RuntimeCatalog,
) -> Result<Arc<[NativeInvariantBundle]>> {
    let mut bundles = Vec::new();
    for (owner, snapshot) in &catalog.packages {
        let application = snapshot
            .package
            .manifest
            .application
            .as_deref()
            .ok_or_else(|| {
                AppRuntimeError::InvalidPackage(
                    "active owner lacks its application manifest".into(),
                )
            })?;
        // Resolve resource/dependency names through the actual verified owner,
        // never through a caller's imported or authority-restricted manifest.
        let validators = lower_carrier_invariants(application)?;
        if validators.is_empty() {
            continue;
        }
        let definition_sha256 = hex::encode(Sha256::digest(serde_json::to_vec(&validators)?));
        bundles.push(NativeInvariantBundle {
            owner: owner.clone(),
            package_sha256: snapshot.package_hash.clone(),
            definition_sha256,
            validators: validators.into(),
        });
    }
    Ok(bundles.into())
}

#[derive(Default)]
struct CatalogAdmissionState {
    invocations: usize,
    lifecycle: bool,
}

/// Exclusive catalog changes require quiescence. Refuse and let the operator
/// retry rather than waiting while holding a database lock or nesting services.
#[derive(Default)]
pub(crate) struct CatalogAdmission {
    state: Mutex<CatalogAdmissionState>,
}

impl CatalogAdmission {
    fn enter_invocation(self: &Arc<Self>) -> Result<CatalogInvocationLease> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| AppRuntimeError::NotReady("catalog admission is unavailable".into()))?;
        if state.lifecycle {
            return Err(AppRuntimeError::NotReady(
                "application catalog is changing; retry invocation".into(),
            ));
        }
        state.invocations = state.invocations.checked_add(1).ok_or_else(|| {
            AppRuntimeError::NotReady("catalog invocation capacity exhausted".into())
        })?;
        Ok(CatalogInvocationLease(Arc::clone(self)))
    }

    pub(super) fn enter_lifecycle(self: &Arc<Self>) -> Result<CatalogLifecycleLease> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| AppRuntimeError::NotReady("catalog admission is unavailable".into()))?;
        if state.lifecycle || state.invocations != 0 {
            return Err(AppRuntimeError::NotReady(
                "catalog change requires quiescent application invocations; retry after draining traffic".into(),
            ));
        }
        state.lifecycle = true;
        Ok(CatalogLifecycleLease(Arc::clone(self)))
    }
}

struct CatalogInvocationLease(Arc<CatalogAdmission>);

impl Drop for CatalogInvocationLease {
    fn drop(&mut self) {
        if let Ok(mut state) = self.0.state.lock() {
            state.invocations -= 1;
        }
    }
}

pub(super) struct CatalogLifecycleLease(Arc<CatalogAdmission>);

impl Drop for CatalogLifecycleLease {
    fn drop(&mut self) {
        if let Ok(mut state) = self.0.state.lock() {
            state.lifecycle = false;
        }
    }
}

pub(crate) struct RuntimeInvariantProvider {
    catalog: Arc<RwLock<Arc<RuntimeCatalog>>>,
    admission: Arc<CatalogAdmission>,
}

impl RuntimeInvariantProvider {
    pub(super) fn new(
        catalog: Arc<RwLock<Arc<RuntimeCatalog>>>,
        admission: Arc<CatalogAdmission>,
    ) -> Self {
        Self { catalog, admission }
    }

    pub(crate) fn acquire(
        &self,
        application: &str,
        package_sha256: &str,
    ) -> Result<RuntimeInvariantContext> {
        let lease = self.admission.enter_invocation()?;
        let catalog = self.catalog.read().clone();
        let snapshot = catalog
            .packages
            .get(&normalized(application))
            .ok_or_else(|| {
                AppRuntimeError::NotReady("invocation package is no longer active".into())
            })?;
        if snapshot.package_hash != package_sha256 {
            return Err(AppRuntimeError::NotReady(
                "invocation package differs from the active catalog; retry against the current package".into(),
            ));
        }
        Ok(RuntimeInvariantContext {
            bundles: Arc::clone(&catalog.native_invariants),
            _lease: lease,
        })
    }
}

pub(crate) struct RuntimeInvariantContext {
    bundles: Arc<[NativeInvariantBundle]>,
    _lease: CatalogInvocationLease,
}

impl RuntimeInvariantContext {
    pub(crate) fn attach(&self, transaction: &mut bicdb_core::Transaction) -> Result<()> {
        for bundle in self.bundles.iter() {
            transaction.register_mutation_invariant_bundle(
                &bundle.owner,
                &bundle.package_sha256,
                &bundle.definition_sha256,
                Arc::clone(&bundle.validators),
            )?;
        }
        Ok(())
    }
}
