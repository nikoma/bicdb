//! Actual selected Joined preimage. Retained constraints are not a native
//! zero floor, image mutation grant or substitute for a fresh request.
use super::*;
use crate::db::{NativeRangeInstallSafety, NativeRecoveryAdmission, RangeImageDbBindings};
use crate::distribution_range_consensus::{NativeJoinedRangeCatalogueAbsence, RecoveryFenceScope};
use crate::distribution_range_mutation::FrozenRangeMutationPermit;

pub(crate) struct NativeJoinedRangeReceivePreflight {
    origin: Arc<NativeJoinedRangeReceiveOrigin>,
    scope: RecoveryFenceScope,
    rows: Arc<crate::db::NativeJoinedRowAbsenceScan>,
    native: NativeRecoveryAdmission,
    bindings: RangeImageDbBindings,
    catalogue: NativeJoinedRangeCatalogueAbsence,
    safety: NativeRangeInstallSafety,
}
impl NativeJoinedRangeReceivePreflight {
    pub(crate) fn origin(&self) -> &Arc<NativeJoinedRangeReceiveOrigin> {
        &self.origin
    }
    pub(crate) fn native(&self) -> &NativeRecoveryAdmission {
        &self.native
    }
    pub(crate) fn bindings(&self) -> &RangeImageDbBindings {
        &self.bindings
    }
    pub(crate) fn safety(&self) -> &NativeRangeInstallSafety {
        &self.safety
    }
    pub(crate) fn catalogue(&self) -> &NativeJoinedRangeCatalogueAbsence {
        &self.catalogue
    }
    pub(crate) fn scope(&self) -> &RecoveryFenceScope {
        &self.scope
    }
    pub(crate) fn require_at_storage_point(
        &self,
        db: &BicDb,
        store: &RangeWriteStore,
        request: &NativeJoinedRangeReceiveRequest,
        point: &crate::db::NativeJoinedReceiveStorageScope<'_>,
    ) -> Result<()> {
        point.require_request(request)?;
        if !Arc::ptr_eq(&self.origin, request.origin_arc())
            || !self
                .scope
                .same_session(&RecoveryFenceScope::from_role(request.role().role())?)
            || point.native().birth_id() != self.native.birth_id()
            || point.native().database_incarnation() != self.native.database_incarnation()
        {
            return Err(invalid(
                "Joined selected preflight changed original borrowed request/native owner",
            ));
        }
        point.require_completed_row_scan(&self.rows)?;
        db.validate_range_image_bindings(&self.bindings)?;
        point.require_install_safety(&self.safety)?;
        self.catalogue.require_current(store, db, point)
    }
    /// Fresh Role/OWN/member and actual runtime Frozen outside all engine
    /// guards, then a new physical scope. Unrelated ranges/B may advance.
    pub(crate) fn with_current<R>(
        &self,
        request: &NativeJoinedRangeReceiveRequest,
        frozen: &Arc<FrozenRangeMutationPermit>,
        operation: impl for<'scope> FnOnce(
            &BicDb,
            &RangeWriteStore,
            &'scope crate::db::NativeJoinedReceiveStorageScope<'_>,
        ) -> Result<R>,
    ) -> Result<R> {
        request.require_metadata_current()?;
        request.require_original_frozen(frozen)?;
        if !Arc::ptr_eq(&self.origin, request.origin_arc())
            || !self
                .scope
                .same_session(&RecoveryFenceScope::from_role(request.role().role())?)
        {
            return Err(invalid(
                "Joined receive preflight changed original scope/origin",
            ));
        }
        let result = {
            let backend = self.origin.backend()?;
            let factory = self.origin.factory()?;
            let service = backend
                .service
                .try_lock()
                .ok_or_else(|| invalid("Joined receive preflight actual service busy"))?;
            let database = service.database();
            if !Arc::ptr_eq(&database, &factory.database) {
                return Err(invalid("Joined receive preflight actual DB owner changed"));
            }
            let db = database
                .try_read()
                .ok_or_else(|| invalid("Joined receive preflight actual DB busy"))?;
            service.with_writer_data_domain_store(&db, |store| {
                self.origin.require_database_store(&db, store)
            })?;
            db.with_joined_receive_storage_scope(request, &self.native, frozen, |point| {
                point.require_completed_row_scan(&self.rows)?;
                db.validate_range_image_bindings(&self.bindings)?;
                point.require_install_safety(&self.safety)?;
                service.with_writer_data_domain_store(&db, |store| {
                    self.catalogue.require_current(store, &db, point)?;
                    let result = operation(&db, store, point);
                    let post = self
                        .catalogue
                        .require_current(store, &db, point)
                        .and_then(|()| point.require_install_safety(&self.safety));
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
                })
            })
        };
        let value = result?;
        request.require_metadata_current()?;
        request.require_original_frozen(frozen)?;
        Ok(value)
    }
}
impl NativeJoinedRangeReceiveRequest {
    pub(crate) fn observe_selected_preflight(
        &self,
        rows: &Arc<crate::db::NativeJoinedRowAbsenceScan>,
        frozen: &Arc<FrozenRangeMutationPermit>,
    ) -> Result<NativeJoinedRangeReceivePreflight> {
        self.require_row_absence_complete(rows, frozen)?;
        let scope = RecoveryFenceScope::from_role(self.role.role())?;
        let (native, bindings, catalogue, safety) = {
            let backend = self.origin.backend()?;
            let factory = self.origin.factory()?;
            let service = backend
                .service
                .try_lock()
                .ok_or_else(|| invalid("Joined selected preflight actual service busy"))?;
            let database = service.database();
            if !Arc::ptr_eq(&database, &factory.database) {
                return Err(invalid("Joined selected preflight actual DB owner changed"));
            }
            let db = database
                .try_read()
                .ok_or_else(|| invalid("Joined selected preflight actual DB busy"))?;
            let role = self.role.role();
            let native = db.native_recovery_admission(
                role.cluster_id(),
                role.bootstrap_id(),
                role.local_node_id(),
                role.local_member_incarnation(),
            )?;
            service.with_writer_data_domain_store(&db, |store| {
                self.origin.require_database_store(&db, store)
            })?;
            let actual = db.with_joined_receive_storage_scope(self, &native, frozen, |point| {
                point.require_completed_row_scan(rows)?;
                service.with_writer_data_domain_store(&db, |store| {
                    let catalogue = store.observe_joined_selected_catalogue_absence(&db, point)?;
                    let safety = point.require_unmaterialized_selected()?;
                    let bindings = db.capture_range_image_bindings()?;
                    catalogue.require_current(store, &db, point)?;
                    Ok((bindings, catalogue, safety))
                })
            })?;
            (native, actual.0, actual.1, actual.2)
        };
        self.require_metadata_current()?;
        self.require_original_frozen(frozen)?;
        Ok(NativeJoinedRangeReceivePreflight {
            origin: self.origin.clone(),
            scope,
            rows: rows.clone(),
            native,
            bindings,
            catalogue,
            safety,
        })
    }
}
