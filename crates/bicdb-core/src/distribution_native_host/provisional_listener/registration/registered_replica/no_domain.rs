//! Actual original Registered selected-range absence, never a genesis/floor.
use super::*;
use crate::db::{NativeRecoveryAdmission, NativeRegisteredNoDomainScope, RangeImageDbBindings};
use crate::distribution_consensus::NativeOwnedMetadataMemberObservation;
use crate::distribution_range_consensus::{NativeRegisteredRangeCatalogueAbsence, RangeWriteStore};
use crate::distribution_range_handoff::NativeJointHandoffPlan;
use crate::distribution_range_mutation::FrozenRangeMutationPermit;
use crate::distribution_range_recovery::HandoffRoleAdmission;

pub(crate) struct NativeRegisteredSelectedRangeNoDomain<'a> {
    original: &'a NativeRegisteredReplicaAdmission,
    role: &'a HandoffRoleAdmission,
    own: &'a HostMetadataReadObservation,
    business: Uuid,
    frozen: &'a FrozenRangeMutationPermit,
    caller: &'a CancellationToken,
    proof: NativeOwnedMetadataMemberObservation,
    native: NativeRecoveryAdmission,
    bindings: RangeImageDbBindings,
    catalogue: NativeRegisteredRangeCatalogueAbsence,
    service: Weak<Mutex<ClusterDataNodeService>>,
    runtime: Weak<RangeRecoveryRuntime>,
    backend: Weak<NativeMetadataControlBackend>,
    _local: std::marker::PhantomData<std::rc::Rc<()>>,
}
pub(super) struct RegisteredRemoteNativePreflight {
    pub(super) native: NativeRecoveryAdmission,
    pub(super) bindings: RangeImageDbBindings,
    catalogue: NativeRegisteredRangeCatalogueAbsence,
    safety: crate::db::NativeRangeInstallSafety,
    metadata_store: Uuid,
}
impl RegisteredRemoteNativePreflight {
    pub(super) fn require_original_boundary(
        &self,
        db: &BicDb,
        scope: &crate::db::NativeRegisteredRemoteAttemptScope<'_>,
        member: &NativeOwnedMetadataMemberObservation,
    ) -> Result<()> {
        if member.store_id() != self.metadata_store
            || self.native.birth_id() != scope.native().birth_id()
        {
            return Err(invalid(
                "Registered proposal original metadata/native owner changed",
            ));
        }
        member.require_database_point(db)?;
        db.validate_range_image_bindings(&self.bindings)?;
        scope.require_install_safety(&self.safety)
    }
    pub(super) fn require_attempt_current(
        &self,
        db: &BicDb,
        store: &RangeWriteStore,
        scope: &crate::db::NativeRegisteredRemoteAttemptScope<'_>,
        role: &crate::distribution_range_recovery::RecoveryRoleAdmission,
        member: &NativeOwnedMetadataMemberObservation,
    ) -> Result<()> {
        self.require_original_boundary(db, scope, member)?;
        self.catalogue
            .require_attempt_current(store, db, scope, role)?;
        scope.require_install_safety(&self.safety)
    }
    pub(super) fn catalogue(&self) -> &NativeRegisteredRangeCatalogueAbsence {
        &self.catalogue
    }
    pub(super) fn safety(&self) -> &crate::db::NativeRangeInstallSafety {
        &self.safety
    }
    pub(super) fn require_current(
        &self,
        db: &BicDb,
        store: &RangeWriteStore,
        scope: &NativeRegisteredNoDomainScope<'_>,
        role: &crate::distribution_range_recovery::RecoveryRoleAdmission,
        member: &NativeOwnedMetadataMemberObservation,
    ) -> Result<()> {
        if member.store_id() != self.metadata_store
            || self.native.birth_id() != scope.native().birth_id()
        {
            return Err(invalid(
                "remote transfer changed original protected member/native preflight",
            ));
        }
        member.require_database_point(db)?;
        db.validate_range_image_bindings(&self.bindings)?;
        scope.require_selected_rows_absent(role.current_range(), &self.bindings)?;
        self.catalogue.require_current(store, db, scope, role)?;
        scope.require_install_safety(&self.safety)
    }
}
impl NativeRegisteredReplicaAdmission {
    /// Role and OWN are the actual caller's existing request, acquired BEFORE
    /// all engine guards. This does not acquire another network observation.
    pub(super) fn observe_selected_range_no_domain<'a>(
        &'a self,
        role: &'a HandoffRoleAdmission,
        own: &'a HostMetadataReadObservation,
        business: Uuid,
        frozen: &'a FrozenRangeMutationPermit,
        parent: &'a CancellationToken,
    ) -> Result<NativeRegisteredSelectedRangeNoDomain<'a>> {
        parent.check()?;
        role.role().require_own_host_metadata(own, business)?;
        let service = self
            .service
            .upgrade()
            .ok_or_else(|| invalid("NoDomain original service ended"))?;
        let runtime = self
            .runtime
            .upgrade()
            .ok_or_else(|| invalid("NoDomain original runtime ended"))?;
        let backend = self
            .backend
            .upgrade()
            .ok_or_else(|| invalid("NoDomain original MStore ended"))?;
        let factory = self
            .factory
            .upgrade()
            .ok_or_else(|| invalid("NoDomain original factory ended"))?;
        self.require_listener(&service, &runtime)?;
        let actual_scope =
            crate::distribution_range_consensus::RecoveryFenceScope::from_role(role.role())?;
        let actual_frozen = runtime.source_capture_freeze(&actual_scope)?;
        if !std::ptr::eq(frozen, actual_frozen.as_ref()) {
            return Err(invalid(
                "NoDomain Frozen is not the actual retained receiver session owner",
            ));
        }
        let plan = NativeJointHandoffPlan::from_actual_role_and_own(role.role(), own, business)?;
        let target = role.committed_relocation();
        if !Arc::ptr_eq(&service, &backend.service)
            || !factory.spec.initial_ranges.is_empty()
            || role.role().local_node_id() != &factory.spec.node_id
            || role.role().local_member_incarnation() != factory.spec.member_incarnation
            || target.target != *role.role().local_node_id()
            || target.phase != crate::distribution::RelocationPhase::LearnerAllocated
            || target.promoted_epoch.is_some()
            || !plan.old_range().replicas.iter().any(|r| {
                r.id == target.target_replica_id
                    && r.node_id == target.target
                    && r.role == crate::RangeReplicaRole::Learner
            })
        {
            return Err(invalid(
                "NoDomain requires actual original Registered pending target/learner birth",
            ));
        }
        let proof = {
            let actual = backend.lock_metadata(parent)?;
            actual.require_listener_database(&self.database)?;
            actual.observe_owned_native_metadata_member(own, business, parent)?
        };
        if proof.store_id() != self.metadata_store_id {
            return Err(invalid("NoDomain original protected MStore UUID changed"));
        }
        let (native, bindings, catalogue) = {
            let actual = service
                .try_lock()
                .ok_or_else(|| invalid("NoDomain actual service busy"))?;
            let database = actual.database();
            if !Arc::ptr_eq(&database, &factory.database) {
                return Err(invalid("NoDomain actual original DB owner changed"));
            }
            let db = database
                .try_read()
                .ok_or_else(|| invalid("NoDomain actual DB busy"))?;
            self.database.require_database(&db)?;
            let native = db.native_recovery_admission(
                role.role().cluster_id(),
                role.role().bootstrap_id(),
                role.role().local_node_id(),
                role.role().local_member_incarnation(),
            )?;
            if crate::distribution::NativeStoreBirthV1::from_actual_native(&native)?
                != *factory.actual_birth()
            {
                return Err(invalid("NoDomain actual protected original birth changed"));
            }
            let (bindings, catalogue) = db.with_registered_selected_no_domain_exclusion(
                &native,
                &self.original_hook,
                role.role().current_range().id,
                frozen,
                parent,
                |scope| {
                    actual.with_writer_data_domain_store(&db, |store| {
                        proof.require_database_point(&db)?;
                        if self.store.id() != store.native_writer_store_open_id() {
                            return Err(invalid("NoDomain actual original Store slot changed"));
                        }
                        let catalogue = store.observe_registered_selected_catalogue_absence(
                            &db,
                            scope,
                            role.role(),
                        )?;
                        let bindings = db.capture_range_image_bindings()?;
                        if role.role().expected_schema_sha256() != Some(bindings.schema_sha256()) {
                            return Err(invalid(
                                "NoDomain actual selected schema differs from fresh role",
                            ));
                        }
                        db.validate_range_image_bindings(&bindings)?;
                        scope
                            .require_selected_rows_absent(role.role().current_range(), &bindings)?;
                        proof.require_database_point(&db)?;
                        scope.require_database(&db)?;
                        Ok((bindings, catalogue))
                    })
                },
            )?;
            (native, bindings, catalogue)
        };
        let result = NativeRegisteredSelectedRangeNoDomain {
            original: self,
            role,
            own,
            business,
            frozen,
            caller: parent,
            proof,
            native,
            bindings,
            catalogue,
            service: Arc::downgrade(&service),
            runtime: Arc::downgrade(&runtime),
            backend: Arc::downgrade(&backend),
            _local: std::marker::PhantomData,
        };
        result.require_metadata_current()?;
        result.require_live()?;
        Ok(result)
    }
}
impl NativeRegisteredSelectedRangeNoDomain<'_> {
    pub(crate) fn role(&self) -> &HandoffRoleAdmission {
        self.role
    }
    pub(crate) fn original_freeze_owner(&self) -> Uuid {
        self.frozen.owner()
    }
    pub(crate) fn plan(&self) -> Result<NativeJointHandoffPlan> {
        self.require_live()?;
        NativeJointHandoffPlan::from_actual_role_and_own(self.role.role(), self.own, self.business)
    }
    pub(super) fn into_remote_native_preflight(self) -> Result<RegisteredRemoteNativePreflight> {
        self.into_remote_native_preflight_with(|_original, _db, _store, _scope| Ok(()))
            .map(|(actual, ())| actual)
    }
    pub(super) fn into_remote_native_preflight_with<R>(
        self,
        operation: impl for<'scope> FnOnce(
            &Self,
            &BicDb,
            &RangeWriteStore,
            &'scope NativeRegisteredNoDomainScope<'_>,
        ) -> Result<R>,
    ) -> Result<(RegisteredRemoteNativePreflight, R)> {
        self.require_metadata_current()?;
        let actual = self
            .service
            .upgrade()
            .ok_or_else(|| invalid("remote preflight actual service ended"))?;
        let (safety, prepared) = {
            let service = actual
                .try_lock()
                .ok_or_else(|| invalid("remote preflight service busy"))?;
            let database = service.database();
            let db = database
                .try_read()
                .ok_or_else(|| invalid("remote preflight database busy"))?;
            self.with_native_exclusion(&db, |scope| {
                service.with_writer_data_domain_store(&db, |store| {
                    self.require_database_store(&db, store, scope)?;
                    let safety = scope.observe_install_safety()?;
                    let prepared = operation(&self, &db, store, scope)?;
                    self.require_database_store(&db, store, scope)?;
                    Ok((safety, prepared))
                })
            })?
        };
        self.require_metadata_current()?;
        Ok((
            RegisteredRemoteNativePreflight {
                native: self.native,
                bindings: self.bindings,
                catalogue: self.catalogue,
                safety,
                metadata_store: self.proof.store_id(),
            },
            prepared,
        ))
    }
    #[cfg(test)]
    pub(crate) fn test_keep_original_registration(
        &self,
    ) -> Result<impl std::any::Any + Send + Sync> {
        self.original
            .original_hook
            .test_keep_original_registration()
    }
    pub(crate) fn range_id(&self) -> crate::RangeId {
        self.role.role().current_range().id
    }
    pub(crate) fn require_live(&self) -> Result<()> {
        self.caller.check()?;
        self.role
            .role()
            .require_own_host_metadata(self.own, self.business)?;
        let service = self
            .service
            .upgrade()
            .ok_or_else(|| invalid("NoDomain actual service ended"))?;
        let runtime = self
            .runtime
            .upgrade()
            .ok_or_else(|| invalid("NoDomain actual runtime ended"))?;
        self.original.require_listener(&service, &runtime)
    }
    /// Only outside DB/service/admission/native-journal guards.
    pub(crate) fn require_metadata_current(&self) -> Result<()> {
        self.require_live()?;
        let backend = self
            .backend
            .upgrade()
            .ok_or_else(|| invalid("NoDomain actual MStore ended"))?;
        let metadata = backend.lock_metadata(self.caller)?;
        metadata.require_listener_database(&self.original.database)?;
        self.proof.require_current(&metadata)?;
        self.require_live()
    }
    /// Read-only necessary point check UNDER the caller's real scoped native
    /// exclusion. A future typed Installing producer must perform this BEFORE
    /// its first write and validate its OWN exact receipt after changing state.
    pub(crate) fn require_database_store(
        &self,
        db: &BicDb,
        store: &RangeWriteStore,
        scope: &NativeRegisteredNoDomainScope<'_>,
    ) -> Result<()> {
        self.require_live()?;
        self.original.database.require_database(db)?;
        self.proof.require_database_point(db)?;
        if self.original.store.id() != store.native_writer_store_open_id()
            || scope.range_id() != self.range_id()
            || scope.native().birth_id() != self.native.birth_id()
            || scope.native().birth_sha256() != self.native.birth_sha256()
        {
            return Err(invalid(
                "NoDomain actual selected scope/Store/birth changed",
            ));
        }
        db.validate_range_image_bindings(&self.bindings)?;
        scope.require_selected_rows_absent(self.role.role().current_range(), &self.bindings)?;
        self.catalogue
            .require_current(store, db, scope, self.role.role())?;
        self.require_live()
    }
    /// No network/MStore guard and no row grant. The callback cannot return
    /// the scoped journal/hook exclusion; it still needs typed Installing and
    /// complete source/history admission for any later native mutation.
    pub(crate) fn with_native_exclusion<R>(
        &self,
        db: &BicDb,
        operation: impl for<'scope> FnOnce(&'scope NativeRegisteredNoDomainScope<'_>) -> Result<R>,
    ) -> Result<R> {
        self.require_live()?;
        self.original.database.require_database(db)?;
        db.with_registered_selected_no_domain_exclusion(
            &self.native,
            &self.original.original_hook,
            self.range_id(),
            self.frozen,
            self.caller,
            |scope| {
                self.proof.require_database_point(db)?;
                db.validate_range_image_bindings(&self.bindings)?;
                let result = operation(scope);
                let post = self
                    .require_live()
                    .and_then(|()| self.proof.require_database_point(db));
                result.and_then(|value| post.map(|()| value))
            },
        )
    }
}
