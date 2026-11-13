//! Prepared target DATA from an actual Registered point and source frame.
//! Rotation, native publication, rows and Serving require their own producers.
use super::*;
use crate::db::NativeRegisteredNoDomainScope;
use crate::distribution_native_host::NativeRegisteredSelectedRangeNoDomain;
use crate::distribution_range_image::{canonical_range_image_catalog, NativeRemoteImageStep};

/// Non-deserializable prepared bytes, not an install fence or a row grant.
/// The retained host attempt must keep the actual original source/target
/// owners, and revalidate this candidate before unique freeze transfer/WAL.
pub(crate) struct NativeRegisteredRemoteImageIntent {
    intent: ImageInstallIntent,
    store_open: Uuid,
    database_incarnation: String,
    catalogue: Uuid,
    original_freeze_owner: Uuid,
    source_manifest_sha256: String,
}
impl NativeRegisteredRemoteImageIntent {
    pub(crate) fn range_id(&self) -> RangeId {
        self.intent.range.id
    }
    pub(crate) fn attempt_id(&self) -> Uuid {
        self.intent.attempt_id
    }
    pub(crate) fn original_freeze_owner(&self) -> Uuid {
        self.original_freeze_owner
    }
    pub(crate) fn sha256(&self) -> Result<String> {
        self.intent.sha256()
    }
    /// Readonly DATA recheck after the lexical NoDomain value has ended.
    /// The caller must independently validate its actual owned native/catalogue
    /// preflight under the unique original Runtime freeze. This method neither
    /// reconstructs NoDomain nor acquires Runtime/MStore/native-journal locks.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn require_prepared_constraints(
        &self,
        store: &RangeWriteStore,
        db: &crate::BicDb,
        native: &NativeRecoveryAdmission,
        bindings: &crate::db::RangeImageDbBindings,
        source: &NativeRemoteImageStep<'_>,
        role: &HandoffRoleAdmission,
        original_frozen: &FrozenRangeMutationPermit,
    ) -> Result<()> {
        source.require_request_live()?;
        native.require_current(db)?;
        self.intent.validate()?;
        require_intent_live_role(
            &self.intent,
            &NativeImageInstallRole::CommittedHandoff(role),
        )?;
        db.validate_range_image_bindings(bindings)?;
        db.range_mutation_gate()
            .validate_quiescent_frozen(self.range_id(), original_frozen)?;
        if store.open_id != self.store_open
            || self.database_incarnation != db.incarnation_id()
            || db.data_path().canonicalize()? != store.root.canonicalize()?
            || db.native_abort_catalogue_store_id()? != Some(self.catalogue)
            || original_frozen.owner() != self.original_freeze_owner
            || source.source().manifest().checksum_sha256 != self.source_manifest_sha256
            || source.source().manifest().target_attempt != self.attempt_id()
            || native.cluster_id() != &self.intent.cluster_id
            || native.bootstrap_id() != self.intent.bootstrap_id
            || native.local_node_id() != &self.intent.target_node_id
            || native.local_member_incarnation() != self.intent.target_member_incarnation
            || native.birth_id() != self.intent.target_birth_id
            || native.birth_sha256() != self.intent.target_birth_sha256
            || bindings.namespace_ids() != self.intent.target_namespace_ids
        {
            return Err(range_write_error(
                "Registered remote prepared constraint identities changed",
            ));
        }
        source.require_request_live()
    }
    /// Rebuild from the same actual point rather than accepting a stored DTO
    /// as renewed permission. This is valid only BEFORE native publication.
    pub(crate) fn require_preflight_current(
        &self,
        store: &RangeWriteStore,
        db: &crate::BicDb,
        target: &NativeRegisteredSelectedRangeNoDomain<'_>,
        source: &NativeRemoteImageStep<'_>,
        scope: &NativeRegisteredNoDomainScope<'_>,
    ) -> Result<()> {
        let current = store.prepare_registered_remote_image_intent(
            db,
            target,
            source,
            scope,
            self.attempt_id(),
        )?;
        if self.intent != current.intent
            || self.store_open != current.store_open
            || self.database_incarnation != current.database_incarnation
            || self.catalogue != current.catalogue
            || self.original_freeze_owner != current.original_freeze_owner
            || self.source_manifest_sha256 != current.source_manifest_sha256
        {
            return Err(range_write_error(
                "Registered remote prepared target/source point changed",
            ));
        }
        Ok(())
    }
}
impl RangeWriteStore {
    /// The actual target NoDomain and live authenticated frame are independent
    /// inputs. Preparing these exact bytes mutates neither catalogue nor WAL.
    pub(crate) fn prepare_registered_remote_image_intent(
        &self,
        db: &crate::BicDb,
        target: &NativeRegisteredSelectedRangeNoDomain<'_>,
        source: &NativeRemoteImageStep<'_>,
        scope: &NativeRegisteredNoDomainScope<'_>,
        attempt: Uuid,
    ) -> Result<NativeRegisteredRemoteImageIntent> {
        target.require_database_store(db, self, scope)?;
        source.require_request_live()?;
        let manifest = source.source().manifest();
        manifest.require_shape()?;
        let plan = target.plan()?;
        let role = target.role();
        let actual = role.role();
        let native = scope.native();
        plan.require_role(actual)?;
        let promise = plan.promise();
        let mut predecessor = plan.old_range().clone();
        predecessor
            .replicas
            .retain(|r| r.node_id != manifest.target_id);
        predecessor.epoch = predecessor
            .epoch
            .checked_sub(1)
            .ok_or_else(|| range_write_error("Registered remote predecessor underflow"))?;
        let source_purpose = RangeImageSourcePurposeV1::CommittedHandoff {
            relocation_id: promise.relocation_id,
            plan_id: promise.plan_id.clone(),
        };
        let install_role = NativeImageInstallRole::CommittedHandoff(role);
        install_role.require_scope(&source_purpose, promise.session_id, promise.nonce)?;
        let (intended_range, intended_members) = install_role.intended_configuration()?;
        let binding = db.capture_range_image_bindings()?;
        let catalogue = db
            .native_abort_catalogue_store_id()?
            .ok_or_else(|| range_write_error("Registered remote protected catalogue absent"))?;
        let portable = canonical_range_image_catalog(
            &manifest.cluster_id,
            &manifest.capture_range,
            manifest.range_index,
            &binding,
        )?;
        if attempt.is_nil()
            || attempt != manifest.target_attempt
            || attempt == promise.session_id
            || target.original_freeze_owner() != promise.session_id
            || db.data_path().canonicalize()? != self.root.canonicalize()?
            || self.catalog.cluster_id != *native.cluster_id()
            || self.catalog.node_id != *native.local_node_id()
            || manifest.cluster_id != *actual.cluster_id()
            || manifest.bootstrap_id != actual.bootstrap_id()
            || manifest.target_id != *native.local_node_id()
            || manifest.target_birth
                != crate::distribution::NativeStoreBirthV1::from_actual_native(native)?
            || manifest.source_birth != *plan.enrolled_birth(&manifest.source_id)?
            || !plan.old_members().contains_key(&manifest.source_id)
            || manifest.handoff_session != promise.session_id
            || manifest.nonce != promise.nonce
            || manifest.relocation_id != promise.relocation_id
            || manifest.plan_id != promise.plan_id
            || !semantic_range(&manifest.capture_range, plan.old_range())
            || !semantic_range(&manifest.native_predecessor, &predecessor)
            || !semantic_range(&manifest.activation_range, &intended_range)
            || manifest.old_members != *plan.old_members()
            || manifest.new_members != intended_members
            || manifest.old_config_sha256 != promise.old_config_sha256
            || manifest.new_config_sha256 != promise.new_config_sha256
            || manifest.schema_sha256 != binding.schema_sha256()
            || manifest.readers != binding.reader_features()
            || manifest.collections != binding.collections()
            || manifest.restore.as_ref() != actual.restore_activation()
            || manifest.restore.as_ref() != native.restore_activation()
            || hex::encode(Sha256::digest(&portable)) != manifest.portable_catalogue_sha256
        {
            return Err(range_write_error(
                "Registered remote intent differs from actual target/source/handoff point",
            ));
        }
        let intent = ImageInstallIntent {
            cluster_id: manifest.cluster_id.clone(),
            bootstrap_id: manifest.bootstrap_id,
            range: canonical_range(plan.old_range()),
            purpose: ImageInstallPurpose::RelocationLearner,
            attempt_id: attempt,
            session_id: promise.session_id,
            nonce: promise.nonce,
            source_purpose,
            target_node_id: native.local_node_id().clone(),
            target_member_incarnation: native.local_member_incarnation(),
            target_birth_id: native.birth_id(),
            target_birth_sha256: native.birth_sha256().to_string(),
            target_namespace_ids: binding.namespace_ids(),
            source_id: manifest.source_id.clone(),
            source_incarnation: manifest.source_birth.node_incarnation(),
            source_range_index: manifest.range_index,
            source_native_receipt_sha256: manifest.source_native_receipt.clone(),
            source_catalog_sha256: manifest.portable_catalogue_sha256.clone(),
            old_config_sha256: promise.old_config_sha256.clone(),
            intended_config_sha256: promise.new_config_sha256.clone(),
            activation_epoch: intended_range.epoch,
            intended_range: canonical_range(&intended_range),
            intended_members,
            current_voter_credentials: actual
                .member_credentials()
                .iter()
                .map(|(id, identity)| (id.clone(), credentials(identity)))
                .collect(),
            target_credentials: credentials(actual.local_member_identity()),
            restore_activation: actual.restore_activation().cloned(),
            schema_sha256: binding.schema_sha256().to_string(),
            protected_proposal: None,
        };
        intent.validate()?;
        target.require_database_store(db, self, scope)?;
        db.validate_range_image_bindings(&binding)?;
        source.require_request_live()?;
        Ok(NativeRegisteredRemoteImageIntent {
            intent,
            store_open: self.open_id,
            database_incarnation: db.incarnation_id().to_string(),
            catalogue,
            original_freeze_owner: target.original_freeze_owner(),
            source_manifest_sha256: manifest.checksum_sha256.clone(),
        })
    }
}
