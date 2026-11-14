//! Prepared DATA from an actual existing-Joined preflight and an actual
//! authenticated source owner/frame. Never a FIRST scope or install grant.
use super::*;
use crate::db::NativeJoinedReceiveStorageScope;
use crate::distribution_native_host::{
    NativeJoinedRangeReceivePreflight, NativeJoinedRangeReceiveRequest,
};
use crate::distribution_range_image::NativeRemoteImageFact;

pub(crate) struct NativeJoinedRemoteImageIntent {
    inner: NativeRegisteredRemoteImageIntent,
    source: Arc<NativeRemoteImageFact>,
}
impl NativeJoinedRemoteImageIntent {
    pub(crate) fn range_id(&self) -> RangeId {
        self.inner.range_id()
    }
    pub(crate) fn attempt_id(&self) -> Uuid {
        self.inner.attempt_id()
    }
    pub(crate) fn original_freeze_owner(&self) -> Uuid {
        self.inner.original_freeze_owner()
    }
    pub(crate) fn sha256(&self) -> Result<String> {
        self.inner.sha256()
    }
    pub(crate) fn source(&self) -> &Arc<NativeRemoteImageFact> {
        &self.source
    }
    pub(crate) fn canonical_intent_bytes(&self) -> Result<Vec<u8>> {
        self.inner.canonical_intent_bytes()
    }
    pub(crate) fn require_current(
        &self,
        store: &RangeWriteStore,
        db: &crate::BicDb,
        target: &NativeJoinedRangeReceivePreflight,
        request: &NativeJoinedRangeReceiveRequest,
        frame: &NativeRemoteImageStep<'_>,
        scope: &NativeJoinedReceiveStorageScope<'_>,
    ) -> Result<()> {
        target.require_at_storage_point(db, store, request, scope)?;
        frame.require_request_live()?;
        if !std::ptr::eq(self.source.as_ref(), frame.source()) {
            return Err(range_write_error(
                "Joined prepared image changed actual MF owner",
            ));
        }
        self.inner.require_prepared_constraints(
            store,
            db,
            target.native(),
            target.bindings(),
            frame,
            request.role(),
            scope.frozen(),
        )?;
        target.require_at_storage_point(db, store, request, scope)
    }
}
impl RangeWriteStore {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn prepare_joined_remote_image_intent(
        &self,
        db: &crate::BicDb,
        target: &NativeJoinedRangeReceivePreflight,
        request: &NativeJoinedRangeReceiveRequest,
        actual_source: Arc<NativeRemoteImageFact>,
        source: &NativeRemoteImageStep<'_>,
        scope: &NativeJoinedReceiveStorageScope<'_>,
        attempt: Uuid,
    ) -> Result<NativeJoinedRemoteImageIntent> {
        target.require_at_storage_point(db, self, request, scope)?;
        if !std::ptr::eq(actual_source.as_ref(), source.source()) {
            return Err(range_write_error(
                "Joined intent requires exact authenticated retained MF owner",
            ));
        }
        source.require_request_live()?;
        let manifest = source.source().manifest();
        manifest.require_shape()?;
        let plan = request.plan()?;
        let role = request.role();
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
            || target.scope().freeze_owner()? != promise.session_id
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
            registered_proposal: None,
        };
        intent.validate()?;
        target.require_at_storage_point(db, self, request, scope)?;
        db.validate_range_image_bindings(&binding)?;
        source.require_request_live()?;
        let inner = NativeRegisteredRemoteImageIntent {
            intent,
            store_open: self.open_id,
            database_incarnation: db.incarnation_id().to_string(),
            catalogue,
            original_freeze_owner: target.scope().freeze_owner()?,
            source_manifest_sha256: manifest.checksum_sha256.clone(),
        };
        Ok(NativeJoinedRemoteImageIntent {
            inner,
            source: actual_source,
        })
    }
}
