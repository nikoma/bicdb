//! Actual SAME-service activation continuation facts, never a Ready/head vote.
//! Released members may legally advance; no application stamp spans a phase.
use super::*;
use crate::distribution_consensus::HostMetadataReadObservation;
use crate::distribution_range_consensus::{
    NativeJointCatalogueActivationFact, NativeJointReleaseObservedObligations,
};

#[derive(Clone, Copy)]
pub(crate) enum NativeJointReleaseOwnerView<'a> {
    Frozen(&'a FrozenRangeMutationPermit),
    Dropped(&'a crate::distribution_range_consensus::NativeJointCatalogueActivationReceipt),
}
pub(crate) struct ActualServiceNativeJointReleaseEvidence {
    committed: CommittedNativeJointDecisionObservation,
    native: Arc<crate::db::NativeJointServingBridgeReceipt>,
    activation: NativeJointCatalogueActivationFact,
    writer: crate::db::NativeJointPhaseOwner,
    writer_domain: crate::db::NativeJointWriterDomain,
    root: std::path::PathBuf,
    frozen_owner: Option<Uuid>,
    continuity: crate::db::NativeJointOpenContinuity,
    obligations: NativeJointReleaseObservedObligations,
}
fn require_original_dropped_binding(
    original: &crate::distribution_range_consensus::NativeJointCatalogueActivationReceipt,
    fact: &NativeJointCatalogueActivationFact,
    native: &crate::db::NativeJointServingBridgeReceipt,
) -> Result<()> {
    // Original receipt is historical binding ONLY; no expired lease is used.
    if original.store_open_id() != fact.store_open_id()
        || original.activation_sha256()? != fact.activation_sha256()?
        || original.native_bridge_sha256() != native.checksum_sha256()
        || original.native().native_database_incarnation() != native.native_database_incarnation()
    {
        return Err(data_error(
            "joint released owner changed actual original activation/store/open",
        ));
    }
    Ok(())
}
impl ActualServiceNativeJointReleaseEvidence {
    /// Actual post-drop AND armed same-open generation. A Frozen or merely
    /// dropped/unarmed member cannot count toward a Serving-policy quorum.
    pub(crate) fn require_released_open(&self) -> Result<Uuid> {
        self.require_live()?;
        if self.frozen_owner.is_some() {
            return Err(data_error(
                "Serving writer policy still has an actual Frozen owner",
            ));
        }
        self.continuity.released_generation()
    }
    pub(crate) fn writer_domain_audit(&self) -> Result<serde_json::Value> {
        let generation = self.require_released_open()?;
        Ok(serde_json::json!({
            "metadata": self.committed.writer_domain_metadata_audit()?,
            "native_bridge_sha256": self.native.checksum_sha256(),
            "catalogue_activation_sha256": self.activation.activation_sha256()?,
            "released_generation": generation,
            "local_node_id": self.native.local_node_id(),
        }))
    }
    pub(crate) fn actual_released_publication_point(
        &self,
    ) -> Result<(Arc<crate::db::NativeJointActivatedOpenGuard>, Uuid, String)> {
        self.require_released_open()?;
        self.continuity.actual_released_publication_point()
    }
    pub(crate) fn observe_released_continuity(
        &self,
        store: &crate::distribution_range_consensus::RangeWriteStore,
    ) -> Result<crate::db::NativeJointOpenContinuity> {
        let expected = self.require_released_open()?;
        let actual = store.observe_native_joint_open_continuity(&self.activation)?;
        if actual.released_generation()? != expected {
            return Err(data_error(
                "Serving writer policy replaced actual released generation",
            ));
        }
        Ok(actual)
    }
    pub(crate) fn require_original_writer_identity(
        &self,
        original: &crate::db::NativeJointPhaseOwner,
    ) -> Result<()> {
        self.require_live()?;
        if !original.same_actual(&self.writer) {
            return Err(data_error(
                "joint release cannot replace preparation's original actual writer",
            ));
        }
        Ok(())
    }
    pub(crate) fn committed(&self) -> &CommittedNativeJointDecisionObservation {
        &self.committed
    }
    pub(crate) fn activation(&self) -> &NativeJointCatalogueActivationFact {
        &self.activation
    }
    pub(crate) fn native(&self) -> &Arc<crate::db::NativeJointServingBridgeReceipt> {
        &self.native
    }
    pub(crate) fn require_live(&self) -> Result<()> {
        self.committed.require_current_metadata_only()?;
        self.continuity.require_observed_lifetime()?;
        self.writer
            .require_store(&self.root, self.activation.store_open_id())?;
        self.writer_domain.require_lifetime(
            &self.root,
            self.activation.store_open_id(),
            self.native.data().activation_range(),
        )
    }
    pub(crate) fn require_current(
        &self,
        service: &ClusterDataNodeService,
        owner: NativeJointReleaseOwnerView<'_>,
    ) -> Result<()> {
        self.require_live()?;
        let database = checked_database_read(&service.db, &service.storage_policy)?;
        service.require_native_catalog_database(&database)?;
        self.writer_domain.require_current(
            &database,
            &service.range_writes,
            self.native.data().activation_range(),
        )?;
        let scope = self.native.historical_cut_scope(&database)?;
        self.require_current_with_scope(service, &scope, owner)
    }
    pub(crate) fn require_current_with_scope(
        &self,
        service: &ClusterDataNodeService,
        scope: &crate::db::NativeJointHistoricalCutScope<'_>,
        owner: NativeJointReleaseOwnerView<'_>,
    ) -> Result<()> {
        if self.native.local_node_id() != &service.node_id {
            return Err(data_error(
                "joint release scope belongs to another actual service member",
            ));
        }
        self.require_current_with_store_scope(&service.range_writes, scope, owner)
    }
    pub(crate) fn require_current_with_store_scope(
        &self,
        store: &crate::distribution_range_consensus::RangeWriteStore,
        scope: &crate::db::NativeJointHistoricalCutScope<'_>,
        owner: NativeJointReleaseOwnerView<'_>,
    ) -> Result<()> {
        self.require_live()?;
        scope.require_native(&self.native)?;
        let database = scope.database();
        self.committed
            .require_current_with_historical_cut_scope(scope)?;
        self.activation
            .require_current_with_historical_cut_scope(scope, store)?;
        let writer = self.writer_domain.phase_owner();
        self.writer_domain.require_current(
            database,
            store,
            self.native.data().activation_range(),
        )?;
        if !writer.same_actual(&self.writer) {
            return Err(data_error(
                "joint release current writer registration/open/member changed",
            ));
        }
        match owner {
            NativeJointReleaseOwnerView::Frozen(frozen) => {
                if self.frozen_owner != Some(frozen.owner()) {
                    return Err(data_error(
                        "joint release cannot refreeze/reassign an ended owner",
                    ));
                }
                store.require_frozen_joint_release_complete(scope, frozen)?;
                store.observe_owned_frozen_joint_generation(&self.activation)?;
            }
            NativeJointReleaseOwnerView::Dropped(original) => {
                require_original_dropped_binding(original, &self.activation, &self.native)?;
                // Actual armed guard OR actual dropped/unarmed drain, never a
                // numeric absence or resurrection of the old Frozen.
                store.observe_native_joint_open_continuity(&self.activation)?;
            }
        }
        store.require_native_joint_open_continuity(&self.activation, &self.continuity)?;
        self.obligations.require_preserved_current(store, scope)?;
        self.committed
            .require_current_with_historical_cut_scope(scope)?;
        self.require_live()
    }
}
impl ClusterDataNodeService {
    pub(crate) fn observe_actual_joint_release_member(
        &self,
        host: HostMetadataReadObservation,
        business: Uuid,
        native: Arc<crate::db::NativeJointServingBridgeReceipt>,
        owner: NativeJointReleaseOwnerView<'_>,
    ) -> Result<ActualServiceNativeJointReleaseEvidence> {
        let writer = self.range_writes.native_writer_host_state()?;
        self.observe_joint_release_member_with_domain(
            &crate::db::NativeJointWriterDomain::Installed(writer),
            host,
            business,
            native,
            owner,
        )
    }
    pub(crate) fn observe_joint_release_member_with_domain(
        &self,
        writer_domain: &crate::db::NativeJointWriterDomain,
        host: HostMetadataReadObservation,
        business: Uuid,
        native: Arc<crate::db::NativeJointServingBridgeReceipt>,
        owner: NativeJointReleaseOwnerView<'_>,
    ) -> Result<ActualServiceNativeJointReleaseEvidence> {
        self.observe_joint_release_member_with_domain_and_point(
            writer_domain,
            host,
            business,
            native,
            owner,
            None,
        )
    }
    pub(crate) fn observe_joint_release_member_with_domain_and_point(
        &self,
        writer_domain: &crate::db::NativeJointWriterDomain,
        host: HostMetadataReadObservation,
        business: Uuid,
        native: Arc<crate::db::NativeJointServingBridgeReceipt>,
        owner: NativeJointReleaseOwnerView<'_>,
        point: Option<Arc<crate::distribution_data::ActualNativeJointMetadataPoint>>,
    ) -> Result<ActualServiceNativeJointReleaseEvidence> {
        let committed = self.observe_joint_decision_with_writer_domain_and_point(
            writer_domain,
            host,
            business,
            native.data().old_range().id,
            point,
        )?;
        let database = checked_database_read(&self.db, &self.storage_policy)?;
        self.require_native_catalog_database(&database)?;
        writer_domain.require_current(
            &database,
            &self.range_writes,
            native.data().activation_range(),
        )?;
        let scope = native.historical_cut_scope(&database)?;
        committed.require_current_with_historical_cut_scope(&scope)?;
        let activation = self
            .range_writes
            .observe_native_joint_activation_fact_at_historical_cut(&scope, native.clone())?;
        let writer = writer_domain.phase_owner();
        writer_domain.require_current(
            &database,
            &self.range_writes,
            native.data().activation_range(),
        )?;
        let (frozen_owner, continuity) = match owner {
            NativeJointReleaseOwnerView::Frozen(frozen) => {
                self.range_writes
                    .require_frozen_joint_release_complete(&scope, frozen)?;
                (
                    Some(frozen.owner()),
                    self.range_writes
                        .observe_owned_frozen_joint_generation(&activation)?,
                )
            }
            NativeJointReleaseOwnerView::Dropped(original) => {
                require_original_dropped_binding(original, &activation, &native)?;
                (
                    None,
                    self.range_writes
                        .observe_native_joint_open_continuity(&activation)?,
                )
            }
        };
        let obligations = self
            .range_writes
            .observe_joint_release_obligations(&scope)?;
        self.range_writes
            .require_native_joint_open_continuity(&activation, &continuity)?;
        committed.require_current_with_historical_cut_scope(&scope)?;
        let root = database.data_path().canonicalize()?;
        drop(scope);
        drop(database);
        Ok(ActualServiceNativeJointReleaseEvidence {
            committed,
            native,
            activation,
            writer,
            writer_domain: writer_domain.clone(),
            root,
            frozen_owner,
            continuity,
            obligations,
        })
    }
}

impl ClusterDataNodeService {
    pub(crate) fn require_joint_release_metadata_point(
        &self,
        committed: &CommittedNativeJointDecisionObservation,
        db: &BicDb,
    ) -> Result<()> {
        committed.require_native_publication_metadata(db, &self.range_writes)
    }
    pub(crate) fn require_fresh_native_joint_release(
        &self,
        admission: &crate::distribution_range_handoff::NativeJointReleaseAdmission,
        owner: NativeJointReleaseOwnerView<'_>,
    ) -> Result<()> {
        let database = checked_database_read(&self.db, &self.storage_policy)?;
        self.require_native_catalog_database(&database)?;
        admission.writer_domain().require_current(
            &database,
            &self.range_writes,
            admission.native().data().activation_range(),
        )?;
        let scope = admission.native().historical_cut_scope(&database)?;
        admission.require_local_with_scope(self, &scope, owner)
    }
    pub(crate) fn arm_fresh_native_joint_release(
        &self,
        admission: &crate::distribution_range_handoff::NativeJointReleaseAdmission,
        original: &crate::distribution_range_consensus::NativeJointCatalogueActivationReceipt,
    ) -> Result<()> {
        let database = checked_database_read(&self.db, &self.storage_policy)?;
        self.require_native_catalog_database(&database)?;
        admission.writer_domain().require_current(
            &database,
            &self.range_writes,
            admission.native().data().activation_range(),
        )?;
        let scope = admission.native().historical_cut_scope(&database)?;
        self.range_writes
            .arm_native_joint_from_fresh_release(&scope, admission, original)
    }
}
