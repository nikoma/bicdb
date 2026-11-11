//! Same-service nonmutating native observation. No MQ, quorum, mutation permit
//! or recovery session is acquired here. Caller scope remains untrusted.
use super::*;
use crate::distribution_native_read::{LocalNativeRangeReadObservation, NativeRangeReadRequest};
/// Only this SAME-service producer can construct the owned evidence bundle.
/// Callers cannot retag separately retained physical/catalogue tokens.
#[derive(Debug)]
pub(crate) struct ActualServiceNativeReadEvidence {
    request: NativeRangeReadRequest,
    physical: crate::db::NativeRangeReadPhysical,
    catalogue: crate::distribution_range_consensus::NativeRangeReadCatalogue,
    cancellation: CancellationToken,
}
impl ActualServiceNativeReadEvidence {
    pub(crate) fn into_parts(
        self,
    ) -> (
        NativeRangeReadRequest,
        crate::db::NativeRangeReadPhysical,
        crate::distribution_range_consensus::NativeRangeReadCatalogue,
        CancellationToken,
    ) {
        (
            self.request,
            self.physical,
            self.catalogue,
            self.cancellation,
        )
    }
}
impl ClusterDataNodeService {
    pub(crate) fn require_native_read_ancestor(
        &self,
        local: &LocalNativeRangeReadObservation,
        peer: &crate::distribution_native_read::NativeRangeReadFacts,
    ) -> Result<()> {
        local.require_current(self)?;
        {
            let database = try_database_read(&self.db)?;
            self.require_native_catalog_database(&database)?;
            local.physical().require_ancestor(
                &database,
                peer.range_index,
                &peer.logical_genesis_sha256,
                &peer.logical_history_sha256,
            )?;
        }
        local.require_current(self)
    }
    pub(crate) fn observe_current_native_range(
        &self,
        request: &NativeRangeReadRequest,
        parent: &CancellationToken,
    ) -> Result<LocalNativeRangeReadObservation> {
        let cancellation = request.cancellation(parent)?;
        let database = try_database_read(&self.db)?;
        self.require_native_catalog_database(&database)?;
        database.schema_compatibility_fingerprint()?;
        let lineage = database.current_native_range_source_lineage(request.scope.range.id)?;
        if lineage.local_node_id() != &self.node_id || lineage.cluster_id() != &self.cluster_id {
            return Err(data_error(
                "native read source belongs to another actual service",
            ));
        }
        let native = database.native_recovery_admission(
            &self.cluster_id,
            request.scope.bootstrap_id,
            &self.node_id,
            lineage.local_member_incarnation(),
        )?;
        let catalogue = self.range_writes.observe_native_read_catalogue(
            &request.scope,
            lineage.range_index(),
            database.data_path(),
            &native,
        )?;
        let physical = database.observe_native_range_read_physical(
            &request.scope,
            &catalogue,
            native,
            &cancellation,
        )?;
        catalogue.require_current(
            &self.range_writes,
            &request.scope,
            physical.range_index(),
            database.data_path(),
            physical.native_admission(),
        )?;
        cancellation.check()?;
        LocalNativeRangeReadObservation::from_actual_service(ActualServiceNativeReadEvidence {
            request: request.clone(),
            physical,
            catalogue,
            cancellation,
        })
    }
    pub(crate) fn revalidate_current_native_range(
        &self,
        observation: &LocalNativeRangeReadObservation,
    ) -> Result<()> {
        observation.require_lifetime()?;
        let database = try_database_read(&self.db)?;
        self.require_native_catalog_database(&database)?;
        let scope = observation.scope();
        observation
            .physical()
            .native_admission()
            .require_current(&database)?;
        if self.cluster_id != scope.cluster_id || self.node_id != observation.facts().node_id {
            return Err(data_error(
                "native read observation belongs to another service",
            ));
        }
        observation.catalogue().require_current(
            &self.range_writes,
            scope,
            observation.facts().range_index,
            database.data_path(),
            observation.physical().native_admission(),
        )?;
        observation
            .physical()
            .require_current(&database, observation.catalogue())?;
        observation.require_lifetime()
    }
}
