//! Same-service nonmutating native observation. No MQ, quorum, mutation permit
//! or recovery session is acquired here. Caller scope remains untrusted.
use super::*;
use crate::distribution_native_read::{LocalNativeRangeReadObservation, NativeRangeReadRequest};
impl ClusterDataNodeService {
    pub(crate) fn observe_current_native_range(
        &self,
        request: &NativeRangeReadRequest,
        parent: &CancellationToken,
    ) -> Result<LocalNativeRangeReadObservation> {
        let cancellation = request.cancellation(parent)?;
        let database = try_database_read(&self.db)?;
        database.schema_compatibility_fingerprint()?;
        let lineage = database.current_native_range_source_lineage(request.scope.range.id)?;
        if lineage.local_node_id() != &self.node_id || lineage.cluster_id() != &self.cluster_id {
            return Err(data_error(
                "native read source belongs to another actual service",
            ));
        }
        let catalogue = self.range_writes.observe_native_read_catalogue(
            &request.scope,
            lineage.range_index(),
            database.data_path(),
        )?;
        let physical = database.observe_native_range_read_physical(
            &request.scope,
            &catalogue,
            &cancellation,
        )?;
        catalogue.require_current(
            &self.range_writes,
            &request.scope,
            physical.range_index(),
            database.data_path(),
        )?;
        cancellation.check()?;
        LocalNativeRangeReadObservation::from_actual_service(
            request,
            physical,
            catalogue,
            cancellation,
        )
    }
    pub(crate) fn revalidate_current_native_range(
        &self,
        observation: &LocalNativeRangeReadObservation,
    ) -> Result<()> {
        observation.require_lifetime()?;
        let database = try_database_read(&self.db)?;
        let scope = observation.scope();
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
        )?;
        observation
            .physical()
            .require_current(&database, observation.catalogue())?;
        observation.require_lifetime()
    }
}
