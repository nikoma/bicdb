//! Same-service nonmutating native observation. No MQ, quorum, mutation permit
//! or recovery session is acquired here. Caller scope remains untrusted.
use super::*;
use crate::distribution_native_read::{LocalNativeRangeReadObservation, NativeRangeReadRequest};
/// A request-bound single-use bundle. Only the actual SAME-service RPC
/// producer can create it; retained local tokens cannot be relabeled under a
/// different business/MQ context by the observation wrapper.
#[derive(Debug)]
pub(crate) struct ActualServiceNativeReadProbeEvidence {
    request: crate::distribution_native_read::NativeRangeReadProbeRequest,
    local: LocalNativeRangeReadObservation,
    cancellation: CancellationToken,
}
impl ActualServiceNativeReadProbeEvidence {
    pub(crate) fn into_parts(
        self,
    ) -> (
        crate::distribution_native_read::NativeRangeReadProbeRequest,
        LocalNativeRangeReadObservation,
        CancellationToken,
    ) {
        (self.request, self.local, self.cancellation)
    }
}
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
    pub(crate) fn observe_current_native_range_probe(
        &self,
        request: &crate::distribution_native_read::NativeRangeReadProbeRequest,
        parent: &CancellationToken,
    ) -> Result<ActualServiceNativeReadProbeEvidence> {
        let cancellation = request.cancellation(parent)?;

        // STRONG_TRACE_BEGIN
        #[cfg(test)]
        let _probe_service =
            crate::distribution_native_read::trace::Span::current("read-probe-service");
        // STRONG_TRACE_END
        let local = self.observe_current_native_range(request, &cancellation)?;
        cancellation.check()?;
        Ok(ActualServiceNativeReadProbeEvidence {
            request: request.clone(),
            local,
            cancellation,
        })
    }
    pub(crate) fn require_native_read_ancestor(
        &self,
        local: &LocalNativeRangeReadObservation,
        peer: &crate::distribution_native_read::NativeRangeReadFacts,
    ) -> Result<()> {
        local.require_current(self)?;
        {
            let database = checked_database_read(&self.db, &self.storage_policy)?;
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
        if self.native_writer_enforcement_version()? == 1 {
            let database = checked_database_read(&self.db, &self.storage_policy)?;
            self.require_native_catalog_database(&database)?;
            let slot = self.range_writes.guard();
            let (physical, catalogue) = database.observe_enforced_native_range_read(
                &self.range_writes,
                &slot,
                &request.scope,
                &cancellation,
            )?;
            physical.require_service_identity(&self.cluster_id, &self.node_id)?;
            cancellation.check()?;
            return LocalNativeRangeReadObservation::from_actual_service(
                ActualServiceNativeReadEvidence {
                    request: request.clone(),
                    physical,
                    catalogue,
                    cancellation,
                },
            );
        }

        // STRONG_TRACE_BEGIN
        #[cfg(test)]
        let read_db_acquire =
            crate::distribution_native_read::trace::Span::current("read-db-acquire");
        // STRONG_TRACE_END
        let database = checked_database_read(&self.db, &self.storage_policy)?;

        // STRONG_TRACE_BEGIN
        #[cfg(test)]
        drop(read_db_acquire);
        // STRONG_TRACE_END

        // STRONG_TRACE_BEGIN
        #[cfg(test)]
        let read_catalog_writer_admission =
            crate::distribution_native_read::trace::Span::current("read-catalog-writer-admission");
        // STRONG_TRACE_END
        self.require_native_catalog_database(&database)?;
        self.require_native_writer_host_for_range(&database, &request.scope.range)?;

        // STRONG_TRACE_BEGIN
        #[cfg(test)]
        drop(read_catalog_writer_admission);
        // STRONG_TRACE_END

        // STRONG_TRACE_BEGIN
        #[cfg(test)]
        let read_schema = crate::distribution_native_read::trace::Span::current("read-schema");
        // STRONG_TRACE_END
        database.schema_compatibility_fingerprint()?;

        // STRONG_TRACE_BEGIN
        #[cfg(test)]
        drop(read_schema);
        // STRONG_TRACE_END

        // STRONG_TRACE_BEGIN
        #[cfg(test)]
        let read_current_lineage =
            crate::distribution_native_read::trace::Span::current("read-current-lineage");
        // STRONG_TRACE_END
        let lineage = database.current_native_range_source_lineage(request.scope.range.id)?;

        // STRONG_TRACE_BEGIN
        #[cfg(test)]
        drop(read_current_lineage);
        // STRONG_TRACE_END
        if lineage.local_node_id() != &self.node_id || lineage.cluster_id() != &self.cluster_id {
            return Err(data_error(
                "native read source belongs to another actual service",
            ));
        }

        // STRONG_TRACE_BEGIN
        #[cfg(test)]
        let read_native_admission =
            crate::distribution_native_read::trace::Span::current("read-native-admission");
        // STRONG_TRACE_END
        let native = database.native_recovery_admission(
            &self.cluster_id,
            request.scope.bootstrap_id,
            &self.node_id,
            lineage.local_member_incarnation(),
        )?;

        // STRONG_TRACE_BEGIN
        #[cfg(test)]
        drop(read_native_admission);
        // STRONG_TRACE_END

        // STRONG_TRACE_BEGIN
        #[cfg(test)]
        let read_abort_controls =
            crate::distribution_native_read::trace::Span::current("read-abort-controls");
        // STRONG_TRACE_END
        self.range_writes.require_actual_abort_controls_covered(
            &database,
            request.scope.range.id,
            lineage.range_index(),
        )?;

        // STRONG_TRACE_BEGIN
        #[cfg(test)]
        drop(read_abort_controls);
        // STRONG_TRACE_END

        // STRONG_TRACE_BEGIN
        #[cfg(test)]
        let read_catalog_observation =
            crate::distribution_native_read::trace::Span::current("read-catalog-observation");
        // STRONG_TRACE_END
        let catalogue = self.range_writes.observe_native_read_catalogue(
            &request.scope,
            lineage.range_index(),
            database.data_path(),
            &native,
        )?;

        // STRONG_TRACE_BEGIN
        #[cfg(test)]
        drop(read_catalog_observation);
        // STRONG_TRACE_END

        // STRONG_TRACE_BEGIN
        #[cfg(test)]
        let read_physical = crate::distribution_native_read::trace::Span::current("read-physical");
        // STRONG_TRACE_END
        let physical = database.observe_native_range_read_physical(
            &request.scope,
            &catalogue,
            native,
            &cancellation,
        )?;

        // STRONG_TRACE_BEGIN
        #[cfg(test)]
        drop(read_physical);
        // STRONG_TRACE_END

        // STRONG_TRACE_BEGIN
        #[cfg(test)]
        let read_catalog_postcurrent =
            crate::distribution_native_read::trace::Span::current("read-catalog-postcurrent");
        // STRONG_TRACE_END
        catalogue.require_current(
            &self.range_writes,
            &request.scope,
            physical.range_index(),
            database.data_path(),
            physical.native_admission(),
        )?;

        // STRONG_TRACE_BEGIN
        #[cfg(test)]
        drop(read_catalog_postcurrent);
        // STRONG_TRACE_END
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
        // STRONG_TRACE_BEGIN
        #[cfg(test)]
        let _revalidate =
            crate::distribution_native_read::trace::Span::current("read-local-revalidate");
        // STRONG_TRACE_END
        observation.require_lifetime()?;
        if self.native_writer_enforcement_version()? == 1 {
            return self.with_native_serving_read_observation(
                observation,
                observation.cancellation(),
                |_| Ok(()),
            );
        }
        let database = checked_database_read(&self.db, &self.storage_policy)?;
        self.require_native_catalog_database(&database)?;
        let scope = observation.scope();
        self.require_native_writer_host_for_range(&database, &scope.range)?;
        observation
            .physical()
            .native_admission()
            .require_current(&database)?;
        if self.cluster_id != scope.cluster_id || self.node_id != observation.facts().node_id {
            return Err(data_error(
                "native read observation belongs to another service",
            ));
        }
        self.range_writes.require_actual_abort_controls_covered(
            &database,
            scope.range.id,
            observation.facts().range_index,
        )?;
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
    pub(crate) fn with_native_serving_read_observation<R>(
        &self,
        observation: &LocalNativeRangeReadObservation,
        parent: &CancellationToken,
        action: impl for<'scope> FnOnce(&crate::db::NativeServingObservationScope<'scope>) -> Result<R>,
    ) -> Result<R> {
        parent.check()?;
        observation.require_lifetime()?;
        let database = checked_database_read(&self.db, &self.storage_policy)?;
        self.require_native_catalog_database(&database)?;
        observation
            .physical()
            .require_service_identity(&self.cluster_id, &self.node_id)?;
        let slot = self.range_writes.guard();
        let result = database.with_existing_native_serving_read_scope(
            observation.physical(),
            observation.catalogue(),
            &self.range_writes,
            &slot,
            parent,
            action,
        )?;
        observation.require_lifetime()?;
        parent.check()?;
        Ok(result)
    }
}
