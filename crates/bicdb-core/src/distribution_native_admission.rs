//! Actual factory enrollment for a NEW cluster or an unassigned learner.
//! Serialized birth bindings are metadata facts, never read/serving permits.
use crate::db::FreshRangeGenesisAuthority;
use crate::distribution::{
    ClusterNodeId, ClusterTopology, MetadataMemberRole, NativeStoreBirthV1, RangeReplicaRole,
};
use crate::{BicDb, BicDbError, Result};

fn invalid(detail: &str) -> BicDbError {
    BicDbError::Cluster(format!("native voter enrollment: {detail}"))
}

/// Single-use actual factory evidence. No deserialization or raw-parts path.
/// A caller must supply the actual still-unwritten factory and its exact
/// initial topology. This can build a proposal; only metadata consensus can
/// publish it. It supplies no data quorum, read floor or voter promotion.
#[derive(Debug)]
pub(crate) struct ActualNativeStoreEnrollment {
    node: ClusterNodeId,
    binding: NativeStoreBirthV1,
    cluster: crate::distribution::ClusterId,
}
impl ActualNativeStoreEnrollment {
    pub(crate) fn from_actual_factory(
        factory: &FreshRangeGenesisAuthority,
        database: &BicDb,
        initial: &ClusterTopology,
    ) -> Result<Self> {
        let actual = factory.require_unwritten_enrollment_database(database, initial)?;
        Ok(Self {
            node: actual.local_node_id().clone(),
            cluster: actual.cluster_id().clone(),
            binding: NativeStoreBirthV1::from_actual_native(&actual)?,
        })
    }

    /// Initial CONFIGURATION builder, not fresh-store authority. Generation
    /// one is only an initial-plan shape: an existing metadata consensus store
    /// MUST reject an unbound voter upgrade even at generation one. Initial
    /// voter bindings must precede actual fresh metadata-store creation.
    /// A live learner proposal must subsequently advance its generation and
    /// pass the ordinary metadata consensus publication path.
    pub(crate) fn bind_initial_or_unassigned_learner(
        self,
        topology: &mut ClusterTopology,
    ) -> Result<()> {
        topology.validate()?;
        if topology.cluster_id != self.cluster
            || topology.range_image_bootstrap_id != Some(self.binding.bootstrap_id())
        {
            return Err(invalid(
                "initial cluster/bootstrap differs from actual factory",
            ));
        }
        let node = topology
            .nodes
            .get(&self.node)
            .ok_or_else(|| invalid("factory node absent from initial topology"))?;
        self.binding
            .validate_for(topology.range_image_bootstrap_id, node.incarnation)?;
        if let Some(existing) = &node.native_store_birth_v1 {
            return if existing == &self.binding {
                Ok(())
            } else {
                Err(invalid(
                    "an enrolled birth cannot be replaced under the same incarnation",
                ))
            };
        }
        let unassigned_learner = node.metadata_role == MetadataMemberRole::Learner
            && !topology.ranges.values().any(|range| {
                range.replicas.iter().any(|replica| {
                    replica.node_id == self.node && replica.role == RangeReplicaRole::Voter
                })
            });
        if topology.generation != 1 && !unassigned_learner {
            return Err(invalid(
                "existing unbound voter cannot be enrolled from a newly created disk",
            ));
        }
        let mut proposed = topology.clone();
        proposed.format_version = 2;
        proposed
            .nodes
            .get_mut(&self.node)
            .unwrap()
            .native_store_birth_v1 = Some(self.binding);
        proposed.validate()?;
        *topology = proposed;
        Ok(())
    }
}

#[cfg(test)]
mod tests;
