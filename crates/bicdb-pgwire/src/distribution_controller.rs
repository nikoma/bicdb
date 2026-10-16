//! Default public placement policy, using the engine's bounded plan validator.
use bicdb_core::{
    build_failure_repair_plan, ClusterPlacementPlanner, ClusterSupervisor, ClusterSupervisorConfig,
};

use crate::{PgWireHostContext, PgWireHostService, Result};

#[derive(Debug)]
struct CapacityPlacement;

impl ClusterPlacementPlanner for CapacityPlacement {
    fn plan_failure_repair(
        &self,
        topology: &bicdb_core::ClusterTopology,
        config: &bicdb_core::DistributionConfig,
        options: &bicdb_core::RebalanceOptions,
        now_ms: u64,
    ) -> bicdb_core::Result<bicdb_core::FailureRepairPlan> {
        build_failure_repair_plan(topology, config, options, now_ms)
    }
}

/// Balance replica bytes, replica counts and leaders after membership changes.
/// Proposals use the same quorum, schema, resource and epoch checks as manual
/// operations. Embedders may retain manual operation or supply another planner.
#[derive(Debug, Clone)]
pub struct BalancedDistributionHostService {
    config: ClusterSupervisorConfig,
}

impl Default for BalancedDistributionHostService {
    fn default() -> Self {
        Self {
            config: ClusterSupervisorConfig {
                refresh_topology_each_tick: false,
                automatically_rebalance: true,
                retry_failed_relocations: true,
                tick_interval_ms: 250,
                max_relocation_steps_per_tick: 4,
                rebalance: bicdb_core::RebalanceOptions {
                    max_replica_moves: 4,
                    max_moves_per_node: 2,
                    ..Default::default()
                },
                ..Default::default()
            },
        }
    }
}

impl BalancedDistributionHostService {
    /// Configure bounded movement and retries. Metadata consensus owns refreshes.
    pub fn with_config(mut config: ClusterSupervisorConfig) -> Result<Self> {
        config.refresh_topology_each_tick = false;
        config.validate()?;
        Ok(Self { config })
    }

    pub(crate) fn supervisor(&self) -> bicdb_core::Result<ClusterSupervisor> {
        ClusterSupervisor::with_planner(self.config.clone(), Box::new(CapacityPlacement))
    }
}

impl PgWireHostService for BalancedDistributionHostService {
    fn name(&self) -> &'static str {
        "balanced-distribution"
    }

    fn start(&self, context: PgWireHostContext) -> Result<()> {
        context.start_distribution_controller(self.supervisor()?)
    }
}
