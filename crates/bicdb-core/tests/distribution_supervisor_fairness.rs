//! Controller scheduling regression; this does not qualify physical relocation.
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

use bicdb_core::{
    CancellationToken, ClusterId, ClusterNode, ClusterNodeId, ClusterRelocationDriver,
    ClusterSupervisor, ClusterSupervisorConfig, DistributionConfig, DistributionStore,
    PlannedReplicaMove, RebalanceOptions, RebalancePlan, RelocationDriveOutcome, RelocationId,
    RelocationPhase, ReplicaMoveReason, Result,
};

struct OwnedDirectory(PathBuf);

impl OwnedDirectory {
    fn new() -> Self {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "bicdb-relocation-fairness-{}-{nonce}",
            std::process::id()
        ));
        std::fs::create_dir(&path).unwrap();
        Self(path)
    }
}

impl Drop for OwnedDirectory {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

struct OneBlockedMove {
    blocked: Option<RelocationId>,
    visited: Vec<RelocationId>,
}

impl ClusterRelocationDriver for OneBlockedMove {
    fn drive_relocation(
        &mut self,
        store: &mut DistributionStore,
        id: RelocationId,
        actor: &ClusterNodeId,
        now_ms: u64,
        cancellation: &CancellationToken,
    ) -> Result<RelocationDriveOutcome> {
        cancellation.check()?;
        self.visited.push(id);
        if Some(id) == self.blocked {
            return Ok(RelocationDriveOutcome::Deferred);
        }
        if store.relocation(id).unwrap().phase == RelocationPhase::LearnerAllocated {
            // The mock transport is ready for every independent move.
            store.begin_relocation_snapshot(id, actor, now_ms)?;
            return Ok(RelocationDriveOutcome::Progressed(
                RelocationPhase::SnapshotCopying,
            ));
        }
        Ok(RelocationDriveOutcome::Deferred)
    }
}

fn relocation_fixture() -> (
    OwnedDirectory,
    DistributionStore,
    ClusterNodeId,
    Vec<RelocationId>,
) {
    let directory = OwnedDirectory::new();
    let actor = ClusterNodeId::new("n1").unwrap();
    let config = DistributionConfig {
        enabled: true,
        cluster_id: ClusterId::new("fair-controller").unwrap(),
        node_id: actor.clone(),
        node_address: "127.0.0.1:9444".into(),
        node_capacity_bytes: 1_000_000,
        initial_ranges: 8,
        replication_factor: 1,
        ..DistributionConfig::default()
    };
    // No database or sockets: RF1/fsync-off isolate metadata scheduling only.
    let mut store = DistributionStore::initialize_at(&directory.0, config, false, 10).unwrap();
    store
        .join_node(
            ClusterNode::new(
                ClusterNodeId::new("n2").unwrap(),
                "127.0.0.1:9445",
                1,
                1_000_000,
                20,
            )
            .unwrap(),
            &actor,
            20,
        )
        .unwrap();
    let options = RebalanceOptions {
        max_replica_moves: 8,
        max_moves_per_node: 8,
        unknown_range_bytes: 1,
        ..RebalanceOptions::default()
    };
    let plan = store.plan_rebalance(&options, 30).unwrap();
    let ids = store.apply_rebalance_plan(&plan, &actor, 30).unwrap();
    assert!(ids.len() > 1, "fixture needs independent pending moves");
    (directory, store, actor, ids)
}

fn bounded_supervisor() -> ClusterSupervisor {
    ClusterSupervisor::new(ClusterSupervisorConfig {
        heartbeat_each_tick: false,
        refresh_topology_each_tick: false,
        max_relocation_steps_per_tick: 1,
        ..ClusterSupervisorConfig::default()
    })
    .unwrap()
}

#[test]
fn one_deferred_move_does_not_starve_other_ranges() {
    let (_directory, mut store, _actor, ids) = relocation_fixture();
    let supervisor = bounded_supervisor();
    let mut driver = OneBlockedMove {
        blocked: Some(ids[0]),
        visited: Vec::new(),
    };
    for tick in 0..ids.len() {
        supervisor
            .tick(
                &mut store,
                &mut driver,
                31 + tick as u64,
                &CancellationToken::uncancelable(),
            )
            .unwrap();
    }
    let starved: Vec<_> = ids
        .iter()
        .skip(1)
        .filter(|id| store.relocation(**id).unwrap().phase == RelocationPhase::LearnerAllocated)
        .collect();
    assert!(
        starved.is_empty(),
        "a deferred move starved healthy ranges: starved={starved:?}, attempted={:?}",
        driver.visited
    );
    assert_eq!(
        store.relocation(ids[0]).unwrap().phase,
        RelocationPhase::LearnerAllocated
    );
}

#[test]
fn newly_queued_moves_do_not_postpone_an_existing_moves_next_turn() {
    let (_directory, mut store, actor, ids) = relocation_fixture();
    let supervisor = bounded_supervisor();
    let mut driver = OneBlockedMove {
        blocked: Some(ids[0]),
        visited: Vec::new(),
    };
    supervisor
        .tick(
            &mut store,
            &mut driver,
            31,
            &CancellationToken::uncancelable(),
        )
        .unwrap();
    assert_eq!(driver.visited, vec![ids[0]]);

    // The transient blocker has cleared. New work must join behind work that
    // was already waiting; monotonically increasing IDs cannot delay its turn.
    driver.blocked = None;
    store
        .join_node(
            ClusterNode::new(
                ClusterNodeId::new("n3").unwrap(),
                "127.0.0.1:9446",
                1,
                1_000_000,
                32,
            )
            .unwrap(),
            &actor,
            32,
        )
        .unwrap();
    let range = store
        .topology()
        .ranges
        .values()
        .find(|range| {
            store
                .topology()
                .active_relocation_for_range(range.id)
                .is_none()
        })
        .unwrap();
    // Allocate independent work through the public operator-plan boundary.
    let plan = RebalancePlan {
        id: "later-independent-move".into(),
        topology_generation: store.topology().generation,
        created_at_ms: 33,
        replica_moves: vec![PlannedReplicaMove {
            range_id: range.id,
            expected_epoch: range.epoch,
            source: Some(actor.clone()),
            target: ClusterNodeId::new("n3").unwrap(),
            estimated_bytes: 1,
            reason: ReplicaMoveReason::LoadBalance,
        }],
        leader_transfers: Vec::new(),
        unplaced_ranges: Vec::new(),
        estimated_bytes_in_flight: 1,
    };
    let later_ids = store.apply_rebalance_plan(&plan, &actor, 33).unwrap();
    assert!(!later_ids.is_empty(), "fixture requires newly queued work");
    for tick in 0..ids.len() {
        supervisor
            .tick(
                &mut store,
                &mut driver,
                34 + tick as u64,
                &CancellationToken::uncancelable(),
            )
            .unwrap();
    }
    assert_eq!(
        store.relocation(ids[0]).unwrap().phase,
        RelocationPhase::SnapshotCopying,
        "new arrivals postponed an existing move: attempted={:?}, new={later_ids:?}",
        driver.visited
    );
}
