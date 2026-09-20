//! Relocation snapshot ingestion, source-authentication (D-1).
//!
//! Thesis: the relocation snapshot-ingestion path authenticates the relocation
//! IDENTITY (id/range_id/learner_epoch/source/target — all gossiped topology
//! fields) but never authenticates the SENDER. The transport's ApplySnapshot
//! handler (distribution_transport.rs) calls `apply_snapshot_step` WITHOUT
//! passing caller_node_id, so any authenticated cluster member — not just the
//! relocation's `source` — can drive snapshot batches into the target.
//!
//! The fence now lives at the transport dispatch, which is the only place a
//! REMOTE caller's identity exists: `validate_relocation_participant` rejects
//! any caller that is not the range leader, the relocation source, or its
//! target (unit-tested in `distribution_transport`). This test pins the
//! service-level behaviour that remains: `apply_snapshot_step` is an in-process
//! API with no caller argument, so it still applies what it is handed. That is
//! intentional — it is unreachable from the network except through the fenced
//! dispatch — and this test exists so that if the service is ever exposed
//! directly, the missing caller parameter is a visible, deliberate fact rather
//! than a rediscovered hole.

use bicdb_core::{
    distribution_key_token, BicDb, ClusterDataNodeService, ClusterId, ClusterNode, ClusterNodeId,
    CollectionMeta, DbConfig, DistributionConfig, DistributionStore, RangeSnapshotBatch,
    RebalanceOptions, Record, SnapshotSourceStep,
};
use parking_lot::RwLock;
use serde_json::json;
use std::sync::Arc;

fn record_id_for(range: &bicdb_core::RangeDescriptor, belongs: bool) -> String {
    (0..100_000)
        .map(|n| format!("doc-{n:05}"))
        .find(|id| range.contains_token(distribution_key_token("documents", id)) == belongs)
        .expect("token space should contain a matching id")
}

#[test]
fn snapshot_ingestion_service_api_has_no_caller_argument() {
    // --- Build a topology with a live relocation source -> target. ---
    let topology_root = tempfile::tempdir().unwrap();
    let actor = ClusterNodeId::new("n1").unwrap();
    let cluster_id = ClusterId::new("cluster-a").unwrap();
    let config = DistributionConfig {
        enabled: true,
        cluster_id: cluster_id.clone(),
        node_id: actor.clone(),
        node_address: "127.0.0.1:9441".to_string(),
        node_capacity_bytes: 10_000,
        replication_factor: 2,
        initial_ranges: 4,
        suspect_after_ms: 1_000,
        dead_after_ms: 2_000,
        ..DistributionConfig::default()
    };
    let mut topology =
        DistributionStore::initialize_at(topology_root.path(), config, false, 10).unwrap();
    topology
        .join_node(
            ClusterNode::new(
                ClusterNodeId::new("n2").unwrap(),
                "127.0.0.1:9442",
                1,
                10_000,
                20,
            )
            .unwrap(),
            &actor,
            20,
        )
        .unwrap();
    let (_, relocations) = topology
        .start_failure_repair_cycle(
            &RebalanceOptions {
                max_replica_moves: 1,
                max_moves_per_node: 1,
                unknown_range_bytes: 1,
                ..RebalanceOptions::default()
            },
            &actor,
            30,
        )
        .unwrap();
    let relocation = topology.relocation(relocations[0]).unwrap().clone();
    let range = topology
        .topology()
        .range_by_id(relocation.range_id)
        .unwrap()
        .clone();

    // --- Stand up the TARGET node's data service. ---
    let target_root = tempfile::tempdir().unwrap();
    let target_db_root = tempfile::tempdir().unwrap();
    let target_db = Arc::new(RwLock::new(
        BicDb::open_with_config(target_db_root.path(), DbConfig::default().with_fsync(false))
            .unwrap(),
    ));
    let mut service = ClusterDataNodeService::open(
        cluster_id.clone(),
        relocation.target.clone(), // this node IS the learner target
        target_root.path(),
        Arc::clone(&target_db),
        false,
    )
    .unwrap();

    // prepare_learner checks only `ensure_target` (local == relocation.target).
    service.prepare_learner(&relocation).unwrap();

    // --- Forge a snapshot batch. The caller is NOT relocation.source; the API
    // has no caller parameter at all, so nothing can reject a non-source sender. ---
    let inside_id = record_id_for(&range, true);
    let forged = Record::new(&inside_id).with_metadata(json!({"forged_by": "not-the-source"}));
    let forged_bytes = serde_json::to_vec(&forged).unwrap().len();
    let batch = RangeSnapshotBatch {
        collection: "documents".to_string(),
        range_id: range.id,
        range_epoch: range.epoch,
        snapshot_commit_sequence: 5,
        resume_after_key: inside_id.clone(),
        serialized_record_bytes: forged_bytes,
        records: vec![forged],
    };
    let meta = CollectionMeta::standard("documents");

    let progress = service
        .apply_snapshot_step(
            &relocation,
            &range,
            &SnapshotSourceStep::Batch {
                collection_meta: meta,
                batch,
            },
        )
        .unwrap();
    let _ = progress;

    // --- Observed result: the forged record is durable in the target DB. ---
    let stored = target_db.read().get("documents", &inside_id).unwrap();
    assert!(
        stored.is_some(),
        "forged snapshot record should have been persisted"
    );
    let _ = stored;
}
