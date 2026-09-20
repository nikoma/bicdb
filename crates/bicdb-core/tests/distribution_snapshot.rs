use bicdb_core::{
    distribution_key_token, BicDb, ClusterNodeId, DbConfig, PlacementPolicy, RangeDescriptor,
    RangeId, RangeReplica, RangeReplicaRole, RangeSnapshotOptions, Record, ReplicaId, StorageMode,
};
use serde_json::json;

fn paged_config() -> DbConfig {
    DbConfig::default()
        .with_fsync(false)
        .with_storage_mode(StorageMode::ServerPaged)
}

fn first_quarter_range() -> RangeDescriptor {
    let node = ClusterNodeId::new("n1").unwrap();
    RangeDescriptor {
        id: RangeId::new(1).unwrap(),
        start_token: 0,
        end_token: Some(1_u64 << 62),
        epoch: 7,
        replicas: vec![RangeReplica {
            id: ReplicaId::new(1).unwrap(),
            node_id: node.clone(),
            role: RangeReplicaRole::Voter,
        }],
        leader: node,
        approximate_bytes: 0,
        approximate_qps: 0,
        placement: PlacementPolicy::default(),
    }
}

#[test]
fn range_snapshot_is_bounded_filtered_and_resumable() {
    let dir = tempfile::tempdir().unwrap();
    {
        let mut db = BicDb::open_with_config(dir.path(), paged_config()).unwrap();
        db.create_collection("documents").unwrap();
        let records = (0..80)
            .map(|number| {
                Record::new(format!("doc-{number:04}"))
                    .with_metadata(json!({"text": format!("document {number}")}))
            })
            .collect::<Vec<_>>();
        db.batch_insert("documents", records).unwrap();
        db.close().unwrap();
    }

    let db = BicDb::open_with_config(dir.path(), paged_config()).unwrap();
    let range = first_quarter_range();
    let expected = (0..80)
        .map(|number| format!("doc-{number:04}"))
        .filter(|id| range.contains_token(distribution_key_token("documents", id)))
        .collect::<Vec<_>>();
    assert!(expected.len() > 4);
    let options = RangeSnapshotOptions {
        max_records_per_batch: 2,
        max_bytes_per_batch: 512,
        max_record_bytes: 256,
    };

    let mut exported = Vec::new();
    let first = db
        .for_each_range_snapshot_batch("documents", &range, None, &options, |batch| {
            assert!(batch.records.len() <= 2);
            assert!(batch.serialized_record_bytes <= 512);
            assert_eq!(batch.range_id, range.id);
            assert_eq!(batch.range_epoch, range.epoch);
            exported.extend(batch.records.into_iter().map(|record| record.id));
            Ok(false)
        })
        .unwrap();
    assert!(!first.completed);
    assert_eq!(exported.len(), 2);
    let resume = first.resume_after_key.clone().unwrap();

    let second = db
        .for_each_range_snapshot_batch("documents", &range, Some(&resume), &options, |batch| {
            assert!(batch.records.len() <= 2);
            assert!(batch.serialized_record_bytes <= 512);
            exported.extend(batch.records.into_iter().map(|record| record.id));
            Ok(true)
        })
        .unwrap();
    assert!(second.completed);
    assert_eq!(
        second.snapshot_commit_sequence,
        first.snapshot_commit_sequence
    );
    assert_eq!(exported, expected);
    assert!(exported
        .iter()
        .all(|id| { range.contains_token(distribution_key_token("documents", id)) }));
}
