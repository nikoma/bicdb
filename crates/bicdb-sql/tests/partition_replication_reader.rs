//! Actual Native logical replication and local promotion control. This does
//! not qualify network elections, portable rename origins or physical HA.
use bicdb_core::{
    BicDb, DbConfig, ReplicationConfig, ReplicationMode, ReplicationTlsConfig, StorageMode,
};
use bicdb_sql::{SqlSession, SqlValue};
use std::collections::BTreeMap;

fn config(mode: StorageMode, role: ReplicationMode) -> DbConfig {
    DbConfig::default()
        .with_fsync(true)
        .with_storage_mode(mode)
        .with_paged_buffer_pool_bytes(8 * 1024 * 1024)
        .with_replication(ReplicationConfig {
            enabled: true,
            mode: role,
            cluster_id: "partition-reader-control".into(),
            listen_addr: Some("127.0.0.1:0".into()),
            tls: Some(ReplicationTlsConfig {
                cert_path: Default::default(),
                key_path: Default::default(),
                ca_path: Default::default(),
                require_client_cert: true,
                dev_localhost_plaintext: true,
            }),
            ..Default::default()
        })
}

fn native_hashes(db: &BicDb) -> BTreeMap<(String, String), String> {
    let mut hashes = BTreeMap::new();
    for name in ["independent", "low", "high"] {
        for row in db.scan_collection(name).unwrap() {
            hashes.insert((name.into(), row.id.clone()), row.content_hash().unwrap());
        }
    }
    hashes
}

fn assert_partition_reads(db: &BicDb) {
    let mut sql = SqlSession::new_shared(db);
    assert_eq!(
        sql.execute("SELECT id,payload FROM partitioned ORDER BY id")
            .unwrap()
            .rows,
        vec![
            vec![SqlValue::Int(11), SqlValue::String("right-ack".into())],
            vec![SqlValue::Int(12), SqlValue::String("left-ack".into())],
        ]
    );
    assert!(sql.execute("SELECT id FROM low").unwrap().rows.is_empty());
    assert_eq!(
        sql.execute("SELECT id FROM high ORDER BY id").unwrap().rows,
        vec![vec![SqlValue::Int(11)], vec![SqlValue::Int(12)]]
    );
}

#[test]
fn native_replicated_partition_rows_are_readable_live_and_survive_local_promotion() {
    for mode in [StorageMode::EmbeddedMemory, StorageMode::ServerPaged] {
        let root = tempfile::tempdir().unwrap();
        let mut source = BicDb::open_with_config(
            root.path().join("source"),
            config(mode.clone(), ReplicationMode::Primary),
        )
        .unwrap();
        source.sync_quorum().hold("partition-reader-control", 0);
        source.persist_standby_slots().unwrap();
        let mut sql = SqlSession::new(&mut source);
        for statement in [
            "CREATE TABLE independent(id INT PRIMARY KEY,payload TEXT)",
            "INSERT INTO independent VALUES(1,'independent-ack')",
            "CREATE TABLE partitioned(id INT PRIMARY KEY,payload TEXT) PARTITION BY RANGE(id)",
            "CREATE TABLE low PARTITION OF partitioned FOR VALUES FROM(0) TO(10)",
            "CREATE TABLE high PARTITION OF partitioned FOR VALUES FROM(10) TO(20)",
            "INSERT INTO partitioned VALUES(1,'left-ack'),(11,'right-ack')",
            "UPDATE partitioned SET id=12 WHERE id=1",
        ] {
            sql.execute(statement)
                .unwrap_or_else(|error| panic!("{mode}: {statement}: {error:?}"));
        }
        drop(sql);
        assert_partition_reads(&source);
        let acknowledged = native_hashes(&source);
        assert_eq!(acknowledged.len(), 3);
        let target_path = root.path().join("target");
        let standby_config = config(mode.clone(), ReplicationMode::Standby);
        let mut target = BicDb::open_with_config(&target_path, standby_config.clone()).unwrap();
        assert!(!target.sql_partition_execution_feature_active());
        let frames = source.export_replication_frames_since(0, 10_000).unwrap();
        assert!(!frames.is_empty());
        target.apply_replication_batch(&frames).unwrap();
        assert!(target.sql_partition_execution_feature_active());
        assert_partition_reads(&target);
        assert_eq!(native_hashes(&target), acknowledged);
        source.close().unwrap();
        target.close().unwrap();
        for _ in 0..2 {
            let target = BicDb::open_with_config(&target_path, standby_config.clone()).unwrap();
            assert_partition_reads(&target);
            assert_eq!(native_hashes(&target), acknowledged);
            target.close().unwrap();
        }
        let mut target = BicDb::open_with_config(&target_path, standby_config).unwrap();
        target
            .promote_by_election(1, "partition-successor", None)
            .unwrap();
        target.close().unwrap();
        let mut promoted =
            BicDb::open_with_config(&target_path, config(mode, ReplicationMode::Primary)).unwrap();
        assert_eq!(promoted.replication_epoch(), 1);
        assert_partition_reads(&promoted);
        assert_eq!(native_hashes(&promoted), acknowledged);
        SqlSession::new(&mut promoted)
            .execute("INSERT INTO partitioned VALUES(3,'after-promotion')")
            .unwrap();
        for ((collection, id), expected) in acknowledged {
            assert_eq!(
                promoted
                    .get(&collection, &id)
                    .unwrap()
                    .unwrap()
                    .content_hash()
                    .unwrap(),
                expected
            );
        }
        promoted.close().unwrap();
    }
}
