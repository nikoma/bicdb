//! Live commit-stream replication: gapless frames from the durable log, a
//! cursor that survives log truncation, monotonic commit sequence numbers, and
//! the synchronous quorum that holds commit acknowledgements.

use bicdb_core::{
    BicDb, DbConfig, Record, ReplicationConfig, ReplicationCursor, ReplicationMode,
    ReplicationTlsConfig, StorageMode, SyncQuorum, SyncTimeoutPolicy,
};
use serde_json::json;
use std::sync::Arc;
use std::time::Duration;

fn replication(mode: ReplicationMode) -> ReplicationConfig {
    ReplicationConfig {
        enabled: true,
        mode,
        listen_addr: Some("127.0.0.1:0".to_string()),
        tls: Some(ReplicationTlsConfig {
            cert_path: Default::default(),
            key_path: Default::default(),
            ca_path: Default::default(),
            require_client_cert: true,
            dev_localhost_plaintext: true,
        }),
        ..ReplicationConfig::default()
    }
}

fn storage_modes() -> Vec<(&'static str, StorageMode)> {
    vec![
        ("embedded", StorageMode::EmbeddedMemory),
        ("paged", StorageMode::ServerPaged),
    ]
}

fn open(path: &std::path::Path, storage: StorageMode, mode: ReplicationMode) -> BicDb {
    BicDb::open_with_config(
        path,
        DbConfig::default()
            .with_fsync(false)
            .with_storage_mode(storage)
            .with_replication(replication(mode)),
    )
    .unwrap()
}

fn put(db: &BicDb, id: &str, value: i64) {
    let mut tx = db.begin_transaction().unwrap();
    tx.insert(
        "items",
        Record::new(id).with_metadata(json!({ "value": value })),
    )
    .unwrap();
    tx.commit().unwrap();
}

fn read_only_commit(db: &BicDb) {
    let tx = db.begin_transaction().unwrap();
    tx.commit().unwrap();
}

fn value(db: &BicDb, id: &str) -> Option<i64> {
    db.get("items", id)
        .unwrap()
        .map(|record| record.metadata["value"].as_i64().unwrap())
}

#[test]
fn every_sequence_ships_including_empty_commits() {
    for (mode, storage) in storage_modes() {
        let primary_dir = tempfile::tempdir().unwrap();
        let standby_dir = tempfile::tempdir().unwrap();
        let mut primary = open(
            primary_dir.path(),
            storage.clone(),
            ReplicationMode::Primary,
        );
        let mut standby = open(
            standby_dir.path(),
            storage.clone(),
            ReplicationMode::Standby,
        );
        primary.create_collection("items").unwrap();
        put(&primary, "a", 1);
        read_only_commit(&primary);
        read_only_commit(&primary);
        put(&primary, "b", 2);

        let mut cursor = ReplicationCursor::new(standby.last_applied_commit_seq());
        let frames = primary
            .replication_frames_after(&mut cursor, 1_000)
            .unwrap();
        assert_eq!(
            frames.len() as u64,
            primary.current_commit_seq() - standby.last_applied_commit_seq(),
            "{mode}: one frame per sequence number"
        );
        standby.apply_replication_batch(&frames).unwrap();
        assert_eq!(value(&standby, "a"), Some(1), "{mode}");
        assert_eq!(value(&standby, "b"), Some(2), "{mode}");
        assert_eq!(
            standby.last_applied_commit_seq(),
            primary.current_commit_seq(),
            "{mode}"
        );

        // Incremental rounds ship only what is new.
        assert!(primary
            .replication_frames_after(&mut cursor, 1_000)
            .unwrap()
            .is_empty());
        put(&primary, "a", 10);
        read_only_commit(&primary);
        let frames = primary
            .replication_frames_after(&mut cursor, 1_000)
            .unwrap();
        assert_eq!(frames.len(), 2, "{mode}");
        standby.apply_replication_batch(&frames).unwrap();
        assert_eq!(value(&standby, "a"), Some(10), "{mode}");

        // A limit splits a backlog into rounds without losing anything.
        for index in 0..5 {
            put(&primary, &format!("n{index}"), index);
        }
        let mut shipped = 0;
        loop {
            let frames = primary.replication_frames_after(&mut cursor, 2).unwrap();
            if frames.is_empty() {
                break;
            }
            assert!(frames.len() <= 2, "{mode}");
            shipped += frames.len();
            standby.apply_replication_batch(&frames).unwrap();
        }
        assert_eq!(shipped, 5, "{mode}");
        assert_eq!(value(&standby, "n4"), Some(4), "{mode}");
    }
}

#[test]
fn cursor_survives_log_truncation_and_reopen() {
    for (mode, storage) in storage_modes() {
        let primary_dir = tempfile::tempdir().unwrap();
        let standby_dir = tempfile::tempdir().unwrap();
        let mut standby = open(
            standby_dir.path(),
            storage.clone(),
            ReplicationMode::Standby,
        );
        let mut cursor = ReplicationCursor::new(0);
        {
            let mut primary = open(
                primary_dir.path(),
                storage.clone(),
                ReplicationMode::Primary,
            );
            primary.create_collection("items").unwrap();
            put(&primary, "a", 1);
            let frames = primary
                .replication_frames_after(&mut cursor, 1_000)
                .unwrap();
            standby.apply_replication_batch(&frames).unwrap();
            // Truncates the transaction log under the cursor.
            primary.compact().unwrap();
            put(&primary, "b", 2);
            let frames = primary
                .replication_frames_after(&mut cursor, 1_000)
                .unwrap();
            standby.apply_replication_batch(&frames).unwrap();
            assert_eq!(value(&standby, "b"), Some(2), "{mode}");
        }
        // After a restart the primary continues the sequence instead of
        // reusing numbers the standby already holds.
        let primary = open(
            primary_dir.path(),
            storage.clone(),
            ReplicationMode::Primary,
        );
        let before = standby.last_applied_commit_seq();
        assert!(primary.current_commit_seq() >= before, "{mode}");
        put(&primary, "c", 3);
        assert!(primary.current_commit_seq() > before, "{mode}");
        let mut cursor = ReplicationCursor::new(before);
        let frames = primary
            .replication_frames_after(&mut cursor, 1_000)
            .unwrap();
        standby.apply_replication_batch(&frames).unwrap();
        assert_eq!(value(&standby, "c"), Some(3), "{mode}");
        assert_eq!(
            standby.last_applied_commit_seq(),
            primary.current_commit_seq(),
            "{mode}"
        );

        // A brand-new standby cannot be served from a truncated log.
        let mut fresh = ReplicationCursor::new(0);
        assert!(
            primary.replication_frames_after(&mut fresh, 1_000).is_err(),
            "{mode}: needs a snapshot bootstrap"
        );
    }
}

#[test]
fn sync_quorum_holds_commits_until_standbys_confirm() {
    let quorum = Arc::new(SyncQuorum::default());
    quorum.wait_for(5).unwrap(); // asynchronous by default

    quorum.configure(2, Duration::from_secs(5), SyncTimeoutPolicy::Fail);
    let waiter = {
        let quorum = Arc::clone(&quorum);
        std::thread::spawn(move || quorum.wait_for(7))
    };
    std::thread::sleep(Duration::from_millis(50));
    quorum.record_ack("s1", 9);
    std::thread::sleep(Duration::from_millis(50));
    assert!(!waiter.is_finished(), "one of two standbys is not a quorum");
    quorum.record_ack("s2", 7);
    waiter.join().unwrap().unwrap();
    assert_eq!(quorum.status().quorum_seq, 7);

    quorum.configure(2, Duration::from_millis(50), SyncTimeoutPolicy::Fail);
    assert!(quorum.wait_for(8).is_err(), "times out without a quorum");

    quorum.configure(2, Duration::from_millis(50), SyncTimeoutPolicy::Degrade);
    quorum.wait_for(8).unwrap();
    assert!(quorum.status().degraded);
    quorum.wait_for(20).unwrap(); // degraded: does not wait
    quorum.record_ack("s2", 30);
    assert!(quorum.status().degraded, "s1 still lags behind commit 20");
    quorum.record_ack("s1", 30);
    assert!(
        !quorum.status().degraded,
        "caught-up standbys restore sync commit"
    );
}

/// What a server's replication sender does: ship, then record the standby's
/// acknowledged position (which also pins the log for it).
fn ship(primary: &BicDb, standby: &mut BicDb, cursor: &mut ReplicationCursor) {
    loop {
        let frames = primary.replication_frames_after(cursor, 1_000).unwrap();
        if frames.is_empty() {
            primary
                .sync_quorum()
                .record_ack("standby", standby.last_applied_commit_seq());
            return;
        }
        standby.apply_replication_batch(&frames).unwrap();
    }
}

fn ids(db: &BicDb, collection: &str) -> Option<Vec<String>> {
    let mut ids = db
        .scan_collection(collection)
        .ok()?
        .into_iter()
        .map(|record| record.id)
        .collect::<Vec<_>>();
    ids.sort();
    Some(ids)
}

#[test]
fn catalog_operations_replicate_in_stream_order() {
    for (mode, storage) in storage_modes() {
        let primary_dir = tempfile::tempdir().unwrap();
        let standby_dir = tempfile::tempdir().unwrap();
        let mut primary = open(
            primary_dir.path(),
            storage.clone(),
            ReplicationMode::Primary,
        );
        let mut standby = open(
            standby_dir.path(),
            storage.clone(),
            ReplicationMode::Standby,
        );
        let mut cursor = ReplicationCursor::new(0);
        primary.sync_quorum().record_ack("standby", 0);

        // An empty collection exists on the standby too.
        primary.create_collection("empty").unwrap();
        primary.create_collection("items").unwrap();
        put(&primary, "old", 1);
        ship(&primary, &mut standby, &mut cursor);
        assert_eq!(ids(&standby, "empty"), Some(vec![]), "{mode}");

        // Drop + re-create with the same name: the standby must not keep the
        // dropped collection's rows under the new one.
        primary.drop_collection("items").unwrap();
        primary.create_collection("items").unwrap();
        put(&primary, "new", 2);
        ship(&primary, &mut standby, &mut cursor);
        assert_eq!(
            ids(&standby, "items"),
            Some(vec!["new".to_string()]),
            "{mode}"
        );

        // Rename away (as the recycle bin does), re-create, rename back.
        primary.rename_collection("items", "items_binned").unwrap();
        primary.create_collection("items").unwrap();
        put(&primary, "other", 3);
        primary.drop_collection("items").unwrap();
        primary.rename_collection("items_binned", "items").unwrap();
        ship(&primary, &mut standby, &mut cursor);
        assert_eq!(
            ids(&standby, "items"),
            Some(vec!["new".to_string()]),
            "{mode}"
        );
        assert_eq!(ids(&standby, "items_binned"), None, "{mode}");
        assert_eq!(ids(&primary, "items"), ids(&standby, "items"), "{mode}");
    }
}

#[test]
fn the_log_is_retained_for_a_lagging_standby() {
    for (mode, storage) in storage_modes() {
        let primary_dir = tempfile::tempdir().unwrap();
        let standby_dir = tempfile::tempdir().unwrap();
        let mut primary = open(
            primary_dir.path(),
            storage.clone(),
            ReplicationMode::Primary,
        );
        let mut standby = open(
            standby_dir.path(),
            storage.clone(),
            ReplicationMode::Standby,
        );
        primary.sync_quorum().record_ack("standby", 0);
        let mut cursor = ReplicationCursor::new(0);
        primary.create_collection("items").unwrap();
        put(&primary, "a", 1);
        ship(&primary, &mut standby, &mut cursor);
        // The standby falls behind, then the primary compacts its log.
        for index in 0..20 {
            put(&primary, &format!("k{index}"), index);
            read_only_commit(&primary);
        }
        primary.compact().unwrap();
        put(&primary, "after", 99);
        // A reconnecting standby (fresh cursor at its position) catches up.
        let mut cursor = ReplicationCursor::new(standby.last_applied_commit_seq());
        ship(&primary, &mut standby, &mut cursor);
        assert_eq!(value(&standby, "k19"), Some(19), "{mode}");
        assert_eq!(value(&standby, "after"), Some(99), "{mode}");
        assert_eq!(
            standby.last_applied_commit_seq(),
            primary.current_commit_seq(),
            "{mode}"
        );
    }
}
