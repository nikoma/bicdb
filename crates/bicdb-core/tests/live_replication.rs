//! Live commit-stream replication: gapless frames from the durable log, a
//! cursor that survives log truncation, monotonic commit sequence numbers, and
//! the synchronous quorum that holds commit acknowledgements.

use bicdb_core::{
    read_only_standby_hint, stage_standby_bootstrap, standby_bootstrap_staging, BicDb, BicDbError,
    DbConfig, NodeRole, Record, ReplicationConfig, ReplicationCursor, ReplicationMode,
    ReplicationTlsConfig, StandbyResume, StorageMode, SyncQuorum, SyncTimeoutPolicy,
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

#[test]
fn standby_positions_survive_a_primary_restart() {
    for (mode, storage) in storage_modes() {
        let primary_dir = tempfile::tempdir().unwrap();
        let standby_dir = tempfile::tempdir().unwrap();
        let mut standby = open(
            standby_dir.path(),
            storage.clone(),
            ReplicationMode::Standby,
        );
        {
            let mut primary = open(
                primary_dir.path(),
                storage.clone(),
                ReplicationMode::Primary,
            );
            primary.sync_quorum().record_ack("standby", 0);
            primary.persist_standby_slots().unwrap();
            let mut cursor = ReplicationCursor::new(0);
            primary.create_collection("items").unwrap();
            put(&primary, "a", 1);
            ship(&primary, &mut standby, &mut cursor);
            for index in 0..20 {
                put(&primary, &format!("k{index}"), index);
            }
        }
        // Restarted, the primary has heard nothing from the standby yet; the
        // persisted position must still keep the log the standby needs.
        let mut primary = open(
            primary_dir.path(),
            storage.clone(),
            ReplicationMode::Primary,
        );
        assert!(
            primary.sync_quorum().acknowledged().contains_key("standby"),
            "{mode}"
        );
        primary.compact().unwrap();
        put(&primary, "after", 99);
        let mut cursor = ReplicationCursor::new(standby.last_applied_commit_seq());
        ship(&primary, &mut standby, &mut cursor);
        assert_eq!(value(&standby, "k19"), Some(19), "{mode}");
        assert_eq!(value(&standby, "after"), Some(99), "{mode}");

        // A standby removed from the allow-list stops pinning the log.
        primary.retain_standby_slots(&[]).unwrap();
        assert!(primary.sync_quorum().acknowledged().is_empty(), "{mode}");
        drop(primary);
        let mut primary = open(
            primary_dir.path(),
            storage.clone(),
            ReplicationMode::Primary,
        );
        assert!(primary.sync_quorum().acknowledged().is_empty(), "{mode}");
    }
}

#[test]
fn a_primary_that_sees_a_newer_epoch_is_fenced_durably() {
    for (mode, storage) in storage_modes() {
        let dir = tempfile::tempdir().unwrap();
        {
            let mut primary = open(dir.path(), storage.clone(), ReplicationMode::Primary);
            primary.create_collection("items").unwrap();
            put(&primary, "a", 1);
            assert_eq!(primary.replication_role(), NodeRole::Primary, "{mode}");
            // Same or older epochs change nothing.
            assert!(!primary.observe_replication_epoch(0).unwrap(), "{mode}");
            assert!(primary.observe_replication_epoch(3).unwrap(), "{mode}");
            assert_eq!(primary.replication_role(), NodeRole::Fenced, "{mode}");
            let error = primary
                .begin_transaction()
                .and_then(|mut tx| {
                    tx.insert("items", Record::new("b"))?;
                    tx.commit()
                })
                .unwrap_err();
            assert!(
                matches!(&error, BicDbError::ReadOnlyStandby(message) if message.contains("fenced")),
                "{mode}: {error}"
            );
            // Reads keep working.
            assert_eq!(value(&primary, "a"), Some(1), "{mode}");
        }
        // The fence survives a restart even though the configuration still
        // says primary.
        let mut reopened = open(dir.path(), storage.clone(), ReplicationMode::Primary);
        assert_eq!(reopened.replication_role(), NodeRole::Fenced, "{mode}");
        assert_eq!(reopened.replication_epoch(), 3, "{mode}");
        assert!(reopened.create_collection("other").is_err(), "{mode}");
    }
}

#[test]
fn standbys_follow_newer_epochs_and_point_writes_at_the_primary() {
    for (mode, storage) in storage_modes() {
        let dir = tempfile::tempdir().unwrap();
        let mut standby = open(dir.path(), storage.clone(), ReplicationMode::Standby);
        assert!(!standby.observe_replication_epoch(2).unwrap(), "{mode}");
        assert_eq!(standby.replication_role(), NodeRole::Standby, "{mode}");
        assert_eq!(standby.replication_epoch(), 2, "{mode}");

        let refusal = |db: &mut BicDb| match db.create_collection("items") {
            Err(BicDbError::ReadOnlyStandby(message)) => message,
            other => panic!("{mode}: expected a read-only refusal, got {other:?}"),
        };
        assert_eq!(
            read_only_standby_hint(&refusal(&mut standby)),
            None,
            "{mode}"
        );
        standby
            .record_primary_sql_address(Some("db1.example:5432".to_string()))
            .unwrap();
        let message = refusal(&mut standby);
        assert_eq!(
            read_only_standby_hint(&message).as_deref(),
            Some("send writes to the primary at db1.example:5432"),
            "{mode}: {message}"
        );
        assert_eq!(
            standby.ha_node_status().primary_sql_address.as_deref(),
            Some("db1.example:5432"),
            "{mode}"
        );
    }
}

#[test]
fn closing_the_quorum_fails_waiting_commits_instead_of_hanging() {
    let quorum = Arc::new(SyncQuorum::default());
    // Wait forever for one standby.
    quorum.configure(1, Duration::ZERO, SyncTimeoutPolicy::Fail);
    let waiter = {
        let quorum = Arc::clone(&quorum);
        std::thread::spawn(move || quorum.wait_for(5))
    };
    std::thread::sleep(Duration::from_millis(50));
    quorum.close("the server is shutting down");
    let error = waiter.join().unwrap().unwrap_err();
    assert!(error.to_string().contains("shutting down"), "{error}");
    // Later waits fail at once.
    assert!(quorum.wait_for(6).is_err());
    // An already confirmed commit still succeeds.
    quorum.record_ack("standby", 10);
    assert!(quorum.wait_for(7).is_ok());
}

#[test]
fn a_standby_that_cannot_stream_is_bootstrapped_from_a_snapshot() {
    for (mode, storage) in storage_modes() {
        let root = tempfile::tempdir().unwrap();
        let primary_dir = root.path().join("primary");
        let standby_dir = root.path().join("standby");
        let mut primary = open(&primary_dir, storage.clone(), ReplicationMode::Primary);
        primary.create_collection("items").unwrap();
        for index in 0..30 {
            put(&primary, &format!("k{index}"), index);
        }
        // No standby holds the log back: it is cut.
        primary.compact().unwrap();
        put(&primary, "after-compact", 1);

        // A brand-new standby cannot stream from commit 0 any more.
        assert!(
            matches!(
                primary.standby_resume(0, 0).unwrap(),
                StandbyResume::Snapshot(_)
            ),
            "{mode}"
        );
        // A standby ahead of the primary has diverged.
        let ahead = primary.current_commit_seq() + 5;
        assert!(
            matches!(
                primary.standby_resume(0, ahead).unwrap(),
                StandbyResume::Snapshot(_)
            ),
            "{mode}"
        );

        {
            let standby = open(&standby_dir, storage.clone(), ReplicationMode::Standby);
            standby.observe_replication_epoch(4).unwrap();
            standby
                .record_primary_sql_address(Some("db1:5432".to_string()))
                .unwrap();
        }
        let archive = root.path().join("snapshot.bak");
        let key = "0123456789abcdef0123456789abcdef";
        let snapshot_seq = primary.create_standby_snapshot(&archive, key).unwrap();
        assert_eq!(snapshot_seq, primary.current_commit_seq(), "{mode}");
        stage_standby_bootstrap(&standby_dir, &archive, key, false).unwrap();
        assert!(standby_bootstrap_staging(&standby_dir).exists(), "{mode}");

        // The next open installs the snapshot and keeps the standby's identity.
        let mut standby = open(&standby_dir, storage.clone(), ReplicationMode::Standby);
        assert!(!standby_bootstrap_staging(&standby_dir).exists(), "{mode}");
        assert_eq!(standby.last_applied_commit_seq(), snapshot_seq, "{mode}");
        assert_eq!(standby.replication_role(), NodeRole::Standby, "{mode}");
        assert_eq!(standby.replication_epoch(), 4, "{mode}");
        assert_eq!(
            standby.ha_node_status().primary_sql_address.as_deref(),
            Some("db1:5432"),
            "{mode}"
        );
        assert_eq!(value(&standby, "k29"), Some(29), "{mode}");
        assert_eq!(value(&standby, "after-compact"), Some(1), "{mode}");
        assert!(standby.create_collection("x").is_err(), "{mode}");

        // From there it streams normally.
        assert_eq!(
            primary.standby_resume(0, snapshot_seq).unwrap(),
            StandbyResume::Stream,
            "{mode}"
        );
        put(&primary, "streamed", 7);
        let mut cursor = ReplicationCursor::new(snapshot_seq);
        ship(&primary, &mut standby, &mut cursor);
        assert_eq!(value(&standby, "streamed"), Some(7), "{mode}");
        assert_eq!(
            standby.last_applied_commit_seq(),
            primary.current_commit_seq(),
            "{mode}"
        );
    }
}

#[test]
fn an_interrupted_bootstrap_leaves_the_old_database_whole() {
    let root = tempfile::tempdir().unwrap();
    let standby_dir = root.path().join("standby");
    {
        let mut primary = open(
            &standby_dir,
            StorageMode::EmbeddedMemory,
            ReplicationMode::Primary,
        );
        primary.create_collection("items").unwrap();
        put(&primary, "kept", 1);
    }
    // Staging without the completion marker is discarded on open.
    let staging = standby_bootstrap_staging(&standby_dir);
    std::fs::create_dir_all(&staging).unwrap();
    std::fs::write(staging.join("partial"), b"x").unwrap();
    let primary = open(
        &standby_dir,
        StorageMode::EmbeddedMemory,
        ReplicationMode::Primary,
    );
    assert!(!staging.exists());
    assert_eq!(value(&primary, "kept"), Some(1));
}

#[test]
fn pinned_standbys_retain_the_log_without_confirming_commits() {
    let quorum = SyncQuorum::default();
    quorum.configure(1, Duration::from_millis(50), SyncTimeoutPolicy::Fail);
    quorum.pin("fresh", 10);
    assert_eq!(quorum.retention_floor(), Some(10));
    assert!(quorum.wait_for(5).is_err(), "a pin is not a confirmation");
    quorum.set_ack("fresh", 12);
    assert!(quorum.wait_for(12).is_ok());
    // A reconnect with less data replaces the higher position.
    quorum.set_ack("fresh", 3);
    assert!(quorum.wait_for(4).is_err());
}

#[test]
fn votes_go_to_one_up_to_date_candidate_per_term() {
    for (mode, storage) in storage_modes() {
        let dir = tempfile::tempdir().unwrap();
        let mut voter = open(dir.path(), storage.clone(), ReplicationMode::Primary);
        voter.create_collection("items").unwrap();
        put(&voter, "a", 1);
        let own = voter.last_applied_commit_seq();
        drop(voter);
        let voter = open(dir.path(), storage.clone(), ReplicationMode::Standby);
        // Behind this node: refused.
        assert!(!voter.consider_vote(1, "c1", 0, own - 1).unwrap(), "{mode}");
        // Up to date: granted, once per term.
        assert!(voter.consider_vote(1, "c1", 0, own).unwrap(), "{mode}");
        assert!(voter.consider_vote(1, "c1", 0, own).unwrap(), "{mode}");
        assert!(!voter.consider_vote(1, "c2", 0, own + 5).unwrap(), "{mode}");
        // A later term is a new election.
        assert!(voter.consider_vote(2, "c2", 0, own).unwrap(), "{mode}");
        assert_eq!(
            voter.ha_node_status().voted,
            Some((2, "c2".to_string())),
            "{mode}"
        );
        // A candidacy starts after every term seen and votes for itself.
        assert_eq!(voter.begin_candidacy("me").unwrap(), 3, "{mode}");
        assert!(!voter.consider_vote(3, "c3", 0, own).unwrap(), "{mode}");
        // Terms at or below a known primary epoch are over.
        voter
            .record_primary("p", 5, None, &Default::default())
            .unwrap();
        assert!(!voter.consider_vote(5, "c5", 9, own).unwrap(), "{mode}");
        // Commits from an older primary lose against a newer primary's.
        assert!(
            !voter.consider_vote(6, "c6", 4, own + 100).unwrap(),
            "{mode}"
        );
        assert!(voter.consider_vote(6, "c6", 5, own).unwrap(), "{mode}");
    }
}

#[test]
fn a_primary_that_grants_a_vote_is_fenced() {
    let dir = tempfile::tempdir().unwrap();
    let primary = open(
        dir.path(),
        StorageMode::EmbeddedMemory,
        ReplicationMode::Primary,
    );
    assert!(primary.consider_vote(1, "c1", 0, 0).unwrap());
    assert_eq!(primary.replication_role(), NodeRole::Fenced);
}

#[test]
fn a_promoted_primary_bootstraps_standbys_that_diverged_from_its_term() {
    for (mode, storage) in storage_modes() {
        let dir = tempfile::tempdir().unwrap();
        {
            let mut standby = open(dir.path(), storage.clone(), ReplicationMode::Primary);
            standby.create_collection("items").unwrap();
            put(&standby, "a", 1);
            put(&standby, "b", 2);
        }
        let standby = open(dir.path(), storage.clone(), ReplicationMode::Standby);
        let start = standby.last_applied_commit_seq();
        standby.promote_by_election(2, "me", None).unwrap();
        drop(standby);
        let mut primary = open(dir.path(), storage.clone(), ReplicationMode::Primary);
        assert_eq!(primary.replication_role(), NodeRole::Primary, "{mode}");
        assert_eq!(primary.replication_epoch(), 2, "{mode}");
        put(&primary, "c", 3);
        // A former primary of term 0 with commits past the term-2 start has
        // a history this primary does not share.
        assert!(
            matches!(
                primary.standby_resume(0, start + 1).unwrap(),
                StandbyResume::Snapshot(_)
            ),
            "{mode}"
        );
        // Up to the start it shares the history and streams.
        assert_eq!(
            primary.standby_resume(0, start).unwrap(),
            StandbyResume::Stream,
            "{mode}"
        );
        // A standby that already followed term 2 streams.
        assert_eq!(
            primary.standby_resume(2, start + 1).unwrap(),
            StandbyResume::Stream,
            "{mode}"
        );
        // The write lease refuses writes until a standby was heard from.
        primary
            .sync_quorum()
            .configure_lease(Duration::from_secs(2), 1);
        assert!(primary.create_collection("leased").is_err(), "{mode}");
        primary.sync_quorum().record_ack("s1", 0);
        primary.create_collection("leased").unwrap();
    }
}

#[test]
fn failover_frames_survive_the_wire_encoding() {
    use bicdb_core::replication::ReplicationFrame;
    use bicdb_core::replication_transport::{recv_frame, send_frame};
    let frames = vec![
        ReplicationFrame::Heartbeat {
            cluster_id: "c".to_string(),
            node_id: "n1".to_string(),
            last_commit_seq: 9,
            epoch: 3,
            primary_sql_address: Some("db1:5432".to_string()),
            epoch_starts: vec![(2, 7), (3, 9)],
        },
        ReplicationFrame::VoteRequest {
            cluster_id: "c".to_string(),
            candidate: "n2".to_string(),
            term: 4,
            data_epoch: 3,
            last_commit_seq: 9,
        },
        ReplicationFrame::VoteResponse {
            node_id: "n3".to_string(),
            term: 4,
            granted: true,
            epoch: 3,
        },
        ReplicationFrame::Ping {
            cluster_id: "c".to_string(),
            node_id: "n1".to_string(),
            epoch: 3,
        },
        ReplicationFrame::Pong {
            node_id: "n2".to_string(),
            epoch: 4,
            primary_id: Some("n2".to_string()),
        },
    ];
    for frame in frames {
        let mut wire = Vec::new();
        send_frame(&mut wire, &frame, 1 << 20).unwrap();
        let decoded = recv_frame(&mut std::io::Cursor::new(wire), 1 << 20).unwrap();
        assert_eq!(decoded, frame);
    }
}

#[test]
fn flashback_queries_work_on_standbys() {
    use bicdb_core::FlashbackPoint;
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
        primary.create_collection("items").unwrap();
        put(&primary, "a", 1);
        primary.enable_flashback("items", 3600).unwrap();
        let before = primary.current_scn().unwrap();
        put(&primary, "a", 2);
        put(&primary, "b", 3);
        ship(&primary, &mut standby, &mut cursor);

        let as_of = |db: &BicDb| {
            db.flashback_rows("items", FlashbackPoint::Scn(before))
                .unwrap_or_else(|error| panic!("{mode}: {error}"))
                .into_iter()
                .map(|record| (record.id, record.metadata["value"].as_i64().unwrap()))
                .collect::<Vec<_>>()
        };
        assert_eq!(as_of(&primary), vec![("a".to_string(), 1)], "{mode}");
        assert_eq!(as_of(&standby), as_of(&primary), "{mode}");
        let one = standby
            .flashback_rows_for_ids("items", FlashbackPoint::Scn(before), &["a".to_string()])
            .unwrap();
        assert_eq!(one.len(), 1, "{mode}");
        // Still true after the standby restarts.
        drop(standby);
        let standby = open(
            standby_dir.path(),
            storage.clone(),
            ReplicationMode::Standby,
        );
        assert_eq!(as_of(&standby), vec![("a".to_string(), 1)], "{mode}");
    }
}

#[test]
fn standbys_build_the_primarys_indexes_from_their_own_rows() {
    use bicdb_core::{IndexDefinition, IndexField, IndexKind, IndexValue};
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
        primary.ensure_catalog_log().unwrap();
        primary.create_collection("items").unwrap();
        for index in 0..50 {
            put(&primary, &format!("k{index}"), index % 5);
        }
        let definition = IndexDefinition {
            name: "items_value".to_string(),
            collection: "items".to_string(),
            fields: vec![IndexField::MetadataPath(vec!["value".to_string()])],
            unique: false,
            kind: IndexKind::default(),
            predicate: None,
            exclusion: None,
        };
        primary.create_index(definition.clone()).unwrap();
        assert!(primary.announce_index_catalog().unwrap(), "{mode}");
        assert!(
            !primary.announce_index_catalog().unwrap(),
            "{mode}: unchanged"
        );
        ship(&primary, &mut standby, &mut cursor);
        assert_eq!(
            standby.index_definitions(),
            vec![definition.clone()],
            "{mode}"
        );
        let lookup = |db: &BicDb| {
            let mut ids = db
                .lookup_index("items_value", &[IndexValue::Int(3)])
                .unwrap();
            ids.sort();
            ids
        };
        assert_eq!(lookup(&standby), lookup(&primary), "{mode}");
        assert_eq!(lookup(&standby).len(), 10, "{mode}");
        // The build is local: the standby stays exactly where the stream left it.
        assert_eq!(
            standby.last_applied_commit_seq(),
            primary.current_commit_seq(),
            "{mode}"
        );
        // Later writes keep the standby's index current.
        put(&primary, "late", 3);
        ship(&primary, &mut standby, &mut cursor);
        assert_eq!(lookup(&standby).len(), 11, "{mode}");
        // Dropped on the primary, dropped on the standby.
        primary.drop_index("items_value").unwrap();
        primary.announce_index_catalog().unwrap();
        put(&primary, "after-drop", 1);
        ship(&primary, &mut standby, &mut cursor);
        assert!(standby.index_definitions().is_empty(), "{mode}");
        assert_eq!(
            standby.last_applied_commit_seq(),
            primary.current_commit_seq(),
            "{mode}"
        );
        // Writes stay refused on the standby.
        assert!(standby.create_collection("nope").is_err(), "{mode}");
    }
}
