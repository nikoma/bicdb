//! File-backed component regressions for issue 190. These drive the real
//! durable log, replication importer and HA state; they are not TLS/failover
//! qualification or an executed five-process loss experiment.
use bicdb_core::{
    stage_standby_bootstrap, BicDb, DbConfig, Record, ReplicationConfig, ReplicationCursor,
    ReplicationMode, ReplicationTlsConfig, StorageMode, HA_NODE_FILE,
};
use serde_json::json;
use std::path::Path;

fn open(path: &Path, mode: ReplicationMode) -> BicDb {
    BicDb::open_with_config(
        path,
        DbConfig::default()
            .with_fsync(true)
            .with_storage_mode(StorageMode::ServerPaged)
            .with_replication(ReplicationConfig {
                enabled: true,
                mode,
                node_id: path.file_name().unwrap().to_string_lossy().into_owned(),
                listen_addr: Some("127.0.0.1:0".into()),
                tls: Some(ReplicationTlsConfig {
                    cert_path: Default::default(),
                    key_path: Default::default(),
                    ca_path: Default::default(),
                    require_client_cert: true,
                    dev_localhost_plaintext: true,
                }),
                ..ReplicationConfig::default()
            }),
    )
    .unwrap()
}

fn source(path: &Path) -> BicDb {
    let mut db = open(path, ReplicationMode::Primary);
    db.create_collection("items").unwrap();
    for n in 0..8 {
        let mut tx = db.begin_transaction().unwrap();
        tx.insert(
            "items",
            Record::new(format!("r{n}")).with_metadata(json!({"n": n})),
        )
        .unwrap();
        tx.commit().unwrap();
    }
    db
}

#[test]
fn partial_stream_does_not_advertise_the_new_epoch_before_its_durable_boundary() {
    let root = tempfile::tempdir().unwrap();
    let p = source(&root.path().join("p"));
    let frames = p
        .replication_frames_after(&mut ReplicationCursor::new(0), 100)
        .unwrap();
    let end = p.replication_election_position().unwrap().1;
    assert!(frames.len() > 2);
    let mut full = open(&root.path().join("full"), ReplicationMode::Standby);
    for frame in &frames {
        full.apply_replication_frame(frame).unwrap();
    }
    let mut successor = open(&root.path().join("successor"), ReplicationMode::Standby);
    for frame in &frames {
        successor.apply_replication_frame(frame).unwrap();
    }
    successor.promote_by_election(1, "successor", None).unwrap();
    let starts = successor.ha_node_status().epoch_starts;
    assert_eq!(starts.get(&1), Some(&end));

    let partial_path = root.path().join("partial");
    let mut partial = open(&partial_path, ReplicationMode::Standby);
    partial
        .record_primary("successor", 1, None, &starts)
        .unwrap();
    assert_eq!(partial.replication_epoch(), 1); // Fencing advances before apply.
    assert_eq!(partial.replication_election_position().unwrap(), (0, 0));
    for frame in &frames[..2] {
        partial.apply_replication_frame(frame).unwrap();
    }
    let cut = partial.last_applied_commit_seq();
    assert!(cut < end);
    assert!(partial.replication_feed().durable_seq() >= cut);
    assert!(!partial
        .qualify_replication_data_epoch("successor", 1, cut)
        .unwrap());
    assert_eq!(partial.replication_election_position().unwrap(), (0, cut));
    drop(partial);
    let mut partial = open(&partial_path, ReplicationMode::Standby);
    let (epoch, seq) = partial.replication_election_position().unwrap();
    assert_eq!((epoch, seq), (0, cut));
    // The full old-epoch voter refuses the partial candidate. Previously its
    // tuple incorrectly became (1, cut), outranking (0, end).
    assert!(!full.consider_vote(2, "partial", epoch, seq).unwrap());
    for frame in &frames[2..] {
        partial.apply_replication_frame(frame).unwrap();
    }
    assert_eq!(partial.last_applied_commit_seq(), end);
    assert!(partial
        .qualify_replication_data_epoch("successor", 1, end)
        .unwrap());
    assert_eq!(partial.replication_election_position().unwrap(), (1, end));
    assert!(full.consider_vote(3, "partial", 1, end).unwrap());
    let mut conflict = starts;
    conflict.insert(1, end - 1);
    let error = partial
        .record_primary("other", 1, None, &conflict)
        .unwrap_err();
    assert!(error
        .to_string()
        .contains("conflicts with qualified data lineage"));
    // Legitimate newer-epoch ordering still beats an old unacknowledged suffix.
    assert!(!partial
        .consider_vote(4, "old-longer", 0, end + 100)
        .unwrap());
    assert_eq!(partial.replication_election_position().unwrap(), (1, end));
    drop(partial);
    let partial = open(&partial_path, ReplicationMode::Standby);
    assert_eq!(partial.replication_election_position().unwrap(), (1, end));
}

#[test]
fn abandoned_announced_epoch_boundaries_need_not_be_monotonic() {
    let root = tempfile::tempdir().unwrap();
    let p = source(&root.path().join("p"));
    let frames = p
        .replication_frames_after(&mut ReplicationCursor::new(0), 100)
        .unwrap();
    let end = p.last_applied_commit_seq();
    let mut e = open(&root.path().join("e"), ReplicationMode::Standby);
    let mut d = open(&root.path().join("d"), ReplicationMode::Standby);
    for frame in &frames[..2] {
        e.apply_replication_frame(frame).unwrap();
        d.apply_replication_frame(frame).unwrap();
    }
    let short = e.last_applied_commit_seq();
    let announced = [(1, end)].into_iter().collect();
    e.record_primary("abandoned", 1, None, &announced).unwrap();
    d.record_primary("abandoned", 1, None, &announced).unwrap();
    assert!(!e
        .qualify_replication_data_epoch("abandoned", 1, short)
        .unwrap());
    assert_eq!(e.begin_candidacy("e").unwrap(), 2);
    e.promote_by_election(2, "e", None).unwrap();
    let starts = e.ha_node_status().epoch_starts;
    assert_eq!(starts.get(&1), Some(&end));
    assert_eq!(starts.get(&2), Some(&short));
    d.record_primary("e", 2, None, &starts).unwrap();
    assert!(d.qualify_replication_data_epoch("e", 2, short).unwrap());
    assert_eq!(d.replication_election_position().unwrap(), (2, short));
}

#[test]
fn legacy_unqualified_epoch_is_not_blessed_on_reopen_or_heartbeat() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("legacy");
    let mut db = open(&path, ReplicationMode::Standby);
    let p = source(&root.path().join("source"));
    for frame in p
        .replication_frames_after(&mut ReplicationCursor::new(0), 100)
        .unwrap()
    {
        db.apply_replication_frame(&frame).unwrap();
    }
    let applied = db.last_applied_commit_seq();
    drop(db);
    let file = path.join(HA_NODE_FILE);
    let mut legacy: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&file).unwrap()).unwrap();
    legacy.as_object_mut().unwrap().remove("data_lineage");
    legacy["data_epoch"] = json!(7); // Actual protocol-4 mislabelled-state shape.
    legacy["epoch"] = json!(7);
    legacy["epoch_starts"] = json!({"7": applied + 100});
    std::fs::write(&file, serde_json::to_vec(&legacy).unwrap()).unwrap();
    let db = open(&path, ReplicationMode::Standby);
    assert!(db.replication_election_position().is_err());
    assert_eq!(db.replication_resume_position().unwrap(), None);
    assert!(db.begin_candidacy("legacy").is_err());
    assert!(db.consider_vote(8, "candidate", 8, applied).is_err());
    assert!(db.promote_by_election(8, "legacy", None).is_err());
    db.record_primary("source", 8, None, &[(8, 0)].into_iter().collect())
        .unwrap();
    assert!(db
        .qualify_replication_data_epoch("source", 8, applied)
        .is_err());
    let backup = root.path().join("legacy-backup");
    assert!(db
        .create_standby_snapshot(&backup, "legacy-test-key")
        .is_err());
    drop(db);
    // A genuine qualified snapshot can replace legacy *data* while preserving
    // the existing local ballot/fencing history. No JSON edit grants authority.
    let backup = root.path().join("qualified-backup");
    p.create_standby_snapshot(&backup, "qualified-test-key")
        .unwrap();
    stage_standby_bootstrap(&path, &backup, "qualified-test-key", true).unwrap();
    let db = open(&path, ReplicationMode::Standby);
    assert_eq!(db.replication_election_position().unwrap(), (0, applied));
    assert_eq!(db.replication_epoch(), 8);
}

#[test]
fn missing_ha_state_in_a_populated_directory_is_not_fresh_genesis() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("used");
    let db = source(&path);
    drop(db);
    std::fs::remove_file(path.join(HA_NODE_FILE)).unwrap();
    let db = open(&path, ReplicationMode::Standby);
    assert!(db.replication_election_position().is_err());
    assert!(db.begin_candidacy("used").is_err());
    assert!(db.consider_vote(1, "other", 0, 0).is_err());
}

#[test]
fn unknown_qualification_publication_fences_until_reopen_validates_the_prefix() {
    use bicdb_page::test_faults::arm_io_failure;
    let root = tempfile::tempdir().unwrap();
    let p = source(&root.path().join("p"));
    let frames = p
        .replication_frames_after(&mut ReplicationCursor::new(0), 100)
        .unwrap();
    let mut leader = open(&root.path().join("leader"), ReplicationMode::Standby);
    let path = root.path().join("replica");
    let mut replica = open(&path, ReplicationMode::Standby);
    for frame in &frames {
        leader.apply_replication_frame(frame).unwrap();
        replica.apply_replication_frame(frame).unwrap();
    }
    leader.promote_by_election(1, "leader", None).unwrap();
    replica
        .record_primary("leader", 1, None, &leader.ha_node_status().epoch_starts)
        .unwrap();
    let applied = replica.last_applied_commit_seq();
    let fault = arm_io_failure("ha_node.dir_sync", &path, 1, false);
    let error = replica
        .qualify_replication_data_epoch("leader", 1, applied)
        .unwrap_err();
    assert_eq!(fault.fired(), 1);
    assert!(error.is_durability_failure());
    assert_eq!(replica.ha_node_status().data_epoch, 0);
    assert!(replica.replication_election_position().is_err());
    assert!(replica.replication_resume_position().is_err());
    assert!(replica.begin_candidacy("replica").is_err());
    drop(fault);
    drop(replica);
    let replica = open(&path, ReplicationMode::Standby);
    assert_eq!(
        replica.replication_election_position().unwrap(),
        (1, applied)
    );
}

#[test]
fn snapshot_install_refuses_a_later_ballot_and_fresh_staging_preserves_it() {
    let root = tempfile::tempdir().unwrap();
    let p_path = root.path().join("p");
    let p = source(&p_path);
    let path = root.path().join("target");
    let target = open(&path, ReplicationMode::Standby);
    let archive = root.path().join("first-snapshot");
    p.create_standby_snapshot(&archive, "ballot-staging-key")
        .unwrap();
    stage_standby_bootstrap(&path, &archive, "ballot-staging-key", true).unwrap();
    let staging = bicdb_core::standby_bootstrap_staging(&path);
    // Boundary test, not an old executable run: old511 recognizes only the
    // unversioned name and therefore cannot treat this stage as ready.
    assert!(!staging.join("BOOTSTRAP_READY").exists());
    assert!(staging.join("BOOTSTRAP_READY_V2").exists());
    std::fs::rename(
        staging.join("BOOTSTRAP_READY_V2"),
        staging.join("BOOTSTRAP_READY"),
    )
    .unwrap();
    let legacy_error = bicdb_core::finish_standby_bootstrap(&path).unwrap_err();
    assert!(legacy_error
        .to_string()
        .contains("legacy bootstrap ready state is unqualified"));
    assert!(staging.join("BOOTSTRAP_READY").exists());
    std::fs::rename(
        staging.join("BOOTSTRAP_READY"),
        staging.join("BOOTSTRAP_READY_V2"),
    )
    .unwrap();
    let metadata: bicdb_core::FormatMetadata = serde_json::from_slice(
        &std::fs::read(path.join(bicdb_core::FORMAT_METADATA_FILE)).unwrap(),
    )
    .unwrap();
    assert!(metadata
        .feature_flags
        .contains("ha_qualified_data_epoch_v1"));
    assert!(target.consider_vote(7, "candidate", 0, 0).unwrap());
    let ballot = target.ha_node_status().voted.clone();
    let original = std::fs::read(path.join(HA_NODE_FILE)).unwrap();
    drop(target);
    let error = bicdb_core::finish_standby_bootstrap(&path).unwrap_err();
    assert!(error
        .to_string()
        .contains("target HA state changed after snapshot staging"));
    assert_eq!(std::fs::read(path.join(HA_NODE_FILE)).unwrap(), original);
    assert!(staging.join("BOOTSTRAP_READY_V2").exists());
    // A genuine newer source snapshot/restage observes the actual latest
    // target ballot. It does not silently merge or replace the failed stage.
    p.promote_by_election(8, "p", None).unwrap();
    drop(p);
    let p = open(&p_path, ReplicationMode::Primary);
    let fresh_archive = root.path().join("fresh-snapshot");
    p.create_standby_snapshot(&fresh_archive, "ballot-staging-key")
        .unwrap();
    stage_standby_bootstrap(&path, &fresh_archive, "ballot-staging-key", true).unwrap();
    let target = open(&path, ReplicationMode::Standby);
    assert_eq!(target.ha_node_status().voted, ballot);
    assert_eq!(target.replication_election_term(), 8);
    assert_eq!(target.replication_election_position().unwrap().0, 8);
    assert!(target.get("items", "r7").unwrap().is_some());
    assert!(!target.consider_vote(7, "other", 8, 100).unwrap());
}

#[test]
fn snapshot_recovers_the_original_preimage_between_actual_directory_renames() {
    let root = tempfile::tempdir().unwrap();
    let p = source(&root.path().join("p"));
    let path = root.path().join("target");
    let target = open(&path, ReplicationMode::Standby);
    assert!(target.consider_vote(3, "candidate", 0, 0).unwrap());
    let ballot = target.ha_node_status().voted;
    drop(target);
    let archive = root.path().join("snapshot");
    p.create_standby_snapshot(&archive, "between-renames-key")
        .unwrap();
    stage_standby_bootstrap(&path, &archive, "between-renames-key", true).unwrap();
    // Actual first directory rename, followed by the real install recovery
    // entry. This is a controlled filesystem cut, not a SIGKILL/power-loss run.
    let replaced = root.path().join("target.replaced");
    std::fs::rename(&path, &replaced).unwrap();
    assert!(!path.exists());
    assert!(bicdb_core::finish_standby_bootstrap(&path).unwrap());
    assert!(!replaced.exists());
    let target = open(&path, ReplicationMode::Standby);
    assert_eq!(target.ha_node_status().voted, ballot);
    assert_eq!(target.replication_election_term(), 3);
    assert!(target.get("items", "r7").unwrap().is_some());
}

#[test]
fn higher_durable_ballot_fences_old_stream_before_apply_and_before_ack_eligibility() {
    let root = tempfile::tempdir().unwrap();
    let p = source(&root.path().join("p"));
    let frames = p
        .replication_frames_after(&mut ReplicationCursor::new(0), 100)
        .unwrap();
    let mut target = open(&root.path().join("target"), ReplicationMode::Standby);
    for frame in &frames[..2] {
        target.apply_replication_frame(frame).unwrap();
    }
    let before = target.last_applied_commit_seq();
    target
        .record_primary("p", 0, None, &Default::default())
        .unwrap();
    assert!(target.consider_vote(1, "candidate", 0, before).unwrap());
    // Same necessary predicate is used by Ping before renewing contact and by
    // record_primary before the wire handler applies any buffered frames.
    assert!(target.require_replication_primary_epoch(0).is_err());
    assert!(target
        .record_primary("p", 0, None, &Default::default())
        .is_err());
    // A real frame application models a vote racing *after* that early check.
    target.apply_replication_frame(&frames[2]).unwrap();
    let after = target.last_applied_commit_seq();
    assert!(after > before);
    assert!(target
        .qualify_replication_data_epoch("p", 0, after)
        .is_err());
    assert_eq!(target.replication_epoch(), 0);
    assert_eq!(target.replication_election_position().unwrap(), (0, after));
    assert_eq!(target.ha_node_status().voted, Some((1, "candidate".into())));
    // Conversely, if apply wins before a later vote, the old shorter candidate
    // is refused against the actual applied/durable prefix.
    assert!(!target.consider_vote(2, "shorter", 0, before).unwrap());
    assert_eq!(target.replication_election_term(), 2);
    assert_eq!(target.ha_node_status().data_epoch, 0);
}

#[test]
fn exact_durable_switchover_offer_replays_but_a_later_term_refuses_it() {
    let root = tempfile::tempdir().unwrap();
    let p = source(&root.path().join("p"));
    let offer = p.drain_for_switchover("target").unwrap();
    assert_eq!(p.drain_for_switchover("target").unwrap(), offer);
    assert!(p.drain_for_switchover("different").is_err());
    p.observe_election_term(offer.target_epoch + 1).unwrap();
    assert!(p.drain_for_switchover("target").is_err());
    assert_eq!(p.planned_switchover(), Some(offer.clone()));
    assert_eq!(p.replication_epoch(), offer.source_epoch);
    assert_eq!(p.ha_node_status().data_epoch, offer.source_epoch);
}

#[test]
fn public_restage_cannot_bypass_an_installed_pending_bootstrap() {
    let root = tempfile::tempdir().unwrap();
    let p = source(&root.path().join("p"));
    let path = root.path().join("target");
    let target = open(&path, ReplicationMode::Standby);
    drop(target);
    let archive = root.path().join("snapshot");
    p.create_standby_snapshot(&archive, "pending-installed-key")
        .unwrap();
    stage_standby_bootstrap(&path, &archive, "pending-installed-key", true).unwrap();
    let staging = bicdb_core::standby_bootstrap_staging(&path);
    let replaced = root.path().join("target.replaced");
    // Both actual rename boundaries, before the install completion sync.
    std::fs::rename(&path, &replaced).unwrap();
    std::fs::rename(&staging, &path).unwrap();
    let original = std::fs::read(replaced.join(HA_NODE_FILE)).unwrap();
    let ready = std::fs::read(path.join("BOOTSTRAP_READY_V2")).unwrap();
    let error =
        stage_standby_bootstrap(&path, &archive, "pending-installed-key", true).unwrap_err();
    assert!(error
        .to_string()
        .contains("recover the installed bootstrap before staging"));
    assert_eq!(
        std::fs::read(replaced.join(HA_NODE_FILE)).unwrap(),
        original
    );
    assert_eq!(
        std::fs::read(path.join("BOOTSTRAP_READY_V2")).unwrap(),
        ready
    );
    assert!(!staging.exists());
    std::fs::rename(
        path.join("BOOTSTRAP_READY_V2"),
        path.join("BOOTSTRAP_READY"),
    )
    .unwrap();
    assert!(stage_standby_bootstrap(&path, &archive, "pending-installed-key", true).is_err());
    assert_eq!(
        std::fs::read(replaced.join(HA_NODE_FILE)).unwrap(),
        original
    );
    std::fs::rename(
        path.join("BOOTSTRAP_READY"),
        path.join("BOOTSTRAP_READY_V2"),
    )
    .unwrap();
    assert!(bicdb_core::finish_standby_bootstrap(&path).unwrap());
    let target = open(&path, ReplicationMode::Standby);
    assert!(target.get("items", "r7").unwrap().is_some());
}

#[test]
fn refused_short_candidate_fences_primary_then_full_prefix_can_be_reelected_and_rejoined() {
    use bicdb_core::NodeRole;
    let root = tempfile::tempdir().unwrap();
    let p_path = root.path().join("p");
    let p = source(&p_path);
    let frames = p
        .replication_frames_after(&mut ReplicationCursor::new(0), 100)
        .unwrap();
    let prefix = p.replication_election_position().unwrap();
    let mut d = open(&root.path().join("d"), ReplicationMode::Standby);
    let mut c = open(&root.path().join("c"), ReplicationMode::Standby);
    for frame in &frames {
        c.apply_replication_frame(frame).unwrap();
    }
    let term = d.begin_candidacy("d").unwrap();
    let short = d.replication_election_position().unwrap();
    let authority = p.replication_writer_authority();
    let original_primary = p.ha_node_status().primary_id;
    assert!(authority.active());
    assert!(!p.consider_vote(term, "d", short.0, short.1).unwrap());
    assert_eq!(p.replication_role(), NodeRole::Fenced);
    assert!(!authority.active());
    assert_eq!(p.replication_election_term(), term);
    assert_eq!(p.ha_node_status().primary_id, original_primary);
    assert_eq!(p.replication_epoch(), 0);
    assert_eq!(p.replication_election_position().unwrap(), prefix);
    assert!(p.ha_node_status().voted.is_none());
    drop(p);
    let p = open(&p_path, ReplicationMode::Primary);
    assert_eq!(p.replication_role(), NodeRole::Fenced);
    p.demote_to_standby().unwrap(); // Actual normal restart transition.
    let next = p.begin_candidacy("p").unwrap();
    assert_eq!(next, term + 1);
    assert!(d.consider_vote(next, "p", prefix.0, prefix.1).unwrap());
    assert!(c.consider_vote(next, "p", prefix.0, prefix.1).unwrap());
    assert_eq!(p.ha_node_status().voted, Some((next, "p".into())));
    p.promote_by_election(next, "p", None).unwrap();
    drop(p);
    let p = open(&p_path, ReplicationMode::Primary);
    let starts = p.ha_node_status().epoch_starts;
    d.record_primary("p", next, None, &starts).unwrap();
    for frame in p
        .replication_frames_after(&mut ReplicationCursor::new(0), 100)
        .unwrap()
    {
        d.apply_replication_frame(&frame).unwrap();
    }
    assert!(d
        .qualify_replication_data_epoch("p", next, prefix.1)
        .unwrap());
    assert_eq!(d.replication_election_position().unwrap(), (next, prefix.1));
    assert_eq!(
        d.get("items", "r7").unwrap().unwrap().metadata,
        p.get("items", "r7").unwrap().unwrap().metadata
    );
}

#[test]
fn higher_term_fence_revokes_the_live_writer_before_native_state_io_failure() {
    use bicdb_page::test_faults::arm_io_failure;
    for consider in [false, true] {
        for step in ["write", "dir_sync"] {
            let root = tempfile::tempdir().unwrap();
            let p = source(&root.path().join("p"));
            let authority = p.replication_writer_authority();
            assert!(authority.active());
            let fault = arm_io_failure(&format!("ha_node.{step}"), p.data_path(), 1, false);
            let result = if consider {
                p.consider_vote(1, "short", 0, 0).map(|_| ())
            } else {
                p.observe_election_term(1)
            };
            assert!(result.is_err());
            assert_eq!(fault.fired(), 1);
            assert!(!authority.active());
            assert_eq!(p.replication_epoch(), 0);
            assert_eq!(p.ha_node_status().data_epoch, 0);
            assert!(p.ha_node_status().voted.is_none());
            let write: bicdb_core::Result<()> = (|| {
                let mut tx = p.begin_transaction()?;
                tx.insert("items", Record::new("after-failed-fence"))?;
                tx.commit()
            })();
            assert!(write.is_err());
        }
    }
}
