use super::*;
use crate::db::{CommitAdmissionIntent, CommitAdmissionMutation};
use crate::distribution_native_read::{NativeRangeReadRequest, NativeRangeReadScope};
use crate::distribution_range_consensus::{
    native_origin_fixture, native_ready_test_command, RangeWriteCoordinator, RangeWriteTransport,
};
use crate::distribution_range_recovery::RecoveryMemberIdentity;
use crate::{DbConfig, DistributionConfig, DistributionStore, Record, StorageMode};
use parking_lot::Mutex;

#[derive(Debug)]
struct NoPeers;
impl RangeWriteTransport for NoPeers {
    fn range_write_progress(
        &self,
        _: &ClusterNodeId,
        _: RangeId,
        _: u64,
    ) -> Result<RangeWriteProgress> {
        unreachable!("RF1 has no remote voter")
    }
    fn apply_range_write_repair(
        &self,
        _: &ClusterNodeId,
        _: &RangeWriteRepairBatch,
        _: &RangeWriteRepairLimits,
    ) -> Result<RangeWriteProgress> {
        unreachable!("RF1 has no remote voter")
    }
    fn fetch_range_write_repair(
        &self,
        _: &ClusterNodeId,
        _: RangeId,
        _: u64,
        _: u64,
        _: &RangeWriteRepairLimits,
    ) -> Result<RangeWriteRepairBatch> {
        unreachable!("RF1 has no remote voter")
    }
    fn probe_range_write(
        &self,
        _: &ClusterNodeId,
        _: RangeId,
        _: u64,
        _: u64,
    ) -> Result<crate::RangeWriteProbe> {
        unreachable!("RF1 has no remote voter")
    }
    fn prepare_range_write(
        &self,
        _: &ClusterNodeId,
        _: &RangeWriteCommand,
    ) -> Result<RangeWriteAck> {
        unreachable!("RF1 has no remote voter")
    }
    fn commit_range_write(
        &self,
        _: &ClusterNodeId,
        _: &RangeWriteCommand,
    ) -> Result<RangeWriteAck> {
        unreachable!("RF1 has no remote voter")
    }
    fn certify_range_write(
        &self,
        _: &ClusterNodeId,
        _: &RangeWriteCommand,
    ) -> Result<RangeWriteAck> {
        unreachable!("RF1 has no remote voter")
    }
    fn apply_range_write(&self, _: &ClusterNodeId, _: &RangeWriteCommand) -> Result<RangeWriteAck> {
        unreachable!("RF1 has no remote voter")
    }
    fn abort_range_write(&self, _: &ClusterNodeId, _: &RangeWriteCommand) -> Result<RangeWriteAck> {
        unreachable!("RF1 has no remote voter")
    }
}
fn command(topology: &crate::ClusterTopology, key: &str) -> RangeWriteCommand {
    let range = topology.range_for_key("items", key).unwrap();
    native_ready_test_command(
        topology.cluster_id.clone(),
        range,
        1,
        range.leader.clone(),
        &CommitAdmissionIntent {
            transaction_id: 99121,
            mutations: vec![CommitAdmissionMutation {
                collection: "items".into(),
                record_id: key.into(),
                record: Some(Record::new(key)),
            }],
        },
    )
    .unwrap()
}
fn request(
    topology: &crate::ClusterTopology,
    spec: &crate::db::RangeImageBirthSpec,
    range: &RangeDescriptor,
) -> NativeRangeReadRequest {
    NativeRangeReadRequest {
        protocol_version: crate::distribution_native_read::NATIVE_RANGE_READ_VERSION,
        scope: NativeRangeReadScope {
            cluster_id: topology.cluster_id.clone(),
            bootstrap_id: spec.bootstrap_id,
            range: range.clone(),
            schema_sha256: range
                .required_schema_sha256(&topology.nodes)
                .unwrap()
                .into(),
            restore_activation: spec.restore_activation.clone(),
            members: spec
                .initial_members
                .iter()
                .map(|(id, m)| {
                    (
                        id.clone(),
                        RecoveryMemberIdentity {
                            incarnation: m.incarnation,
                            active_certificate_sha256: m.tls_certificate_sha256.clone(),
                            pending_certificate_sha256: m.pending_tls_certificate_sha256.clone(),
                        },
                    )
                })
                .collect(),
        },
        challenge: Uuid::now_v7(),
        budget_ms: 5000,
    }
}
#[test]
fn native_catalog_fsync_false_born_service_refuses_before_catalogs_or_native_changes() {
    let (_root, db, topology, spec) = native_origin_fixture();
    let path = db.read().data_path().to_path_buf();
    let range = topology.ranges.values().next().unwrap();
    assert!(!path
        .join(crate::distribution_range_consensus::DEFAULT_RANGE_WRITE_LOG)
        .exists());
    assert!(!path.join(DEFAULT_RANGE_LEARNER_APPLY_STATE).exists());
    let error = ClusterDataNodeService::open(
        topology.cluster_id.clone(),
        spec.node_id.clone(),
        &path,
        db.clone(),
        false,
    )
    .unwrap_err();
    assert!(error.to_string().contains("requires fsync=true"), "{error}");
    assert!(!path
        .join(crate::distribution_range_consensus::DEFAULT_RANGE_WRITE_LOG)
        .exists());
    assert!(!path.join(DEFAULT_RANGE_LEARNER_APPLY_STATE).exists());
    assert!(!path.join(DEFAULT_RANGE_DIGEST_STATE_DIR).exists());
    assert!(!path.join(DEFAULT_RANGE_DIGEST_REPAIR_STATE_DIR).exists());
    assert_eq!(
        db.read()
            .current_native_range_source_lineage(range.id)
            .unwrap()
            .range_index(),
        0
    );
}
#[test]
fn native_catalog_fsync_true_real_origin_ack_and_local_observation_remain_available() {
    let (_root, db, topology, spec) = native_origin_fixture();
    let service = Arc::new(Mutex::new(
        ClusterDataNodeService::open(
            topology.cluster_id.clone(),
            spec.node_id.clone(),
            db.read().data_path(),
            db.clone(),
            true,
        )
        .unwrap(),
    ));
    let coor = Arc::new(
        RangeWriteCoordinator::new_schema_fenced(
            topology.cluster_id.clone(),
            spec.node_id.clone(),
            Arc::new(RwLock::new(topology.clone())),
            service.clone(),
            Arc::new(NoPeers),
        )
        .unwrap(),
    );
    coor.recover_local_leadership(8).unwrap();
    db.read().install_commit_admission(coor.clone());
    let expected =
        Record::new("native-fsync-positive").with_metadata(serde_json::json!({"payload":1}));
    {
        let database = db.write();
        let mut tx = database.begin_transaction().unwrap();
        tx.insert("items", expected.clone()).unwrap();
        tx.commit().unwrap();
    }
    let range = topology.range_for_key("items", &expected.id).unwrap();
    let observed = service
        .lock()
        .observe_current_native_range(
            &request(&topology, &spec, range),
            &CancellationToken::uncancelable(),
        )
        .unwrap();
    assert_eq!(observed.facts().range_index, 1);
    observed.require_current(&service.lock()).unwrap();
    assert_eq!(
        service
            .lock()
            .range_write_progress(range.id)
            .resolved_through,
        1
    );
    assert_eq!(
        *db.read().get("items", &expected.id).unwrap().unwrap(),
        expected
    );
    db.read().clear_commit_admission();
    drop(coor);
}
#[test]
fn native_catalog_separate_root_refuses_before_any_foreign_catalog_creation() {
    let (_root, db, topology, spec) = native_origin_fixture();
    let foreign = tempfile::tempdir().unwrap();
    std::fs::write(foreign.path().join("canary"), b"unchanged").unwrap();
    let error = ClusterDataNodeService::open(
        topology.cluster_id.clone(),
        spec.node_id,
        foreign.path(),
        db,
        true,
    )
    .unwrap_err();
    assert!(error.to_string().contains("root differs"), "{error}");
    assert_eq!(
        std::fs::read(foreign.path().join("canary")).unwrap(),
        b"unchanged"
    );
    assert_eq!(std::fs::read_dir(foreign.path()).unwrap().count(), 1);
}
#[test]
fn native_catalog_lower_native_proof_guards_refuse_false_fsync_without_mutation() {
    let (_root, db, topology, spec) = native_origin_fixture();
    let database = db.read();
    let range = topology.ranges.values().next().unwrap();
    // Real lower store, owned once: it is not a service ACK/quorum proof.
    let store = RangeWriteStore::open(
        database.data_path(),
        topology.cluster_id.clone(),
        spec.node_id.clone(),
        false,
    )
    .unwrap();
    let cmd = command(&topology, "native-lower-fsync-negative");
    assert!(store
        .normal_native_write_allowed(&cmd)
        .unwrap_err()
        .to_string()
        .contains("fsync=true"));
    let native = database
        .native_recovery_admission(
            &topology.cluster_id,
            spec.bootstrap_id,
            &spec.node_id,
            spec.member_incarnation,
        )
        .unwrap();
    let read = request(&topology, &spec, range);
    assert!(store
        .observe_native_read_catalogue(&read.scope, 0, database.data_path(), &native)
        .unwrap_err()
        .to_string()
        .contains("fsync=true"));
    assert!(!database
        .data_path()
        .join(crate::distribution_range_consensus::DEFAULT_RANGE_WRITE_LOG)
        .exists());
    assert_eq!(
        database
            .current_native_range_source_lineage(range.id)
            .unwrap()
            .range_index(),
        0
    );
}
#[test]
fn native_catalog_existing_legacy_async_service_refuses_cq_after_actual_format_activation() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("database");
    let mut database = BicDb::open_with_config(
        &path,
        DbConfig::default()
            .with_fsync(true)
            .with_storage_mode(StorageMode::ServerPaged)
            .with_paged_buffer_pool_bytes(8 * 1024 * 1024),
    )
    .unwrap();
    database.create_collection("items").unwrap();
    let db = Arc::new(RwLock::new(database));
    let topology = DistributionStore::initialize_at(
        root.path().join("metadata"),
        DistributionConfig {
            enabled: true,
            initial_ranges: 1,
            replication_factor: 1,
            ..Default::default()
        },
        true,
        1,
    )
    .unwrap()
    .topology()
    .clone();
    let range = topology.ranges.values().next().unwrap();
    let mut service = ClusterDataNodeService::open(
        topology.cluster_id.clone(),
        range.leader.clone(),
        &path,
        db.clone(),
        false,
    )
    .unwrap();
    let cmd = command(&topology, "legacy-before-activation");
    assert_eq!(
        service
            .prepare_range_write(range, &range.leader, cmd.clone())
            .unwrap()
            .state,
        RangeWriteState::Prepared
    );
    assert_eq!(
        service
            .commit_range_write_decision(range, &range.leader, &cmd)
            .unwrap()
            .state,
        RangeWriteState::Committed
    );
    let before =
        std::fs::read(path.join(crate::distribution_range_consensus::DEFAULT_RANGE_WRITE_LOG))
            .unwrap();
    crate::format::enable_range_image_install(&path).unwrap();
    assert!(crate::format::range_image_install_active(&path).unwrap());
    assert!(
        !db.read().native_range_image_birth_present(),
        "feature activation alone does not invent native birth"
    );
    let error = service
        .certify_range_write(range, &range.leader, &cmd)
        .unwrap_err();
    assert!(error.to_string().contains("fsync=true"), "{error}");
    assert_eq!(
        std::fs::read(path.join(crate::distribution_range_consensus::DEFAULT_RANGE_WRITE_LOG))
            .unwrap(),
        before
    );
    assert_eq!(
        service.range_writes.state(&cmd).unwrap(),
        RangeWriteState::Committed
    );
    assert!(ClusterDataNodeService::open(
        topology.cluster_id.clone(),
        range.leader.clone(),
        &path,
        db,
        false
    )
    .is_err());
}
#[test]
fn native_catalog_explicit_legacy_async_prepare_commit_certify_stays_supported() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("database");
    let mut database =
        BicDb::open_with_config(&path, DbConfig::default().with_fsync(false)).unwrap();
    database.create_collection("items").unwrap();
    let db = Arc::new(RwLock::new(database));
    let topology = DistributionStore::initialize_at(
        root.path().join("metadata"),
        DistributionConfig {
            enabled: true,
            initial_ranges: 1,
            replication_factor: 1,
            ..Default::default()
        },
        false,
        1,
    )
    .unwrap()
    .topology()
    .clone();
    let range = topology.ranges.values().next().unwrap();
    let mut service = ClusterDataNodeService::open(
        topology.cluster_id.clone(),
        range.leader.clone(),
        &path,
        db,
        false,
    )
    .unwrap();
    let cmd = command(&topology, "explicit-legacy-async");
    assert_eq!(
        service
            .prepare_range_write(range, &range.leader, cmd.clone())
            .unwrap()
            .state,
        RangeWriteState::Prepared
    );
    assert_eq!(
        service
            .commit_range_write_decision(range, &range.leader, &cmd)
            .unwrap()
            .state,
        RangeWriteState::Committed
    );
    assert_eq!(
        service
            .certify_range_write(range, &range.leader, &cmd)
            .unwrap()
            .state,
        RangeWriteState::QuorumCommitted
    );
}

#[test]
fn native_service_actual_open_cannot_be_revived_by_outer_arc_or_retained_directory() {
    let (_seed_root, _seed_db, topology, spec) = native_origin_fixture();
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("enforced");
    let config = DbConfig::default()
        .with_fsync(true)
        .with_storage_mode(StorageMode::ServerPaged)
        .with_paged_buffer_pool_bytes(8 * 1024 * 1024);
    let (mut original, _) =
        BicDb::create_new_enrolled_range_image_database(&path, config, spec.clone()).unwrap();
    original.create_collection("items").unwrap();
    let directory = original
        .native_service_directory_lifetime()
        .unwrap()
        .upgrade()
        .unwrap();
    let db = Arc::new(RwLock::new(original));
    let service = ClusterDataNodeService::open(
        topology.cluster_id.clone(),
        spec.node_id,
        &path,
        Arc::clone(&db),
        true,
    )
    .unwrap();
    let guard = service.listener_open_guard();
    assert_eq!(service.native_writer_enforcement_version().unwrap(), 1);
    guard.require_database(&db.read()).unwrap();

    let replacement = BicDb::open_with_config(
        root.path().join("foreign-legacy"),
        DbConfig::default().with_fsync(true),
    )
    .unwrap();
    // Even while A is still alive, its guard cannot authorize actual B.
    assert!(guard.require_database(&replacement).is_err());
    let old = std::mem::replace(&mut *db.write(), replacement);
    guard.require_live().unwrap();
    assert!(guard.require_database(&db.read()).is_err());
    assert!(service
        .schema_compatibility_fingerprint()
        .unwrap_err()
        .to_string()
        .contains("another actual database/open"));
    assert!(checked_database_write(&db, &service.storage_policy).is_err());
    drop(old);

    // Neither the same outer Arc nor a deliberately retained old directory
    // lock owns the old BicDb lifetime token. All necessary refusal checks
    // must now fail without reading or acquiring the replacement database.
    assert!(Arc::strong_count(&directory) > 0);
    for result in [
        guard.require_live(),
        guard.require_database(&db.read()),
        service.require_catalog_storage_policy(),
    ] {
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("actual database open ended"));
    }
    assert!(service.native_writer_enforcement_version().is_err());
    assert!(service
        .storage_policy
        .validate_configuration(&path, true)
        .is_err());
    assert_eq!(
        db.read()
            .actual_native_writer_enforcement_version()
            .unwrap(),
        0
    );
}

#[test]
fn native_service_open_guard_does_not_wait_for_service_or_database_latches() {
    let (_root, db, topology, spec) = native_origin_fixture();
    let path = db.read().data_path().to_path_buf();
    let service = Mutex::new(
        ClusterDataNodeService::open(
            topology.cluster_id,
            spec.node_id,
            &path,
            Arc::clone(&db),
            true,
        )
        .unwrap(),
    );
    let guard = service.lock().listener_open_guard();
    let database_latch = db.write();
    let service_latch = service.lock();
    let (send, receive) = std::sync::mpsc::sync_channel(1);
    let worker = std::thread::spawn(move || send.send(guard.require_live()).unwrap());
    let result = receive.recv_timeout(std::time::Duration::from_secs(2));
    drop(service_latch);
    drop(database_latch);
    worker.join().unwrap();
    result
        .expect("open fence must not reacquire either held latch")
        .unwrap();
}
