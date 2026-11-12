//! Actual native DB/page-WAL and metadata files. Orderly closure + explicit
//! file-loss/error cuts, not SIGKILL, media rollback, physical HA or wire proof.
use super::*;
use crate::db::ActualNativeRf3Fixture;
use crate::{DbConfig, StorageMode};
fn node(name: &str) -> ClusterNodeId {
    ClusterNodeId::new(name).unwrap()
}
fn fixture() -> ActualNativeRf3Fixture {
    let mut f = ActualNativeRf3Fixture::new_with_enforced_network_and_ranges(&BTreeMap::new(), 1);
    f.root.disable_cleanup(true);
    eprintln!("NATIVE_METADATA_OWNER_ROOT {}", f.root.path().display());
    f
}
fn initialize(f: &ActualNativeRf3Fixture) -> MetadataConsensusStore {
    MetadataConsensusStore::initialize_native_owned(
        f.root.path().join("native-meta-n0"),
        Arc::clone(&f.databases[&node("n0")]),
        &f.fresh_factories[&node("n0")],
        f.topology.read().clone(),
        &CancellationToken::uncancelable(),
    )
    .unwrap()
}
fn reopen(f: &ActualNativeRf3Fixture) -> Result<MetadataConsensusStore> {
    MetadataConsensusStore::reopen_native_owned(
        f.root.path().join("native-meta-n0"),
        Arc::clone(&f.databases[&node("n0")]),
        node("n0"),
        f.topology.read().clone(),
        &CancellationToken::uncancelable(),
    )
}
#[test]
fn native_metadata_owner_preserves_term_vote_through_actual_db_cold_reopen() {
    let mut f = fixture();
    let topology = f.topology.read().clone();
    let spec = f.specs[&node("n0")].clone();
    let mut store = initialize(&f);
    let response = store
        .handle_vote_request(MetadataVoteRequest {
            cluster_id: topology.cluster_id.clone(),
            term: 7,
            candidate_id: node("n1"),
            last_log_index: 0,
            last_log_term: 0,
        })
        .unwrap();
    assert!(response.vote_granted);
    assert_eq!(store.status().voted_for, Some(node("n1")));
    let saved = fs::read(
        f.root
            .path()
            .join("native-meta-n0")
            .join(DEFAULT_CLUSTER_METADATA_CONSENSUS_STATE),
    )
    .unwrap();
    // The actual Store also owns the DB; independent metadata roots cannot
    // create two local voters over one still-open native database.
    assert!(MetadataConsensusStore::initialize_native_owned(
        f.root.path().join("alias-meta"),
        Arc::clone(&f.databases[&node("n0")]),
        &f.fresh_factories[&node("n0")],
        topology.clone(),
        &CancellationToken::uncancelable()
    )
    .is_err());
    drop(store);
    f.root.disable_cleanup(true);
    let root = f.root.path().to_path_buf();
    let weak = f.databases.values().map(Arc::downgrade).collect::<Vec<_>>();
    drop(f);
    assert!(weak.iter().all(|x| x.upgrade().is_none()));
    let config = DbConfig::default()
        .with_fsync(true)
        .with_storage_mode(StorageMode::ServerPaged)
        .with_paged_buffer_pool_bytes(8 * 1024 * 1024);
    let (db, _) = BicDb::open_range_image_database(&root.join("n0"), config, &spec).unwrap();
    let db = Arc::new(RwLock::new(db));
    let mut store = MetadataConsensusStore::reopen_native_owned(
        root.join("native-meta-n0"),
        Arc::clone(&db),
        node("n0"),
        topology.clone(),
        &CancellationToken::uncancelable(),
    )
    .unwrap();
    assert_eq!(store.status().current_term, 7);
    assert_eq!(store.status().voted_for, Some(node("n1")));
    assert_eq!(
        fs::read(
            root.join("native-meta-n0")
                .join(DEFAULT_CLUSTER_METADATA_CONSENSUS_STATE)
        )
        .unwrap(),
        saved
    );
    let denied = store
        .handle_vote_request(MetadataVoteRequest {
            cluster_id: topology.cluster_id,
            term: 7,
            candidate_id: node("n2"),
            last_log_index: 0,
            last_log_term: 0,
        })
        .unwrap();
    assert!(!denied.vote_granted);
    drop(store);
    drop(db);
    fs::remove_dir_all(root).unwrap();
}
#[test]
fn native_metadata_owner_missing_state_never_recreates_term_zero() {
    let f = fixture();
    let store = initialize(&f);
    drop(store);
    let path = f
        .root
        .path()
        .join("native-meta-n0")
        .join(DEFAULT_CLUSTER_METADATA_CONSENSUS_STATE);
    fs::remove_file(&path).unwrap();
    assert!(reopen(&f).unwrap_err().to_string().contains("never reset"));
    assert!(!path.exists());
    assert!(MetadataConsensusStore::open(
        f.root.path().join("native-meta-n0"),
        f.topology.read().cluster_id.clone(),
        node("n0"),
        f.topology.read().clone(),
        true
    )
    .is_err());
    assert!(!path.exists());
    assert!(f.databases[&node("n0")]
        .read()
        .require_native_metadata_uninitialized()
        .is_err());
    assert!(MetadataConsensusStore::initialize_native_owned(
        f.root.path().join("new-location"),
        Arc::clone(&f.databases[&node("n0")]),
        &f.fresh_factories[&node("n0")],
        f.topology.read().clone(),
        &CancellationToken::uncancelable()
    )
    .is_err());
}
#[test]
fn native_metadata_owner_prepared_crash_cuts_refuse_all_voter_exposure() {
    for point in [
        NativeMetadataInitializationCut::AfterProtectedPrepare,
        NativeMetadataInitializationCut::AfterStateFile,
    ] {
        let f = fixture();
        let path = f.root.path().join("native-meta-n0");
        let error = MetadataConsensusStore::test_initialize_native_owned_with(
            &path,
            Arc::clone(&f.databases[&node("n0")]),
            &f.fresh_factories[&node("n0")],
            f.topology.read().clone(),
            &CancellationToken::uncancelable(),
            |cut| {
                if cut == point {
                    Err(metadata_error(
                        "injected constructor cut before voter exposure",
                    ))
                } else {
                    Ok(())
                }
            },
        )
        .unwrap_err();
        assert!(error.to_string().contains("before voter exposure"));
        assert_eq!(
            path.join(DEFAULT_CLUSTER_METADATA_CONSENSUS_STATE).exists(),
            point == NativeMetadataInitializationCut::AfterStateFile
        );
        assert!(reopen(&f).unwrap_err().to_string().contains("incomplete"));
        assert!(f.databases[&node("n0")]
            .read()
            .require_native_metadata_uninitialized()
            .is_err());
        assert!(MetadataConsensusStore::initialize_native_owned(
            &path,
            Arc::clone(&f.databases[&node("n0")]),
            &f.fresh_factories[&node("n0")],
            f.topology.read().clone(),
            &CancellationToken::uncancelable()
        )
        .is_err());
    }
}
#[test]
fn native_metadata_owner_rejects_other_store_id_birth_and_legacy_codec_bypass() {
    let f = fixture();
    let store = initialize(&f);
    drop(store);
    let path = f
        .root
        .path()
        .join("native-meta-n0")
        .join(DEFAULT_CLUSTER_METADATA_CONSENSUS_STATE);
    let original = fs::read(&path).unwrap();
    let mut envelope: MetadataConsensusEnvelope = serde_json::from_slice(&original).unwrap();
    assert_eq!(envelope.format_version, NATIVE_METADATA_CONSENSUS_FORMAT);
    envelope.state.native_store_id = Some(Uuid::now_v7());
    envelope.checksum_sha256 = state_checksum(&envelope.state).unwrap();
    crate::storage::write_atomic(&path, &serde_json::to_vec(&envelope).unwrap(), true).unwrap();
    assert!(reopen(&f)
        .unwrap_err()
        .to_string()
        .contains("protected store identity"));
    crate::storage::write_atomic(&path, &original, true).unwrap();
    let other = fixture();
    assert!(MetadataConsensusStore::reopen_native_owned(
        f.root.path().join("native-meta-n0"),
        Arc::clone(&other.databases[&node("n0")]),
        node("n0"),
        f.topology.read().clone(),
        &CancellationToken::uncancelable()
    )
    .is_err());
    let held = reopen(&f).unwrap();
    drop(held);
    assert!(MetadataConsensusStore::open(
        f.root.path().join("native-meta-n0"),
        f.topology.read().cluster_id.clone(),
        node("n0"),
        f.topology.read().clone(),
        true
    )
    .is_err());
    assert_eq!(fs::read(&path).unwrap(), original);
    assert!(crate::format::native_metadata_store_active(
        f.databases[&node("n0")].read().data_path()
    )
    .unwrap());
    assert!(
        !crate::format::required_reader_features(f.databases[&node("n0")].read().data_path())
            .unwrap()
            .contains(&crate::format::NATIVE_METADATA_STORE_FEATURE_FLAG.to_owned())
    );
}

#[test]
fn native_metadata_owner_leaves_legacy_unbound_codec_and_open_behavior_unchanged() {
    let f = ActualNativeRf3Fixture::new();
    let topology = f.topology.read().clone();
    let root = f.root.path().join("legacy-metadata");
    let mut store = MetadataConsensusStore::open(
        &root,
        topology.cluster_id.clone(),
        node("n0"),
        topology.clone(),
        true,
    )
    .unwrap();
    store.start_election().unwrap();
    assert_eq!(store.status().current_term, 1);
    let encoded: serde_json::Value = serde_json::from_slice(
        &fs::read(root.join(DEFAULT_CLUSTER_METADATA_CONSENSUS_STATE)).unwrap(),
    )
    .unwrap();
    assert_eq!(encoded["format_version"], 1);
    assert!(encoded["state"].get("native_store_id").is_none());
    drop(store);
    let reopened = MetadataConsensusStore::open(
        &root,
        topology.cluster_id.clone(),
        node("n0"),
        topology,
        true,
    )
    .unwrap();
    assert_eq!(reopened.status().current_term, 1);
    assert!(!crate::format::native_metadata_store_active(
        f.databases[&node("n0")].read().data_path()
    )
    .unwrap());
}

#[test]
fn native_metadata_owner_refuses_replaced_inner_db_even_while_outer_arc_survives() {
    let f = fixture();
    let mut store = initialize(&f);
    let topology = f.topology.read().clone();
    let path = f
        .root
        .path()
        .join("native-meta-n0")
        .join(DEFAULT_CLUSTER_METADATA_CONSENSUS_STATE);
    let original = fs::read(&path).unwrap();
    let spec = f.specs[&node("n0")].clone();
    let config = DbConfig::default()
        .with_fsync(true)
        .with_storage_mode(StorageMode::ServerPaged)
        .with_paged_buffer_pool_bytes(8 * 1024 * 1024);
    let (replacement, _) = BicDb::create_new_enrolled_range_image_database(
        &f.root.path().join("genuine-new-birth"),
        config,
        spec,
    )
    .unwrap();
    // Hold the OLD actual directory lock deliberately. A Weak DirectoryLock
    // alone would still upgrade after the original BicDb has ended.
    let old_directory = f.databases[&node("n0")]
        .read()
        .native_service_directory_lifetime()
        .unwrap()
        .upgrade()
        .unwrap();
    let ended = std::mem::replace(&mut *f.databases[&node("n0")].write(), replacement);
    drop(ended);
    assert!(Arc::strong_count(&f.databases[&node("n0")]) > 1);
    assert!(store
        .start_election()
        .unwrap_err()
        .to_string()
        .contains("open lifetime ended"));
    assert!(store
        .handle_vote_request(MetadataVoteRequest {
            cluster_id: topology.cluster_id,
            term: 9,
            candidate_id: node("n1"),
            last_log_index: 0,
            last_log_term: 0
        })
        .is_err());
    let local = MetadataReadParticipant::from_transport(node("n0"), 1, None, true);
    assert!(store
        .begin_read_authority(Uuid::now_v7(), Duration::from_secs(1), &local)
        .unwrap_err()
        .to_string()
        .contains("open lifetime ended"));
    assert_eq!(fs::read(path).unwrap(), original);
    drop(old_directory);
}

#[test]
fn native_metadata_owner_flag_only_prepublication_cut_can_retry_actual_unwritten_factory() {
    let f = fixture();
    let db = f.databases[&node("n0")].read();
    // Models failure AFTER sticky reader fence but BEFORE any protected
    // initialization record. No Store/vote has ever been exposed at this cut.
    crate::format::enable_native_metadata_store(db.data_path()).unwrap();
    assert!(db.require_native_metadata_uninitialized().is_ok());
    drop(db);
    assert!(!f
        .root
        .path()
        .join("native-meta-n0")
        .join(DEFAULT_CLUSTER_METADATA_CONSENSUS_STATE)
        .exists());
    let store = initialize(&f);
    assert_eq!(store.status().current_term, 0);
    drop(store);
    let current = reopen(&f).unwrap();
    assert_eq!(current.status().current_term, 0);
}

#[test]
fn native_metadata_owner_refuses_genuine_pre_capability_born2_factory_without_retrofit() {
    let f = fixture();
    let mut topology = f.topology.read().clone();
    let local = node("n3");
    // Declared unassigned learner BEFORE its genuine old-profile factory.
    // No serialized binding or range counter stands in for an actual birth.
    topology.nodes.insert(
        local.clone(),
        crate::distribution::ClusterNode::new(local.clone(), "127.0.0.1:19003", 1, 1_000_000, 1)
            .unwrap()
            .as_metadata_learner(),
    );
    topology.validate().unwrap();
    let mut spec = f.specs[&node("n0")].clone();
    spec.node_id = local.clone();
    spec.initial_ranges.clear();
    spec.initial_members.insert(
        local.clone(),
        crate::db::RangeImageBirthMember {
            incarnation: 1,
            tls_certificate_sha256: None,
            pending_tls_certificate_sha256: None,
        },
    );
    let config = DbConfig::default()
        .with_fsync(true)
        .with_storage_mode(StorageMode::ServerPaged)
        .with_paged_buffer_pool_bytes(8 * 1024 * 1024);
    let old_path = f.root.path().join("actual-pre-capability-born2");
    let (old, factory) = BicDb::test_create_pre_metadata_capability_enrolled_database(
        &old_path,
        config.clone(),
        spec.clone(),
    )
    .unwrap();
    crate::distribution_native_admission::ActualNativeStoreEnrollment::from_actual_factory(
        &factory, &old, &topology,
    )
    .unwrap()
    .bind_initial_or_unassigned_learner(&mut topology)
    .unwrap();
    let native = old
        .native_recovery_admission(
            &topology.cluster_id,
            topology.range_image_bootstrap_id.unwrap(),
            &local,
            1,
        )
        .unwrap();
    assert_eq!(native.writer_enforcement_version(), 1);
    assert_eq!(native.metadata_initialization_version(), 0);
    drop(old);
    let (old, factory) = BicDb::open_range_image_database(&old_path, config, &spec).unwrap();
    let old = Arc::new(RwLock::new(old));
    let metadata_root = f.root.path().join("old-profile-metadata");
    assert!(MetadataConsensusStore::initialize_native_owned(
        &metadata_root,
        Arc::clone(&old),
        &factory,
        topology.clone(),
        &CancellationToken::uncancelable()
    )
    .is_err());
    assert!(!metadata_root
        .join(DEFAULT_CLUSTER_METADATA_CONSENSUS_STATE)
        .exists());
    assert!(!crate::format::native_metadata_store_active(old.read().data_path()).unwrap());
    // Old contract remains readable. A sticky flag alone cannot add a missing
    // immutable birth capability or retroactively assert never-voted history.
    assert_eq!(
        old.read()
            .native_recovery_admission(
                &topology.cluster_id,
                topology.range_image_bootstrap_id.unwrap(),
                &local,
                1
            )
            .unwrap()
            .metadata_initialization_version(),
        0
    );
    crate::format::enable_native_metadata_store(old.read().data_path()).unwrap();
    assert!(old
        .read()
        .native_recovery_admission(
            &topology.cluster_id,
            topology.range_image_bootstrap_id.unwrap(),
            &local,
            1
        )
        .is_err());
}
