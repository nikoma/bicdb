use super::*;
use crate::db::{
    CommitAdmissionIntent, CommitAdmissionMutation, CommitAdmissionTicket, StorageMode,
};
use crate::distribution::NativeStoreBirthV1;
use crate::distribution_range_consensus::{
    native_origin_fixture, test_catalog_command, RangeWriteStore, DEFAULT_RANGE_WRITE_LOG,
};
use crate::distribution_range_mutation::RangeMutationAuthority;
use parking_lot::RwLock;

fn config() -> DbConfig {
    DbConfig::default()
        .with_fsync(true)
        .with_storage_mode(StorageMode::ServerPaged)
        .with_paged_buffer_pool_bytes(8 * 1024 * 1024)
}
fn fresh() -> (
    tempfile::TempDir,
    Arc<RwLock<BicDb>>,
    RangeImageBirthSpec,
    crate::ClusterTopology,
) {
    let (_old_root, _old, topology, spec) = native_origin_fixture();
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("database");
    let (mut db, _) =
        BicDb::create_new_enrolled_range_image_database(&path, config(), spec.clone()).unwrap();
    db.create_collection("items").unwrap();
    drop(db);
    let (db, factory) = BicDb::open_range_image_database(&path, config(), &spec).unwrap();
    db.schema_compatibility_fingerprint().unwrap();
    let db = Arc::new(RwLock::new(db));
    for range in &spec.initial_ranges {
        let gate = db.read().range_mutation_gate();
        let frozen = gate.try_freeze(range.id, Uuid::now_v7()).unwrap();
        BicDb::initialize_range_image_genesis(
            &db,
            &factory,
            range,
            &frozen,
            Duration::from_secs(3),
            &CancellationToken::uncancelable(),
        )
        .unwrap();
    }
    (root, db, spec, topology)
}

#[test]
fn native_writer_birth_v2_is_actual_sticky_and_strict_reopen_preserves_enforcement() {
    let (_root, db, spec, _) = fresh();
    let path = db.read().data_path().to_path_buf();
    let native = db
        .read()
        .native_recovery_admission(
            &spec.cluster_id,
            spec.bootstrap_id,
            &spec.node_id,
            spec.member_incarnation,
        )
        .unwrap();
    assert_eq!(native.writer_enforcement_version(), 1);
    assert!(format::native_writer_enrollment_active(&path).unwrap());
    assert!(format::current_image_proposal_active(&path).unwrap());
    let binding = NativeStoreBirthV1::from_actual_native(&native).unwrap();
    assert_eq!(binding.writer_enforcement_version(), 1);
    assert_eq!(
        serde_json::to_value(&binding).unwrap()["writer_enforcement_version"],
        1
    );
    let birth = native.birth_id();
    drop(native);
    drop(db);
    let (db, _) = BicDb::open_range_image_database(&path, config(), &spec).unwrap();
    db.schema_compatibility_fingerprint().unwrap();
    let actual = db
        .native_recovery_admission(
            &spec.cluster_id,
            spec.bootstrap_id,
            &spec.node_id,
            spec.member_incarnation,
        )
        .unwrap();
    assert_eq!(actual.writer_enforcement_version(), 1);
    assert_eq!(actual.birth_id(), birth);
}

#[test]
fn native_writer_birth_v1_cannot_be_upgraded_by_factory_or_sticky_flag() {
    let (_root, db, _, spec) = native_origin_fixture();
    let path = db.read().data_path().to_path_buf();
    assert_eq!(
        db.read()
            .actual_native_writer_enforcement_version()
            .unwrap(),
        0
    );
    assert!(
        BicDb::create_new_enrolled_range_image_database(&path, config(), spec.clone()).is_err()
    );
    assert!(!format::native_writer_enrollment_active(&path).unwrap());
    let native = db
        .read()
        .native_recovery_admission(
            &spec.cluster_id,
            spec.bootstrap_id,
            &spec.node_id,
            spec.member_incarnation,
        )
        .unwrap();
    let binding = NativeStoreBirthV1::from_actual_native(&native).unwrap();
    assert_eq!(binding.writer_enforcement_version(), 0);
    assert!(serde_json::to_value(&binding)
        .unwrap()
        .get("writer_enforcement_version")
        .is_none());
    format::enable_current_image_proposal(&path).unwrap();
    format::enable_native_writer_enrollment(&path).unwrap();
    assert!(db
        .read()
        .actual_native_writer_enforcement_version()
        .is_err());
    assert!(db
        .read()
        .native_recovery_admission(
            &spec.cluster_id,
            spec.bootstrap_id,
            &spec.node_id,
            spec.member_incarnation
        )
        .is_err());
    let original: NativeDatabaseBirth = db
        .read()
        .read_native_range_image_record(BIRTH_KEY)
        .unwrap()
        .unwrap();
    assert_eq!(original.format_version, 1);
    drop(db);
    assert!(BicDb::open_range_image_database(&path, config(), &spec).is_err());
}

#[test]
fn native_writer_birth_v2_partial_factory_keeps_fence_and_refuses_reopen() {
    let (_old_root, _db, _, spec) = native_origin_fixture();
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("partial");
    let result = BicDb::create_new_range_image_database_with_mode(
        &path,
        config(),
        spec.clone(),
        true,
        true,
        |_| Err(invalid("injected factory after actual flags")),
    );
    assert!(result.is_err());
    assert!(path.is_dir());
    assert!(format::native_writer_enrollment_active(&path).unwrap());
    assert!(BicDb::open_range_image_database(&path, config(), &spec).is_err());
    assert!(BicDb::create_new_enrolled_range_image_database(&path, config(), spec).is_err());
}

#[test]
fn native_writer_broker_effects_refuse_before_wal_even_without_record_mutations() {
    for include_record in [false, true] {
        let (_root, database, spec, _) = fresh();
        let db = database.read();
        let path = db.data_path().join(DEFAULT_RANGE_WRITE_LOG);
        let read_catalog = || match std::fs::read(&path) {
            Ok(bytes) => Some(bytes),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => None,
            Err(e) => panic!("cannot observe actual catalogue: {e}"),
        };
        let before_catalog = read_catalog();
        let before_wal = db.tx_log_handle().stats();
        let before = db
            .current_native_range_source_lineage(spec.initial_ranges[0].id)
            .unwrap()
            .actual_local_visibility_sequence();
        let mut tx = db.begin_transaction().unwrap();
        if include_record {
            tx.insert("items", Record::new("no-local-only-broker-ack"))
                .unwrap();
        }
        tx.buffer_broker_publish(
            "native-effects",
            serde_json::json!({"work":1}),
            crate::broker::PublishOptions::default(),
        )
        .unwrap();
        let outcome = tx.observe_outcome();
        let error = tx.commit().unwrap_err();
        assert!(
            matches!(&error, BicDbError::Unsupported(detail) if detail.contains("broker effects")),
            "{error}"
        );
        assert_eq!(outcome.outcome(), crate::TransactionOutcome::Aborted);
        assert_eq!(
            db.tx_log_handle().stats().written_seq,
            before_wal.written_seq
        );
        assert_eq!(
            db.tx_log_handle().stats().commits_written,
            before_wal.commits_written
        );
        assert_eq!(read_catalog(), before_catalog);
        let lineage = db
            .current_native_range_source_lineage(spec.initial_ranges[0].id)
            .unwrap();
        assert_eq!(lineage.range_index(), 0);
        assert_eq!(lineage.actual_local_visibility_sequence(), before);
        assert!(db
            .get("items", "no-local-only-broker-ack")
            .unwrap()
            .is_none());
        assert!(db
            .events
            .lock()
            .broker()
            .peek("native-effects", 0, 10)
            .unwrap()
            .is_empty());
    }
}

#[test]
fn native_writer_broker_effect_refusal_preserves_legacy_transactional_outbox() {
    let root = tempfile::tempdir().unwrap();
    let mut db = BicDb::open_with_config(root.path(), config()).unwrap();
    db.create_collection("items").unwrap();
    let mut tx = db.begin_transaction().unwrap();
    tx.insert("items", Record::new("legacy-row-and-broker"))
        .unwrap();
    let message = tx
        .buffer_broker_publish(
            "legacy-effects",
            serde_json::json!({"work":1}),
            crate::broker::PublishOptions::default(),
        )
        .unwrap();
    tx.commit().unwrap();
    assert!(db.get("items", "legacy-row-and-broker").unwrap().is_some());
    let messages = db
        .events
        .lock()
        .broker()
        .peek("legacy-effects", 0, 10)
        .unwrap();
    assert_eq!(messages.len(), 1);
    assert_eq!(messages[0].message_id, message);
}

#[test]
fn native_writer_birth_v2_uninstalled_actual_store_and_service_refuse_before_prepare() {
    let (_root, db, spec, topology) = fresh();
    let path = db.read().data_path().to_path_buf();
    let range = spec.initial_ranges[0].clone();
    let key = (0..1000)
        .map(|i| format!("writer-uninstalled-{i}"))
        .find(|key| topology.range_for_key("items", key).unwrap().id == range.id)
        .unwrap();
    let intent = CommitAdmissionIntent {
        transaction_id: 777,
        mutations: vec![CommitAdmissionMutation {
            collection: "items".into(),
            record_id: key.clone(),
            record: Some(Record::new(&key)),
        }],
    };
    let command = test_catalog_command(spec.cluster_id.clone(), &range, 1, &intent).unwrap();
    let mut store =
        RangeWriteStore::open(&path, spec.cluster_id.clone(), spec.node_id.clone(), true).unwrap();
    assert!(store
        .prepare(command.clone())
        .unwrap_err()
        .to_string()
        .contains("policy"));
    assert_eq!(store.progress(range.id).last_index, 0);
    assert_eq!(store.progress(range.id).resolved_through, 0);
    assert!(!path.join(DEFAULT_RANGE_WRITE_LOG).exists());
    drop(store);
    let mut service = crate::ClusterDataNodeService::open(
        spec.cluster_id.clone(),
        spec.node_id.clone(),
        &path,
        Arc::clone(&db),
        true,
    )
    .unwrap();
    assert_eq!(service.native_writer_enforcement_version().unwrap(), 1);
    assert!(service
        .prepare_range_write(&range, &range.leader, command)
        .is_err());
    assert!(!path.join(DEFAULT_RANGE_WRITE_LOG).exists());
    assert_eq!(
        db.read()
            .native_range_prefix_observation(range.id)
            .unwrap()
            .native_applied_index,
        Some(0)
    );
    assert!(db.read().get("items", &key).unwrap().is_none());
}

#[derive(Debug)]
struct NeverAdmits;
impl CommitAdmission for NeverAdmits {
    fn admit(&self, _: &CommitAdmissionIntent) -> Result<CommitAdmissionTicket> {
        Err(invalid("test authority never admits"))
    }
    fn local_applied(&self, _: &CommitAdmissionTicket, _: u64) -> Result<()> {
        Err(invalid("test authority never applies"))
    }
}
#[test]
fn native_writer_hook_clear_same_arc_reinstall_does_not_reactivate_old_registration() {
    let (_root, db, spec, _) = fresh();
    let authority: Arc<dyn CommitAdmission> = Arc::new(NeverAdmits);
    let database = db.read();
    database.install_commit_admission(authority.clone());
    let old = Arc::clone(database.commit_admission.read().registration());
    let generation = old.generation();
    assert!(old.is_active());
    database.clear_commit_admission();
    assert!(!old.is_active());
    database.install_commit_admission(authority);
    assert!(!old.is_active());
    assert_ne!(
        database.commit_admission.read().registration().generation(),
        generation
    );
    assert!(database.commit_admission.read().native_writer().is_none());
    // A generic hook is not an enrolled writer policy, even on the same real DB.
    let path = database.data_path().to_path_buf();
    drop(database);
    let service = crate::ClusterDataNodeService::open(
        spec.cluster_id,
        spec.node_id,
        &path,
        Arc::clone(&db),
        true,
    )
    .unwrap();
    assert!(service
        .require_native_writer_host_for_range(&db.read(), &spec.initial_ranges[0])
        .is_err());
}

#[derive(Debug)]
struct CountAdmissionCalls(Arc<std::sync::atomic::AtomicUsize>);
impl CommitAdmission for CountAdmissionCalls {
    fn admit(&self, _: &CommitAdmissionIntent) -> Result<CommitAdmissionTicket> {
        self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Err(invalid("dummy admission must not be called in born-v2"))
    }
    fn local_applied(&self, _: &CommitAdmissionTicket, _: u64) -> Result<()> {
        panic!("dummy cannot materialize native writes")
    }
}

#[test]
fn native_writer_generic_open_config_false_and_dummy_refuse_before_admission_or_rows() {
    let (_root, db, spec, topology) = fresh();
    let path = db.read().data_path().to_path_buf();
    let range = &spec.initial_ranges[0];
    let key = (0..1000)
        .map(|i| format!("writer-generic-{i}"))
        .find(|key| topology.range_for_key("items", key).unwrap().id == range.id)
        .unwrap();
    drop(db);
    let database =
        BicDb::open_with_config(&path, config().with_required_commit_admission(false)).unwrap();
    database.schema_compatibility_fingerprint().unwrap();
    assert_eq!(
        database.actual_native_writer_enforcement_version().unwrap(),
        1
    );
    let before = database.current_commit_seq();
    let mut transaction = database.begin_transaction().unwrap();
    transaction.insert("items", Record::new(&key)).unwrap();
    assert!(transaction
        .commit()
        .unwrap_err()
        .to_string()
        .contains("installed writer policy"));
    assert!(database.get("items", &key).unwrap().is_none());
    assert_eq!(database.current_commit_seq(), before);
    let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    database.install_commit_admission(Arc::new(CountAdmissionCalls(calls.clone())));
    let mut transaction = database.begin_transaction().unwrap();
    transaction.insert("items", Record::new(&key)).unwrap();
    assert!(transaction
        .commit()
        .unwrap_err()
        .to_string()
        .contains("installed writer policy"));
    assert_eq!(
        calls.load(std::sync::atomic::Ordering::SeqCst),
        0,
        "reject before generic authority side effects"
    );
    assert!(database.get("items", &key).unwrap().is_none());
    assert_eq!(database.current_commit_seq(), before);
    assert!(!path.join(DEFAULT_RANGE_WRITE_LOG).exists());
}

#[test]
fn native_writer_born_v2_raw_imports_refuse_without_invalidating_genesis() {
    let (_root, db, spec, topology) = fresh();
    let range = &spec.initial_ranges[0];
    let key = (0..1000)
        .map(|i| format!("writer-raw-{i}"))
        .find(|key| topology.range_for_key("items", key).unwrap().id == range.id)
        .unwrap();
    let mut database = db.write();
    let initial = database
        .current_native_range_source_lineage(range.id)
        .unwrap();
    let initial_sha = initial.native_receipt_sha256().to_owned();
    let before = database.current_commit_seq();
    let gate = database.range_mutation_gate();
    let permit = gate.try_enter(range.id).unwrap();
    let authority = RangeMutationAuthority::Mutation(&permit);
    let mutation = CommitAdmissionMutation {
        collection: "items".into(),
        record_id: key.clone(),
        record: Some(Record::new(&key)),
    };
    assert!(database
        .apply_admitted_mutations_with_mutation(range.id, &[mutation], &authority)
        .unwrap_err()
        .to_string()
        .contains("generic import"));
    let frame = crate::replication::CommitFrame::new(
        "unexpected-cluster",
        "peer",
        "stream",
        before + 1,
        42,
        0,
        Vec::new(),
    );
    assert!(database
        .apply_replication_frame(&frame)
        .unwrap_err()
        .to_string()
        .contains("generic import"));
    assert!(database
        .delete_range_records_as_local_with_mutation(range, "items", &[key.clone()], &authority)
        .is_err());
    assert_eq!(database.current_commit_seq(), before);
    assert!(database.get("items", &key).unwrap().is_none());
    let after = database
        .current_native_range_source_lineage(range.id)
        .unwrap();
    assert_eq!(after.range_index(), 0);
    assert_eq!(after.native_receipt_sha256(), initial_sha);
}
