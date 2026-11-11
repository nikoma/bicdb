use super::*;
use crate::db::RangeImageBirthSpec;
use crate::distribution_range_consensus::{native_origin_fixture, seed_actual_origin};
use crate::{CancellationToken, DbConfig, StorageMode};
use parking_lot::RwLock;
use std::sync::Arc;
use std::time::Duration;
use uuid::Uuid;

fn config() -> DbConfig {
    DbConfig::default()
        .with_fsync(true)
        .with_storage_mode(StorageMode::ServerPaged)
        .with_paged_buffer_pool_bytes(8 * 1024 * 1024)
}
fn reattest_empty_genesis(
    db: &Arc<RwLock<BicDb>>,
    factory: &FreshRangeGenesisAuthority,
    spec: &RangeImageBirthSpec,
) {
    // Actual schema scan for this open; no cache or digest is assigned.
    db.read().schema_compatibility_fingerprint().unwrap();
    for range in &spec.initial_ranges {
        let gate = db.read().range_mutation_gate();
        let frozen = gate.try_freeze(range.id, Uuid::now_v7()).unwrap();
        BicDb::initialize_range_image_genesis(
            db,
            factory,
            range,
            &frozen,
            Duration::from_secs(3),
            &CancellationToken::uncancelable(),
        )
        .unwrap();
    }
}
fn fresh_same_spec(
    root: &std::path::Path,
    spec: &RangeImageBirthSpec,
) -> (Arc<RwLock<BicDb>>, FreshRangeGenesisAuthority) {
    let (mut db, factory) =
        BicDb::create_new_range_image_database(root, config(), spec.clone()).unwrap();
    db.create_collection("items").unwrap();
    let db = Arc::new(RwLock::new(db));
    reattest_empty_genesis(&db, &factory, spec);
    (db, factory)
}

#[test]
fn native_enrollment_actual_factory_distinguishes_same_spec_recreated_disk() {
    let (_root, original, mut topology, spec) = native_origin_fixture();
    // INITIAL test plan before creating metadata consensus, not a live change.
    topology.generation = 1;
    topology.history.truncate(1);
    let path = original.read().data_path().to_path_buf();
    drop(original);
    let (db, factory) = BicDb::open_range_image_database(&path, config(), &spec).unwrap();
    let db = Arc::new(RwLock::new(db));
    reattest_empty_genesis(&db, &factory, &spec);
    let enrollment =
        ActualNativeStoreEnrollment::from_actual_factory(&factory, &db.read(), &topology).unwrap();
    enrollment
        .bind_initial_or_unassigned_learner(&mut topology)
        .unwrap();
    let enrolled = topology.nodes[&spec.node_id]
        .native_store_birth_v1
        .clone()
        .unwrap();
    assert_eq!(enrolled.birth_id(), factory.birth_id());
    assert_eq!(topology.format_version, 2);
    let other = tempfile::tempdir().unwrap();
    let (other_db, other_factory) = fresh_same_spec(&other.path().join("new-disk"), &spec);
    assert_ne!(factory.birth_id(), other_factory.birth_id());
    let other_enrollment = ActualNativeStoreEnrollment::from_actual_factory(
        &other_factory,
        &other_db.read(),
        &topology,
    )
    .unwrap();
    let before = topology.clone();
    assert!(other_enrollment
        .bind_initial_or_unassigned_learner(&mut topology)
        .unwrap_err()
        .to_string()
        .contains("enrolled birth cannot be replaced"));
    assert_eq!(topology, before);

    // A live old voter with missing registry cannot auto-enroll the new disk.
    let mut missing = before.clone();
    missing.generation += 1;
    missing
        .nodes
        .get_mut(&spec.node_id)
        .unwrap()
        .native_store_birth_v1 = None;
    let other_enrollment = ActualNativeStoreEnrollment::from_actual_factory(
        &other_factory,
        &other_db.read(),
        &missing,
    )
    .unwrap();
    let unchanged = missing.clone();
    assert!(other_enrollment
        .bind_initial_or_unassigned_learner(&mut missing)
        .unwrap_err()
        .to_string()
        .contains("existing unbound voter"));
    assert_eq!(missing, unchanged);

    // Strict cold reopen of the SAME actual birth remains idempotent at R0.
    drop(db);
    let (reopened, actual_factory) =
        BicDb::open_range_image_database(&path, config(), &spec).unwrap();
    let reopened = Arc::new(RwLock::new(reopened));
    reattest_empty_genesis(&reopened, &actual_factory, &spec);
    assert_eq!(actual_factory.birth_id(), enrolled.birth_id());
    ActualNativeStoreEnrollment::from_actual_factory(&actual_factory, &reopened.read(), &topology)
        .unwrap()
        .bind_initial_or_unassigned_learner(&mut topology)
        .unwrap();
    assert_eq!(topology, before);
}

#[test]
fn native_enrollment_refuses_genuine_factory_after_actual_committed_write() {
    let (_root, original, mut topology, spec) = native_origin_fixture();
    topology.generation = 1;
    topology.history.truncate(1);
    let path = original.read().data_path().to_path_buf();
    drop(original);
    let (db, factory) = BicDb::open_range_image_database(&path, config(), &spec).unwrap();
    let db = Arc::new(RwLock::new(db));
    reattest_empty_genesis(&db, &factory, &spec);
    ActualNativeStoreEnrollment::from_actual_factory(&factory, &db.read(), &topology).unwrap();
    let range = spec.initial_ranges[0].id;
    seed_actual_origin(&db, topology.clone(), &spec, range, 1);
    let error = ActualNativeStoreEnrollment::from_actual_factory(&factory, &db.read(), &topology)
        .unwrap_err();
    assert!(
        error.to_string().contains("unwritten native genesis"),
        "{error}"
    );
}

#[test]
fn native_enrollment_actual_new_learner_birth_does_not_promote_or_assign_ranges() {
    let (_root, _original, mut topology, mut spec) = native_origin_fixture();
    let id = ClusterNodeId::new("new-learner").unwrap();
    let learner =
        crate::distribution::ClusterNode::new(id.clone(), "127.0.0.1:12111", 1, 1 << 30, 0)
            .unwrap()
            .as_metadata_learner();
    topology.nodes.insert(id.clone(), learner);
    topology.generation += 1;
    spec.node_id = id.clone();
    spec.initial_ranges.clear();
    spec.initial_members.insert(
        id.clone(),
        crate::db::RangeImageBirthMember {
            incarnation: 1,
            tls_certificate_sha256: None,
            pending_tls_certificate_sha256: None,
        },
    );
    topology.validate().unwrap();
    let root = tempfile::tempdir().unwrap();
    let (db, factory) = fresh_same_spec(&root.path().join("learner"), &spec);
    let ranges = topology.ranges.clone();
    ActualNativeStoreEnrollment::from_actual_factory(&factory, &db.read(), &topology)
        .unwrap()
        .bind_initial_or_unassigned_learner(&mut topology)
        .unwrap();
    assert_eq!(
        topology.nodes[&id].metadata_role,
        MetadataMemberRole::Learner
    );
    assert!(topology.nodes[&id].native_store_birth_v1.is_some());
    assert_eq!(topology.ranges, ranges);
}
