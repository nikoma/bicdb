use super::*;
use crate::db::{CommitAdmissionIntent, CommitAdmissionMutation, RangeImageBirthSpec};
use crate::distribution_range_consensus::{
    native_origin_fixture, native_ready_test_command, seed_actual_origin,
};
use crate::{BicDb, ClusterDataNodeService, Record};
use parking_lot::RwLock;
use std::sync::Arc;

struct Fixture {
    _root: tempfile::TempDir,
    db: Arc<RwLock<BicDb>>,
    topology: crate::ClusterTopology,
    spec: RangeImageBirthSpec,
    service: Option<ClusterDataNodeService>,
    request: NativeRangeReadRequest,
}
impl Fixture {
    fn new(populated: bool) -> Self {
        let (root, db, topology, spec) = native_origin_fixture();
        let ranges = topology.ranges.values().map(|r| r.id).collect::<Vec<_>>();
        let selected = ranges[0];
        if populated {
            seed_actual_origin(&db, topology.clone(), &spec, ranges[1], 2);
            seed_actual_origin(&db, topology.clone(), &spec, selected, 1);
        }
        let range = topology.range_by_id(selected).unwrap().clone();
        let members = spec
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
            .collect();
        let request = NativeRangeReadRequest {
            protocol_version: NATIVE_RANGE_READ_VERSION,
            scope: NativeRangeReadScope {
                cluster_id: topology.cluster_id.clone(),
                bootstrap_id: spec.bootstrap_id,
                range,
                members,
                schema_sha256: db.read().schema_compatibility_fingerprint().unwrap().sha256,
                restore_activation: spec.restore_activation.clone(),
            },
            challenge: Uuid::now_v7(),
            budget_ms: 5000,
        };
        let service = ClusterDataNodeService::open(
            topology.cluster_id.clone(),
            spec.node_id.clone(),
            db.read().data_path(),
            Arc::clone(&db),
            true,
        )
        .unwrap();
        Self {
            _root: root,
            db,
            topology,
            spec,
            service: Some(service),
            request,
        }
    }
    fn service(&self) -> &ClusterDataNodeService {
        self.service.as_ref().unwrap()
    }
    fn observe(&self) -> LocalNativeRangeReadObservation {
        self.service()
            .observe_current_native_range(&self.request, &CancellationToken::uncancelable())
            .unwrap()
    }
}
#[test]
fn native_read_observation_actual_genesis_and_r1_use_target_local_data_b() {
    for populated in [false, true] {
        let fixture = Fixture::new(populated);
        let path = fixture
            .db
            .read()
            .data_path()
            .join(crate::distribution_range_consensus::DEFAULT_RANGE_WRITE_LOG);
        let before = std::fs::read(&path).ok();
        let observed = fixture.observe();
        observed.require_current(fixture.service()).unwrap();
        let lineage = fixture
            .db
            .read()
            .current_native_range_source_lineage(fixture.request.scope.range.id)
            .unwrap();
        assert_eq!(observed.facts().range_index, if populated { 1 } else { 0 });
        assert_eq!(
            observed.local_visibility().get(),
            lineage.actual_local_visibility_sequence()
        );
        assert_eq!(
            observed.facts().local_visibility_db_sequence,
            observed.local_visibility().get()
        );
        if populated {
            assert!(observed.local_visibility().get() > observed.facts().range_index);
        } else {
            assert_eq!(
                observed.facts().logical_genesis_sha256,
                observed.facts().logical_history_sha256
            );
        }
        assert!(observed
            .facts()
            .native_serving_record_commitment_sha256
            .is_none());
        assert_eq!(
            std::fs::read(&path).ok(),
            before,
            "observation did not rewrite catalogue"
        );
    }
}
#[test]
fn native_read_observation_rejects_actual_raw_prepare_and_cq_ahead_of_native() {
    let mut fixture = Fixture::new(true);
    let observed = fixture.observe();
    let range = fixture.request.scope.range.clone();
    let command = native_ready_test_command(
        fixture.topology.cluster_id.clone(),
        &range,
        2,
        range.leader.clone(),
        &CommitAdmissionIntent {
            transaction_id: 99991,
            mutations: vec![CommitAdmissionMutation {
                collection: "items".into(),
                record_id: "unmaterialized-read-negative".into(),
                record: Some(Record::new("unmaterialized-read-negative")),
            }],
        },
    )
    .unwrap();
    let service = fixture.service.as_mut().unwrap();
    service
        .prepare_range_write(&range, &range.leader, command.clone())
        .unwrap();
    assert!(observed.require_current(service).is_err());
    assert!(service
        .observe_current_native_range(&fixture.request, &CancellationToken::uncancelable())
        .is_err());
    service
        .commit_range_write_decision(&range, &range.leader, &command)
        .unwrap();
    service
        .certify_range_write(&range, &range.leader, &command)
        .unwrap();
    assert!(service
        .observe_current_native_range(&fixture.request, &CancellationToken::uncancelable())
        .is_err());
    // Actual legacy raw checkpoint primitive supplies no native WAL receipt.
    // Even matching last/resolved/Applied counters cannot manufacture R2.
    service
        .mark_range_write_applied(&command.command_id)
        .unwrap();
    assert_eq!(service.range_write_progress(range.id).resolved_through, 2);
    assert!(service
        .observe_current_native_range(&fixture.request, &CancellationToken::uncancelable())
        .is_err());
    assert_eq!(
        fixture
            .db
            .read()
            .current_native_range_source_lineage(range.id)
            .unwrap()
            .range_index(),
        1
    );
    assert!(fixture
        .db
        .read()
        .get("items", "unmaterialized-read-negative")
        .unwrap()
        .is_none());
}
#[test]
fn native_read_observation_scope_cancellation_and_store_replacement_are_not_permissions() {
    let mut fixture = Fixture::new(true);
    for variant in 0..5 {
        let mut request = fixture.request.clone();
        match variant {
            0 => request.scope.range.epoch += 1,
            1 => request.scope.schema_sha256 = "f".repeat(64),
            2 => {
                request
                    .scope
                    .members
                    .get_mut(&fixture.spec.node_id)
                    .unwrap()
                    .incarnation += 1
            }
            3 => {
                request
                    .scope
                    .members
                    .get_mut(&fixture.spec.node_id)
                    .unwrap()
                    .active_certificate_sha256 = Some("e".repeat(64))
            }
            _ => request.protocol_version = 0,
        }
        assert!(fixture
            .service()
            .observe_current_native_range(&request, &CancellationToken::uncancelable())
            .is_err());
    }
    let parent = CancellationToken::uncancelable().child();
    let cancelled = fixture
        .service()
        .observe_current_native_range(&fixture.request, &parent)
        .unwrap();
    parent.cancel();
    assert!(cancelled.require_current(fixture.service()).is_err());
    let observed = fixture.observe();
    drop(fixture.service.take());
    fixture.service = Some(
        ClusterDataNodeService::open(
            fixture.topology.cluster_id.clone(),
            fixture.spec.node_id.clone(),
            fixture.db.read().data_path(),
            Arc::clone(&fixture.db),
            true,
        )
        .unwrap(),
    );
    assert!(observed.require_current(fixture.service()).is_err());
    fixture
        .observe()
        .require_current(fixture.service())
        .unwrap();
}
#[test]
fn native_read_observation_failed_mutation_and_health_invalidate_owned_observation() {
    let fixture = Fixture::new(true);
    let observed = fixture.observe();
    let gate = fixture.db.read().range_mutation_gate();
    let permit = gate.try_enter(fixture.request.scope.range.id).unwrap();
    assert!(observed.require_lifetime().is_err());
    assert!(fixture
        .service()
        .observe_current_native_range(&fixture.request, &CancellationToken::uncancelable())
        .is_err());
    drop(permit);
    assert!(
        observed.require_lifetime().is_err(),
        "failed/no-data writer does not renew an older observation"
    );
    let current = fixture.observe();
    fixture.db.read().fence_durability(
        "test.native_read",
        &"deliberate durability-negative fixture",
    );
    assert!(current.require_lifetime().is_err());
    assert!(fixture
        .service()
        .observe_current_native_range(&fixture.request, &CancellationToken::uncancelable())
        .is_err());
}
#[test]
fn native_read_observation_actual_cold_open_invalidates_old_lifetime() {
    let mut fixture = Fixture::new(true);
    let observed = fixture.observe();
    let path = fixture.db.read().data_path().to_path_buf();
    drop(fixture.service.take());
    let Fixture {
        _root,
        db,
        topology,
        spec,
        service: _,
        request,
    } = fixture;
    assert_eq!(Arc::strong_count(&db), 1);
    drop(db);
    assert!(observed.require_lifetime().is_err());
    let (reopened, _) = BicDb::open_range_image_database(
        &path,
        crate::DbConfig::default()
            .with_fsync(true)
            .with_storage_mode(crate::StorageMode::ServerPaged)
            .with_paged_buffer_pool_bytes(8 * 1024 * 1024),
        &spec,
    )
    .unwrap();
    let reopened = Arc::new(RwLock::new(reopened));
    let service = ClusterDataNodeService::open(
        topology.cluster_id,
        spec.node_id,
        path,
        Arc::clone(&reopened),
        true,
    )
    .unwrap();
    assert!(observed.require_current(&service).is_err());
    let fresh = service
        .observe_current_native_range(&request, &CancellationToken::uncancelable())
        .unwrap();
    assert_ne!(
        fresh.facts().native_database_incarnation,
        observed.facts().native_database_incarnation
    );
    assert_eq!(fresh.facts().range_index, 1);
    fresh.require_current(&service).unwrap();
    drop(_root);
}
