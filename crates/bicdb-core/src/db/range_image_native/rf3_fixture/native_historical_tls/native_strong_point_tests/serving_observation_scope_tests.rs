//! Actual fresh born2/three-policy-before-ACK controls. Metadata election is
//! the existing fixture's declared precondition; MQ and peer calls use real TLS.
use super::*;
use crate::distribution_native_read::NativeReadQuorumRound;
use crate::distribution_range_consensus::RangeWriteStore;
use std::sync::atomic::{AtomicBool, Ordering};

#[derive(Clone, Copy, Debug)]
enum Cut {
    ForeignDatabase,
    ForeignStore,
    Hook,
    Caller,
    SchemaGeneration,
    MiddleHistory,
}
fn token() -> CancellationToken {
    CancellationToken::uncancelable().child_with_deadline(Instant::now() + Duration::from_secs(5))
}
fn seed() -> (EnforcedNetwork, RangeDescriptor) {
    let fixture = EnforcedNetwork::new();
    let (first, range) = fixture.seed();
    let second = fixture.key(range.id, "scoped-read-actual-ack2");
    fixture.native.append(&second).unwrap();
    fixture.idle();
    let db = fixture.native.databases[&node("n0")].read();
    let records = [first, second].map(|key| db.get("items", &key).unwrap().unwrap());
    drop(db);
    // These are actual acknowledged records. The separate selected-range
    // ledger is fsynced; no extra application commit is invented by this file.
    let path = fixture
        .native
        .root
        .path()
        .join("actual-joint-selected-range-ack-ledger.json");
    std::fs::write(&path, serde_json::to_vec(&records).unwrap()).unwrap();
    std::fs::File::open(&path).unwrap().sync_all().unwrap();
    std::fs::File::open(fixture.native.root.path())
        .unwrap()
        .sync_all()
        .unwrap();
    (fixture, range)
}
fn snapshot(fixture: &EnforcedNetwork, range: &RangeDescriptor) -> serde_json::Value {
    snapshot_at(fixture, range, 2)
}
fn snapshot_at(
    fixture: &EnforcedNetwork,
    range: &RangeDescriptor,
    expected_r: u64,
) -> serde_json::Value {
    let mut values = BTreeMap::new();
    for id in [node("n0"), node("n1"), node("n2")] {
        let db = fixture.native.databases[&id].read();
        let actual = db.current_native_range_source_lineage(range.id).unwrap();
        assert_eq!(actual.range_index(), expected_r);
        let mut rows = db.scan_collection("items").unwrap();
        rows.sort_by(|a, b| a.id.cmp(&b.id));
        let raw = std::fs::read(
            db.data_path()
                .join(crate::distribution_range_consensus::DEFAULT_RANGE_WRITE_LOG),
        )
        .unwrap();
        values.insert(id, serde_json::json!({"R":actual.range_index(),"H":actual.logical_history_sha256(),
            "B":actual.actual_local_visibility_sequence(),"UID":db.incarnation_id(),
            "birth":actual.birth.birth_id,"namespace_uid":db.local_namespace_identity("items").unwrap(),"history_generation":actual.history_generation(),"raw":raw,"rows":rows}));
    }
    serde_json::json!({"nodes":values,"ledger":std::fs::read(fixture.native.root.path().join("actual-joint-selected-range-ack-ledger.json")).unwrap()})
}
fn round(
    fixture: &EnforcedNetwork,
    range: &RangeDescriptor,
    parent: &CancellationToken,
) -> NativeReadQuorumRound {
    let business = Uuid::now_v7();
    let first = fixture.first(business, range, parent);
    NativeReadQuorumRound::begin(first, business, Uuid::now_v7(), &node("n0"), parent).unwrap()
}
fn negative(cut: Cut) {
    let (fixture, range) = seed();
    let before = snapshot(&fixture, &range); // Never spend the live MQ on report IO.
    let foreign_database = fixture.native.databases[&node("n1")].clone();
    let foreign_root = tempfile::tempdir().unwrap();
    let foreign_store = RangeWriteStore::open(
        foreign_root.path(),
        range_write_cluster(&fixture),
        node("n0"),
        true,
    )
    .unwrap();
    let replacement = fixture.coordinators[&node("n0")].clone();
    let parent = token();
    let actual_round = round(&fixture, &range, &parent);
    let request = actual_round.request().clone();
    let canceled = parent.clone();
    let reached = Arc::new(AtomicBool::new(false));
    let actual_reached = reached.clone();
    let service = fixture.native.services[&node("n0")].lock();
    let error = BicDb::test_with_native_serving_read_scope_cut(
        move |scope| {
            actual_reached.store(true, Ordering::Release);
            match cut {
                Cut::ForeignDatabase => {
                    let db = foreign_database
                        .try_read()
                        .expect("real foreign DB remains available");
                    scope.require_database(&db)?;
                    panic!("foreign actual DB must never match scope");
                }
                Cut::ForeignStore => {
                    scope.require_database_store(scope.database(), &foreign_store)?;
                    panic!("foreign actual Store must never match scope");
                }
                Cut::Hook => scope
                    .database()
                    .install_commit_admission(replacement.clone()),
                Cut::Caller => canceled.cancel(),
                Cut::SchemaGeneration => scope
                    .database()
                    .test_invalidate_serving_read_schema_generation(scope)?,
                Cut::MiddleHistory => scope
                    .database()
                    .test_remove_serving_read_middle_history(scope)?,
            }
            Ok(())
        },
        || service.observe_current_native_range(&request, &parent),
    )
    .unwrap_err();
    drop(service);
    assert!(
        reached.load(Ordering::Acquire),
        "actual scope boundary not reached for {cut:?}: {error}"
    );
    match cut {
        Cut::Caller => assert!(matches!(error, crate::BicDbError::QueryCanceled), "{error}"),
        Cut::ForeignDatabase | Cut::ForeignStore => {
            parent.check().unwrap();
            assert!(
                error
                    .to_string()
                    .contains("serving read scope substituted actual DB/Store"),
                "{error}"
            );
        }
        Cut::Hook => {
            parent.check().unwrap();
            assert!(error.to_string().contains("native writer hook"), "{error}");
        }
        Cut::SchemaGeneration => {
            parent.check().unwrap();
            assert!(
                error.to_string().contains("native writer schema changed"),
                "{error}"
            );
        }
        Cut::MiddleHistory => {
            parent.check().unwrap();
            assert!(
                error
                    .to_string()
                    .contains("native logical history is incomplete at index 1"),
                "{error}"
            );
        }
    }
    fixture.idle();
    // The history fault intentionally removes its one actual middle entry;
    // immutable HEAD/physical B/UID, full rows, raw slots and both ACKs stay exact.
    assert_eq!(snapshot(&fixture, &range), before);
}
fn range_write_cluster(fixture: &EnforcedNetwork) -> crate::ClusterId {
    fixture.native.topology.read().cluster_id.clone()
}
#[test]
fn native_serving_observation_scope_actual_foreign_database_refused() {
    negative(Cut::ForeignDatabase);
}
#[test]
fn native_serving_observation_scope_actual_foreign_store_refused() {
    negative(Cut::ForeignStore);
}
#[test]
fn native_serving_observation_scope_actual_hook_replacement_refused() {
    negative(Cut::Hook);
}
#[test]
fn native_serving_observation_scope_actual_original_caller_cancel_refused() {
    negative(Cut::Caller);
}
#[test]
fn native_serving_observation_scope_actual_schema_generation_refused() {
    negative(Cut::SchemaGeneration);
}
#[test]
fn native_serving_observation_scope_actual_missing_middle_history_refused() {
    negative(Cut::MiddleHistory);
}
#[test]
fn native_serving_observation_scope_actual_tls_reply_and_fresh_finish() {
    let (fixture, range) = seed();
    let before = snapshot(&fixture, &range);
    let parent = token();
    let mut actual = round(&fixture, &range, &parent);
    let request = actual.request().clone();
    let peer = fixture.clients[&node("n0")]
        .observe_current_native_range(&node("n1"), &request, &parent)
        .unwrap();
    assert_eq!(peer.facts().range_index, 2);
    let expected_peer_b = before["nodes"]["n1"]["B"].as_u64().unwrap();
    assert_eq!(peer.facts().local_visibility_db_sequence, expected_peer_b);
    actual.record(peer).unwrap();
    // The same actual collector/service producer is used for local evidence;
    // no decoded reply can be relabeled as this local physical observation.
    let actor = fixture.clients[&node("n0")].local_metadata_read_participant();
    let local = fixture.runtimes[&node("n0")]
        .observe_current_native_range(&actor, &request, &parent)
        .unwrap();
    actual.record(local).unwrap();
    let candidate = actual
        .finish(&fixture.native.services[&node("n0")].lock())
        .unwrap();
    candidate.require_lifetime().unwrap();
    parent.check().unwrap();
    drop(candidate);
    fixture.idle();
    assert_eq!(snapshot(&fixture, &range), before);
}

mod normal_serving_record_tests;
