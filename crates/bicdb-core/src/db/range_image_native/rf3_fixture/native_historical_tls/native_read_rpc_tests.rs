//! Actual mTLS probes plus LOCAL factual candidate collection. Current data
//! leader n1 / metadata leader n2 and metadata anchors are fixture preconditions.
//! No host enrollment, snapshot, final MQ, read gateway or StrongGet is tested.
use super::*;
use crate::distribution_consensus::MetadataReadObservation;
use crate::distribution_native_read::{NativeRangeReadObservation, NativeReadQuorumRound};

fn live_fixture() -> NetworkFixture {
    let (fixture, scope) = NetworkFixture::all_r1_prepared();
    for name in ["n0", "n1", "n2"] {
        fixture.complete(name, &scope).unwrap();
        fixture.wait_idle();
    }
    for name in ["n0", "n1", "n2"] {
        fixture.runtimes[&node(name)]
            .activate_native_recovery(
                &fixture.actor(),
                &fixture.completion_request(&scope),
                &CancellationToken::uncancelable(),
            )
            .unwrap();
        fixture.wait_idle();
    }
    fixture
}
fn fresh_metadata(
    fixture: &NetworkFixture,
    business: Uuid,
    parent: &CancellationToken,
) -> MetadataReadObservation {
    let range = fixture
        .current
        .read()
        .ranges
        .values()
        .next()
        .unwrap()
        .clone();
    fixture.clients[&node("n1")]
        .acquire_metadata_read_observation(
            &node("n2"),
            business,
            range.id,
            range.epoch,
            Duration::from_secs(5),
            parent,
        )
        .unwrap()
}
#[test]
fn native_read_rpc_actual_tls_fresh_metadata_unadmitted_candidate() {
    let fixture = live_fixture();
    let parent = CancellationToken::uncancelable();
    let business = Uuid::now_v7();
    let metadata = fresh_metadata(&fixture, business, &parent);
    let context = metadata.context_id();
    let challenge = Uuid::now_v7();
    let candidate = fixture.runtimes[&node("n1")]
        .acquire_native_read_candidate(metadata, business, challenge, &parent)
        .unwrap();
    candidate.require_lifetime().unwrap();
    assert_eq!(candidate.metadata_context_id(), context);
    assert_eq!(candidate.business_request_id(), business);
    assert_eq!(candidate.challenge(), challenge);
    assert_eq!(candidate.scope().range.leader, node("n1"));
    assert!(candidate.matching_member_ids().len() >= 2);
    assert_eq!(candidate.point().0, 1);
    assert_ne!(candidate.point().1, candidate.point().2);
    fixture.wait_idle();
    // All three observations originate in actual producers. The local genesis
    // or Activated facts still do NOT supply host birth-continuation admission.
    parent.cancel();
    assert!(candidate.require_lifetime().is_err());
}

fn live_enrolled_fixture(
    initial_edit: Option<fn(&mut ClusterTopology, &ActualNativeRf3Fixture, &Path)>,
) -> NetworkFixture {
    let fixture = NetworkFixture::new_with_initial_metadata_edit(
        true,
        true,
        false,
        "n1",
        "n2",
        true,
        initial_edit,
    );
    let scope = fixture.scope();
    for name in ["n0", "n1", "n2"] {
        fixture.call(name, &scope, false).unwrap();
        fixture.wait_idle();
    }
    fixture
}

#[test]
fn native_read_rpc_actual_tls_enrolled_births_require_actual_metadata_and_peer_facts() {
    let fixture = live_enrolled_fixture(None);
    for name in ["n0", "n1", "n2"] {
        fixture.call(name, &scope, true).unwrap();
        fixture.wait_idle();
        fixture.prepare_ready(name, &scope).unwrap();
        fixture.wait_idle();
    }
    for name in ["n0", "n1", "n2"] {
        fixture.complete(name, &scope).unwrap();
        fixture.wait_idle();
    }
    for name in ["n0", "n1", "n2"] {
        fixture.runtimes[&node(name)]
            .activate_native_recovery(
                &fixture.actor(),
                &fixture.completion_request(&scope),
                &CancellationToken::uncancelable(),
            )
            .unwrap();
        fixture.wait_idle();
    }
    let parent = CancellationToken::uncancelable();
    let business = Uuid::now_v7();
    let metadata = fresh_metadata(&fixture, business, &parent);
    assert_eq!(
        metadata
            .committed_topology(business)
            .unwrap()
            .format_version,
        2
    );
    let candidate = fixture.runtimes[&node("n1")]
        .acquire_native_read_candidate(metadata, business, Uuid::now_v7(), &parent)
        .unwrap();
    let checked = candidate.check_enrolled_voter_births().unwrap();
    checked.require_lifetime().unwrap();
    assert_eq!(checked.point_for_test().0, 1);
    parent.cancel();
    assert!(checked.require_lifetime().is_err());
    fixture.wait_idle();
    // Birth continuity checked, not a StrongGet or row/absence permission.
}

fn initial_metadata_binds_another_genuine_birth(
    topology: &mut ClusterTopology,
    native: &ActualNativeRf3Fixture,
    root: &Path,
) {
    let spec = native.specs[&node("n1")].clone();
    let config = DbConfig::default()
        .with_fsync(true)
        .with_storage_mode(StorageMode::ServerPaged)
        .with_paged_buffer_pool_bytes(8 * 1024 * 1024);
    let (mut db, factory) = BicDb::create_new_range_image_database(
        &root.join("other-actual-native-birth"),
        config,
        spec.clone(),
    )
    .unwrap();
    db.create_collection("items").unwrap();
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
    let actual = db
        .read()
        .native_recovery_admission(
            &spec.cluster_id,
            spec.bootstrap_id,
            &spec.node_id,
            spec.member_incarnation,
        )
        .unwrap();
    let binding = crate::distribution::NativeStoreBirthV1::from_actual_native(&actual).unwrap();
    assert_ne!(
        binding.birth_id(),
        topology.nodes[&node("n1")]
            .native_store_birth_v1
            .as_ref()
            .unwrap()
            .birth_id()
    );
    // INITIAL mismatch fixture, before metadata-store creation. It is not a
    // live birth rewrite, and the returned peer facts still come from A.
    topology
        .nodes
        .get_mut(&node("n1"))
        .unwrap()
        .native_store_birth_v1 = Some(binding);
}

#[test]
fn native_read_rpc_actual_tls_genuine_other_birth_in_metadata_cannot_admit_local_a() {
    let fixture = live_enrolled_fixture(Some(initial_metadata_binds_another_genuine_birth));
    let parent = CancellationToken::uncancelable();
    let business = Uuid::now_v7();
    let candidate = fixture.runtimes[&node("n1")]
        .acquire_native_read_candidate(
            fresh_metadata(&fixture, business, &parent),
            business,
            Uuid::now_v7(),
            &parent,
        )
        .unwrap();
    assert!(candidate
        .check_enrolled_voter_births()
        .unwrap_err()
        .to_string()
        .contains("actual read voter birth differs"));
    fixture.wait_idle();
}

#[test]
fn native_read_rpc_actual_tls_unenrolled_quorum_cannot_be_admitted() {
    let fixture = live_fixture();
    let parent = CancellationToken::uncancelable();
    let business = Uuid::now_v7();
    let candidate = fixture.runtimes[&node("n1")]
        .acquire_native_read_candidate(
            fresh_metadata(&fixture, business, &parent),
            business,
            Uuid::now_v7(),
            &parent,
        )
        .unwrap();
    assert!(candidate
        .check_enrolled_voter_births()
        .unwrap_err()
        .to_string()
        .contains("no fenced native birth registry"));
    fixture.wait_idle();
}
#[test]
fn native_read_rpc_actual_tls_rejects_old_challenge_actor_incarnation_and_pins() {
    let fixture = live_fixture();
    let parent = CancellationToken::uncancelable();
    let business = Uuid::now_v7();
    let round = NativeReadQuorumRound::begin(
        fresh_metadata(&fixture, business, &parent),
        business,
        Uuid::now_v7(),
        &node("n1"),
        &parent,
    )
    .unwrap();
    let request = round.request().clone();
    let old = fixture.clients[&node("n1")]
        .observe_current_native_range(&node("n2"), &request, &parent)
        .unwrap();
    assert_eq!(old.facts().node_id, node("n2"));
    fixture.wait_idle();
    let business2 = Uuid::now_v7();
    let mut next = NativeReadQuorumRound::begin(
        fresh_metadata(&fixture, business2, &parent),
        business2,
        Uuid::now_v7(),
        &node("n1"),
        &parent,
    )
    .unwrap();
    assert!(
        next.record(old).is_err(),
        "a genuine old response cannot answer a new challenge"
    );
    let old_same_challenge = fixture.clients[&node("n1")]
        .observe_current_native_range(&node("n0"), &request, &parent)
        .unwrap();
    fixture.wait_idle();
    let business3 = Uuid::now_v7();
    let mut reused = NativeReadQuorumRound::begin(
        fresh_metadata(&fixture, business3, &parent),
        business3,
        request.challenge,
        &node("n1"),
        &parent,
    )
    .unwrap();
    assert!(reused.record(old_same_challenge).is_err(),
        "even deliberate challenge reuse cannot relabel a genuine response under a new MQ context/business");
    assert!(fixture.clients[&node("n2")]
        .observe_current_native_range(&node("n0"), &request, &parent)
        .is_err());
    fixture.wait_idle();
    let mut wrong = request.clone();
    wrong
        .scope
        .members
        .get_mut(&node("n2"))
        .unwrap()
        .incarnation += 1;
    assert!(fixture.clients[&node("n1")]
        .observe_current_native_range(&node("n2"), &wrong, &parent)
        .is_err());
    fixture.wait_idle();
    wrong = request.clone();
    wrong
        .scope
        .members
        .get_mut(&node("n2"))
        .unwrap()
        .active_certificate_sha256 = Some("00".repeat(32));
    assert!(fixture.clients[&node("n1")]
        .observe_current_native_range(&node("n2"), &wrong, &parent)
        .is_err());
    fixture.wait_idle();
    // An authenticated loopback RPC response cannot substitute for the actual
    // same-service local proof required by the candidate's final revalidation.
    let actual_remote_local: NativeRangeReadObservation = fixture.clients[&node("n1")]
        .observe_current_native_range(&node("n1"), &request, &parent)
        .unwrap();
    let mut round = round;
    assert!(round.record(actual_remote_local).is_err());
    fixture.wait_idle();
}
#[test]
fn native_read_rpc_actual_tls_missing_quorum_never_returns_candidate() {
    let mut fixture = live_fixture();
    let parent = CancellationToken::uncancelable()
        .child_with_deadline(Instant::now() + Duration::from_secs(2));
    let business = Uuid::now_v7();
    let metadata = fresh_metadata(&fixture, business, &parent);
    // Capture genuine MQ first, then stop only this fixture's actual n0/n2
    // data/metadata listeners. Candidate has one actual local native point.
    fixture.servers.remove(2).join().unwrap();
    fixture.servers.remove(0).join().unwrap();
    assert!(fixture.runtimes[&node("n1")]
        .acquire_native_read_candidate(metadata, business, Uuid::now_v7(), &parent)
        .is_err());
    fixture.wait_idle();
}

#[test]
fn native_read_rpc_actual_local_io_context_outlives_canceled_collector() {
    let fixture = live_fixture();
    let parent = CancellationToken::uncancelable();
    let business = Uuid::now_v7();
    let metadata = fresh_metadata(&fixture, business, &parent);
    let runtime = Arc::clone(&fixture.runtimes[&node("n1")]);
    let barrier = runtime.test_hold_native_read_io();
    let worker = Arc::clone(&runtime);
    let cancellation = parent.clone();
    let thread = std::thread::spawn(move || {
        worker
            .acquire_native_read_candidate(metadata, business, Uuid::now_v7(), &cancellation)
            .is_err()
    });
    let until = Instant::now() + Duration::from_secs(2);
    while !barrier.entered() {
        assert!(Instant::now() < until);
        std::thread::sleep(Duration::from_millis(1));
    }
    assert!(fixture.native.services[&node("n1")].try_lock().is_some());
    assert!(fixture.native.databases[&node("n1")].try_write().is_some());
    parent.cancel();
    assert!(thread.join().unwrap());
    assert!(
        runtime.test_active_context_count() > 0,
        "caller cancellation must not release the context before actual local worker return"
    );
    barrier.release();
    fixture.wait_idle();
    assert_eq!(runtime.test_active_context_count(), 0);
}

#[test]
fn native_read_rpc_actual_tls_majority_returns_while_minority_worker_retains_context() {
    let fixture = live_fixture();
    let parent = CancellationToken::uncancelable();
    let business = Uuid::now_v7();
    let metadata = fresh_metadata(&fixture, business, &parent);
    fixture.read_hold.armed.store(true, Ordering::SeqCst);
    let candidate = fixture.runtimes[&node("n1")]
        .acquire_native_read_candidate(metadata, business, Uuid::now_v7(), &parent)
        .unwrap();
    candidate.require_lifetime().unwrap();
    assert_eq!(candidate.point().0, 1);
    assert!(candidate.matching_member_ids().contains(&node("n1")));
    assert!(candidate.matching_member_ids().contains(&node("n2")));
    assert!(fixture.read_hold.entered.load(Ordering::SeqCst));
    assert!(fixture.runtimes[&node("n1")].test_active_context_count() > 0);
    fixture.read_hold.released.store(true, Ordering::SeqCst);
    fixture.wait_idle();
    candidate.require_lifetime().unwrap();
}

#[test]
fn native_read_rpc_actual_tls_expired_healthy_attempt_retries_without_renewing_parent() {
    let mut fixture = live_fixture();
    let parent = CancellationToken::uncancelable()
        .child_with_deadline(Instant::now() + Duration::from_secs(5));
    let business = Uuid::now_v7();
    let metadata = fresh_metadata(&fixture, business, &parent);
    let original_deadline = metadata.deadline();
    fixture.servers.remove(0).join().unwrap();
    fixture.read_expired_once.store(true, Ordering::SeqCst);
    let candidate = fixture.runtimes[&node("n1")]
        .acquire_native_read_candidate(metadata, business, Uuid::now_v7(), &parent)
        .unwrap();
    assert!(!fixture.read_expired_once.load(Ordering::SeqCst));
    assert!(
        fixture.read_n2_successful_calls.load(Ordering::SeqCst) >= 2,
        "candidate needs a second actual successful TLS RPC; the expired first reply never votes"
    );
    assert_eq!(
        candidate.matching_member_ids(),
        &std::collections::BTreeSet::from([node("n1"), node("n2")])
    );
    candidate.require_lifetime().unwrap();
    fixture.wait_idle();
    // Revalidation neither renews the actual MQ round nor the caller budget.
    let mut cancellation_checked = false;
    while Instant::now() < original_deadline {
        cancellation_checked = true;
        std::thread::sleep(Duration::from_millis(1));
    }
    assert!(cancellation_checked);
    assert!(candidate.require_lifetime().is_err());
}
