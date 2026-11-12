//! Actual native factories/MQ/metadata votes/physical bound learner ACK. These
//! metadata-only listeners remain APP-closed; no data Serving is manufactured.
use super::*;
use crate::distribution_range_consensus::RangeWriteCoordinator;
type Coordinator = RangeWriteCoordinator<TcpClusterRelocationTransport>;

fn writer(
    f: &EnrollmentFixture,
    legacy_none: bool,
) -> (Arc<Coordinator>, crate::db::InstalledNativeWriterHostGuard) {
    let (metadata, business) = f.current();
    let mut policy = {
        let service = f.launches[0].service.lock();
        let database = f.seeds[0].database.read();
        service
            .prepare_native_host_writer_enrollment(&database, metadata, business)
            .unwrap()
    };
    if legacy_none {
        policy.test_legacy_policy_without_member_continuity();
    }
    let cluster = f.launches[0].topology.read().cluster_id.clone();
    let coordinator = Arc::new(
        RangeWriteCoordinator::new_initial_schema_fenced(
            cluster,
            node(0),
            Arc::clone(&f.launches[0].topology),
            Arc::clone(&f.launches[0].service),
            Arc::clone(&f.launches[0].transport),
        )
        .unwrap(),
    );
    let guard = f.seeds[0]
        .database
        .read()
        .install_enrolled_native_coordinator(Arc::clone(&coordinator), policy)
        .unwrap();
    assert!(f.launches[0].service.lock().initial_consensus_only());
    for range in f.launches[0].topology.read().ranges.values() {
        assert!(!coordinator.initial_data_leader_admission_observed(range));
    }
    (coordinator, guard)
}
fn refresh(
    f: &EnrollmentFixture,
    coordinator: &Arc<Coordinator>,
    guard: &crate::db::InstalledNativeWriterHostGuard,
) -> crate::Result<()> {
    let (metadata, business) = f.current();
    let parent = CancellationToken::uncancelable()
        .child_with_deadline(Instant::now() + Duration::from_secs(5));
    let database = f.seeds[0].database.read();
    let prepared = ClusterDataNodeService::prepare_native_writer_registry_refresh(
        &f.launches[0].service,
        &database,
        coordinator,
        guard,
        metadata,
        business,
    )?;
    let result = guard.refresh_metadata_registry(&database, coordinator, prepared, &parent);
    parent.check()?;
    result
}
#[test]
fn native_initial_data_registry_legacy_none_policy_refuses_retroactive_continuity_capture() {
    let f = EnrollmentFixture::new();
    let (coordinator, guard) = writer(&f, true);
    let before = f.seeds[0]
        .database
        .read()
        .test_current_native_writer_policy_projection()
        .unwrap();
    assert!(
        before["facts"].get("member_continuity").is_none(),
        "old OPTIONAL-None layout must omit bytes"
    );
    assert_eq!(
        before["checksum_sha256"],
        guard.binding_diagnostic()["persisted_policy_checksum_sha256"]
    );
    let error = refresh(&f, &coordinator, &guard).unwrap_err();
    assert!(
        error
            .to_string()
            .contains("no recorded member continuity; no retroactive refresh"),
        "{error}"
    );
    assert!(f.seeds[0]
        .database
        .read()
        .test_current_native_writer_registry_audit()
        .unwrap()
        .is_none());
    assert_eq!(
        f.seeds[0]
            .database
            .read()
            .test_current_native_writer_policy_projection()
            .unwrap(),
        before
    );
    assert!(guard
        .retire_if_current(&f.seeds[0].database.read(), &coordinator)
        .unwrap());
    f.shutdown();
}
#[test]
fn native_initial_data_registry_actual_added_voter_remains_bound_on_later_member_mutation() {
    let f = EnrollmentFixture::new();
    let (_factory, mut host) = f.factory("actual-registry-added-voter");
    let (coordinator, guard) = writer(&f, false);
    let parent = CancellationToken::uncancelable();
    let id = start_owned(&f, &mut host);
    let actual = ready(&f, &parent);
    assert_eq!(actual.receiver_store_id(), id);
    let (post, business) = f.current();
    let now: u64 = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis()
        .try_into()
        .unwrap();
    let configuration = config(&f);
    let entry = f.launches[0]
        .metadata
        .lock()
        .propose_native_learner_promotion(&actual, &post, business, &configuration, now, &parent)
        .unwrap()
        .unwrap();
    f.launches[0].heartbeat_initial_metadata(&parent).unwrap();
    let request = f.launches[0]
        .metadata
        .lock()
        .append_request_for_peer(&node(4), 16)
        .unwrap();
    let ack = f.launches[0]
        .transport
        .append_metadata(&node(4), request)
        .unwrap();
    assert!(ack.success && ack.match_index >= entry.index);
    f.launches[0]
        .metadata
        .lock()
        .record_append_response(&node(4), &ack)
        .unwrap();
    let owned = host.test_owned_metadata().unwrap();
    assert!(owned.lock().status().voters.contains(&node(4)));
    assert!(owned.lock().status().commit_index >= entry.index);
    refresh(&f, &coordinator, &guard).unwrap();
    let accepted = f.seeds[0]
        .database
        .read()
        .test_current_native_writer_registry_audit()
        .unwrap()
        .unwrap();
    assert!(accepted["facts"]["member_continuity"]["n4"].is_object());
    assert_eq!(accepted["sequence"], 1);
    // Actual current-voter consensus commits an ordinary non-schema label
    // change on the PREVIOUSLY ADDED voter. No role/vote/log setters. Initial
    // four identity equality alone would incorrectly permit another audit.
    let mut changed = f.launches[0].metadata.lock().committed_topology().clone();
    changed
        .nodes
        .get_mut(&node(4))
        .unwrap()
        .labels
        .insert("registry.continuity.test".into(), "changed".into());
    f.commit(changed);
    let error = refresh(&f, &coordinator, &guard).unwrap_err();
    assert!(
        error
            .to_string()
            .contains("previously accepted member address/lifecycle/labels"),
        "{error}"
    );
    assert_eq!(
        f.seeds[0]
            .database
            .read()
            .test_current_native_writer_registry_audit()
            .unwrap()
            .unwrap(),
        accepted
    );
    assert!(f.launches[0].service.lock().initial_consensus_only());
    assert!(guard
        .retire_if_current(&f.seeds[0].database.read(), &coordinator)
        .unwrap());
    host.shutdown().unwrap();
    f.shutdown();
}
