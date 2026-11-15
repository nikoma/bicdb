//! The remote joined-handoff backend against the real four members: every
//! member is reached only through the product RPCs of the coordinator's own
//! transport. In-process handles are read only to compare outcomes.
use super::*;
use crate::distribution_native_host::joined_data_handoff::{
    JoinedHandoffBackend, JoinedHandoffMetadataHosts, NativeJoinedDataHandoff,
    NativeJoinedDataHandoffStage as Stage, NativeJoinedDataPlanningView,
    RemoteJoinedHandoffBackend, RemoteMembers, RemoteOldCutObservation as Observed,
};
use crate::distribution_range_handoff::{NativeRemoteOldOperationV1, NativeRemoteOldRequestV1};

const COMMIT_REACHED: &str = "remote backend test hosts commit nothing";

#[test]
fn native_remote_backend_close_round_matches_manual_remote_old_close() {
    run_registered_image_baseline_with_source(
        Case::Required,
        Some(SourceCase::RemoteBackendCloseRound),
    );
}
#[test]
fn native_remote_backend_fresh_instance_resync_reports_cut_started_only_after_close() {
    run_registered_image_baseline_with_source(
        Case::Required,
        Some(SourceCase::RemoteBackendResync),
    );
}
#[test]
fn native_remote_backend_driver_reports_unimplemented_stage_without_panic() {
    run_registered_image_baseline_with_source(
        Case::Required,
        Some(SourceCase::RemoteBackendDriverStub),
    );
}

pub(super) fn is_case(case: SourceCase) -> bool {
    matches!(
        case,
        SourceCase::RemoteBackendCloseRound
            | SourceCase::RemoteBackendResync
            | SourceCase::RemoteBackendDriverStub
    )
}

/// The committed metadata of the real metadata leader, read-only. A commit
/// through these hosts is a test failure signal, never a commit.
struct MetadataOnlyHosts {
    metadata: Arc<Mutex<crate::MetadataConsensusStore>>,
}
impl JoinedHandoffMetadataHosts for MetadataOnlyHosts {
    fn metadata_leader(&self) -> Result<ClusterNodeId> {
        Ok(node(0))
    }
    fn committed_topology(&self) -> Result<ClusterTopology> {
        Ok(self.metadata.lock().committed_topology().clone())
    }
    fn commit_topology(
        &mut self,
        _candidate: ClusterTopology,
        _parent: &CancellationToken,
    ) -> Result<()> {
        Err(BicDbError::Cluster(COMMIT_REACHED.into()))
    }
    fn planning_view(
        &mut self,
        _parent: &CancellationToken,
    ) -> Result<NativeJoinedDataPlanningView> {
        Err(BicDbError::Cluster(COMMIT_REACHED.into()))
    }
}

struct Moved<'a> {
    range: &'a crate::RangeDescriptor,
    relocation: &'a crate::distribution::RangeRelocation,
    /// Fixture indices of the old voters, in node order.
    old: Vec<usize>,
    leader: usize,
}
fn moved<'a>(pending: &'a ClusterTopology, original: &crate::RangeDescriptor) -> Moved<'a> {
    let range = pending.range_by_id(original.id).unwrap();
    let relocation = pending
        .relocations
        .values()
        .find(|plan| plan.range_id == range.id)
        .unwrap();
    let mut old = range
        .replicas
        .iter()
        .filter(|replica| replica.role == RangeReplicaRole::Voter && replica.node_id != node(4))
        .map(|replica| (0..4).find(|i| node(*i) == replica.node_id).unwrap())
        .collect::<Vec<_>>();
    old.sort_unstable();
    let leader = (0..4).find(|i| node(*i) == range.leader).unwrap();
    Moved {
        range,
        relocation,
        old,
        leader,
    }
}
fn native_records(
    services: &[Arc<Mutex<crate::ClusterDataNodeService>>],
    moved: &Moved<'_>,
) -> BTreeMap<String, Option<serde_json::Value>> {
    moved
        .old
        .iter()
        .map(|&i| {
            let database = services[i].lock().database();
            let record = database
                .read()
                .test_native_old_close_record(moved.range.id)
                .unwrap();
            (node(i).to_string(), record)
        })
        .collect()
}
/// The Close loop of `remote_old_seed`, unchanged: the range leader's
/// transport supplies the plan, the metadata leader's transport initiates.
fn manual_close(
    clients: &[Arc<TcpClusterRelocationTransport>],
    moved: &Moved<'_>,
) -> BTreeMap<String, serde_json::Value> {
    let business = Uuid::now_v7();
    let parent = budget(5000);
    let observation = clients[moved.leader]
        .acquire_metadata_read_observation(
            &node(0),
            business,
            moved.range.id,
            moved.range.epoch,
            Duration::from_secs(5),
            &parent,
        )
        .unwrap();
    let round = NativeJointOldRound::begin(observation, business, moved.relocation.id).unwrap();
    let plan = round.plan();
    drop(round);
    let mut outcome = BTreeMap::new();
    for recipient in plan.old_members().keys() {
        let parent = budget(5000);
        let business = Uuid::now_v7();
        let own = clients[0]
            .acquire_host_metadata_observation(&node(0), business, Duration::from_secs(5), &parent)
            .unwrap();
        let request = NativeRemoteOldRequestV1::from_plan(
            &plan,
            NativeRemoteOldOperationV1::Close,
            business,
            4000,
        )
        .unwrap();
        let actual = clients[0]
            .request_remote_native_old(recipient, &plan, &request, &own, &parent)
            .unwrap();
        actual.require_request_live().unwrap();
        outcome.insert(recipient.to_string(), member_outcome(actual.reply()));
    }
    outcome
}
/// What one member's Close answered, without the per-request fields.
fn member_outcome(
    reply: &crate::distribution_range_handoff::NativeRemoteOldReplyV1,
) -> serde_json::Value {
    serde_json::json!({
        "node_id":reply.node_id,"birth":reply.birth,"scope_sha256":reply.request.scope_sha256,
        "database_open":reply.database_open,"store_open":reply.store_open,
        "metadata_store_id":reply.metadata_store_id,
        "closed":reply.closed,"closed_native_record":reply.closed_native_record,
        "constraints":reply.constraints,
    })
}
fn backend_outcome(
    backend: &RemoteJoinedHandoffBackend,
    coordinator: usize,
) -> BTreeMap<String, serde_json::Value> {
    backend
        .closed_frames()
        .iter()
        .map(|(recipient, frame)| {
            assert_eq!(frame.actual_observer(), &node(coordinator));
            frame.reply().request.require_shape().unwrap();
            assert_eq!(&frame.reply().node_id, recipient);
            (recipient.to_string(), member_outcome(frame.reply()))
        })
        .collect()
}
fn resolve(
    backend: &RemoteJoinedHandoffBackend,
    hosts: &MetadataOnlyHosts,
    moved: &Moved<'_>,
) -> RemoteMembers {
    let members = backend
        .members(hosts, moved.range, moved.relocation)
        .unwrap();
    assert_eq!(members.metadata_leader, node(0));
    assert_eq!(members.target, node(4));
    assert_eq!(Some(&members.source), moved.relocation.source.as_ref());
    let mut old = members.old_voters.clone();
    old.sort();
    let mut expected = moved.old.iter().map(|&i| node(i)).collect::<Vec<_>>();
    expected.sort();
    assert_eq!(old, expected);
    assert_eq!(members.new_members.len(), members.old_voters.len());
    assert!(members.new_members.contains(&node(4)));
    assert!(!members.new_members.contains(&members.source));
    members
}

pub(super) fn exercise(
    case: SourceCase,
    clients: &[Arc<TcpClusterRelocationTransport>],
    services: &[Arc<Mutex<crate::ClusterDataNodeService>>],
    metadata: &[Arc<Mutex<crate::MetadataConsensusStore>>],
    pending: &ClusterTopology,
    ranges: &[crate::RangeDescriptor],
) {
    let mut hosts = MetadataOnlyHosts {
        metadata: metadata[0].clone(),
    };
    assert_eq!(&hosts.committed_topology().unwrap(), pending);
    match case {
        SourceCase::RemoteBackendCloseRound => {
            close_round(clients, services, &hosts, pending, ranges)
        }
        SourceCase::RemoteBackendResync => resync(clients, services, &mut hosts, pending, ranges),
        SourceCase::RemoteBackendDriverStub => {
            driver_stub(clients, services, &mut hosts, pending, ranges)
        }
        _ => unreachable!(),
    }
}

fn close_round(
    clients: &[Arc<TcpClusterRelocationTransport>],
    services: &[Arc<Mutex<crate::ClusterDataNodeService>>],
    hosts: &MetadataOnlyHosts,
    pending: &ClusterTopology,
    ranges: &[crate::RangeDescriptor],
) {
    // First range: the backend closes as the range leader's process, then
    // the manual loop of `remote_old_seed` confirms.
    let first = moved(pending, &ranges[0]);
    let mut backend = RemoteJoinedHandoffBackend::new(node(4), clients[first.leader].clone());
    let members = resolve(&backend, hosts, &first);
    assert!(native_records(services, &first)
        .values()
        .all(Option::is_none));
    assert!(!backend.cut_started());
    backend
        .close_old_members(&members, first.range, first.relocation, &budget(20_000))
        .unwrap();
    assert!(backend.cut_started());
    let by_backend = backend_outcome(&backend, first.leader);
    let records = native_records(services, &first);
    assert_eq!(by_backend.len(), first.old.len());
    for (recipient, outcome) in &by_backend {
        let record = records[recipient]
            .as_ref()
            .expect("each backend Close retained the member's native receipt");
        assert_eq!(
            outcome["closed"]["native_receipt_sha256"],
            record["checksum_sha256"]
        );
    }
    let by_manual = manual_close(clients, &first);
    assert_eq!(
        by_manual, by_backend,
        "the manual Close loop confirms exactly the backend's per-member outcome"
    );
    assert_eq!(
        native_records(services, &first),
        records,
        "the confirming Close rewrote no native receipt"
    );
    // A repeated backend round confirms the same outcome again.
    backend
        .close_old_members(&members, first.range, first.relocation, &budget(20_000))
        .unwrap();
    assert_eq!(backend_outcome(&backend, first.leader), by_backend);
    assert_eq!(native_records(services, &first), records);

    // Second range: the manual loop closes, then the backend confirms.
    let second = moved(pending, &ranges[1]);
    let mut backend = RemoteJoinedHandoffBackend::new(node(4), clients[second.leader].clone());
    let members = resolve(&backend, hosts, &second);
    // A round that fails before its first request has still started a cut.
    let mut wrong = members.clone();
    wrong.old_voters.pop();
    let error = backend
        .close_old_members(&wrong, second.range, second.relocation, &budget(20_000))
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("the pending plan differs from the resolved members"),
        "{error}"
    );
    assert!(backend.cut_started());
    assert!(backend.closed_frames().is_empty());
    assert!(native_records(services, &second)
        .values()
        .all(Option::is_none));
    let by_manual = manual_close(clients, &second);
    let records = native_records(services, &second);
    assert!(records.values().all(Option::is_some));
    backend
        .close_old_members(&members, second.range, second.relocation, &budget(20_000))
        .unwrap();
    assert_eq!(
        backend_outcome(&backend, second.leader),
        by_manual,
        "the backend round confirms exactly the manual loop's per-member outcome"
    );
    assert_eq!(native_records(services, &second), records);

    // The metadata leader's process is not the leader of either moved range
    // in this fixture: it is refused the range metadata observation the plan
    // comes from, before any member is asked to act.
    for moved in [&first, &second] {
        if moved.leader == 0 {
            continue;
        }
        let mut at_metadata_leader = RemoteJoinedHandoffBackend::new(node(4), clients[0].clone());
        let members = resolve(&at_metadata_leader, hosts, moved);
        let error = at_metadata_leader
            .close_old_members(&members, moved.range, moved.relocation, &budget(20_000))
            .unwrap_err()
            .to_string();
        assert!(
            error.contains("source round metadata")
                && error.contains("metadata acquisition requester/range authority changed"),
            "{error}"
        );
        assert!(at_metadata_leader.cut_started());
        assert!(at_metadata_leader.closed_frames().is_empty());
    }
}

fn resync(
    clients: &[Arc<TcpClusterRelocationTransport>],
    services: &[Arc<Mutex<crate::ClusterDataNodeService>>],
    hosts: &mut MetadataOnlyHosts,
    pending: &ClusterTopology,
    ranges: &[crate::RangeDescriptor],
) {
    for original in ranges {
        let moved = moved(pending, original);
        let members = resolve(
            &RemoteJoinedHandoffBackend::new(node(4), clients[moved.leader].clone()),
            hosts,
            &moved,
        );
        let observe = |observer: usize, _hosts: &mut MetadataOnlyHosts| {
            let mut fresh = RemoteJoinedHandoffBackend::new(node(4), clients[observer].clone());
            assert!(!fresh.cut_started());
            // The lenient observation itself: `resync` additionally refuses a
            // coordinator that is not both leaders before it observes.
            fresh
                .observe_cut(&members, moved.range, moved.relocation, &budget(20_000))
                .unwrap();
            (fresh.cut_started(), fresh.cut_observations().to_vec())
        };
        let strict = |observer: usize, hosts: &mut MetadataOnlyHosts| {
            let mut fresh = RemoteJoinedHandoffBackend::new(node(4), clients[observer].clone());
            let result =
                fresh.resync_before_abort(hosts, moved.range, moved.relocation, &budget(20_000));
            (
                fresh.cut_started(),
                result.map_err(|error| error.to_string()),
            )
        };
        let abort = |observer: usize, hosts: &mut MetadataOnlyHosts| {
            NativeJoinedDataHandoff::with_backend(
                moved.range.id,
                node(4),
                RemoteJoinedHandoffBackend::new(node(4), clients[observer].clone()),
            )
            .abort_before_cut(hosts, &budget(20_000))
            .unwrap_err()
            .to_string()
        };
        // Before any Close: no fresh instance reports a started cut.
        let before = observe(moved.leader, hosts);
        let before_strict = strict(moved.leader, hosts);
        let before_metadata_leader = (observe(0, hosts), strict(0, hosts));
        assert!(native_records(services, &moved)
            .values()
            .all(Option::is_none));

        let mut backend = RemoteJoinedHandoffBackend::new(node(4), clients[moved.leader].clone());
        backend
            .close_old_members(&members, moved.range, moved.relocation, &budget(20_000))
            .unwrap();
        assert!(backend.cut_started());
        let records = native_records(services, &moved);
        assert!(records.values().all(Option::is_some));

        // After the Close round: a FRESH instance re-derives it.
        let after = observe(moved.leader, hosts);
        let after_strict = strict(moved.leader, hosts);
        let after_metadata_leader = (observe(0, hosts), strict(0, hosts));
        // The driver refuses the abort of a fresh coordinator before any
        // epoch read or commit.
        let aborted = abort(moved.leader, hosts);
        let aborted_metadata_leader = abort(0, hosts);
        assert_eq!(
            native_records(services, &moved),
            records,
            "the read-only observation rewrote no native receipt"
        );
        let debug = serde_json::json!({
            "range":moved.range.id,"old":moved.old,"leader":moved.leader,
            "before":format!("{before:?}"),"before_strict":format!("{before_strict:?}"),
            "before_metadata_leader":format!("{before_metadata_leader:?}"),
            "after":format!("{after:?}"),"after_strict":format!("{after_strict:?}"),
            "after_metadata_leader":format!("{after_metadata_leader:?}"),
            "aborted":aborted,"aborted_metadata_leader":aborted_metadata_leader,
        });
        eprintln!("REMOTE_BACKEND_RESYNC {debug}");
        assert!(!before.0, "{debug:#}");
        assert_eq!(before.1.len(), moved.old.len(), "{debug:#}");
        assert!(
            before
                .1
                .iter()
                .all(|(_, answer)| *answer == Observed::Absent),
            "{debug:#}"
        );
        assert_eq!(before_strict, (false, Ok(())), "{debug:#}");
        assert!(after.0, "{debug:#}");
        assert_eq!(after.1.len(), moved.old.len(), "{debug:#}");
        assert!(
            after
                .1
                .iter()
                .all(|(_, answer)| *answer == Observed::Retained { closed: true }),
            "{debug:#}"
        );
        assert_eq!(after_strict, (true, Ok(())), "{debug:#}");
        assert!(
            aborted.contains("a source round was started") && !aborted.contains(COMMIT_REACHED),
            "{debug:#}"
        );
        if moved.leader != 0 {
            // The metadata leader's process cannot obtain the plan: the
            // lenient form learns nothing and the strict form refuses.
            for ((started, answers), (strict_started, refused)) in
                [&before_metadata_leader, &after_metadata_leader]
            {
                assert!(!started && !strict_started, "{debug:#}");
                assert_eq!(answers.len(), moved.old.len(), "{debug:#}");
                assert!(
                    answers.iter().all(|(_, answer)| matches!(
                        answer,
                        Observed::Inconclusive(reason) if reason.contains("pending plan unavailable")
                    )),
                    "{debug:#}"
                );
                assert!(
                    refused
                        .as_ref()
                        .is_err_and(|error| error
                            .contains("cannot establish that no source round was started")),
                    "{debug:#}"
                );
            }
            assert!(
                aborted_metadata_leader
                    .contains("cannot establish that no source round was started")
                    && !aborted_metadata_leader.contains(COMMIT_REACHED),
                "{debug:#}"
            );
        }
    }
}

fn driver_stub(
    clients: &[Arc<TcpClusterRelocationTransport>],
    services: &[Arc<Mutex<crate::ClusterDataNodeService>>],
    hosts: &mut MetadataOnlyHosts,
    pending: &ClusterTopology,
    ranges: &[crate::RangeDescriptor],
) {
    let moved = moved(pending, &ranges[0]);
    // A coordinator that is not the metadata leader is refused at the first
    // stage, whatever the requested stage, before any member is asked.
    let outsider = (1..4).find(|i| *i != moved.leader).unwrap_or(1);
    let mut handoff = NativeJoinedDataHandoff::with_backend(
        moved.range.id,
        node(4),
        RemoteJoinedHandoffBackend::new(node(4), clients[outsider].clone()),
    );
    for until in [
        Some(Stage::LearnerAllocated),
        Some(Stage::ImageVerified),
        Some(Stage::DecisionPublished),
        None,
    ] {
        let error = handoff
            .drive_until(hosts, until, &budget(20_000))
            .unwrap_err()
            .to_string();
        assert!(
            error.contains("remote joined handoff backend: coordinator")
                && error.contains("is not the metadata leader"),
            "{error}"
        );
        assert!(!error.contains(COMMIT_REACHED), "{error}");
    }
    assert!(!handoff.completed());
    assert!(native_records(services, &moved)
        .values()
        .all(Option::is_none));

    let mut backend = RemoteJoinedHandoffBackend::new(node(4), clients[outsider].clone());
    let members = resolve(&backend, hosts, &moved);
    let token = budget(20_000);
    // The source stage of such a coordinator starts no cut.
    let error = backend
        .cut_and_ship(hosts, &members, moved.range, moved.relocation, &token)
        .unwrap_err()
        .to_string();
    assert!(error.contains("is not the metadata leader"), "{error}");
    assert!(!backend.cut_started());
    assert!(backend.closed_frames().is_empty());
    assert!(native_records(services, &moved)
        .values()
        .all(Option::is_none));
    // Without an installed image and a certificate the later stages refuse.
    assert!(backend.cut_required() && !backend.decision_durable());
    let error = backend.decide(&members, &token).unwrap_err().to_string();
    assert!(
        error.contains("needs the retained plan and source manifest"),
        "{error}"
    );
    let error = backend.require_decision().unwrap_err().to_string();
    assert!(error.contains("decision certificate absent"), "{error}");
    // The member step that does not exist yet names itself.
    let refused = |result: Result<()>, stage: &str| {
        let error = result.unwrap_err().to_string();
        assert!(
            error.contains(&format!(
                "remote joined handoff backend: {stage} is not implemented"
            )),
            "{error}"
        );
    };
    // The activation sends bridge steps, which the new members admit from
    // the metadata leader: another coordinator is refused before a request.
    let error = backend
        .activate(hosts, &members, &token)
        .unwrap_err()
        .to_string();
    assert!(
        error.contains("remote joined handoff backend: coordinator")
            && error.contains("is not the metadata leader")
            && error.contains("admit bridge steps from the metadata leader"),
        "{error}"
    );
    refused(backend.serve(hosts, &token).map(|_| ()), "target serve");
    backend.forget_aborted_allocation();
    assert!(backend.closed_frames().is_empty());
}
