//! The production range-move controller and the metadata leader's own
//! metadata hosts, against the five real members.
//!
//! What is real here: the metadata hosts (`NativeLeaderRangeMoveHosts`) of the
//! actual control host of the metadata leader, its metadata consensus with
//! the other four members over their sockets, and the slot's worker. What is
//! scripted: the member-side stages, through a fake `JoinedHandoffBackend`,
//! wherever the remote backend cannot perform them yet. The lifecycle test
//! also replaces the metadata hosts with an in-memory copy of the fixture's
//! committed configuration, so that leadership can be scripted.
use super::*;
use crate::distribution::NativeJointHandoffStageV1;
use crate::distribution_consensus::HostMetadataReadObservation;
use crate::distribution_native_host::joined_data_handoff::{
    JoinedHandoffBackend, RemoteJoinedHandoffBackend,
};
use crate::distribution_native_host::range_move::{
    request_range_move, request_range_move_abort, NativeRangeMoveController, NativeRangeMoveHosts,
    NativeRangeMoveSlot, NativeRangeMoveStatus,
};
use std::collections::VecDeque;

#[derive(Default)]
struct FakeState {
    /// Outcomes of the next target admissions: `Some(text)` fails with that
    /// text, `None` times out. Empty: the admission succeeds.
    admissions: VecDeque<Option<String>>,
    built: usize,
    cut_started: bool,
    image: bool,
    decided: bool,
    forgotten: usize,
    old_epoch: Option<u64>,
}
struct FakeBackend {
    state: Arc<Mutex<FakeState>>,
}
fn fake(state: &Arc<Mutex<FakeState>>) -> Box<dyn FnMut() -> FakeBackend + Send> {
    let state = Arc::clone(state);
    Box::new(move || {
        state.lock().built += 1;
        FakeBackend {
            state: Arc::clone(&state),
        }
    })
}
fn scripted(detail: &str) -> BicDbError {
    BicDbError::Cluster(format!("fake backend: {detail}"))
}
impl JoinedHandoffBackend for FakeBackend {
    type Members = ();
    type Hosts = dyn JoinedHandoffMetadataHosts;
    fn cut_started(&self) -> bool {
        self.state.lock().cut_started
    }
    fn old_epoch(
        &self,
        _hosts: &Self::Hosts,
        _node: &ClusterNodeId,
        _range: &crate::RangeDescriptor,
    ) -> Result<u64> {
        self.state
            .lock()
            .old_epoch
            .ok_or_else(|| scripted("old voter epoch is unreadable"))
    }
    fn forget_aborted_allocation(&mut self) {
        self.state.lock().forgotten += 1;
    }
    fn admit_target(
        &mut self,
        _hosts: &mut Self::Hosts,
        _parent: &CancellationToken,
    ) -> Result<()> {
        match self.state.lock().admissions.pop_front() {
            None => Ok(()),
            Some(None) => Err(BicDbError::QueryTimedOut),
            Some(Some(text)) => Err(scripted(&text)),
        }
    }
    fn members(
        &self,
        _hosts: &Self::Hosts,
        _range: &crate::RangeDescriptor,
        _relocation: &RangeRelocation,
    ) -> Result<()> {
        Ok(())
    }
    fn cut_required(&self) -> bool {
        !self.state.lock().image
    }
    fn cut_and_ship(
        &mut self,
        _hosts: &mut Self::Hosts,
        _members: &(),
        _range: &crate::RangeDescriptor,
        _relocation: &RangeRelocation,
        _parent: &CancellationToken,
    ) -> Result<()> {
        let mut state = self.state.lock();
        state.cut_started = true;
        state.image = true;
        Ok(())
    }
    fn decision_durable(&self) -> bool {
        self.state.lock().decided
    }
    fn decide(&mut self, _members: &(), _parent: &CancellationToken) -> Result<()> {
        self.state.lock().decided = true;
        Ok(())
    }
    fn require_decision(&self) -> Result<()> {
        Err(scripted("no durable decision is held"))
    }
    fn stage_candidate(
        &mut self,
        _committed: Option<NativeJointHandoffStageV1>,
        _view: &mut crate::DistributionStore,
        _fresh: &HostMetadataReadObservation,
        _business: Uuid,
        _actor: &ClusterNodeId,
        _now: u64,
    ) -> Result<()> {
        Err(scripted("no membership stage"))
    }
    fn activate(
        &mut self,
        _hosts: &mut Self::Hosts,
        _members: &(),
        _parent: &CancellationToken,
    ) -> Result<()> {
        Err(scripted("no activation"))
    }
    fn serve(&mut self, _hosts: &mut Self::Hosts, _parent: &CancellationToken) -> Result<usize> {
        Err(scripted("no serving"))
    }
}

/// In-memory metadata hosts over a copy of the committed configuration.
struct ScriptedHosts {
    leader: ClusterNodeId,
    topology: ClusterTopology,
    commits: usize,
    settle: Option<String>,
    /// This node cannot read its own metadata.
    unreadable: bool,
}
impl JoinedHandoffMetadataHosts for ScriptedHosts {
    fn metadata_leader(&self) -> Result<ClusterNodeId> {
        if self.unreadable {
            return Err(scripted("metadata store busy"));
        }
        Ok(self.leader.clone())
    }
    fn committed_topology(&self) -> Result<ClusterTopology> {
        Ok(self.topology.clone())
    }
    fn commit_topology(
        &mut self,
        candidate: ClusterTopology,
        _parent: &CancellationToken,
    ) -> Result<()> {
        self.topology = candidate;
        self.commits += 1;
        Ok(())
    }
    fn planning_view(
        &mut self,
        _parent: &CancellationToken,
    ) -> Result<NativeJoinedDataPlanningView> {
        Err(scripted("no planning view"))
    }
}
impl NativeRangeMoveHosts for ScriptedHosts {
    fn metadata(&mut self) -> &mut (dyn JoinedHandoffMetadataHosts + 'static) {
        self
    }
    fn settle(&mut self, _parent: &CancellationToken) -> Result<()> {
        match self.settle.take() {
            Some(text) => Err(scripted(&text)),
            None => Ok(()),
        }
    }
}

/// A range the metadata leader (member 0) leads. The wave's coordinator must
/// be both.
fn led_by_metadata_leader(topology: &ClusterTopology) -> crate::RangeDescriptor {
    topology
        .ranges
        .values()
        .find(|range| range.leader == node(0))
        .unwrap_or_else(|| {
            panic!(
                "the fixture has no range led by the metadata leader: {:?}",
                topology
                    .ranges
                    .values()
                    .map(|range| (range.id, range.leader.clone()))
                    .collect::<Vec<_>>()
            )
        })
        .clone()
}

fn step<B>(slot: &mut Option<NativeRangeMoveSlot<B>>) -> NativeRangeMoveStatus
where
    B: JoinedHandoffBackend<Hosts = dyn JoinedHandoffMetadataHosts> + Send + 'static,
{
    let slot = slot.as_mut().unwrap();
    slot.advance(&CancellationToken::uncancelable()).unwrap();
    slot.wait_step(&budget(120_000)).unwrap();
    let status = slot.status();
    assert!(!status.step_in_flight);
    eprintln!("NATIVE_RANGE_MOVE status={status:?}");
    status
}

/// (a) The production metadata hosts commit through the real consensus and
/// return only after the commit and after the participating members route by
/// it, with no topology installed by hand on any member.
#[test]
fn native_range_move_leader_hosts_commit_returns_after_commit_and_member_routing() {
    run("range-move-hosts-commit", |five| {
        let before = committed(five);
        let range = led_by_metadata_leader(&before);
        let mut hosts = five.controls[0].range_move_hosts().unwrap();
        assert_eq!(hosts.metadata_leader().unwrap(), node(0));
        assert_eq!(hosts.committed_topology().unwrap(), before);
        let planning = hosts.planning_view(&budget(5000)).unwrap();
        assert_eq!(planning.view.topology(), &before);
        drop(planning);

        // A member that is not the metadata leader names the leader and
        // cannot commit; nothing is proposed.
        let mut follower = five.controls[1].range_move_hosts().unwrap();
        let candidate =
            crate::distribution_native_host::joined_data_handoff::test_plan_learner_allocation(
                &before,
                range.id,
                &node(4),
            )
            .unwrap();
        let refused = follower
            .commit_topology(candidate.clone(), &budget(10_000))
            .unwrap_err()
            .to_string();
        eprintln!("NATIVE_RANGE_MOVE follower commit refused: {refused}");
        assert_eq!(committed(five), before);
        let status = five.metadata[0].lock().status();
        assert_eq!(status.last_log_index, status.commit_index);

        hosts
            .commit_topology(candidate.clone(), &budget(30_000))
            .unwrap();
        // Committed on the leader's own store when the call returns.
        assert_eq!(committed(five), candidate);
        assert!(hosts.test_unsettled().is_empty());
        // Every participating member's own store committed it, and the
        // routing table of each original owner's DATA client follows it. No
        // `install_topology` was called on a member by this test.
        let participants = candidate
            .range_by_id(range.id)
            .unwrap()
            .replicas
            .iter()
            .map(|replica| index_of(&replica.node_id))
            .collect::<Vec<_>>();
        assert_eq!(participants.len(), 4);
        assert!(participants.contains(&4));
        for &i in &participants {
            assert_eq!(
                five.metadata[i].lock().committed_topology(),
                &candidate,
                "member {i} has not committed the configuration"
            );
            if i < 4 {
                assert_eq!(
                    five.controls[i].test_routing_generation(),
                    candidate.generation,
                    "member {i} does not route by the committed configuration"
                );
            }
        }
    });
}

/// (b) Controller lifecycle over scripted hosts and a scripted backend.
#[test]
fn native_range_move_controller_lifecycle_with_scripted_backend() {
    run("range-move-lifecycle", |five| {
        let original = committed(five);
        let range = original.ranges.values().next().unwrap().clone();
        let local = range.leader.clone();
        let other = (0..4).map(node).find(|id| *id != local).unwrap();
        let parent = CancellationToken::uncancelable();
        let mut hosts = ScriptedHosts {
            leader: local.clone(),
            topology: original.clone(),
            commits: 0,
            settle: None,
            unreadable: false,
        };
        let state = Arc::new(Mutex::new(FakeState::default()));

        // Creation is refused unless the node is both leaders, and for a
        // target that cannot take the learner allocation.
        hosts.leader = other.clone();
        let refused =
            NativeRangeMoveController::new(local.clone(), range.id, node(4), &hosts, fake(&state))
                .err()
                .unwrap()
                .to_string();
        assert!(refused.contains("is not the metadata leader"), "{refused}");
        let refused =
            NativeRangeMoveController::new(other.clone(), range.id, node(4), &hosts, fake(&state))
                .err()
                .unwrap()
                .to_string();
        assert!(
            refused.contains("is not the leader of range")
                && refused.contains("both the metadata leader and the leader of the range"),
            "{refused}"
        );
        hosts.leader = local.clone();
        let refused = NativeRangeMoveController::new(
            local.clone(),
            range.id,
            other.clone(),
            &hosts,
            fake(&state),
        )
        .err()
        .unwrap()
        .to_string();
        assert!(
            refused.contains("target already holds a range replica"),
            "{refused}"
        );

        let mut controller =
            NativeRangeMoveController::new(local.clone(), range.id, node(4), &hosts, fake(&state))
                .unwrap()
                .with_backoff(Duration::ZERO, Duration::ZERO);
        let status = controller.status();
        assert_eq!(
            (status.stage, status.next_stage, status.attempts),
            (None, Some("target_admitted"), 0)
        );
        assert_eq!(state.lock().built, 0);

        // One stage per step.
        controller.tick(&mut hosts, &parent);
        let status = controller.status();
        assert_eq!(
            (status.stage, status.next_stage, status.attempts),
            (Some("target_admitted"), Some("learner_allocated"), 1)
        );
        assert_eq!(hosts.commits, 0);
        controller.tick(&mut hosts, &parent);
        let status = controller.status();
        assert_eq!(status.stage, Some("learner_allocated"));
        assert_eq!(hosts.commits, 1);
        assert_eq!(
            hosts.topology,
            crate::distribution_native_host::joined_data_handoff::test_plan_learner_allocation(
                &original,
                range.id,
                &node(4)
            )
            .map(|mut planned| {
                // Identity of the session and the timestamps are per plan.
                planned.relocations = hosts.topology.relocations.clone();
                planned
            })
            .unwrap()
        );

        // An error is recorded and the same stage is driven again.
        state
            .lock()
            .admissions
            .push_back(Some("member unreachable".into()));
        controller.tick(&mut hosts, &parent);
        let status = controller.status();
        assert_eq!(status.stage, Some("learner_allocated"));
        assert_eq!(
            status.last_error.as_deref(),
            Some("blocked at image_verified: cluster error: joined data handoff: target admission: cluster error: fake backend: member unreachable")
        );
        assert_eq!(status.attempts, 3);
        assert!(!state.lock().cut_started);
        controller.tick(&mut hosts, &parent);
        let status = controller.status();
        assert_eq!(
            (status.stage, status.last_error.clone(), status.attempts),
            (Some("image_verified"), None, 4)
        );
        // A timeout is a normal repetition.
        state.lock().admissions.push_back(None);
        controller.tick(&mut hosts, &parent);
        let status = controller.status();
        assert_eq!(
            status.last_error.as_deref(),
            Some("blocked at decision_published: timed out; repeated on the next step")
        );
        // A failed confirmation of an earlier commit blocks the step too.
        hosts.settle = Some("member 3 has not acknowledged".into());
        controller.tick(&mut hosts, &parent);
        assert_eq!(
            controller.status().last_error.as_deref(),
            Some("blocked at route confirmation: cluster error: fake backend: member 3 has not acknowledged")
        );
        assert!(!state.lock().decided);
        controller.tick(&mut hosts, &parent);
        let status = controller.status();
        assert_eq!(
            (status.stage, status.last_error.clone()),
            (Some("decision_published"), None)
        );

        // After the source cut the abort is refused with the driver's text.
        let refusal = controller
            .abort(&mut hosts, &parent)
            .unwrap_err()
            .to_string();
        assert_eq!(
            refusal,
            "cluster error: joined data handoff: a source round was started and a permanent handoff promise may exist; the handoff can only be resumed"
        );
        let status = controller.status();
        assert_eq!(status.last_error, Some(format!("abort refused: {refusal}")));
        assert!(!status.aborted);
        assert_eq!(hosts.commits, 1);

        // The node cannot read its own metadata: nothing is driven and the
        // driver is kept, because no other coordinator is named.
        let built = state.lock().built;
        let attempts = controller.status().attempts;
        hosts.unreadable = true;
        controller.tick(&mut hosts, &parent);
        let status = controller.status();
        assert!(
            status
                .paused
                .as_deref()
                .is_some_and(|reason| reason.contains("the metadata leader is unknown")),
            "{status:?}"
        );
        assert_eq!(
            (status.stage, status.attempts),
            (Some("decision_published"), attempts)
        );
        hosts.unreadable = false;

        // The node loses the metadata leadership: the controller pauses and
        // keeps identity data only.
        hosts.leader = other.clone();
        controller.tick(&mut hosts, &parent);
        let status = controller.status();
        assert!(
            status
                .paused
                .as_deref()
                .is_some_and(|reason| reason.contains("is not the metadata leader")),
            "{status:?}"
        );
        assert_eq!(
            (status.stage, status.attempts, status.completed),
            (None, attempts, false)
        );
        assert_eq!((status.range, status.target.clone()), (range.id, node(4)));
        let refused = controller
            .abort(&mut hosts, &parent)
            .unwrap_err()
            .to_string();
        assert!(refused.contains("abort refused"), "{refused}");
        // It holds again: a new driver, and the stage is derived again.
        hosts.leader = local.clone();
        controller.tick(&mut hosts, &parent);
        let status = controller.status();
        assert_eq!(
            (status.paused.clone(), status.stage, status.attempts),
            (None, Some("target_admitted"), attempts + 1)
        );
        assert_eq!(state.lock().built, built + 1);

        // The membership stages need what the scripted backend cannot give:
        // blocked, reported, not fatal.
        let mut status = controller.status();
        for _ in 0..8 {
            controller.tick(&mut hosts, &parent);
            status = controller.status();
            if status.last_error.is_some() {
                break;
            }
        }
        assert_eq!(status.stage, Some("decision_published"));
        assert_eq!(
            status.last_error.as_deref(),
            Some("blocked at certified: cluster error: fake backend: no durable decision is held")
        );

        // A failure backs off: a step inside the backoff does nothing.
        let mut slow =
            NativeRangeMoveController::new(local.clone(), range.id, node(4), &hosts, fake(&state))
                .unwrap()
                .with_backoff(Duration::from_secs(3600), Duration::from_secs(3600));
        state
            .lock()
            .admissions
            .push_back(Some("member unreachable".into()));
        slow.tick(&mut hosts, &parent);
        slow.tick(&mut hosts, &parent);
        let status = slow.status();
        assert_eq!(status.attempts, 1);
        assert!(status
            .last_error
            .as_deref()
            .is_some_and(|error| error.starts_with("blocked at target_admitted: ")));
        assert!(state.lock().admissions.is_empty());

        // The committed metadata records the completed handoff: the next
        // step reports completion and later steps change nothing.
        let mut done = hosts.topology.clone();
        let plan = done
            .relocations
            .values_mut()
            .find(|plan| plan.range_id == range.id)
            .unwrap();
        let session = plan.handoff_session.clone().unwrap();
        let digest = |byte: char| std::iter::repeat_n(byte, 64).collect::<String>();
        plan.native_joint_handoff_v1 = Some(
            serde_json::from_value(serde_json::json!({
                "version": 1,
                "session_id": session.session_id,
                "nonce": session.nonce,
                "decision_sha256": digest('d'),
                "old_configuration_sha256": digest('1'),
                "new_configuration_sha256": digest('2'),
                "range_index": 3,
                "logical_genesis_sha256": digest('3'),
                "logical_history_sha256": digest('4'),
                "source_snapshot_db_sequence": 5,
                "target_local_snapshot_db_sequence": 6,
                "stage": "Cleaned",
                "activated_quorum_sha256": digest('a'),
                "source_cleanup_sha256": digest('c'),
            }))
            .unwrap(),
        );
        plan.phase = RelocationPhase::Completed;
        let target_replica = plan.target_replica_id;
        let descriptor = done
            .ranges
            .values_mut()
            .find(|descriptor| descriptor.id == range.id)
            .unwrap();
        descriptor
            .replicas
            .iter_mut()
            .find(|replica| replica.id == target_replica)
            .unwrap()
            .role = RangeReplicaRole::Voter;
        descriptor.leader = node(4);
        hosts.topology = done;
        controller.tick(&mut hosts, &parent);
        let status = controller.status();
        assert_eq!(
            (
                status.completed,
                status.stage,
                status.next_stage,
                status.last_error.clone(),
                status.paused.clone()
            ),
            (true, Some("source_retired"), None, None, None)
        );
        let attempts = status.attempts;
        controller.tick(&mut hosts, &parent);
        assert_eq!(controller.status().attempts, attempts);
        let refused = controller
            .abort(&mut hosts, &parent)
            .unwrap_err()
            .to_string();
        assert!(refused.contains("the move is complete"), "{refused}");
    });
}

/// (a, d) The slot over the production metadata hosts: the learner allocation
/// and its abort are committed by the real consensus; a second move is
/// refused while one is unfinished; after the abort a new move is accepted.
#[test]
fn native_range_move_abort_before_cut_commits_and_a_new_move_is_accepted() {
    run("range-move-abort", |five| {
        let original = committed(five);
        let range = led_by_metadata_leader(&original);
        let another = original
            .ranges
            .values()
            .find(|candidate| candidate.id != range.id)
            .unwrap()
            .id;
        let state = Arc::new(Mutex::new(FakeState {
            old_epoch: Some(range.epoch),
            ..FakeState::default()
        }));
        let mut slot: Option<NativeRangeMoveSlot<FakeBackend>> = None;
        let start = |five: &Five, range: crate::RangeId, state: &Arc<Mutex<FakeState>>| {
            NativeRangeMoveSlot::start_with(
                five.controls[0].range_move_hosts()?,
                range,
                node(4),
                fake(state),
            )
        };
        // A host that is not both leaders refuses the request.
        let refused = NativeRangeMoveSlot::start_with(
            five.controls[1].range_move_hosts().unwrap(),
            range.id,
            node(4),
            fake(&state),
        )
        .err()
        .unwrap()
        .to_string();
        assert!(refused.contains("is not the metadata leader"), "{refused}");
        // Nothing to abort before a move exists.
        let refused = request_range_move_abort(&mut slot, range.id, &budget(5000))
            .unwrap_err()
            .to_string();
        assert!(refused.contains("has no move of range"), "{refused}");

        request_range_move(&mut slot, range.id, &node(4), &budget(5000), || {
            start(five, range.id, &state)
        })
        .unwrap();
        assert_eq!(step(&mut slot).stage, Some("target_admitted"));
        assert_eq!(committed(five), original);
        let status = step(&mut slot);
        assert_eq!(
            (status.stage, status.last_error.clone()),
            (Some("learner_allocated"), None)
        );
        // The allocation is committed by the real consensus when the step
        // has returned, on every participating member.
        let pending = committed(five);
        assert_eq!(pending.generation, original.generation + 1);
        let allocated = pending.range_by_id(range.id).unwrap().clone();
        assert_eq!(allocated.epoch, range.epoch + 1);
        assert!(allocated
            .replicas
            .iter()
            .any(|r| r.node_id == node(4) && r.role == RangeReplicaRole::Learner));
        for replica in &allocated.replicas {
            let i = index_of(&replica.node_id);
            assert_eq!(five.metadata[i].lock().committed_topology(), &pending);
            if i < 4 {
                assert_eq!(
                    five.controls[i].test_routing_generation(),
                    pending.generation
                );
            }
        }

        // One move at a time per host; the same request is confirmed and
        // does not replace the controller.
        let refused = request_range_move(&mut slot, another, &node(4), &budget(5000), || {
            start(five, another, &state)
        })
        .unwrap_err()
        .to_string();
        assert!(refused.contains("one move at a time per host"), "{refused}");
        request_range_move(&mut slot, range.id, &node(4), &budget(5000), || {
            panic!("an unfinished move must not be replaced")
        })
        .unwrap();
        assert_eq!(slot.as_ref().unwrap().status().attempts, 2);
        let refused = request_range_move_abort(&mut slot, another, &budget(5000))
            .unwrap_err()
            .to_string();
        assert!(refused.contains("has no move of range"), "{refused}");

        // The abort commits the original descriptor again.
        request_range_move_abort(&mut slot, range.id, &budget(30_000)).unwrap();
        let status = slot.as_ref().unwrap().status();
        assert!(status.aborted && !status.completed, "{status:?}");
        assert_eq!(status.next_stage, None);
        assert_eq!(state.lock().forgotten, 1);
        let aborted = committed(five);
        assert_eq!(aborted.generation, pending.generation + 1);
        assert_eq!(aborted.range_by_id(range.id).unwrap(), &range);
        assert!(aborted
            .relocations
            .values()
            .all(|plan| plan.range_id != range.id));
        for replica in &range.replicas {
            let i = index_of(&replica.node_id);
            assert_eq!(five.metadata[i].lock().committed_topology(), &aborted);
            assert_eq!(
                five.controls[i].test_routing_generation(),
                aborted.generation
            );
        }
        // An aborted move does nothing on later steps.
        let status = step(&mut slot);
        assert_eq!(status.attempts, 2);
        assert_eq!(committed(five), aborted);

        // A new move is accepted and starts from the beginning.
        request_range_move(&mut slot, range.id, &node(4), &budget(5000), || {
            start(five, range.id, &state)
        })
        .unwrap();
        let status = slot.as_ref().unwrap().status();
        assert_eq!(
            (status.aborted, status.attempts, status.next_stage),
            (false, 0, Some("target_admitted"))
        );
        assert_eq!(step(&mut slot).stage, Some("target_admitted"));
        slot.as_mut().unwrap().stop(&budget(5000)).unwrap();
    });
}

/// (c) The production move with the remote backend in its current state: the
/// stage it cannot perform is reported as blocked in the status; the host and
/// its metadata are unaffected and the step is repeated after the backoff.
#[test]
fn native_range_move_remote_backend_reports_blocked_stage_without_failing_host() {
    run("range-move-remote-blocked", |five| {
        let original = committed(five);
        let range = led_by_metadata_leader(&original);
        let mut slot: Option<NativeRangeMoveSlot<RemoteJoinedHandoffBackend>> = None;
        request_range_move(&mut slot, range.id, &node(4), &budget(5000), || {
            NativeRangeMoveSlot::start(&five.controls[0], range.id, node(4))
        })
        .unwrap();
        let mut status = step(&mut slot);
        for _ in 0..12 {
            if status.last_error.is_some() {
                break;
            }
            status = step(&mut slot);
        }
        let error = status.last_error.clone().expect("a blocked stage");
        assert!(
            error.starts_with(&format!(
                "blocked at {}: ",
                status.next_stage.expect("an unfinished move")
            )) && error.contains("remote joined handoff backend: ")
                && error.contains("is not implemented"),
            "{error}"
        );
        assert!(!status.completed && !status.aborted && status.paused.is_none());
        // The host is unaffected: its metadata still answers as the leader.
        assert_eq!(
            five.controls[0]
                .metadata_status(&budget(5000))
                .unwrap()
                .role,
            crate::MetadataConsensusRole::Leader
        );
        // Inside the backoff a step does nothing; after it the stage is
        // attempted again and reports the same block.
        let attempts = status.attempts;
        assert_eq!(step(&mut slot).attempts, attempts);
        std::thread::sleep(Duration::from_millis(700));
        let status = step(&mut slot);
        assert_eq!(status.attempts, attempts + 1);
        assert!(status
            .last_error
            .as_deref()
            .is_some_and(|error| error.contains("is not implemented")));
        slot.as_mut().unwrap().stop(&budget(5000)).unwrap();
    });
}

/// The operator action is identity DATA only and survives the command file.
#[test]
fn native_range_move_action_wire_shape_is_identity_data_only() {
    use crate::NativeMetadataHostAction as Action;
    let range = crate::RangeId::new(4).unwrap();
    let start = Action::MoveRange {
        range,
        target: node(4),
    };
    let wire = serde_json::to_value(&start).unwrap();
    assert_eq!(
        wire,
        serde_json::json!({"move_range": {"range": 4, "target": node(4)}})
    );
    assert_eq!(serde_json::from_value::<Action>(wire).unwrap(), start);
    let abort = Action::AbortMoveRange { range };
    let wire = serde_json::to_value(&abort).unwrap();
    assert_eq!(wire, serde_json::json!({"abort_move_range": {"range": 4}}));
    assert_eq!(serde_json::from_value::<Action>(wire).unwrap(), abort);
}
