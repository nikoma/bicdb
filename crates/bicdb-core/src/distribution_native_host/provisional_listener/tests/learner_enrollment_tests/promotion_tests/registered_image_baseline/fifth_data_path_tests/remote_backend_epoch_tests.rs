//! The remote backend's read-only range move probe against the five real
//! members: the old voter epoch read, the pre-flight of the target and the
//! voters, and the abort they make possible. Every member is reached only
//! through the coordinator's transport; in-process services are read only to
//! compare.
use super::*;
use crate::distribution_native_host::joined_data_handoff::{
    JoinedHandoffBackend, RemoteJoinedHandoffBackend,
};

fn read(
    five: &Five,
    coordinator: usize,
    voters: &[usize],
    range: &crate::RangeDescriptor,
) -> Vec<(usize, u64, std::result::Result<u64, String>)> {
    let backend = RemoteJoinedHandoffBackend::new(node(4), five.clients[coordinator].clone());
    voters
        .iter()
        .map(|&i| {
            let in_process = five.services[i]
                .lock()
                .range_write_progress(range.id)
                .current_epoch;
            let over_rpc = backend
                .old_epoch(five, &node(i), range)
                .map_err(|error| error.to_string());
            (i, in_process, over_rpc)
        })
        .collect()
}

#[test]
fn native_remote_backend_old_epoch_preflight_and_abort_over_rpc() {
    run("remote-backend-old-epoch", |five| {
        let mut scenario = prepare(five);
        let voters = scenario.old_voters.clone();
        let leader = scenario.old_leader;
        let original = scenario.original.clone();
        let other = (0..4).find(|i| *i != leader).unwrap();
        // The committed four-node configuration: every old voter answers
        // its range leader's process with its in-process durable epoch.
        let before = read(five, leader, &voters, &original);
        let before_other = read(five, other, &voters, &original);
        eprintln!("REMOTE_BACKEND_OLD_EPOCH before={before:?} other={other} before_other={before_other:?}");
        assert_eq!(before.len(), 3);
        for (i, in_process, over_rpc) in &before {
            assert_eq!(*in_process, original.epoch);
            assert_eq!(over_rpc, &Ok(*in_process), "old voter {i}: {before:?}");
        }
        // Any other process is an unreadable voter, never a guessed epoch.
        for (i, _, over_rpc) in &before_other {
            let refusal = over_rpc.as_ref().unwrap_err();
            assert!(
                refusal.contains(&format!(
                    "remote joined handoff backend: old voter {} epoch is unreadable",
                    node(*i)
                )) && refusal.contains(&format!(
                    "range move probe: {} is not the leader of range",
                    node(other)
                )),
                "{before_other:?}"
            );
        }
        // The pre-flight before anything is committed: the target answers as
        // a live registered replica receiver and every voter answers. A
        // target that is an original data owner is refused by name.
        let leader_client = five.clients[leader].clone();
        let backend =
            |target: ClusterNodeId| RemoteJoinedHandoffBackend::new(target, leader_client.clone());
        backend(node(4))
            .preflight(five, &original, &budget(20_000))
            .unwrap();
        use crate::distribution_transport::NativeRangeMoveProbeRoleV1 as Role;
        assert_eq!(
            five.clients[leader]
                .native_range_move_probe(&node(4), original.id, original.epoch, &budget(5000))
                .unwrap()
                .role,
            Role::RegisteredReplicaReceiver
        );
        let wrong = backend(node(other))
            .preflight(five, &original, &budget(20_000))
            .unwrap_err()
            .to_string();
        eprintln!("REMOTE_BACKEND_PREFLIGHT wrong target: {wrong}");
        assert!(
            wrong.contains(&format!(
                "the move of range {} onto {} cannot start: target {} is not ready",
                original.id,
                node(other),
                node(other)
            )) && wrong.contains("Nothing was committed"),
            "{wrong}"
        );
        // A coordinator that does not lead the range asks nobody.
        let not_leader = RemoteJoinedHandoffBackend::new(node(4), five.clients[other].clone())
            .preflight(five, &original, &budget(20_000))
            .unwrap_err()
            .to_string();
        assert!(
            not_leader.contains(&format!(
                "coordinator {} is not the leader {} of range {}",
                node(other),
                node(leader),
                original.id
            )),
            "{not_leader}"
        );
        assert_eq!(committed(five), scenario.current);

        // After the committed learner allocation, the interval the abort
        // exists for: the range is outside every original owner's installed
        // writer policy, so the range-write progress request is refused. The
        // probe is read-only and not tied to that policy: every voter
        // answers its range leader with its in-process durable epoch, still
        // the epoch the allocation expected.
        let pending = allocate_only(five, &mut scenario);
        let range = pending.range_by_id(original.id).unwrap().clone();
        let after = read(five, leader, &voters, &range);
        eprintln!("REMOTE_BACKEND_OLD_EPOCH after_allocation={after:?}");
        for (i, in_process, over_rpc) in &after {
            assert_eq!(*in_process, original.epoch);
            assert_eq!(over_rpc, &Ok(*in_process), "old voter {i}: {after:?}");
        }
        // The probe's own answer of a voter: its durable epoch and no
        // unfinished command of the range (no write was in flight).
        for &i in &voters {
            let reply = five.clients[leader]
                .native_range_move_probe(&node(i), range.id, range.epoch, &budget(5000))
                .unwrap();
            assert_eq!(
                reply.role,
                Role::Voter {
                    durable_epoch: original.epoch,
                    unfinished_command: false
                },
                "old voter {i}"
            );
        }
        // Still only the range leader, and only the committed epoch.
        for (_, _, over_rpc) in read(five, other, &voters, &range) {
            assert!(over_rpc.unwrap_err().contains("is not the leader of range"));
        }
        for (_, _, over_rpc) in read(five, leader, &voters, &original) {
            assert!(over_rpc.unwrap_err().contains("unknown or stale range"));
        }
        // The abort of a remote coordinator: each member hosts its OLD
        // receiver under its own process owner, as the remote route
        // requires; the read-only observation answers that no member retains
        // an operation, every voter's durable epoch is the expected one, and
        // the original descriptor is committed again.
        let process = CancellationToken::uncancelable();
        for control in &five.controls {
            control.bind_initial_image_source_process(&process).unwrap();
        }
        let remote = |five: &Five| {
            NativeJoinedDataHandoff::with_backend(
                original.id,
                node(4),
                RemoteJoinedHandoffBackend::new(node(4), five.clients[leader].clone()),
            )
        };
        remote(five)
            .abort_before_cut(&mut *five, &CancellationToken::uncancelable())
            .unwrap();
        let aborted = committed(five);
        assert_eq!(aborted.generation, pending.generation + 1);
        assert_eq!(aborted.range_by_id(original.id).unwrap(), &original);
        assert!(aborted
            .relocations
            .values()
            .all(|plan| plan.range_id != original.id));

        // A new allocation, then a source Close on every old voter: the
        // voters' durable epoch is beyond the expected one, the probe reports
        // exactly that, and the abort is refused and commits nothing.
        let pending = allocate_only(five, &mut scenario);
        let range = pending.range_by_id(original.id).unwrap().clone();
        let plan = relocation(five, &scenario);
        let mut closing = RemoteJoinedHandoffBackend::new(node(4), five.clients[leader].clone());
        let members = closing.members(&*five, &range, &plan).unwrap();
        closing
            .close_old_members(&members, &range, &plan, &budget(60_000))
            .unwrap();
        let closed = read(five, leader, &voters, &range);
        eprintln!("REMOTE_BACKEND_OLD_EPOCH after_close={closed:?}");
        for (i, in_process, over_rpc) in &closed {
            assert!(*in_process > original.epoch, "old voter {i}: {closed:?}");
            assert_eq!(over_rpc, &Ok(*in_process), "old voter {i}: {closed:?}");
        }
        let refused = remote(five)
            .abort_before_cut(&mut *five, &CancellationToken::uncancelable())
            .unwrap_err()
            .to_string();
        eprintln!("REMOTE_BACKEND_ABORT after_close: {refused}");
        assert!(
            refused.contains("the handoff can only be resumed"),
            "{refused}"
        );
        assert_eq!(committed(five), pending);

        // A target whose receiver is gone is named by the pre-flight.
        five.host.stop_owned_metadata(&budget(10_000)).unwrap();
        five.target_stopped = true;
        let stopped = backend(node(4))
            .preflight(five, &range, &budget(20_000))
            .unwrap_err()
            .to_string();
        eprintln!("REMOTE_BACKEND_PREFLIGHT stopped target: {stopped}");
        assert!(
            stopped.contains(&format!("target {} is not ready", node(4))),
            "{stopped}"
        );
    });
}
