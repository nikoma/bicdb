//! The remote backend's read-only range move probe against the five real
//! members: the old voter epoch read, the pre-flight of the target and the
//! voters, and the abort they make possible. Every member is reached only
//! through the coordinator's transport; in-process services are read only to
//! compare.
use super::*;
use crate::distribution_native_host::joined_data_handoff::{
    AllocationCommitOutcome, JoinedHandoffBackend, RemoteJoinedHandoffBackend,
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
                    unfinished_command: false,
                    unfinished_recovery: false,
                    writer_policy_refusal: None
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

/// A write that fails on its origin after its decision quorum and before its
/// local WAL leaves an unfinished native command on the range leader. The
/// probe reports it for that voter only, the remote backend refuses the
/// source cut by name before it sends anything, and the abort returns the
/// range to its original voters. The hot recovery then settles the command
/// and leaves its activated recovery record on every voter. A new handoff of
/// the range must complete over those records: its source close advances
/// each old voter's range log past the epoch that record was written for.
#[test]
fn native_remote_backend_probe_reports_unfinished_command_and_recovered_range_moves() {
    run("remote-backend-unfinished-command", |five| {
        use crate::distribution_transport::NativeRangeMoveProbeRoleV1 as Role;
        let parent = CancellationToken::uncancelable();
        let mut scenario = prepare(five);
        let voters = scenario.old_voters.clone();
        let leader = scenario.old_leader;
        let original = scenario.original.clone();
        let probe = |five: &Five, i: usize, range: &crate::RangeDescriptor| {
            five.clients[leader]
                .native_range_move_probe(&node(i), range.id, range.epoch, &budget(5000))
                .unwrap()
                .role
        };
        for &i in &voters {
            assert_eq!(
                probe(five, i, &original),
                Role::Voter {
                    durable_epoch: original.epoch,
                    unfinished_command: false,
                    unfinished_recovery: false,
                    writer_policy_refusal: None
                }
            );
        }

        // One append fails on the leader after its decision and before its
        // local WAL. No hot recovery runs in this fixture yet.
        let key = key_for(&scenario.current, original.id, "unfinished-on-leader");
        let value = serde_json::json!({"fifth_data_path":"decided, unfinished on its origin"});
        let arm = five.databases[leader]
            .read()
            .test_arm_native_origin_pre_wal_fault(original.id, "items", &key)
            .unwrap();
        let fault = arm.observation();
        let client = gateway(five, &scenario.current, 5);
        let refused = client.append("items", &key, value.clone());
        let until = Instant::now() + Duration::from_secs(10);
        while !fault.app_span_ended() {
            assert!(Instant::now() < until, "the origin fault never ended");
            std::thread::sleep(Duration::from_millis(5));
        }
        let snapshot = fault.snapshot();
        eprintln!(
            "REMOTE_BACKEND_UNFINISHED append={:?} fault={snapshot}",
            refused.as_ref().map_err(ToString::to_string)
        );
        assert_eq!(snapshot["fired"], true, "{snapshot}");
        assert!(
            refused.is_err(),
            "the faulted append must not be acknowledged"
        );
        drop(arm);
        assert_eq!(
            probe(five, leader, &original),
            Role::Voter {
                durable_epoch: original.epoch,
                unfinished_command: true,
                unfinished_recovery: false,
                writer_policy_refusal: None
            }
        );
        for &i in voters.iter().filter(|i| **i != leader) {
            eprintln!(
                "REMOTE_BACKEND_UNFINISHED follower {i}: {:?}",
                probe(five, i, &original)
            );
        }

        // The production backend holds its own write coordinator. It does
        // not propose a learner allocation while a voter holds an unfinished
        // command: nothing is committed, the step names what it waits for,
        // and the admission hold it took is released.
        let writer = five.controls[leader]
            .initial_range_write_coordinator()
            .unwrap();
        let mut held = RemoteJoinedHandoffBackend::new(node(4), five.clients[leader].clone())
            .with_local_writer(writer.clone())
            .test_allocation_waits(Duration::from_millis(400), Duration::from_millis(400));
        let not_proposed = held
            .before_allocation(five, &original, &budget(20_000))
            .unwrap_err()
            .to_string();
        eprintln!("REMOTE_BACKEND_UNFINISHED allocation not proposed: {not_proposed}");
        assert!(
            not_proposed.contains(&format!(
                "the learner allocation of range {} was not proposed: voter {} holds an unfinished write command",
                original.id,
                node(leader)
            )) && not_proposed.contains("Nothing was committed"),
            "{not_proposed}"
        );
        assert!(
            held.waiting()
                .is_some_and(|what| what.contains("unfinished write command")),
            "{:?}",
            held.waiting()
        );
        assert_eq!(committed(five), scenario.current);
        drop(
            writer
                .hold_range_for_allocation(original.id, Duration::from_secs(1), &budget(5000))
                .expect("the refused step released its admission hold"),
        );

        // The learner allocation commits over it. The probe still reports
        // the unfinished command at the new descriptor, and the remote
        // backend refuses the source cut by name with nothing sent.
        let pending = allocate_only(five, &mut scenario);
        let range = pending.range_by_id(original.id).unwrap().clone();
        assert_eq!(
            probe(five, leader, &range),
            Role::Voter {
                durable_epoch: original.epoch,
                unfinished_command: true,
                unfinished_recovery: false,
                writer_policy_refusal: None
            }
        );
        let backend = RemoteJoinedHandoffBackend::new(node(4), five.clients[leader].clone());
        let blocked = backend
            .require_no_unfinished_command(&range, &budget(20_000))
            .unwrap_err()
            .to_string();
        eprintln!("REMOTE_BACKEND_UNFINISHED refusal: {blocked}");
        assert!(
            blocked.contains(&format!(
                "old voter {} holds an unfinished write command of range {}",
                node(leader),
                range.id
            )) && blocked.contains("Nothing was sent"),
            "{blocked}"
        );
        assert!(!backend.cut_started());

        // The abort of a remote coordinator, as the operator would do. Each
        // member hosts its OLD receiver under its own process owner.
        let process = CancellationToken::uncancelable();
        for control in &five.controls {
            control.bind_initial_image_source_process(&process).unwrap();
        }
        NativeJoinedDataHandoff::with_backend(
            original.id,
            node(4),
            RemoteJoinedHandoffBackend::new(node(4), five.clients[leader].clone()),
        )
        .abort_before_cut(&mut *five, &parent)
        .unwrap();
        let aborted = committed(five);
        assert_eq!(aborted.range_by_id(original.id).unwrap(), &original);
        scenario.mark("aborted with an unfinished command on the leader");

        // The leader's hot recovery settles the command (its attempts are
        // made here; this fixture starts no process worker): the probe
        // reports neither an unfinished command nor an unfinished recovery,
        // the decided append is readable, and every voter retains the
        // activated recovery record of the original epoch.
        let until = Instant::now() + Duration::from_secs(90);
        let mut seen_unfinished_recovery = false;
        loop {
            let attempt = five.controls[leader].test_hot_recovery_attempt(&budget(5000));
            let roles = voters
                .iter()
                .map(|&i| {
                    five.clients[leader]
                        .native_range_move_probe(
                            &node(i),
                            original.id,
                            original.epoch,
                            &budget(5000),
                        )
                        .map(|reply| {
                            // The record that names an unfinished recovery is
                            // present exactly when the flag says so, and it
                            // is a record of this range's recovery.
                            let flagged = matches!(
                                reply.role,
                                Role::Voter {
                                    unfinished_recovery: true,
                                    ..
                                }
                            );
                            assert_eq!(
                                flagged,
                                reply.unfinished_recovery_record.is_some(),
                                "{reply:?}"
                            );
                            if let Some(record) = &reply.unfinished_recovery_record {
                                eprintln!("REMOTE_BACKEND_UNFINISHED named recovery: {record}");
                                assert!(!record.recovery_nonce.is_nil(), "{record:?}");
                            }
                            reply.role
                        })
                })
                .collect::<Vec<_>>();
            seen_unfinished_recovery |= roles.iter().any(|role| {
                matches!(
                    role,
                    Ok(Role::Voter {
                        unfinished_recovery: true,
                        ..
                    })
                )
            });
            if roles.iter().all(|role| {
                matches!(
                    role,
                    Ok(Role::Voter {
                        unfinished_command: false,
                        unfinished_recovery: false,
                        ..
                    })
                )
            }) {
                break;
            }
            assert!(
                Instant::now() < until,
                "the unfinished command was never settled: {roles:?} last attempt: {:?}",
                attempt.map_err(|error| error.to_string())
            );
            std::thread::sleep(Duration::from_millis(20));
        }
        eprintln!(
            "REMOTE_BACKEND_UNFINISHED unfinished recovery observed: {seen_unfinished_recovery}"
        );
        scenario.mark("unfinished command settled by the hot recovery");
        let client = gateway(five, &aborted, 5);
        assert_eq!(strong_until_served(&client, &key).as_ref(), Some(&value));
        scenario.acknowledged.push((key, value));
        let key = key_for(&aborted, original.id, "after-recovery");
        let value = serde_json::json!({"fifth_data_path":"acknowledged after the recovery"});
        append_until_acknowledged(&client, &key, &value);
        scenario.acknowledged.push((key, value));
        let until = Instant::now() + Duration::from_secs(60);
        loop {
            let states = voters
                .iter()
                .map(|&i| {
                    five.services[i]
                        .lock()
                        .test_native_replica_open_state(original.id)
                        .unwrap()
                })
                .collect::<Vec<_>>();
            if states
                .iter()
                .all(|state| state["phase"] == "NativeActivatedAnchor")
            {
                eprintln!("REMOTE_BACKEND_UNFINISHED recovery records: {states:?}");
                break;
            }
            assert!(
                Instant::now() < until,
                "not every voter retains an activated recovery record: {states:?}"
            );
            let _ = five.controls[leader].test_hot_recovery_attempt(&budget(5000));
            std::thread::sleep(Duration::from_millis(100));
        }

        // With every voter settled the production backend takes its
        // admission hold for the allocation. While it is held an append is
        // refused by the leader before it prepares anything and leaves no
        // unfinished command; after the attempt it is released.
        held.before_allocation(five, &original, &budget(20_000))
            .unwrap();
        let key = key_for(&aborted, original.id, "during-allocation-hold");
        let value =
            serde_json::json!({"fifth_data_path":"refused while the allocation is proposed"});
        let refused = gateway(five, &aborted, 2)
            .append("items", &key, value.clone())
            .unwrap_err()
            .to_string();
        eprintln!("REMOTE_BACKEND_UNFINISHED append under the hold: {refused}");
        assert!(refused.contains("range-move:allocation-hold:"), "{refused}");
        for &i in &voters {
            assert_eq!(
                probe(five, i, &original),
                Role::Voter {
                    durable_epoch: original.epoch,
                    unfinished_command: false,
                    unfinished_recovery: false,
                    writer_policy_refusal: None
                }
            );
        }
        // An attempt that cannot commit any more releases the hold at once
        // and leaves no note.
        let started = Instant::now();
        held.after_allocation(
            &original,
            AllocationCommitOutcome::NotCommitted,
            &budget(5000),
        );
        assert!(started.elapsed() < Duration::from_millis(200));
        assert!(held.take_notes().is_empty());
        append_until_acknowledged(&client, &key, &value);
        scenario.acknowledged.push((key, value));

        // An attempt of unknown outcome keeps the hold for its bounded wait:
        // the allocation may still commit. Nothing is committed here, so the
        // published range never changes; the wait runs out, the hold is
        // released and the fact is reported as an operator note. The same
        // holds for a committed allocation this node did not publish in time.
        for (outcome, text) in [
            (AllocationCommitOutcome::Unknown, "is not known"),
            (AllocationCommitOutcome::Committed, "is committed"),
        ] {
            held.before_allocation(five, &original, &budget(20_000))
                .unwrap();
            let started = Instant::now();
            held.after_allocation(&original, outcome, &budget(5000));
            assert!(
                started.elapsed() >= Duration::from_millis(400),
                "{outcome:?}: the hold was kept for its bounded wait"
            );
            let notes = held.take_notes();
            eprintln!("REMOTE_BACKEND_UNFINISHED hold ended unpublished ({outcome:?}): {notes:?}");
            assert!(
                notes.len() == 1
                    && notes[0].contains(&format!(
                        "the write hold of range {} ended after",
                        original.id
                    ))
                    && notes[0].contains("had not published the changed range")
                    && notes[0].contains(text),
                "{notes:?}"
            );
            drop(
                writer
                    .hold_range_for_allocation(original.id, Duration::from_secs(1), &budget(5000))
                    .expect("the hold was released when its wait ended"),
            );
        }
        drop(held);

        // A backend that was given no write coordinator proposes without the
        // hold and says so; nothing is held.
        let mut bare = RemoteJoinedHandoffBackend::new(node(4), five.clients[leader].clone());
        bare.before_allocation(five, &original, &budget(20_000))
            .unwrap();
        let notes = bare.take_notes();
        assert!(
            notes.len() == 1 && notes[0].contains("is proposed without a write hold"),
            "{notes:?}"
        );
        bare.after_allocation(&original, AllocationCommitOutcome::Committed, &budget(5000));
        assert!(bare.take_notes().is_empty());
        drop(bare);

        // A new handoff completes: every old voter's source close succeeds
        // over its retained recovery record, and the catalogue each of them
        // wrote is one its own loader accepts.
        assert_eq!(
            scenario.handoff.drive(&mut *five, &parent).unwrap(),
            Stage::SourceRetired
        );
        scenario.mark("handoff completed after the recovery");
        for &i in &voters {
            let service = five.services[i].lock();
            assert!(
                service.range_write_progress(original.id).current_epoch > original.epoch,
                "old voter {i} was closed"
            );
            service
                .test_require_range_catalogue_loadable()
                .unwrap_or_else(|error| panic!("old voter {i} catalogue: {error}"));
        }
        accept(five, &mut scenario);
    });
}
