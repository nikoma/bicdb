//! Metadata leadership transfer together with the range-move precondition,
//! on the five real members: the operator makes the metadata leader coincide
//! with the leader of the range to move.
//!
//! The fixture elects member 0 once and has no schedulers. This test starts
//! the production metadata supervisor on the old leader and on the target,
//! as every serving process does; the other members only answer.
use super::*;
use crate::distribution_native_host::joined_data_handoff::RemoteJoinedHandoffBackend;
use crate::distribution_native_host::range_move::{
    require_no_running_range_move, NativeRangeMoveSlot, NativeRangeMoveStatus,
};
use crate::MetadataConsensusRole;

fn supervise(five: &Five, i: usize) -> NativeMetadataSupervisor {
    let retry = budget(5000);
    loop {
        match five.controls[i].start_automatic_metadata(&CancellationToken::uncancelable()) {
            Ok(worker) => return worker,
            Err(error) if error.to_string().contains("busy") && retry.check().is_ok() => {
                std::thread::sleep(Duration::from_millis(5));
            }
            Err(error) => panic!("metadata supervisor of member {i} did not start: {error}"),
        }
    }
}
/// The status read takes the member's service without waiting, and a
/// supervisor worker that has just started may hold it for a moment. The
/// product refusal ("busy") is unchanged; the test repeats the read, bounded.
fn status(five: &Five, i: usize) -> crate::MetadataConsensusStatus {
    let retry = budget(5000);
    loop {
        match five.controls[i].metadata_status(&budget(5000)) {
            Ok(status) => return status,
            Err(error) if error.to_string().contains("busy") && retry.check().is_ok() => {
                std::thread::sleep(Duration::from_millis(5));
            }
            Err(error) => panic!("metadata status of member {i} is unreadable: {error}"),
        }
    }
}
fn one_step(slot: &mut NativeRangeMoveSlot<RemoteJoinedHandoffBackend>) -> NativeRangeMoveStatus {
    slot.advance(&CancellationToken::uncancelable()).unwrap();
    slot.wait_step(&budget(120_000)).unwrap();
    let status = slot.status();
    eprintln!("NATIVE_METADATA_LEADERSHIP_TRANSFER range move status={status:?}");
    status
}
fn refusal(five: &Five, member: usize, range: crate::RangeId) -> String {
    match NativeRangeMoveSlot::start(&five.controls[member], range, node(4)) {
        Ok(_) => panic!("member {member} accepted a move it cannot coordinate"),
        Err(error) => error.to_string(),
    }
}

/// (e) A move of a range led by another member is refused with what to do;
/// after the transfer that member is accepted as the coordinator, and the
/// old leader's own controller pauses.
#[test]
fn native_metadata_leadership_transfer_lets_the_range_leader_coordinate_its_move() {
    run("leadership-transfer-range-move", |five| {
        let original = committed(five);
        let range = original
            .ranges
            .values()
            .find(|range| range.leader != node(0) && index_of(&range.leader) < 4)
            .expect("a range led by another original member")
            .clone();
        let leader = range.leader.clone();
        let x = index_of(&leader);
        let mut workers = vec![supervise(five, 0), supervise(five, x)];
        let term = status(five, 0).current_term;

        // Refused on the metadata leader, which does not lead the range, and
        // on the range leader, which does not lead metadata. Both say what
        // the operator does.
        let advice = format!(
            "range {} is led by {leader}; metadata is led by n0; run transfer-metadata-leadership --to {leader} on n0, then send move-range to {leader}",
            range.id
        );
        let on_metadata_leader = refusal(five, 0, range.id);
        eprintln!("NATIVE_METADATA_LEADERSHIP_TRANSFER refused on n0: {on_metadata_leader}");
        assert!(
            on_metadata_leader.contains("is not the leader of range")
                && on_metadata_leader.contains(&advice),
            "{on_metadata_leader}"
        );
        let on_range_leader = refusal(five, x, range.id);
        eprintln!("NATIVE_METADATA_LEADERSHIP_TRANSFER refused on {leader}: {on_range_leader}");
        assert!(
            on_range_leader.contains("is not the metadata leader (n0)")
                && on_range_leader.contains(&advice),
            "{on_range_leader}"
        );
        assert_eq!(committed(five), original);

        // A move the old leader coordinates itself, created before the
        // transfer (when the fixture has a range it leads).
        let mut own = original
            .ranges
            .values()
            .find(|range| range.leader == node(0))
            .map(|range| NativeRangeMoveSlot::start(&five.controls[0], range.id, node(4)).unwrap());

        // The operator's transfer action is refused while this host's own
        // move is unfinished and running (the operator guard is checked here;
        // the transfer below is the control host's, which the guard precedes).
        if let Some(own) = own.as_ref() {
            let refused = require_no_running_range_move(Some(own))
                .unwrap_err()
                .to_string();
            assert!(
                refused.contains("metadata leadership transfer refused"),
                "{refused}"
            );
        }

        let report = five.controls[0]
            .transfer_metadata_leadership(&leader, &budget(5000))
            .unwrap();
        eprintln!("NATIVE_METADATA_LEADERSHIP_TRANSFER report={report:?}");
        assert!(report.term > term);
        let old = status(five, 0);
        assert_eq!(old.role, MetadataConsensusRole::Follower);
        assert_eq!(old.leader_id, Some(leader.clone()));
        let until = Instant::now() + Duration::from_secs(8);
        loop {
            let new = status(five, x);
            assert_eq!(new.role, MetadataConsensusRole::Leader, "{new:?}");
            assert_eq!(new.current_term, report.term);
            if new.commit_index >= new.last_log_index {
                break;
            }
            assert!(Instant::now() < until, "new leader never anchored: {new:?}");
            std::thread::sleep(Duration::from_millis(20));
        }
        // Nothing committed changed.
        assert_eq!(five.metadata[x].lock().committed_topology(), &original);

        // The old leader's controller pauses: it is not both leaders now.
        if let Some(own) = own.as_mut() {
            let status = one_step(own);
            assert!(
                status.paused.as_deref().is_some_and(
                    |reason| reason.contains(&format!("is not the metadata leader ({leader})"))
                ),
                "{status:?}"
            );
            assert_eq!(status.attempts, 0);
            // A paused move does not refuse a transfer: transferring back is
            // how its coordinator becomes the metadata leader again.
            require_no_running_range_move(Some(&*own)).unwrap();
            own.stop(&budget(5000)).unwrap();
        }
        // The old leader still refuses the range, now naming the new leader.
        let after = refusal(five, 0, range.id);
        assert!(
            after.contains(&format!("is not the metadata leader ({leader})"))
                && after.contains(&format!("send move-range to {leader}")),
            "{after}"
        );

        // The range's leader is now accepted as the coordinator, and its
        // first step passes the precondition.
        let mut accepted =
            NativeRangeMoveSlot::start(&five.controls[x], range.id, node(4)).unwrap();
        assert!(accepted.status().paused.is_none());
        let status = one_step(&mut accepted);
        assert!(status.paused.is_none(), "{status:?}");
        assert_eq!(status.attempts, 1, "{status:?}");
        accepted.stop(&budget(5000)).unwrap();

        for worker in &mut workers {
            worker.stop(&budget(5000)).unwrap();
        }
    });
}

/// The operator action is identity DATA only and survives the command file.
#[test]
fn native_metadata_leadership_transfer_action_wire_shape_is_identity_data_only() {
    use crate::NativeMetadataHostAction as Action;
    let action = Action::TransferMetadataLeadership { to: node(2) };
    let wire = serde_json::to_value(&action).unwrap();
    assert_eq!(
        wire,
        serde_json::json!({"transfer_metadata_leadership": {"to": node(2)}})
    );
    assert_eq!(serde_json::from_value::<Action>(wire).unwrap(), action);
}
