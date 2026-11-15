//! The remote backend's old voter epoch read against the five real members.
//! The voter is reached only through the coordinator's transport; its
//! in-process service is read only to compare.
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
fn native_remote_backend_old_epoch_over_rpc_equals_in_process_value() {
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
                )) && refusal.contains("is not the current leader"),
                "{before_other:?}"
            );
        }
        // After the committed learner allocation, the interval the abort
        // exists for: the range is outside every original owner's installed
        // writer policy, and the voter refuses the progress read on that
        // ground even to its range leader. The in-process read has no such
        // gate. The backend reports an unreadable voter; it guesses nothing.
        let pending = allocate_only(five, &mut scenario);
        let range = pending.range_by_id(original.id).unwrap().clone();
        let after = read(five, leader, &voters, &range);
        eprintln!("REMOTE_BACKEND_OLD_EPOCH after_allocation={after:?}");
        for (i, in_process, over_rpc) in &after {
            assert_eq!(*in_process, original.epoch);
            let refusal = over_rpc.as_ref().unwrap_err();
            assert!(
                refusal.contains(&format!(
                    "remote joined handoff backend: old voter {} epoch is unreadable",
                    node(*i)
                )) && refusal
                    .contains("native host writer enrollment: range configuration changed"),
                "{after:?}"
            );
        }
        // The abort of a remote coordinator is therefore refused here, at
        // the epoch read, and commits nothing. Each member hosts its OLD
        // receiver under its own process owner, as the remote route requires;
        // the read-only observation then answers that no member retains an
        // operation.
        let process = CancellationToken::uncancelable();
        for control in &five.controls {
            control.bind_initial_image_source_process(&process).unwrap();
        }
        let refused = NativeJoinedDataHandoff::with_backend(
            original.id,
            node(4),
            RemoteJoinedHandoffBackend::new(node(4), five.clients[leader].clone()),
        )
        .abort_before_cut(&mut *five, &CancellationToken::uncancelable())
        .unwrap_err()
        .to_string();
        assert!(refused.contains("epoch is unreadable"), "{refused}");
        assert_eq!(committed(five), pending);
    });
}
