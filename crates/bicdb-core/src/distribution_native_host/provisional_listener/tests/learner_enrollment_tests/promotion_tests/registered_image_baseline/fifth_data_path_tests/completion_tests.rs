//! Completion of the four-to-five handoff in committed metadata: the
//! activation record, the retired source's own retirement and the completed
//! relocation, on the same five actual hosts.
use super::*;
use crate::distribution::NativeJointHandoffStageV1 as Recorded;
use crate::distribution_native_host::joined_data_handoff::{
    test_plan_learner_allocation, test_range_has_unfinished_relocation,
};

/// One member's retirement attempt on its own committed metadata read.
fn retire(
    five: &Five,
    member: usize,
    range: crate::RangeId,
) -> Result<crate::distribution_data::NativeJointSourceRetired> {
    let token = budget(5000);
    let business = Uuid::now_v7();
    let own = five.clients[member].acquire_host_metadata_observation(
        &node(0),
        business,
        Duration::from_secs(5),
        &token,
    )?;
    crate::ClusterDataNodeService::retire_actual_native_joint_source(
        &five.services[member],
        &own,
        business,
        range,
        &token,
    )
}

fn refused(
    outcome: Result<crate::distribution_data::NativeJointSourceRetired>,
    label: &str,
) -> String {
    let error = match outcome {
        Ok(_) => panic!("{label}: a retirement was recorded"),
        Err(error) => error.to_string(),
    };
    eprintln!("NATIVE_JOINT_HANDOFF_COMPLETION {label} refused: {error}");
    error
}

/// The outcome of planning the next learner allocation of `range` onto a
/// member that holds no range replica. The fixture has no such member, so a
/// PLANNING COPY of the committed configuration gets one: a copy of an
/// enrolled member's entry under another name. Nothing here is committed.
fn plan_next_onto_spare(
    committed: &ClusterTopology,
    range: crate::RangeId,
    enrolled: usize,
) -> Result<ClusterTopology> {
    let mut planning = committed.clone();
    let mut spare = planning.nodes[&node(enrolled)].clone();
    spare.id = ClusterNodeId::new("planning-spare").unwrap();
    spare.address = "127.0.0.1:1".into();
    planning.nodes.insert(spare.id.clone(), spare.clone());
    test_plan_learner_allocation(&planning, range, &spare.id)
}

#[test]
fn native_joint_handoff_completion_records_activation_then_source_retirement() {
    run("fifth-data-path-completion", |five| {
        let parent = CancellationToken::uncancelable();
        let mut scenario = prepare(five);
        let range = scenario.original.id;
        let source = scenario.old_leader;
        let survivor = *scenario
            .old_voters
            .iter()
            .find(|member| **member != source)
            .unwrap();

        // Serving: the membership is final, the activation is not recorded.
        assert_eq!(
            scenario
                .handoff
                .drive_until(&mut *five, Some(Stage::Serving), &parent)
                .unwrap(),
            Stage::Serving
        );
        assert!(!scenario.handoff.completed());
        let serving = relocation(five, &scenario);
        let record = serving.native_joint_handoff_v1.as_ref().unwrap();
        assert_eq!(record.stage(), Recorded::AwaitingActivation);
        assert_eq!(record.activated_quorum_sha256(), None);
        assert_eq!(serving.phase, RelocationPhase::CleaningUp);
        assert!(test_range_has_unfinished_relocation(
            &committed(five),
            range
        ));
        // The retired source must not retire before the new set's activation
        // is committed: its own metadata read does not show it.
        let early = refused(retire(five, source, range), "before the activation record");
        assert!(early.contains("requires the committed activation quorum"));
        scenario.mark("serving; activation not recorded");

        // The activation record.
        let before = committed(five);
        assert_eq!(
            scenario
                .handoff
                .drive_until(&mut *five, Some(Stage::ActivationRecorded), &parent)
                .unwrap(),
            Stage::ActivationRecorded
        );
        assert!(!scenario.handoff.completed());
        let activated = committed(five);
        assert_eq!(
            activated.range_by_id(range),
            before.range_by_id(range),
            "the activation record changes no membership"
        );
        let awaiting = relocation(five, &scenario);
        let record = awaiting.native_joint_handoff_v1.as_ref().unwrap();
        assert_eq!(record.stage(), Recorded::AwaitingCleanup);
        let quorum = record.activated_quorum_sha256().unwrap().to_string();
        assert_eq!(quorum.len(), 64);
        assert_eq!(record.source_cleanup_sha256(), None);
        assert_eq!(awaiting.phase, RelocationPhase::CleaningUp);
        assert!(awaiting.is_active());
        assert!(test_range_has_unfinished_relocation(&activated, range));
        let unfinished = plan_next_onto_spare(&activated, range, source).unwrap_err();
        assert!(unfinished
            .to_string()
            .contains("range already has a committed relocation"));
        scenario.mark("activation recorded");

        // A driver created now has no custody. The committed metadata alone
        // tells it that the handoff is past Serving: nothing is driven again.
        let mut restarted = NativeJoinedDataHandoff::new(range, node(4));
        assert_eq!(
            restarted
                .drive_until(&mut *five, Some(Stage::Serving), &parent)
                .unwrap(),
            Stage::Serving
        );
        assert_eq!(
            restarted
                .drive_until(&mut *five, Some(Stage::ActivationRecorded), &parent)
                .unwrap(),
            Stage::ActivationRecorded
        );
        assert!(!restarted.cut_attempted());
        assert_eq!(restarted.test_custody_snapshot()["source_cut_rounds"], 0);
        assert_eq!(committed(five), activated);

        // Only the relocation's source retires, and only from its own range.
        refused(retire(five, survivor, range), "a surviving voter");
        let other = activated
            .ranges
            .values()
            .map(|descriptor| descriptor.id)
            .find(|id| *id != range);
        if let Some(other) = other {
            refused(retire(five, source, other), "another range");
        }
        assert_eq!(committed(five), activated);

        // The driver without custody finishes: the source records its
        // retirement on its own metadata read and the relocation completes.
        assert_eq!(
            restarted.drive(&mut *five, &parent).unwrap(),
            Stage::SourceRetired
        );
        assert!(restarted.completed());
        let completed = committed(five);
        assert_eq!(
            completed.range_by_id(range),
            activated.range_by_id(range),
            "the source retirement changes no membership"
        );
        let done = relocation_after(completed.clone(), &scenario);
        let record = done.native_joint_handoff_v1.as_ref().unwrap();
        assert_eq!(record.stage(), Recorded::Cleaned);
        assert_eq!(record.activated_quorum_sha256(), Some(quorum.as_str()));
        let cleanup = record.source_cleanup_sha256().unwrap().to_string();
        assert_eq!(done.phase, RelocationPhase::Completed);
        assert!(!done.is_active());
        // What the metadata records is the record the source itself holds.
        let held = retire(five, source, range).unwrap();
        assert_eq!(held.checksum_sha256(), cleanup);
        assert_eq!(held.activated_quorum_sha256(), quorum);
        assert_eq!(held.node_id(), &node(source));
        drop(held);
        // No row left the retired source and it is as unwritable as before.
        for (key, value) in &scenario.acknowledged {
            assert_eq!(stored(five, source, key).as_ref(), Some(value));
        }
        let progress = five.services[source].lock().range_write_progress(range);
        assert_eq!(
            progress.current_epoch,
            completed.range_by_id(range).unwrap().epoch
        );
        scenario.mark("source retired; relocation completed");

        // Every driver now reports completion from the metadata and changes
        // nothing: a new one, and the one that holds the original custody.
        let mut later = NativeJoinedDataHandoff::new(range, node(4));
        assert_eq!(
            later.drive(&mut *five, &parent).unwrap(),
            Stage::SourceRetired
        );
        assert!(later.completed() && !later.cut_attempted());
        assert_eq!(
            scenario.handoff.drive(&mut *five, &parent).unwrap(),
            Stage::SourceRetired
        );
        assert!(scenario.handoff.completed());
        assert_eq!(committed(five), completed);

        // The completed relocation no longer refuses a new handoff of the
        // range: the same planning that was refused above is not refused for
        // an unfinished relocation any more.
        assert!(!test_range_has_unfinished_relocation(&completed, range));
        let next = plan_next_onto_spare(&completed, range, source);
        eprintln!(
            "NATIVE_JOINT_HANDOFF_COMPLETION next allocation planned: {:?}",
            next.as_ref().map(|_| ()).map_err(|error| error.to_string())
        );
        match &next {
            Ok(planned) => {
                let plans = planned
                    .relocations
                    .values()
                    .filter(|plan| plan.range_id == range)
                    .collect::<Vec<_>>();
                assert_eq!(plans.len(), 2);
                assert!(plans
                    .iter()
                    .any(|plan| plan.id == done.id && plan.phase == RelocationPhase::Completed));
                assert!(plans.iter().any(|plan| plan.id != done.id
                    && plan.phase == RelocationPhase::LearnerAllocated
                    && plan.source.as_ref() == Some(&node(4))));
            }
            Err(error) => assert!(
                !error
                    .to_string()
                    .contains("range already has a committed relocation"),
                "a completed relocation still refuses the next handoff: {error}"
            ),
        }
        accept(five, &mut scenario);
    });
}
