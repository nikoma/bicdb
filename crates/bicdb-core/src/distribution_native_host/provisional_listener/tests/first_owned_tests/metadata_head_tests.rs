//! Actual five-object/TLS native metadata consensus controls. No raw topology
//! write, native grant, extra deadline or fabricated competing data head.
use super::*;
pub(super) fn exercise(
    five: &mut Five,
    actual_meta: usize,
    selected: crate::RangeId,
    intent:crate::distribution_native_host::provisional_listener::NativeFirstCatalogueCertificationIntent,
    target: &mpsc::Sender<OwnerCommand>,
    report: &mut Report,
) -> Result<()> {
    let relocation = intent.relocation_id();
    let phase = intent.phase_id();
    let expected_participants = intent.participant_count();
    let before = report.data_call(
        "actual_target_native_before_metadata_other_range_cuts",
        || native_command(target, selected, None),
    )?;
    let drift =
        crate::distribution_native_host::provisional_listener::test_arm_remote_metadata_head_drift(
        )?;
    let actual = report.data_call(
        "actual_certified_after_real_other_range_metadata_commit",
        || five.sources[actual_meta].certify_finite_remote_catalogue(intent, &five.lifetime),
    );
    report.event(serde_json::json!({"actual_head_drift_cut_returns":drift(),
        "actual_certification_return":actual.as_ref().ok(),"actual_error":actual.as_ref().err().map(ToString::to_string)}));
    let certified = actual?;
    assert_eq!(certified["actual_committed_record"]["stage"], "Certified");
    for expected in ["PromotedFrozen", "AwaitingActivation"] {
        let actual = report.data_call(
            "actual_frozen_membership_after_real_other_range_metadata_commit",
            || {
                five.sources[actual_meta].advance_finite_remote_membership(
                    relocation,
                    phase,
                    &five.lifetime,
                )
            },
        );
        report.event(serde_json::json!({"expected_stage":expected,"actual_head_drift_cut_returns":drift(),
            "actual_membership_return":actual.as_ref().ok(),"actual_error":actual.as_ref().err().map(ToString::to_string)}));
        let actual = actual?;
        assert_eq!(actual["actual_committed_record"]["stage"], expected);
    }
    let reads = report.data_call(
        "actual_post_membership_recipient_native_decision_reads",
        || {
            five.sources[actual_meta].observe_finite_remote_native_decisions(
                relocation,
                phase,
                &five.lifetime,
            )
        },
    );
    report.event(
        serde_json::json!({"actual_post_membership_native_reads":reads.as_ref().ok(),
        "actual_read_error":reads.as_ref().err().map(ToString::to_string)}),
    );
    let reads = reads?;
    assert_eq!(
        reads["actual_protected_recipient_reads"]
            .as_array()
            .unwrap()
            .len(),
        expected_participants
    );
    assert_eq!(reads["local_quorum_constructed"], false);
    let after = report.data_call(
        "actual_target_native_after_metadata_other_range_cuts",
        || native_command(target, selected, None),
    )?;
    let actual = drift();
    report.event(serde_json::json!({"actual_complete_metadata_head_drift":actual,"target_before":before,"target_after":after,
        "Serving_Open_release":false,"metadata_native_WAL_expected":true,"native_D_replay":false}));
    assert_eq!(actual["remaining"], 0);
    let returns = actual["actual_returns"].as_array().unwrap();
    assert_eq!(returns.len(), 3);
    for returned in returns {
        assert_eq!(returned["returned_ok"], true);
        assert_ne!(returned["other_range"], returned["selected_range"]);
        assert_eq!(returned["actual_other_range_proposal_return"]["ok"], true);
        assert!(
            returned["actual_committed_read"]["read_index"]
                .as_u64()
                .unwrap()
                >= returned["actual_other_range_proposal_return"]["index"]
                    .as_u64()
                    .unwrap()
        );
    }
    // Genuine metadata WAL was written; do not assert unchanged global XID/LSN.
    assert_eq!(before["native"], after["native"]);
    assert_eq!(before["catalogue"], after["catalogue"]);
    Ok(())
}
