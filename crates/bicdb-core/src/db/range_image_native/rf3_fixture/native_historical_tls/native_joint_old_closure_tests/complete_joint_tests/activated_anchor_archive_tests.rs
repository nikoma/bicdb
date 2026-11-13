//! Full real CurrentJoint recovery prerequisite, then historical AnchorR3 at
//! actual R4; an actual missing serving archive must never produce a reply.
use super::post_joint_no_data_tests::{diagnostic_cut, report};
use super::*;
use crate::distribution_range_recovery::{NativeCompletionRequest, NativeCompletionStage};

#[test]
fn native_current_joint_actual_image_target_anchor_r3_at_r4_refuses_missing_serving_archive() {
    assert_eq!(
        run_complete_joint_with_continuation(
            0,
            JointFullContinuation::PostJointActivatedServingArchive
        ),
        JointTestOutcome::FullHandoffThenCurrentJointServingArchiveRefused,
        "full canonical Abort3/all3 activation/ACK4/Strong must finish first"
    );
}
fn token() -> CancellationToken {
    CancellationToken::uncancelable().child_with_deadline(Instant::now() + Duration::from_secs(5))
}
fn rows(fixture: &EnforcedNetwork) -> BTreeMap<ClusterNodeId, Vec<crate::Record>> {
    fixture
        .native
        .databases
        .iter()
        .map(|(id, database)| {
            let db = database.read();
            let mut records = db.scan_collection("items").unwrap();
            records.sort_by(|a, b| a.id.cmp(&b.id));
            (id.clone(), records)
        })
        .collect()
}
pub(super) fn after_actual_ack4_and_strong(
    fixture: &EnforcedNetwork,
    range: &crate::RangeDescriptor,
    ack1: &crate::Record,
    ack2: &crate::Record,
) {
    let unknown_key = fixture.key(range.id, "post-joint-new-domain-prepare-only-no-ACK3");
    let ledger: Vec<crate::Record> = serde_json::from_slice(
        &std::fs::read(
            fixture
                .native
                .root
                .path()
                .join("actual-joint-selected-range-ack-ledger.json"),
        )
        .unwrap(),
    )
    .unwrap();
    assert_eq!(
        ledger.len(),
        3,
        "real fsynced business ACK1/ACK2/ACK4 ledger"
    );
    assert_eq!(&ledger[0], ack1);
    assert_eq!(&ledger[1], ack2);
    let recipient = node("n3"); // actual copied-image target, not a genesis survivor
    let before = diagnostic_cut(fixture, range, ack1, ack2, &unknown_key);
    let before_rows = rows(fixture);
    let before_catalogues = fixture
        .native
        .databases
        .iter()
        .map(|(id, db)| {
            (
                id.clone(),
                std::fs::read(
                    db.read()
                        .data_path()
                        .join(crate::distribution_range_consensus::DEFAULT_RANGE_WRITE_LOG),
                )
                .unwrap(),
            )
        })
        .collect::<BTreeMap<_, _>>();
    for id in [node("n1"), node("n2"), node("n3")] {
        assert_eq!(before[&id]["native"]["r"], 4);
        let db = fixture.native.databases[&id].read();
        for ack in &ledger {
            assert_eq!(db.get("items", &ack.id).unwrap().as_deref(), Some(ack));
        }
        assert!(db.get("items", &unknown_key).unwrap().is_none());
    }
    let scope: RecoveryFenceScope =
        serde_json::from_value(before[&recipient]["recovery_fence"]["scope"].clone()).unwrap();
    assert!(matches!(
        scope.purpose,
        crate::distribution_consensus::MetadataRecoveryRolePurpose::CurrentJointRecovery { .. }
    ));
    let activated = &before[&recipient]["recovery_fence"]["phase"]["NativeActivated"];
    assert_eq!(activated["completed"]["point"]["range_index"], 3);
    let decision_sha = activated["native_activation_sha256"].as_str().unwrap();
    {
        let db = fixture.native.databases[&recipient].read();
        let actual = db.current_native_range_source_lineage(range.id).unwrap();
        assert_ne!(
            actual.base_range(),
            actual.native_write_range().unwrap(),
            "real target keeps immutable capture descriptor"
        );
        assert_ne!(
            actual.physical_range_members().unwrap(),
            actual.native_write_members().unwrap(),
            "actual OLD physical voters differ from protected NEW cohort"
        );
    }
    let request = || NativeCompletionRequest {
        scope: scope.clone(),
        challenge: Uuid::now_v7(),
        budget_ms: 4500,
    };
    let parent = token();
    let positive = fixture.clients[&range.leader]
        .observe_native_completion_observation(&recipient, &request(), &parent)
        .expect("actual fresh readonly TLS observation must accept n3 protected NEW domain");
    parent.check().unwrap();
    assert_eq!(
        positive.reply().stage,
        NativeCompletionStage::ActivatedAnchor
    );
    assert_eq!(
        positive.reply().range_index,
        3,
        "historical AnchorR3 is not fabricated ReadyR4"
    );
    assert_eq!(
        positive.reply().activation_decision_sha256.as_deref(),
        Some(decision_sha)
    );
    fixture.idle();
    assert_eq!(
        diagnostic_cut(fixture, range, ack1, ack2, &unknown_key),
        before
    );
    assert_eq!(rows(fixture), before_rows);

    // Deliberate actual metadata-WAL damage, after all positive acceptance.
    // No decoded DTO supplies native authority or a fabricated history floor.
    let damage_parent = token();
    let damage = fixture.native.databases[&recipient]
        .read()
        .test_remove_current_joint_serving_archive(range.id, &damage_parent)
        .unwrap();
    damage_parent.check().unwrap();
    assert_eq!(damage["actual_native_R"], 4);
    assert_eq!(damage["current_record"], damage["removed_archive"]);
    assert_eq!(damage["current_record"]["activation_index"], 3);
    assert_eq!(
        damage["current_record"]["checksum_sha256"].as_str(),
        Some(positive.reply().native_serving_sha256.as_str())
    );
    let parent = token();
    let error = fixture.clients[&range.leader]
        .observe_native_completion_observation(&recipient, &request(), &parent)
        .expect_err("actual missing immutable serving archive must not produce a completion reply");
    parent.check().unwrap();
    assert!(
        error
            .to_string()
            .contains("immutable joint activated serving archive is missing"),
        "{error}"
    );
    fixture.idle();
    let after = diagnostic_cut(fixture, range, ack1, ack2, &unknown_key);
    assert_eq!(
        after, before,
        "metadata archive damage cannot alter actual R4/H/B/UID/raw/slots/promises/ACK data"
    );
    assert_eq!(rows(fixture), before_rows);
    for (id, bytes) in &before_catalogues {
        assert_eq!(
            std::fs::read(
                fixture.native.databases[id]
                    .read()
                    .data_path()
                    .join(crate::distribution_range_consensus::DEFAULT_RANGE_WRITE_LOG)
            )
            .unwrap(),
            *bytes
        );
    }
    report(
        fixture,
        "actual-current-joint-n3-anchor-missing-serving-archive.json",
        &serde_json::json!({
            "scope_request_DATA":scope,"actual_positive_reply":positive.reply(),
            "actual_corruption_DATA":damage,"actual_missing_archive_response":error.to_string(),
            "original_negative_parent_live":true,"current_native_R":4,"immutable_anchor_R":3,
            "real_business_ACK_ledger":ledger,"no_new_ACK":true,"no_Ready_grant":true,
            "after_fault_Strong_qualification":false,"unchanged_actual_data":after,
        }),
    );
    eprintln!("ACTUAL_CURRENT_JOINT_N3_ANCHOR_ARCHIVE_CONTROL all3Activate=true realACK4StrongPrerequisite=true currentR=4 anchorR=3 actualArchiveDeleted=true readonlyReplyRefused=true actualDataUnchanged=true");
}
