//! Actual five-host same-process cancellation cuts. No fresh driver, install
//! attempt, source cut, authority DTO or larger request budget repairs a refusal.
use super::*;
use crate::distribution_native_host::joined_data_handoff::TestHandoffCustodyCut as Cut;
use serde_json::{json, Value};

fn preparation_records(five: &Five, range: crate::RangeId) -> BTreeMap<String, Value> {
    five.databases
        .iter()
        .enumerate()
        .map(|(i, db)| {
            (
                node(i).to_string(),
                db.read()
                    .test_joint_preparation_record(range)
                    .unwrap()
                    .unwrap_or(Value::Null),
            )
        })
        .collect()
}
fn target_image(snapshot: &Value) -> Value {
    if !snapshot["image"].is_null() {
        return snapshot["image"].clone();
    }
    snapshot["decision"]["images"][node(4).to_string()].clone()
}
fn exercise(cut: Cut) {
    run(&format!("custody-retry-{cut:?}"), |five| {
        let mut scenario = prepare(five);
        let output = std::env::var_os("BICDB_TEST_EVIDENCE_ROOT")
            .map(PathBuf::from)
            .unwrap_or_else(|| five.f.root.path().join("custody-retry-evidence"))
            .join(format!("driver-custody-{}", Uuid::now_v7()));
        std::fs::create_dir_all(&output).unwrap();
        let stage = if cut == Cut::AfterTargetNew {
            Stage::ImageVerified
        } else {
            Stage::MembershipFinal
        };
        let initial = scenario.handoff.drive_until(
            &mut *five,
            Some(stage),
            &CancellationToken::uncancelable(),
        );
        evidence(
            &output.join("initial-stage-before-assertions.json"),
            &json!({
                "cut":format!("{cut:?}"),"stage":format!("{stage:?}"),
                "actual_stage":initial.as_ref().ok().map(|s|format!("{s:?}")),
                "actual_error":initial.as_ref().err().map(ToString::to_string),
                "acknowledged_full_payloads":scenario.acknowledged,
                "custody":scenario.handoff.test_custody_snapshot(),
            }),
        )
        .unwrap();
        assert_eq!(initial.unwrap(), stage);
        let before = scenario.handoff.test_custody_snapshot();
        let original_image = target_image(&before);
        assert!(original_image.is_object());
        assert_eq!(original_image["attempt"], before["last_attempt"]);
        assert!(original_image["base_page_xid"].as_u64().unwrap() > 0);
        let before_preparations = preparation_records(five, scenario.original.id);
        assert!(before_preparations.values().all(Value::is_null));
        let original_topology = committed(five);
        scenario.handoff.test_cancel_custody_once(cut);
        let caller = CancellationToken::uncancelable();
        let failed = scenario.handoff.drive(&mut *five, &caller);
        // Record the actual API outcome before assertions or diagnostic reads.
        evidence(&output.join("cut-primary-result.json"), &json!({
            "cut":format!("{cut:?}"),"actual_stage":failed.as_ref().ok().map(|s|format!("{s:?}")),
            "actual_error":failed.as_ref().err().map(ToString::to_string),
            "actual_error_debug":failed.as_ref().err().map(|e|format!("{e:?}")),
            "caller_canceled":caller.is_cancel_requested(),
        })).unwrap();
        let after = scenario.handoff.test_custody_snapshot();
        let after_preparations = preparation_records(five, scenario.original.id);
        evidence(
            &output.join("cut-state-before-assertions.json"),
            &json!({
                "before":before,"after":after,"before_preparations":before_preparations,
                "after_preparations":after_preparations,"topology_before":original_topology,
                "topology_after":committed(five),"acknowledged_full_payloads":scenario.acknowledged,
            }),
        )
        .unwrap();
        assert!(
            failed.is_err(),
            "the actual caller cancellation must refuse this call"
        );
        assert!(
            caller.is_cancel_requested(),
            "the intended genuine cut was not reached"
        );
        assert_eq!(after["trace"][0]["point"], format!("{cut:?}"));
        assert_eq!(after["trace"][0]["caller_was_live"], true);
        assert_eq!(after["trace"][0]["session_was_live"], true);
        assert_eq!(after["session_canceled"], false);
        assert_eq!(after["source_cut_rounds"], before["source_cut_rounds"]);
        assert_eq!(after["last_attempt"], before["last_attempt"]);
        assert_eq!(target_image(&after), original_image);
        assert_eq!(committed(five), original_topology);
        if cut == Cut::AfterTargetNew {
            assert_eq!(after["trace"][0]["detail"], original_image);
            assert_eq!(after["publishing_retained"], false);
            assert_eq!(after["publication_rounds"], 0);
        } else {
            assert_eq!(
                after["decision"], before["decision"],
                "original durable receipts/owners must remain held"
            );
        }
        let present: Vec<_> = after_preparations
            .iter()
            .filter(|(_, v)| !v.is_null())
            .collect();
        if cut == Cut::AfterFirstNativePreparation {
            assert_eq!(
                present.len(),
                1,
                "exactly one real native preparation completed before cancellation"
            );
            let (_, record) = present[0];
            assert_eq!(record["record"], record["archive"]);
            assert!(record["record"]["native_page_xid"].as_u64().unwrap() > 0);
            assert_eq!(
                after["trace"][0]["detail"]["actual_preparation_checksum"],
                record["record"]["checksum_sha256"]
            );
            // Actual finite drive request ended AFTER the native fact, while
            // the producer-sealed original preparation Session remains live.
            // A subsequent current collection must use that fact/owner, never
            // renew the canceled OWN or repeat native preparation.
            let retained = &after["native_preparation_custody"]
                [record["record"]["local_node_id"].as_str().unwrap()];
            assert_eq!(retained["original_request"]["cancel_requested"], true);
            assert!(retained["original_session_error"].is_null());
            assert_eq!(
                retained["original_request"]["context_id"],
                record["record"]["metadata_context_id"]
            );
            assert_eq!(
                retained["native_preparation_sha256"],
                record["record"]["checksum_sha256"]
            );
        } else {
            assert!(present.is_empty());
        }
        assert_unwritable_and_intact(five, &scenario, "after-real-caller-custody-cut");
        let resumed = scenario
            .handoff
            .drive(&mut *five, &CancellationToken::uncancelable());
        let completed = scenario.handoff.test_custody_snapshot();
        let completed_preparations = preparation_records(five, scenario.original.id);
        evidence(&output.join("fresh-same-driver-before-assertions.json"), &json!({
            "actual_stage":resumed.as_ref().ok().map(|s|format!("{s:?}")),
            "actual_error":resumed.as_ref().err().map(ToString::to_string),
            "original_image":original_image,"completed":completed,
            "original_cut_records":after_preparations,"completed_records":completed_preparations,
            "acknowledged_full_payloads":scenario.acknowledged,
        })).unwrap();
        assert_eq!(resumed.unwrap(), Stage::Serving);
        assert_eq!(
            completed["source_cut_rounds"], before["source_cut_rounds"],
            "fresh caller must not recut"
        );
        assert_eq!(
            completed["last_attempt"], before["last_attempt"],
            "fresh caller must not install a new A"
        );
        assert_eq!(
            completed["publication_rounds"], 1,
            "one original DECISION publication round"
        );
        let prepared = completed["trace"]
            .as_array()
            .unwrap()
            .iter()
            .find(|entry| entry["point"] == "complete_prepared_quorum_retained")
            .unwrap();
        assert_eq!(
            prepared["original_decision"]["images"][node(4).to_string()],
            original_image
        );
        let receipts = prepared["original_decision"]["durable_receipts"]
            .as_object()
            .unwrap();
        assert_eq!(receipts.len(), scenario.old_voters.len() + 1);
        assert!(receipts
            .values()
            .all(|r| r["page_xid"].as_u64().unwrap() > 0));
        if cut != Cut::AfterTargetNew {
            assert_eq!(
                prepared["original_decision"], before["decision"],
                "no new DECISION xid/body on retry"
            );
        }
        for (node, old) in present {
            assert_eq!(
                &completed_preparations[node], old,
                "retry must reuse real preparation CURRENT/archive/xid"
            );
        }
        // Keep every original acceptance assertion, full Strong payload and
        // acknowledged append after stopping an original voter.
        accept(five, &mut scenario);
        evidence(
            &output.join("full-acceptance-ledger.json"),
            &json!({
                "acknowledged_full_payloads":scenario.acknowledged,"custody":completed,
                "same_process_only":true,"cold_restart_or_controller_qualification":false,
            }),
        )
        .unwrap();
    });
}
#[test]
fn native_fifth_data_path_actual_target_new_caller_cancel_retains_original_image_and_resumes() {
    exercise(Cut::AfterTargetNew);
}
#[test]
fn native_fifth_data_path_actual_preparation_own_cancel_retains_decision_and_resumes() {
    exercise(Cut::BeforePreparationOwn);
}
#[test]
fn native_fifth_data_path_actual_native_preparation_caller_cancel_reuses_record_and_resumes() {
    exercise(Cut::AfterFirstNativePreparation);
}
