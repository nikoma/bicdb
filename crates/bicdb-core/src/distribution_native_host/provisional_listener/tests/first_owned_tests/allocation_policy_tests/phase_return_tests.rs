//! Real normal Five, real placement/native audits and actual authenticated
//! recipient successes. Each deliberate one-quantum stop is the ORIGINAL
//! caller error, never Ok before the whole job's real publisher completes.
use super::*;
use std::collections::BTreeSet;
fn action() -> CancellationToken {
    CancellationToken::uncancelable().child_with_deadline(Instant::now() + Duration::from_secs(5))
}
struct Fixture {
    five: Five,
    report: Report,
    client: crate::ClusterEventClient,
    selected: crate::RangeDescriptor,
    other: crate::RangeDescriptor,
    leader: usize,
    before: Vec<serde_json::Value>,
    before_selected_native: serde_json::Value,
}
impl Fixture {
    fn start() -> Self {
        let mut report = Report {
            events: Vec::new(),
            omitted: 0,
            stderr_write_errors: 0,
        };
        let mut five = Five::start(&mut report).unwrap();
        let setup = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let selected = five.topology.ranges.values().next().unwrap().clone();
            let other = five
                .topology
                .ranges
                .values()
                .find(|r| r.id != selected.id)
                .unwrap()
                .clone();
            let leader = (0..five.sources.len())
                .find(|i| node(*i) == selected.leader)
                .unwrap();
            let client = crate::ClusterEventClient::new(&five.config, five.topology.clone())
                .unwrap()
                .with_request_timeout(Duration::from_secs(2))
                .unwrap();
            sdk(
                &mut report,
                &client,
                &key(&five.topology, selected.id, "phase-before-selected").unwrap(),
                serde_json::json!({"whole":"before selected","null":null}),
            )
            .unwrap();
            sdk(
                &mut report,
                &client,
                &key(&five.topology, other.id, "phase-before-other").unwrap(),
                serde_json::json!({"whole":"before untouched"}),
            )
            .unwrap();
            let before_selected_native = five.sources[leader]
                .test_initial_allocation_native_point(selected.id)
                .unwrap();
            let before = five
                .sources
                .iter()
                .map(|h| h.test_initial_allocation_evidence())
                .collect::<Result<Vec<_>>>()
                .unwrap();
            // Existing test-only scheduling hold. Workers still perform genuine
            // native v2 audits; no readiness/Role/ACK/native result is injected.
            for host in &five.sources {
                host.test_initial_allocation_pause(true).unwrap();
            }
            let original =
                five.sources[0].test_first_commit_allocation(selected.id, &node(4), &action());
            report.event(serde_json::json!({"phase_control_original_placement":original.as_ref().ok(),"original_error":original.as_ref().err().map(ToString::to_string)}));
            original.unwrap();
            let parent = action();
            loop {
                parent.check().unwrap();
                let current = five
                    .sources
                    .iter()
                    .map(|h| h.test_initial_allocation_evidence())
                    .collect::<Result<Vec<_>>>()
                    .unwrap();
                if current
                    .iter()
                    .all(|e| e["published"]["record"]["version"] == 2)
                    && current[leader]["actual_live_recovery_hold_cuts"]
                        .as_u64()
                        .is_some_and(|n| n > 0)
                {
                    break;
                }
                std::thread::sleep(Duration::from_millis(5));
            }
            (
                client,
                selected,
                other,
                leader,
                before,
                before_selected_native,
            )
        }));
        match setup {
            Ok((client, selected, other, leader, before, before_selected_native)) => Self {
                five,
                report,
                client,
                selected,
                other,
                leader,
                before,
                before_selected_native,
            },
            Err(original) => {
                let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    five.stop(&mut report)
                }));
                std::panic::resume_unwind(original)
            }
        }
    }
    fn advance(&mut self, mode: u8, shorter: bool) -> (bool, serde_json::Value) {
        let parent = if shorter {
            CancellationToken::uncancelable()
                .child_with_deadline(Instant::now() + Duration::from_secs(1))
        } else {
            action()
        };
        let original_deadline = format!("{:?}", parent.deadline().unwrap());
        let (actual, diagnostics) = self.five.sources[self.leader]
            .test_actual_allocation_phase_return(self.selected.id, mode, &parent)
            .unwrap();
        // Preserve ORIGINAL typed Result before any strict forensic assertion.
        self.report.event(serde_json::json!({"stage":"actual_phase_turn_return","original_result":format!("{actual:?}"),"diagnostics":diagnostics.as_ref().ok(),"diagnostic_error":diagnostics.as_ref().err().map(ToString::to_string),"original_caller_deadline":original_deadline,"shorter_original_parent":shorter}));
        let observed = diagnostics.unwrap();
        assert_eq!(observed["before"]["owner"], observed["after"]["owner"]);
        assert_eq!(observed["before"]["state"], observed["after"]["state"]);
        let before = &observed["before"]["job"];
        let after = &observed["after"]["job"];
        let fact = &observed["actual_cut"];
        if actual.is_ok() {
            assert!(fact.is_null(), "no fake early-Ok at a recipient cut");
            assert!(
                after.is_null(),
                "Ok means the actual complete job was removed"
            );
            assert!(
                before["phase"] == "Publish"
                    || (before["phase"] == "Activate"
                        && before["position"].as_u64()
                            == before["members"].as_array().map(|v| v.len() as u64)),
                "Ok must follow all real recipient phases before actual publisher"
            );
            return (true, observed);
        }
        if mode == 1 {
            assert!(
                matches!(actual, Err(BicDbError::QueryCanceled)),
                "must reach original actual late cancel"
            );
        } else {
            assert!(
                matches!(actual, Err(BicDbError::QueryTimedOut)),
                "must reach original earliest bounded expiry"
            );
        }
        assert!(
            fact.is_object(),
            "actual successful recipient/live cut prerequisite unreached"
        );
        for k in [
            "parent_live",
            "role_live",
            "effective_live",
            "slot_live",
            "effective_not_after_parent",
        ] {
            assert_eq!(fact[k], true, "actual cut liveness/currentness {k}");
        }
        assert_eq!(
            fact["parent_deadline"], original_deadline,
            "the original caller was not renewed"
        );
        if mode == 1 {
            assert_eq!(
                observed["original_parent_expired_or_canceled_at_return"],
                true
            );
        } else {
            assert_eq!(observed["effective_expired_at_return"],true,"actual earliest original scope must have expired; OWN-only or caller-only isolation is not claimed");
        }
        let n = before["members"].as_array().unwrap().len() as u64;
        let position = before["position"].as_u64().unwrap();
        let (expected_phase, expected_position) = if position == n {
            (
                match before["phase"].as_str().unwrap() {
                    "Fence" => "Prepare",
                    "Prepare" => "Complete",
                    "Complete" => "Activate",
                    _ => panic!("no recipient after terminal activation"),
                },
                1,
            )
        } else {
            (before["phase"].as_str().unwrap(), position + 1)
        };
        assert_eq!(after["phase"], expected_phase);
        assert_eq!(
            after["position"], expected_position,
            "known success must advance before outer error"
        );
        assert_eq!(fact["phase"], expected_phase);
        assert_eq!(fact["position"], expected_position);
        assert_eq!(
            after["nonce"], before["nonce"],
            "no new session on late outer error"
        );
        assert_eq!(after["last_actual"]["returned_ok"], true);
        assert_eq!(after["last_actual"]["node"], fact["node"]);
        assert_eq!(after["last_actual"]["phase"], fact["phase"]);
        assert_eq!(
            fact["node"],
            after["members"][(expected_position - 1) as usize]
        );
        (false, observed)
    }
    fn finish(&mut self, first_shorter: bool) {
        let mut seen = BTreeSet::new();
        let mut done = false;
        let mut known_activate = false;
        let mut previous: Option<serde_json::Value> = None;
        for ordinal in 0..=4 * 64 {
            let (finished, observed) = self.advance(
                if first_shorter && ordinal == 0 { 2 } else { 1 },
                first_shorter && ordinal == 0,
            );
            if finished {
                done = true;
                break;
            }
            let fact = &observed["actual_cut"];
            let id = (
                fact["phase"].as_str().unwrap().to_string(),
                fact["position"].as_u64().unwrap(),
            );
            assert!(
                seen.insert(id),
                "same successful phase/recipient was replayed"
            );
            if let Some(before) = previous.as_ref() {
                assert_eq!(
                    observed["before"]["job"]["nonce"],
                    before["after"]["job"]["nonce"]
                );
                assert_eq!(
                    observed["before"]["job"]["position"],
                    before["after"]["job"]["position"]
                );
                assert_eq!(
                    observed["before"]["job"]["phase"],
                    before["after"]["job"]["phase"]
                );
            }
            known_activate |= fact["phase"] == "Activate";
            previous = Some(observed);
        }
        assert!(
            done && known_activate,
            "real Activate/publisher prerequisites unreached"
        );
        let actual_native = self.five.sources[self.leader]
            .test_initial_allocation_native_point(self.selected.id)
            .unwrap();
        assert_eq!(
            actual_native["range_index"], self.before_selected_native["range_index"],
            "command-free recovery must preserve actual materialized prefix"
        );
        assert_eq!(
            actual_native["history"],
            self.before_selected_native["history"]
        );
        let actual_range: crate::RangeDescriptor =
            serde_json::from_value(actual_native["actual_range"].clone()).unwrap();
        let mut expected = self.selected.clone();
        expected.epoch += 1;
        assert_eq!(
            actual_range, expected,
            "full effective voter descriptor after real native activation"
        );
        for host in &self.five.sources {
            host.test_initial_allocation_pause(false).unwrap();
        }
        // The SAME original SDK, no manual route refresh. Existing native
        // read/write/QQ/MQ producers must make both complete payloads succeed.
        let selected_old_key = key(
            &self.five.topology,
            self.selected.id,
            "phase-before-selected",
        )
        .unwrap();
        let selected_old = self
            .report
            .data_call("phase_original_full_selected_after_publisher", || {
                self.client.get_strong("items", &selected_old_key)
            })
            .unwrap();
        assert_eq!(
            serde_json::to_value(selected_old).unwrap(),
            serde_json::json!({"whole":"before selected","null":null})
        );
        let other_old_key = key(&self.five.topology, self.other.id, "phase-before-other").unwrap();
        let other_old = self
            .report
            .data_call("phase_original_full_untouched_after_publisher", || {
                self.client.get_strong("items", &other_old_key)
            })
            .unwrap();
        assert_eq!(
            serde_json::to_value(other_old).unwrap(),
            serde_json::json!({"whole":"before untouched"})
        );
        sdk(
            &mut self.report,
            &self.client,
            &key(
                &self.five.topology,
                self.selected.id,
                "phase-after-selected",
            )
            .unwrap(),
            serde_json::json!({"whole":"selected after actual phases","null":null}),
        )
        .unwrap();
        sdk(
            &mut self.report,
            &self.client,
            &key(&self.five.topology, self.other.id, "phase-after-other").unwrap(),
            serde_json::json!({"whole":"untouched after actual phases"}),
        )
        .unwrap();
        let after = self
            .five
            .sources
            .iter()
            .map(|h| h.test_initial_allocation_evidence())
            .collect::<Result<Vec<_>>>()
            .unwrap();
        self.report.event(serde_json::json!({"original_owner_evidence":self.before,"final_owner_evidence":after,"source_only_control_not_physical_qualification":true}));
        assert_eq!(self.before.len(), after.len());
        for (old, current) in self.before.iter().zip(after.iter()) {
            for field in ["registration", "coordinator", "database_open", "store_open"] {
                assert_eq!(
                    old[field], current[field],
                    "actual original writer/State owner changed at {field}"
                );
            }
        }
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        for host in &self.five.sources {
            let _ = host.test_initial_allocation_pause(false);
        }
        self.five.stop(&mut self.report);
    }
}
#[test]
fn native_allocation_actual_late_phase_cancel_retains_cursor_then_next_recipient_and_full_sdk() {
    let mut f = Fixture::start();
    f.finish(false);
}
#[test]
fn native_allocation_actual_earlier_parent_bounds_effective_expiry_and_retains_known_phase() {
    let mut f = Fixture::start();
    f.finish(true);
}
