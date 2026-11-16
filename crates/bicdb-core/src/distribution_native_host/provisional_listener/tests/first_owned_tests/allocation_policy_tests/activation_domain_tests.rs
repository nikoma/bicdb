//! Genuine normal-host allocation then bounded DATA-only comparison probes.
//! Definitions only; no compiler/runtime/physical acceptance is claimed.
use super::*;
#[test]
fn native_allocation_actual_activation_retains_full_metadata_and_effective_native_voters() {
    run(&[0]);
}
#[test]
fn native_allocation_actual_activation_refuses_changed_metadata_shape_learner_and_epoch() {
    run(&[1, 2, 3, 4]);
}
#[test]
fn native_allocation_actual_activation_refuses_other_retained_installed_writer_state() {
    run(&[5]);
}
#[test]
fn native_allocation_actual_activation_refuses_changed_effective_native_claim() {
    run(&[6]);
}
fn run(modes: &[u8]) {
    let mut report = Report {
        events: Vec::new(),
        omitted: 0,
        stderr_write_errors: 0,
    };
    let mut five = Five::start(&mut report)
        .unwrap_or_else(|e| panic!("actual allocation activation prerequisite: {e}"));
    let actual = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| -> Result<()> {
        // Original body/assertions/windows remain exact, including real
        // allocation->Complete->Activate and full selected/untouched SDK tails.
        super::exercise(&mut five, &mut report, 0)?;
        let selected = five.topology.ranges.values().next().unwrap().clone();
        let untouched = five
            .topology
            .ranges
            .values()
            .find(|r| r.id != selected.id)
            .unwrap()
            .clone();
        let leader = (0..five.sources.len())
            .find(|i| node(*i) == selected.leader)
            .unwrap();
        let foreign = (leader + 1) % five.sources.len();
        for mode in modes {
            let owner = if *mode == 5 { foreign } else { leader };
            let observed = report.call("allocation_actual_activation_domain_comparison", || {
                five.sources[leader].test_allocation_activation_domain_comparison(
                    &five.sources[owner],
                    selected.id,
                    *mode,
                    &five.lifetime,
                )
            })?;
            // Actual original Result was retained by the helper before any
            // projection; report it before assertions/another probe/cleanup.
            report.event(serde_json::json!({"actual_allocation_activation_comparison":observed,
                "source_mode":mode,"actual_destination":node(leader),"actual_retained_owner":node(owner),
                "claim_DATA_not_Role_or_modified_native_record":true,
                "negative_scope":"shared production comparison before new decision IO, not a new activation RPC"}));
            assert_eq!(
                observed["postcomparison_forensic_error"],
                serde_json::Value::Null
            );
            assert_eq!(observed["native_before"], observed["native_after"]);
            assert_eq!(
                observed["decision_before_sha256"],
                observed["decision_after_sha256"]
            );
            assert_eq!(
                observed["decision_before_sha256"].as_str().unwrap().len(),
                64
            );
            assert_eq!(
                observed["actual_scope_configuration_sha256"]
                    .as_str()
                    .unwrap()
                    .len(),
                64
            );
            assert!(observed["actual_decision_xid_and_R"][0].as_u64().unwrap() > 0);
            assert!(observed["actual_decision_xid_and_R"][1].as_u64().unwrap() > 0);
            let full: crate::RangeDescriptor =
                serde_json::from_value(observed["actual_full_metadata"].clone())?;
            let native: crate::RangeDescriptor =
                serde_json::from_value(observed["actual_effective_native"].clone())?;
            assert_eq!(full.id, selected.id);
            assert_eq!(full.epoch, selected.epoch + 1);
            assert_eq!(
                full.replicas
                    .iter()
                    .filter(|r| r.role == RangeReplicaRole::Learner)
                    .count(),
                1
            );
            assert_eq!(native.epoch, full.epoch);
            let mut effective = full.clone();
            effective
                .replicas
                .retain(|r| r.role == RangeReplicaRole::Voter);
            assert_eq!(effective, native);
            assert_eq!(native.replicas.len(), selected.replicas.len());
            if *mode == 0 {
                assert_eq!(observed["actual_returned_ok"], true);
                assert_eq!(observed["actual_original_error"], serde_json::Value::Null);
            } else {
                assert_eq!(observed["actual_returned_ok"], false);
                let expected=match mode {
                    1..=4=>"native range image: allocation serving projection differs from actual full metadata range",
                    5=>"native range image: allocation serving projection requires SAME installed writer State",
                    6=>"native range image: allocation serving projection differs from actual effective native range",
                    _=>unreachable!(),
                };
                assert_eq!(observed["actual_exact_error_class"],expected,
                    "missing/earlier timeout, busy, IO or unrelated refusal cannot satisfy comparison cut");
            }
        }
        let client = crate::ClusterEventClient::new(&five.config, five.topology.clone())?
            .with_request_timeout(Duration::from_secs(2))?;
        sdk(
            &mut report,
            &client,
            &key(
                &five.topology,
                selected.id,
                "activation-domain-after-selected",
            )?,
            serde_json::json!({"full":"selected after actual comparison","present_null":null}),
        )?;
        sdk(
            &mut report,
            &client,
            &key(
                &five.topology,
                untouched.id,
                "activation-domain-after-untouched",
            )?,
            serde_json::json!({"full":"untouched after actual comparison","present_null":null}),
        )?;
        Ok(())
    }));
    // Preserve the original primary outcome before cleanup/projection.
    report.event(serde_json::json!({"actual_activation_domain_primary_before_cleanup":actual.as_ref().ok()
        .and_then(|r|r.as_ref().err()).map(crate::distribution_native_host::remote_first::error_data),
        "same_process_normal_TLS_hosts_only":true,"physical_qualification":false}));
    five.stop(&mut report);
    match actual {
        Ok(r) => r.unwrap(),
        Err(p) => std::panic::resume_unwind(p),
    }
}
