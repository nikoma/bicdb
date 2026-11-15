//! Wait for an observed ORIGINAL function exit, never retry a business RPC or
//! renew its Role/OWN/request. The independent 2s scope is diagnostic only.
use super::*;
pub(super) fn await_original_work(
    donor: &NativeMetadataControlHost,
    original: &NativeSourceExportRequestV1,
    original_result: &Result<crate::distribution_range_image::NativeSourceExportReplyV1>,
    output: &std::path::Path,
) {
    let diagnostic = budget(2000);
    let started = Instant::now();
    let mut samples = Vec::new();
    let mut polls = 0u64;
    let final_observation = (|| -> Result<serde_json::Value> {
        loop {
            diagnostic.check()?;
            polls += 1;
            let observed = donor.test_source_export_original_work_status(original);
            if samples.len() < 8 {
                samples.push(serde_json::json!({"elapsed_us":started.elapsed().as_micros(),"actual_work":observed.as_ref().ok(),"observation_error":observed.as_ref().err().map(|e|format!("{e:?}"))}));
            }
            if let Ok(value) = observed {
                if value["entries"] == 1 && value["all_completed"] == true {
                    diagnostic.check()?;
                    return Ok(value);
                }
                if value["entries"].as_u64().is_some_and(|n| n > 1) {
                    return Err(BicDbError::Cluster("diagnostic observed more than one original Retire work entry; no retry expected".into()));
                }
            }
            std::thread::sleep(Duration::from_millis(2));
        }
    })();
    evidence(&output.join("source-export-original-work-completion.json"),&serde_json::json!({
        "exact_original_request":original,"original_RPC_error":original_result.as_ref().err().map(|e|format!("{e:?}")),
        "original_RPC_reply":original_result.as_ref().ok(),"diagnostic_scope_ms":2000,
        "elapsed_us":started.elapsed().as_micros(),"polls":polls,"first_samples":samples,
        "final_actual_work":final_observation.as_ref().ok(),"diagnostic_error":final_observation.as_ref().err().map(|e|format!("{e:?}")),
        "business_RPC_retries":0,"new_Role_or_OWN":false,"original_budgets_extended":false,
        "completion_is_recipient_function_after_guards_not_full_socket_worker":true,
        "server_parent_recorded_separately_from_client_parent":true})).unwrap();
    let observed = final_observation
        .expect("actual original work did not finish within separate diagnostic scope");
    assert_eq!(observed["entries"], 1);
    assert_eq!(
        observed["work"][0]["exact_request"],
        serde_json::to_value(original).unwrap()
    );
    assert_eq!(
        observed["work"][0]["completed_after_all_business_guards_dropped"],
        true
    );
    assert_eq!(observed["work"][0]["panicked"], false);
}
