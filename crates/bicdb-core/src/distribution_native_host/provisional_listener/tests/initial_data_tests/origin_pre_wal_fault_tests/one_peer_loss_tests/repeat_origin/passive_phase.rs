//! Passive observation only: no client traffic until original native5 and the
//! actual publisher's scheduling hint have been observed. The subsequent
//! ordinary Strong calls reprove authority/readQQ; the hint grants nothing.
use super::*;

pub(super) fn require_original_call_fits(end: Instant, parent: &CancellationToken) -> Result<()> {
    parent.check()?;
    // Keep the SAME actual client and its original 2s timeout. Do not begin
    // such a call if that bound would extend beyond the original 20s gate.
    check(
        end.saturating_duration_since(Instant::now()) >= Duration::from_secs(2),
        "passive phase remaining original20s cannot contain another original2s caller",
    )
}

#[allow(clippy::too_many_arguments)]
pub(super) fn wait_for_original_native_five(
    f: &Fixture,
    originals: &[OriginalWriter],
    range: &crate::RangeDescriptor,
    expected: u64,
    keys: &[&str],
    original_all: &serde_json::Value,
    journal: &Arc<Journal>,
    start: Instant,
    end: Instant,
    parent: &CancellationToken,
) -> Result<()> {
    journal.append(
        serde_json::json!({"kind":"passive_native5_phase_observation_started",
        "elapsed_ns":start.elapsed().as_nanos(),"expected_actual_head":expected,
        "no_client_read_or_write_until_actual_phase_observed":true,
        "no_direct_recovery_or_manual_maintenance":true,"same_original20s_clock":true}),
    )?;
    let mut samples = 0;
    loop {
        parent.check()?;
        samples += 1;
        let actual = actual_all(f, originals, range, expected, keys);
        // Reading this existing real publication hint cannot grant any
        // authority. Ordinary APP and Strong below must independently pass.
        let publication_hint = originals[0].0.initial_data_leader_admission_observed(range);
        let elapsed = start.elapsed().as_nanos();
        journal.append(serde_json::json!({"kind":"passive_original_native5_sample",
            "sample":samples,"receipt_elapsed_ns":elapsed,"actual_all":actual.as_ref().ok(),
            "actual_error":actual.as_ref().err().map(ToString::to_string),
            "actual_publisher_hint_observed":publication_hint,"hint_is_not_read_or_write_authority":true}))?;
        if let Ok(actual) = actual {
            if publication_hint {
                for key in keys.iter().take(keys.len().saturating_sub(1)) {
                    check(
                        actual[0]["records"][*key] == original_all[0]["records"][*key],
                        "passive phase changed a prior complete acknowledged record",
                    )?;
                }
                parent.check()?;
                require_original_call_fits(end, parent)?;
                journal.append(serde_json::json!({"kind":"passive_original_native5_observed_before_normal_calls",
                    "receipt_elapsed_ns":start.elapsed().as_nanos(),"actual_all":actual,
                    "actual_publisher_hint_observed":true,"native_snapshots_are_data_not_Ready":true,
                    "ordinary_Strong_must_still_reprove_fresh_authority_and_readQQ":true,
                    "same_original_client_2s_setting":true,"same_original20s_absolute_end":true}))?;
                return Ok(());
            }
        }
        std::thread::sleep(Duration::from_millis(25));
    }
}
