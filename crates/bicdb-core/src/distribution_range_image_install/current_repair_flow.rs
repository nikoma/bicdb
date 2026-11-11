//! Shared actual target transfer phases for component tests. Every authority
//! remains an opaque source/role/service value obtained by the caller's actual
//! consensus/native fixture; this helper never creates grants or counters.
use super::*;
use crate::distribution_range_recovery::RecoveryRoleAdmission;
use parking_lot::Mutex;

pub(crate) fn materialize_actual_current_repair_image(
    capture: &mut Option<crate::distribution_range_image::RangeImageCapture>,
    service: &Arc<Mutex<crate::distribution_data::ClusterDataNodeService>>,
    fresh_role: impl Fn() -> RecoveryRoleAdmission,
    before_clear: impl FnOnce(),
    mut install: NativeRangeImageInstaller,
) -> VerifiedNativeRangeImageInstall {
    stage_and_clear_actual_current_repair_image(
        capture,
        service,
        &fresh_role,
        before_clear,
        &mut install,
    );
    loop {
        let step = {
            let capture = capture.as_mut().unwrap();
            let cursor = capture.cursor().clone();
            capture
                .export_step(&cursor, &options(), &CancellationToken::uncancelable())
                .unwrap()
        };
        let role = fresh_role();
        let source = capture
            .as_ref()
            .unwrap()
            .authorize_target_install()
            .unwrap();
        let attempt = install.fence().attempt_id();
        service
            .lock()
            .copy_native_range_image_step(
                &mut install,
                &NativeImageInstallRole::CurrentVoterRepair(&role),
                &source,
                attempt,
                &step,
                &CancellationToken::uncancelable(),
            )
            .unwrap();
        if step.complete {
            break;
        }
    }
    let source = capture.take().unwrap().complete().unwrap();
    let role = fresh_role();
    let mut verification = service
        .lock()
        .begin_native_range_image_verification(
            install,
            &NativeImageInstallRole::CurrentVoterRepair(&role),
            source,
            &CancellationToken::uncancelable(),
        )
        .unwrap();
    let mut completed = false;
    for _ in 0..128 {
        let role = fresh_role();
        if service
            .lock()
            .advance_native_range_image_verification(
                &mut verification,
                &NativeImageInstallRole::CurrentVoterRepair(&role),
                &CancellationToken::uncancelable(),
            )
            .unwrap()
            == NativeTargetImageVerificationProgress::Complete
        {
            completed = true;
            break;
        }
    }
    assert!(
        completed,
        "bounded actual lease target verification must finish"
    );
    let role = fresh_role();
    service
        .lock()
        .finish_native_range_image_verification(
            verification,
            &NativeImageInstallRole::CurrentVoterRepair(&role),
            &CancellationToken::uncancelable(),
        )
        .unwrap()
}

/// Stops after actual complete history staging/audit and target clear. It
/// deliberately publishes no native image receipt and performs no row copy.
pub(crate) fn stage_and_clear_actual_current_repair_image(
    capture: &mut Option<crate::distribution_range_image::RangeImageCapture>,
    service: &Arc<Mutex<crate::distribution_data::ClusterDataNodeService>>,
    fresh_role: &impl Fn() -> RecoveryRoleAdmission,
    before_clear: impl FnOnce(),
    install: &mut NativeRangeImageInstaller,
) {
    loop {
        let step = {
            let capture = capture.as_mut().unwrap();
            let cursor = capture.history_cursor().clone();
            capture
                .export_history_step(&cursor, 256, &CancellationToken::uncancelable())
                .unwrap()
        };
        let role = fresh_role();
        let source = capture
            .as_ref()
            .unwrap()
            .authorize_target_install()
            .unwrap();
        let stage_id = install.history_stage_id();
        let progress = service
            .lock()
            .stage_native_range_image_history(
                install,
                &NativeImageInstallRole::CurrentVoterRepair(&role),
                &source,
                stage_id,
                &step,
                &CancellationToken::uncancelable(),
            )
            .unwrap();
        if progress.cursor.complete() {
            break;
        }
    }
    loop {
        let role = fresh_role();
        let source = capture
            .as_ref()
            .unwrap()
            .authorize_target_install()
            .unwrap();
        if service
            .lock()
            .audit_native_range_image_history(
                install,
                &NativeImageInstallRole::CurrentVoterRepair(&role),
                &source,
                256,
                &CancellationToken::uncancelable(),
            )
            .unwrap()
            .audit_complete
        {
            break;
        }
    }
    before_clear();
    loop {
        let role = fresh_role();
        let source = capture
            .as_ref()
            .unwrap()
            .authorize_target_install()
            .unwrap();
        if service
            .lock()
            .clear_native_range_image_step(
                install,
                &NativeImageInstallRole::CurrentVoterRepair(&role),
                &source,
                &CancellationToken::uncancelable(),
            )
            .unwrap()
        {
            break;
        }
    }
}
