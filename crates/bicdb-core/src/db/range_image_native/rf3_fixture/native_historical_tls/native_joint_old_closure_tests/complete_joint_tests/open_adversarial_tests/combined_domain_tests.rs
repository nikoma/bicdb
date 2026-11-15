//! Actual original owners only: no metadata/epoch setter or constructed QQ.
//! These are same-process native component definitions, not SDK acceptance.
use super::*;

#[test]
fn native_joint_open_after_final_external_sweep_actual_cancel_refuses_before_io() {
    run_complete_joint(7);
}
#[test]
fn native_joint_open_same_local_original_domain_reenrollment_refuses_old_admission() {
    run_complete_joint(8);
}

pub(super) fn exercise(
    fixture: &EnforcedNetwork,
    published: &Arc<crate::distribution_range_handoff::NativeJointPublishedServingQuorum>,
    parent: &CancellationToken,
    range: crate::RangeId,
    mode: u8,
) {
    // n1 is a real OLD/NEW survivor with the original installed writer. Using
    // the original target's never-writer identity would test another contract.
    let local = node("n1");
    let path = catalog_path(fixture, &local);
    let before = std::fs::read(&path).unwrap();
    let native_before = points(fixture, range);
    let round = collect(fixture, published).unwrap();
    let open = round
        .acquire_local_open(&fixture.clients[&local], &node("n1"))
        .unwrap();
    if mode == 7 {
        after_external_cancel(
            fixture,
            &open,
            parent,
            range,
            &path,
            &before,
            &native_before,
        );
        return;
    }

    let original = fixture.hosts[&local].binding_diagnostic();
    // All real MQ/enrollment work is outside the original activation's
    // service/DB guards. The normal factory reacquires its actual service;
    // calling it from an activation hook would self-contend, not prove drift.
    let business = Uuid::now_v7();
    let actual = (|| -> crate::Result<_> {
        let mq = fixture.clients[&local].acquire_host_metadata_observation(
            &node("n1"),
            business,
            Duration::from_secs(5),
            parent,
        )?;
        let policy = {
            let service = fixture.native.services[&local].lock();
            let db = fixture.native.databases[&local].read();
            service.prepare_native_host_writer_enrollment(&db, mq, business)?
        };
        let guard = fixture.native.databases[&local]
            .read()
            .install_enrolled_native_coordinator(fixture.coordinators[&local].clone(), policy)?;
        let old_after_install = fixture.hosts[&local].binding_diagnostic();
        let new_binding = guard.binding_diagnostic();
        let original_open_live_before_call = open.cancellation().check().is_ok();
        let result = open.persist_local_catalogue_activation(parent);
        Ok((
            guard,
            old_after_install,
            new_binding,
            original_open_live_before_call,
            result,
        ))
    })();
    // Preserve the original API result before any assertions/fresh observer.
    eprintln!(
        "ACTUAL_JOINT_OPEN_SAME_LOCAL_DOMAIN original={original} actual={:?}",
        actual
            .as_ref()
            .map(|(_, old, new, live, result)| (old, new, live, result.as_ref().err()))
    );
    let (retained_new_owner, old, new, original_live, result) =
        actual.expect("actual same-local enrollment prerequisite must complete");
    assert!(original_live, "expiry is not the changed-domain denial cut");
    assert_eq!(original["observed_binding_lifetime_live"], true);
    assert_eq!(old["observed_binding_lifetime_live"], false);
    assert_eq!(new["observed_binding_lifetime_live"], true);
    assert_ne!(
        original["persisted_policy_registration_generation"],
        new["persisted_policy_registration_generation"]
    );
    assert_eq!(
        original["persisted_policy_coordinator_id"],
        new["persisted_policy_coordinator_id"]
    );
    assert_eq!(
        original["persisted_policy_database_open_incarnation"],
        new["persisted_policy_database_open_incarnation"]
    );
    assert_eq!(
        original["persisted_policy_range_store_open_id"],
        new["persisted_policy_range_store_open_id"]
    );
    assert!(
        matches!(&result, Err(crate::BicDbError::Cluster(detail))
        if detail == "native range image: native writer hook lifetime ended"
        || detail == "native range image: native writer hook/open is no longer current"),
        "must refuse the actual retired original registration, not a transport/timeout error: {:?}",
        result.as_ref().err()
    );
    assert_eq!(std::fs::read(path).unwrap(), before);
    assert_eq!(points(fixture, range), native_before);
    assert_closed(fixture, range, published.data().range_index());
    // Retain the actual replacement through all original-domain assertions.
    assert_eq!(
        retained_new_owner.binding_diagnostic()["observed_binding_lifetime_live"],
        true
    );
}

#[allow(clippy::too_many_arguments)]
fn after_external_cancel(
    fixture: &EnforcedNetwork,
    open: &crate::distribution_range_handoff::NativeJointOpenAdmission,
    parent: &CancellationToken,
    range: crate::RangeId,
    path: &std::path::Path,
    before: &[u8],
    native_before: &BTreeMap<crate::ClusterNodeId, Point>,
) {
    let reached = Rc::new(Cell::new(false));
    let live = Rc::new(Cell::new(false));
    let cut_reached = reached.clone();
    let cut_live = live.clone();
    let original = open.cancellation().clone();
    let result = RangeWriteStore::test_joint_open_after_external(
        move || {
            cut_reached.set(true);
            let is_live = original.check().is_ok();
            cut_live.set(is_live);
            if is_live {
                original.cancel();
            }
            Ok(())
        },
        || open.persist_local_catalogue_activation(parent),
    );
    eprintln!("ACTUAL_JOINT_OPEN_FINAL_EXTERNAL_CANCEL actual_cut={} original_live={} original_result={:?}",
        reached.get(), live.get(), result.as_ref().err());
    assert!(
        reached.get(),
        "earlier refusal is an unreached final-external cut"
    );
    assert!(
        live.get(),
        "original deadline must still be live before cancellation"
    );
    assert!(
        matches!(&result, Err(crate::BicDbError::QueryCanceled)),
        "must preserve the original actual cancellation: {:?}",
        result.as_ref().err()
    );
    assert_eq!(std::fs::read(path).unwrap().as_slice(), before);
    assert_eq!(&points(fixture, range), native_before);
    assert_closed(fixture, range, open.data().range_index());
}
