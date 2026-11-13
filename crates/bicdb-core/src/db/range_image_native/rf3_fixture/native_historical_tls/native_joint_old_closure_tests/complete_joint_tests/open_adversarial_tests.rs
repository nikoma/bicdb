//! Adversarial scheduling of real producers. No synthesized receipt, changed
//! quorum, forced generation, schema import or released Frozen owner.
use super::*;
use crate::distribution_range_consensus::RangeWriteStore;
use std::cell::{Cell, RefCell};
use std::rc::Rc;

#[test]
fn native_joint_open_post_fsync_step_cancel_recovers_exact_fact_with_original_caller_live() {
    run_complete_joint(4);
}
#[test]
fn native_joint_open_post_fsync_original_caller_cancel_cannot_start_fresh_phase() {
    run_complete_joint(5);
}
#[test]
fn native_joint_open_final_sweep_foreign_actual_writer_reenrollment_refuses_old_phase() {
    run_complete_joint(6);
}

fn collect(
    fixture: &EnforcedNetwork,
    published: &Arc<crate::distribution_range_handoff::NativeJointPublishedServingQuorum>,
) -> crate::Result<Arc<crate::distribution_range_handoff::NativeJointOpenPhaseQuorum>> {
    let mut round = published.begin_fresh_open_phase(Duration::from_secs(4))?;
    for id in [node("n1"), node("n2"), node("n3")] {
        round.acquire_actual_member(&fixture.clients[&id], &node("n1"))?;
    }
    Ok(Arc::new(round.finish()?))
}
fn catalog_path(fixture: &EnforcedNetwork, id: &crate::ClusterNodeId) -> std::path::PathBuf {
    fixture.native.databases[id]
        .read()
        .data_path()
        .join(crate::distribution_range_consensus::DEFAULT_RANGE_WRITE_LOG)
}
// Receipt SHA binds the actual base/prefix and physical namespace mapping;
// B remains the actual target-local visibility bound, never range R.
type Point = (u64, u64, Uuid, String, String, String);
fn points(
    fixture: &EnforcedNetwork,
    range: crate::RangeId,
) -> BTreeMap<crate::ClusterNodeId, Point> {
    [node("n1"), node("n2"), node("n3")]
        .into_iter()
        .map(|id| {
            let db = fixture.native.databases[&id].read();
            let point = db.current_native_range_source_lineage(range).unwrap();
            (
                id,
                (
                    point.range_index(),
                    point.actual_local_visibility_sequence(),
                    point.history_generation(),
                    point.logical_genesis_sha256().to_owned(),
                    point.logical_history_sha256().to_owned(),
                    point.native_receipt_sha256().to_owned(),
                ),
            )
        })
        .collect()
}
fn assert_closed(fixture: &EnforcedNetwork, range: crate::RangeId, index: u64) {
    for id in [node("n1"), node("n2"), node("n3")] {
        let db = fixture.native.databases[&id].read();
        assert!(db.range_mutation_gate().try_enter(range).is_err());
        assert_eq!(
            db.current_native_range_source_lineage(range)
                .unwrap()
                .range_index(),
            index
        );
    }
}

pub(super) fn exercise(
    fixture: &EnforcedNetwork,
    prepared: &Arc<NativeJointPreparedServingQuorum>,
    published: Arc<crate::distribution_range_handoff::NativeJointPublishedServingQuorum>,
    parent: &CancellationToken,
    range: crate::RangeId,
    mode: u8,
) {
    assert!(
        prepared.require_current().is_err(),
        "old preparation stays expired"
    );
    let local = node("n3");
    let path = catalog_path(fixture, &local);
    let before = std::fs::read(&path).unwrap();
    let native_before = points(fixture, range);
    if mode == 6 {
        foreign_reenrollment(fixture, &published, parent, range, &path, &before);
        assert_eq!(points(fixture, range), native_before);
        return;
    }
    let reached = Rc::new(Cell::new(false));
    let persisted = Rc::new(RefCell::new(None));
    let mut failed_after_fsync = false;
    for attempt in 0..3 {
        let step = parent.child();
        let result = (|| -> crate::Result<()> {
            let round = collect(fixture, &published)?;
            let open = round.acquire_local_open(&fixture.clients[&local], &node("n1"))?;
            let cut_reached = reached.clone();
            let bytes = persisted.clone();
            let cut_path = path.clone();
            let canceled = if mode == 5 {
                parent.clone()
            } else {
                step.clone()
            };
            RangeWriteStore::test_joint_open_after_persist(
                move || {
                    cut_reached.set(true);
                    *bytes.borrow_mut() = Some(std::fs::read(cut_path)?);
                    canceled.cancel();
                    Ok(())
                },
                || open.persist_local_catalogue_activation(&step),
            )?;
            panic!("post-fsync cancellation must not return a live activation receipt");
        })();
        eprintln!(
            "ACTUAL_JOINT_OPEN_ADVERSARIAL mode={mode} attempt={attempt} fsync_cut={} error={:?}",
            reached.get(),
            result.as_ref().err()
        );
        if reached.get() {
            assert!(matches!(result, Err(crate::BicDbError::QueryCanceled)));
            failed_after_fsync = true;
            break;
        }
        assert!(
            matches!(result, Err(crate::BicDbError::QueryTimedOut)),
            "pre-cut failure: {result:?}"
        );
    }
    assert!(
        failed_after_fsync,
        "must reach actual successful catalogue fsync"
    );
    let bytes = persisted.borrow().clone().unwrap();
    assert_ne!(bytes, before, "real activation changed durable catalogue");
    assert_eq!(std::fs::read(&path).unwrap(), bytes);
    assert_closed(fixture, range, published.data().range_index());
    assert_eq!(points(fixture, range), native_before);
    if mode == 5 {
        assert!(parent.is_cancel_requested());
        assert!(matches!(
            published.begin_fresh_open_phase(Duration::from_secs(4)),
            Err(crate::BicDbError::QueryCanceled)
        ));
        // There is no new-operation/cold-controller producer in this contract;
        // a new phase cannot replace the retained original caller token.
        assert_eq!(std::fs::read(path).unwrap(), bytes);
        eprintln!("ACTUAL_JOINT_OPEN_POST_FSYNC original-caller-canceled=true fresh-same-operation-refused=true still-frozen=true");
        return;
    }
    parent.check().unwrap();
    let mut recovered = None;
    for attempt in 0..3 {
        let result = (|| {
            let round = collect(fixture, &published)?;
            let open = round.acquire_local_open(&fixture.clients[&local], &node("n1"))?;
            open.persist_local_catalogue_activation(parent)
        })();
        match result {
            Ok(receipt) => {
                recovered = Some(receipt);
                break;
            }
            Err(crate::BicDbError::QueryTimedOut) if attempt < 2 => (),
            Err(error) => panic!("fresh exact-fact recovery refused: {error}"),
        }
    }
    let recovered = recovered.expect("bounded recovery reaches actual retained activation");
    recovered.require_live().unwrap();
    assert_eq!(
        std::fs::read(path).unwrap(),
        bytes,
        "exact persisted fact recovery must NOT rewrite catalogue"
    );
    assert!(
        prepared.require_current().is_err(),
        "new phase never renews old preparation"
    );
    assert_closed(fixture, range, published.data().range_index());
    assert_eq!(points(fixture, range), native_before);
    eprintln!("ACTUAL_JOINT_OPEN_POST_FSYNC step-canceled=true original-caller-live=true fresh-exact-fact-recovered=true catalogue-bytes-identical=true still-frozen=true");
}

fn foreign_reenrollment(
    fixture: &EnforcedNetwork,
    published: &Arc<crate::distribution_range_handoff::NativeJointPublishedServingQuorum>,
    parent: &CancellationToken,
    range: crate::RangeId,
    path: &std::path::Path,
    before: &[u8],
) {
    let foreign = node("n1");
    let reached = Rc::new(Cell::new(false));
    // Retain the actual new registration through the final old-phase refusal.
    let installed = Rc::new(RefCell::new(None));
    let mut rejected = false;
    for attempt in 0..3 {
        let result = (|| -> crate::Result<()> {
            // All real TLS/MQ work precedes the actual local service/DB locks.
            let business = Uuid::now_v7();
            let mq = fixture.clients[&foreign].acquire_host_metadata_observation(
                &node("n1"),
                business,
                Duration::from_secs(5),
                parent,
            )?;
            let policy = {
                let service = fixture.native.services[&foreign].lock();
                let db = fixture.native.databases[&foreign].read();
                service.prepare_native_host_writer_enrollment(&db, mq, business)?
            };
            let round = collect(fixture, published)?;
            let open = round.acquire_local_open(&fixture.clients[&node("n3")], &node("n1"))?;
            let db = fixture.native.databases[&foreign].clone();
            let coordinator = fixture.coordinators[&foreign].clone();
            let retained = installed.clone();
            let cut_reached = reached.clone();
            RangeWriteStore::test_joint_open_after_external(
                move || {
                    let guard = db
                        .read()
                        .install_enrolled_native_coordinator(coordinator, policy)?;
                    *retained.borrow_mut() = Some(guard);
                    cut_reached.set(true);
                    Ok(())
                },
                || open.persist_local_catalogue_activation(parent),
            )?;
            panic!("old phase must not survive actual foreign writer replacement");
        })();
        eprintln!(
            "ACTUAL_JOINT_OPEN_FOREIGN_REENROLL attempt={attempt} actual_install={} error={:?}",
            reached.get(),
            result.as_ref().err()
        );
        if reached.get() {
            let error = result.err().expect("old phase must refuse").to_string();
            assert!(
                error.contains("writer") || error.contains("registration"),
                "must reject changed actual writer, not incidental timeout: {error}"
            );
            rejected = true;
            break;
        }
        assert!(
            matches!(result, Err(crate::BicDbError::QueryTimedOut)),
            "pre-cut failure: {result:?}"
        );
    }
    assert!(rejected && installed.borrow().is_some());
    assert_eq!(
        std::fs::read(path).unwrap().as_slice(),
        before,
        "local catalogue unchanged at final-sweep race refusal"
    );
    assert_closed(fixture, range, published.data().range_index());
    assert!(
        collect(fixture, published).is_err(),
        "fresh MQ cannot hide changed original writer registration"
    );
    eprintln!("ACTUAL_JOINT_OPEN_FOREIGN_REENROLL after-last-sweep=true actual-fresh-enrollment=true frozen-retained=true old-phase-refused-before-local-catalogue-write=true");
}
