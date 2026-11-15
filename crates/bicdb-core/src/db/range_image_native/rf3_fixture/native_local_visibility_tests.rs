//! Genuine INITIAL RF3/local-apply component. It does not provide a current
//! metadata read quorum, enrolled host, SDK endpoint, or Strong permission.
use super::*;
use crate::db::commit_visibility_tests::{arm_after_real_wal, arm_native_public_finalizer_probe};
use crate::TransactionOutcome;
use serde_json::json;
use std::sync::mpsc::{self, Sender};

struct ReleaseOrigin(Option<Sender<()>>);
impl ReleaseOrigin {
    fn release(&mut self) {
        if let Some(sender) = self.0.take() {
            let _ = sender.send(());
        }
    }
}
impl Drop for ReleaseOrigin {
    fn drop(&mut self) {
        self.release();
    }
}

fn actual_key(topology: &ClusterTopology, range: RangeId, label: &str) -> String {
    (0..100_000)
        .map(|index| format!("native-local-{label}-{index}"))
        .find(|key| topology.range_for_key("items", key).unwrap().id == range)
        .expect("actual namespace/key hash must reach selected INITIAL range")
}

#[test]
fn native_local_rf3_real_follower_apply_completes_before_other_range_global_gap_closes() {
    let fixture = ActualNativeRf3Fixture::new_with_split_initial_range_leaders();
    let topology = fixture.topology.read().clone();
    let n0 = ClusterNodeId::new("n0").unwrap();
    let n1 = ClusterNodeId::new("n1").unwrap();
    let range_a = topology
        .ranges
        .values()
        .find(|range| range.leader == n1)
        .unwrap()
        .clone();
    let range_b = topology
        .ranges
        .values()
        .find(|range| range.leader == n0)
        .unwrap()
        .clone();
    assert_ne!(range_a.id, range_b.id);
    // This is the same genuine service transport as the original RF3 fixture,
    // with its actual second INITIAL leader as caller, not a synthetic backend.
    let n1_transport = Arc::new(ActualNativeRf3ServiceTransport {
        caller: n1.clone(),
        topology: fixture.topology.clone(),
        services: fixture.services.clone(),
        observations: Mutex::new(Vec::new()),
        unavailable: Mutex::new(BTreeSet::new()),
        native_delivery: std::sync::atomic::AtomicBool::new(false),
        native_drop_once: Mutex::new(None),
        lose_commit_replies_after_durability: std::sync::atomic::AtomicBool::new(false),
    });
    let n1_coordinator = Arc::new(
        RangeWriteCoordinator::new_schema_fenced(
            topology.cluster_id.clone(),
            n1.clone(),
            fixture.topology.clone(),
            fixture.services[&n1].clone(),
            n1_transport.clone(),
        )
        .unwrap(),
    );
    n1_coordinator.recover_local_leadership(8).unwrap();
    fixture.databases[&n1]
        .read()
        .install_commit_admission(n1_coordinator.clone());
    n1_transport.enable_native_delivery();
    fixture.transport.enable_native_delivery();
    for (node, database) in &fixture.databases {
        let database = database.read();
        for range in [&range_a, &range_b] {
            let actual = database
                .current_native_range_source_lineage(range.id)
                .unwrap();
            assert_eq!(actual.range_index(), 0);
            assert_eq!(actual.native_write_range().unwrap(), range);
            assert_eq!(actual.bootstrap_id(), fixture.specs[node].bootstrap_id);
        }
    }
    eprintln!(
        "ISSUE205_NATIVE_COMPONENT_INITIAL {}",
        json!({
            "actual_initial_ranges": [&range_a, &range_b],
            "actual_birth_specs": fixture.specs,
            "storage": "ServerPaged with fsync=true; actual native birth/genesis",
            "identity": "trusted embedded INITIAL RF3 service callers; no TLS claim",
            "enforcement": "original non-enrolled/non-enforced fixture constructor",
            "metadata": "declared INITIAL topology; no current request-specific MQ",
            "scope": "typed local materialization component; no Strong/SDK/physical qualification"
        })
    );
    let a = Record::new(actual_key(&topology, range_a.id, "held-origin"))
        .with_metadata(json!({"value": "full lower native value", "present_null": null}))
        .with_payload(vec![0, 255, 17, 1]);
    let b = Record::new(actual_key(&topology, range_b.id, "independent-follower"))
        .with_metadata(json!({"value": "full higher native value", "nested": [2, null, 7]}))
        .with_payload(vec![0, 255, 17, 2]);
    eprintln!(
        "ISSUE205_NATIVE_COMPONENT_INTENTS {}",
        json!({
            "A_record": &a, "A_payload_sha256": a.content_hash().unwrap(),
            "A_actual_initial_key_route": topology.range_for_key("items", &a.id).unwrap(),
            "B_record": &b, "B_payload_sha256": b.content_hash().unwrap(),
            "B_actual_initial_key_route": topology.range_for_key("items", &b.id).unwrap(),
            "evidence": "test stdout only; not an independent fsynced external ledger or SDK trace"
        })
    );
    std::thread::scope(|scope| {
        let (a_wal_sender, a_wal) = mpsc::channel();
        let (release_sender, release_receiver) = mpsc::channel();
        // On assertion/timeout this local guard drops before implicit scope
        // joins; it never leaves the original A committer stranded.
        let mut release_a = ReleaseOrigin(Some(release_sender));
        let lower = scope.spawn(|| {
            // No service guard is held. Actual commit APIs borrow &BicDb;
            // sharing its outer read lock permits real independent ingress.
            let database = fixture.databases[&n1].read();
            let mut transaction = database.begin_transaction().unwrap();
            transaction.insert("items", a.clone()).unwrap();
            let observer = transaction.observe_outcome();
            let hook_observer = observer.clone();
            let _hook = arm_after_real_wal(Box::new(move |sequence| {
                a_wal_sender.send((sequence, hook_observer)).unwrap();
                release_receiver
                    .recv_timeout(Duration::from_secs(30))
                    .expect("native component parent failed to release actual A writer");
            }));
            (observer, transaction.commit_with_sequence())
        });
        let (a_sequence, a_observer) = a_wal.recv_timeout(Duration::from_secs(15)).unwrap();
        assert_eq!(
            a_observer.outcome(),
            TransactionOutcome::Committing {
                sequence: a_sequence
            }
        );
        let (b_sender, b_return) = mpsc::channel();
        let (public_probe_sender, public_probe) = mpsc::channel();
        let target_open = fixture.databases[&n1].read().incarnation_id().to_owned();
        let higher = scope.spawn(|| {
            // Selection uses the actual target open, never an authority DTO.
            // The private native finalizer validates its real producer before
            // this DATA-only probe invokes the wrong public API on the same Tx.
            let _public_probe = arm_native_public_finalizer_probe(
                target_open,
                Box::new(move |data| public_probe_sender.send(data).unwrap()),
            );
            let target_floor_before = fixture.databases[&n1]
                .read()
                .last_commit_seq_on_this_thread();
            let database = fixture.databases[&n0].read();
            let mut transaction = database.begin_transaction().unwrap();
            transaction.insert("items", b.clone()).unwrap();
            let started = Instant::now();
            let result = transaction.commit_with_sequence();
            let elapsed = started.elapsed();
            let target_floor_after = fixture.databases[&n1]
                .read()
                .last_commit_seq_on_this_thread();
            b_sender
                .send((result, elapsed, target_floor_before, target_floor_after))
                .unwrap();
        });
        let returned = b_return.recv_timeout(Duration::from_secs(5));
        let (b_result, b_elapsed, target_floor_before, target_floor_after) = match returned {
            Ok(result) => result,
            Err(error) => {
                // Release promptly on missing actual outcome. This is a failed
                // component prerequisite, not permission to skip service locks.
                eprintln!(
                    "ISSUE205_NATIVE_COMPONENT_NO_B_OUTCOME {}",
                    json!({
                        "actual_A_WAL_sequence": a_sequence,
                        "B_outcome_error": error.to_string(),
                        "scope": "B did not finish while actual A was held; no local proof claimed"
                    })
                );
                release_a.release();
                panic!("actual B failed to complete before original global five-second bound");
            }
        };
        let inspection = (|| -> Result<_> {
            let database = fixture.databases[&n1].read();
            let current = database.current_native_range_source_lineage(range_b.id)?;
            let floor = current.actual_local_visibility_sequence();
            let global_prefix = database.current_visibility_watermark();
            // This is a local physical assertion at the bound from the actual
            // selected native receipt; it is explicitly NOT a Strong permit.
            let mut reader = database.begin_transaction()?;
            reader.set_snapshot_isolation(false, floor)?;
            let full_value = reader.get("items", &b.id)?;
            let data = json!({
                "actual_R": current.range_index(),
                "actual_H": current.logical_history_sha256(),
                "actual_local_B": floor,
                "native_receipt_sha256": current.native_receipt_sha256(),
                "actual_birth_bootstrap": current.bootstrap_id(),
                "actual_global_prefix_while_A_held": global_prefix,
                "actual_local_snapshot": reader.visibility_watermark(),
                "full_value": full_value.as_deref()
            });
            Ok((
                data,
                current.range_index(),
                floor,
                global_prefix,
                full_value,
            ))
        })();
        let public_finalizer_data = public_probe.try_recv();
        // No need to exhaust A's ordinary wait. Once B's actual selected
        // physical point is captured, permit the genuine lower apply to finish.
        release_a.release();
        eprintln!(
            "ISSUE205_NATIVE_COMPONENT_B_RETURN {}",
            json!({
                "actual_A_WAL_sequence": a_sequence,
                "actual_B_origin_result": format!("{b_result:?}"),
                "B_elapsed_millis": b_elapsed.as_millis(),
                "target_thread_floor_before": target_floor_before,
                "target_thread_floor_after": target_floor_after,
                "actual_target_point": inspection.as_ref().ok().map(|point| &point.0),
            "wrong_public_finalizer_probe": public_finalizer_data.as_ref().ok(),
            "wrong_public_finalizer_probe_error": public_finalizer_data.as_ref().err().map(ToString::to_string),
                "target_error": inspection.as_ref().err().map(ToString::to_string),
                "transport_observations": format!("{:?}", fixture.transport.observations()),
                "qualification": "local native only; no Strong proof manufactured"
            })
        );
        b_result.unwrap();
        assert!(b_elapsed < Duration::from_secs(5));
        let (_, actual_r, actual_local_b, held_global_prefix, full_value) = inspection.unwrap();
        assert_eq!(actual_r, 1);
        assert_eq!(actual_local_b, a_sequence + 1);
        assert!(held_global_prefix < a_sequence);
        assert_eq!(full_value.as_deref(), Some(&b));
        assert_eq!(target_floor_after, target_floor_before);
        let public_finalizer_data = public_finalizer_data
            .expect("actual target typed-local finalizer must reach DATA-only public API probe");
        assert_eq!(public_finalizer_data["public_returned_ok"], false);
        assert_eq!(public_finalizer_data["public_exact_original_unknown"], true);
        assert_eq!(public_finalizer_data["before"]["wait_exhausted"], false);
        assert_eq!(public_finalizer_data["before"]["sequence"], actual_local_b);
        assert_eq!(
            public_finalizer_data["before"]["global_prefix"],
            held_global_prefix
        );
        assert_eq!(
            public_finalizer_data["before"]["incarnation"],
            fixture.databases[&n1].read().incarnation_id()
        );
        assert_eq!(public_finalizer_data["before"]["memory_index_count"], 0);
        assert_eq!(public_finalizer_data["before"]["memory_job_count"], 0);
        assert_eq!(public_finalizer_data["before"]["wal_pin_present"], true);
        assert!(public_finalizer_data["before"]["transaction_id"]
            .as_u64()
            .is_some_and(|transaction| transaction > 0));
        assert_eq!(
            public_finalizer_data["before"],
            public_finalizer_data["after"]
        );
        let actual_apply = fixture.transport.observations();
        assert!(actual_apply.iter().any(|observation| {
            observation.operation == "apply"
                && observation.destination == n1
                && observation.state == Some(RangeWriteState::Applied)
                && observation.command_sha256.is_some()
                && observation.error.is_none()
        }));
        higher.join().unwrap();
        let (observer, result) = lower.join().unwrap();
        assert_eq!(result.unwrap(), a_sequence);
        assert_eq!(
            observer.outcome(),
            TransactionOutcome::Committed {
                sequence: a_sequence
            }
        );
    });
    // After release, actual native delivery converges both INITIAL ranges on
    // all three real services. No direct mark-applied or retry write is used.
    for (node, database) in &fixture.databases {
        let database = database.read();
        database.check_recovery_state().unwrap();
        let a_lineage = database
            .current_native_range_source_lineage(range_a.id)
            .unwrap();
        let b_lineage = database
            .current_native_range_source_lineage(range_b.id)
            .unwrap();
        assert_eq!(a_lineage.range_index(), 1, "{node}");
        assert_eq!(b_lineage.range_index(), 1, "{node}");
        let reader = database.begin_transaction().unwrap();
        assert_eq!(
            reader.get("items", &a.id).unwrap().as_deref(),
            Some(&a),
            "{node}"
        );
        assert_eq!(
            reader.get("items", &b.id).unwrap().as_deref(),
            Some(&b),
            "{node}"
        );
    }
}
