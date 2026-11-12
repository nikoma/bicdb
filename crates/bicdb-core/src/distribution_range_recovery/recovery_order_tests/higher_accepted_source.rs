//! P1R1 survives orderly all-owner loss; a genuine recovered ordinary 0+2
//! majority then ACKs command2 before source0 data delivery is lost. P2R2 must
//! cover P1H1. Local delivery schedule, not mTLS/host/power-loss qualification.
use super::copied_full_image_case::recipient_role;
use super::*;
use crate::distribution_range_consensus::{
    ImageInstallPurpose, NativeCurrentImageProposalPointer, NativeImageInstallRole,
};
use crate::distribution_range_image::RangeImageCapture;
use crate::distribution_range_image_install::{
    materialize_actual_current_repair_image, stage_and_clear_actual_current_repair_image,
    NativeImageInstallFreeze,
};
use std::path::{Path, PathBuf};

fn sha(value: &impl serde::Serialize) -> String {
    hex::encode(Sha256::digest(serde_json::to_vec(value).unwrap()))
}
fn save(root: &Path, name: &str, value: &serde_json::Value) {
    let mut file = std::fs::File::create(root.join(name)).unwrap();
    file.write_all(&serde_json::to_vec_pretty(value).unwrap())
        .unwrap();
    file.sync_all().unwrap();
    std::fs::File::open(root).unwrap().sync_all().unwrap();
}
fn runtimes(
    f: &ActualNativeRf3Fixture,
    providers: &BTreeMap<ClusterNodeId, Arc<MetadataReadProvider>>,
    offline: ClusterNodeId,
) -> BTreeMap<ClusterNodeId, Arc<RangeRecoveryRuntime>> {
    let registry = Arc::new(Mutex::new(BTreeMap::new()));
    let unavailable = Arc::new(Mutex::new(BTreeSet::from([offline.clone()])));
    f.transport.enable_native_delivery();
    f.transport.set_unavailable(&offline, true);
    f.services
        .iter()
        .map(|(id, service)| {
            let bus = Arc::new(ObservedRecoveryBus {
                inner: Arc::clone(&f.transport),
                caller: providers[id].local_participant(),
                receivers: Arc::clone(&registry),
                unavailable: Arc::clone(&unavailable),
            });
            let runtime = Arc::new(
                RangeRecoveryRuntime::new(Arc::clone(service), Arc::clone(&providers[id]))
                    .with_participant_transport(bus),
            );
            registry.lock().insert(id.clone(), Arc::downgrade(&runtime));
            (id.clone(), runtime)
        })
        .collect()
}
fn closed(weak: &[Weak<parking_lot::RwLock<BicDb>>]) {
    let deadline = Instant::now() + Duration::from_secs(5);
    while weak.iter().any(|w| w.upgrade().is_some()) && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(1));
    }
    assert!(
        weak.iter().all(|w| w.upgrade().is_none()),
        "all original DB/open/capture owners must end before the copy"
    );
}

fn seed_p1_at_r1() -> PathBuf {
    let mut f = ActualNativeRf3Fixture::new();
    f.root.disable_cleanup(true);
    f.transport.enable_native_delivery();
    f.transport.set_unavailable(&node("n1"), true);
    f.append("higher-cut-ack-1").unwrap();
    let range = f.topology.read().ranges.values().next().unwrap().clone();
    let topology = f.topology.read().clone();
    let record = f.databases[&node("n0")]
        .read()
        .get("items", "higher-cut-ack-1")
        .unwrap()
        .unwrap();
    let digest = sha(&record);
    let h1 = f.databases[&node("n0")]
        .read()
        .current_native_range_source_lineage(range.id)
        .unwrap()
        .logical_history_sha256()
        .to_owned();
    assert_eq!(
        f.databases[&node("n2")]
            .read()
            .native_range_prefix_observation(range.id)
            .unwrap()
            .native_applied_index,
        Some(1)
    );
    assert_eq!(
        sha(&f.databases[&node("n2")]
            .read()
            .get("items", "higher-cut-ack-1")
            .unwrap()
            .unwrap()),
        digest
    );
    let mut ledger =
        std::fs::File::create(f.root.path().join("higher-cut-acknowledged.jsonl")).unwrap();
    writeln!(
        ledger,
        "{}",
        serde_json::json!({"key":"higher-cut-ack-1","record_sha256":digest})
    )
    .unwrap();
    ledger.sync_all().unwrap();
    drop(ledger);
    f.transport.set_unavailable(&node("n1"), false);
    let p = test_recovery_metadata_cluster(
        &f.root.path().join("recovery-metadata"),
        &topology,
        &node("n2"),
    )
    .unwrap();
    let actor = p[&node("n0")].local_participant();
    let r = runtimes(&f, &p, node("n2"));
    let parent = CancellationToken::uncancelable();
    let role = fresh_role(&p, &actor, &range, Uuid::now_v7());
    let scope = RecoveryFenceScope::from_role(&role).unwrap();
    drop(role);
    for id in [node("n0"), node("n1")] {
        r[&id]
            .fence_epoch(
                &actor,
                &RecoveryEpochFenceRequest {
                    scope: scope.clone(),
                    budget_ms: 5000,
                },
                &parent,
            )
            .unwrap();
    }
    let mut round = RecoveryFenceQuorumRound::begin(
        fresh_role(&p, &actor, &range, scope.recovery_nonce),
        &parent,
    )
    .unwrap();
    for id in [node("n0"), node("n1")] {
        round
            .record(
                r[&id]
                    .fence_epoch(
                        &actor,
                        &RecoveryEpochFenceRequest {
                            scope: scope.clone(),
                            budget_ms: 5000,
                        },
                        &parent,
                    )
                    .unwrap(),
            )
            .unwrap();
    }
    let authority = Arc::new(round.finish().unwrap());
    let frozen = r[&node("n0")].source_capture_freeze(&scope).unwrap();
    let ready = f.services[&node("n0")]
        .lock()
        .attest_native_recovery_ready(
            fresh_role(&p, &actor, &range, scope.recovery_nonce),
            Arc::clone(&frozen),
            &parent,
        )
        .unwrap();
    assert_eq!(ready.range_index(), 1);
    let mut history = f.services[&node("n0")]
        .lock()
        .begin_native_recovery_source_history(authority, &ready, node("n1"))
        .unwrap();
    assert!(f.services[&node("n0")]
        .lock()
        .advance_native_recovery_source_history(&mut history, &ready, &parent)
        .unwrap());
    let cut = f.services[&node("n0")]
        .lock()
        .finish_native_recovery_source_history(history, &ready)
        .unwrap();
    let lineage = f.databases[&node("n0")]
        .read()
        .current_native_range_source_lineage(range.id)
        .unwrap();
    drop(ready);
    let mut capture = Some(
        RangeImageCapture::begin(
            Arc::clone(&f.databases[&node("n0")]),
            cut,
            frozen,
            lineage,
            Duration::from_secs(30),
            &parent,
        )
        .unwrap(),
    );
    let attempt = Uuid::now_v7();
    let target_role = recipient_role(&p, &actor, &range, scope.recovery_nonce);
    let source = capture
        .as_ref()
        .unwrap()
        .authorize_target_install()
        .unwrap();
    let lease = r[&node("n1")]
        .transfer_image_install(
            &scope,
            &source,
            &NativeImageInstallRole::CurrentVoterRepair(&target_role),
            ImageInstallPurpose::CurrentVoterRepair,
            attempt,
            &parent,
        )
        .unwrap();
    let mut install = f.services[&node("n1")]
        .lock()
        .begin_native_range_image_install(
            &source,
            &NativeImageInstallRole::CurrentVoterRepair(&target_role),
            lease,
            ImageInstallPurpose::CurrentVoterRepair,
            attempt,
            &parent,
        )
        .unwrap();
    drop(source);
    drop(target_role);
    stage_and_clear_actual_current_repair_image(
        &mut capture,
        &f.services[&node("n1")],
        &|| recipient_role(&p, &actor, &range, scope.recovery_nonce),
        || {},
        &mut install,
    );
    // Copy the ACTUAL P1 row using its typed image grant, then stop before
    // verification/native publication. No diagnostic/raw import changes B.
    let mut copied_rows = false;
    for _ in 0..4 {
        let step = {
            let source = capture.as_mut().unwrap();
            let cursor = source.cursor().clone();
            source
                .export_step(
                    &cursor,
                    &crate::distribution::RangeSnapshotOptions {
                        max_records_per_batch: 1024,
                        max_bytes_per_batch: 1024 * 1024,
                        max_record_bytes: 1024 * 1024,
                    },
                    &parent,
                )
                .unwrap()
        };
        let role = recipient_role(&p, &actor, &range, scope.recovery_nonce);
        let source = capture
            .as_ref()
            .unwrap()
            .authorize_target_install()
            .unwrap();
        f.services[&node("n1")]
            .lock()
            .copy_native_range_image_step(
                &mut install,
                &NativeImageInstallRole::CurrentVoterRepair(&role),
                &source,
                attempt,
                &step,
                &parent,
            )
            .unwrap();
        if step.complete {
            copied_rows = true;
            break;
        }
    }
    assert!(
        copied_rows,
        "one genuine P1 row must be imported before the source owners end"
    );
    assert_eq!(
        sha(&f.databases[&node("n1")]
            .read()
            .get("items", "higher-cut-ack-1")
            .unwrap()
            .unwrap()),
        digest
    );
    let db = f.databases[&node("n1")].read();
    let native = db
        .native_recovery_admission(
            &topology.cluster_id,
            topology.range_image_bootstrap_id.unwrap(),
            &node("n1"),
            topology.nodes[&node("n1")].incarnation,
        )
        .unwrap();
    let protected = db
        .read_native_current_image_proposal_by_id(&native, attempt)
        .unwrap()
        .unwrap();
    assert_eq!(protected.historical_source().source_range_index, 1);
    assert_eq!(protected.historical_source().logical_history_sha256, h1);
    let pointer = protected.pointer();
    let stage = db
        .native_history_stage_state_commitment(range.id)
        .unwrap()
        .unwrap();
    let uid = db.local_namespace_identity("items").unwrap().unwrap();
    assert_eq!(
        db.native_range_prefix_observation(range.id)
            .unwrap()
            .native_applied_index,
        None
    );
    let old_source_open = f.databases[&node("n0")]
        .read()
        .native_recovery_admission(
            &topology.cluster_id,
            topology.range_image_bootstrap_id.unwrap(),
            &node("n0"),
            topology.nodes[&node("n0")].incarnation,
        )
        .unwrap()
        .database_incarnation()
        .to_owned();
    save(
        f.root.path(),
        "higher-cut-seed.json",
        &serde_json::json!({"proposal":pointer,"h1":h1,"ack1_sha256":digest,"stage_sha256":stage,"target_uid":uid,"old_source_open":old_source_open,"scope":scope,"partial_target_data_b":db.current_commit_seq()}),
    );
    drop(protected);
    drop(native);
    drop(db);
    let weak = f.databases.values().map(Arc::downgrade).collect::<Vec<_>>();
    let root = f.root.path().to_path_buf();
    eprintln!("HIGHER_ACCEPTED_P1_STOPPED_ROOT {}", root.display());
    drop(install);
    drop(capture);
    wait_for_contexts(&r);
    drop(r);
    drop(p);
    drop(f);
    closed(&weak);
    root
}

/// A cold open does NOT grant writes: persistent Active is closed until actual
/// fresh 0+2 Prepared/Completed QQ, protected activation and arm release LAST.
fn recover_majority_and_ack2(stopped: &Path) -> PathBuf {
    let copied = ActualNativeRf3Fixture::copy_stopped_recovery_tree(stopped).unwrap();
    let mut f = ActualNativeRf3Fixture::reopen_copied_recovery_state(copied).unwrap();
    f.root.disable_cleanup(true);
    let seed: serde_json::Value =
        serde_json::from_slice(&std::fs::read(f.root.path().join("higher-cut-seed.json")).unwrap())
            .unwrap();
    let topology = f.topology.read().clone();
    let range = topology.ranges.values().next().unwrap().clone();
    assert_ne!(
        f.databases[&node("n0")]
            .read()
            .native_recovery_admission(
                &topology.cluster_id,
                topology.range_image_bootstrap_id.unwrap(),
                &node("n0"),
                topology.nodes[&node("n0")].incarnation
            )
            .unwrap()
            .database_incarnation(),
        seed["old_source_open"].as_str().unwrap()
    );
    let p = test_recovery_metadata_cluster(
        &f.root.path().join("recovery-metadata"),
        &topology,
        &node("n2"),
    )
    .unwrap();
    let actor = p[&node("n0")].local_participant();
    let r = runtimes(&f, &p, node("n1"));
    let parent = CancellationToken::uncancelable();
    let role = fresh_role(&p, &actor, &range, Uuid::now_v7());
    let scope = RecoveryFenceScope::from_role(&role).unwrap();
    drop(role);
    assert_ne!(
        scope.recovery_nonce.to_string(),
        seed["scope"]["recovery_nonce"].as_str().unwrap(),
        "fresh recovery after old owners end must not revive P1's old permission"
    );
    let target_catalogue_before =
        std::fs::read(f.root.path().join("n1/cluster-range-write-log.json")).unwrap();
    for id in [node("n0"), node("n2")] {
        r[&id]
            .fence_epoch(
                &actor,
                &RecoveryEpochFenceRequest {
                    scope: scope.clone(),
                    budget_ms: 5000,
                },
                &parent,
            )
            .unwrap();
        let prepared = r[&id]
            .prepare_native_ready(
                &actor,
                &NativeReadyRequest {
                    scope: scope.clone(),
                    challenge: Uuid::now_v7(),
                    budget_ms: 5000,
                },
                &parent,
            )
            .unwrap();
        assert_eq!(prepared.reply().range_index, 1);
        assert_eq!(
            prepared.reply().logical_history_sha256,
            seed["h1"].as_str().unwrap()
        );
    }
    for id in [node("n0"), node("n2")] {
        let completed = r[&id]
            .complete_native_recovery(
                &actor,
                &NativeCompletionRequest {
                    scope: scope.clone(),
                    challenge: Uuid::now_v7(),
                    budget_ms: 5000,
                },
                &parent,
            )
            .unwrap();
        assert_eq!(
            completed.reply().stage,
            NativeCompletionStage::CompletedFrozen
        );
        assert_eq!(completed.reply().range_index, 1);
    }
    wait_for_contexts(&r);
    for id in [node("n0"), node("n2")] {
        let limit = Instant::now() + Duration::from_secs(5);
        loop {
            let result = r[&id].activate_native_recovery(
                &actor,
                &NativeCompletionRequest {
                    scope: scope.clone(),
                    challenge: Uuid::now_v7(),
                    budget_ms: 5000,
                },
                &parent,
            );
            match result {
                Ok(actual) => {
                    assert_eq!(actual.reply().stage, NativeCompletionStage::ActivatedAnchor);
                    break;
                }
                Err(error)
                    if error.to_string().contains("owners drain") && Instant::now() < limit =>
                {
                    eprintln!("HIGHER_CUT_ACTUAL_OWNER_DRAIN {error}");
                    wait_for_contexts(&r);
                }
                Err(error) => panic!("genuine ordinary majority activation failed: {error}"),
            }
        }
    }
    assert_eq!(
        f.databases[&node("n1")]
            .read()
            .native_history_stage_state_commitment(range.id)
            .unwrap()
            .unwrap(),
        seed["stage_sha256"].as_str().unwrap()
    );
    f.coordinator.recover_local_leadership(8).unwrap();
    f.databases[&node("n0")]
        .read()
        .install_commit_admission(f.coordinator.clone());
    f.append("higher-cut-ack-2").unwrap();
    let mut ledger = std::fs::OpenOptions::new()
        .append(true)
        .open(f.root.path().join("higher-cut-acknowledged.jsonl"))
        .unwrap();
    let row = f.databases[&node("n0")]
        .read()
        .get("items", "higher-cut-ack-2")
        .unwrap()
        .unwrap();
    let digest = sha(&row);
    writeln!(
        ledger,
        "{}",
        serde_json::json!({"key":"higher-cut-ack-2","record_sha256":digest})
    )
    .unwrap();
    ledger.sync_all().unwrap();
    drop(ledger);
    drop(row);
    for id in [node("n0"), node("n2")] {
        assert_eq!(
            f.databases[&id]
                .read()
                .native_range_prefix_observation(range.id)
                .unwrap()
                .native_applied_index,
            Some(2)
        );
        assert_eq!(
            sha(&f.databases[&id]
                .read()
                .get("items", "higher-cut-ack-2")
                .unwrap()
                .unwrap()),
            digest
        );
    }
    assert_eq!(
        f.databases[&node("n1")]
            .read()
            .native_history_stage_state_commitment(range.id)
            .unwrap()
            .unwrap(),
        seed["stage_sha256"].as_str().unwrap(),
        "legitimate ACK2 must not alter the offline old P1 stage"
    );
    assert_eq!(
        f.services[&node("n1")]
            .lock()
            .range_write_progress(range.id)
            .last_index,
        0,
        "offline Installing target must not receive a fabricated command2"
    );
    assert_eq!(std::fs::read(f.root.path().join("n1/cluster-range-write-log.json")).unwrap(),target_catalogue_before,
        "fresh ordinary 0+2 recovery/ACK2 cannot rewrite or clear the offline target's old P1 refusal");
    let h2 = f.databases[&node("n2")]
        .read()
        .current_native_range_source_lineage(range.id)
        .unwrap()
        .logical_history_sha256()
        .to_owned();
    assert_ne!(h2, seed["h1"].as_str().unwrap());
    save(
        f.root.path(),
        "higher-cut-grown.json",
        &serde_json::json!({"h2":h2,"ack2_sha256":digest,"fresh_scope":scope,"ordinary_activated_voters":["n0","n2"]}),
    );
    let weak = f.databases.values().map(Arc::downgrade).collect::<Vec<_>>();
    let root = f.root.path().to_path_buf();
    eprintln!("HIGHER_ACCEPTED_R2_STOPPED_ROOT {}", root.display());
    wait_for_contexts(&r);
    drop(r);
    drop(p);
    drop(f);
    closed(&weak);
    root
}

#[test]
fn actual_installing_replacement_covers_higher_accepted_cut_after_cold_majority_ack2() {
    let original = seed_p1_at_r1();
    let grown = recover_majority_and_ack2(&original);
    let copied = ActualNativeRf3Fixture::copy_stopped_recovery_tree(&grown).unwrap();
    let mut f = ActualNativeRf3Fixture::reopen_copied_recovery_state(copied).unwrap();
    f.root.disable_cleanup(true);
    eprintln!(
        "HIGHER_ACCEPTED_P2_REPLACEMENT_ROOT {}",
        f.root.path().display()
    );
    let seed: serde_json::Value =
        serde_json::from_slice(&std::fs::read(f.root.path().join("higher-cut-seed.json")).unwrap())
            .unwrap();
    let grown: serde_json::Value = serde_json::from_slice(
        &std::fs::read(f.root.path().join("higher-cut-grown.json")).unwrap(),
    )
    .unwrap();
    let pointer: NativeCurrentImageProposalPointer =
        serde_json::from_value(seed["proposal"].clone()).unwrap();
    let old = f.topology.read().clone();
    let mut new = old.clone();
    new.generation += 1;
    let range = new.ranges.values_mut().next().unwrap();
    range.epoch += 1;
    range.leader = node("n2");
    let range = range.clone();
    super::installing_replacement_case::commit_actual_higher_epoch(
        &f.root.path().join("recovery-metadata"),
        &old,
        &new,
    );
    *f.topology.write() = new.clone();
    let p =
        test_recovery_metadata_cluster(&f.root.path().join("recovery-metadata"), &new, &node("n2"))
            .unwrap();
    let actor = p[&node("n2")].local_participant();
    let r = runtimes(&f, &p, node("n0"));
    let parent = CancellationToken::uncancelable();
    let role = fresh_role(&p, &actor, &range, Uuid::now_v7());
    let scope = RecoveryFenceScope::from_role(&role).unwrap();
    drop(role);
    let source_id = node("n2");
    let target_id = node("n1");
    assert_eq!(
        f.databases[&target_id]
            .read()
            .native_history_stage_state_commitment(range.id)
            .unwrap()
            .unwrap(),
        seed["stage_sha256"].as_str().unwrap()
    );
    let mut round = InstallingCommittedQuorumRound::begin(
        fresh_role(&p, &actor, &range, scope.recovery_nonce),
        target_id.clone(),
        pointer.clone(),
        &parent,
    )
    .unwrap();
    round
        .record_ordinary(
            r[&source_id]
                .fence_epoch(
                    &actor,
                    &RecoveryEpochFenceRequest {
                        scope: scope.clone(),
                        budget_ms: 5000,
                    },
                    &parent,
                )
                .unwrap(),
        )
        .unwrap();
    let installing = r[&target_id]
        .acquire_installing_committed_fence(
            &actor,
            &InstallingCommittedRequest {
                scope: scope.clone(),
                proposal: pointer.clone(),
                challenge: Uuid::now_v7(),
                budget_ms: 5000,
            },
            &parent,
        )
        .unwrap();
    assert_eq!(
        installing.evidence().historical_source.source_range_index,
        1
    );
    assert_eq!(
        installing
            .evidence()
            .historical_source
            .logical_history_sha256,
        seed["h1"].as_str().unwrap()
    );
    assert_eq!(installing.evidence().native.native_applied_index, None);
    round.record_installing(installing).unwrap();
    let authority = Arc::new(round.finish().unwrap());
    let frozen = r[&source_id].source_capture_freeze(&scope).unwrap();
    let ready = f.services[&source_id]
        .lock()
        .attest_native_recovery_ready(
            fresh_role(&p, &actor, &range, scope.recovery_nonce),
            Arc::clone(&frozen),
            &parent,
        )
        .unwrap();
    assert_eq!(ready.range_index(), 2);
    assert_eq!(
        ready.logical_history_sha256(),
        grown["h2"].as_str().unwrap()
    );
    // Native ACK2 ALREADY exists. Do not abuse the old-cut certification lane,
    // which correctly refuses command2 > historical P1R1. Only the FULL live
    // replacement verifier covers old H1 and every fresh ordinary obligation.
    let mut history = f.services[&source_id]
        .lock()
        .begin_current_image_replacement_history(authority, &ready)
        .unwrap();
    assert!(f.services[&source_id]
        .lock()
        .advance_current_image_replacement_history(&mut history, &ready, &parent)
        .unwrap());
    let cut = f.services[&source_id]
        .lock()
        .finish_current_image_replacement_history(history, &ready)
        .unwrap();
    let lineage = f.databases[&source_id]
        .read()
        .current_native_range_source_lineage(range.id)
        .unwrap();
    drop(ready);
    let mut capture = Some(
        RangeImageCapture::begin(
            Arc::clone(&f.databases[&source_id]),
            cut,
            frozen,
            lineage,
            Duration::from_secs(30),
            &parent,
        )
        .unwrap(),
    );
    let source = capture
        .as_ref()
        .unwrap()
        .authorize_target_install()
        .unwrap();
    source
        .require_current_quorum_history_points(&BTreeMap::from([(
            1,
            seed["h1"].as_str().unwrap().to_owned(),
        )]))
        .unwrap();
    let wrong_h1 = if seed["h1"].as_str().unwrap().bytes().all(|b| b == b'0') {
        "1".repeat(64)
    } else {
        "0".repeat(64)
    };
    assert!(
        source
            .require_current_quorum_history_points(&BTreeMap::from([(1, wrong_h1)]))
            .is_err(),
        "a higher accepted cut cannot skip or fork the actual old P1 H1 membership"
    );

    let attempt = Uuid::now_v7();
    let role = recipient_role(&p, &actor, &range, scope.recovery_nonce);
    let lease = r[&target_id]
        .transfer_current_image_replacement(
            &scope,
            &source,
            &NativeImageInstallRole::CurrentVoterRepair(&role),
            attempt,
            &parent,
        )
        .unwrap();
    let install = f.services[&target_id]
        .lock()
        .begin_native_range_image_install(
            &source,
            &NativeImageInstallRole::CurrentVoterRepair(&role),
            lease,
            ImageInstallPurpose::CurrentVoterRepair,
            attempt,
            &parent,
        )
        .unwrap();
    let archived = f.databases[&target_id]
        .read()
        .test_native_history_substitution_archive(range.id)
        .unwrap();
    assert_eq!(archived["previous_header_sha256"], seed["stage_sha256"]);
    assert_eq!(archived["previous_source_range_index"], 1);
    assert_eq!(archived["current_source_range_index"], 2);
    assert_eq!(archived["previous_history_sha256"], seed["h1"]);
    assert_eq!(archived["current_history_sha256"], grown["h2"]);
    drop(source);
    drop(role);
    let verified = materialize_actual_current_repair_image(
        &mut capture,
        &f.services[&target_id],
        || recipient_role(&p, &actor, &range, scope.recovery_nonce),
        || {},
        install,
    );
    assert_eq!(verified.base().range_index(), 2);
    assert_eq!(
        verified.base().logical_history_sha256(),
        grown["h2"].as_str().unwrap()
    );
    let target_b = verified.base().target_snapshot_bound().get();
    assert_ne!(
        target_b, 2,
        "actual target-local B must never be cast from rangeR2"
    );
    assert!(
        target_b > seed["partial_target_data_b"].as_u64().unwrap(),
        "the new delete/import must use new ACTUAL target transactions"
    );
    let role = recipient_role(&p, &actor, &range, scope.recovery_nonce);
    let adopted = f.services[&target_id]
        .lock()
        .adopt_native_range_image_catalogue(
            verified,
            &NativeImageInstallRole::CurrentVoterRepair(&role),
            &parent,
        )
        .unwrap();
    let role = recipient_role(&p, &actor, &range, scope.recovery_nonce);
    let (freeze, receipt) = f.services[&target_id]
        .lock()
        .finalize_native_range_image_catalogue(
            adopted,
            &NativeImageInstallRole::CurrentVoterRepair(&role),
            &parent,
        )
        .unwrap();
    let NativeImageInstallFreeze::Recovery(lease) = freeze else {
        panic!("real target Runtime must retain image freeze")
    };
    let role = recipient_role(&p, &actor, &range, scope.recovery_nonce);
    let image = lease
        .return_after_catalogue_adoption(
            receipt,
            &NativeImageInstallRole::CurrentVoterRepair(&role),
            &parent,
        )
        .unwrap();
    let target_frozen = r[&target_id].source_capture_freeze(&scope).unwrap();
    let prepared = f.services[&target_id]
        .lock()
        .prepare_finalized_native_recovery_image(
            image,
            recipient_role(&p, &actor, &range, scope.recovery_nonce),
            Arc::clone(&target_frozen),
            &parent,
        )
        .unwrap();
    assert_eq!(prepared.range_index(), 2);
    assert_eq!(prepared.local_visibility_sequence(), target_b);
    assert_eq!(
        f.databases[&target_id]
            .read()
            .local_namespace_identity("items")
            .unwrap()
            .unwrap()
            .to_string(),
        seed["target_uid"].as_str().unwrap()
    );
    let ledger =
        std::fs::read_to_string(f.root.path().join("higher-cut-acknowledged.jsonl")).unwrap();
    let mut checked = 0;
    for line in ledger.lines() {
        let acknowledged: serde_json::Value = serde_json::from_str(line).unwrap();
        let key = acknowledged["key"].as_str().unwrap();
        let actual = f.databases[&target_id]
            .read()
            .get("items", key)
            .unwrap()
            .unwrap();
        assert_eq!(
            sha(&actual),
            acknowledged["record_sha256"].as_str().unwrap()
        );
        checked += 1;
    }
    assert_eq!(
        checked, 2,
        "both genuine pre- and post-close application ACKs must reconcile"
    );
    drop(prepared);
    drop(target_frozen);
    let ready = r[&target_id]
        .prepare_native_ready(
            &actor,
            &NativeReadyRequest {
                scope: scope.clone(),
                challenge: Uuid::now_v7(),
                budget_ms: 5000,
            },
            &parent,
        )
        .unwrap();
    assert_eq!(ready.reply().range_index, 2);
    assert_eq!(ready.reply().local_visibility_sequence, target_b);
    assert!(f.databases[&target_id]
        .read()
        .range_mutation_gate()
        .try_enter(range.id)
        .is_err());
    save(
        f.root.path(),
        "higher-accepted-cut-result.json",
        &serde_json::json!({"accepted_p1_range_index":1,"new_p2_range_index":2,"old_history_sha256":seed["h1"],"new_history_sha256":grown["h2"],"preserved_old_stage":archived,"actual_target_local_visibility":target_b,"acknowledged_values_reconciled":checked,"original_source_capture_owners_ended":true,"new_ack2_after_actual_fresh_ordinary_activation":true,"target_still_frozen":true,"operator_or_physical_qualification":false,"new_target_activation_or_strong_read_proven":false}),
    );
}
