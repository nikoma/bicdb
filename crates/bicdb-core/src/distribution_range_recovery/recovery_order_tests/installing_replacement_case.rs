//! Actual ACK1 + uncertain2 + partial image, strict cold copy, source0 data
//! unavailable, committed E2/leader2, old CQ2 selected/certified without any
//! Installing Apply. Simulated delivery, not mTLS/VM/power-loss qualification.
use super::copied_full_image_case::recipient_role;
use super::*;
use crate::distribution_consensus::MetadataConsensusStore;
use crate::distribution_range_consensus::NativeCurrentImageProposalPointer;
fn catalogue(f: &ActualNativeRf3Fixture, id: &ClusterNodeId) -> serde_json::Value {
    serde_json::from_slice(
        &std::fs::read(
            f.root
                .path()
                .join(id.as_str())
                .join("cluster-range-write-log.json"),
        )
        .unwrap(),
    )
    .unwrap()
}
fn persist_json(root: &std::path::Path, name: &str, x: &serde_json::Value) {
    let mut f = std::fs::File::create(root.join(name)).unwrap();
    f.write_all(&serde_json::to_vec_pretty(x).unwrap()).unwrap();
    f.sync_all().unwrap();
    std::fs::File::open(root).unwrap().sync_all().unwrap();
}
fn commit_actual_higher_epoch(
    root: &std::path::Path,
    old: &crate::ClusterTopology,
    new: &crate::ClusterTopology,
) {
    let ids = old.metadata_voters();
    let leader = node("n2");
    let mut stores = ids
        .iter()
        .map(|id| {
            (
                id.clone(),
                MetadataConsensusStore::open(
                    root.join(id.as_str()),
                    old.cluster_id.clone(),
                    id.clone(),
                    old.clone(),
                    true,
                )
                .unwrap(),
            )
        })
        .collect::<BTreeMap<_, _>>();
    let request = stores.get_mut(&leader).unwrap().start_election().unwrap();
    let mut granted = BTreeSet::from([leader.clone()]);
    for id in ids.iter().filter(|id| **id != leader) {
        let reply = stores
            .get_mut(id)
            .unwrap()
            .handle_vote_request(request.clone())
            .unwrap();
        if reply.vote_granted {
            granted.insert(id.clone());
        }
    }
    stores
        .get_mut(&leader)
        .unwrap()
        .become_leader(&granted)
        .unwrap();
    // Genuine request/response delivery advances each store; no commit-index edit.
    fn replicate(
        stores: &mut BTreeMap<ClusterNodeId, MetadataConsensusStore>,
        leader: &ClusterNodeId,
        ids: &BTreeSet<ClusterNodeId>,
    ) {
        for _ in 0..3 {
            for id in ids.iter().filter(|id| *id != leader) {
                let req = stores[leader].append_request_from(1, 128).unwrap();
                let reply = stores
                    .get_mut(id)
                    .unwrap()
                    .handle_append_request(req)
                    .unwrap();
                stores
                    .get_mut(leader)
                    .unwrap()
                    .record_append_response(id, &reply)
                    .unwrap();
            }
        }
    }
    replicate(&mut stores, &leader, &ids);
    stores
        .get_mut(&leader)
        .unwrap()
        .propose_topology(new.clone())
        .unwrap();
    replicate(&mut stores, &leader, &ids);
    assert!(stores.values().all(|s| s.committed_topology() == new));
    let summary = stores
        .iter()
        .map(|(id, s)| (id.clone(), s.status()))
        .collect::<BTreeMap<_, _>>();
    persist_json(
        root,
        "replacement-metadata-actual-status.json",
        &serde_json::to_value(summary).unwrap(),
    );
}
fn runtimes(
    f: &ActualNativeRf3Fixture,
    p: &BTreeMap<ClusterNodeId, Arc<MetadataReadProvider>>,
) -> BTreeMap<ClusterNodeId, Arc<RangeRecoveryRuntime>> {
    let registry = Arc::new(Mutex::new(BTreeMap::new()));
    let unavailable = Arc::new(Mutex::new(BTreeSet::from([node("n0")])));
    f.transport.set_unavailable(&node("n0"), true);
    f.transport.set_unavailable(&node("n2"), false);
    f.services
        .iter()
        .map(|(id, svc)| {
            let bus = Arc::new(ObservedRecoveryBus {
                inner: Arc::clone(&f.transport),
                caller: p[id].local_participant(),
                receivers: Arc::clone(&registry),
                unavailable: Arc::clone(&unavailable),
            });
            let runtime = Arc::new(
                RangeRecoveryRuntime::new(Arc::clone(svc), Arc::clone(&p[id]))
                    .with_participant_transport(bus),
            );
            registry.lock().insert(id.clone(), Arc::downgrade(&runtime));
            (id.clone(), runtime)
        })
        .collect()
}
#[allow(clippy::too_many_arguments)]
fn selected_decision(
    recipient: &ClusterNodeId,
    p: &BTreeMap<ClusterNodeId, Arc<MetadataReadProvider>>,
    r: &BTreeMap<ClusterNodeId, Arc<RangeRecoveryRuntime>>,
    actor: &MetadataReadParticipant,
    range: &RangeDescriptor,
    scope: &RecoveryFenceScope,
    pointer: &NativeCurrentImageProposalPointer,
    command: &RangeWriteCommand,
    parent: &CancellationToken,
) -> Arc<InstallingCommittedDecision> {
    let role = if recipient == &node("n2") {
        fresh_role(p, actor, range, scope.recovery_nonce)
    } else {
        recipient_role(p, actor, range, scope.recovery_nonce)
    };
    let mut q =
        InstallingCommittedQuorumRound::begin(role, node("n1"), pointer.clone(), parent).unwrap();
    let probe_parent = parent.child();
    q.record_ordinary(
        r[&node("n2")]
            .fence_epoch(
                actor,
                &RecoveryEpochFenceRequest {
                    scope: scope.clone(),
                    budget_ms: 5000,
                },
                &probe_parent,
            )
            .unwrap(),
    )
    .unwrap();
    q.record_installing(
        r[&node("n1")]
            .acquire_installing_committed_fence(
                actor,
                &InstallingCommittedRequest {
                    scope: scope.clone(),
                    proposal: pointer.clone(),
                    challenge: Uuid::now_v7(),
                    budget_ms: 5000,
                },
                &probe_parent,
            )
            .unwrap(),
    )
    .unwrap();
    probe_parent.cancel(); // consumed fsynced refusals retain ORIGINAL round budget
    let authority = Arc::new(q.finish().unwrap());
    let mut poisoned =
        InstallingCommittedDecisionRound::begin(Arc::clone(&authority), command.clone()).unwrap();
    let wrong = r[&node("n2")]
        .observe_history(
            actor,
            &RecoveryFencedProbeRequest {
                scope: scope.clone(),
                index: Some(1),
                budget_ms: 5000,
            },
            parent,
        )
        .unwrap();
    assert!(poisoned.record_history(wrong).is_err());
    let valid = r[&node("n2")]
        .observe_history(
            actor,
            &RecoveryFencedProbeRequest {
                scope: scope.clone(),
                index: Some(command.index),
                budget_ms: 5000,
            },
            parent,
        )
        .unwrap();
    assert!(poisoned.record_history(valid).is_err());
    assert!(poisoned.finish().is_err());
    let mut d = InstallingCommittedDecisionRound::begin(authority, command.clone()).unwrap();
    d.record_history(
        r[&node("n2")]
            .observe_history(
                actor,
                &RecoveryFencedProbeRequest {
                    scope: scope.clone(),
                    index: Some(command.index),
                    budget_ms: 5000,
                },
                parent,
            )
            .unwrap(),
    )
    .unwrap();
    Arc::new(d.finish().unwrap())
}
#[test]
fn actual_installing_existing_decision_replaces_dead_source_under_higher_data_epoch() {
    run_replacement(None);
}
#[test]
fn actual_installing_replacement_native_proposal_orphan_cold_resume() {
    run_replacement(Some(true));
}
#[test]
fn actual_installing_replacement_catalogue_handover_cold_resume() {
    run_replacement(Some(false));
}
fn run_replacement(handover_cut: Option<bool>) {
    let stopped = super::interrupted_normal_repair::exercise_interrupted_normal_image(
        super::interrupted_normal_repair::ImageStop::AfterClear,
        false,
    );
    // The first routine returned and dropped ALL owners; retain the seed unchanged.
    let original_before = std::fs::read(stopped.join("n1/cluster-range-write-log.json")).unwrap();
    let copy = ActualNativeRf3Fixture::copy_stopped_recovery_tree(&stopped).unwrap();
    let mut f = ActualNativeRf3Fixture::reopen_copied_recovery_state(copy).unwrap();
    f.root.disable_cleanup(true);
    eprintln!("REPLACEMENT_SOURCE_COPIED_ROOT {}", f.root.path().display());
    let old = f.topology.read().clone();
    let old_range = old.ranges.values().next().unwrap().clone();
    let target = node("n1");
    let source = node("n2");
    let initial = catalogue(&f, &target);
    let k = old_range.id.get().to_string();
    let pointer: NativeCurrentImageProposalPointer = serde_json::from_value(
        initial["catalog"]["image_install_intents"][&k]["protected_proposal"].clone(),
    )
    .unwrap();
    let old_fence = initial["catalog"]["recovery_fences"][&k].clone();
    let old_intent = initial["catalog"]["image_install_intents"][&k].clone();
    let sparse_sha =
        initial["catalog"]["sparse_committed_decisions"][&k]["2"]["checksum_sha256"].clone();
    let command: RangeWriteCommand = serde_json::from_value(
        catalogue(&f, &source)["catalog"]["ranges"][&k]["entries"]["2"]["command"].clone(),
    )
    .unwrap();
    assert_eq!(command.range_epoch, old_range.epoch);
    assert_eq!(command.index, 2);
    assert_eq!(
        f.databases[&source]
            .read()
            .native_range_prefix_observation(old_range.id)
            .unwrap()
            .native_applied_index,
        Some(1)
    );
    assert_eq!(
        f.databases[&target]
            .read()
            .native_range_prefix_observation(old_range.id)
            .unwrap()
            .native_applied_index,
        None
    );
    let before: serde_json::Value = serde_json::from_slice(
        &std::fs::read(f.root.path().join("interrupted-normal-cold-result.json")).unwrap(),
    )
    .unwrap();
    let target_uid: Uuid = serde_json::from_value(before["target_uid"].clone()).unwrap();
    let expected_h = before["source_logical_history_sha256"]
        .as_str()
        .unwrap()
        .to_owned();
    let expected = serde_json::from_str::<serde_json::Value>(
        &std::fs::read_to_string(f.root.path().join("controller-acknowledged.jsonl")).unwrap(),
    )
    .unwrap();
    let acknowledged = expected["acknowledged_key"].as_str().unwrap().to_owned();
    let acknowledged_sha = expected["record_sha256"].as_str().unwrap().to_owned();
    let mut new = old.clone();
    new.generation += 1;
    // Topology keys are token-span starts, not numeric range identifiers.
    let range = new
        .ranges
        .values_mut()
        .find(|range| range.id == old_range.id)
        .expect("the original range remains in the new topology");
    range.epoch += 1;
    range.leader = source.clone();
    let range = range.clone();
    new.validate().unwrap();
    commit_actual_higher_epoch(&f.root.path().join("recovery-metadata"), &old, &new);
    *f.topology.write() = new.clone();
    let p = test_recovery_metadata_cluster(&f.root.path().join("recovery-metadata"), &new, &source)
        .unwrap();
    let actor = p[&source].local_participant();
    let r = runtimes(&f, &p);
    let parent = CancellationToken::uncancelable();
    let nonce = Uuid::now_v7();
    let role = fresh_role(&p, &actor, &range, nonce);
    let scope = RecoveryFenceScope::from_role(&role).unwrap();
    drop(role);
    // Old generic target admission stays unavailable in the NEW domain.
    let generic = r[&target]
        .fence_epoch(
            &actor,
            &RecoveryEpochFenceRequest {
                scope: scope.clone(),
                budget_ms: 5000,
            },
            &parent,
        )
        .unwrap_err();
    assert!(generic
        .to_string()
        .contains("durable native image installation"));
    let d_target = selected_decision(
        &target, &p, &r, &actor, &range, &scope, &pointer, &command, &parent,
    );
    let uncertain = r[&target]
        .test_certify_installing_existing_committed_with(
            &d_target,
            &parent,
            |path, bytes, fsync| {
                assert!(fsync);
                crate::storage::write_atomic(path, bytes, fsync)?;
                Err(invalid(
                    "injected after ACTUAL durable Installing certification publish",
                ))
            },
        )
        .unwrap_err();
    assert!(uncertain
        .to_string()
        .contains("ACTUAL durable Installing certification"));
    let uncertain_bytes =
        std::fs::read(f.root.path().join("n1/cluster-range-write-log.json")).unwrap();
    assert!(r[&target]
        .prepare_native_ready(
            &actor,
            &NativeReadyRequest {
                scope: scope.clone(),
                challenge: Uuid::now_v7(),
                budget_ms: 5000
            },
            &parent
        )
        .is_err());
    r[&target]
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
        std::fs::read(f.root.path().join("n1/cluster-range-write-log.json")).unwrap(),
        uncertain_bytes
    );
    let cert_parent = parent.child();
    let cert_target = r[&target]
        .certify_installing_existing_committed(&d_target, &cert_parent)
        .unwrap();
    let after_cert = catalogue(&f, &target);
    assert_eq!(after_cert["catalog"]["recovery_fences"][&k], old_fence);
    assert_eq!(
        after_cert["catalog"]["image_install_intents"][&k],
        old_intent
    );
    assert_eq!(after_cert["catalog"]["ranges"][&k]["last_index"], 0);
    assert_eq!(
        after_cert["catalog"]["sparse_committed_decisions"][&k]["2"]["checksum_sha256"],
        sparse_sha
    );
    assert_eq!(
        f.databases[&target]
            .read()
            .native_range_prefix_observation(range.id)
            .unwrap()
            .native_applied_index,
        None
    );
    let d_source = selected_decision(
        &source, &p, &r, &actor, &range, &scope, &pointer, &command, &parent,
    );
    // The ordinary source has never needed sparse history. Its first current
    // certification must upgrade the reader fence before validating/publishing
    // the newly present sparse body; the target was already sparse-capable.
    let source_before_cert = catalogue(&f, &source);
    assert!(source_before_cert["catalog"]["sparse_committed_decisions"].is_null());
    let cert_source = r[&source]
        .certify_installing_existing_committed(&d_source, &cert_parent)
        .unwrap();
    let source_after_cert = catalogue(&f, &source);
    assert!(
        source_after_cert["catalog"]["format_version"]
            .as_u64()
            .unwrap()
            > source_before_cert["catalog"]["format_version"]
                .as_u64()
                .unwrap()
    );
    assert_eq!(
        source_after_cert["catalog"]["sparse_committed_decisions"][&k]["2"]["command"]
            ["checksum_sha256"],
        command.checksum_sha256
    );
    assert_eq!(
        f.databases[&source]
            .read()
            .native_range_prefix_observation(range.id)
            .unwrap()
            .native_applied_index,
        Some(1),
        "durable certification must not apply the uncertain command"
    );
    let mut current = InstallingCurrentCertificationRound::begin(d_source).unwrap();
    current.record(cert_target).unwrap();
    current.record(cert_source).unwrap();
    cert_parent.cancel(); // accepted durable markers retain ORIGINAL round TTL

    let applied = r[&source]
        .apply_installing_existing_committed(current.finish().unwrap(), &parent)
        .unwrap();
    drop(applied);
    assert_eq!(
        f.databases[&source]
            .read()
            .native_range_prefix_observation(range.id)
            .unwrap()
            .native_applied_index,
        Some(2)
    );
    assert_eq!(
        f.databases[&source]
            .read()
            .current_native_range_source_lineage(range.id)
            .unwrap()
            .logical_history_sha256(),
        expected_h
    );
    assert_eq!(
        f.databases[&target]
            .read()
            .native_range_prefix_observation(range.id)
            .unwrap()
            .native_applied_index,
        None
    );
    let final_result = super::replacement_source_positive::finish_actual_current_image_replacement(
        &f,
        &p,
        &r,
        &actor,
        &range,
        &scope,
        pointer.clone(),
        &command,
        &acknowledged,
        &acknowledged_sha,
        target_uid,
        &expected_h,
        &parent,
        handover_cut,
    );
    if let Some(before_catalogue) = handover_cut {
        let weak = f.databases.values().map(Arc::downgrade).collect::<Vec<_>>();
        let root = f.root.path().to_path_buf();
        let second_pointer: NativeCurrentImageProposalPointer =
            serde_json::from_value(final_result["p2"].clone()).unwrap();
        let frozen_catalogue = std::fs::read(root.join("n1/cluster-range-write-log.json")).unwrap();
        wait_for_contexts(&r);
        drop(r);
        drop(p);
        drop(f);
        let deadline = Instant::now() + Duration::from_secs(5);
        while weak.iter().any(|w| w.upgrade().is_some()) && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(1));
        }
        assert!(
            weak.iter().all(|w| w.upgrade().is_none()),
            "all actual replacement owners must close"
        );
        let copied = ActualNativeRf3Fixture::copy_stopped_recovery_tree(&root).unwrap();
        let mut cold = ActualNativeRf3Fixture::reopen_copied_recovery_state(copied).unwrap();
        cold.root.disable_cleanup(true);
        eprintln!(
            "REPLACEMENT_HANDOVER_COLD_ROOT {}",
            cold.root.path().display()
        );
        assert_eq!(
            cold.topology.read().range_by_id(range.id).unwrap().epoch,
            range.epoch
        );
        let p = test_recovery_metadata_cluster(
            &cold.root.path().join("recovery-metadata"),
            &new,
            &source,
        )
        .unwrap();
        let actor = p[&source].local_participant();
        let r = runtimes(&cold, &p);
        let current = catalogue(&cold, &target);
        let active: NativeCurrentImageProposalPointer = serde_json::from_value(
            current["catalog"]["image_install_intents"][&k]["protected_proposal"].clone(),
        )
        .unwrap();
        let result = if before_catalogue {
            assert_eq!(active, pointer);
            super::replacement_source_positive::finish_actual_current_image_replacement(
                &cold,
                &p,
                &r,
                &actor,
                &range,
                &scope,
                pointer.clone(),
                &command,
                &acknowledged,
                &acknowledged_sha,
                target_uid,
                &expected_h,
                &parent,
                None,
            )
        } else {
            assert_eq!(active, second_pointer);
            assert_ne!(active, pointer);
            // Genuine old-session request is refused by CURRENT MQ/intent;
            // historical P1 is never promoted back into a current admission.
            let old_scope: RecoveryFenceScope =
                serde_json::from_value(old_fence["scope"].clone()).unwrap();
            assert!(r[&target]
                .acquire_current_image_installing_fence(
                    &actor,
                    &CurrentImageInstallingRequest {
                        scope: old_scope,
                        proposal: pointer.clone(),
                        challenge: Uuid::now_v7(),
                        budget_ms: 5000
                    },
                    &parent
                )
                .is_err());
            super::protected_resume_positive::finish_actual_current_image_resume_at_source(
                source.clone(),
                &cold,
                &p,
                &r,
                &actor,
                &range,
                &scope,
                active,
                &command,
                &acknowledged,
                &acknowledged_sha,
                target_uid,
                &expected_h,
                &parent,
            )
        };
        assert_eq!(
            std::fs::read(root.join("n1/cluster-range-write-log.json")).unwrap(),
            frozen_catalogue
        );
        let report = serde_json::json!({"classification":"ACTUAL_REPLACEMENT_HANDOVER_COLD_RESUME","cut":final_result,"cold_result":result,"seed":stopped,"first_copy":root,"final_copy":cold.root.path(),"orderly_all_owner_close":true,"physical_qualification":false,"born2_writer_policy_exercised":false,"activation_proven":false});
        persist_json(
            cold.root.path(),
            "replacement-handover-cold-result.json",
            &report,
        );
        eprintln!("REPLACEMENT_HANDOVER_COLD_RESULT {report}");
        return;
    }
    assert_eq!(
        std::fs::read(stopped.join("n1/cluster-range-write-log.json")).unwrap(),
        original_before
    );
    let final_cat = catalogue(&f, &target);
    let archives = final_cat["catalog"]["installing_recovery_handovers"][&k]
        .as_object()
        .unwrap();
    assert_eq!(archives.len(), 1);
    let handover = archives.values().next().unwrap();
    assert_eq!(handover["old_fence"], old_fence);
    assert_eq!(handover["old_intent"], old_intent);
    assert_eq!(handover["new_scope"]["range_epoch"], range.epoch);
    let result = serde_json::json!({"classification":"ACTUAL_REPLACEMENT_SOURCE_EXISTING_COMMITTED_TO_NATIVE_PREPARED_READY","seed":stopped,"root":f.root.path(),"old_epoch":old_range.epoch,"new_epoch":range.epoch,"new_leader":source,"original_command_epoch":command.range_epoch,"original_command_sha256":command.checksum_sha256,"unknown2_is_new_ack":false,"target_certificate_did_not_advance_raw_or_native":true,"old_fence_and_intent_archived":true,"old_sparse_sha_preserved":sparse_sha,"final":final_result,"physical_qualification":false,"born2_writer_policy_exercised":false,"strong_get_tested":false});
    persist_json(f.root.path(), "replacement-source-result.json", &result);
    eprintln!("REPLACEMENT_SOURCE_RESULT {result}");
}
