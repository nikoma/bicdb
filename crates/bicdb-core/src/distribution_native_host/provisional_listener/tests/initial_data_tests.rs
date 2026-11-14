//! Actual INITIAL factories, real metadata votes and pinned TLS. No replay,
//! fake progress, declared election result, reset or blank learner permission.
use super::*;
use crate::distribution_native_host::NativeMetadataControlHost;
use std::time::Instant;

fn budget(ms: u64) -> CancellationToken {
    CancellationToken::uncancelable()
        .child_with_deadline(Instant::now() + Duration::from_millis(ms))
}
fn template(f: &Fixture, i: usize) -> DistributionConfig {
    let local = &f.plan.topology.nodes[&node(i)];
    DistributionConfig {
        enabled: true,
        cluster_id: f.plan.topology.cluster_id.clone(),
        range_image_bootstrap_id: f.plan.topology.range_image_bootstrap_id,
        node_id: local.id.clone(),
        node_address: local.address.clone(),
        node_incarnation: local.incarnation,
        node_capacity_bytes: local.capacity_bytes,
        node_tls_certificate_sha256: local.tls_certificate_sha256.clone(),
        replication_factor: f.plan.topology.replication_factor,
        initial_ranges: f.plan.topology.ranges.len().try_into().unwrap(),
        transport: f.networks[i].clone(),
        ..Default::default()
    }
}

#[test]
fn native_initial_data_genesis_refuses_changed_epoch_restore_and_actual_foreign_birth() {
    let f = Fixture::new();
    let seed = &f.hosts[0].seed;
    let range = seed.spec.initial_ranges.first().unwrap();
    let database = seed.database.read();
    let proof = database
        .observe_initial_data_genesis(&seed.factory, range, &f.plan.topology)
        .unwrap();
    proof.require_current(&database, &f.plan.topology).unwrap();
    let mut changed = f.plan.topology.clone();
    changed
        .ranges
        .values_mut()
        .find(|r| r.id == range.id)
        .unwrap()
        .epoch += 1;
    assert!(proof.require_current(&database, &changed).is_err());
    let mut changed = f.plan.topology.clone();
    changed.range_image_bootstrap_id = Some(Uuid::now_v7());
    assert!(proof.require_current(&database, &changed).is_err());
    // Both are actual healthy native factories; an opaque proof from n0
    // cannot be used with the different actual n1 birth/open/root.
    assert!(proof
        .require_current(&f.hosts[1].seed.database.read(), &f.plan.topology)
        .is_err());
    assert!(!database.commit_admission_ready());
    assert!(database
        .get("items", "not-written-by-initial-data-proof")
        .unwrap()
        .is_none());
}

#[test]
fn native_initial_data_genesis_refuses_same_root_same_birth_new_actual_open() {
    let planned = planned_network(3);
    let path = planned.root.path().join("same-birth-open");
    let seed = NativeProvisionalNode::create(
        &path,
        db_config(),
        planned.plan.clone(),
        &node(0),
        &budget(5000),
    )
    .unwrap();
    let range = seed.spec.initial_ranges.first().unwrap().clone();
    let birth = seed.manifest.birth_id;
    let old_open = seed.database.read().incarnation_id().to_string();
    let ended = Arc::downgrade(&seed.database);
    let actual_open_ended = seed.database.read().native_metadata_owner_open_lifetime();
    let proof = seed
        .database
        .read()
        .observe_initial_data_genesis(&seed.factory, &range, &planned.plan.topology)
        .unwrap();
    drop(seed);
    assert!(
        ended.upgrade().is_none(),
        "all actual old DB owners must end before reopen"
    );
    assert!(
        actual_open_ended.upgrade().is_none(),
        "old inner BicDb open sentinel must end, not only its outer Arc"
    );
    let reopened = NativeProvisionalNode::open_unregistered(
        &path,
        db_config(),
        planned.plan.clone(),
        &node(0),
        &budget(5000),
    )
    .unwrap();
    assert_eq!(reopened.manifest.birth_id, birth);
    let database = reopened.database.read();
    assert_ne!(database.incarnation_id(), old_open);
    assert!(
        proof
            .require_current(&database, &planned.plan.topology)
            .is_err(),
        "same root/birth/R0 is not the original actual open"
    );
    database
        .observe_initial_data_genesis(&reopened.factory, &range, &planned.plan.topology)
        .unwrap()
        .require_current(&database, &planned.plan.topology)
        .unwrap();
    assert!(!database.commit_admission_ready());
}

fn actual_initial_metadata_controls() -> (
    Fixture,
    Vec<NativeMetadataControlHost>,
    Vec<DistributionConfig>,
) {
    let mut f = Fixture::new();
    let configuration = f.hosts[0]
        .observe_initial_configuration(&budget(5000))
        .unwrap();
    let permits = f
        .hosts
        .iter()
        .map(|host| {
            host.prepare_common_initial_configuration(&configuration, &budget(5000))
                .unwrap()
        })
        .collect::<Vec<_>>();
    let launches = f
        .hosts
        .iter_mut()
        .zip(&permits)
        .map(|(host, permit)| {
            host.finish_initial_consensus(permit, &budget(5000))
                .unwrap()
        })
        .collect::<Vec<_>>();
    let listeners = launches
        .iter()
        .enumerate()
        .map(|(i, launch)| {
            launch
                .start_metadata_listener(f.networks[i].clone(), &budget(5000))
                .unwrap()
        })
        .collect::<Vec<_>>();
    launches[0].elect_initial_metadata(&budget(5000)).unwrap();
    {
        // Hold the ACTUAL service, not a synthetic transport. Constructing
        // the finite runtime must expire without starting an uncancellable
        // background recovery or publishing any writer/readiness.
        let _held = launches[0].service.lock();
        let result = crate::distribution_range_recovery::RangeRecoveryRuntime::new_cancellable(
            launches[0].service.clone(),
            launches[0].provider.clone(),
            &budget(30),
        );
        assert!(matches!(result, Err(crate::BicDbError::QueryTimedOut)));
    }
    let configs = (0..3).map(|i| template(&f, i)).collect::<Vec<_>>();
    let mut controls = launches
        .into_iter()
        .zip(listeners)
        .enumerate()
        .map(|(i, (launch, listener))| {
            NativeMetadataControlHost::new(
                launch,
                listener,
                f.networks[i].clone(),
                configs[i].clone(),
            )
        })
        .collect::<Vec<_>>();
    for control in &mut controls {
        control.advance(&budget(5000)).unwrap();
    }
    (f, controls, configs)
}

#[test]
fn native_initial_data_actual_tls_cancel_partial_quorum_then_nonempty_is_not_new_genesis() {
    let (f, mut controls, configs) = actual_initial_metadata_controls();
    let canceled = budget(1000);
    canceled.cancel();
    assert!(controls[0].open_initial_data(&canceled).is_err());
    assert!(controls[0]
        .initial_data_status(&budget(1000))
        .unwrap()
        .is_none());
    assert!(!f.hosts[0].seed.database.read().commit_admission_ready());
    assert!(controls[0].open_initial_data(&budget(2000)).is_err());
    let pending = controls[0]
        .initial_data_status(&budget(1000))
        .unwrap()
        .unwrap();
    assert!(pending.receiver_listening && pending.writer_policy_current);
    assert!(!pending.dispatcher_published);
    assert!(pending.ranges.iter().any(|r| r.local_leader));
    assert!(pending.ranges.iter().all(|r| !r.leader_admission_observed));
    let until = Instant::now() + Duration::from_secs(20);
    let mut ready = [false; 3];
    while !ready.iter().all(|r| *r) {
        for (i, control) in controls.iter_mut().enumerate() {
            if !ready[i] {
                ready[i] = control.open_initial_data(&budget(1500)).is_ok();
            }
        }
        assert!(
            Instant::now() < until,
            "actual initial DATA QQ did not complete: {ready:?}"
        );
    }
    let bootstrap = crate::TcpClusterBootstrapClient::new(
        configs[0].cluster_id.clone(),
        node(0),
        f.networks[0].clone(),
    )
    .unwrap();
    let snapshot = bootstrap
        .fetch_snapshot(&node(0), &configs[0].node_address)
        .unwrap();
    let topology = snapshot.topology;
    let gateway = crate::ClusterEventClient::new(&configs[0], topology.clone())
        .unwrap()
        .with_request_timeout(Duration::from_secs(2))
        .unwrap();
    let key = "native-initial-real-nonempty";
    let expected =
        serde_json::json!({"actual":"first externally visible native point", "nested":[null,17]});
    assert!(gateway.append("items", key, expected.clone()).unwrap());
    assert_eq!(gateway.get_strong("items", key).unwrap(), Some(expected));
    let id = crate::cluster_event_record_id("items", key).unwrap();
    let range = topology
        .range_for_key(crate::CLUSTER_EVENTS_COLLECTION, &id)
        .unwrap();
    let leader_index = (0..3).find(|i| node(*i) == range.leader).unwrap();
    let leader = &f.hosts[leader_index].seed;
    let database = leader.database.read();
    assert_eq!(
        database
            .current_native_range_source_lineage(range.id)
            .unwrap()
            .range_index(),
        1
    );
    assert!(
        database
            .observe_initial_data_genesis(&leader.factory, range, &topology)
            .is_err(),
        "real R1 is not a new initial genesis"
    );
    // Native local B can change because another range received an ACK. Its
    // unchanged selected protected genesis remains eligible, without casting R.
    for other in leader
        .spec
        .initial_ranges
        .iter()
        .filter(|r| r.id != range.id)
    {
        database
            .observe_initial_data_genesis(&leader.factory, other, &topology)
            .unwrap();
    }
    drop(database);
    // Deterministic replacement cut: the old owner's exact hook is genuinely
    // checked first; then a separately fresh own MQ publishes a NEW actual
    // enrolled writer before old-owner shutdown. Stop must not erase it.
    let replacement = controls[0]
        .test_replace_initial_data_writer_after_identity_check(&node(0), &budget(5000))
        .unwrap();
    controls[0].stop(&budget(5000)).unwrap();
    let database = f.hosts[0].seed.database.read();
    let first = f.hosts[0].seed.spec.initial_ranges.first().unwrap();
    replacement
        .guard
        .require_for_coordinator(&database, first, &replacement.coordinator)
        .unwrap();
    drop(database);
    for control in controls.iter_mut().skip(1) {
        control.stop(&budget(5000)).unwrap();
    }
    let database = f.hosts[0].seed.database.read();
    assert!(replacement
        .guard
        .retire_if_current(&database, &replacement.coordinator)
        .unwrap());
    assert!(!database.commit_admission_ready());
    // Even SAME Arc reinstallation is a new actual registration. The stale
    // proof cannot compare-clear it; generic installation supplies no native
    // policy permission and is removed only as explicit test cleanup.
    database.install_commit_admission(replacement.coordinator.clone());
    assert!(!replacement
        .guard
        .retire_if_current(&database, &replacement.coordinator)
        .unwrap());
    assert!(database.commit_admission_ready());
    database.clear_commit_admission();
}

#[test]
fn native_initial_data_actual_tls_drop_without_stop_ends_inner_owner_and_reopens_same_root() {
    for mode in 0..3 {
        let (f, mut controls, configs) = actual_initial_metadata_controls();
        let until = Instant::now() + Duration::from_secs(20);
        let mut ready = [false; 3];
        while !ready.iter().all(|ready| *ready) {
            for (i, control) in controls.iter_mut().enumerate() {
                if !ready[i] {
                    ready[i] = control.open_initial_data(&budget(1500)).is_ok();
                }
            }
            assert!(
                Instant::now() < until,
                "actual DATA failed before lifecycle cut: {ready:?}"
            );
        }
        let bootstrap = crate::TcpClusterBootstrapClient::new(
            configs[0].cluster_id.clone(),
            node(0),
            f.networks[0].clone(),
        )
        .unwrap();
        let snapshot = bootstrap
            .fetch_snapshot(&node(0), &configs[0].node_address)
            .unwrap();
        let current_topology = snapshot.topology.clone();
        let gateway = crate::ClusterEventClient::new(&configs[0], snapshot.topology)
            .unwrap()
            .with_request_timeout(Duration::from_secs(2))
            .unwrap();
        let key = "actual-before-owner-drop";
        let expected = serde_json::json!({"preserved":"ACK through the actually opened DATA owner", "mode":mode});
        assert!(gateway.append("items", key, expected.clone()).unwrap());
        assert_eq!(gateway.get_strong("items", key).unwrap(), Some(expected));
        let id = crate::cluster_event_record_id("items", key).unwrap();
        let selected = current_topology
            .range_for_key(crate::CLUSTER_EVENTS_COLLECTION, &id)
            .unwrap();
        let materialized = f
            .hosts
            .iter()
            .map(|host| {
                let database = host.seed.database.read();
                let record = database
                    .get(crate::CLUSTER_EVENTS_COLLECTION, &id)
                    .unwrap()
                    .unwrap();
                let native = database
                    .current_native_range_source_lineage(selected.id)
                    .unwrap();
                assert_eq!(
                    native.range_index(),
                    1,
                    "all three actual replicas must materialize the ACK before the Drop cut"
                );
                (
                    (*record).clone(),
                    native.range_index(),
                    native.logical_genesis_sha256().to_string(),
                    native.logical_history_sha256().to_string(),
                )
            })
            .collect::<Vec<_>>();
        assert!(materialized.iter().all(|point| point.1 == materialized[0].1
            && point.2 == materialized[0].2
            && point.3 == materialized[0].3));
        let specs = f
            .hosts
            .iter()
            .map(|host| host.seed.spec.clone())
            .collect::<Vec<_>>();
        let births = f
            .hosts
            .iter()
            .map(|host| host.seed.manifest.birth_id)
            .collect::<Vec<_>>();
        let roots = f
            .hosts
            .iter()
            .map(|host| host.seed.root.clone())
            .collect::<Vec<_>>();
        let outer = f
            .hosts
            .iter()
            .map(|host| Arc::downgrade(&host.seed.database))
            .collect::<Vec<_>>();
        let inner = f
            .hosts
            .iter()
            .map(|host| {
                host.seed
                    .database
                    .read()
                    .native_metadata_owner_open_lifetime()
            })
            .collect::<Vec<_>>();
        if mode == 1 {
            let canceled = budget(1000);
            canceled.cancel();
            for control in &mut controls {
                assert!(control.stop(&canceled).is_err());
            }
        } else if mode == 2 {
            // Actual durability failure is a refusal, never a reason to keep
            // this exact hook cycle alive. No cold-repair success is claimed.
            f.hosts[0].seed.database.read().fence_durability(
                "initial-data-drop-test",
                &"injected health failure before revocation-only cleanup",
            );
            assert!(f.hosts[0]
                .seed
                .database
                .read()
                .check_recovery_state()
                .is_err());
        }
        drop(gateway);
        drop(bootstrap);
        // Neither mode0 nor mode1 obtains a successful explicit stop receipt.
        drop(controls);
        let Fixture {
            hosts,
            plan: _,
            networks: _,
            _root,
        } = f;
        drop(hosts);
        let until = Instant::now() + Duration::from_secs(5);
        while outer.iter().any(|weak| weak.upgrade().is_some())
            || inner.iter().any(|weak| weak.upgrade().is_some())
        {
            assert!(Instant::now() < until, "opened DATA hook retained its actual DB/directory after all external owners ended, mode={mode}");
            std::thread::sleep(Duration::from_millis(5));
        }
        if mode != 2 {
            for i in 0..3 {
                // Strict native open of the SAME storage root: no factory
                // recreation, empty replacement catalogue, reset or policy
                // restoration. Native metadata voting cold-start is separate.
                let (database, _factory) =
                    BicDb::open_range_image_database(&roots[i], db_config(), &specs[i]).unwrap();
                assert_eq!(
                    database
                        .native_recovery_admission(
                            &specs[i].cluster_id,
                            specs[i].bootstrap_id,
                            &specs[i].node_id,
                            specs[i].member_incarnation
                        )
                        .unwrap()
                        .birth_id(),
                    births[i]
                );
                assert!(
                    !database.commit_admission_ready(),
                    "cold local storage open cannot revive serving"
                );
                database.schema_compatibility_fingerprint().unwrap();
                let row = database.get(crate::CLUSTER_EVENTS_COLLECTION, &id).unwrap();
                assert!(row.is_some());
                assert_eq!(row.as_deref(), Some(&materialized[i].0));
                let native = database
                    .current_native_range_source_lineage(selected.id)
                    .unwrap();
                assert_eq!(native.range_index(), materialized[i].1);
                assert_eq!(native.logical_genesis_sha256(), materialized[i].2);
                assert_eq!(native.logical_history_sha256(), materialized[i].3);
            }
        }
        drop(_root);
    }
}

mod routing_discovery_tests;

mod registry_refresh_tests;

mod hot_recovery_tests;
mod origin_pre_wal_fault_tests;

mod borrowed_hook_tests;
