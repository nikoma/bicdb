//! Four actual OS processes/mTLS listeners, on one host. This checkpoint ends
//! at policies with the metadata-only allowlist still enforced. No data Ready,
//! first ACK, production deployment or physical failure result is implied.
use super::*;

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Stage {
    sequence: u64,
    name: String,
    process_id: u32,
    store_open_id: Uuid,
    metadata_term: u64,
    metadata_read_index: u64,
    metadata_context_id: Option<Uuid>,
    last_log_index: u64,
    topology_generation: u64,
}
pub(super) fn drive_startup(mut host: NativeProvisionalHost, config: &ChildConfig) {
    let command_path = config.ready.with_extension("command");
    let stage_path = config.ready.with_extension("stage");
    let until = Instant::now() + Duration::from_secs(90);
    let mut permit = None;
    let mut launch = None;
    let mut listener = None;
    let mut writer = None;
    let mut control: Option<crate::distribution_native_host::NativeMetadataControlHost> = None;
    let mut completed = 0;
    while !config.stop.exists() && Instant::now() < until {
        if let Ok(bytes) = std::fs::read(&command_path) {
            let (sequence, name): (u64, String) = serde_json::from_slice(&bytes).unwrap();
            if sequence > completed {
                assert_eq!(sequence, completed + 1);
                let token = CancellationToken::uncancelable()
                    .child_with_deadline(Instant::now() + Duration::from_secs(5));
                match name.as_str() {
                    "prepare" => {
                        let configuration = host.observe_initial_configuration(&token).unwrap();
                        permit = Some(
                            host.prepare_common_initial_configuration(&configuration, &token)
                                .unwrap(),
                        );
                    }
                    "launch" => {
                        let actual = host
                            .finish_initial_consensus(permit.as_ref().unwrap(), &token)
                            .unwrap();
                        listener = Some(
                            actual
                                .start_metadata_listener(config.network.clone(), &token)
                                .unwrap(),
                        );
                        launch = Some(actual);
                        permit = None;
                    }
                    "elect" => {
                        launch
                            .as_ref()
                            .unwrap()
                            .elect_initial_metadata(&token)
                            .unwrap();
                    }
                    "policy" => {
                        writer = Some(
                            launch
                                .as_ref()
                                .unwrap()
                                .enroll_initial_writer(&node(0), &token)
                                .unwrap(),
                        );
                    }
                    "control" => {
                        if control.is_none() {
                            let member = &host.seed.plan.topology.nodes[&node(config.index)];
                            let template = crate::DistributionConfig {
                                enabled: true,
                                cluster_id: host.seed.plan.topology.cluster_id.clone(),
                                range_image_bootstrap_id: host
                                    .seed
                                    .plan
                                    .topology
                                    .range_image_bootstrap_id,
                                node_id: member.id.clone(),
                                node_address: member.address.clone(),
                                node_incarnation: member.incarnation,
                                node_capacity_bytes: member.capacity_bytes,
                                node_tls_certificate_sha256: member.tls_certificate_sha256.clone(),
                                replication_factor: host.seed.plan.topology.replication_factor,
                                initial_ranges: host
                                    .seed
                                    .plan
                                    .topology
                                    .ranges
                                    .len()
                                    .try_into()
                                    .unwrap(),
                                transport: config.network.clone(),
                                ..Default::default()
                            };
                            control = Some(
                                crate::distribution_native_host::NativeMetadataControlHost::new(
                                    launch.take().unwrap(),
                                    listener.take().unwrap(),
                                    config.network.clone(),
                                    template,
                                ),
                            );
                        }
                        control.as_mut().unwrap().advance(&token).unwrap();
                    }
                    "interrupt-control" => control.as_ref().unwrap().fail_after_next_proposal(),
                    "status-control" => {}
                    "pause-control" => control.as_mut().unwrap().stop(&token).unwrap(),
                    "heartbeat-control" => control.as_ref().unwrap().heartbeat(&token).unwrap(),
                    _ => panic!("unknown parent-owned startup step"),
                }
                token.check().unwrap();
                let metadata = control
                    .as_ref()
                    .map(|c| c.metadata_status(&token).unwrap())
                    .or_else(|| launch.as_ref().map(|l| l.metadata.lock().status()));
                let stage = Stage {
                    sequence,
                    name,
                    process_id: std::process::id(),
                    store_open_id: host._service.lock().native_initial_listener_store_open_id(),
                    metadata_term: writer.as_ref().map_or_else(
                        || metadata.as_ref().map_or(0, |m| m.current_term),
                        |w| w.metadata_term,
                    ),
                    metadata_read_index: writer.as_ref().map_or_else(
                        || metadata.as_ref().map_or(0, |m| m.commit_index),
                        |w| w.metadata_read_index,
                    ),
                    metadata_context_id: writer.as_ref().map(|w| w.metadata_context_id),
                    last_log_index: metadata.as_ref().map_or(0, |m| m.last_log_index),
                    topology_generation: metadata.as_ref().map_or(0, |m| m.topology_generation),
                };
                crate::storage::write_atomic(
                    &stage_path,
                    &serde_json::to_vec(&stage).unwrap(),
                    true,
                )
                .unwrap();
                completed = sequence;
            }
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    if let Some(mut actual) = control {
        actual
            .stop(
                &CancellationToken::uncancelable()
                    .child_with_deadline(Instant::now() + Duration::from_secs(5)),
            )
            .unwrap();
    }
    if let Some(mut actual) = listener {
        actual
            .stop_and_drain(
                &CancellationToken::uncancelable()
                    .child_with_deadline(Instant::now() + Duration::from_secs(5)),
            )
            .unwrap();
    }
    // This child exclusively owns this fresh host and its one hook. Clear that
    // task-owned hook before dropping service/DB/coordinator strong owners.
    host.seed.database.read().clear_commit_admission();
    drop(writer);
    drop(launch);
    host.shutdown().unwrap();
}

fn step(processes: &mut Processes, index: usize, sequence: u64, name: &str) -> Stage {
    crate::storage::write_atomic(
        &processes.ready[index].with_extension("command"),
        &serde_json::to_vec(&(sequence, name)).unwrap(),
        true,
    )
    .unwrap();
    let until = Instant::now() + Duration::from_secs(15);
    loop {
        assert!(
            processes.children[index]
                .as_mut()
                .unwrap()
                .try_wait()
                .unwrap()
                .is_none(),
            "startup child failed; retained stopped root contains exact logs"
        );
        if let Ok(bytes) = std::fs::read(processes.ready[index].with_extension("stage")) {
            let stage: Stage = serde_json::from_slice(&bytes).unwrap();
            if stage.sequence == sequence {
                assert_eq!(stage.name, name);
                assert_eq!(
                    stage.process_id,
                    processes.children[index].as_ref().unwrap().id()
                );
                return stage;
            }
        }
        assert!(
            Instant::now() < until,
            "bounded actual startup step timed out"
        );
        std::thread::sleep(Duration::from_millis(10));
    }
}

#[test]
fn native_initial_startup_four_processes_actual_election_host_mq_and_writer_policies() {
    let PlannedNetwork {
        root,
        plan,
        networks,
        ports,
    } = planned_network(4);
    let binary = binary_sha256();
    let mut processes = Processes {
        children: (0..4).map(|_| None).collect(),
        root: Some(root),
        configuration: Vec::new(),
        ready: Vec::new(),
        stop: Vec::new(),
    };
    for (index, network) in networks.iter().enumerate() {
        let root = processes.root.as_ref().unwrap().path();
        let configuration = root.join(format!("startup-node{index}.json"));
        let ready = root.join(format!("startup-node{index}.ready"));
        let stop = root.join(format!("startup-node{index}.stop"));
        crate::storage::write_atomic(
            &configuration,
            &serde_json::to_vec(&ChildConfig {
                version: 1,
                index,
                database: root.join(format!("startup-db{index}")),
                ready: ready.clone(),
                stop: stop.clone(),
                plan: plan.clone(),
                network: network.clone(),
                live_metadata: true,
            })
            .unwrap(),
            true,
        )
        .unwrap();
        processes.configuration.push(configuration);
        processes.ready.push(ready);
        processes.stop.push(stop);
    }
    drop(ports);
    for index in 0..4 {
        processes.start(index);
    }
    let initial = (0..4)
        .map(|i| processes.wait_ready(i, &binary))
        .collect::<Vec<_>>();
    assert_eq!(
        initial
            .iter()
            .map(|r| r.process_id)
            .collect::<BTreeSet<_>>()
            .len(),
        4
    );
    assert!(!initial.iter().any(|r| r.process_id == std::process::id()));
    let mut transcript = Vec::new();
    let mut stores = Vec::new();
    for index in 0..4 {
        let stage = step(&mut processes, index, 1, "prepare");
        assert_eq!(stage.metadata_term, 0);
        stores.push(stage.store_open_id);
        transcript.push(stage);
    }
    for index in 0..4 {
        let stage = step(&mut processes, index, 2, "launch");
        assert_eq!(
            stage.store_open_id, stores[index],
            "same actual service/store must survive listener drain"
        );
        transcript.push(stage);
    }
    let elected = step(&mut processes, 0, 3, "elect");
    assert!(elected.metadata_term > 0 && elected.metadata_read_index > 0);
    transcript.push(elected);
    for index in 0..4 {
        let sequence = if index == 0 { 4 } else { 3 };
        let stage = step(&mut processes, index, sequence, "policy");
        assert_eq!(stage.store_open_id, stores[index]);
        assert!(stage.metadata_term > 0 && stage.metadata_read_index > 0);
        assert!(stage.metadata_context_id.is_some_and(|id| !id.is_nil()));
        transcript.push(stage);
    }
    // All processes retain the original metadata-only listener. Installing a
    // policy cannot turn that listener into an ordinary data surface.
    let transport = TcpClusterRelocationTransport::new(
        plan.topology.cluster_id.clone(),
        node(0),
        Arc::new(RwLock::new(plan.topology.clone())),
        networks[0].clone(),
    )
    .unwrap();
    let range = plan.topology.ranges.values().next().unwrap();
    for index in 0..4 {
        assert!(transport
            .application_event(
                &node(index),
                crate::ClusterEventRequest {
                    range_id: range.id,
                    range_epoch: range.epoch,
                    operation: crate::ClusterEventOperation::Scan {
                        namespace: "items".into(),
                        after: None,
                        limit: 1
                    },
                }
            )
            .unwrap_err()
            .to_string()
            .contains("metadata endpoints only"));
    }
    crate::storage::write_atomic(
        &processes
            .root
            .as_ref()
            .unwrap()
            .path()
            .join("startup-transcript.json"),
        &serde_json::to_vec_pretty(&transcript).unwrap(),
        true,
    )
    .unwrap();
    processes.graceful_stop();
    let root = processes.root.take().unwrap().keep();
    eprintln!("NATIVE_INITIAL_STARTUP_PROCESSES_ROOT {}", root.display());
}

mod registration_tests;
