//! Actual process commands and existing authenticated bootstrap RPCs only.
use super::*;

#[test]
fn native_metadata_registration_four_process_commands_commit_exact_learner() {
    let PlannedNetwork {
        root,
        plan,
        networks,
        ports,
    } = planned_network_with_spare(4, 1);
    let binary = binary_sha256();
    let mut processes = Processes {
        children: (0..4).map(|_| None).collect(),
        root: Some(root),
        configuration: Vec::new(),
        ready: Vec::new(),
        stop: Vec::new(),
    };
    for (index, network) in networks.iter().take(4).enumerate() {
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
    let fifth_address = ports[4].local_addr().unwrap().to_string();
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

    let mut sequences = [0u64; 4];
    for index in 0..4 {
        sequences[index] += 1;
        step(&mut processes, index, sequences[index], "prepare");
    }
    for index in 0..4 {
        sequences[index] += 1;
        step(&mut processes, index, sequences[index], "launch");
    }
    sequences[0] += 1;
    step(&mut processes, 0, sequences[0], "elect");
    let target = crate::ClusterNode::new(node(4), fifth_address, 1, 1 << 30, 100)
        .unwrap()
        .as_metadata_learner()
        .with_tls_certificate_sha256(
            crate::replication_transport::replication_certificate_sha256(
                &networks[4].tls.as_ref().unwrap().cert_path,
            )
            .unwrap(),
        )
        .unwrap();
    let client = crate::distribution_transport::TcpClusterBootstrapClient::new(
        plan.topology.cluster_id.clone(),
        node(4),
        networks[4].clone(),
    )
    .unwrap();
    let address = plan.topology.nodes[&node(0)].address.clone();
    // Existing metadata-only listener still refuses the old registration arm.
    assert!(client
        .register_metadata_learner(&node(0), &address, target.clone(), 100)
        .is_err());
    for index in 0..4 {
        sequences[index] += 1;
        step(&mut processes, index, sequences[index], "control");
    }
    let before = client.fetch_snapshot(&node(0), &address).unwrap();
    assert!(!before.topology.nodes.contains_key(&node(4)));
    let mut bad = target.clone();
    bad.incarnation = 0;
    assert!(client
        .register_metadata_learner(&node(0), &address, bad, 100)
        .is_err());
    // Real listeners pause while their actual FSM/store owners stay alive.
    // An unavailable old-voter quorum is not a committed registration.
    for index in [2, 3] {
        sequences[index] += 1;
        step(&mut processes, index, sequences[index], "pause-control");
    }
    assert!(client
        .register_metadata_learner(&node(0), &address, target.clone(), 100)
        .is_err());
    assert!(!client
        .fetch_snapshot(&node(0), &address)
        .unwrap()
        .topology
        .nodes
        .contains_key(&node(4)));
    for index in [2, 3] {
        sequences[index] += 1;
        step(&mut processes, index, sequences[index], "control");
    }
    // Interrupt only AFTER the real owned store fsyncs its proposed CONFIG.
    sequences[0] += 1;
    step(&mut processes, 0, sequences[0], "interrupt-control");
    assert!(client
        .register_metadata_learner(&node(0), &address, target.clone(), 100)
        .is_err());
    sequences[0] += 1;
    let pending = step(&mut processes, 0, sequences[0], "status-control");
    assert!(pending.last_log_index > pending.metadata_read_index);
    assert!(!client
        .fetch_snapshot(&node(0), &address)
        .unwrap()
        .topology
        .nodes
        .contains_key(&node(4)));
    let committed = client
        .register_metadata_learner(&node(0), &address, target.clone(), 100)
        .unwrap();
    assert_eq!(committed.nodes[&node(4)], target);
    sequences[0] += 1;
    let finished = step(&mut processes, 0, sequences[0], "status-control");
    assert_eq!(finished.last_log_index, pending.last_log_index);
    assert_eq!(finished.metadata_read_index, pending.last_log_index);
    assert_eq!(
        finished.topology_generation,
        pending.topology_generation + 1
    );

    assert_eq!(committed.ranges, before.topology.ranges);
    assert!(committed.nodes[&node(4)].native_store_birth_v1.is_none());
    assert_eq!(
        committed.nodes[&node(4)].metadata_role,
        crate::MetadataMemberRole::Learner
    );
    let duplicate = client
        .register_metadata_learner(&node(0), &address, target.clone(), 101)
        .unwrap();
    assert_eq!(
        duplicate, committed,
        "duplicate must not allocate another CONFIG"
    );
    let mut wrong = target.clone();
    wrong.incarnation += 1;
    assert!(client
        .register_metadata_learner(&node(0), &address, wrong, 102)
        .is_err());
    assert_eq!(
        client.fetch_snapshot(&node(0), &address).unwrap().topology,
        committed
    );
    let transport = TcpClusterRelocationTransport::new(
        plan.topology.cluster_id.clone(),
        node(0),
        Arc::new(RwLock::new(committed.clone())),
        networks[0].clone(),
    )
    .unwrap();
    let range = committed.ranges.values().next().unwrap();
    // The registration is committed by an old-voter QUORUM (3 of 4): the
    // leader stops replicating at the commit and each later read round returns
    // at its own quorum, so one follower may legitimately still lack the entry
    // or its commit index. Nothing in these manually stepped processes runs
    // the automatic supervisor, so drive the leader's ordinary full heartbeat
    // (every active peer, awaited) before requiring all four local views.
    for round in 0..5 {
        sequences[0] += 1;
        step(&mut processes, 0, sequences[0], "heartbeat-control");
        let lagging = (0..4)
            .filter(|index| {
                !client
                    .fetch_snapshot(&node(*index), &committed.nodes[&node(*index)].address)
                    .is_ok_and(|snapshot| snapshot.topology.nodes.get(&node(4)) == Some(&target))
            })
            .collect::<Vec<_>>();
        eprintln!("NATIVE_METADATA_REGISTRATION_CONVERGENCE round={round} lagging={lagging:?}");
        if lagging.is_empty() {
            break;
        }
    }
    for index in 0..4 {
        let snapshot = client
            .fetch_snapshot(&node(index), &committed.nodes[&node(index)].address)
            .unwrap();
        assert_eq!(snapshot.topology.nodes[&node(4)], target);
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
                    }
                }
            )
            .is_err());
    }
    processes.graceful_stop();
    let root = processes.root.take().unwrap().keep();
    eprintln!(
        "NATIVE_METADATA_REGISTRATION_PROCESS_ROOT {}",
        root.display()
    );
}
