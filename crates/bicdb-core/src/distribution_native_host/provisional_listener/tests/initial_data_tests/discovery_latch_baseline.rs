//! Expected RED until INITIAL routing discovery can survive busy DATA latches.
//! Real INITIAL owners/TLS/MQ; no topology or handler policy emulation.
use super::*;
use std::fs::{File, OpenOptions};
use std::io::{self, Write};
use std::path::Path;

struct ExactJournal(std::sync::Mutex<File>);
impl ExactJournal {
    fn create(path: &Path) -> Self {
        Self(std::sync::Mutex::new(
            OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(path)
                .unwrap(),
        ))
    }
    fn append(&self, value: &impl serde::Serialize) -> io::Result<()> {
        let mut file = self
            .0
            .lock()
            .map_err(|_| io::Error::other("journal lock poisoned"))?;
        serde_json::to_writer(&mut *file, value)?;
        file.write_all(b"\n")?;
        file.sync_all()
    }
}
impl crate::ClusterRpcEvidenceObserver for ExactJournal {
    fn record(&self, value: &crate::ClusterRpcEvidence) -> io::Result<()> {
        self.append(value)
    }
}
fn evidence_records(path: &Path) -> Vec<serde_json::Value> {
    std::fs::read_to_string(path)
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect()
}
fn exact_frames(records: &[serde_json::Value]) -> String {
    records
        .iter()
        .filter_map(|record| {
            let bytes = record["raw_frame"]
                .as_array()?
                .iter()
                .map(|b| u8::try_from(b.as_u64().unwrap()).unwrap())
                .collect::<Vec<_>>();
            // Actual complete length-framed bytes are retained in JSONL; this
            // string is only an assertion/search projection, never a reply source.
            (bytes.len() >= 4).then(|| String::from_utf8_lossy(&bytes[4..]).into_owned())
        })
        .collect::<Vec<_>>()
        .join("\n")
}
fn complete_error(error: &dyn std::error::Error) -> Vec<String> {
    let mut chain = vec![error.to_string()];
    let mut next = error.source();
    while let Some(error) = next {
        chain.push(error.to_string());
        next = error.source();
    }
    chain
}
fn key_for_leader(topology: &ClusterTopology, leader: &ClusterNodeId) -> String {
    (0..10_000)
        .map(|i| format!("initial-discovery-{leader}-{i}"))
        .find(|key| {
            let id = crate::cluster_event_record_id("items", key).unwrap();
            topology
                .range_for_key(crate::CLUSTER_EVENTS_COLLECTION, &id)
                .is_ok_and(|range| &range.leader == leader)
        })
        .expect("actual initial topology must include the requested leader")
}

#[test]
fn native_initial_data_healthy_metadata_discovery_while_all_data_services_held() {
    let (f, mut controls, configs) = actual_initial_metadata_controls();
    let output = std::env::var_os("BICDB_TEST_EVIDENCE_ROOT")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|| f._root.path().join("discovery-evidence"))
        .join(format!("initial-data-held-discovery-{}", Uuid::now_v7()));
    std::fs::create_dir_all(&output).unwrap();
    let audit = ExactJournal::create(&output.join("audit.jsonl"));
    let rpc_path = output.join("rpc.jsonl");
    let sink = Arc::new(ExactJournal::create(&rpc_path));
    // Persist the new journal directory's entry AND workflow-created parent
    // entries before the first observation. Resolve aliases and exclude every
    // actual source DB directory: this is diagnostic ancestry, never a DB fsync.
    let actual_output = std::fs::canonicalize(&output).unwrap();
    let actual_databases = (0..f.hosts.len())
        .map(|i| std::fs::canonicalize(f._root.path().join(format!("db{i}"))).unwrap())
        .collect::<Vec<_>>();
    assert!(
        actual_databases
            .iter()
            .all(|db| !actual_output.starts_with(db)),
        "diagnostic journal must be outside every original dbN"
    );
    for ancestor in actual_output.ancestors() {
        assert!(actual_databases.iter().all(|db| !ancestor.starts_with(db)));
        File::open(ancestor).unwrap().sync_all().unwrap();
    }
    eprintln!(
        "ACTUAL_INITIAL_DATA_DISCOVERY_EVIDENCE={}",
        output.display()
    );
    audit
        .append(&serde_json::json!({
            "phase":"begin", "source_sha":std::env::var("BICDB_TEST_SOURCE_SHA")
                .or_else(|_| std::env::var("GITHUB_SHA")).ok(),
            "declared_fixture_precondition":"actual initial metadata election n0",
            "members":3, "rf":3, "real_tls":true,
            "scope":"same-process actual INITIAL owners; no CLI/failure-cause/HA qualification"
        }))
        .unwrap();
    let until = Instant::now() + Duration::from_secs(20);
    let mut ready = [false; 3];
    while !ready.iter().all(|v| *v) {
        for (i, control) in controls.iter_mut().enumerate() {
            if !ready[i] {
                ready[i] = control.open_initial_data(&budget(1500)).is_ok();
            }
        }
        assert!(
            Instant::now() < until,
            "actual INITIAL opening incomplete: {ready:?}"
        );
    }
    let bootstrap = crate::TcpClusterBootstrapClient::new(
        configs[0].cluster_id.clone(),
        node(0),
        f.networks[0].clone(),
    )
    .unwrap();
    let topology = bootstrap
        .fetch_snapshot(&node(0), &configs[0].node_address)
        .unwrap()
        .topology;
    // The provisional host transport still has the BEFORE-enrollment plan.
    // Use actual authenticated committed snapshot DATA for destination binding,
    // exactly as the public gateway does; only the real server can mint HostMQ.
    let metadata_transport = TcpClusterRelocationTransport::new(
        configs[0].cluster_id.clone(),
        node(0),
        Arc::new(RwLock::new(topology.clone())),
        f.networks[0].clone(),
    )
    .unwrap();
    let gateway = crate::ClusterEventClient::new(&configs[0], topology.clone())
        .unwrap()
        .with_request_timeout(Duration::from_secs(2))
        .unwrap()
        .with_rpc_evidence_observer(sink, crate::ClusterRpcEvidenceLimits::default())
        .unwrap();
    let first_key = key_for_leader(&topology, &node(0));
    let first_value = serde_json::json!({"first":"actual acknowledged value", "nested":[null,17]});
    assert!(gateway
        .append("items", &first_key, first_value.clone())
        .unwrap());
    audit
        .append(
            &serde_json::json!({"phase":"business_ack1", "key":first_key,
        "value":first_value, "leader":node(0)}),
        )
        .unwrap();
    assert_eq!(
        gateway.get_strong("items", &first_key).unwrap(),
        Some(first_value.clone())
    );
    let before = controls
        .iter()
        .map(|c| c.initial_data_status(&budget(1000)).unwrap())
        .collect::<Vec<_>>();
    let record_start = evidence_records(&rpc_path).len();
    let services = f
        .hosts
        .iter()
        .map(|h| h._service.clone())
        .collect::<Vec<_>>();
    // The holder owns the DATA guards on its own thread. The public RPC
    // caller/observer holds no engine latch and does no reentrant engine work.
    // Channels are inside scope: panic drops release_tx before scoped join,
    // so even a failed MQ prerequisite cannot strand the holder.
    let (discovery, captured) = std::thread::scope(|scope| {
        let (ready_tx, ready_rx) = std::sync::mpsc::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel::<()>();
        scope.spawn(move || {
            let held = services
                .iter()
                .map(|service| service.lock())
                .collect::<Vec<_>>();
            ready_tx.send(held.len()).unwrap();
            let _ = release_rx.recv();
            drop(held);
        });
        let held_count = ready_rx.recv().unwrap();
        assert_eq!(held_count, 3);
        // Healthy metadata is a genuine new QQ while ALL actual DATA mutexes
        // remain held. No test-created topology/term is a substitute for this.
        let business = Uuid::now_v7();
        let mq = metadata_transport.acquire_host_metadata_observation(
            &node(0),
            business,
            Duration::from_secs(5),
            &budget(5000),
        );
        audit
            .append(
                &serde_json::json!({"phase":"held_metadata_acquisition_result",
        "business":business, "ok":mq.is_ok(),
        "error_chain":mq.as_ref().err().map(|error| complete_error(error))}),
            )
            .unwrap();
        let mq = mq.expect("actual HostMQ must be healthy WHILE every DATA service is held; see fsynced exact error if not");
        let held_topology = mq.committed_topology(business).unwrap().clone();
        assert_eq!(held_topology, topology);
        audit
            .append(
                &serde_json::json!({"phase":"held_metadata_qq", "business":business,
        "context":mq.context_id(), "term":mq.term(), "read_index":mq.read_index(),
        "metadata_leader":mq.metadata_leader_id(), "committed_topology":held_topology,
        "all_actual_service_latches_held":held_count}),
            )
            .unwrap();
        let discovery = gateway.refresh();
        let captured = evidence_records(&rpc_path);
        audit
            .append(&serde_json::json!({"phase":"held_public_refresh",
        "ok":discovery.is_ok(),
        "error_chain":discovery.as_ref().err().map(|error| complete_error(error)),
        "rpc_record_start":record_start, "rpc_record_end":captured.len()}))
            .unwrap();
        let post_business = Uuid::now_v7();
        let post_mq = metadata_transport.acquire_host_metadata_observation(
            &node(0),
            post_business,
            Duration::from_secs(5),
            &budget(5000),
        );
        audit
            .append(
                &serde_json::json!({"phase":"held_post_metadata_acquisition_result",
        "business":post_business, "ok":post_mq.is_ok(),
        "error_chain":post_mq.as_ref().err().map(|error| complete_error(error))}),
            )
            .unwrap();
        let post_mq = post_mq.expect("actual post-refresh HostMQ while DATA held");
        assert_eq!(
            post_mq.committed_topology(post_business).unwrap(),
            &topology
        );
        audit
            .append(
                &serde_json::json!({"phase":"held_metadata_qq_after_refresh",
        "business":post_business, "context":post_mq.context_id(),
        "term":post_mq.term(), "read_index":post_mq.read_index(),
        "metadata_leader":post_mq.metadata_leader_id()}),
            )
            .unwrap();
        drop(release_tx);
        (discovery, captured)
    });
    let frames = exact_frames(&captured[record_start..]);
    assert!(
        frames.contains("FetchBootstrapSnapshot"),
        "must use actual bootstrap RPC"
    );
    assert!(
        frames.contains("native bootstrap service busy"),
        "exact busy reply missing"
    );
    assert!(
        frames.contains("FetchTopology"),
        "must execute actual fallback RPC"
    );
    assert!(frames.contains("initial native DATA refuses generic snapshot/import/repair/cleanup and unsupported operations"),
        "exact immutable-listener fallback refusal missing");
    // The missing discovery response must not be interpreted as all members
    // dead or a lost ACK. Release contention, then use the SAME public client
    // and a DIFFERENT actual data leader. JSON null is Some(Null), not absence.
    let next_key = key_for_leader(&topology, &node(1));
    assert!(gateway
        .append("items", &next_key, serde_json::Value::Null)
        .unwrap());
    audit
        .append(
            &serde_json::json!({"phase":"business_ack2_after_release", "key":next_key,
        "value":null, "leader":node(1)}),
        )
        .unwrap();
    assert_eq!(
        gateway.get_strong("items", &next_key).unwrap(),
        Some(serde_json::Value::Null)
    );
    assert_eq!(
        gateway.get_strong("items", &first_key).unwrap(),
        Some(first_value)
    );
    let after = controls
        .iter()
        .map(|c| c.initial_data_status(&budget(1000)).unwrap())
        .collect::<Vec<_>>();
    let after_snapshot = bootstrap
        .fetch_snapshot(&node(0), &configs[0].node_address)
        .unwrap();
    assert_eq!(after_snapshot.topology, topology);
    audit
        .append(
            &serde_json::json!({"phase":"healthy_after_release", "before":before,
        "after":after, "topology_unchanged":true,
        "business_acks":2, "both_exact_strong_values_verified":true}),
        )
        .unwrap();
    for control in &mut controls {
        control.stop(&budget(5000)).unwrap();
    }
    audit
        .append(
            &serde_json::json!({"phase":"acceptance", "owners_stopped":true,
        "held_discovery_should_succeed":discovery.is_ok()}),
        )
        .unwrap();
    assert!(discovery.is_ok(), "healthy metadata routing discovery must survive busy DATA services; actual result: {discovery:?}; exact evidence: {}", output.display());
}
