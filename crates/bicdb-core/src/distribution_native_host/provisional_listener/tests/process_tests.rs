//! Separate OS processes, shared physical host. Only pre-metadata factory
//! bootstrap and cold reopen are exercised. No data ACK, metadata election,
//! physical host-loss, rebalance, throughput or production claim follows.
use super::*;
use std::process::{Child, Command, Stdio};
use std::time::Instant;

const CHILD_TEST: &str = "distribution_native_host::provisional_listener::tests::process_tests::native_initial_birth_process_child";
const CONFIG_ENV: &str = "BICDB_NATIVE_INITIAL_FACTORY_PROCESS_CONFIG";

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ChildConfig {
    version: u32,
    index: usize,
    database: PathBuf,
    ready: PathBuf,
    stop: PathBuf,
    plan: NativeClusterBootstrapPlan,
    network: ClusterNetworkTransportConfig,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ChildReady {
    process_id: u32,
    database_incarnation: String,
    birth_id: Uuid,
    birth_sha256: String,
    binary_sha256: String,
}
fn binary_sha256() -> String {
    use std::io::Read;
    let mut input = std::fs::File::open(std::env::current_exe().unwrap()).unwrap();
    let mut digest = Sha256::new();
    let mut bytes = [0u8; 64 * 1024];
    loop {
        let len = input.read(&mut bytes).unwrap();
        if len == 0 {
            break;
        }
        digest.update(&bytes[..len]);
    }
    hex::encode(digest.finalize())
}

#[test]
#[ignore = "internal child entry requires a parent-owned bounded private process configuration"]
fn native_initial_birth_process_child() {
    let input = PathBuf::from(std::env::var_os(CONFIG_ENV).expect("parent configuration required"));
    let bytes = std::fs::read(input).unwrap();
    assert!(bytes.len() < MAX_PLAN_BYTES);
    let config: ChildConfig = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(config.version, 1);
    assert!(config.index < 4);
    let token = CancellationToken::uncancelable();
    let local = node(config.index);
    let seed = if config.database.exists() {
        NativeProvisionalNode::open_unregistered(
            &config.database,
            db_config(),
            config.plan,
            &local,
            &token,
        )
    } else {
        NativeProvisionalNode::create(&config.database, db_config(), config.plan, &local, &token)
    }
    .unwrap();
    let ready = ChildReady {
        process_id: std::process::id(),
        database_incarnation: seed.database.read().incarnation_id().to_owned(),
        birth_id: seed.manifest.birth_id,
        birth_sha256: seed.manifest.birth_sha256.clone(),
        binary_sha256: binary_sha256(),
    };
    let host = NativeProvisionalHost::start(Arc::new(seed), config.network, &token).unwrap();
    crate::storage::write_atomic(&config.ready, &serde_json::to_vec(&ready).unwrap(), true)
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(90);
    // A parent process termination cannot leave unbounded orphan listeners.
    while !config.stop.exists() && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(10));
    }
    host.shutdown().unwrap();
}

struct Processes {
    children: Vec<Option<Child>>,
    root: Option<tempfile::TempDir>,
    configuration: Vec<PathBuf>,
    ready: Vec<PathBuf>,
    stop: Vec<PathBuf>,
}
impl Processes {
    fn start(&mut self, index: usize) {
        assert!(self.children[index].is_none());
        if self.ready[index].exists() {
            std::fs::remove_file(&self.ready[index]).unwrap();
        }
        let root = self.root.as_ref().unwrap().path();
        // A restart appends; the killed process's original output survives.
        let stdout = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(root.join(format!("node{index}.stdout")))
            .unwrap();
        let stderr = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(root.join(format!("node{index}.stderr")))
            .unwrap();
        let child = Command::new(std::env::current_exe().unwrap())
            .args(["--exact", CHILD_TEST, "--ignored", "--nocapture"])
            .env(CONFIG_ENV, &self.configuration[index])
            .env("RUST_TEST_THREADS", "1")
            .stdin(Stdio::null())
            .stdout(stdout)
            .stderr(stderr)
            .spawn()
            .unwrap();
        self.children[index] = Some(child);
    }
    fn wait_ready(&mut self, index: usize, binary: &str) -> ChildReady {
        let until = Instant::now() + Duration::from_secs(20);
        loop {
            let child = self.children[index].as_mut().unwrap();
            assert!(
                child.try_wait().unwrap().is_none(),
                "node{index} exited; retained private logs contain its failure"
            );
            if self.ready[index].exists() {
                let value: ChildReady =
                    serde_json::from_slice(&std::fs::read(&self.ready[index]).unwrap()).unwrap();
                assert_eq!(value.process_id, child.id());
                assert_eq!(value.binary_sha256, binary);
                return value;
            }
            assert!(
                Instant::now() < until,
                "node{index} did not become a provisional listener within20s"
            );
            std::thread::sleep(Duration::from_millis(10));
        }
    }
    fn hard_stop(&mut self, index: usize) {
        let mut child = self.children[index].take().unwrap();
        child.kill().unwrap();
        assert!(!child.wait().unwrap().success());
    }
    fn graceful_stop(&mut self) {
        for path in &self.stop {
            crate::storage::write_atomic(path, b"stop", true).unwrap();
        }
        let deadline = Instant::now() + Duration::from_secs(5);
        for (index, slot) in self.children.iter_mut().enumerate() {
            if let Some(child) = slot.as_mut() {
                loop {
                    if let Some(status) = child.try_wait().unwrap() {
                        assert!(status.success(), "node{index} did not stop cleanly");
                        break;
                    }
                    assert!(
                        Instant::now() < deadline,
                        "node{index} exceeded bounded shutdown"
                    );
                    std::thread::sleep(Duration::from_millis(10));
                }
            }
            *slot = None;
        }
    }
}
impl Drop for Processes {
    fn drop(&mut self) {
        for child in self.children.iter_mut().filter_map(Option::as_mut) {
            if child.try_wait().ok().flatten().is_none() {
                let _ = child.kill();
                let _ = child.wait();
            }
        }
        if std::thread::panicking() {
            if let Some(root) = self.root.take() {
                eprintln!(
                    "retained stopped private native process fixture: {}",
                    root.keep().display()
                );
            }
        }
    }
}

#[test]
fn native_initial_birth_process_four_independent_factories_and_cold_restart() {
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
        let configuration = root.join(format!("node{index}.json"));
        let ready = root.join(format!("node{index}.ready"));
        let stop = root.join(format!("node{index}.stop"));
        let config = ChildConfig {
            version: 1,
            index,
            database: root.join(format!("db{index}")),
            ready: ready.clone(),
            stop: stop.clone(),
            plan: plan.clone(),
            network: network.clone(),
        };
        crate::storage::write_atomic(&configuration, &serde_json::to_vec(&config).unwrap(), true)
            .unwrap();
        processes.configuration.push(configuration);
        processes.ready.push(ready);
        processes.stop.push(stop);
    }
    drop(ports);
    for index in 0..4 {
        processes.start(index);
    }
    let ready = (0..4)
        .map(|i| processes.wait_ready(i, &binary))
        .collect::<Vec<_>>();
    crate::storage::write_atomic(
        &processes
            .root
            .as_ref()
            .unwrap()
            .path()
            .join("initial-processes.json"),
        &serde_json::to_vec_pretty(&ready).unwrap(),
        true,
    )
    .unwrap();
    let pids = ready.iter().map(|r| r.process_id).collect::<BTreeSet<_>>();
    assert_eq!(pids.len(), 4);
    assert!(!pids.contains(&std::process::id()));
    let transport = TcpClusterRelocationTransport::new(
        plan.topology.cluster_id.clone(),
        node(0),
        Arc::new(RwLock::new(plan.topology.clone())),
        networks[0].clone(),
    )
    .unwrap();
    let observe = || {
        let parent = CancellationToken::uncancelable()
            .child_with_deadline(Instant::now() + Duration::from_secs(5));
        let offers = (0..4)
            .map(|i| {
                transport
                    .acquire_initial_birth_offer(&node(i), &plan, &parent)
                    .unwrap()
            })
            .collect();
        NativeInitialMetadataConfiguration::from_authenticated_offers(&plan, offers, &parent)
            .unwrap()
    };
    let before = observe();
    for (index, actual) in ready.iter().enumerate() {
        let birth = before.topology.nodes[&node(index)]
            .native_store_birth_v1
            .as_ref()
            .unwrap();
        assert_eq!(birth.birth_id(), actual.birth_id);
        assert_eq!(birth.birth_sha256(), actual.birth_sha256);
    }
    // RAM in one actual server process disappears. The host page cache remains;
    // this cannot establish disk power-loss durability or physical-host HA.
    processes.hard_stop(2);
    processes.start(2);
    let reopened = processes.wait_ready(2, &binary);
    assert_ne!(reopened.database_incarnation, ready[2].database_incarnation);
    assert_eq!(reopened.birth_id, ready[2].birth_id);
    assert_eq!(reopened.birth_sha256, ready[2].birth_sha256);
    crate::storage::write_atomic(
        &processes
            .root
            .as_ref()
            .unwrap()
            .path()
            .join("reopened-process.json"),
        &serde_json::to_vec_pretty(&reopened).unwrap(),
        true,
    )
    .unwrap();
    let after = observe();
    assert_eq!(before.topology, after.topology);
    for index in 0..4 {
        let db = processes
            .root
            .as_ref()
            .unwrap()
            .path()
            .join(format!("db{index}"));
        assert!(!db
            .join(crate::distribution::DEFAULT_CLUSTER_TOPOLOGY)
            .exists());
        assert!(!db
            .join(crate::distribution_consensus::DEFAULT_CLUSTER_METADATA_CONSENSUS_STATE)
            .exists());
    }
    processes.graceful_stop();
}
