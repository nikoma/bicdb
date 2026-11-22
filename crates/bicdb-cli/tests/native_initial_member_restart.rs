//! Restart of INITIAL members, each in its own process.
//!
//! Four `serve-fresh` hosts are brought to data serving through the operator
//! commands. A member is then ended with SIGKILL and started again with
//! `serve-reopen` on its own root, which rejoins metadata, enrolls a new
//! writer policy, opens data and recovers the ranges it leads by itself.
//!
//! Checked from outside, through the public event client only:
//! - a killed follower does not stop writes on its ranges, including while a
//!   client is appending at the moment of the kill;
//! - the restarted follower catches up and votes: with the range's OTHER
//!   follower killed afterwards, appends still succeed, which needs the
//!   restarted member's durable acknowledgement of every one of them;
//! - a killed range leader makes its range refuse appends until it is back
//!   (no other member takes a range over), ranges led by survivors keep
//!   accepting appends, and the restarted leader accepts appends again;
//! - a killed metadata leader is replaced by election and rejoins as a
//!   follower;
//! - every acknowledged append is readable at the end with its full value.
use bicdb_core::{
    cluster_event_record_id, ClusterEventClient, ClusterId, ClusterNetworkTransportConfig,
    ClusterNode, ClusterNodeId, ClusterTopology, CollectionMode, DistributionConfig,
    NativeMetadataBootstrapPlan, NativeMetadataCollection, RangeId, ReplicationTlsConfig,
    TcpClusterBootstrapClient, CLUSTER_EVENTS_COLLECTION,
};
use rcgen::{
    BasicConstraints, CertificateParams, ExtendedKeyUsagePurpose, IsCa, KeyPair, KeyUsagePurpose,
};
use serde_json::Value;
use std::net::TcpListener;
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

const MEMBERS: usize = 4;
const HOST_RUN_SECONDS: u64 = 900;
const POLL: Duration = Duration::from_millis(100);
/// How long a restarted member may take until it serves data again.
const RESTART_TIMEOUT: Duration = Duration::from_secs(120);
/// How long appends on a range with a surviving quorum may be refused in a
/// row after a member was killed. Not a product promise: wide enough for a
/// busy test host, and it fails when the range stays unavailable.
const MAX_STALL: Duration = Duration::from_secs(20);

/// How often the range leader is killed while a client appends to its range.
const LEADER_KILL_ROUNDS: usize = 3;
/// How long the range may refuse appends after its killed leader is back.
const LEADER_RECOVERY_PATIENCE: Duration = Duration::from_secs(60);

fn node(i: usize) -> ClusterNodeId {
    ClusterNodeId::new(format!("n{i}")).unwrap()
}
fn binary() -> Command {
    Command::new(env!("CARGO_BIN_EXE_bicdb"))
}
fn stamp(started: Instant) -> String {
    format!("[+{:8.3}s]", started.elapsed().as_secs_f64())
}

struct Cluster {
    /// `None` once retained for diagnosis.
    directory: Option<tempfile::TempDir>,
    root: PathBuf,
    children: Vec<(usize, Child)>,
    sequences: [u64; MEMBERS],
    generations: [u32; MEMBERS],
    addresses: Vec<String>,
    config: DistributionConfig,
    plan: PathBuf,
    started: Instant,
}
impl Drop for Cluster {
    fn drop(&mut self) {
        for (i, child) in &mut self.children {
            let _ = child.kill();
            let waited = child.wait();
            eprintln!(
                "RESTART_M teardown node{i} pid={} exit={waited:?}",
                child.id()
            );
        }
        if std::thread::panicking() {
            for i in 0..MEMBERS {
                match std::fs::read_to_string(self.paths(i).1) {
                    Ok(text) => eprintln!(
                        "RESTART_M FAILURE node{i} last status file:\n{}",
                        text.chars().take(12_000).collect::<String>()
                    ),
                    Err(error) => eprintln!("RESTART_M FAILURE node{i} status unreadable: {error}"),
                }
                for generation in 0..=self.generations[i] {
                    let log = self.root.join(format!("node{i}.g{generation}.stderr"));
                    if let Ok(text) = std::fs::read_to_string(&log) {
                        let lines = text.lines().collect::<Vec<_>>();
                        let tail = &lines[lines.len().saturating_sub(40)..];
                        eprintln!(
                            "RESTART_M FAILURE node{i} generation {generation} stderr tail ({} lines):\n{}",
                            lines.len(),
                            tail.join("\n")
                        );
                    }
                }
            }
            if let Some(directory) = self.directory.take() {
                eprintln!("RESTART_M FAILURE retained {}", directory.keep().display());
            }
        }
    }
}
impl Cluster {
    fn paths(&self, i: usize) -> (PathBuf, PathBuf) {
        (
            self.root.join(format!("node{i}.command")),
            self.root.join(format!("node{i}.status")),
        )
    }
    fn network(&self, i: usize) -> ClusterNetworkTransportConfig {
        ClusterNetworkTransportConfig {
            tls: Some(ReplicationTlsConfig {
                cert_path: self.root.join(format!("n{i}.pem")),
                key_path: self.root.join(format!("n{i}.key")),
                ca_path: self.root.join("ca.pem"),
                require_client_cert: true,
                dev_localhost_plaintext: false,
            }),
            dev_localhost_plaintext: false,
            max_frame_bytes: 4 * 1024 * 1024,
            ..Default::default()
        }
    }
    /// `serve-fresh` or `serve-reopen` of member `i` on its own root.
    fn spawn(&mut self, i: usize, subcommand: &str) -> u32 {
        let (command, status) = self.paths(i);
        let mut process = binary();
        process
            .args(["cluster", "native-metadata", subcommand])
            .arg(self.root.join(format!("db{i}")))
            .arg("--plan")
            .arg(&self.plan)
            .arg("--node-id")
            .arg(node(i).as_str())
            .arg("--address")
            .arg(&self.addresses[i])
            .arg("--command-file")
            .arg(command)
            .arg("--status-file")
            .arg(status)
            .arg("--run-seconds")
            .arg(HOST_RUN_SECONDS.to_string())
            .arg("--cluster-tls-cert")
            .arg(self.root.join(format!("n{i}.pem")))
            .arg("--cluster-tls-key")
            .arg(self.root.join(format!("n{i}.key")))
            .arg("--cluster-tls-ca")
            .arg(self.root.join("ca.pem"));
        let generation = self.generations[i];
        process
            .stdin(Stdio::null())
            .stdout(
                std::fs::File::create(self.root.join(format!("node{i}.g{generation}.stdout")))
                    .unwrap(),
            )
            .stderr(
                std::fs::File::create(self.root.join(format!("node{i}.g{generation}.stderr")))
                    .unwrap(),
            );
        let child = process.spawn().unwrap();
        let pid = child.id();
        println!(
            "{} RESTART_M spawned node{i} `{subcommand}` pid={pid}",
            stamp(self.started)
        );
        self.children.push((i, child));
        pid
    }
    fn require_alive(&mut self, during: &str) {
        for (i, child) in &mut self.children {
            if let Some(exit) = child.try_wait().unwrap() {
                panic!(
                    "node{i} (pid {}) exited ({exit}) while {during}",
                    child.id()
                );
            }
        }
    }
    fn status(&self, i: usize) -> Option<Value> {
        serde_json::from_slice(&std::fs::read(self.paths(i).1).ok()?).ok()
    }
    fn wait<T>(
        &mut self,
        what: &str,
        timeout: Duration,
        mut probe: impl FnMut(&mut Self) -> Result<T, String>,
    ) -> T {
        let begun = Instant::now();
        loop {
            self.require_alive(what);
            let seen = match probe(self) {
                Ok(value) => return value,
                Err(seen) => seen,
            };
            assert!(
                begun.elapsed() < timeout,
                "timed out after {timeout:?} waiting for {what}; last observation: {seen}"
            );
            std::thread::sleep(POLL);
        }
    }
    /// One `step` against member `i`; returns the host's reply to it.
    fn step(&mut self, i: usize, arguments: &[&str]) -> (bool, Value) {
        self.sequences[i] += 1;
        let sequence = self.sequences[i];
        let (command, status) = self.paths(i);
        let output = binary()
            .args(["cluster", "native-metadata", "step", "--command-file"])
            .arg(&command)
            .arg("--status-file")
            .arg(&status)
            .arg("--sequence")
            .arg(sequence.to_string())
            .args(arguments)
            .output()
            .unwrap();
        let what = format!("node{i} to answer step {arguments:?} (sequence {sequence})");
        let status = self.wait(&what, Duration::from_secs(10), |cluster| {
            match cluster.status(i) {
                Some(value) if value["sequence"].as_u64() == Some(sequence) => Ok(value),
                other => Err(format!(
                    "{:?}",
                    other.map(|value| value["sequence"].clone())
                )),
            }
        });
        (output.status.success(), status)
    }
    fn accepted(&mut self, i: usize, action: &str) -> Value {
        let (ok, status) = self.step(i, &["--action", action]);
        assert!(
            ok && status["error"].is_null(),
            "node{i} refused {action}: {}",
            status["error"]
        );
        status
    }
    fn topology(&self, via: usize) -> Result<ClusterTopology, String> {
        TcpClusterBootstrapClient::new(self.config.cluster_id.clone(), node(0), self.network(0))
            .and_then(|client| client.fetch_snapshot(&node(via), &self.addresses[via]))
            .map(|snapshot| snapshot.topology)
            .map_err(|error| error.to_string())
    }
    /// The metadata leader as the live members' own status files name it.
    fn metadata_leader(&self, among: &[usize]) -> Option<usize> {
        among.iter().copied().find(|&i| {
            self.status(i).is_some_and(|status| {
                status["host"]["metadata"]["role"] == "leader"
                    && status["host"]["metadata_leader"] == node(i).as_str()
            })
        })
    }
    /// SIGKILL: no stop action, no drain, nothing flushed by the host.
    fn kill(&mut self, i: usize) {
        let at = self
            .children
            .iter()
            .position(|(member, _)| *member == i)
            .unwrap_or_else(|| panic!("node{i} is not running"));
        let (_, mut child) = self.children.remove(at);
        assert!(
            child.try_wait().unwrap().is_none(),
            "node{i} had already exited before the kill"
        );
        child.kill().unwrap();
        let exit = child.wait().unwrap();
        println!(
            "{} RESTART_M killed node{i} pid={} ({exit})",
            stamp(self.started),
            child.id()
        );
    }
    /// `serve-reopen` on the member's own root. The operator sends nothing.
    fn respawn(&mut self, i: usize) -> u32 {
        let (command, status) = self.paths(i);
        // The dead process's operator files are stale: the host refuses them.
        let _ = std::fs::remove_file(&command);
        std::fs::remove_file(&status).unwrap();
        self.sequences[i] = 0;
        self.generations[i] += 1;
        self.spawn(i, "serve-reopen")
    }
    /// Wait until the NEW process of member `i` has opened DATA: it votes in
    /// metadata and answers as a replica. `serving` additionally requires
    /// that every range it leads has been recovered.
    fn wait_reopened(&mut self, i: usize, pid: u32, serving: bool) -> Value {
        let what = format!(
            "restarted node{i} (pid {pid}) to {}",
            if serving { "serve data" } else { "open data" }
        );
        self.wait(&what, RESTART_TIMEOUT, |cluster| match cluster.status(i) {
            Some(status)
                if status["process_id"] == pid
                    && status["host"]["phase"] == "initial_data"
                    && status["host"]["initial_data"]["dispatcher_published"] == true
                    && (!serving || status["host"]["data_serving"] == true) =>
            {
                Ok(status)
            }
            other => Err(other
                .map(|status| {
                    format!(
                        "phase={} data_serving={} error={} initial_data={}",
                        status["host"]["phase"],
                        status["host"]["data_serving"],
                        status["error"],
                        status["host"]["initial_data"]
                    )
                })
                .unwrap_or_else(|| "no status file yet".into())),
        })
    }
    /// `serve-reopen`, then wait until the new process serves data again.
    fn restart(&mut self, i: usize) -> Duration {
        let begun = Instant::now();
        let pid = self.respawn(i);
        self.wait_reopened(i, pid, true);
        println!(
            "{} RESTART_M node{i} serves data again {:.1}s after its restart began",
            stamp(self.started),
            begun.elapsed().as_secs_f64()
        );
        begun.elapsed()
    }
}

fn start_four_initial_members() -> Cluster {
    let directory = tempfile::Builder::new()
        .prefix("native-range-move-restart-")
        .tempdir()
        .unwrap();
    let root = directory.path().to_path_buf();
    let ca_key = KeyPair::generate().unwrap();
    let mut ca_params = CertificateParams::default();
    ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    ca_params.key_usages = vec![
        KeyUsagePurpose::KeyCertSign,
        KeyUsagePurpose::DigitalSignature,
    ];
    let ca = ca_params.self_signed(&ca_key).unwrap();
    std::fs::write(root.join("ca.pem"), ca.pem()).unwrap();
    let listeners = (0..MEMBERS)
        .map(|_| TcpListener::bind("127.0.0.1:0").unwrap())
        .collect::<Vec<_>>();
    let addresses = listeners
        .iter()
        .map(|listener| listener.local_addr().unwrap().to_string())
        .collect::<Vec<_>>();
    let mut members = Vec::new();
    for i in 0..MEMBERS {
        let key = KeyPair::generate().unwrap();
        let mut params =
            CertificateParams::new(vec!["localhost".into(), "127.0.0.1".into()]).unwrap();
        params.extended_key_usages = vec![
            ExtendedKeyUsagePurpose::ServerAuth,
            ExtendedKeyUsagePurpose::ClientAuth,
        ];
        params.key_usages = vec![KeyUsagePurpose::DigitalSignature];
        let certificate = params.signed_by(&key, &ca, &ca_key).unwrap();
        let path = root.join(format!("n{i}.pem"));
        std::fs::write(&path, certificate.pem()).unwrap();
        std::fs::write(root.join(format!("n{i}.key")), key.serialize_pem()).unwrap();
        members.push(
            ClusterNode::new(node(i), addresses[i].clone(), 1, 1 << 30, 10)
                .unwrap()
                .with_tls_certificate_sha256(
                    bicdb_core::replication_transport::replication_certificate_sha256(&path)
                        .unwrap(),
                )
                .unwrap(),
        );
    }
    let mut config = DistributionConfig {
        enabled: true,
        cluster_id: ClusterId::new(format!("native-member-restart-{}", std::process::id()))
            .unwrap(),
        range_image_bootstrap_id: Some(uuid::Uuid::now_v7()),
        node_id: node(0),
        node_address: addresses[0].clone(),
        node_tls_certificate_sha256: members[0].tls_certificate_sha256.clone(),
        node_capacity_bytes: 1 << 30,
        replication_factor: 3,
        initial_ranges: 4,
        ..Default::default()
    };
    // The plan's TLS paths are placeholders: every process uses its own.
    config.transport.dev_localhost_plaintext = false;
    config.transport.tls = Some(ReplicationTlsConfig {
        cert_path: PathBuf::from("/seed-vm-only/seed.crt"),
        key_path: PathBuf::from("/seed-vm-only/seed.key"),
        ca_path: PathBuf::from("/seed-vm-only/ca.crt"),
        require_client_cert: true,
        dev_localhost_plaintext: false,
    });
    let plan = NativeMetadataBootstrapPlan {
        version: 1,
        configuration: config.clone(),
        members,
        created_at_ms: 10,
        collections: vec![
            NativeMetadataCollection {
                name: "items".into(),
                mode: CollectionMode::Standard,
            },
            NativeMetadataCollection {
                name: CLUSTER_EVENTS_COLLECTION.into(),
                mode: CollectionMode::Standard,
            },
        ],
    };
    let plan_path = root.join("common-plan.json");
    std::fs::write(&plan_path, serde_json::to_vec(&plan).unwrap()).unwrap();
    drop(listeners);
    let mut cluster = Cluster {
        directory: Some(directory),
        root,
        children: Vec::new(),
        sequences: [0; MEMBERS],
        generations: [0; MEMBERS],
        addresses,
        config,
        plan: plan_path,
        started: Instant::now(),
    };
    for i in 0..MEMBERS {
        cluster.spawn(i, "serve-fresh");
    }
    for i in 0..MEMBERS {
        let what = format!("node{i} to publish its first status");
        cluster.wait(&what, Duration::from_secs(60), |cluster| {
            match cluster.status(i) {
                Some(status) if status["host"]["phase"] == "provisional" => Ok(()),
                other => Err(format!("{:?}", other.map(|s| s["host"]["phase"].clone()))),
            }
        });
    }
    for action in ["prepare", "launch"] {
        for i in 0..MEMBERS {
            cluster.accepted(i, action);
        }
    }
    cluster.accepted(0, "elect");
    for i in 0..MEMBERS {
        cluster.accepted(i, "enable-control");
    }
    // `open-initial-data` until all four serve: a member refuses while its
    // peers' receivers are still closed, and that refusal is repeated.
    let begun = Instant::now();
    let mut ready = [false; MEMBERS];
    let mut last = vec![String::new(); MEMBERS];
    while !ready.iter().all(|ready| *ready) {
        for i in 0..MEMBERS {
            if ready[i] {
                continue;
            }
            let (_, status) = cluster.step(
                i,
                &["--action", "open-initial-data", "--timeout-ms", "1500"],
            );
            ready[i] = status["error"].is_null() && status["host"]["data_serving"] == true;
            last[i] = status["error"].to_string();
        }
        assert!(
            begun.elapsed() < Duration::from_secs(60),
            "the four initial members do not serve data: ready={ready:?} last errors={last:?}"
        );
    }
    println!(
        "{} RESTART_M four initial members serve data",
        stamp(cluster.started)
    );
    cluster
}

fn range_of(topology: &ClusterTopology, key: &str) -> RangeId {
    topology
        .range_for_key(
            CLUSTER_EVENTS_COLLECTION,
            &cluster_event_record_id("items", key).unwrap(),
        )
        .unwrap()
        .id
}
fn row_value(key: &str) -> Value {
    serde_json::json!({"key": key, "payload": {"nested": [1, null, {"kept": true}]}})
}
fn member_of(id: &ClusterNodeId) -> usize {
    (0..MEMBERS).find(|&i| &node(i) == id).unwrap()
}

/// One append attempt of the background writer.
struct Attempt {
    at: f64,
    key: String,
    ok: bool,
    error: String,
}
/// A client that appends new keys of ONE range, one after another, for as
/// long as it runs. Every acknowledged key is recorded.
struct Writer {
    stop: Arc<AtomicBool>,
    acknowledged: Arc<AtomicUsize>,
    worker: Option<std::thread::JoinHandle<Vec<Attempt>>>,
}
impl Writer {
    fn start(
        config: DistributionConfig,
        topology: ClusterTopology,
        range: RangeId,
        prefix: &'static str,
        started: Instant,
    ) -> Self {
        let stop = Arc::new(AtomicBool::new(false));
        let acknowledged = Arc::new(AtomicUsize::new(0));
        let (stopped, count) = (Arc::clone(&stop), Arc::clone(&acknowledged));
        let worker = std::thread::spawn(move || {
            let gateway = ClusterEventClient::new(&config, topology.clone())
                .unwrap()
                .with_request_timeout(Duration::from_secs(2))
                .unwrap();
            let mut attempts = Vec::new();
            let mut candidate = 0u64;
            while !stopped.load(Ordering::SeqCst) {
                let key = loop {
                    let key = format!("{prefix}-{candidate}");
                    candidate += 1;
                    if range_of(&topology, &key) == range {
                        break key;
                    }
                };
                let at = started.elapsed().as_secs_f64();
                let write = gateway.append("items", &key, row_value(&key));
                let ok = write.is_ok();
                attempts.push(Attempt {
                    at,
                    key,
                    ok,
                    error: write.err().map(|e| e.to_string()).unwrap_or_default(),
                });
                if ok {
                    count.fetch_add(1, Ordering::SeqCst);
                    std::thread::sleep(Duration::from_millis(20));
                } else {
                    let _ = gateway.refresh();
                    std::thread::sleep(Duration::from_millis(200));
                }
            }
            attempts
        });
        Self {
            stop,
            acknowledged,
            worker: Some(worker),
        }
    }
    fn acknowledged(&self) -> usize {
        self.acknowledged.load(Ordering::SeqCst)
    }
    fn finish(mut self) -> Vec<Attempt> {
        self.stop.store(true, Ordering::SeqCst);
        self.worker.take().unwrap().join().unwrap()
    }
}
impl Drop for Writer {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::SeqCst);
        if let Some(worker) = self.worker.take() {
            let _ = worker.join();
        }
    }
}
/// The longest run of failed attempts: from the first failed attempt to the
/// next acknowledged one (or to the last attempt).
fn longest_stall(attempts: &[Attempt]) -> f64 {
    let (mut longest, mut since) = (0f64, None::<f64>);
    for attempt in attempts {
        match (attempt.ok, since) {
            (false, None) => since = Some(attempt.at),
            (true, Some(begun)) => {
                longest = longest.max(attempt.at - begun);
                since = None;
            }
            _ => {}
        }
    }
    if let (Some(begun), Some(last)) = (since, attempts.last()) {
        longest = longest.max(last.at - begun);
    }
    longest
}
/// Wait until the writer got `more` further acknowledgements.
fn progress(cluster: &mut Cluster, writer: &Writer, more: usize, what: &str) {
    let target = writer.acknowledged() + more;
    cluster.wait(what, MAX_STALL, |_| {
        let now = writer.acknowledged();
        if now >= target {
            Ok(())
        } else {
            Err(format!("{now} of {target} acknowledgements"))
        }
    });
}
fn gateway(cluster: &Cluster, via: usize) -> ClusterEventClient {
    let mut config = cluster.config.clone();
    config.transport = cluster.network(0);
    ClusterEventClient::new(&config, cluster.topology(via).unwrap())
        .unwrap()
        .with_request_timeout(Duration::from_secs(2))
        .unwrap()
}
fn append_within(gateway: &ClusterEventClient, key: &str, timeout: Duration) -> Result<(), String> {
    let begun = Instant::now();
    loop {
        match gateway.append("items", key, row_value(key)) {
            Ok(_) => return Ok(()),
            Err(error) if begun.elapsed() >= timeout => return Err(error.to_string()),
            Err(_) => {
                let _ = gateway.refresh();
                std::thread::sleep(Duration::from_millis(200));
            }
        }
    }
}
/// Strong reads that had to be repeated, with the refusal that caused it.
static REPEATED_READS: Mutex<Vec<String>> = Mutex::new(Vec::new());
fn read_within(
    gateway: &ClusterEventClient,
    key: &str,
    timeout: Duration,
) -> Result<Option<Value>, String> {
    let begun = Instant::now();
    // Every refusal of this read, by its text without the per-attempt
    // holder ages, so a give-up names what kept refusing it.
    let mut refusals = std::collections::BTreeMap::<String, usize>::new();
    loop {
        match gateway.get_strong("items", key) {
            Ok(value) => return Ok(value),
            Err(error) if begun.elapsed() >= timeout => {
                *refusals
                    .entry(refusal_kind(&error.to_string()))
                    .or_default() += 1;
                return Err(format!(
                    "{error} (Strong read of {key} refused for {:.1}s: {})",
                    begun.elapsed().as_secs_f64(),
                    refusals
                        .iter()
                        .map(|(kind, n)| format!("{n}x {kind}"))
                        .collect::<Vec<_>>()
                        .join(" | ")
                ));
            }
            Err(error) => {
                *refusals
                    .entry(refusal_kind(&error.to_string()))
                    .or_default() += 1;
                let mut repeated = REPEATED_READS.lock().unwrap();
                if repeated.len() < 12 {
                    println!(
                        "RESTART_M Strong read of {key} repeated after {:.2}s: {}",
                        begun.elapsed().as_secs_f64(),
                        error.to_string().chars().take(4000).collect::<String>()
                    );
                }
                repeated.push(format!("{key}: {error}"));
                drop(repeated);
                let _ = gateway.refresh();
                std::thread::sleep(Duration::from_millis(200));
            }
        }
    }
}
/// A refusal with its numbers (holder ages, step times, slices) elided, so
/// refusals of one kind group together. Names such as `n2` or `r1` stay.
fn refusal_kind(error: &str) -> String {
    let mut kind = String::new();
    // Inside a digit run: whether it is kept (part of a name).
    let mut run: Option<bool> = None;
    let mut previous = ' ';
    for c in error.chars() {
        if c.is_ascii_digit() {
            let keep = *run.get_or_insert(previous.is_ascii_alphabetic());
            if keep {
                kind.push(c);
            } else if previous.is_ascii_digit() {
                // Elided run continues.
            } else {
                kind.push('#');
            }
        } else {
            run = None;
            kind.push(c);
        }
        previous = c;
    }
    kind.chars().take(4000).collect()
}
/// Next unused key of `prefix` that falls into `range`.
fn key_in(topology: &ClusterTopology, range: RangeId, prefix: &str, counter: &mut u64) -> String {
    loop {
        let key = format!("{prefix}-{counter}");
        *counter += 1;
        if range_of(topology, &key) == range {
            return key;
        }
    }
}

#[test]
fn native_initial_member_restart_four_process_kill_and_reopen() {
    let mut cluster = start_four_initial_members();
    let all = [0, 1, 2, 3];
    let topology = cluster.topology(0).unwrap();
    let metadata_leader = cluster.wait(
        "a metadata leader in the status files",
        Duration::from_secs(30),
        |cluster| {
            cluster
                .metadata_leader(&all)
                .ok_or_else(|| "none".to_string())
        },
    );
    // A range led by a member that does not lead metadata, with a follower
    // that does not lead metadata either: the three roles are killed apart.
    let (range, leader, first, second) = topology
        .ranges
        .values()
        .find_map(|range| {
            let leader = member_of(&range.leader);
            let followers = range
                .replicas
                .iter()
                .map(|replica| member_of(&replica.node_id))
                .filter(|&i| i != leader)
                .collect::<Vec<_>>();
            let first = *followers.iter().find(|&&i| i != metadata_leader)?;
            let second = *followers.iter().find(|&&i| i != first)?;
            (leader != metadata_leader).then_some((range.id, leader, first, second))
        })
        .expect("a range led by a member that does not lead metadata");
    println!(
        "{} RESTART_M range {range}: leader n{leader}, followers n{first} and n{second}; metadata leader n{metadata_leader}",
        stamp(cluster.started)
    );
    let mut client_config = cluster.config.clone();
    client_config.transport = cluster.network(0);
    let direct = gateway(&cluster, leader);
    let mut counter = 0u64;
    let mut acknowledged = Vec::new();
    for every in topology.ranges.values() {
        for _ in 0..2 {
            let key = key_in(&topology, every.id, "before", &mut counter);
            append_within(&direct, &key, Duration::from_secs(30)).unwrap();
            acknowledged.push(key);
        }
    }

    let begun = Instant::now();
    for key in &acknowledged {
        assert_eq!(
            read_within(&direct, key, MAX_STALL).unwrap(),
            Some(row_value(key))
        );
    }
    println!(
        "{} RESTART_M before any kill: {} Strong reads took {:.2}s",
        stamp(cluster.started),
        acknowledged.len(),
        begun.elapsed().as_secs_f64()
    );

    // 1. A follower is killed while a client appends to its range.
    let writer = Writer::start(
        client_config.clone(),
        topology.clone(),
        range,
        "under-follower-kill",
        cluster.started,
    );
    progress(&mut cluster, &writer, 5, "appends before any kill");
    cluster.kill(first);
    progress(
        &mut cluster,
        &writer,
        10,
        "appends with one follower killed",
    );
    println!(
        "{} RESTART_M appends continue with follower n{first} killed",
        stamp(cluster.started)
    );
    cluster.restart(first);
    progress(
        &mut cluster,
        &writer,
        10,
        "appends after the follower returned",
    );
    // 2. The OTHER follower is killed: leader + restarted member must be
    // the quorum of every further append.
    cluster.kill(second);
    progress(
        &mut cluster,
        &writer,
        10,
        "appends on the quorum of the leader and the restarted follower",
    );
    println!(
        "{} RESTART_M leader n{leader} and restarted n{first} carry the quorum with n{second} killed",
        stamp(cluster.started)
    );
    cluster.restart(second);
    progress(
        &mut cluster,
        &writer,
        5,
        "appends after the second follower returned",
    );
    let attempts = writer.finish();
    let stall = longest_stall(&attempts);
    let (ok, failed) = (
        attempts.iter().filter(|a| a.ok).count(),
        attempts.iter().filter(|a| !a.ok).count(),
    );
    println!(
        "{} RESTART_M follower kills: {ok} appends acknowledged, {failed} refused, longest refused run {stall:.1}s",
        stamp(cluster.started)
    );
    for attempt in attempts.iter().filter(|a| !a.ok).take(8) {
        println!(
            "RESTART_M refused append at +{:.3}s {}: {}",
            attempt.at, attempt.key, attempt.error
        );
    }
    assert!(
        stall <= MAX_STALL.as_secs_f64(),
        "appends were refused for {stall:.1}s in a row with a surviving quorum"
    );
    acknowledged.extend(attempts.into_iter().filter(|a| a.ok).map(|a| a.key));

    // 3. The range leader is killed (no append in flight).
    let surviving = topology
        .ranges
        .values()
        .find(|other| member_of(&other.leader) != leader)
        .unwrap()
        .id;
    cluster.kill(leader);
    let elsewhere = gateway(&cluster, metadata_leader);
    let key = key_in(&topology, surviving, "leader-down", &mut counter);
    append_within(&elsewhere, &key, MAX_STALL)
        .unwrap_or_else(|error| panic!("append on a range led by a survivor failed: {error}"));
    acknowledged.push(key);
    let refused = key_in(&topology, range, "leader-down", &mut counter);
    let leaderless = append_within(&elsewhere, &refused, Duration::from_secs(5));
    println!(
        "{} RESTART_M append on range {range} with its leader n{leader} killed: {leaderless:?}",
        stamp(cluster.started)
    );
    assert!(
        leaderless.is_err(),
        "a range whose leader is down must not acknowledge an append"
    );
    cluster.restart(leader);
    for _ in 0..3 {
        let key = key_in(&topology, range, "leader-back", &mut counter);
        append_within(&elsewhere, &key, MAX_STALL)
            .unwrap_or_else(|error| panic!("append after the leader returned failed: {error}"));
        acknowledged.push(key);
    }
    println!(
        "{} RESTART_M restarted leader n{leader} accepts appends on range {range} again",
        stamp(cluster.started)
    );

    // 4. The metadata leader is killed.
    let metadata_leader = cluster.wait(
        "the current metadata leader",
        Duration::from_secs(30),
        |cluster| {
            cluster
                .metadata_leader(&all)
                .ok_or_else(|| "none".to_string())
        },
    );
    let survivors = all
        .iter()
        .copied()
        .filter(|&i| i != metadata_leader)
        .collect::<Vec<_>>();
    let term_before = cluster.status(metadata_leader).unwrap()["host"]["metadata"]["current_term"]
        .as_u64()
        .unwrap();
    cluster.kill(metadata_leader);
    let successor = cluster.wait(
        "the three survivors to elect a metadata leader",
        MAX_STALL,
        |cluster| {
            cluster
                .metadata_leader(&survivors)
                .filter(|&i| {
                    cluster.status(i).unwrap()["host"]["metadata"]["current_term"]
                        .as_u64()
                        .is_some_and(|term| term > term_before)
                })
                .ok_or_else(|| "none".to_string())
        },
    );
    println!(
        "{} RESTART_M metadata leader n{metadata_leader} killed; n{successor} elected",
        stamp(cluster.started)
    );
    let after = gateway(&cluster, successor);
    let led_by_survivor = topology
        .ranges
        .values()
        .find(|other| member_of(&other.leader) != metadata_leader)
        .unwrap()
        .id;
    let key = key_in(
        &topology,
        led_by_survivor,
        "metadata-leader-down",
        &mut counter,
    );
    append_within(&after, &key, MAX_STALL)
        .unwrap_or_else(|error| panic!("append under the new metadata leader failed: {error}"));
    acknowledged.push(key);
    cluster.restart(metadata_leader);
    cluster.wait(
        "the restarted member to follow the new metadata term",
        MAX_STALL,
        |cluster| match cluster.status(metadata_leader) {
            Some(status)
                if status["host"]["metadata"]["current_term"]
                    .as_u64()
                    .is_some_and(|term| term > term_before)
                    && !status["host"]["metadata_leader"].is_null() =>
            {
                Ok(())
            }
            other => Err(format!(
                "{:?}",
                other.map(|s| s["host"]["metadata"].clone())
            )),
        },
    );

    println!(
        "{} RESTART_M restarted n{metadata_leader} follows the new metadata term",
        stamp(cluster.started)
    );

    // 5. All four members are killed at once (nothing in flight) and all
    // four are started again: nobody of the earlier processes is left.
    for every in topology.ranges.values() {
        let key = key_in(&topology, every.id, "before-all-down", &mut counter);
        append_within(&after, &key, MAX_STALL).unwrap_or_else(|error| {
            panic!(
                "append on {} before the whole-cluster kill failed: {error}",
                every.id
            )
        });
        acknowledged.push(key);
    }
    drop(after);
    for i in all {
        cluster.kill(i);
    }
    let begun = Instant::now();
    let pids = all.map(|i| cluster.respawn(i));
    for i in all {
        cluster.wait_reopened(i, pids[i], true);
    }
    println!(
        "{} RESTART_M all four members serve data again {:.1}s after the whole cluster was killed",
        stamp(cluster.started),
        begun.elapsed().as_secs_f64()
    );
    let successor = cluster.wait(
        "a metadata leader after the whole-cluster restart",
        MAX_STALL,
        |cluster| {
            cluster
                .metadata_leader(&all)
                .ok_or_else(|| "none".to_string())
        },
    );
    let after = gateway(&cluster, successor);

    // 6. Everything is back: every range accepts appends, every member
    // serves, and every acknowledged row is readable with its full value.
    for every in topology.ranges.values() {
        let key = key_in(&topology, every.id, "after", &mut counter);
        append_within(&after, &key, MAX_STALL).unwrap_or_else(|error| {
            panic!("append on {} after all restarts failed: {error}", every.id)
        });
        acknowledged.push(key);
    }
    for i in all {
        let status = cluster.status(i).unwrap();
        assert_eq!(status["host"]["data_serving"], true, "node{i}: {status}");
    }
    // No restart changed who owns or leads a range, or its epoch.
    let finally = cluster.topology(successor).unwrap();
    for every in topology.ranges.values() {
        let now = finally.range_by_id(every.id).unwrap();
        assert_eq!(
            (now.epoch, &now.leader, &now.replicas),
            (every.epoch, &every.leader, &every.replicas),
            "range {} changed across the restarts",
            every.id
        );
    }
    println!(
        "{} RESTART_M every range accepts appends after all restarts",
        stamp(cluster.started)
    );
    let mut slowest = (Duration::ZERO, String::new());
    let reads_begun = Instant::now();
    for key in &acknowledged {
        let begun = Instant::now();
        assert_eq!(
            read_within(&after, key, MAX_STALL).unwrap(),
            Some(row_value(key)),
            "acknowledged row {key} must be readable"
        );
        if begun.elapsed() > slowest.0 {
            slowest = (begun.elapsed(), key.clone());
        }
    }
    {
        let repeated = REPEATED_READS.lock().unwrap();
        println!(
            "{} RESTART_M Strong reads that had to be repeated: {}",
            stamp(cluster.started),
            repeated.len()
        );
    }
    println!(
        "{} RESTART_M all {} acknowledged rows are readable after the kills and restarts ({:.2}s in all, slowest read {:.2}s, {})",
        stamp(cluster.started),
        acknowledged.len(),
        reads_begun.elapsed().as_secs_f64(),
        slowest.0.as_secs_f64(),
        slowest.1
    );
    cluster.require_alive("finishing");
}

/// The leader of a range is killed WHILE a client appends to that range, so
/// a write is usually in flight at the kill.
///
/// What must hold in every case: the restarted member opens DATA again (it
/// votes in metadata and answers as a replica), ranges led by other members
/// keep accepting appends, an append acknowledged before the kill is never
/// lost, and nothing unacknowledged is reported as acknowledged.
///
/// A command the killed leader's own log certifies committed (raw entry
/// QuorumCommitted/Applied just above the native prefix) is recovered forward
/// by the hot recovery, and the range accepts appends again.
///
/// What is NOT promised yet: recovery of a tail WITHOUT that certificate in
/// the leader's own log (prepared only). It needs the native abort or a
/// recovery sourced from a follower, which no host drives after a restart.
/// The range then stays refusing appends and the member says so in its
/// status. The test accepts exactly these outcomes and prints which one
/// occurred in each round.
/// The member's own diagnostic names a first unresolved entry that its log
/// certifies committed.
fn committed_tail(attempt: &Value) -> bool {
    attempt["unrecovered_ranges"]
        .as_array()
        .is_some_and(|ranges| {
            ranges.iter().any(|range| {
                matches!(
                    range["first_unresolved_state"].as_str(),
                    Some("QuorumCommitted" | "Applied")
                ) && range["native_pending_command_index"].is_null()
                    && range["native_materialized_pending_index"].is_null()
                    && range["native_prefix_invalidated"] == false
            })
        })
}
#[test]
fn native_initial_member_restart_four_process_leader_killed_under_writes() {
    let mut cluster = start_four_initial_members();
    let all = [0, 1, 2, 3];
    let topology = cluster.topology(0).unwrap();
    let metadata_leader = cluster.wait(
        "a metadata leader in the status files",
        Duration::from_secs(30),
        |cluster| {
            cluster
                .metadata_leader(&all)
                .ok_or_else(|| "none".to_string())
        },
    );
    let mut client_config = cluster.config.clone();
    client_config.transport = cluster.network(0);
    let mut acknowledged = Vec::new();
    let mut unknown = Vec::new();
    let mut counter = 0u64;
    let (mut recovered_rounds, mut refused_rounds) = (0, 0);
    let mut unrecovered = Vec::new();
    // One round per range leader that does not lead metadata.
    let led = topology
        .ranges
        .values()
        .filter(|range| member_of(&range.leader) != metadata_leader)
        .cloned()
        .collect::<Vec<_>>();
    assert!(!led.is_empty());
    for (round, led) in led.iter().take(LEADER_KILL_ROUNDS).enumerate() {
        let leader = member_of(&led.leader);
        let prefix: &'static str = ["leader-kill-a", "leader-kill-b", "leader-kill-c"][round];
        let writer = Writer::start(
            client_config.clone(),
            topology.clone(),
            led.id,
            prefix,
            cluster.started,
        );
        progress(
            &mut cluster,
            &writer,
            8,
            "appends before the leader is killed",
        );
        // Not aligned with the writer's pause: the kill lands at an
        // arbitrary point of an append more often than between two.
        std::thread::sleep(Duration::from_millis(7 + 13 * round as u64));
        cluster.kill(leader);
        let before = writer.acknowledged();
        std::thread::sleep(Duration::from_secs(2));
        assert_eq!(
            writer.acknowledged(),
            before,
            "an append was acknowledged on range {} while its leader was dead",
            led.id
        );
        let pid = cluster.respawn(leader);
        let status = cluster.wait_reopened(leader, pid, false);
        assert_eq!(status["host"]["initial_data"]["receiver_listening"], true);
        // A range led by another member, in which the restarted member
        // votes where possible: its replica role is back.
        let elsewhere = topology
            .ranges
            .values()
            .filter(|other| member_of(&other.leader) != leader && !unrecovered.contains(&other.id))
            .max_by_key(|other| {
                other
                    .replicas
                    .iter()
                    .any(|replica| member_of(&replica.node_id) == leader)
            })
            .unwrap()
            .id;
        let direct = gateway(&cluster, metadata_leader);
        for _ in 0..2 {
            let key = key_in(&topology, elsewhere, "elsewhere", &mut counter);
            append_within(&direct, &key, MAX_STALL).unwrap_or_else(|error| {
                panic!("append on range {elsewhere} led by a survivor failed: {error}")
            });
            acknowledged.push(key);
        }
        // The killed leader's own range: appends resume, or the member
        // reports why its leadership is not recovered.
        let begun = Instant::now();
        let (recovered, status) = loop {
            let status = cluster.status(leader).unwrap();
            if writer.acknowledged() > before + 3 {
                break (true, status);
            }
            let attempt = &status["host"]["initial_data"]["reopened_leader_recovery_last_attempt"];
            if begun.elapsed() > LEADER_RECOVERY_PATIENCE {
                break (false, status);
            }
            // A tail that is certified committed in the killed leader's own
            // log is recovered forward by the hot recovery: wait for it.
            if attempt["result"] == "refused_or_pending"
                && attempt["error"].as_str().is_some_and(|error| {
                    error.contains("native unresolved leader tail")
                        || error.contains("unfinished command")
                })
                && !committed_tail(attempt)
                && begun.elapsed() > Duration::from_secs(10)
            {
                break (false, status);
            }
            cluster.require_alive("waiting for appends after the leader restart");
            std::thread::sleep(POLL);
        };
        let attempts = writer.finish();
        let diagnostic =
            status["host"]["initial_data"]["reopened_leader_recovery_last_attempt"].clone();
        println!(
            "{} RESTART_M round {round}: leader n{leader} of range {} killed under appends; appends resumed={recovered}; data_serving={}; leader recovery: {diagnostic}; hot recovery: {}",
            stamp(cluster.started),
            led.id,
            status["host"]["data_serving"],
            status["host"]["initial_data"]["hot_recovery_last_attempt"]
        );
        for attempt in attempts.iter().filter(|a| !a.ok).take(3) {
            println!(
                "RESTART_M round {round} refused append at +{:.3}s {}: {}",
                attempt.at, attempt.key, attempt.error
            );
        }
        let mut range_rows = Vec::new();
        for attempt in attempts {
            if attempt.ok {
                range_rows.push(attempt.key);
            } else {
                unknown.push(attempt.key);
            }
        }
        if recovered {
            recovered_rounds += 1;
            // The status file is rewritten every half second: it may still
            // show the state before the recovery that the appends observed.
            cluster.wait(
                "the restarted leader's status to report data serving",
                Duration::from_secs(10),
                |cluster| match cluster.status(leader) {
                    Some(status) if status["host"]["data_serving"] == true => Ok(()),
                    other => Err(format!("{other:?}")),
                },
            );
            acknowledged.extend(range_rows);
        } else {
            refused_rounds += 1;
            unrecovered.push(led.id);
            // Not recovered: the member must say so, and must say why, and
            // the reason must not be a tail its own log certifies committed.
            assert_eq!(status["host"]["data_serving"], false, "{status}");
            assert_eq!(diagnostic["result"], "refused_or_pending", "{status}");
            assert!(
                !committed_tail(&diagnostic),
                "a certified committed tail must be recovered forward: {status}"
            );
            assert!(
                diagnostic["unrecovered_ranges"]
                    .as_array()
                    .is_some_and(|ranges| ranges.iter().any(|r| r["range_id"] == led.id.get())),
                "the refusal must name range {}: {diagnostic}",
                led.id
            );
            // Rows acknowledged before the kill: readable, or the read is
            // refused while the range has no recovered leader. Never absent.
            let (mut readable, mut refused) = (0, 0);
            for key in &range_rows {
                match read_within(&direct, key, Duration::from_secs(3)) {
                    Ok(found) => {
                        assert_eq!(
                            found,
                            Some(row_value(key)),
                            "acknowledged row {key} of the unrecovered range is absent or foreign"
                        );
                        readable += 1;
                    }
                    Err(_) => refused += 1,
                }
            }
            println!(
                "{} RESTART_M round {round}: range {} not recovered; of its {} acknowledged rows {readable} are readable now and {refused} reads were refused",
                stamp(cluster.started),
                led.id,
                range_rows.len()
            );
        }
    }
    let reader = gateway(&cluster, metadata_leader);
    for key in &acknowledged {
        assert_eq!(
            read_within(&reader, key, MAX_STALL).unwrap(),
            Some(row_value(key)),
            "acknowledged row {key} must be readable"
        );
    }
    // An append whose outcome the client never learned is either absent,
    // present with exactly the value it sent, or unreadable for now.
    let (mut present, mut unreadable) = (0, 0);
    for key in &unknown {
        match read_within(&reader, key, Duration::from_secs(3)) {
            Ok(None) => {}
            Ok(Some(value)) => {
                assert_eq!(value, row_value(key), "row {key} has a foreign value");
                present += 1;
            }
            Err(_) => unreadable += 1,
        }
    }
    println!(
        "{} RESTART_M leader kills under appends: {recovered_rounds} rounds recovered, {refused_rounds} rounds left the range refusing appends; {} acknowledged rows readable; of {} unacknowledged appends {present} exist and {unreadable} are unreadable for now",
        stamp(cluster.started),
        acknowledged.len(),
        unknown.len()
    );
    cluster.require_alive("finishing");
}
