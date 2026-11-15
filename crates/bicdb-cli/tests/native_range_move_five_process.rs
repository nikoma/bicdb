//! Five real processes: four initial members serve data, a fifth joins, and
//! ONE range is moved onto it with the operator commands only
//! (`bicdb cluster native-metadata …`). The test process is an operator and a
//! client: it holds no member state, and everything it learns comes from the
//! hosts' status files, the authenticated bootstrap read and the event client.
//!
//! The final assertions are one marked block switched by `EXPECT_COMPLETION`.
//!
//! Availability of the moved range is measured: a second client does one
//! Strong read and one append on the moved range about every 100 ms from
//! before `move-range` until after completion, and the test prints when
//! reads and writes were refused and at which stage. Asserted: reads and
//! writes succeed again after completion, every append that was
//! acknowledged before or during the move is readable afterwards, and (for
//! a move that completes in one attempt) neither reads nor writes were
//! refused for longer than `MAX_UNAVAILABLE` in a row.
use bicdb_core::{
    cluster_event_record_id, ClusterBootstrapSnapshot, ClusterEventClient, ClusterEventOperation,
    ClusterEventRequest, ClusterId, ClusterNetworkTransportConfig, ClusterNode, ClusterNodeId,
    ClusterTopology, CollectionMode, DistributionConfig, MetadataMemberRole,
    NativeMetadataBootstrapPlan, NativeMetadataCollection, RangeId, RangeReplicaRole,
    RelocationPhase, ReplicationTlsConfig, TcpClusterBootstrapClient,
    TcpClusterRelocationTransport, CLUSTER_EVENTS_COLLECTION,
};
use rcgen::{
    BasicConstraints, CertificateParams, ExtendedKeyUsagePurpose, IsCa, KeyPair, KeyUsagePurpose,
};
use serde_json::Value;
use std::collections::BTreeMap;
use std::net::TcpListener;
use std::path::PathBuf;
use std::process::{Child, Command, Output, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// THE switch of the final assertion block. `false`: the move must reach at
/// least `MIN_STAGE_WHEN_BLOCKED`, may then report a blocked stage, and the
/// cluster must stay healthy. `true`: the move must complete and the fifth
/// node must lead the range.
const EXPECT_COMPLETION: bool = true;
/// The stage a move that does not complete must have reached at least.
const MIN_STAGE_WHEN_BLOCKED: &str = "membership_final";
/// Stage names in order, as `range_move.rs` publishes them.
const STAGES: [&str; 10] = [
    "target_admitted",
    "learner_allocated",
    "image_verified",
    "decision_published",
    "certified",
    "promoted_frozen",
    "membership_final",
    "serving",
    "activation_recorded",
    "source_retired",
];
const FINAL_STAGE: &str = "source_retired";
/// Prefer a range whose leader is not the metadata leader, so that the run
/// also exercises `transfer-metadata-leadership`.
const PREFER_LEADERSHIP_TRANSFER: bool = true;
const INITIAL_MEMBERS: usize = 4;
const FIFTH: usize = 4;
const ROWS_PER_RANGE: usize = 3;
/// Every host ends itself after this long even if the test process is killed.
const HOST_RUN_SECONDS: u64 = 900;
/// The product's own supervisor tick; no wait here sleeps longer than it.
const PRODUCT_TICK: Duration = Duration::from_millis(500);
const POLL: Duration = Duration::from_millis(100);
const MOVE_TIMEOUT: Duration = Duration::from_secs(300);
/// A blocked stage is stable when the same error (digits ignored) at the same
/// stage was seen on this many consecutive polls and reproduced by this many
/// further attempts.
const STABLE_POLLS: u32 = 8;
const STABLE_REPEATS: u64 = 2;
/// With `EXPECT_COMPLETION` a stable error must also have lasted this long
/// before the move is called blocked: a stage may be refused for a while and
/// then pass.
const BLOCK_PATIENCE: Duration = Duration::from_secs(90);
/// The availability client's extra pause after a round in which a request
/// failed, as a client that backs off does. Members read their own state
/// without waiting for their data service and answer "busy" to a handoff
/// step while a client request holds it, so a client that hammers a refusing
/// range slows the very move it waits for. The outage boundaries are
/// therefore measured to about half a second.
const CLIENT_BACKOFF: Duration = Duration::from_millis(400);
/// What the availability client appends while the first move attempt runs.
///
/// `true`: it appends through the whole move, from before `move-range`. An
/// append that is in flight on the range leader when the learner allocation
/// commits is refused half-way by the changed writer policy and leaves an
/// unfinished native command there, and a source Close refuses such a voter
/// on every repetition. The coordinator asks before it fences or closes
/// anything and reports the move blocked before the cut. The test then does
/// what the operator does: `move-range --abort`, wait until the range
/// acknowledges appends again, and `move-range` again, this time with the
/// client's appends paused until the allocation is committed.
///
/// `false`: the client pauses its appends around the first allocation too,
/// so no append is in flight while it commits. It reads throughout.
const APPENDS_DURING_ALLOCATION: bool = false;

/// The longest time the moved range may refuse Strong reads, or appends, in
/// a row during a move that completes in its first attempt.
///
/// The range is refused from the learner allocation until the target
/// serves: the allocation changes the range epoch, the old voters' writer
/// policy is of the previous epoch, and the cut follows at once. Measured
/// between five processes on a shared build host: 16 to 20 s (before the
/// coordinator stopped waiting a supervisor tick per stage and the members
/// stopped answering "not complete" on a 0.5 s timing edge: 22 to 46 s).
/// The bound is not a product promise; it fails when a stage waits again.
const MAX_UNAVAILABLE: Duration = Duration::from_secs(30);

/// The longest run of failed attempts of one kind: from the first failed
/// attempt to the next successful one (or to the last attempt).
fn longest_outage(attempts: &[Attempt], write: bool) -> f64 {
    let (mut longest, mut open) = (0f64, None::<f64>);
    let mut last = 0f64;
    for attempt in attempts.iter().filter(|attempt| attempt.write == write) {
        last = attempt.at;
        match (open, attempt.ok) {
            (None, false) => open = Some(attempt.at),
            (Some(from), true) => {
                longest = longest.max(attempt.at - from);
                open = None;
            }
            _ => {}
        }
    }
    if let Some(from) = open {
        longest = longest.max(last - from);
    }
    longest
}

fn node(i: usize) -> ClusterNodeId {
    ClusterNodeId::new(format!("n{i}")).unwrap()
}
fn binary() -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_bicdb"));
    // The coordinator reports where the move spends its time (standard error).
    command.env("BICDB_RANGE_MOVE_TRACE", "1");
    command
}
fn stamp(started: Instant) -> String {
    let wall = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis())
        .unwrap_or(0);
    format!("[+{:8.3}s unix_ms={wall}]", started.elapsed().as_secs_f64())
}

struct Cluster {
    /// `None` once retained for diagnosis.
    directory: Option<tempfile::TempDir>,
    root: PathBuf,
    children: Vec<(usize, Child)>,
    sequences: [u64; 5],
    addresses: Vec<String>,
    config: DistributionConfig,
    started: Instant,
}
impl Drop for Cluster {
    fn drop(&mut self) {
        // Always: every spawned host is killed and waited for.
        for (i, child) in &mut self.children {
            let before = child.try_wait();
            let _ = child.kill();
            let waited = child.wait();
            eprintln!(
                "XPROC_M teardown node{i} pid={} before_kill={before:?} exit={waited:?}",
                child.id()
            );
        }
        if std::thread::panicking() {
            for i in 0..5 {
                let (_, status) = self.paths(i);
                match std::fs::read_to_string(&status) {
                    Ok(text) => eprintln!(
                        "XPROC_M FAILURE node{i} last status file:\n{}",
                        text.chars().take(12_000).collect::<String>()
                    ),
                    Err(error) => eprintln!("XPROC_M FAILURE node{i} status unreadable: {error}"),
                }
                let log = self.root.join(format!("node{i}.stderr"));
                match std::fs::read_to_string(&log) {
                    Ok(text) => {
                        let lines = text.lines().collect::<Vec<_>>();
                        let tail = &lines[lines.len().saturating_sub(40)..];
                        eprintln!(
                            "XPROC_M FAILURE node{i} stderr tail ({} lines total):\n{}",
                            lines.len(),
                            tail.join("\n")
                        );
                    }
                    Err(error) => eprintln!("XPROC_M FAILURE node{i} stderr unreadable: {error}"),
                }
            }
            if let Some(directory) = self.directory.take() {
                eprintln!("XPROC_M FAILURE retained {}", directory.keep().display());
            }
        }
    }
}

/// Outcome of one operator command: the CLI's exit and the host's reply.
struct Reply {
    ok: bool,
    stdout: Value,
    stderr: String,
    /// The host's status file once it carries this command's sequence.
    status: Value,
}

impl Cluster {
    fn paths(&self, i: usize) -> (PathBuf, PathBuf) {
        (
            self.root.join(format!("node{i}.command")),
            self.root.join(format!("node{i}.status")),
        )
    }
    fn tls(&self, command: &mut Command, i: usize) {
        command
            .arg("--cluster-tls-cert")
            .arg(self.root.join(format!("n{i}.pem")))
            .arg("--cluster-tls-key")
            .arg(self.root.join(format!("n{i}.key")))
            .arg("--cluster-tls-ca")
            .arg(self.root.join("ca.pem"));
    }
    fn spawn(&mut self, i: usize, mut command: Command) {
        command
            .stdin(Stdio::null())
            .stdout(std::fs::File::create(self.root.join(format!("node{i}.stdout"))).unwrap())
            .stderr(std::fs::File::create(self.root.join(format!("node{i}.stderr"))).unwrap());
        let child = command.spawn().unwrap();
        println!(
            "{} XPROC_M spawned node{i} pid={} {:?}",
            stamp(self.started),
            child.id(),
            command.get_args().take(3).collect::<Vec<_>>()
        );
        self.children.push((i, child));
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
    /// The last status the host published; `None` while absent or mid-rename.
    fn status(&self, i: usize) -> Option<Value> {
        let bytes = std::fs::read(self.paths(i).1).ok()?;
        serde_json::from_slice(&bytes).ok()
    }
    /// Bounded poll. `probe` returns the awaited value or what it saw instead.
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
    /// One operator command against node `i`'s command file. The sequence is
    /// consumed whether the host accepts or refuses, so this always waits for
    /// the host's reply to it before returning.
    fn operator(&mut self, i: usize, subcommand: &str, arguments: &[&str]) -> Reply {
        self.sequences[i] += 1;
        let sequence = self.sequences[i];
        let (command, status) = self.paths(i);
        let output = binary()
            .args(["cluster", "native-metadata", subcommand, "--command-file"])
            .arg(&command)
            .arg("--status-file")
            .arg(&status)
            .arg("--sequence")
            .arg(sequence.to_string())
            .args(arguments)
            .output()
            .unwrap();
        let what = format!(
            "node{i} to answer `{subcommand} {}` (sequence {sequence})",
            arguments.join(" ")
        );
        let status = self.wait(&what, Duration::from_secs(10), |cluster| {
            match cluster.status(i) {
                Some(value) if value["sequence"].as_u64() == Some(sequence) => Ok(value),
                other => Err(format!(
                    "status sequence {:?}",
                    other.map(|value| value["sequence"].clone())
                )),
            }
        });
        Reply {
            ok: output.status.success(),
            stdout: serde_json::from_slice(&output.stdout).unwrap_or(Value::Null),
            stderr: String::from_utf8_lossy(&output.stderr).into_owned(),
            status,
        }
    }
    /// The operator CLI alone, with an explicit sequence: no bookkeeping and
    /// no wait for the host.
    fn cli(&self, i: usize, subcommand: &str, sequence: u64, arguments: &[&str]) -> Output {
        let (command, status) = self.paths(i);
        binary()
            .args(["cluster", "native-metadata", subcommand, "--command-file"])
            .arg(&command)
            .arg("--status-file")
            .arg(&status)
            .arg("--sequence")
            .arg(sequence.to_string())
            .args(arguments)
            .output()
            .unwrap()
    }
    /// The command-file rules on node `i`: a reused sequence and a skipped
    /// one are refused by the CLI before anything is sent, and a skipped
    /// sequence written to the command file directly is refused by the host,
    /// recorded in its status, and does not end it.
    fn command_file_rules(&mut self, i: usize) {
        let applied = self.sequences[i];
        assert!(applied > 0 || i == FIFTH);
        let before = self.status(i).unwrap();
        assert_eq!(before["sequence"].as_u64(), Some(applied));
        if applied > 0 {
            let reused = self.cli(i, "step", applied, &["--action", "status"]);
            let stderr = String::from_utf8_lossy(&reused.stderr).into_owned();
            assert!(
                !reused.status.success()
                    && stderr.contains(&format!("sequence {applied} was already used")),
                "node{i}: a reused sequence must fail in the CLI: {stderr}"
            );
        }
        let skipped = self.cli(
            i,
            "step",
            applied + 5,
            &["--action", "status", "--timeout-ms", "300"],
        );
        let stderr = String::from_utf8_lossy(&skipped.stderr).into_owned();
        assert!(
            !skipped.status.success()
                && stderr.contains(&format!("sequence {} skips ahead", applied + 5)),
            "node{i}: a skipped sequence must fail in the CLI: {stderr}"
        );
        assert_eq!(
            self.status(i).unwrap()["sequence"].as_u64(),
            Some(applied),
            "node{i}: the CLI sent nothing"
        );
        // Written past the CLI: the host itself refuses and keeps running.
        let (command, _) = self.paths(i);
        let temporary = command.with_extension("typo");
        std::fs::write(
            &temporary,
            serde_json::to_vec(&serde_json::json!({
                "version": 1, "sequence": applied + 7, "action": "status", "budget_ms": 1000,
            }))
            .unwrap(),
        )
        .unwrap();
        std::fs::rename(&temporary, &command).unwrap();
        let what = format!("node{i} to record the refused sequence {}", applied + 7);
        let refused = self.wait(&what, Duration::from_secs(10), |cluster| {
            match cluster.status(i) {
                Some(status)
                    if status["error"].as_str().is_some_and(|error| {
                        error.contains(&format!(
                            "operator command with sequence {} refused and ignored",
                            applied + 7
                        ))
                    }) =>
                {
                    Ok(status)
                }
                other => Err(format!("{:?}", other.map(|status| status["error"].clone()))),
            }
        });
        assert_eq!(refused["sequence"].as_u64(), Some(applied));
        std::thread::sleep(PRODUCT_TICK * 2);
        self.require_alive("a skipped operator sequence was refused");
        // The next sequence is accepted and clears the refusal.
        self.accepted(i, "step", &["--action", "status"]);
        println!(
            "{} XPROC_M node{i} command-file rules hold (reused and skipped sequences refused, host alive)",
            stamp(self.started)
        );
    }
    /// An operator command that must be accepted, repeated while the host
    /// answers that one of its owners is busy: the host reads its status
    /// without waiting, and a client request may hold the service then.
    fn accepted_patiently(&mut self, i: usize, subcommand: &str, arguments: &[&str]) -> Reply {
        for _ in 0..40 {
            let reply = self.operator(i, subcommand, arguments);
            if reply.ok && reply.status["error"].is_null() {
                println!(
                    "{} XPROC_M node{i} accepted `{subcommand} {}`",
                    stamp(self.started),
                    arguments.join(" ")
                );
                return reply;
            }
            let error = reply.status["error"].to_string();
            assert!(
                error.contains("busy"),
                "node{i} refused `{subcommand} {}`: cli stderr: {}; host error: {error}",
                arguments.join(" "),
                reply.stderr.trim()
            );
            println!(
                "{} XPROC_M node{i} answered `{subcommand}` with a busy owner; repeated: {error}",
                stamp(self.started)
            );
            std::thread::sleep(POLL);
        }
        panic!("node{i} stayed busy for `{subcommand}`");
    }
    /// An operator command that must be accepted.
    fn accepted(&mut self, i: usize, subcommand: &str, arguments: &[&str]) -> Reply {
        let reply = self.operator(i, subcommand, arguments);
        assert!(
            reply.ok && reply.status["error"].is_null(),
            "node{i} refused `{subcommand} {}`: cli stderr: {}; host error: {}",
            arguments.join(" "),
            reply.stderr.trim(),
            reply.status["error"]
        );
        println!(
            "{} XPROC_M node{i} accepted `{subcommand} {}`",
            stamp(self.started),
            arguments.join(" ")
        );
        reply
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
    /// Committed metadata as member `i` itself serves it.
    fn snapshot(&self, i: usize) -> Result<ClusterBootstrapSnapshot, String> {
        TcpClusterBootstrapClient::new(self.config.cluster_id.clone(), node(0), self.network(0))
            .and_then(|client| client.fetch_snapshot(&node(i), &self.addresses[i]))
            .map_err(|error| error.to_string())
    }
}

/// Certificates, the common plan and four `serve-fresh` processes brought to
/// metadata control: prepare, launch (all), elect (n0), enable-control (all).
fn start_four_initial_members() -> Cluster {
    let directory = tempfile::Builder::new()
        .prefix("native-range-move-")
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
    // OS-assigned loopback ports, unique per run.
    let listeners = (0..5)
        .map(|_| TcpListener::bind("127.0.0.1:0").unwrap())
        .collect::<Vec<_>>();
    let addresses = listeners
        .iter()
        .map(|listener| listener.local_addr().unwrap().to_string())
        .collect::<Vec<_>>();
    let mut members = Vec::new();
    for i in 0..5 {
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
        cluster_id: ClusterId::new(format!("native-range-move-{}", std::process::id())).unwrap(),
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
        members: members[..INITIAL_MEMBERS].to_vec(),
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
        sequences: [0; 5],
        addresses,
        config,
        started: Instant::now(),
    };
    for i in 0..INITIAL_MEMBERS {
        let (command, status) = cluster.paths(i);
        let mut process = binary();
        // Automatic mode (no --manual-enrollment): the host's own tick runs
        // elections, joins, the writer-registry refresh and the range move.
        process
            .args(["cluster", "native-metadata", "serve-fresh"])
            .arg(cluster.root.join(format!("db{i}")))
            .arg("--plan")
            .arg(&plan_path)
            .arg("--node-id")
            .arg(node(i).as_str())
            .arg("--address")
            .arg(&cluster.addresses[i])
            .arg("--command-file")
            .arg(command)
            .arg("--status-file")
            .arg(status)
            .arg("--run-seconds")
            .arg(HOST_RUN_SECONDS.to_string());
        cluster.tls(&mut process, i);
        cluster.spawn(i, process);
    }
    for i in 0..INITIAL_MEMBERS {
        let what = format!("node{i} to publish its first status (phase provisional)");
        cluster.wait(&what, Duration::from_secs(60), |cluster| {
            match cluster.status(i) {
                Some(status) if status["host"]["phase"] == "provisional" => Ok(()),
                other => Err(format!("{:?}", other.map(|s| s["host"]["phase"].clone()))),
            }
        });
    }
    for action in ["prepare", "launch"] {
        for i in 0..INITIAL_MEMBERS {
            cluster.accepted(i, "step", &["--action", action]);
        }
    }
    cluster.accepted(0, "step", &["--action", "elect"]);
    for i in 0..INITIAL_MEMBERS {
        let reply = cluster.accepted(i, "step", &["--action", "enable-control"]);
        assert_eq!(reply.status["host"]["phase"], "metadata_control");
    }
    cluster
}

/// `open-initial-data` on every initial member until all four serve data. A
/// member refuses while its peers' receivers are still closed; that refusal
/// is repeated, never replaced.
fn open_initial_data(cluster: &mut Cluster) {
    let begun = Instant::now();
    let mut ready = [false; INITIAL_MEMBERS];
    let mut last = vec![String::new(); INITIAL_MEMBERS];
    while !ready.iter().all(|ready| *ready) {
        for i in 0..INITIAL_MEMBERS {
            if ready[i] {
                continue;
            }
            let reply = cluster.operator(
                i,
                "step",
                &["--action", "open-initial-data", "--timeout-ms", "1500"],
            );
            ready[i] =
                reply.status["error"].is_null() && reply.status["host"]["data_serving"] == true;
            last[i] = reply.status["error"].to_string();
        }
        assert!(
            begun.elapsed() < Duration::from_secs(60),
            "timed out after 60s waiting for the four initial members to serve data: ready={ready:?} last errors={last:?}"
        );
    }
    println!(
        "{} XPROC_M four initial members serve data",
        stamp(cluster.started)
    );
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

/// A Strong read repeated within `timeout`: membership changes may make a
/// route briefly unavailable, but an acknowledged row is never absent.
fn strong_read(
    gateway: &ClusterEventClient,
    key: &str,
    timeout: Duration,
) -> Result<Option<Value>, String> {
    let begun = Instant::now();
    loop {
        match gateway.get_strong("items", key) {
            Ok(value) => return Ok(value),
            Err(error) if begun.elapsed() >= timeout => return Err(error.to_string()),
            Err(_) => {
                let _ = gateway.refresh();
                std::thread::sleep(POLL);
            }
        }
    }
}

/// What a range looks like in committed metadata, for before/after equality.
fn assignment(topology: &ClusterTopology, range: RangeId) -> Value {
    let descriptor = topology.range_by_id(range).unwrap();
    serde_json::json!({
        "epoch": descriptor.epoch,
        "leader": descriptor.leader,
        "replicas": descriptor.replicas,
    })
}

/// One attempt of the availability client against the moved range.
struct Attempt {
    /// Seconds since the cluster started (the same clock as `stamp`).
    at: f64,
    write: bool,
    key: String,
    ok: bool,
    error: String,
    /// "stage>next" of the coordinator's `range_move` status at that time.
    stage: String,
}
struct Availability {
    stop: Arc<AtomicBool>,
    /// While set the client does not append (it still reads).
    quiet: Arc<AtomicBool>,
    /// Set by the test once the move completed: the client then ends after
    /// one more successful read and one more successful write.
    completed: Arc<Mutex<Option<Instant>>>,
    worker: Option<std::thread::JoinHandle<Vec<Attempt>>>,
}
impl Drop for Availability {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::SeqCst);
        if let Some(worker) = self.worker.take() {
            let _ = worker.join();
        }
    }
}
impl Availability {
    /// A second product client: one Strong read of `read_key` and one append
    /// of a fresh key of `range` about every 100 ms.
    fn start(
        config: DistributionConfig,
        topology: ClusterTopology,
        range: RangeId,
        read_key: String,
        coordinator_status: PathBuf,
        started: Instant,
    ) -> Self {
        let stop = Arc::new(AtomicBool::new(false));
        let quiet = Arc::new(AtomicBool::new(false));
        let completed = Arc::new(Mutex::new(None::<Instant>));
        let (stopped, done, silent) = (
            Arc::clone(&stop),
            Arc::clone(&completed),
            Arc::clone(&quiet),
        );
        let worker = std::thread::spawn(move || {
            let gateway = ClusterEventClient::new(&config, topology.clone())
                .unwrap()
                .with_request_timeout(Duration::from_secs(1))
                .unwrap();
            let stage = || {
                std::fs::read(&coordinator_status)
                    .ok()
                    .and_then(|bytes| serde_json::from_slice::<Value>(&bytes).ok())
                    .map(|status| {
                        let current = &status["host"]["range_move"];
                        if current.is_null() {
                            "no-move".to_string()
                        } else {
                            format!(
                                "{}>{}",
                                current["stage"].as_str().unwrap_or("none"),
                                current["next_stage"].as_str().unwrap_or("done")
                            )
                        }
                    })
                    .unwrap_or_else(|| "status-unreadable".into())
            };
            let mut attempts = Vec::new();
            let mut candidate = 0u64;
            let (mut read_after, mut write_after) = (false, false);
            while !stopped.load(Ordering::SeqCst) {
                let round = Instant::now();
                let finished = *done.lock().unwrap();
                if let Some(finished) = finished {
                    if (read_after && write_after) || finished.elapsed() > Duration::from_secs(60)
                    {
                        break;
                    }
                }
                // Reads and writes of this round both start after `finished`
                // was read, so a success counts as after completion.
                let at = started.elapsed().as_secs_f64();
                let now = stage();
                let read = gateway.get_strong("items", &read_key);
                let ok = matches!(&read, Ok(Some(value)) if value == &row_value(&read_key));
                attempts.push(Attempt {
                    at,
                    write: false,
                    key: read_key.clone(),
                    ok,
                    error: match &read {
                        Ok(Some(_)) if ok => String::new(),
                        Ok(other) => format!("unexpected value {other:?}"),
                        Err(error) => error.to_string(),
                    },
                    stage: now,
                });
                read_after |= ok && finished.is_some();
                let read_failed = !ok;
                if silent.load(Ordering::SeqCst) {
                    if read_failed {
                        let _ = gateway.refresh();
                        std::thread::sleep(CLIENT_BACKOFF);
                    }
                    if let Some(rest) = POLL.checked_sub(round.elapsed()) {
                        std::thread::sleep(rest);
                    }
                    continue;
                }
                let key = loop {
                    let key = format!("during-move-{candidate}");
                    candidate += 1;
                    if range_of(&topology, &key) == range {
                        break key;
                    }
                };
                let at = started.elapsed().as_secs_f64();
                let now = stage();
                let write = gateway.append("items", &key, row_value(&key));
                let ok = write.is_ok();
                attempts.push(Attempt {
                    at,
                    write: true,
                    key,
                    ok,
                    error: write.err().map(|error| error.to_string()).unwrap_or_default(),
                    stage: now,
                });
                write_after |= ok && finished.is_some();
                if read_failed || !ok {
                    let _ = gateway.refresh();
                    std::thread::sleep(CLIENT_BACKOFF);
                }
                if let Some(rest) = POLL.checked_sub(round.elapsed()) {
                    std::thread::sleep(rest);
                }
            }
            attempts
        });
        Self {
            stop,
            quiet,
            completed,
            worker: Some(worker),
        }
    }
    /// Tell the client that the move completed and collect its attempts.
    fn finish(mut self) -> Vec<Attempt> {
        *self.completed.lock().unwrap() = Some(Instant::now());
        self.worker.take().unwrap().join().unwrap()
    }
}

/// Print what the availability client saw. `origin` is the time of the
/// accepted `move-range`, `end` the time the move was seen completed, both
/// on the cluster clock. Returns the acknowledged append keys.
fn availability_summary(attempts: &[Attempt], origin: f64, end: f64) -> Vec<String> {
    for (write, name) in [(false, "READ "), (true, "WRITE")] {
        let own = attempts
            .iter()
            .filter(|attempt| attempt.write == write)
            .collect::<Vec<_>>();
        let failed = own.iter().filter(|attempt| !attempt.ok).count();
        // An outage: from the first failed attempt to the next success.
        let mut outages: Vec<(f64, f64, String, String, String, usize)> = Vec::new();
        let mut open: Option<(f64, String, String, usize)> = None;
        for attempt in &own {
            match (&mut open, attempt.ok) {
                (None, false) => {
                    open = Some((attempt.at, attempt.stage.clone(), attempt.error.clone(), 1))
                }
                (Some(outage), false) => outage.3 += 1,
                (Some(_), true) => {
                    let (from, stage, error, count) = open.take().unwrap();
                    outages.push((from, attempt.at, stage, attempt.stage.clone(), error, count));
                }
                (None, true) => {}
            }
        }
        if let Some((from, stage, error, count)) = open.take() {
            let last = own.last().unwrap();
            outages.push((from, last.at, stage, format!("{} (never recovered)", last.stage), error, count));
        }
        let total: f64 = outages.iter().map(|outage| outage.1 - outage.0).sum();
        let first = own.iter().find(|attempt| !attempt.ok);
        let last = own.iter().rev().find(|attempt| !attempt.ok);
        println!(
            "XPROC_M AVAILABILITY {name} attempts={} failed={failed} first_failure={} last_failure={} unavailable_total={total:.3}s outages={} (times are seconds after the accepted move-range; the move completed at +{:.3}s)",
            own.len(),
            first.map_or("none".into(), |attempt| format!("{:+.3}s", attempt.at - origin)),
            last.map_or("none".into(), |attempt| format!("{:+.3}s", attempt.at - origin)),
            outages.len(),
            end - origin
        );
        for (from, to, began, ended, error, count) in &outages {
            println!(
                "XPROC_M AVAILABILITY {name} outage {:+.3}s..{:+.3}s ({:.3}s, {count} failed attempts) began at stage {began}, ended at stage {ended}; first error: {}",
                from - origin,
                to - origin,
                to - from,
                error.chars().take(400).collect::<String>()
            );
        }
        // Every distinct error text (digits ignored) with its stage span.
        let mut texts: BTreeMap<String, (usize, String, String)> = BTreeMap::new();
        for attempt in own.iter().filter(|attempt| !attempt.ok) {
            let text = attempt
                .error
                .chars()
                .map(|c| if c.is_ascii_digit() { '#' } else { c })
                .take(300)
                .collect::<String>();
            let entry = texts
                .entry(text)
                .or_insert((0, attempt.stage.clone(), attempt.stage.clone()));
            entry.0 += 1;
            entry.2 = attempt.stage.clone();
        }
        for (text, (count, from, to)) in texts {
            println!("XPROC_M AVAILABILITY {name} error x{count} stages {from} .. {to}: {text}");
        }
    }
    attempts
        .iter()
        .filter(|attempt| attempt.write && attempt.ok)
        .map(|attempt| attempt.key.clone())
        .collect()
}

/// The first value stored under `key` anywhere inside `value`.
fn find_key<'a>(value: &'a Value, key: &str) -> Option<&'a Value> {
    match value {
        Value::Object(map) => map
            .get(key)
            .or_else(|| map.values().find_map(|inner| find_key(inner, key))),
        Value::Array(items) => items.iter().find_map(|inner| find_key(inner, key)),
        _ => None,
    }
}

enum MoveOutcome {
    Completed,
    /// The same error at the same stage, reproduced by repeated attempts.
    Blocked,
    TimedOut,
}

#[test]
fn native_range_move_five_process_operator_sequence() {
    // (a) Four initial members, mTLS, serving initial data.
    let mut cluster = start_four_initial_members();
    open_initial_data(&mut cluster);
    let started = cluster.started;

    // (b) N acknowledged rows across all ranges through the product client.
    let initial = cluster.wait(
        "the authenticated bootstrap read from n0",
        Duration::from_secs(20),
        |cluster| cluster.snapshot(0),
    );
    let mut client_config = cluster.config.clone();
    client_config.transport = cluster.network(0);
    let gateway = ClusterEventClient::new(&client_config, initial.topology.clone())
        .unwrap()
        .with_request_timeout(Duration::from_secs(5))
        .unwrap();
    let mut rows: BTreeMap<RangeId, Vec<String>> = initial
        .topology
        .ranges
        .values()
        .map(|range| (range.id, Vec::new()))
        .collect();
    assert_eq!(rows.len(), 4, "the plan asks for four initial ranges");
    let mut candidate = 0;
    while rows.values().any(|keys| keys.len() < ROWS_PER_RANGE) {
        assert!(candidate < 10_000, "no key found for some range");
        let key = format!("row-{candidate}");
        candidate += 1;
        let keys = rows.get_mut(&range_of(&initial.topology, &key)).unwrap();
        if keys.len() < ROWS_PER_RANGE {
            keys.push(key);
        }
    }
    let all_rows = rows.values().flatten().cloned().collect::<Vec<_>>();
    for key in &all_rows {
        let what = format!("the acknowledgement of row {key}");
        cluster.wait(&what, Duration::from_secs(20), |_| {
            // The same key and value: a repeated append is the same operation.
            gateway
                .append("items", key, row_value(key))
                .map(|_| ())
                .map_err(|error| error.to_string())
        });
        assert_eq!(
            strong_read(&gateway, key, Duration::from_secs(10)),
            Ok(Some(row_value(key))),
            "Strong read of the acknowledged row {key}"
        );
    }
    println!(
        "{} XPROC_M {} rows acknowledged and read back: {rows:?}",
        stamp(started),
        all_rows.len()
    );

    // (c) The fifth process: ONE command registers it, starts its metadata
    // learner, is promoted to metadata voter by the leader's maintenance and
    // opens its replica receiver (the host's own OpenReplicaReceiver).
    let (command, status) = cluster.paths(FIFTH);
    let mut fifth = binary();
    fifth
        .args([
            "cluster",
            "native-metadata",
            "join-fresh",
            "--replica-receiver",
        ])
        .arg(cluster.root.join("db4"))
        .arg("--cluster-id")
        .arg(cluster.config.cluster_id.as_str())
        .arg("--seed-node-id")
        .arg("n0")
        .arg("--seed-address")
        .arg(&cluster.addresses[0])
        .arg("--node-id")
        .arg("n4")
        .arg("--address")
        .arg(&cluster.addresses[FIFTH])
        .arg("--capacity-bytes")
        .arg((1u64 << 30).to_string())
        .arg("--command-file")
        .arg(&command)
        .arg("--status-file")
        .arg(&status)
        .arg("--run-seconds")
        .arg(HOST_RUN_SECONDS.to_string());
    cluster.tls(&mut fifth, FIFTH);
    cluster.spawn(FIFTH, fifth);
    let receiver = cluster.wait(
        "the fifth node to be a metadata voter with an open replica receiver",
        Duration::from_secs(90),
        |cluster| match cluster.status(FIFTH) {
            Some(status)
                if status["error"].is_null()
                    && status["host"]["metadata_role"] == "voter"
                    && status["host"]["replica_receiver"]["receiver_ready"] == true =>
            {
                Ok(status)
            }
            Some(status) => Err(format!(
                "phase={} role={} receiver={} error={}",
                status["host"]["phase"],
                status["host"]["metadata_role"],
                status["host"]["replica_receiver"]["receiver_ready"],
                status["error"]
            )),
            None => Err("no status file yet".into()),
        },
    );
    println!(
        "{} XPROC_M fifth node joined: phase={} control_owned={} receiver_ready=true",
        stamp(started),
        receiver["host"]["phase"],
        receiver["host"]["metadata_control_owned"]
    );
    // The existing data stays served after the metadata-only addition.
    for keys in rows.values() {
        assert_eq!(
            strong_read(&gateway, &keys[0], Duration::from_secs(30)),
            Ok(Some(row_value(&keys[0]))),
            "Strong read of {} after the fifth node joined",
            keys[0]
        );
    }

    // (d) Pick the range; make its leader the metadata leader.
    let before = cluster.wait(
        "committed metadata on n0 naming n4 a metadata voter outside every range",
        Duration::from_secs(30),
        |cluster| {
            let snapshot = cluster.snapshot(0)?;
            let voter = snapshot
                .topology
                .nodes
                .get(&node(FIFTH))
                .is_some_and(|member| member.metadata_role == MetadataMemberRole::Voter);
            if voter && snapshot.metadata_leader_id.is_some() {
                Ok(snapshot)
            } else {
                Err(format!(
                    "n4 voter={voter} metadata leader={:?}",
                    snapshot.metadata_leader_id
                ))
            }
        },
    );
    assert!(
        before
            .topology
            .ranges
            .values()
            .all(|range| range.replicas.iter().all(|r| r.node_id != node(FIFTH))),
        "a metadata-only join must not assign data to the fifth node"
    );
    let metadata_leader = before.metadata_leader_id.clone().unwrap();
    let moved = before
        .topology
        .ranges
        .values()
        .find(|range| (range.leader != metadata_leader) == PREFER_LEADERSHIP_TRANSFER)
        .or_else(|| before.topology.ranges.values().next())
        .unwrap()
        .clone();
    let range = moved.id;
    let leader = moved.leader.clone();
    let index_of = |id: &ClusterNodeId| (0..5).find(|i| &node(*i) == id).unwrap();
    let l = index_of(&leader);
    let m = index_of(&metadata_leader);
    println!(
        "{} XPROC_M range {range} (epoch {}, replicas {:?}) is led by {leader}; metadata is led by {metadata_leader}",
        stamp(started),
        moved.epoch,
        moved.replicas.iter().map(|r| r.node_id.as_str().to_string()).collect::<Vec<_>>()
    );
    if l != m {
        assert!(m < INITIAL_MEMBERS && l < INITIAL_MEMBERS);
        let to = leader.as_str().to_string();
        let reply = cluster.operator(m, "transfer-metadata-leadership", &["--to", &to]);
        println!(
            "{} XPROC_M transfer-metadata-leadership --to {to} on {metadata_leader}: ok={} report={} stderr={}",
            stamp(started),
            reply.ok,
            reply.status["host"]["last_metadata_leadership_transfer"],
            reply.stderr.trim()
        );
        assert!(
            reply.ok && reply.status["error"].is_null(),
            "{metadata_leader} refused transfer-metadata-leadership --to {to}: {} / {}",
            reply.stderr.trim(),
            reply.status["error"]
        );
        assert_eq!(
            reply.stdout, reply.status["host"]["last_metadata_leadership_transfer"],
            "the command prints the host's transfer report"
        );
    }
    let what = format!("{leader}'s own status to show that it leads metadata");
    cluster.wait(&what, Duration::from_secs(30), |cluster| {
        let status = cluster.status(l).ok_or("no status")?;
        if status["host"]["metadata_leader"] == leader.as_str()
            && status["host"]["metadata"]["role"] == "leader"
        {
            Ok(())
        } else {
            Err(format!(
                "metadata_leader={} role={}",
                status["host"]["metadata_leader"], status["host"]["metadata"]["role"]
            ))
        }
    });
    println!(
        "{} XPROC_M {leader} leads metadata and range {range}",
        stamp(started)
    );

    // The operator command-file rules, on an initial member that does not
    // coordinate and on the fifth node (the two host loops).
    let bystander = (0..INITIAL_MEMBERS).find(|i| *i != l).unwrap();
    cluster.command_file_rules(bystander);
    cluster.command_file_rules(FIFTH);

    // The availability client of the moved range, from before the move.
    let availability = Availability::start(
        client_config.clone(),
        before.topology.clone(),
        range,
        rows[&range][0].clone(),
        cluster.paths(l).1,
        started,
    );
    std::thread::sleep(Duration::from_secs(2));
    if !APPENDS_DURING_ALLOCATION {
        // No append is in flight while the allocation commits: the client's
        // request timeout is one second.
        availability.quiet.store(true, Ordering::SeqCst);
        std::thread::sleep(Duration::from_millis(1500));
    }

    // (e) Start the move on the coordinator and follow its status section.
    let range_text = range.get().to_string();
    // The origin of every "+s" below: the first time the command is sent.
    // The host starts the move then, even when it answers "busy" for its
    // status read and the command is repeated.
    let move_origin = started.elapsed().as_secs_f64();
    let requested = cluster.accepted_patiently(
        l,
        "move-range",
        &["--range-id", &range_text, "--target-node-id", "n4"],
    );
    // While this host's move runs, its metadata leadership is not handed
    // away: the operator's transfer is refused and changes nothing.
    {
        let to = node(bystander).as_str().to_string();
        let refused = cluster.operator(l, "transfer-metadata-leadership", &["--to", &to]);
        assert!(
            !refused.ok
                && refused.status["error"]
                    .as_str()
                    .is_some_and(|error| error.contains("metadata leadership transfer refused")),
            "a transfer away from the coordinator of a running move must be refused: ok={} error={} stderr={}",
            refused.ok,
            refused.status["error"],
            refused.stderr.trim()
        );
        assert_eq!(refused.status["host"]["metadata"]["role"], "leader");
        println!(
            "{} XPROC_M transfer-metadata-leadership refused during the move: {}",
            stamp(started),
            refused.status["error"]
        );
    }
    assert_eq!(
        requested.stdout, requested.status["host"]["range_move"],
        "the command prints the host's range_move section"
    );
    assert_eq!(requested.stdout["range"], range.get());
    assert_eq!(requested.stdout["target"], "n4");
    let move_started = Instant::now();
    let mut transitions: Vec<String> = Vec::new();
    let mut shown = Value::Null;
    let mut stable_polls = 0u32;
    let mut stable_key: Option<(String, String)> = None;
    let mut stable_since_attempts = 0u64;
    let mut stable_since = Instant::now();
    let mut retried = 0u32;
    let mut last = Value::Null;
    let outcome = loop {
        cluster.require_alive("the range move runs");
        if let Some(status) = cluster.status(l) {
            let current = status["host"]["range_move"].clone();
            let view = serde_json::json!({
                "stage": current["stage"], "next_stage": current["next_stage"],
                "last_error": current["last_error"], "paused": current["paused"],
                "completed": current["completed"], "aborted": current["aborted"],
            });
            if view != shown {
                let line = format!(
                    "{} RANGE_MOVE +{:.3}s stage={} next={} attempts={} in_flight={} completed={} paused={} last_error={}",
                    stamp(started),
                    (started.elapsed().as_secs_f64() - move_origin),
                    current["stage"],
                    current["next_stage"],
                    current["attempts"],
                    current["step_in_flight"],
                    current["completed"],
                    current["paused"],
                    current["last_error"]
                );
                println!("XPROC_M {line}");
                transitions.push(line);
                shown = view;
            }
            assert_eq!(current["aborted"], false, "nobody aborted the move");
            // The move is blocked before the cut by an unfinished write
            // command on a voter: the operator's procedure.
            if EXPECT_COMPLETION
                && retried < 2
                && current["step_in_flight"] == false
                && current["last_error"]
                    .as_str()
                    .is_some_and(|error| error.contains("holds an unfinished write command"))
            {
                retried += 1;
                let blocked_at = started.elapsed().as_secs_f64();
                println!(
                    "{} XPROC_M BLOCKED BEFORE THE CUT (+{:.3}s after move-range): {}",
                    stamp(started),
                    blocked_at - move_origin,
                    current["last_error"]
                );
                let aborted =
                    cluster.accepted_patiently(l, "move-range", &["--range-id", &range_text, "--abort"]);
                assert_eq!(
                    aborted.status["host"]["range_move"]["aborted"], true,
                    "the abort before the cut must be committed: {}",
                    aborted.status["host"]["range_move"]
                );
                let abort_at = started.elapsed().as_secs_f64();
                // The range is back on its original voters: an append is
                // acknowledged again once the unfinished command is settled.
                let mut n = 0;
                let what = "an append on the range to be acknowledged after the abort";
                cluster.wait(what, Duration::from_secs(90), |_| {
                    let _ = gateway.refresh();
                    let key = loop {
                        let key = format!("after-abort-{retried}-{n}");
                        n += 1;
                        if range_of(&before.topology, &key) == range {
                            break key;
                        }
                    };
                    gateway
                        .append("items", &key, row_value(&key))
                        .map(|_| ())
                        .map_err(|error| error.to_string())
                });
                let writable_at = started.elapsed().as_secs_f64();
                println!(
                    "{} XPROC_M ABORTED BEFORE THE CUT: abort committed +{:.3}s after move-range ({:.3}s after the block was seen); appends acknowledged again {:.3}s after the abort",
                    stamp(started),
                    abort_at - move_origin,
                    abort_at - blocked_at,
                    writable_at - abort_at
                );
                // The second attempt: no append in flight at the allocation.
                availability.quiet.store(true, Ordering::SeqCst);
                std::thread::sleep(Duration::from_millis(1500));
                cluster.accepted_patiently(
                    l,
                    "move-range",
                    &["--range-id", &range_text, "--target-node-id", "n4"],
                );
                println!(
                    "{} XPROC_M SECOND move-range accepted +{:.3}s after the first",
                    stamp(started),
                    started.elapsed().as_secs_f64() - move_origin
                );
                stable_key = None;
                stable_polls = 0;
                shown = Value::Null;
                continue;
            }
            if current["stage"]
                .as_str()
                .and_then(|stage| STAGES.iter().position(|name| *name == stage))
                .is_some_and(|reached| reached >= 1)
            {
                // The learner allocation is committed: appends resume.
                availability.quiet.store(false, Ordering::SeqCst);
            }
            let attempts = current["attempts"].as_u64().unwrap_or(0);
            if current["completed"] == true {
                last = current;
                break MoveOutcome::Completed;
            }
            // Digits are ignored: an error may carry indexes or durations.
            let key = current["last_error"].as_str().map(|error| {
                (
                    current["stage"].to_string(),
                    error
                        .chars()
                        .map(|c| if c.is_ascii_digit() { '#' } else { c })
                        .collect::<String>(),
                )
            });
            if key.is_some() && key == stable_key {
                stable_polls += 1;
            } else {
                stable_polls = 0;
                stable_since_attempts = attempts;
                stable_since = Instant::now();
                stable_key = key;
            }
            last = current;
            if stable_key.is_some()
                && attempts >= 1
                && stable_polls >= STABLE_POLLS
                && attempts >= stable_since_attempts + STABLE_REPEATS
                && (!EXPECT_COMPLETION || stable_since.elapsed() >= BLOCK_PATIENCE)
            {
                break MoveOutcome::Blocked;
            }
        }
        if move_started.elapsed() >= MOVE_TIMEOUT {
            break MoveOutcome::TimedOut;
        }
        std::thread::sleep(PRODUCT_TICK / 2);
    };
    let stage = last["stage"].as_str().map(str::to_string);
    let stage_index = stage
        .as_deref()
        .map(|stage| STAGES.iter().position(|name| *name == stage).unwrap());
    println!(
        "{} XPROC_M move outcome after {:.3}s: {} stage={stage:?} next={} attempts={} last_error={}",
        stamp(started),
        (started.elapsed().as_secs_f64() - move_origin),
        match outcome {
            MoveOutcome::Completed => "COMPLETED",
            MoveOutcome::Blocked => "BLOCKED (stable error)",
            MoveOutcome::TimedOut => "TIMED OUT",
        },
        last["next_stage"],
        last["attempts"],
        last["last_error"]
    );
    println!("XPROC_M STAGE TRANSITIONS ({}):", transitions.len());
    for line in &transitions {
        println!("XPROC_M   {line}");
    }
    let move_end = started.elapsed().as_secs_f64();
    if !matches!(outcome, MoveOutcome::Completed) {
        // Reported, not asserted: what the operator can still do.
        let abort = cluster.operator(l, "move-range", &["--range-id", &range_text, "--abort"]);
        println!(
            "{} XPROC_M NOT COMPLETED: move-range --abort: ok={} host error={} range_move={}",
            stamp(started),
            abort.ok,
            abort.status["error"],
            abort.status["host"]["range_move"]
        );
        std::thread::sleep(Duration::from_secs(5));
        if let Some(status) = cluster.status(l) {
            println!(
                "{} XPROC_M NOT COMPLETED: coordinator range_move 5 s later: {}",
                stamp(started),
                status["host"]["range_move"]
            );
        }
    }
    // The availability client ends after one more successful read and write
    // (or 60 s); its record is printed whatever the outcome.
    let attempts = if matches!(outcome, MoveOutcome::Completed) {
        availability.finish()
    } else {
        availability.stop.store(true, Ordering::SeqCst);
        availability.finish()
    };
    let acknowledged = availability_summary(&attempts, move_origin, move_end);
    // Reported, not asserted: each initial member's last writer registry
    // refresh after the move (its 500 ms maintenance).
    std::thread::sleep(PRODUCT_TICK * 3);
    for i in 0..INITIAL_MEMBERS {
        println!(
            "XPROC_M node{i} registry_refresh_last_attempt after the move: {}",
            cluster
                .status(i)
                .as_ref()
                .and_then(|status| find_key(status, "registry_refresh_last_attempt"))
                .map_or("absent".to_string(), |value| value
                    .to_string()
                    .chars()
                    .take(500)
                    .collect())
        );
    }
    // Where the coordinating host spent the move, part by part.
    for i in 0..5 {
        if let Ok(text) = std::fs::read_to_string(cluster.root.join(format!("node{i}.stderr"))) {
            for line in text.lines().filter(|line| line.contains("RANGE_MOVE_TRACE")) {
                println!("XPROC_M node{i} {}", line.chars().take(420).collect::<String>());
            }
        }
    }
    let _ = gateway.refresh();
    let untouched = rows
        .iter()
        .filter(|(id, _)| **id != range)
        .flat_map(|(_, keys)| keys.iter().cloned())
        .collect::<Vec<_>>();

    // ===================================================================
    // (f) ASSERTION BLOCK — switched by EXPECT_COMPLETION only.
    // ===================================================================
    if !EXPECT_COMPLETION {
        let minimum = STAGES
            .iter()
            .position(|name| *name == MIN_STAGE_WHEN_BLOCKED)
            .unwrap();
        assert!(
            stage_index.is_some_and(|reached| reached >= minimum),
            "the move must reach at least {MIN_STAGE_WHEN_BLOCKED}; it reached {stage:?} and reports: {}",
            last["last_error"]
        );
        // Every member's own committed metadata names the fifth node a
        // replica of the range.
        for i in 0..5 {
            let what =
                format!("node{i}'s committed metadata to name n4 a replica of range {range}");
            cluster.wait(&what, Duration::from_secs(30), |cluster| {
                let topology = cluster.snapshot(i)?.topology;
                let descriptor = topology.range_by_id(range).ok_or("range absent")?;
                if descriptor.replicas.iter().any(|r| r.node_id == node(FIFTH)) {
                    Ok(())
                } else {
                    Err(format!("replicas {:?}", descriptor.replicas))
                }
            });
        }
        // The untouched ranges still answer Strong reads of acknowledged rows.
        for key in &untouched {
            assert_eq!(
                strong_read(&gateway, key, Duration::from_secs(30)),
                Ok(Some(row_value(key))),
                "Strong read of {key} on a range the move did not touch"
            );
        }
        // Reported, not asserted: the moved range while its move is unfinished.
        for key in &rows[&range] {
            println!(
                "XPROC_M unfinished move: Strong read of {key} on range {range}: {:?}",
                strong_read(&gateway, key, Duration::from_secs(5))
            );
        }
        match outcome {
            MoveOutcome::Completed => println!(
                "XPROC_M NOTE: the move COMPLETED; set EXPECT_COMPLETION = true to assert the full result"
            ),
            _ => println!(
                "XPROC_M BLOCKED ERROR TEXT (not asserted): {}",
                last["last_error"]
            ),
        }
    } else {
        assert!(
            matches!(outcome, MoveOutcome::Completed),
            "the move did not complete: stage={stage:?} last_error={}",
            last["last_error"]
        );
        assert_eq!(stage.as_deref(), Some(FINAL_STAGE));
        // Every member's committed metadata: the fifth node is a voter and
        // the leader of the range, the source is retired, the relocation is
        // Completed, and the other ranges are exactly as before.
        for i in 0..5 {
            let what = format!("node{i}'s committed metadata to show the finished move");
            cluster.wait(&what, Duration::from_secs(30), |cluster| {
                let topology = cluster.snapshot(i)?.topology;
                let descriptor = topology.range_by_id(range).ok_or("range absent")?;
                let voter = descriptor
                    .replicas
                    .iter()
                    .any(|r| r.node_id == node(FIFTH) && r.role == RangeReplicaRole::Voter);
                let retired = descriptor.replicas.iter().all(|r| r.node_id != leader);
                let relocated = topology.relocations.values().any(|plan| {
                    plan.range_id == range
                        && plan.target == node(FIFTH)
                        && plan.phase == RelocationPhase::Completed
                });
                let others = before
                    .topology
                    .ranges
                    .values()
                    .filter(|other| other.id != range)
                    .all(|other| {
                        assignment(&before.topology, other.id) == assignment(&topology, other.id)
                    });
                if voter && descriptor.leader == node(FIFTH) && retired && relocated && others {
                    Ok(())
                } else {
                    Err(format!(
                        "n4 voter={voter} leader={} source retired={retired} relocation completed={relocated} other ranges unchanged={others}",
                        descriptor.leader
                    ))
                }
            });
        }
        // All N rows by Strong reads; the moved range's leader is the fifth
        // node, so its rows are read through it.
        for key in &all_rows {
            assert_eq!(
                strong_read(&gateway, key, Duration::from_secs(30)),
                Ok(Some(row_value(key))),
                "Strong read of {key} after the move"
            );
        }
        // The availability client: after completion a Strong read and an
        // append on the moved range succeeded again, and no acknowledged
        // append was lost, whenever during the move it was acknowledged.
        for (write, name) in [(false, "Strong read"), (true, "append")] {
            assert!(
                attempts
                    .iter()
                    .any(|attempt| attempt.write == write && attempt.ok && attempt.at > move_end),
                "no {name} on the moved range succeeded after the move completed"
            );
        }
        // The unavailable window. With appends in flight at the allocation
        // the first attempt is aborted and repeated, which is another
        // sequence with its own windows; it is measured and not bounded here.
        if !APPENDS_DURING_ALLOCATION {
            for (write, name) in [(false, "Strong reads"), (true, "appends")] {
                let longest = longest_outage(&attempts, write);
                assert!(
                    longest <= MAX_UNAVAILABLE.as_secs_f64(),
                    "{name} on the moved range were refused for {longest:.3}s in a row; the bound is {MAX_UNAVAILABLE:?}"
                );
            }
        }
        // A Strong read that answers, answers the acknowledged row: an
        // absence or another value is never an availability matter.
        let wrong = attempts
            .iter()
            .filter(|a| !a.write && !a.ok && a.error.starts_with("unexpected value"))
            .map(|a| format!("+{:.3}s {} {}", a.at - move_origin, a.stage, a.error))
            .collect::<Vec<_>>();
        assert!(
            wrong.is_empty(),
            "a Strong read of an acknowledged row answered without that row: {wrong:?}"
        );
        let (before_move, during_move) = (
            attempts
                .iter()
                .filter(|a| a.write && a.ok && a.at < move_origin)
                .count(),
            attempts
                .iter()
                .filter(|a| a.write && a.ok && a.at >= move_origin && a.at <= move_end)
                .count(),
        );
        for key in &acknowledged {
            assert_eq!(
                strong_read(&gateway, key, Duration::from_secs(30)),
                Ok(Some(row_value(key))),
                "an acknowledged append ({key}) of the moved range is lost after the move"
            );
        }
        println!(
            "{} XPROC_M AVAILABILITY all {} acknowledged appends are Strong-readable after the move ({before_move} acknowledged before move-range, {during_move} during the move, {} after)",
            stamp(started),
            acknowledged.len(),
            acknowledged.len() - before_move - during_move
        );
        // A write after the move, on the moved range, is acknowledged.
        let current = cluster.snapshot(FIFTH).unwrap().topology;
        let fresh = (0..10_000)
            .map(|i| format!("after-move-{i}"))
            .find(|key| range_of(&current, key) == range)
            .unwrap();
        let what = format!("the acknowledgement of {fresh} on the moved range");
        cluster.wait(&what, Duration::from_secs(30), |_| {
            gateway
                .append("items", &fresh, row_value(&fresh))
                .map(|_| ())
                .map_err(|error| error.to_string())
        });
        assert_eq!(
            strong_read(&gateway, &fresh, Duration::from_secs(30)),
            Ok(Some(row_value(&fresh)))
        );
        // The retired member no longer serves the range.
        let descriptor = current.range_by_id(range).unwrap().clone();
        let probe = TcpClusterRelocationTransport::new(
            cluster.config.cluster_id.clone(),
            node(0),
            std::sync::Arc::new(parking_lot::RwLock::new(current)),
            cluster.network(0),
        )
        .unwrap();
        let refused = probe.application_event(
            &leader,
            ClusterEventRequest {
                range_id: range,
                range_epoch: descriptor.epoch,
                operation: ClusterEventOperation::Get {
                    namespace: "items".into(),
                    key: rows[&range][0].clone(),
                },
            },
        );
        assert!(
            refused.is_err(),
            "the retired member {leader} still answers for range {range}: {refused:?}"
        );
    }
    // ===================================================================
    // (g) Teardown: `Cluster::drop` kills and waits for all five processes,
    // and on failure prints each host's last status and stderr tail.
    // ===================================================================
}
