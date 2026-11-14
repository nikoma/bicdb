//! Kill-the-primary: a three-node group with synchronous replication and
//! automatic failover loses no acknowledged write when its primary is killed,
//! elects a new primary, and takes the old one back as a standby.
use bicdb_core::replication::ReplicationFrame;
use bicdb_core::replication_transport::{recv_frame, send_frame};
use std::collections::BTreeSet;
use std::net::{TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

const NODES: [&str; 3] = ["n1", "n2", "n3"];

struct Node {
    id: &'static str,
    sql_port: u16,
    replication_port: u16,
    sql_reservation: Option<TcpListener>,
    replication_reservation: Option<TcpListener>,
    last_exit: Option<String>,
    last_pid: Option<u32>,
    child: Option<Child>,
}

struct Group {
    root: PathBuf,
    nodes: Vec<Node>,
    runtime: tokio::runtime::Runtime,
}

impl Group {
    fn new(root: &Path) -> Self {
        let nodes = NODES
            .iter()
            .map(|id| {
                // Retain every real listener, including across other parallel
                // Groups. An address selected then immediately dropped is not
                // a reservation and can repeat even within this six-port set.
                let sql = TcpListener::bind("127.0.0.1:0").unwrap();
                let replication = TcpListener::bind("127.0.0.1:0").unwrap();
                Node {
                    id,
                    sql_port: sql.local_addr().unwrap().port(),
                    replication_port: replication.local_addr().unwrap().port(),
                    sql_reservation: Some(sql),
                    replication_reservation: Some(replication),
                    last_exit: None,
                    last_pid: None,
                    child: None,
                }
            })
            .collect();
        Self {
            root: root.to_path_buf(),
            nodes,
            runtime: tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap(),
        }
    }

    fn start(&mut self, index: usize) {
        let node = &self.nodes[index];
        let mut command = Command::new(env!("CARGO_BIN_EXE_bicdb"));
        command
            .arg("serve")
            .arg(self.root.join(node.id))
            .args(["--host", "127.0.0.1", "--port", &node.sql_port.to_string()])
            .args(["--replication-cluster-id", "chaos"])
            .args(["--replication-node-id", node.id])
            .args([
                "--replication-listen",
                &format!("127.0.0.1:{}", node.replication_port),
            ])
            .args(["--synchronous-standbys", "1"])
            .args(["--synchronous-timeout-ms", "3000"])
            .args(["--failover-timeout-ms", "3000"])
            .arg("--replication-dev-plaintext")
            .args(["--advertise-sql", &format!("127.0.0.1:{}", node.sql_port)]);
        for peer in &self.nodes {
            if peer.id != node.id {
                command
                    .args([
                        "--ha-peer",
                        &format!("{}=127.0.0.1:{}", peer.id, peer.replication_port),
                    ])
                    .args(["--allow-standby", peer.id]);
            }
        }
        if index != 0 {
            command.args([
                "--standby-of",
                &format!("127.0.0.1:{}", self.nodes[0].replication_port),
            ]);
        }
        let log = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(self.root.join(format!("{}.log", node.id)))
            .unwrap();
        command
            .stdout(Stdio::from(log.try_clone().unwrap()))
            .stderr(Stdio::from(log));
        // Only this child's pair is released. All future children and parallel
        // fixtures retain their endpoints until their own immediate spawn.
        drop(self.nodes[index].sql_reservation.take());
        drop(self.nodes[index].replication_reservation.take());
        self.nodes[index].last_exit = None;
        let child = command.spawn().unwrap();
        self.nodes[index].last_pid = Some(child.id());
        self.nodes[index].child = Some(child);
        // This establishes actual child/listener readiness, not a sleep or a
        // TCP connection to another fixture. There is no port reallocation or
        // restart-on-failure that could hide the original startup error.
        self.wait_listener_ready(index);
    }

    fn child_live(&mut self, index: usize) -> Result<(), String> {
        let node = &mut self.nodes[index];
        let child = node
            .child
            .as_mut()
            .ok_or_else(|| format!("{} child absent", node.id))?;
        match child
            .try_wait()
            .map_err(|error| format!("{} try_wait: {error}", node.id))?
        {
            Some(status) => {
                node.last_exit = Some(status.to_string());
                Err(format!(
                    "{} pid={} exited before readiness: {status}; SQL={} replication={}",
                    node.id,
                    child.id(),
                    node.sql_port,
                    node.replication_port
                ))
            }
            None => Ok(()),
        }
    }
    fn listeners_ready(&self, index: usize) -> Result<(), String> {
        let node = &self.nodes[index];
        let rows = self.execute(index, "SELECT * FROM bicdb_region")?;
        let id = rows
            .first()
            .ok_or("SQL identity result is empty")?
            .get::<_, String>(1);
        if id != node.id {
            return Err(format!(
                "SQL endpoint identifies {id}, expected {}",
                node.id
            ));
        }
        let address = format!("127.0.0.1:{}", node.replication_port)
            .parse()
            .unwrap();
        let mut stream = TcpStream::connect_timeout(&address, Duration::from_millis(200))
            .map_err(|error| format!("{} replication connect: {error}", node.id))?;
        stream
            .set_read_timeout(Some(Duration::from_millis(500)))
            .map_err(|e| e.to_string())?;
        stream
            .set_write_timeout(Some(Duration::from_millis(500)))
            .map_err(|e| e.to_string())?;
        // A deliberately foreign cluster is refused before peer/epoch/contact
        // handling. This proves the real HA protocol listener is serving while
        // never renewing a primary heartbeat or submitting an election vote.
        send_frame(
            &mut stream,
            &ReplicationFrame::Hello {
                protocol_version: bicdb_pgwire::HA_REPLICATION_PROTOCOL_VERSION,
                cluster_id: "ha-cli-readiness-not-the-configured-cluster".into(),
                node_id: "readiness-observer".into(),
                epoch: 0,
            },
            64 * 1024,
        )
        .map_err(|e| e.to_string())?;
        match recv_frame(&mut stream, 64 * 1024).map_err(|e| e.to_string())? {
            ReplicationFrame::Error { code, message }
                if code == "replication_refused" && message == "cluster_id mismatch" =>
            {
                Ok(())
            }
            other => Err(format!(
                "{} unexpected replication readiness response: {other:?}",
                node.id
            )),
        }
    }
    fn wait_listener_ready(&mut self, index: usize) {
        let mut last_error = None;
        let ready = eventually(Duration::from_secs(30), || {
            if let Err(error) = self.child_live(index) {
                panic!("{error}\n{}", self.logs());
            }
            match self.listeners_ready(index) {
                Ok(()) => Some(()),
                Err(error) => {
                    last_error = Some(error);
                    None
                }
            }
        });
        self.child_live(index)
            .unwrap_or_else(|error| panic!("{error}\n{}", self.logs()));
        assert!(
            ready.is_some(),
            "{} listeners not ready: {last_error:?}\n{}",
            self.nodes[index].id,
            self.logs()
        );
    }
    fn require_all_live_and_listening(&mut self) {
        for index in 0..self.nodes.len() {
            self.wait_listener_ready(index);
        }
        // Check all actual process owners once more after the last listener
        // probe. This is a fixture prerequisite, not a replica catch-up proof.
        for index in 0..self.nodes.len() {
            self.child_live(index)
                .unwrap_or_else(|error| panic!("{error}\n{}", self.logs()));
        }
    }
    fn kill(&mut self, index: usize) {
        if let Some(mut child) = self.nodes[index].child.take() {
            let _ = child.kill();
            self.nodes[index].last_exit = Some(match child.wait() {
                Ok(status) => status.to_string(),
                Err(error) => format!("wait failed: {error}"),
            });
        }
    }

    fn execute(&self, index: usize, sql: &str) -> Result<Vec<tokio_postgres::Row>, String> {
        let port = self.nodes[index].sql_port;
        self.runtime.block_on(async move {
            let mut config = tokio_postgres::Config::new();
            config
                .host("127.0.0.1")
                .port(port)
                .user("postgres")
                .dbname("bicdb")
                .connect_timeout(Duration::from_secs(3));
            let (client, connection) = config
                .connect(tokio_postgres::NoTls)
                .await
                .map_err(|error| error.to_string())?;
            tokio::spawn(connection);
            let query = client.query(sql, &[]);
            tokio::time::timeout(Duration::from_secs(10), query)
                .await
                .map_err(|_| "query timed out".to_string())?
                .map_err(|error| match error.as_db_error() {
                    Some(db) => format!("{}: {}", db.code().code(), db.message()),
                    None => error.to_string(),
                })
        })
    }

    fn ids(&self, index: usize) -> BTreeSet<i32> {
        self.execute(index, "SELECT id FROM t")
            .unwrap()
            .iter()
            .map(|row| row.get::<_, i32>(0))
            .collect()
    }

    fn logs(&self) -> String {
        NODES
            .iter()
            .map(|id| {
                let log = std::fs::read_to_string(self.root.join(format!("{id}.log")))
                    .unwrap_or_default();
                let node = self.nodes.iter().find(|node| node.id == *id).unwrap();
                format!(
                    "== {id} SQL={} replication={} pid={:?} last_exit={:?}\n{log}",
                    node.sql_port, node.replication_port, node.last_pid, node.last_exit
                )
            })
            .collect::<Vec<_>>()
            .join("\n")
    }
}

impl Drop for Group {
    fn drop(&mut self) {
        for index in 0..self.nodes.len() {
            self.kill(index);
        }
        if std::thread::panicking() {
            // Process termination precedes this read, so the complete final
            // child logs survive TempDir cleanup in the original test output.
            eprintln!(
                "complete HA CLI child logs after owned teardown\n{}",
                self.logs()
            );
        }
    }
}

fn eventually<T>(timeout: Duration, mut attempt: impl FnMut() -> Option<T>) -> Option<T> {
    let end = Instant::now() + timeout;
    while Instant::now() < end {
        if let Some(value) = attempt() {
            return Some(value);
        }
        std::thread::sleep(Duration::from_millis(200));
    }
    None
}

#[test]
fn killing_the_primary_loses_no_acknowledged_write() {
    let root = tempfile::tempdir().unwrap();
    let mut group = Group::new(root.path());
    for index in 0..3 {
        group.start(index);
        std::thread::sleep(Duration::from_millis(500));
    }
    group.require_all_live_and_listening();
    eventually(Duration::from_secs(30), || {
        group
            .execute(0, "CREATE TABLE t (id INT PRIMARY KEY, v TEXT)")
            .ok()
    })
    .unwrap_or_else(|| panic!("the first primary never accepted writes\n{}", group.logs()));

    let mut acked = BTreeSet::new();
    let mut next = 0;
    let started = Instant::now();
    while started.elapsed() < Duration::from_secs(3) {
        next += 1;
        if group
            .execute(0, &format!("INSERT INTO t VALUES ({next}, 'v')"))
            .is_ok()
        {
            acked.insert(next);
        }
    }
    assert!(!acked.is_empty(), "no write acknowledged\n{}", group.logs());

    group.require_all_live_and_listening();
    group.kill(0);
    let killed = Instant::now();
    let primary = eventually(Duration::from_secs(45), || {
        next += 1;
        (1..3).find(|index| {
            let ok = group
                .execute(*index, &format!("INSERT INTO t VALUES ({next}, 'v')"))
                .is_ok();
            if ok {
                acked.insert(next);
            }
            ok
        })
    })
    .unwrap_or_else(|| panic!("no new primary after the kill\n{}", group.logs()));
    eprintln!(
        "{} took over {:.1}s after the kill",
        NODES[primary],
        killed.elapsed().as_secs_f64()
    );
    for _ in 0..20 {
        next += 1;
        if group
            .execute(primary, &format!("INSERT INTO t VALUES ({next}, 'v')"))
            .is_ok()
        {
            acked.insert(next);
        }
    }

    // Every acknowledged write survived the failover.
    let on_primary = group.ids(primary);
    let lost = acked.difference(&on_primary).collect::<Vec<_>>();
    assert!(
        lost.is_empty(),
        "lost acknowledged writes {lost:?}\n{}",
        group.logs()
    );

    // The old primary rejoins as a standby, catches up, and redirects writes.
    group.start(0);
    eventually(Duration::from_secs(45), || {
        (group.execute(0, "SELECT id FROM t").ok()?.len() == on_primary.len()).then_some(())
    })
    .unwrap_or_else(|| panic!("the old primary did not rejoin\n{}", group.logs()));
    let refused = group
        .execute(0, "INSERT INTO t VALUES (-1, 'stale')")
        .unwrap_err();
    assert!(refused.starts_with("25006"), "{refused}");
    // The cluster view agrees: one primary, two standbys following it.
    let mut status = Command::new(env!("CARGO_BIN_EXE_bicdb"));
    status.args(["ha", "status", "--json"]);
    for node in &group.nodes {
        status.args(["--node", &format!("127.0.0.1:{}", node.sql_port)]);
    }
    let output = status.output().unwrap();
    let view: serde_json::Value = serde_json::from_slice(&output.stdout)
        .unwrap_or_else(|error| panic!("{error}: {}", String::from_utf8_lossy(&output.stdout)));
    let roles = view["nodes"]
        .as_array()
        .unwrap()
        .iter()
        .map(|node| node["role"].as_str().unwrap_or("").to_string())
        .collect::<Vec<_>>();
    assert_eq!(roles[primary], "primary", "{view}");
    assert_eq!(
        roles.iter().filter(|role| *role == "standby").count(),
        2,
        "{view}"
    );
    for index in 0..3 {
        if index != primary {
            let standby = eventually(Duration::from_secs(20), || {
                let ids = group.ids(index);
                (ids == on_primary).then_some(ids)
            });
            assert!(
                standby.is_some(),
                "standby {} diverged from the primary\n{}",
                NODES[index],
                group.logs()
            );
        }
    }
}

#[test]
fn planned_switchover_preserves_all_acknowledged_writes_and_fences_the_source() {
    let root = tempfile::tempdir().unwrap();
    let mut group = Group::new(root.path());
    for index in 0..3 {
        group.start(index);
        std::thread::sleep(Duration::from_millis(500));
    }
    group.require_all_live_and_listening();
    eventually(Duration::from_secs(30), || {
        group
            .execute(0, "CREATE TABLE t (id INT PRIMARY KEY, v TEXT)")
            .ok()
    })
    .unwrap_or_else(|| panic!("primary never became writable\n{}", group.logs()));
    for id in 1..=5 {
        group
            .execute(0, &format!("INSERT INTO t VALUES ({id}, 'acknowledged')"))
            .unwrap();
    }
    group.require_all_live_and_listening();
    let output = Command::new(env!("CARGO_BIN_EXE_bicdb"))
        .args([
            "ha",
            "switchover",
            "--primary",
            &format!("127.0.0.1:{}", group.nodes[0].sql_port),
            "--target",
            &format!("127.0.0.1:{}", group.nodes[1].sql_port),
            "--target-node",
            "n2",
            "--timeout-secs",
            "45",
            "--json",
        ])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}\n{}",
        String::from_utf8_lossy(&output.stderr),
        group.logs()
    );
    let report: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(report["zero_loss"], true);
    assert_eq!(group.ids(1), (1..=5).collect());
    eventually(Duration::from_secs(20), || {
        group
            .execute(1, "INSERT INTO t VALUES (6, 'after handoff')")
            .ok()
    })
    .unwrap_or_else(|| panic!("target never became writable\n{}", group.logs()));
    assert!(group
        .execute(0, "INSERT INTO t VALUES (7, 'must refuse')")
        .is_err());
    eventually(Duration::from_secs(20), || {
        (group.ids(1) == (1..=6).collect()).then_some(())
    })
    .unwrap();
}

#[test]
fn held_endpoint_reservations_exclude_parallel_ha_groups() {
    let root = tempfile::tempdir().unwrap();
    let groups = std::thread::scope(|threads| {
        let first = threads.spawn(|| Group::new(root.path()));
        let second = threads.spawn(|| Group::new(root.path()));
        [first.join().unwrap(), second.join().unwrap()]
    });
    let ports = groups
        .iter()
        .flat_map(|group| {
            group
                .nodes
                .iter()
                .flat_map(|node| [node.sql_port, node.replication_port])
        })
        .collect::<Vec<_>>();
    assert_eq!(ports.iter().copied().collect::<BTreeSet<_>>().len(), 12);
    for port in ports {
        let error = TcpListener::bind(("127.0.0.1", port)).unwrap_err();
        assert_eq!(
            error.kind(),
            std::io::ErrorKind::AddrInUse,
            "held actual endpoint {port}"
        );
    }
}
#[test]
fn occupied_replication_listener_is_reported_before_ha_fault() {
    let root = tempfile::tempdir().unwrap();
    let mut group = Group::new(root.path());
    // Hold the ACTUAL selected replication endpoint through child startup.
    // No arbitrary guessed port or synthetic child outcome is used.
    let _occupied = group.nodes[0].replication_reservation.take().unwrap();
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| group.start(0)));
    let panic =
        result.expect_err("occupied HA listener must fail readiness before fault injection");
    let message = panic
        .downcast_ref::<String>()
        .map(String::as_str)
        .or_else(|| panic.downcast_ref::<&str>().copied())
        .unwrap_or("");
    assert!(
        message.contains("n1") && message.contains("exited before readiness"),
        "{message}"
    );
    let log = std::fs::read_to_string(root.path().join("n1.log")).unwrap();
    assert!(
        log.contains("Address already in use") || log.contains("address already in use"),
        "{log}"
    );
}
