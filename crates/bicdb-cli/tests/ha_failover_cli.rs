//! Kill-the-primary: a three-node group with synchronous replication and
//! automatic failover loses no acknowledged write when its primary is killed,
//! elects a new primary, and takes the old one back as a standby.
use std::collections::BTreeSet;
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

const NODES: [&str; 3] = ["n1", "n2", "n3"];

struct Node {
    id: &'static str,
    sql_port: u16,
    replication_port: u16,
    child: Option<Child>,
}

struct Group {
    root: PathBuf,
    nodes: Vec<Node>,
    runtime: tokio::runtime::Runtime,
}

fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

impl Group {
    fn new(root: &Path) -> Self {
        let nodes = NODES
            .iter()
            .map(|id| Node {
                id,
                sql_port: free_port(),
                replication_port: free_port(),
                child: None,
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
        self.nodes[index].child = Some(command.spawn().unwrap());
    }

    fn kill(&mut self, index: usize) {
        if let Some(mut child) = self.nodes[index].child.take() {
            let _ = child.kill();
            let _ = child.wait();
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
                let tail = log.lines().rev().take(40).collect::<Vec<_>>();
                format!(
                    "== {id}\n{}",
                    tail.into_iter().rev().collect::<Vec<_>>().join("\n")
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
