use std::{
    fs,
    net::TcpListener,
    path::Path,
    process::{Child, Command, Stdio},
    time::{Duration, Instant},
};

use base64::Engine;
use bicdb_app_host::manifest::Release;
use serde_json::{json, Value};

const TOKEN: &str = "native-state-test-management-token-0000000000000000";
#[path = "support/state_network.rs"]
mod state_network;

struct Group {
    root: tempfile::TempDir,
    ports: Vec<u16>,
    children: Vec<Option<Child>>,
    worker: Option<Child>,
    client: reqwest::blocking::Client,
    network: Option<state_network::Network>,
    reservations: Vec<Option<TcpListener>>,
}
impl Drop for Group {
    fn drop(&mut self) {
        if std::thread::panicking() {
            for name in ["n0", "n1", "n2", "worker"] {
                eprintln!(
                    "{name}: {}",
                    fs::read_to_string(self.root.path().join(format!("{name}.log")))
                        .unwrap_or_default()
                );
            }
        }
        for child in self
            .children
            .iter_mut()
            .filter_map(Option::as_mut)
            .chain(self.worker.iter_mut())
        {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}
fn secret(path: &Path, text: &str) {
    bicdb_app_host::store::write_synced(path, text.as_bytes()).unwrap();
}
impl Group {
    fn start(remote: bool, live_partition: bool, existing: bool) -> Self {
        Self::start_options(remote, live_partition, existing, false)
    }
    fn start_options(remote: bool, live_partition: bool, existing: bool, edge: bool) -> Self {
        let root = tempfile::tempdir().unwrap();
        let listeners: Vec<_> = (0..16)
            .map(|_| TcpListener::bind("127.0.0.1:0").unwrap())
            .collect();
        let ports: Vec<_> = listeners
            .iter()
            .map(|listener| listener.local_addr().unwrap().port())
            .collect();
        let nodes: serde_json::Map<_, _> = (0..3).map(|index| (format!("n{index}"), json!({"replication":format!("127.0.0.1:{}", ports[index+3]), "endpoint":format!("http://127.0.0.1:{}", ports[index])}))).collect();
        secret(&root.path().join("management.token"), TOKEN);
        if edge {
            secret(&root.path().join("edge-sync.key"), &"ab".repeat(32));
        }
        secret(
            &root.path().join("worker.token"),
            "separate-worker-management-token-0000000000000000",
        );
        for index in 0..3 {
            let mut nodes = nodes.clone();
            if live_partition {
                for peer in 0..3 {
                    if peer != index {
                        nodes.get_mut(&format!("n{peer}")).unwrap()["replication"] =
                            json!(format!("127.0.0.1:{}", ports[7 + index * 3 + peer]));
                    }
                }
            }
            secret(&root.path().join(format!("state-{index}.json")), &json!({"format":1,"applications":{"counter":{
                "cluster_id":"application-test", "node_id":format!("n{index}"), "initial_primary":"n0", "nodes":nodes,
                "dev_localhost_plaintext":true,"failover_timeout_ms":5000,"synchronous_timeout_ms":10000,
                "edge_sync_key_file":if edge {Some("edge-sync.key")} else {None}
            }}}).to_string());
        }
        secret(&root.path().join("fleet.json"), &json!({"format":1,"workers":{"worker":{"endpoint":format!("http://127.0.0.1:{}",ports[6]),"token_file":"worker.token","region":"test","capacity":1,"isolation":"process"}}}).to_string());
        let mut reservations = listeners.into_iter().map(Some).collect::<Vec<_>>();
        let network =
            live_partition.then(|| state_network::Network::open(&ports, &mut reservations));
        let mut group = Self {
            root,
            ports,
            children: (0..3).map(|_| None).collect(),
            worker: None,
            client: reqwest::blocking::Client::builder()
                .no_proxy()
                .timeout(Duration::from_secs(10))
                .build()
                .unwrap(),
            network,
            reservations,
        };
        if existing {
            let mut command = Command::new(env!("CARGO_BIN_EXE_bic"));
            command
                .arg("--data")
                .arg(group.root.path().join("n0"))
                .arg("--token-file")
                .arg(group.root.path().join("management.token"))
                .args([
                    "serve",
                    "--listen",
                    &format!("127.0.0.1:{}", group.ports[0]),
                ]);
            group.reservations[0].take();
            group.children[0] = Some(group.spawn(command, "n0"));
            group.wait(|| group.status(0).is_some());
            let release = Release::read(
                &Path::new(env!("CARGO_MANIFEST_DIR"))
                    .join("../../examples/platform/counter/bic.json"),
            )
            .unwrap();
            group.deploy(0, &release);
            assert_eq!(
                group.post(0, "increment", "pre-upgrade"),
                json!({"value":1})
            );
            group.kill(0);
        }
        if remote {
            let mut command = Command::new(env!("CARGO_BIN_EXE_bic"));
            command
                .arg("--data")
                .arg(group.root.path().join("worker"))
                .arg("--token-file")
                .arg(group.root.path().join("worker.token"))
                .args([
                    "worker",
                    "--listen",
                    &format!("127.0.0.1:{}", group.ports[6]),
                    "--state-endpoint",
                    &group.base(0),
                ]);
            for index in 1..3 {
                command.args(["--state-peer", &group.base(index)]);
            }
            group.reservations[6].take();
            group.worker = Some(group.spawn(command, "worker"));
            group.wait(|| {
                group
                    .client
                    .get(format!("http://127.0.0.1:{}/health", group.ports[6]))
                    .send()
                    .is_ok_and(|response| response.status().is_success())
            });
        }
        for index in 0..3 {
            group.start_node(index, remote);
        }
        group.wait(|| {
            (0..3).all(|index| group.status(index).is_some())
                && group
                    .status(0)
                    .is_some_and(|status| status[0]["state"]["writer"] == true)
        });
        group
    }
    fn spawn(&self, mut command: Command, name: &str) -> Child {
        command.env("TOKIO_WORKER_THREADS", "2");
        let log = fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(self.root.path().join(format!("{name}.log")))
            .unwrap();
        command
            .stdout(Stdio::from(log.try_clone().unwrap()))
            .stderr(Stdio::from(log))
            .spawn()
            .unwrap()
    }
    fn start_node(&mut self, index: usize, remote: bool) {
        self.reservations[index].take();
        self.reservations[index + 3].take();
        let mut command = Command::new(env!("CARGO_BIN_EXE_bic"));
        command
            .arg("--data")
            .arg(self.root.path().join(format!("n{index}")))
            .arg("--token-file")
            .arg(self.root.path().join("management.token"))
            .args([
                "serve",
                "--listen",
                &format!("127.0.0.1:{}", self.ports[index]),
            ])
            .arg("--state-config")
            .arg(self.root.path().join(format!("state-{index}.json")));
        if remote {
            command
                .arg("--fleet-config")
                .arg(self.root.path().join("fleet.json"));
        }
        self.children[index] = Some(self.spawn(command, &format!("n{index}")));
    }
    fn kill(&mut self, index: usize) {
        let mut child = self.children[index].take().unwrap();
        child.kill().unwrap();
        child.wait().unwrap();
    }
    fn base(&self, index: usize) -> String {
        format!("http://127.0.0.1:{}", self.ports[index])
    }
    fn status(&self, index: usize) -> Option<Value> {
        self.client
            .get(format!("{}/v1/apps", self.base(index)))
            .bearer_auth(TOKEN)
            .send()
            .ok()?
            .error_for_status()
            .ok()?
            .json()
            .ok()
    }
    #[track_caller]
    fn wait(&self, mut condition: impl FnMut() -> bool) {
        let start = Instant::now();
        while !condition() {
            if start.elapsed() > Duration::from_secs(50) {
                for name in ["n0", "n1", "n2", "worker"] {
                    eprintln!(
                        "{name}: {}",
                        fs::read_to_string(self.root.path().join(format!("{name}.log")))
                            .unwrap_or_default()
                    );
                }
                panic!("application state condition timed out");
            }
            std::thread::sleep(Duration::from_millis(100));
        }
    }
    fn state(&self, index: usize) -> Option<Value> {
        self.client
            .get(format!("{}/apps/counter", self.base(index)))
            .send()
            .ok()?
            .error_for_status()
            .ok()?
            .json()
            .ok()
    }
    fn post(&self, index: usize, path: &str, key: &str) -> Value {
        let response = self
            .client
            .post(format!("{}/apps/counter/{path}", self.base(index)))
            .header("idempotency-key", key)
            .send()
            .unwrap();
        assert!(
            response.status().is_success(),
            "{}",
            response.text().unwrap()
        );
        response.json().unwrap()
    }
    fn deploy(&self, index: usize, release: &Release) {
        let response = self
            .client
            .post(format!("{}/v1/apps", self.base(index)))
            .bearer_auth(TOKEN)
            .timeout(Duration::from_secs(30))
            .json(release)
            .send()
            .unwrap();
        assert!(
            response.status().is_success(),
            "{}",
            response.text().unwrap()
        );
    }
}

fn scenario(remote: bool, live_partition: bool) {
    let mut group = Group::start(remote, live_partition, false);
    let mut release = Release::read(
        &Path::new(env!("CARGO_MANIFEST_DIR")).join("../../examples/platform/counter/bic.json"),
    )
    .unwrap();
    if remote {
        release.manifest.placement = Some(bicdb_app_host::manifest::Placement {
            region: "test".into(),
        });
        let response=group.client.post(format!("http://127.0.0.1:{}/v1/executions",group.ports[6])).bearer_auth("separate-worker-management-token-0000000000000000")
            .json(&json!({"id":"f".repeat(32),"token":"a".repeat(64),"release":release,"state_endpoint":format!("http://127.0.0.1:{}",group.ports[15])})).send().unwrap();
        assert_eq!(
            response.status(),
            400,
            "a launch cannot select an unpinned state origin"
        );
    }
    // Versioned behavior proves that a successor recovers code from its database,
    // without a deployment upload to that host.
    let source = String::from_utf8(release.decode("app.mjs").unwrap()).unwrap();
    let versioned = |version: u32| {
        source.replace("const path = new URL(request.url).pathname;", &format!("const path = new URL(request.url).pathname; if(path === '/version') return Response.json({version});"))
    };
    release.files.insert(
        "app.mjs".into(),
        base64::engine::general_purpose::STANDARD.encode(versioned(1)),
    );
    assert_eq!(
        group
            .client
            .post(format!("{}/v1/apps", group.base(1)))
            .json(&release)
            .send()
            .unwrap()
            .status(),
        401
    );
    group.deploy(1, &release);
    group.wait(|| (0..3).all(|index| group.state(index).is_some()));
    assert_eq!(
        group.post(2, "increment", "request-before"),
        json!({"value":1})
    );
    let job = group.post(1, "jobs", "job-before");
    group.wait(|| group.state(2).is_some_and(|rows| rows[0]["value"] == 1));
    release.manifest.version = "2".into();
    release.files.insert(
        "app.mjs".into(),
        base64::engine::general_purpose::STANDARD.encode(versioned(2)),
    );
    group.deploy(2, &release);
    let digest = release.digest().unwrap();
    group.wait(|| {
        (0..3).all(|index| {
            group
                .status(index)
                .is_some_and(|status| status[0]["deployment"]["release"] == digest)
        })
    });
    if let Some(network) = &group.network {
        network.isolate();
    } else {
        group.kill(0);
    }
    let mut primary = None;
    group.wait(|| {
        primary = (1..3).find(|index| {
            group.status(*index).is_some_and(|status| {
                status[0]["state"]["writer"] == true && status[0]["running"] == true
            })
        });
        primary.is_some()
    });
    let primary = primary.unwrap();
    let other = if primary == 1 { 2 } else { 1 };
    group.wait(|| group.state(other).is_some());
    assert_eq!(
        group
            .client
            .get(format!("{}/apps/counter/version", group.base(other)))
            .send()
            .unwrap()
            .json::<Value>()
            .unwrap(),
        json!(2)
    );
    assert_eq!(
        group.post(other, "increment", "request-before"),
        json!({"value":1})
    );
    assert_eq!(group.post(other, "jobs", "job-before")["id"], job["id"]);
    if let Some(network) = &group.network {
        assert_eq!(group.status(0).unwrap()[0]["running"], false);
        assert_eq!(group.status(0).unwrap()[0]["state"]["writer"], false);
        network.heal();
    } else {
        group.start_node(0, remote);
    }
    group.wait(|| {
        group.state(0).is_some_and(|rows| {
            rows == json!([{"id":"jobs","value":1},{"id":"requests","value":1}])
        })
    });
    assert_eq!(group.status(0).unwrap()[0]["running"], false);
    for operation in ["stop", "start", "rollback"] {
        let response = group
            .client
            .post(format!("{}/v1/apps/counter/{operation}", group.base(0)))
            .bearer_auth(TOKEN)
            .send()
            .unwrap();
        assert!(
            response.status().is_success(),
            "{operation}: {}",
            response.text().unwrap()
        );
        if operation == "stop" {
            group.wait(|| {
                (0..3).all(|index| {
                    group.status(index).is_some_and(|status| {
                        status[0]["deployment"]["enabled"] == false && status[0]["running"] == false
                    })
                })
            });
        }
    }
    group.wait(|| {
        group
            .client
            .get(format!("{}/apps/counter/version", group.base(other)))
            .send()
            .is_ok_and(|response| {
                response
                    .json::<Value>()
                    .is_ok_and(|version| version == json!(1))
            })
    });
    if !remote {
        let passphrase = "state-recovery-test-passphrase-0000000000000000000";
        let response = group
            .client
            .post(format!("{}/v1/apps/counter/backup", group.base(0)))
            .bearer_auth(TOKEN)
            .json(&json!({"passphrase":passphrase}))
            .timeout(Duration::from_secs(45))
            .send()
            .unwrap()
            .error_for_status()
            .unwrap();
        let archive = group.root.path().join("state.bicbak");
        bicdb_app_host::store::write_synced(&archive, &response.bytes().unwrap()).unwrap();
        let key = group.root.path().join("backup.key");
        secret(&key, passphrase);
        let recovered = group.root.path().join("recovered");
        let output = Command::new(env!("CARGO_BIN_EXE_bic"))
            .arg("--data")
            .arg(&recovered)
            .arg("restore")
            .arg(&archive)
            .arg("--passphrase-file")
            .arg(&key)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        let mut db = bicdb_core::BicDb::open(recovered.join("apps/counter/database/data")).unwrap();
        let result = bicdb_sql::SqlSession::new(&mut db)
            .execute("SELECT payload FROM __bic_platform_deployment WHERE id = 'current'")
            .unwrap();
        let bicdb_sql::SqlValue::String(payload) = &result.rows[0][0] else {
            panic!("deployment payload missing")
        };
        assert_eq!(
            serde_json::from_str::<Value>(payload).unwrap()["enabled"],
            false,
            "native enrollment must retain the inactive restore"
        );
        drop(db);

        let mut broken = release.clone();
        broken.manifest.version = "3".into();
        broken.manifest.files.push("002.sql".into());
        broken.manifest.migrations.push("002.sql".into());
        broken.files.insert(
            "002.sql".into(),
            base64::engine::general_purpose::STANDARD
                .encode("CREATE TABLE extra (id INT PRIMARY KEY); INSERT INTO absent VALUES (1)"),
        );
        let response = group
            .client
            .post(format!("{}/v1/apps", group.base(0)))
            .bearer_auth(TOKEN)
            .json(&broken)
            .send()
            .unwrap();
        assert_eq!(
            response.status(),
            400,
            "a failed migration must reject activation"
        );
        let broken_digest = broken.digest().unwrap();
        group.wait(|| {
            (0..3).all(|index| {
                group.status(index).is_some_and(|status| {
                    status[0]["deployment"]["release"] == broken_digest
                        && status[0]["deployment"]["enabled"] == false
                        && status[0]["running"] == false
                })
            })
        });
        group.kill(primary);
        let survivors: Vec<_> = (0..3).filter(|index| *index != primary).collect();
        let mut successor = None;
        group.wait(|| {
            successor = survivors.iter().copied().find(|index| {
                group
                    .status(*index)
                    .is_some_and(|status| status[0]["state"]["writer"] == true)
            });
            successor.is_some()
        });
        for index in &survivors {
            assert!(
                group.state(*index).is_none(),
                "failover must not reactivate old code after a migration failure"
            );
        }
        broken.files.insert(
            "002.sql".into(),
            base64::engine::general_purpose::STANDARD
                .encode("CREATE TABLE extra (id INT PRIMARY KEY); INSERT INTO extra VALUES (1)"),
        );
        group.deploy(successor.unwrap(), &broken);
        group.wait(|| {
            group
                .state(successor.unwrap())
                .is_some_and(|rows| rows[1]["value"] == 1)
        });
    }
}

#[test]
fn deployment_catalog_routes_and_jobs_follow_the_native_state_primary() {
    scenario(false, false);
}

#[test]
fn remote_compute_uses_a_pinned_successor_state_origin() {
    scenario(true, false);
}

#[test]
fn a_live_isolated_writer_retires_compute_before_rejoining_as_a_standby() {
    scenario(true, true);
}

#[test]
fn an_existing_standalone_catalog_enrolls_without_losing_data_or_receipts() {
    let group = Group::start(false, false, true);
    group.wait(|| {
        (0..3).all(|index| {
            group.state(index).is_some_and(|rows| {
                rows == json!([{"id":"jobs","value":0},{"id":"requests","value":1}])
            })
        })
    });
    assert_eq!(
        group.post(2, "increment", "pre-upgrade"),
        json!({"value":1})
    );
    assert_eq!(
        group.post(1, "increment", "after-upgrade"),
        json!({"value":2})
    );
}

fn edge_failover(live_partition: bool) {
    let mut group = Group::start_options(false, live_partition, false, true);
    let edge = format!("http://127.0.0.1:{}", group.ports[6]);
    let mut command = Command::new(env!("CARGO_BIN_EXE_bic"));
    command
        .arg("--data")
        .arg(group.root.path().join("edge"))
        .arg("--token-file")
        .arg(group.root.path().join("management.token"))
        .args([
            "serve",
            "--listen",
            &format!("127.0.0.1:{}", group.ports[6]),
        ]);
    group.reservations[6].take();
    group.worker = Some(group.spawn(command, "worker"));
    group.wait(|| {
        group
            .client
            .get(format!("{edge}/health"))
            .send()
            .is_ok_and(|response| response.status().is_success())
    });
    let mut release = Release::read(
        &Path::new(env!("CARGO_MANIFEST_DIR")).join("../../examples/platform/counter/bic.json"),
    )
    .unwrap();
    release.manifest.sync.insert(
        "notes".into(),
        bicdb_app_host::manifest::TableSync::AppendOnly,
    );
    release.manifest.files.push("002-notes.sql".into());
    release.manifest.migrations.push("002-notes.sql".into());
    release.files.insert(
        "002-notes.sql".into(),
        base64::engine::general_purpose::STANDARD
            .encode("CREATE TABLE notes (id TEXT PRIMARY KEY, body TEXT)"),
    );
    release.files.insert("app.mjs".into(), base64::engine::general_purpose::STANDARD.encode(r#"
export default { async fetch(request, env) {
  if (request.method === 'GET' && new URL(request.url).pathname === '/live') {
    return env.DB.stream('SELECT * FROM notes ORDER BY id', [], {signal: request.signal});
  }
  if (request.method === 'POST') {
    const { id } = await request.json();
    return Response.json(await env.DB.query('INSERT INTO notes VALUES ($1, $2) RETURNING id', [id, id]));
  }
  return Response.json(await env.DB.query('SELECT * FROM notes ORDER BY id'));
}};
"#));
    group.deploy(1, &release);
    let response = group
        .client
        .post(format!("{edge}/v1/apps"))
        .bearer_auth(TOKEN)
        .json(&release)
        .send()
        .unwrap();
    assert!(
        response.status().is_success(),
        "{}",
        response.text().unwrap()
    );
    let identity = |base: &str| -> Value {
        group
            .client
            .get(format!("{base}/v1/apps/counter/identity"))
            .bearer_auth(TOKEN)
            .send()
            .unwrap()
            .error_for_status()
            .unwrap()
            .json()
            .unwrap()
    };
    let central = identity(&group.base(2));
    let edge_id = identity(&edge);
    let pair_token = "edge-state-pair-transport-token-0000000000000000000";
    for (local, remote, pin) in [
        (group.base(1), edge.clone(), edge_id.clone()),
        (edge.clone(), group.base(2), central.clone()),
    ] {
        let response = group
            .client
            .post(format!("{local}/v1/apps/counter/peers"))
            .bearer_auth(TOKEN)
            .json(
                &json!({"node_id":pin["node_id"],"public_key":pin["public_key"],
                "endpoint":remote,"token":pair_token}),
            )
            .send()
            .unwrap();
        assert!(
            response.status().is_success(),
            "{}",
            response.text().unwrap()
        );
    }
    let write = |base: &str, id: &str| {
        let response = group
            .client
            .post(format!("{base}/apps/counter"))
            .json(&json!({"id":id}))
            .send()
            .unwrap();
        assert!(
            response.status().is_success(),
            "{}",
            response.text().unwrap()
        );
    };
    let read = |base: &str| -> Option<Value> {
        group
            .client
            .get(format!("{base}/apps/counter"))
            .send()
            .ok()?
            .error_for_status()
            .ok()?
            .json()
            .ok()
    };
    write(&group.base(1), "central-before");
    write(&edge, "edge-before");
    group.wait(|| {
        read(&edge).is_some_and(|rows| rows.as_array().unwrap().len() == 2)
            && read(&group.base(2)).is_some_and(|rows| rows.as_array().unwrap().len() == 2)
    });
    let mut live = if live_partition {
        use std::io::BufRead;
        let response = group
            .client
            .get(format!("{}/apps/counter/live", group.base(0)))
            .send()
            .unwrap()
            .error_for_status()
            .unwrap();
        let mut reader = std::io::BufReader::new(response);
        let mut initial = String::new();
        assert!(reader.read_line(&mut initial).unwrap() > 0 && initial.starts_with("data: "));
        let mut delimiter = String::new();
        reader.read_line(&mut delimiter).unwrap();
        Some(reader)
    } else {
        None
    };
    if let Some(network) = &group.network {
        let isolated_at = Instant::now();
        network.isolate();
        // The previous writer is still reachable. An indeterminate local
        // commit must never be advertised to an independent edge.
        let result = group
            .client
            .post(format!("{}/apps/counter", group.base(0)))
            .json(&json!({"id":"unacknowledged"}))
            .send();
        assert!(result.is_err() || !result.unwrap().status().is_success());
        let response = group
            .client
            .post(format!("{}/sync/counter/vector", group.base(0)))
            .bearer_auth(pair_token)
            .json(&edge_id)
            .send()
            .unwrap();
        assert!(!response.status().is_success());
        if let Some(reader) = &mut live {
            use std::io::BufRead;
            let mut tail = String::new();
            let ended = reader.read_line(&mut tail);
            assert!(
                isolated_at.elapsed() < Duration::from_secs(8),
                "authority loss did not close the live stream before its request deadline"
            );
            assert!(ended.is_err() || ended.unwrap() == 0);
        }
    } else {
        // Only the failed writer has the original standalone grant caches.
        group.kill(0);
    }
    let mut primary = None;
    group.wait(|| {
        primary = (1..3).find(|index| {
            group.status(*index).is_some_and(|status| {
                status[0]["state"]["writer"] == true && status[0]["running"] == true
            })
        });
        primary.is_some()
    });
    let primary = primary.unwrap();
    let other = if primary == 1 { 2 } else { 1 };
    // Writer recovery and standby route discovery are separate events.
    // Reads may receive a transition response until the follower hears the
    // elected writer; retry the read before exercising the recovered route.
    let mut identity = None;
    group.wait(|| {
        identity = group
            .client
            .get(format!("{}/v1/apps/counter/identity", group.base(other)))
            .bearer_auth(TOKEN)
            .send()
            .ok()
            .and_then(|response| response.error_for_status().ok())
            .and_then(|response| response.json::<Value>().ok());
        identity.is_some()
    });
    let identity = identity.unwrap();
    assert_eq!(
        identity, central,
        "physical promotion retains the pinned logical author"
    );
    for (base, id) in [
        (group.base(other), "central-after"),
        (edge.clone(), "edge-after"),
    ] {
        let response = group
            .client
            .post(format!("{base}/apps/counter"))
            .json(&json!({"id":id}))
            .send()
            .unwrap();
        assert!(
            response.status().is_success(),
            "{}",
            response.text().unwrap()
        );
    }
    group.wait(|| {
        [edge.clone(), group.base(other)].iter().all(|base| {
            group
                .client
                .get(format!("{base}/apps/counter"))
                .send()
                .ok()
                .and_then(|response| response.error_for_status().ok())
                .and_then(|response| response.json::<Value>().ok())
                .is_some_and(|rows| rows.as_array().unwrap().len() == 4)
        })
    });
    let response = group
        .client
        .delete(format!(
            "{}/v1/apps/counter/peers/{}",
            group.base(other),
            edge_id["node_id"].as_str().unwrap()
        ))
        .bearer_auth(TOKEN)
        .send()
        .unwrap();
    assert!(
        response.status().is_success(),
        "{}",
        response.text().unwrap()
    );
    // A second election must not resurrect the revoked grant from host files.
    if let Some(network) = &group.network {
        network.heal();
    } else {
        group.start_node(0, false);
    }
    group.wait(|| {
        // A cached release already exists on the returning host. Wait until
        // it has actually followed the new data epoch before removing the
        // current writer and relying on the restored majority.
        group.status(0).is_some_and(|status| {
            status[0]["running"] == false
                && status[0]["state"]["primary"] == format!("n{primary}")
                && status[0]["deployment"]["release"] == release.digest().unwrap()
        })
    });
    group.kill(primary);
    let mut next = None;
    group.wait(|| {
        next = [0, other].into_iter().find(|index| {
            group.status(*index).is_some_and(|status| {
                status[0]["state"]["writer"] == true && status[0]["running"] == true
            })
        });
        next.is_some()
    });
    let next = next.unwrap();
    let response = group
        .client
        .post(format!("{}/sync/counter/vector", group.base(next)))
        .bearer_auth(pair_token)
        .json(&edge_id)
        .send()
        .unwrap();
    assert_eq!(response.status(), 401);
    let peers: Value = group
        .client
        .get(format!("{}/v1/apps/counter/peers", group.base(next)))
        .bearer_auth(TOKEN)
        .send()
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .unwrap();
    assert_eq!(peers, json!([]));
}

#[test]
fn signed_edge_sync_and_peer_revocation_survive_state_failover() {
    edge_failover(false);
}

#[test]
fn an_http_reachable_isolated_writer_cannot_export_unacknowledged_edge_history() {
    edge_failover(true);
}
