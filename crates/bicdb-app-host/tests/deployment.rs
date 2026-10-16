use std::net::TcpListener;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use base64::Engine;
use bicdb_app_host::manifest::Release;
use serde_json::{json, Value};

struct Service {
    child: Child,
    root: tempfile::TempDir,
    port: u16,
    client: reqwest::blocking::Client,
}
impl Drop for Service {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}
impl Service {
    fn start() -> Self {
        let root = tempfile::tempdir().unwrap();
        let port = TcpListener::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap()
            .port();
        let child = spawn(root.path(), port);
        let service = Self {
            child,
            root,
            port,
            client: reqwest::blocking::Client::builder()
                .timeout(Duration::from_secs(15))
                .build()
                .unwrap(),
        };
        service.ready();
        service
    }
    fn base(&self) -> String {
        format!("http://127.0.0.1:{}", self.port)
    }
    fn ready(&self) {
        wait(|| {
            self.client
                .get(format!("{}/health", self.base()))
                .send()
                .is_ok_and(|r| r.status().is_success())
        });
    }
    fn token(&self) -> String {
        std::fs::read_to_string(self.root.path().join("management.token"))
            .unwrap()
            .trim()
            .into()
    }
    fn deploy(&self, release: &Release) -> reqwest::blocking::Response {
        self.client
            .post(format!("{}/v1/apps", self.base()))
            .bearer_auth(self.token())
            .timeout(Duration::from_secs(35))
            .json(release)
            .send()
            .unwrap()
    }
    fn state(&self) -> Value {
        self.client
            .get(format!("{}/apps/counter", self.base()))
            .send()
            .unwrap()
            .error_for_status()
            .unwrap()
            .json()
            .unwrap()
    }
    fn restart(&mut self) {
        self.child.kill().unwrap();
        self.child.wait().unwrap();
        self.child = spawn(self.root.path(), self.port);
        self.ready();
    }
}
fn spawn(root: &std::path::Path, port: u16) -> Child {
    Command::new(env!("CARGO_BIN_EXE_bic"))
        .arg("--data")
        .arg(root)
        .args(["serve", "--listen", &format!("127.0.0.1:{port}")])
        .stdout(Stdio::null())
        .stderr(Stdio::inherit())
        .spawn()
        .unwrap()
}
fn wait(mut condition: impl FnMut() -> bool) {
    let started = Instant::now();
    while !condition() {
        assert!(
            started.elapsed() < Duration::from_secs(25),
            "condition did not become true"
        );
        std::thread::sleep(Duration::from_millis(50));
    }
}
fn fixture() -> Release {
    Release::read(
        &std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../examples/platform/counter/bic.json"),
    )
    .unwrap()
}

#[test]
fn deploy_crash_restart_jobs_upgrade_and_route_authorization() {
    let mut service = Service::start();
    let package = fixture();
    let unauthorized = service
        .client
        .post(format!("{}/v1/apps", service.base()))
        .json(&package)
        .send()
        .unwrap();
    assert_eq!(unauthorized.status(), 401);
    let response = service.deploy(&package);
    assert!(
        response.status().is_success(),
        "{}",
        response.text().unwrap()
    );
    assert_eq!(
        service.state(),
        json!([{"id":"jobs","value":0},{"id":"requests","value":0}])
    );
    for _ in 0..2 {
        let result: Value = service
            .client
            .post(format!("{}/apps/counter/increment", service.base()))
            .header("idempotency-key", "request-one")
            .send()
            .unwrap()
            .error_for_status()
            .unwrap()
            .json()
            .unwrap();
        assert_eq!(result, json!({"value":1}));
    }
    assert_eq!(
        service
            .client
            .post(format!("{}/internal/counter/query", service.base()))
            .json(&json!({"statements":[{"sql":"DELETE FROM counters"}]}))
            .send()
            .unwrap()
            .status(),
        401
    );
    assert_eq!(
        service
            .client
            .get(format!("{}/apps/counter/__bic/ready", service.base()))
            .header("x-bic-internal-token", service.token())
            .send()
            .unwrap()
            .status(),
        404
    );
    let mut job_id = None;
    for _ in 0..2 {
        let result: Value = service
            .client
            .post(format!("{}/apps/counter/jobs", service.base()))
            .header("idempotency-key", "job-one")
            .send()
            .unwrap()
            .error_for_status()
            .unwrap()
            .json()
            .unwrap();
        if let Some(id) = &job_id {
            assert_eq!(&result["id"], id);
        } else {
            job_id = Some(result["id"].clone());
        }
    }
    wait(|| service.state()[0]["value"] == 1);
    service.restart();
    assert_eq!(
        service.state(),
        json!([{"id":"jobs","value":1},{"id":"requests","value":1}])
    );
    let mut upgrade = package.clone();
    upgrade.manifest.version = "2.0.0".into();
    assert!(service.deploy(&upgrade).status().is_success());
    assert_eq!(service.state()[1]["value"], 1);
    let rollback = service
        .client
        .post(format!("{}/v1/apps/counter/rollback", service.base()))
        .bearer_auth(service.token())
        .send()
        .unwrap();
    assert!(
        rollback.status().is_success(),
        "{}",
        rollback.text().unwrap()
    );
    let mut broken = package.clone();
    broken.manifest.version = "broken".into();
    broken.files.insert(
        "app.mjs".into(),
        base64::engine::general_purpose::STANDARD.encode("throw new Error('startup failed')"),
    );
    assert!(!service.deploy(&broken).status().is_success());
    assert_eq!(
        service
            .client
            .get(format!("{}/apps/counter", service.base()))
            .send()
            .unwrap()
            .status(),
        503
    );
    assert!(service.deploy(&package).status().is_success());
    assert_eq!(service.state()[1]["value"], 1);
    let output = Command::new(env!("CARGO_BIN_EXE_bic"))
        .arg("--data")
        .arg(service.root.path())
        .args(["--endpoint", &service.base(), "list"])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(String::from_utf8_lossy(&output.stdout).contains("counter"));
}

#[test]
fn two_platforms_continue_offline_and_automatically_reconnect_with_pinned_peers() {
    let mut primary = Service::start();
    let edge = Service::start();
    let mut release = fixture();
    release.manifest.name = "notes".into();
    release.manifest.sync.insert(
        "notes".into(),
        bicdb_app_host::manifest::TableSync::AppendOnly,
    );
    release.files.insert(
        "001-counter.sql".into(),
        base64::engine::general_purpose::STANDARD
            .encode("CREATE TABLE notes (id TEXT PRIMARY KEY, body TEXT)"),
    );
    release.files.insert(
        "app.mjs".into(),
        base64::engine::general_purpose::STANDARD.encode(
            r#"
        export default {
          async fetch(request, env) {
            if (request.method === 'POST') {
              const note = await request.json();
              await env.DB.query('INSERT INTO notes VALUES ($1, $2)', [note.id, note.body]);
            }
            return Response.json(await env.DB.query('SELECT * FROM notes ORDER BY id'));
          }
        }
    "#,
        ),
    );
    for service in [&primary, &edge] {
        let response = service.deploy(&release);
        assert!(
            response.status().is_success(),
            "{}",
            response.text().unwrap()
        );
    }
    let write = |service: &Service, id: &str| {
        service
            .client
            .post(format!("{}/apps/notes", service.base()))
            .json(&json!({"id":id,"body":id}))
            .send()
            .unwrap()
            .error_for_status()
            .unwrap();
    };
    let read = |service: &Service| -> Vec<Value> {
        service
            .client
            .get(format!("{}/apps/notes", service.base()))
            .send()
            .unwrap()
            .error_for_status()
            .unwrap()
            .json()
            .unwrap()
    };
    write(&primary, "a");
    write(&edge, "b");
    assert_eq!(read(&primary).len(), 1);
    assert_eq!(read(&edge).len(), 1);
    let identity = |service: &Service| -> Value {
        service
            .client
            .get(format!("{}/v1/apps/notes/identity", service.base()))
            .bearer_auth(service.token())
            .send()
            .unwrap()
            .error_for_status()
            .unwrap()
            .json()
            .unwrap()
    };
    let left = identity(&primary);
    let right = identity(&edge);
    let peer_token = "application-pair-token-with-at-least-32-characters";
    for (service, remote, remote_id) in [(&primary, &edge, &right), (&edge, &primary, &left)] {
        let response = service.client.post(format!("{}/v1/apps/notes/peers", service.base())).bearer_auth(service.token())
            .json(&json!({"node_id":remote_id["node_id"],"public_key":remote_id["public_key"],"endpoint":remote.base(),"token":peer_token})).send().unwrap();
        assert!(
            response.status().is_success(),
            "{}",
            response.text().unwrap()
        );
    }
    wait(|| read(&primary).len() == 2 && read(&edge).len() == 2);
    primary.child.kill().unwrap();
    primary.child.wait().unwrap();
    write(&edge, "c");
    assert_eq!(read(&edge).len(), 3);
    primary.child = spawn(primary.root.path(), primary.port);
    primary.ready();
    wait(|| read(&primary).len() == 3);
    assert_eq!(read(&primary), read(&edge));
    let response = primary
        .client
        .post(format!("{}/sync/notes/vector", primary.base()))
        .bearer_auth(primary.token())
        .json(&right)
        .send()
        .unwrap();
    assert_eq!(
        response.status(),
        401,
        "management token does not grant peer access"
    );
}

#[test]
fn execution_deadline_replaces_a_blocked_runtime_without_losing_database_state() {
    let service = Service::start();
    let mut release = fixture();
    release.manifest.resources.request_timeout_ms = 250;
    release.files.insert(
        "app.mjs".into(),
        base64::engine::general_purpose::STANDARD.encode(
            r#"
        export default { async fetch(request, env) {
            if (new URL(request.url).pathname === '/hang') { while (true) {} }
            return Response.json(await env.DB.query('SELECT * FROM counters ORDER BY id'));
        }};
    "#,
        ),
    );
    let response = service.deploy(&release);
    assert!(
        response.status().is_success(),
        "{}",
        response.text().unwrap()
    );
    let response = service
        .client
        .get(format!("{}/apps/counter/hang", service.base()))
        .send()
        .unwrap();
    assert_eq!(response.status(), 504);
    wait(|| {
        service
            .client
            .get(format!("{}/apps/counter", service.base()))
            .send()
            .is_ok_and(|r| r.status().is_success())
    });
    assert_eq!(service.state()[0]["value"], 0);
}

#[test]
fn crash_after_migration_before_activation_keeps_old_code_disabled() {
    let mut service = Service::start();
    let original = fixture();
    assert!(service.deploy(&original).status().is_success());
    let mut pending = original.clone();
    pending.manifest.version = "2".into();
    pending.manifest.files.push("002.sql".into());
    pending.manifest.migrations.push("002.sql".into());
    pending.files.insert(
        "002.sql".into(),
        base64::engine::general_purpose::STANDARD
            .encode("ALTER TABLE counters ADD COLUMN label TEXT"),
    );
    pending.files.insert("app.mjs".into(), base64::engine::general_purpose::STANDARD.encode("console.log('activation-pending'); await new Promise(resolve => setTimeout(resolve, 20000)); export default {fetch(){return new Response('ok')}}"));
    let client = service.client.clone();
    let base = service.base();
    let token = service.token();
    let request = std::thread::spawn(move || {
        client
            .post(format!("{base}/v1/apps"))
            .bearer_auth(token)
            .json(&pending)
            .send()
    });
    let log = service.root.path().join("apps/counter/application.log");
    wait(|| std::fs::read_to_string(&log).is_ok_and(|text| text.contains("activation-pending")));
    service.restart();
    assert!(!request
        .join()
        .unwrap()
        .is_ok_and(|response| response.status().is_success()));
    assert_eq!(
        service
            .client
            .get(format!("{}/apps/counter", service.base()))
            .send()
            .unwrap()
            .status(),
        503
    );
    let mut fixed = original;
    fixed.manifest.version = "2-fixed".into();
    fixed.manifest.files.push("002.sql".into());
    fixed.manifest.migrations.push("002.sql".into());
    fixed.files.insert(
        "002.sql".into(),
        base64::engine::general_purpose::STANDARD
            .encode("ALTER TABLE counters ADD COLUMN label TEXT"),
    );
    let response = service.deploy(&fixed);
    assert!(
        response.status().is_success(),
        "{}",
        response.text().unwrap()
    );
    assert_eq!(service.state()[0]["value"], 0);
}

#[test]
fn in_flight_job_recovers_after_supervisor_crash_with_one_database_effect() {
    let mut service = Service::start();
    let mut release = fixture();
    release.manifest.resources.request_timeout_ms = 500;
    let source = String::from_utf8(release.decode("app.mjs").unwrap()).unwrap();
    let source = source.replace("], { idempotencyKey: `job:${message.id}` });", "], { idempotencyKey: `job:${message.id}` }); console.log(`job-attempt:${message.attempt}`); if (message.attempt === 1) await new Promise(resolve => setTimeout(resolve, 15000));");
    assert!(source.contains("job-attempt:"));
    release.files.insert(
        "app.mjs".into(),
        base64::engine::general_purpose::STANDARD.encode(source),
    );
    let response = service.deploy(&release);
    assert!(
        response.status().is_success(),
        "{}",
        response.text().unwrap()
    );
    service
        .client
        .post(format!("{}/apps/counter/jobs", service.base()))
        .header("idempotency-key", "crash-retry")
        .send()
        .unwrap()
        .error_for_status()
        .unwrap();
    let log = service.root.path().join("apps/counter/application.log");
    wait(|| std::fs::read_to_string(&log).is_ok_and(|text| text.contains("job-attempt:1")));
    service.restart();
    wait(|| std::fs::read_to_string(&log).is_ok_and(|text| text.contains("job-attempt:2")));
    wait(|| {
        let status: Value = service
            .client
            .get(format!("{}/v1/apps/counter/jobs", service.base()))
            .bearer_auth(service.token())
            .send()
            .unwrap()
            .json()
            .unwrap();
        status["in_flight"] == 0 && status["pending_redelivery"] == 0 && status["dead_letters"] == 0
    });
    assert_eq!(service.state()[0]["value"], 1);
}

#[test]
fn cli_initializes_and_deploys_from_the_default_manifest_path() {
    let service = Service::start();
    let application = tempfile::tempdir().unwrap();
    let output = Command::new(env!("CARGO_BIN_EXE_bic"))
        .arg("init")
        .arg(application.path())
        .args(["--name", "counter"])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let output = Command::new(env!("CARGO_BIN_EXE_bic"))
        .current_dir(application.path())
        .arg("--data")
        .arg(service.root.path())
        .args(["--endpoint", &service.base(), "deploy"])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(service.state()[1]["value"], 0);
}

#[test]
fn sigterm_drains_the_supervisor_and_releases_storage_for_restart() {
    let mut service = Service::start();
    assert!(service.deploy(&fixture()).status().is_success());
    let unauthorized = service
        .client
        .post(format!("{}/v1/apps", service.base()))
        .header("content-type", "application/json")
        .body("invalid JSON")
        .send()
        .unwrap();
    assert_eq!(
        unauthorized.status(),
        401,
        "authorization runs before package decoding"
    );
    assert_eq!(
        unsafe { libc::kill(service.child.id() as i32, libc::SIGTERM) },
        0
    );
    wait(|| service.child.try_wait().unwrap().is_some());
    service.child = spawn(service.root.path(), service.port);
    service.ready();
    assert_eq!(service.state()[1]["value"], 0);
}

#[test]
fn encrypted_recovery_image_moves_data_jobs_and_release_to_an_inactive_catalog() {
    let mut source = Service::start();
    let package = fixture();
    assert!(source.deploy(&package).status().is_success());
    source
        .client
        .post(format!("{}/apps/counter/increment", source.base()))
        .header("idempotency-key", "before-backup")
        .send()
        .unwrap()
        .error_for_status()
        .unwrap();
    let files = tempfile::tempdir().unwrap();
    let key = files.path().join("backup.key");
    bicdb_app_host::store::write_synced(&key, b"recovery-test-key-with-at-least-32-characters")
        .unwrap();
    let archive = files.path().join("counter.bicbak");
    let output = Command::new(env!("CARGO_BIN_EXE_bic"))
        .arg("--data")
        .arg(source.root.path())
        .args(["--endpoint", &source.base(), "backup", "counter"])
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
    let target = tempfile::tempdir().unwrap();
    let output = Command::new(env!("CARGO_BIN_EXE_bic"))
        .arg("--data")
        .arg(target.path())
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
    let deployment: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(deployment["enabled"], false);
    // Fence the previous runtime before activating the recovered identity.
    source.child.kill().unwrap();
    source.child.wait().unwrap();
    let port = TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port();
    let recovered = Service {
        child: spawn(target.path(), port),
        root: target,
        port,
        client: source.client.clone(),
    };
    recovered.ready();
    assert_eq!(
        recovered
            .client
            .get(format!("{}/apps/counter", recovered.base()))
            .send()
            .unwrap()
            .status(),
        503
    );
    let response = recovered
        .client
        .post(format!("{}/v1/apps/counter/start", recovered.base()))
        .bearer_auth(recovered.token())
        .send()
        .unwrap();
    assert!(
        response.status().is_success(),
        "{}",
        response.text().unwrap()
    );
    assert_eq!(recovered.state()[1]["value"], 1);
    for _ in 0..2 {
        recovered
            .client
            .post(format!(
                "{}/apps/counter/increment-and-enqueue",
                recovered.base()
            ))
            .header("idempotency-key", "atomic-job")
            .send()
            .unwrap()
            .error_for_status()
            .unwrap();
    }
    wait(|| recovered.state()[0]["value"] == 1);
    assert_eq!(recovered.state()[1]["value"], 2);
    let again = Command::new(env!("CARGO_BIN_EXE_bic"))
        .arg("--data")
        .arg(recovered.root.path())
        .arg("restore")
        .arg(&archive)
        .arg("--passphrase-file")
        .arg(&key)
        .output()
        .unwrap();
    assert!(
        !again.status.success(),
        "restore cannot replace a live catalog"
    );
}

#[test]
fn stopped_and_replaced_execution_credentials_cannot_query_the_application() {
    let service = Service::start();
    let mut release = fixture();
    let source = String::from_utf8(release.decode("app.mjs").unwrap()).unwrap();
    let source = source.replace("const path = new URL(request.url).pathname;", "const path = new URL(request.url).pathname; if (path === '/execution-token') return Response.json({ token: process.env.BICDB_DATABASE_TOKEN });");
    release.files.insert(
        "app.mjs".into(),
        base64::engine::general_purpose::STANDARD.encode(source),
    );
    assert!(service.deploy(&release).status().is_success());
    let read_token = || -> Value {
        service
            .client
            .get(format!("{}/apps/counter/execution-token", service.base()))
            .send()
            .unwrap()
            .error_for_status()
            .unwrap()
            .json()
            .unwrap()
    };
    let old = read_token()["token"].as_str().unwrap().to_owned();
    service
        .client
        .post(format!("{}/v1/apps/counter/stop", service.base()))
        .bearer_auth(service.token())
        .send()
        .unwrap()
        .error_for_status()
        .unwrap();
    let request =
        json!({"statements":[{"sql":"UPDATE counters SET value = 99 WHERE id = 'requests'"}]});
    assert_eq!(
        service
            .client
            .post(format!("{}/internal/counter/query", service.base()))
            .bearer_auth(&old)
            .json(&request)
            .send()
            .unwrap()
            .status(),
        401
    );
    service
        .client
        .post(format!("{}/v1/apps/counter/start", service.base()))
        .bearer_auth(service.token())
        .send()
        .unwrap()
        .error_for_status()
        .unwrap();
    assert_ne!(read_token()["token"], old);
    assert_eq!(
        service
            .client
            .post(format!("{}/internal/counter/query", service.base()))
            .bearer_auth(&old)
            .json(&request)
            .send()
            .unwrap()
            .status(),
        401
    );
    assert_eq!(service.state()[1]["value"], 0);
}

#[test]
fn live_sql_streams_updates_and_releases_admission_when_consumers_disconnect() {
    use std::io::{BufRead, BufReader};
    let service = Service::start();
    let release = fixture();
    let response = service.deploy(&release);
    assert!(
        response.status().is_success(),
        "{}",
        response.text().unwrap()
    );
    let open = || {
        let started = Instant::now();
        loop {
            let response = service
                .client
                .get(format!("{}/apps/counter/live", service.base()))
                .send()
                .unwrap();
            if response.status() != 429 {
                break response.error_for_status().unwrap();
            }
            assert!(
                started.elapsed() < Duration::from_secs(5),
                "cancelled live query leaked admission"
            );
            std::thread::sleep(Duration::from_millis(50));
        }
    };
    fn event(reader: &mut impl BufRead) -> Value {
        let mut value = None;
        loop {
            let mut line = String::new();
            assert!(
                reader.read_line(&mut line).unwrap() > 0,
                "live response ended before an event"
            );
            if let Some(data) = line.strip_prefix("data: ") {
                value = Some(serde_json::from_str(data.trim()).unwrap());
            }
            if line.trim().is_empty() {
                if let Some(value) = value.take() {
                    return value;
                }
            }
        }
    }
    let response = open();
    assert!(response.headers()["content-type"]
        .to_str()
        .unwrap()
        .starts_with("text/event-stream"));
    let mut reader = BufReader::new(response);
    let initial = event(&mut reader);
    assert_eq!(initial["initial"], true);
    assert_eq!(initial["changes"].as_array().unwrap().len(), 2);
    let response = service
        .client
        .post(format!("{}/apps/counter/increment", service.base()))
        .header("idempotency-key", "live-update")
        .send()
        .unwrap();
    assert!(
        response.status().is_success(),
        "{}",
        response.text().unwrap()
    );
    let changed = event(&mut reader);
    assert_eq!(changed["initial"], false);
    assert!(changed["changes"]
        .as_array()
        .unwrap()
        .iter()
        .any(|change| change["change"] == "+"
            && change["row"]["id"] == "requests"
            && change["row"]["value"] == 1));
    assert!(changed["changes"]
        .as_array()
        .unwrap()
        .iter()
        .all(|change| change["commit_seq"].is_string()));
    drop(reader);
    // More than the live admission ceiling, sequentially: cancellation must
    // release each permit rather than leaking it until the connection deadline.
    for _ in 0..20 {
        let mut reader = BufReader::new(open());
        assert_eq!(event(&mut reader)["initial"], true);
        drop(reader);
    }
    let mut reader = BufReader::new(open());
    event(&mut reader);
    // An active subscription cannot hold the application deployment gate open.
    let response = service
        .client
        .post(format!("{}/v1/apps/counter/stop", service.base()))
        .bearer_auth(service.token())
        .send()
        .unwrap();
    assert!(
        response.status().is_success(),
        "{}",
        response.text().unwrap()
    );
    let mut tail = String::new();
    let started = Instant::now();
    let stopped = reader.read_line(&mut tail);
    assert!(
        started.elapsed() < Duration::from_secs(3),
        "revoked stream did not terminate promptly"
    );
    assert!(stopped.is_err() || stopped.unwrap() == 0);
}

#[test]
fn cli_creates_inactive_isolated_previews_and_can_explicitly_activate_them() {
    let service = Service::start();
    assert!(service.deploy(&fixture()).status().is_success());
    let response = service
        .client
        .post(format!("{}/apps/counter/increment", service.base()))
        .header("idempotency-key", "shared-preview-effect")
        .send()
        .unwrap();
    assert_eq!(
        response
            .error_for_status()
            .unwrap()
            .json::<Value>()
            .unwrap(),
        json!({"value":1})
    );
    assert_eq!(
        service
            .client
            .post(format!("{}/v1/apps/counter/previews", service.base()))
            .json(&json!({"application":"unauthorized-preview"}))
            .send()
            .unwrap()
            .status(),
        401
    );
    let run = |arguments: &[&str]| {
        Command::new(env!("CARGO_BIN_EXE_bic"))
            .arg("--data")
            .arg(service.root.path())
            .arg("--endpoint")
            .arg(service.base())
            .args(arguments)
            .output()
            .unwrap()
    };
    let output = run(&["preview", "counter", "counter-preview"]);
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let deployment: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(deployment["enabled"], false);
    assert_eq!(
        service
            .client
            .get(format!("{}/apps/counter-preview", service.base()))
            .send()
            .unwrap()
            .status(),
        503
    );
    let identity = |application: &str| -> Value {
        service
            .client
            .get(format!("{}/v1/apps/{application}/identity", service.base()))
            .bearer_auth(service.token())
            .send()
            .unwrap()
            .error_for_status()
            .unwrap()
            .json()
            .unwrap()
    };
    assert_ne!(
        identity("counter")["node_id"],
        identity("counter-preview")["node_id"]
    );
    assert_ne!(
        identity("counter")["public_key"],
        identity("counter-preview")["public_key"]
    );
    let output = run(&["start", "counter-preview"]);
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let response = service
        .client
        .post(format!("{}/apps/counter-preview/increment", service.base()))
        .header("idempotency-key", "shared-preview-effect")
        .send()
        .unwrap();
    assert_eq!(
        response
            .error_for_status()
            .unwrap()
            .json::<Value>()
            .unwrap(),
        json!({"value":2})
    );
    assert_eq!(service.state()[1]["value"], 1);
    let output = run(&["preview", "counter", "active-preview", "--start"]);
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(
        serde_json::from_slice::<Value>(&output.stdout).unwrap()["enabled"],
        true
    );
    assert!(service
        .client
        .get(format!("{}/apps/active-preview", service.base()))
        .send()
        .unwrap()
        .status()
        .is_success());
    assert!(!run(&["preview", "counter", "counter-preview"])
        .status
        .success());
    assert!(!run(&["preview", "counter", "../outside"]).status.success());
    assert_eq!(
        service.state()[1]["value"],
        1,
        "failed preview creation must leave the parent serving"
    );
}
