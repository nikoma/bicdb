use std::{
    net::TcpListener,
    path::Path,
    process::{Child, Command, Stdio},
    time::{Duration, Instant},
};

use base64::Engine;
use bicdb_app_host::{
    manifest::{Placement, Release},
    store::write_synced,
};
use serde_json::{json, Value};

struct Service {
    child: Child,
    port: u16,
}
impl Service {
    fn spawn(root: &Path, port: u16, args: &[String]) -> Self {
        let child = Command::new(env!("CARGO_BIN_EXE_bic"))
            .arg("--data")
            .arg(root)
            .args(args)
            .args(["--listen", &format!("127.0.0.1:{port}")])
            .stdout(Stdio::null())
            .stderr(Stdio::inherit())
            .spawn()
            .unwrap();
        let service = Self { child, port };
        wait(|| {
            reqwest::blocking::get(format!("{}/health", service.base()))
                .is_ok_and(|r| r.status().is_success())
        });
        service
    }
    fn base(&self) -> String {
        format!("http://127.0.0.1:{}", self.port)
    }
    fn kill(&mut self) {
        self.child.kill().unwrap();
        self.child.wait().unwrap();
    }
}
impl Drop for Service {
    fn drop(&mut self) {
        if self.child.try_wait().is_ok_and(|status| status.is_some()) {
            return;
        }
        unsafe {
            libc::kill(self.child.id() as i32, libc::SIGTERM);
        }
        let deadline = Instant::now() + Duration::from_secs(3);
        while Instant::now() < deadline {
            if self.child.try_wait().is_ok_and(|status| status.is_some()) {
                return;
            }
            std::thread::sleep(Duration::from_millis(25));
        }
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

#[test]
#[ignore = "requires Linux KVM, root and BICDB_TEST_MICROVM_CONFIG with pinned images"]
fn remote_microvm_reconnects_to_authoritative_state_after_worker_crash() {
    let config =
        std::env::var_os("BICDB_TEST_MICROVM_CONFIG").expect("microVM host configuration required");
    let root = tempfile::tempdir().unwrap();
    let state_port = port();
    let worker_port = port();
    let state_base = format!("http://127.0.0.1:{state_port}");
    let worker_root = root.path().join("worker");
    let state_root = root.path().join("state");
    std::fs::create_dir(&worker_root).unwrap();
    std::fs::create_dir(&state_root).unwrap();
    let management = "state-management-token-for-vm-test-0000000";
    let worker_token = "worker-management-token-for-vm-test-000000";
    write_synced(&state_root.join("management.token"), management.as_bytes()).unwrap();
    write_synced(
        &worker_root.join("management.token"),
        worker_token.as_bytes(),
    )
    .unwrap();
    let worker_args = vec![
        "worker".into(),
        "--state-endpoint".into(),
        state_base.clone(),
        "--microvm-config".into(),
        config.to_str().unwrap().into(),
    ];
    let mut worker = Service::spawn(&worker_root, worker_port, &worker_args);
    let fleet = root.path().join("fleet.json");
    write_synced(
        &fleet,
        &serde_json::to_vec(&json!({"format":1,"workers":{"vm":{
            "endpoint":worker.base(),"token_file":worker_root.join("management.token"),
            "region":"test","capacity":1,"isolation":"microvm"
        }}}))
        .unwrap(),
    )
    .unwrap();
    let state = Service::spawn(
        &state_root,
        state_port,
        &[
            "serve".into(),
            "--fleet-config".into(),
            fleet.to_str().unwrap().into(),
        ],
    );
    let client = reqwest::blocking::Client::builder()
        .timeout(Duration::from_secs(20))
        .build()
        .unwrap();
    let mut release = fixture();
    release.manifest.isolation = bicdb_app_host::manifest::Isolation::Microvm;
    release.manifest.resources.memory_mb = 256;
    release.manifest.resources.request_timeout_ms = 5000;
    let response = client
        .post(format!("{state_base}/v1/apps"))
        .bearer_auth(management)
        .json(&release)
        .send()
        .unwrap();
    assert!(
        response.status().is_success(),
        "{}",
        response.text().unwrap()
    );
    let app = format!("{state_base}/apps/counter");
    let started = Instant::now();
    let mut live = std::io::BufReader::new(
        client
            .get(format!("{app}/live"))
            .send()
            .unwrap()
            .error_for_status()
            .unwrap(),
    );
    let initial = live_event(&mut live);
    assert!(
        started.elapsed() < Duration::from_secs(3),
        "microVM event response was buffered"
    );
    assert_eq!(initial["initial"], true);
    let result = client
        .post(format!("{app}/increment-and-enqueue"))
        .header("idempotency-key", "vm-job")
        .send()
        .unwrap()
        .error_for_status()
        .unwrap()
        .json::<Value>()
        .unwrap();
    assert_eq!(result, json!({"value":1}));
    loop {
        let changes = live_event(&mut live);
        if changes["changes"].as_array().unwrap().iter().any(|change| {
            change["change"] == "+"
                && change["row"]["id"] == "requests"
                && change["row"]["value"] == 1
        }) {
            break;
        }
    }
    drop(live);
    let record = worker_root.join("apps/counter/microvm-process.json");
    let previous: Value = serde_json::from_slice(&std::fs::read(&record).unwrap()).unwrap();
    worker.kill();
    worker = Service::spawn(&worker_root, worker_port, &worker_args);
    wait(|| {
        client
            .get(&app)
            .send()
            .ok()
            .and_then(|r| r.json::<Value>().ok())
            == Some(json!([{"id":"jobs","value":1},{"id":"requests","value":1}]))
    });
    let replacement: Value = serde_json::from_slice(&std::fs::read(&record).unwrap()).unwrap();
    assert_ne!(previous["pid"], replacement["pid"]);
    assert!(
        !Path::new(&format!("/proc/{}", previous["pid"].as_u64().unwrap())).exists(),
        "orphaned VMM must be fenced"
    );
    assert!(
        !worker_root.join("apps/counter/database").exists(),
        "worker must not hold authoritative database state"
    );
    assert!(state_root.join("apps/counter/database/data").exists());
    assert_eq!(
        client
            .post(format!("{app}/increment-and-enqueue"))
            .header("idempotency-key", "vm-job")
            .send()
            .unwrap()
            .error_for_status()
            .unwrap()
            .json::<Value>()
            .unwrap(),
        json!({"value":1})
    );
    // Removing VM configuration must still fence the recorded guest. The
    // worker can start in process mode without silently retaining an old VM.
    worker.kill();
    worker = Service::spawn(
        &worker_root,
        worker_port,
        &[
            "worker".into(),
            "--state-endpoint".into(),
            state_base.clone(),
        ],
    );
    assert!(!Path::new(&format!("/proc/{}", replacement["pid"].as_u64().unwrap())).exists());
    assert!(!record.exists());
    drop(worker);
    worker = Service::spawn(&worker_root, worker_port, &worker_args);
    wait(|| {
        client
            .get(&app)
            .send()
            .ok()
            .and_then(|r| r.json::<Value>().ok())
            == Some(json!([{"id":"jobs","value":1},{"id":"requests","value":1}]))
    });
    drop(state);
    drop(worker);
}
fn live_event(reader: &mut impl std::io::BufRead) -> Value {
    let mut value = None;
    loop {
        let mut line = String::new();
        assert!(
            reader.read_line(&mut line).unwrap() > 0,
            "remote live stream ended"
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
fn port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}
fn wait(mut condition: impl FnMut() -> bool) {
    let start = Instant::now();
    while !condition() {
        assert!(
            start.elapsed() < Duration::from_secs(25),
            "fleet condition timed out"
        );
        std::thread::sleep(Duration::from_millis(50));
    }
}
fn fixture() -> Release {
    let mut release = Release::read(
        &Path::new(env!("CARGO_MANIFEST_DIR")).join("../../examples/platform/counter/bic.json"),
    )
    .unwrap();
    release.manifest.resources.request_timeout_ms = 1000;
    release.manifest.placement = Some(Placement {
        region: "test".into(),
    });
    let source = String::from_utf8(release.decode("app.mjs").unwrap())
        .unwrap()
        .replace("export default {", "const application = {");
    // Exercise database access during module initialization as well as requests.
    let source = format!(
        r#"
const boot = await fetch(process.env.BICDB_DATABASE_URL + '/query', {{
  method: 'POST', headers: {{ authorization: `Bearer ${{process.env.BICDB_DATABASE_TOKEN}}`, 'content-type':'application/json' }},
  body: JSON.stringify({{ statements: [{{sql:'SELECT 1'}}] }})
}});
if (!boot.ok) throw new Error('bootstrap database unavailable');
console.log('application initialized');
{source}
export default {{ ...application, async fetch(request, env) {{
  if (new URL(request.url).pathname === '/inspect') return Response.json({{ pid:process.pid, token:process.env.BICDB_DATABASE_TOKEN }});
  if (new URL(request.url).pathname === '/hang') return await new Promise(() => {{}});
  return application.fetch(request, env);
}} }};
"#
    );
    release.files.insert(
        "app.mjs".into(),
        base64::engine::general_purpose::STANDARD.encode(source),
    );
    release
}

#[test]
fn old_request_timeout_does_not_retire_a_replacement_execution() {
    let root = tempfile::tempdir().unwrap();
    let state_port = port();
    let state_base = format!("http://127.0.0.1:{state_port}");
    let management = "state-management-token-for-race-test-000000";
    let worker_token = "worker-management-token-for-race-test-00000";
    let state_root = root.path().join("state");
    std::fs::create_dir(&state_root).unwrap();
    write_synced(&state_root.join("management.token"), management.as_bytes()).unwrap();
    let mut workers = Vec::new();
    let mut allocations = serde_json::Map::new();
    for name in ["a", "b"] {
        let directory = root.path().join(name);
        std::fs::create_dir(&directory).unwrap();
        let secret = directory.join("management.token");
        write_synced(&secret, worker_token.as_bytes()).unwrap();
        let worker = Service::spawn(
            &directory,
            port(),
            &[
                "worker".into(),
                "--state-endpoint".into(),
                state_base.clone(),
            ],
        );
        allocations.insert(name.into(), json!({"endpoint":worker.base(),"token_file":secret,"region":"test","capacity":1,"isolation":"process"}));
        workers.push(worker);
    }
    let config = root.path().join("fleet.json");
    write_synced(
        &config,
        &serde_json::to_vec(&json!({"format":1,"workers":allocations})).unwrap(),
    )
    .unwrap();
    let _state = Service::spawn(
        &state_root,
        state_port,
        &[
            "serve".into(),
            "--fleet-config".into(),
            config.to_str().unwrap().into(),
        ],
    );
    let client = reqwest::blocking::Client::builder()
        .timeout(Duration::from_secs(15))
        .build()
        .unwrap();
    let mut release = fixture();
    release.manifest.resources.request_timeout_ms = 5000;
    client
        .post(format!("{state_base}/v1/apps"))
        .bearer_auth(management)
        .json(&release)
        .send()
        .unwrap()
        .error_for_status()
        .unwrap();
    let request = std::thread::spawn({
        let client = client.clone();
        let base = state_base.clone();
        move || {
            client
                .get(format!("{base}/apps/counter/hang"))
                .send()
                .unwrap()
                .status()
        }
    });
    // The old request is waiting on its response when worker management is
    // cut off. Its five-second request deadline outlasts heartbeat failover.
    std::thread::sleep(Duration::from_millis(200));
    unsafe {
        assert_eq!(libc::kill(workers[0].child.id() as i32, libc::SIGSTOP), 0);
    }
    wait(|| {
        client
            .get(format!("{state_base}/v1/apps"))
            .bearer_auth(management)
            .send()
            .ok()
            .and_then(|r| r.json::<Value>().ok())
            .is_some_and(|apps| apps[0]["worker"] == "b")
    });
    let inspect = || -> Value {
        client
            .get(format!("{state_base}/apps/counter/inspect"))
            .send()
            .unwrap()
            .error_for_status()
            .unwrap()
            .json()
            .unwrap()
    };
    let replacement = inspect();
    assert_eq!(request.join().unwrap(), 504);
    assert_eq!(
        inspect()["token"],
        replacement["token"],
        "late timeout must preserve the replacement"
    );
    unsafe {
        assert_eq!(libc::kill(workers[0].child.id() as i32, libc::SIGCONT), 0);
    }
}

#[test]
fn worker_failure_and_partition_recover_compute_without_replaying_database_effects() {
    let root = tempfile::tempdir().unwrap();
    let state_port = port();
    let state_base = format!("http://127.0.0.1:{state_port}");
    let management = "state-management-token-for-test-only-000000";
    let worker_token = "worker-management-token-for-test-only-00000";
    let client = reqwest::blocking::Client::builder()
        .timeout(Duration::from_secs(20))
        .build()
        .unwrap();
    let state = root.path().join("state");
    std::fs::create_dir(&state).unwrap();
    write_synced(&state.join("management.token"), management.as_bytes()).unwrap();
    let mut workers = Vec::new();
    let mut allocations = serde_json::Map::new();
    for name in ["a", "b"] {
        let directory = root.path().join(name);
        std::fs::create_dir(&directory).unwrap();
        let token = directory.join("management.token");
        write_synced(&token, worker_token.as_bytes()).unwrap();
        let service = Service::spawn(
            &directory,
            port(),
            &[
                "worker".into(),
                "--state-endpoint".into(),
                state_base.clone(),
                "--capacity".into(),
                "1".into(),
            ],
        );
        allocations.insert(name.into(), json!({"endpoint":service.base(),"token_file":token,"region":"test","capacity":1,"isolation":"process"}));
        workers.push(service);
    }
    let config = root.path().join("fleet.json");
    write_synced(
        &config,
        &serde_json::to_vec(&json!({"format":1,"workers":allocations})).unwrap(),
    )
    .unwrap();
    let state_service = Service::spawn(
        &state,
        state_port,
        &[
            "serve".into(),
            "--fleet-config".into(),
            config.to_str().unwrap().into(),
        ],
    );
    assert_eq!(
        client
            .post(format!("{}/v1/executions", workers[0].base()))
            .json(&json!({}))
            .send()
            .unwrap()
            .status(),
        401
    );
    let release = fixture();
    let response = client
        .post(format!("{state_base}/v1/apps"))
        .bearer_auth(management)
        .json(&release)
        .send()
        .unwrap();
    assert!(
        response.status().is_success(),
        "{}",
        response.text().unwrap()
    );
    let app = format!("{state_base}/apps/counter");
    let inspect = || -> Value {
        client
            .get(format!("{app}/inspect"))
            .send()
            .unwrap()
            .error_for_status()
            .unwrap()
            .json()
            .unwrap()
    };
    let old = inspect();
    let log = client
        .get(format!("{state_base}/v1/apps/counter/logs"))
        .bearer_auth(management)
        .send()
        .unwrap()
        .error_for_status()
        .unwrap()
        .text()
        .unwrap();
    assert!(log.contains("application initialized"));
    // A second application consumes the other worker's slot. A third stays
    // disabled until capacity becomes available, rather than running locally.
    let mut second = release.clone();
    second.manifest.name = "other".into();
    assert!(client
        .post(format!("{state_base}/v1/apps"))
        .bearer_auth(management)
        .json(&second)
        .send()
        .unwrap()
        .status()
        .is_success());
    let allocations: Value = client
        .get(format!("{state_base}/v1/apps"))
        .bearer_auth(management)
        .send()
        .unwrap()
        .json()
        .unwrap();
    assert_eq!(allocations[1]["worker"], "b");
    second.manifest.name = "third".into();
    assert_eq!(
        client
            .post(format!("{state_base}/v1/apps"))
            .bearer_auth(management)
            .json(&second)
            .send()
            .unwrap()
            .status(),
        400
    );
    client
        .post(format!("{state_base}/v1/apps/other/stop"))
        .bearer_auth(management)
        .send()
        .unwrap()
        .error_for_status()
        .unwrap();
    let response = client
        .post(format!("{app}/increment-and-enqueue"))
        .header("idempotency-key", "same-request")
        .send()
        .unwrap()
        .error_for_status()
        .unwrap();
    assert_eq!(response.json::<Value>().unwrap(), json!({"value":1}));
    workers[0].kill();
    wait(|| {
        client
            .get(format!("{app}/inspect"))
            .send()
            .is_ok_and(|r| r.status().is_success())
    });
    let replacement = inspect();
    assert_ne!(old["token"], replacement["token"]);
    assert_eq!(
        client
            .post(format!("{state_base}/internal/counter/query"))
            .bearer_auth(old["token"].as_str().unwrap())
            .json(&json!({"statements":[{"sql":"SELECT 1"}]}))
            .send()
            .unwrap()
            .status(),
        401
    );
    let status: Value = client
        .get(format!("{state_base}/v1/apps"))
        .bearer_auth(management)
        .send()
        .unwrap()
        .json()
        .unwrap();
    assert_eq!(status[0]["worker"], "b");
    // The original request and its job retain their IDs at the state service.
    assert_eq!(
        client
            .post(format!("{app}/increment-and-enqueue"))
            .header("idempotency-key", "same-request")
            .send()
            .unwrap()
            .error_for_status()
            .unwrap()
            .json::<Value>()
            .unwrap(),
        json!({"value":1})
    );
    wait(|| {
        let counters = client
            .get(&app)
            .send()
            .ok()
            .and_then(|r| r.json::<Value>().ok());
        counters == Some(json!([{"id":"jobs","value":1},{"id":"requests","value":1}]))
    });

    // Freeze the authority, leaving the worker alive. Lease expiry must kill
    // the computation even though no stop instruction can reach it.
    let pid = replacement["pid"].as_i64().unwrap() as i32;
    unsafe {
        assert_eq!(
            libc::kill(state_service.child.id() as i32, libc::SIGSTOP),
            0
        );
    }
    wait(|| unsafe { libc::kill(pid, 0) != 0 });
    unsafe {
        assert_eq!(
            libc::kill(state_service.child.id() as i32, libc::SIGCONT),
            0
        );
    }
    wait(|| {
        client
            .get(format!("{app}/inspect"))
            .send()
            .is_ok_and(|r| r.status().is_success())
    });
    let resumed = inspect();
    assert_ne!(replacement["token"], resumed["token"]);
    assert_eq!(
        client.get(&app).send().unwrap().json::<Value>().unwrap(),
        json!([{"id":"jobs","value":1},{"id":"requests","value":1}])
    );
    // Region requirements fail closed, without falling back to local execution.
    let mut invalid = release;
    invalid.manifest.placement.as_mut().unwrap().region = "unavailable".into();
    assert_eq!(
        client
            .post(format!("{state_base}/v1/apps"))
            .bearer_auth(management)
            .json(&invalid)
            .send()
            .unwrap()
            .status(),
        400
    );
    assert_eq!(inspect()["token"], resumed["token"]);
    client
        .post(format!("{state_base}/v1/apps/counter/stop"))
        .bearer_auth(management)
        .send()
        .unwrap()
        .error_for_status()
        .unwrap();
    assert_eq!(
        client
            .post(format!("{state_base}/internal/counter/query"))
            .bearer_auth(resumed["token"].as_str().unwrap())
            .json(&json!({"statements":[{"sql":"SELECT 1"}]}))
            .send()
            .unwrap()
            .status(),
        401
    );
    wait(|| unsafe { libc::kill(resumed["pid"].as_i64().unwrap() as i32, 0) != 0 });
}
